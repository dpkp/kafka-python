from __future__ import absolute_import

import collections
import copy
import logging
import threading

import six

from .. import errors as Errors
from ..structs import TopicPartition
from ..version import __version__
from ..protocol.produce import ProduceRequest

log = logging.getLogger(__name__)


class Sender(threading.Thread):
    """
    The background thread that handles the sending of produce requests to the
    Kafka cluster. This thread makes metadata requests to renew its view of the
    cluster and then sends produce requests to the appropriate nodes.
    """
    _DEFAULT_CONFIG = {
        'max_request_size': 1048576,
        'acks': 1,
        'retries': 0,
        'request_timeout_ms': 30000,
        'guarantee_message_order': False,
        'client_id': 'kafka-python-' + __version__,
        'api_version': (0, 8, 0),
    }

    def __init__(self, client, metadata, accumulator, **configs):
        super(Sender, self).__init__()
        self.config = copy.copy(self._DEFAULT_CONFIG)
        for key in self.config:
            if key in configs:
                self.config[key] = configs.pop(key)

        self.name = self.config['client_id'] + '-network-thread'
        self._client = client
        self._accumulator = accumulator
        self._metadata = client.cluster
        self._running = True
        self._force_close = False
        self._topics_to_add = set()

    def run(self):
        """The main run loop for the sender thread."""
        log.debug("Starting Kafka producer I/O thread.")

        # main loop, runs until close is called
        while self._running:
            try:
                self.run_once()
            except Exception:
                log.exception("Uncaught error in kafka producer I/O thread")

        log.debug("Beginning shutdown of Kafka producer I/O thread, sending"
                  " remaining records.")

        # okay we stopped accepting requests but there may still be
        # requests in the accumulator or waiting for acknowledgment,
        # wait until these are completed.
        while (not self._force_close
               and (self._accumulator.has_unsent()
                    or self._client.in_flight_request_count() > 0)):
            try:
                self.run_once()
            except Exception:
                log.exception("Uncaught error in kafka producer I/O thread")

        if self._force_close:
            # We need to fail all the incomplete batches and wake up the
            # threads waiting on the futures.
            self._accumulator.abort_incomplete_batches()

        try:
            self._client.close()
        except Exception:
            log.exception("Failed to close network client")

        log.debug("Shutdown of Kafka producer I/O thread has completed.")

    def run_once(self):
        """Run a single iteration of sending."""
        while self._topics_to_add:
            self._client.add_topic(self._topics_to_add.pop())

        # get the list of partitions with data ready to send
        result = self._accumulator.ready(self._metadata)
        ready_nodes, next_ready_check_delay, unknown_leaders_exist = result

        # if there are any partitions whose leaders are not known yet, force
        # metadata update
        if unknown_leaders_exist:
            log.debug('Unknown leaders exist, requesting metadata update')
            self._metadata.request_update()

        # remove any nodes we aren't ready to send to
        not_ready_timeout = 999999999
        for node in list(ready_nodes):
            if not self._client.ready(node):
                log.debug('Node %s not ready; delaying produce of accumulated batch', node)
                ready_nodes.remove(node)
                not_ready_timeout = min(not_ready_timeout,
                                        self._client.connection_delay(node))

        # create produce requests
        batches_by_node = self._accumulator.drain(
            self._metadata, ready_nodes, self.config['max_request_size'])

        if self.config['guarantee_message_order']:
            # Mute all the partitions drained
            for batch_list in six.itervalues(batches_by_node):
                for batch in batch_list:
                    self._accumulator.muted.add(batch.topic_partition)

        expired_batches = self._accumulator.abort_expired_batches(
            self.config['request_timeout_ms'], self._metadata)

        requests = self._create_produce_requests(batches_by_node)
        # If we have any nodes that are ready to send + have sendable data,
        # poll with 0 timeout so this can immediately loop and try sending more
        # data. Otherwise, the timeout is determined by nodes that have
        # partitions with data that isn't yet sendable (e.g. lingering, backing
        # off). Note that this specifically does not include nodes with
        # sendable data that aren't ready to send since they would cause busy
        # looping.
        poll_timeout_ms = min(next_ready_check_delay * 1000, not_ready_timeout)
        if ready_nodes:
            log.debug("Nodes with data ready to send: %s", ready_nodes) # trace
            log.debug("Created %d produce requests: %s", len(requests), requests) # trace
            poll_timeout_ms = 0

        for node_id, request in six.iteritems(requests):
            batches = batches_by_node[node_id]
            log.debug('Sending Produce Request: %r', request)
            (self._client.send(node_id, request)
                 .add_callback(
                     self._handle_produce_response, batches)
                 .add_errback(
                     self._failed_produce, batches, node_id))

        # if some partitions are already ready to be sent, the select time
        # would be 0; otherwise if some partition already has some data
        # accumulated but not ready yet, the select time will be the time
        # difference between now and its linger expiry time; otherwise the
        # select time will be the time difference between now and the
        # metadata expiry time
        self._client.poll(poll_timeout_ms, sleep=True)

    def initiate_close(self):
        """Start closing the sender (won't complete until all data is sent)."""
        self._running = False
        self._accumulator.close()
        self.wakeup()

    def force_close(self):
        """Closes the sender without sending out any pending messages."""
        self._force_close = True
        self.initiate_close()

    def add_topic(self, topic):
        if topic not in self._topics_to_add:
            self._topics_to_add.add(topic)
            self.wakeup()

    def _failed_produce(self, batches, node_id, error):
        log.debug("Error sending produce request to node %d: %s", node_id, error) # trace
        for batch in batches:
            self._complete_batch(batch, error, -1, None)

    def _handle_produce_response(self, batches, response):
        """Handle a produce response."""
        # if we have a response, parse it
        log.debug('Parsing produce response: %r', response)
        if response:
            batches_by_partition = dict([(batch.topic_partition, batch)
                                         for batch in batches])

            for topic, partitions in response.topics:
                for partition_info in partitions:
                    if response.API_VERSION < 2:
                        partition, error_code, offset = partition_info
                        ts = None
                    else:
                        partition, error_code, offset, ts = partition_info
                    tp = TopicPartition(topic, partition)
                    error = Errors.for_code(error_code)
                    batch = batches_by_partition[tp]
                    self._complete_batch(batch, error, offset, ts)

        else:
            # this is the acks = 0 case, just complete all requests
            for batch in batches:
                self._complete_batch(batch, None, -1, None)

    def _complete_batch(self, batch, error, base_offset, timestamp_ms=None):
        """Complete or retry the given batch of records.

        Arguments:
            batch (RecordBatch): The record batch
            error (Exception): The error (or None if none)
            base_offset (int): The base offset assigned to the records if successful
            timestamp_ms (int, optional): The timestamp returned by the broker for this batch
        """
        # Standardize no-error to None
        if error is Errors.NoError:
            error = None

        if error is not None and self._can_retry(batch, error):
            # retry
            log.warning("Got error produce response on topic-partition %s,"
                        " retrying (%d attempts left). Error: %s",
                        batch.topic_partition,
                        self.config['retries'] - batch.attempts - 1,
                        error)
            self._accumulator.reenqueue(batch)
        else:
            if error is Errors.TopicAuthorizationFailedError:
                error = error(batch.topic_partition.topic)

            # tell the user the result of their request
            batch.done(base_offset, timestamp_ms, error)
            self._accumulator.deallocate(batch)

        if getattr(error, 'invalid_metadata', False):
            self._metadata.request_update()

        # Unmute the completed partition.
        if self.config['guarantee_message_order']:
            self._accumulator.muted.remove(batch.topic_partition)

    def _can_retry(self, batch, error):
        """
        We can retry a send if the error is transient and the number of
        attempts taken is fewer than the maximum allowed
        """
        return (batch.attempts < self.config['retries']
                and getattr(error, 'retriable', False))

    def _create_produce_requests(self, collated):
        """
        Transfer the record batches into a list of produce requests on a
        per-node basis.

        Arguments:
            collated: {node_id: [RecordBatch]}

        Returns:
            dict: {node_id: ProduceRequest} (version depends on api_version)
        """
        requests = {}
        for node_id, batches in six.iteritems(collated):
            requests[node_id] = self._produce_request(
                node_id, self.config['acks'],
                self.config['request_timeout_ms'], batches)
        return requests

    def _produce_request(self, node_id, acks, timeout, batches):
        """Create a produce request from the given record batches.

        Returns:
            ProduceRequest (version depends on api_version)
        """
        produce_records_by_partition = collections.defaultdict(dict)
        for batch in batches:
            topic = batch.topic_partition.topic
            partition = batch.topic_partition.partition

            # TODO: bytearray / memoryview
            buf = batch.records.buffer()
            produce_records_by_partition[topic][partition] = buf

        if self.config['api_version'] >= (0, 10):
            version = 2
        elif self.config['api_version'] == (0, 9):
            version = 1
        else:
            version = 0
        return ProduceRequest[version](
            required_acks=acks,
            timeout=timeout,
            topics=[(topic, list(partition_info.items()))
                    for topic, partition_info
                    in six.iteritems(produce_records_by_partition)]
        )

    def wakeup(self):
        """Wake up the selector associated with this send thread."""
        self._client.wakeup()
