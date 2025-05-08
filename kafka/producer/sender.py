from __future__ import absolute_import, division

import collections
import copy
import heapq
import logging
import threading
import time

from kafka.vendor import six

from kafka import errors as Errors
from kafka.metrics.measurable import AnonMeasurable
from kafka.metrics.stats import Avg, Max, Rate
from kafka.producer.transaction_manager import ProducerIdAndEpoch
from kafka.protocol.init_producer_id import InitProducerIdRequest
from kafka.protocol.produce import ProduceRequest
from kafka.structs import TopicPartition
from kafka.version import __version__

log = logging.getLogger(__name__)


class Sender(threading.Thread):
    """
    The background thread that handles the sending of produce requests to the
    Kafka cluster. This thread makes metadata requests to renew its view of the
    cluster and then sends produce requests to the appropriate nodes.
    """
    DEFAULT_CONFIG = {
        'max_request_size': 1048576,
        'acks': 1,
        'retries': float('inf'),
        'request_timeout_ms': 30000,
        'retry_backoff_ms': 100,
        'metrics': None,
        'guarantee_message_order': False,
        'transaction_manager': None,
        'transactional_id': None,
        'transaction_timeout_ms': 60000,
        'client_id': 'kafka-python-' + __version__,
    }

    def __init__(self, client, metadata, accumulator, **configs):
        super(Sender, self).__init__()
        self.config = copy.copy(self.DEFAULT_CONFIG)
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
        if self.config['metrics']:
            self._sensors = SenderMetrics(self.config['metrics'], self._client, self._metadata)
        else:
            self._sensors = None
        self._transaction_manager = self.config['transaction_manager']
        # A per-partition queue of batches ordered by creation time for tracking the in-flight batches
        self._in_flight_batches = collections.defaultdict(list)

    def _maybe_remove_from_inflight_batches(self, batch):
        try:
            queue = self._in_flight_batches[batch.topic_partition]
        except KeyError:
            return
        try:
            idx = queue.index((batch.created, batch))
        except ValueError:
            return
        # https://stackoverflow.com/questions/10162679/python-delete-element-from-heap
        queue[idx] = queue[-1]
        queue.pop()
        heapq.heapify(queue)

    def _get_expired_inflight_batches(self, now=None):
        """Get the in-flight batches that has reached delivery timeout."""
        expired_batches = []
        to_remove = []
        for tp, queue in six.iteritems(self._in_flight_batches):
            while queue:
                _created_at, batch = queue[0]
                if batch.has_reached_delivery_timeout(self._accumulator.delivery_timeout_ms):
                    heapq.heappop(queue)
                    if batch.final_state is None:
                        expired_batches.append(batch)
                    else:
                        raise Errors.IllegalStateError("%s batch created at %s gets unexpected final state %s" % (batch.topic_partition, batch.created, batch.final_state))
                else:
                    self._accumulator.maybe_update_next_batch_expiry_time(batch)
                    break
            else:
                # Avoid mutating in_flight_batches during iteration
                to_remove.append(tp)
        for tp in to_remove:
            del self._in_flight_batches[tp]
        return expired_batches

    def run(self):
        """The main run loop for the sender thread."""
        log.debug("%s: Starting Kafka producer I/O thread.", str(self))

        # main loop, runs until close is called
        while self._running:
            try:
                self.run_once()
            except Exception:
                log.exception("%s: Uncaught error in kafka producer I/O thread", str(self))

        log.debug("%s: Beginning shutdown of Kafka producer I/O thread, sending"
                  " remaining records.", str(self))

        # okay we stopped accepting requests but there may still be
        # requests in the accumulator or waiting for acknowledgment,
        # wait until these are completed.
        while (not self._force_close
               and (self._accumulator.has_undrained()
                    or self._client.in_flight_request_count() > 0)):
            try:
                self.run_once()
            except Exception:
                log.exception("%s: Uncaught error in kafka producer I/O thread", str(self))

        if self._force_close:
            # We need to fail all the incomplete batches and wake up the
            # threads waiting on the futures.
            self._accumulator.abort_incomplete_batches()

        try:
            self._client.close()
        except Exception:
            log.exception("%s: Failed to close network client", str(self))

        log.debug("%s: Shutdown of Kafka producer I/O thread has completed.", str(self))

    def run_once(self):
        """Run a single iteration of sending."""
        while self._topics_to_add:
            self._client.add_topic(self._topics_to_add.pop())

        if self._transaction_manager:
            try:
                if not self._transaction_manager.is_transactional():
                    # this is an idempotent producer, so make sure we have a producer id
                    self._maybe_wait_for_producer_id()
                elif self._transaction_manager.has_in_flight_transactional_request() or self._maybe_send_transactional_request():
                    # as long as there are outstanding transactional requests, we simply wait for them to return
                    self._client.poll(timeout_ms=self.config['retry_backoff_ms'])
                    return

                # do not continue sending if the transaction manager is in a failed state or if there
                # is no producer id (for the idempotent case).
                if self._transaction_manager.has_fatal_error() or not self._transaction_manager.has_producer_id():
                    last_error = self._transaction_manager.last_error
                    if last_error is not None:
                        self._maybe_abort_batches(last_error)
                    self._client.poll(timeout_ms=self.config['retry_backoff_ms'])
                    return
                elif self._transaction_manager.has_abortable_error():
                    self._accumulator.abort_undrained_batches(self._transaction_manager.last_error)

            except Errors.SaslAuthenticationFailedError as e:
                # This is already logged as error, but propagated here to perform any clean ups.
                log.debug("%s: Authentication exception while processing transactional request: %s", str(self), e)
                self._transaction_manager.authentication_failed(e)

        poll_timeout_ms = self._send_producer_data()
        self._client.poll(timeout_ms=poll_timeout_ms)

    def _send_producer_data(self, now=None):
        now = time.time() if now is None else now
        # get the list of partitions with data ready to send
        result = self._accumulator.ready(self._metadata, now=now)
        ready_nodes, next_ready_check_delay, unknown_leaders_exist = result

        # if there are any partitions whose leaders are not known yet, force
        # metadata update
        if unknown_leaders_exist:
            log.debug('%s: Unknown leaders exist, requesting metadata update', str(self))
            self._metadata.request_update()

        # remove any nodes we aren't ready to send to
        not_ready_timeout_ms = float('inf')
        for node in list(ready_nodes):
            if not self._client.is_ready(node):
                node_delay_ms = self._client.connection_delay(node)
                log.debug('%s: Node %s not ready; delaying produce of accumulated batch (%f ms)', str(self), node, node_delay_ms)
                self._client.maybe_connect(node, wakeup=False)
                ready_nodes.remove(node)
                not_ready_timeout_ms = min(not_ready_timeout_ms, node_delay_ms)

        # create produce requests
        batches_by_node = self._accumulator.drain(
            self._metadata, ready_nodes, self.config['max_request_size'], now=now)

        for batch_list in six.itervalues(batches_by_node):
            for batch in batch_list:
                item = (batch.created, batch)
                queue = self._in_flight_batches[batch.topic_partition]
                heapq.heappush(queue, item)

        if self.config['guarantee_message_order']:
            # Mute all the partitions drained
            for batch_list in six.itervalues(batches_by_node):
                for batch in batch_list:
                    self._accumulator.muted.add(batch.topic_partition)

        self._accumulator.reset_next_batch_expiry_time()
        expired_batches = self._accumulator.expired_batches(now=now)
        expired_batches.extend(self._get_expired_inflight_batches(now=now))

        if expired_batches:
            log.debug("%s: Expired %s batches in accumulator", str(self), len(expired_batches))

        # Reset the producer_id if an expired batch has previously been sent to the broker.
        # See the documentation of `TransactionState.reset_producer_id` to understand why
        # we need to reset the producer id here.
        if self._transaction_manager and any([batch.in_retry() for batch in expired_batches]):
            needs_transaction_state_reset = True
        else:
            needs_transaction_state_reset = False

        for expired_batch in expired_batches:
            error = Errors.KafkaTimeoutError(
                "Expiring %d record(s) for %s: %s ms has passed since batch creation" % (
                    expired_batch.record_count, expired_batch.topic_partition,
                    int((time.time() - expired_batch.created) * 1000)))
            self._fail_batch(expired_batch, error, base_offset=-1)

        if self._sensors:
            self._sensors.update_produce_request_metrics(batches_by_node)

        if needs_transaction_state_reset:
            self._transaction_manager.reset_producer_id()
            return 0

        requests = self._create_produce_requests(batches_by_node)
        # If we have any nodes that are ready to send + have sendable data,
        # poll with 0 timeout so this can immediately loop and try sending more
        # data. Otherwise, the timeout will be the smaller value between next
        # batch expiry time, and the delay time for checking data availability.
        # Note that the nodes may have data that isn't yet sendable due to
        # lingering, backing off, etc. This specifically does not include nodes with
        # sendable data that aren't ready to send since they would cause busy
        # looping.
        poll_timeout_ms = min(next_ready_check_delay * 1000,
                              not_ready_timeout_ms,
                              self._accumulator.next_expiry_time_ms - now * 1000)
        if poll_timeout_ms < 0:
            poll_timeout_ms = 0

        if ready_nodes:
            log.debug("%s: Nodes with data ready to send: %s", str(self), ready_nodes) # trace
            log.debug("%s: Created %d produce requests: %s", str(self), len(requests), requests) # trace
            # if some partitions are already ready to be sent, the select time
            # would be 0; otherwise if some partition already has some data
            # accumulated but not ready yet, the select time will be the time
            # difference between now and its linger expiry time; otherwise the
            # select time will be the time difference between now and the
            # metadata expiry time
            poll_timeout_ms = 0

        for node_id, request in six.iteritems(requests):
            batches = batches_by_node[node_id]
            log.debug('%s: Sending Produce Request: %r', str(self), request)
            (self._client.send(node_id, request, wakeup=False)
                 .add_callback(
                     self._handle_produce_response, node_id, time.time(), batches)
                 .add_errback(
                     self._failed_produce, batches, node_id))
        return poll_timeout_ms

    def _maybe_send_transactional_request(self):
        if self._transaction_manager.is_completing() and self._accumulator.has_incomplete:
            if self._transaction_manager.is_aborting():
                self._accumulator.abort_undrained_batches(Errors.KafkaError("Failing batch since transaction was aborted"))
            # There may still be requests left which are being retried. Since we do not know whether they had
            # been successfully appended to the broker log, we must resend them until their final status is clear.
            # If they had been appended and we did not receive the error, then our sequence number would no longer
            # be correct which would lead to an OutOfSequenceNumberError.
            if not self._accumulator.flush_in_progress():
                self._accumulator.begin_flush()

        next_request_handler = self._transaction_manager.next_request_handler(self._accumulator.has_incomplete)
        if next_request_handler is None:
            return False

        log.debug("%s: Sending transactional request %s", str(self), next_request_handler.request)
        while not self._force_close:
            target_node = None
            try:
                if next_request_handler.needs_coordinator():
                    target_node = self._transaction_manager.coordinator(next_request_handler.coordinator_type)
                    if target_node is None:
                        self._transaction_manager.lookup_coordinator_for_request(next_request_handler)
                        break
                    elif not self._client.await_ready(target_node, timeout_ms=self.config['request_timeout_ms']):
                        self._transaction_manager.lookup_coordinator_for_request(next_request_handler)
                        target_node = None
                        break
                else:
                    target_node = self._client.least_loaded_node()
                    if target_node is not None and not self._client.await_ready(target_node, timeout_ms=self.config['request_timeout_ms']):
                        target_node = None

                if target_node is not None:
                    if next_request_handler.is_retry:
                        time.sleep(self.config['retry_backoff_ms'] / 1000)
                    txn_correlation_id = self._transaction_manager.next_in_flight_request_correlation_id()
                    future = self._client.send(target_node, next_request_handler.request)
                    future.add_both(next_request_handler.on_complete, txn_correlation_id)
                    return True

            except Exception as e:
                log.warn("%s: Got an exception when trying to find a node to send a transactional request to. Going to back off and retry: %s", str(self), e)
                if next_request_handler.needs_coordinator():
                    self._transaction_manager.lookup_coordinator_for_request(next_request_handler)
                    break

            time.sleep(self.config['retry_backoff_ms'] / 1000)
            self._metadata.request_update()

        if target_node is None:
            self._transaction_manager.retry(next_request_handler)

        return True

    def _maybe_abort_batches(self, exc):
        if self._accumulator.has_incomplete:
            log.error("%s: Aborting producer batches due to fatal error: %s", str(self), exc)
            self._accumulator.abort_batches(exc)

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
        # This is generally called from a separate thread
        # so this needs to be a thread-safe operation
        # we assume that checking set membership across threads
        # is ok where self._client._topics should never
        # remove topics for a producer instance, only add them.
        if topic not in self._client._topics:
            self._topics_to_add.add(topic)
            self.wakeup()

    def _maybe_wait_for_producer_id(self):
        while not self._transaction_manager.has_producer_id():
            try:
                node_id = self._client.least_loaded_node()
                if node_id is None or not self._client.await_ready(node_id):
                    log.debug("%s, Could not find an available broker to send InitProducerIdRequest to." +
                              " Will back off and try again.", str(self))
                    time.sleep(self._client.least_loaded_node_refresh_ms() / 1000)
                    continue
                version = self._client.api_version(InitProducerIdRequest, max_version=1)
                request = InitProducerIdRequest[version](
                    transactional_id=self.config['transactional_id'],
                    transaction_timeout_ms=self.config['transaction_timeout_ms'],
                )
                response = self._client.send_and_receive(node_id, request)
                error_type = Errors.for_code(response.error_code)
                if error_type is Errors.NoError:
                    self._transaction_manager.set_producer_id_and_epoch(ProducerIdAndEpoch(response.producer_id, response.producer_epoch))
                    break
                elif getattr(error_type, 'retriable', False):
                    log.debug("%s: Retriable error from InitProducerId response: %s", str(self), error_type.__name__)
                    if getattr(error_type, 'invalid_metadata', False):
                        self._metadata.request_update()
                else:
                    self._transaction_manager.transition_to_fatal_error(error_type())
                    break
            except Errors.KafkaConnectionError:
                log.debug("%s: Broker %s disconnected while awaiting InitProducerId response", str(self), node_id)
            except Errors.RequestTimedOutError:
                log.debug("%s: InitProducerId request to node %s timed out", str(self), node_id)
            log.debug("%s: Retry InitProducerIdRequest in %sms.", str(self), self.config['retry_backoff_ms'])
            time.sleep(self.config['retry_backoff_ms'] / 1000)

    def _failed_produce(self, batches, node_id, error):
        log.error("%s: Error sending produce request to node %d: %s", str(self), node_id, error) # trace
        for batch in batches:
            self._complete_batch(batch, error, -1)

    def _handle_produce_response(self, node_id, send_time, batches, response):
        """Handle a produce response."""
        # if we have a response, parse it
        log.debug('%s: Parsing produce response: %r', str(self), response)
        if response:
            batches_by_partition = dict([(batch.topic_partition, batch)
                                         for batch in batches])

            for topic, partitions in response.topics:
                for partition_info in partitions:
                    if response.API_VERSION < 2:
                        partition, error_code, offset = partition_info
                        ts = None
                    elif 2 <= response.API_VERSION <= 4:
                        partition, error_code, offset, ts = partition_info
                    elif 5 <= response.API_VERSION <= 7:
                        partition, error_code, offset, ts, _log_start_offset = partition_info
                    else:
                        # Currently unused / TODO: KIP-467
                        partition, error_code, offset, ts, _log_start_offset, _record_errors, _global_error = partition_info
                    tp = TopicPartition(topic, partition)
                    error = Errors.for_code(error_code)
                    batch = batches_by_partition[tp]
                    self._complete_batch(batch, error, offset, timestamp_ms=ts)

        else:
            # this is the acks = 0 case, just complete all requests
            for batch in batches:
                self._complete_batch(batch, None, -1)

    def _fail_batch(self, batch, exception, base_offset=None, timestamp_ms=None):
        exception = exception if type(exception) is not type else exception()
        if self._transaction_manager:
            if isinstance(exception, Errors.OutOfOrderSequenceNumberError) and \
                    not self._transaction_manager.is_transactional() and \
                    self._transaction_manager.has_producer_id(batch.producer_id):
                log.error("%s: The broker received an out of order sequence number for topic-partition %s"
                          " at offset %s. This indicates data loss on the broker, and should be investigated.",
                          str(self), batch.topic_partition, base_offset)

                # Reset the transaction state since we have hit an irrecoverable exception and cannot make any guarantees
                # about the previously committed message. Note that this will discard the producer id and sequence
                # numbers for all existing partitions.
                self._transaction_manager.reset_producer_id()
            elif isinstance(exception, (Errors.ClusterAuthorizationFailedError,
                                        Errors.TransactionalIdAuthorizationFailedError,
                                        Errors.ProducerFencedError,
                                        Errors.InvalidTxnStateError)):
                self._transaction_manager.transition_to_fatal_error(exception)
            elif self._transaction_manager.is_transactional():
                self._transaction_manager.transition_to_abortable_error(exception)

        if self._sensors:
            self._sensors.record_errors(batch.topic_partition.topic, batch.record_count)

        if batch.done(base_offset=base_offset, timestamp_ms=timestamp_ms, exception=exception):
            self._maybe_remove_from_inflight_batches(batch)
            self._accumulator.deallocate(batch)

    def _complete_batch(self, batch, error, base_offset, timestamp_ms=None):
        """Complete or retry the given batch of records.

        Arguments:
            batch (ProducerBatch): The record batch
            error (Exception): The error (or None if none)
            base_offset (int): The base offset assigned to the records if successful
            timestamp_ms (int, optional): The timestamp returned by the broker for this batch
        """
        # Standardize no-error to None
        if error is Errors.NoError:
            error = None

        if error is not None:
            if self._can_retry(batch, error):
                # retry
                log.warning("%s: Got error produce response on topic-partition %s,"
                            " retrying (%s attempts left). Error: %s",
                            str(self), batch.topic_partition,
                            self.config['retries'] - batch.attempts - 1,
                            error)

                # If idempotence is enabled only retry the request if the batch matches our current producer id and epoch
                if not self._transaction_manager or self._transaction_manager.producer_id_and_epoch.match(batch):
                    log.debug("%s: Retrying batch to topic-partition %s. Sequence number: %s",
                              str(self), batch.topic_partition,
                              self._transaction_manager.sequence_number(batch.topic_partition) if self._transaction_manager else None)
                    self._accumulator.reenqueue(batch)
                    self._maybe_remove_from_inflight_batches(batch)
                    if self._sensors:
                        self._sensors.record_retries(batch.topic_partition.topic, batch.record_count)
                else:
                    log.warning("%s: Attempted to retry sending a batch but the producer id/epoch changed from %s/%s to %s/%s. This batch will be dropped",
                                str(self), batch.producer_id, batch.producer_epoch,
                                self._transaction_manager.producer_id_and_epoch.producer_id,
                                self._transaction_manager.producer_id_and_epoch.epoch)
                    self._fail_batch(batch, error, base_offset=base_offset, timestamp_ms=timestamp_ms)
            else:
                if error is Errors.TopicAuthorizationFailedError:
                    error = error(batch.topic_partition.topic)

                # tell the user the result of their request
                self._fail_batch(batch, error, base_offset=base_offset, timestamp_ms=timestamp_ms)

            if error is Errors.UnknownTopicOrPartitionError:
                log.warning("%s: Received unknown topic or partition error in produce request on partition %s."
                            " The topic/partition may not exist or the user may not have Describe access to it",
                            str(self), batch.topic_partition)

            if getattr(error, 'invalid_metadata', False):
                self._metadata.request_update()

        else:
            if batch.done(base_offset=base_offset, timestamp_ms=timestamp_ms):
                self._maybe_remove_from_inflight_batches(batch)
                self._accumulator.deallocate(batch)

            if self._transaction_manager and self._transaction_manager.producer_id_and_epoch.match(batch):
                self._transaction_manager.increment_sequence_number(batch.topic_partition, batch.record_count)
                log.debug("%s: Incremented sequence number for topic-partition %s to %s", str(self), batch.topic_partition,
                          self._transaction_manager.sequence_number(batch.topic_partition))

        # Unmute the completed partition.
        if self.config['guarantee_message_order']:
            self._accumulator.muted.remove(batch.topic_partition)

    def _can_retry(self, batch, error):
        """
        We can retry a send if the error is transient and the number of
        attempts taken is fewer than the maximum allowed
        """
        return (not batch.has_reached_delivery_timeout(self._accumulator.delivery_timeout_ms) and
                batch.attempts < self.config['retries'] and
                batch.final_state is None and
                getattr(error, 'retriable', False))

    def _create_produce_requests(self, collated):
        """
        Transfer the record batches into a list of produce requests on a
        per-node basis.

        Arguments:
            collated: {node_id: [ProducerBatch]}

        Returns:
            dict: {node_id: ProduceRequest} (version depends on client api_versions)
        """
        requests = {}
        for node_id, batches in six.iteritems(collated):
            if batches:
                requests[node_id] = self._produce_request(
                    node_id, self.config['acks'],
                    self.config['request_timeout_ms'], batches)
        return requests

    def _produce_request(self, node_id, acks, timeout, batches):
        """Create a produce request from the given record batches.

        Returns:
            ProduceRequest (version depends on client api_versions)
        """
        produce_records_by_partition = collections.defaultdict(dict)
        for batch in batches:
            topic = batch.topic_partition.topic
            partition = batch.topic_partition.partition

            buf = batch.records.buffer()
            produce_records_by_partition[topic][partition] = buf

        version = self._client.api_version(ProduceRequest, max_version=7)
        topic_partition_data = [
            (topic, list(partition_info.items()))
            for topic, partition_info in six.iteritems(produce_records_by_partition)]
        transactional_id = self._transaction_manager.transactional_id if self._transaction_manager else None
        if version >= 3:
            return ProduceRequest[version](
                transactional_id=transactional_id,
                required_acks=acks,
                timeout=timeout,
                topics=topic_partition_data,
            )
        else:
            if transactional_id is not None:
                log.warning('%s: Broker does not support ProduceRequest v3+, required for transactional_id', str(self))
            return ProduceRequest[version](
                required_acks=acks,
                timeout=timeout,
                topics=topic_partition_data,
            )

    def wakeup(self):
        """Wake up the selector associated with this send thread."""
        self._client.wakeup()

    def bootstrap_connected(self):
        return self._client.bootstrap_connected()

    def __str__(self):
        return "<Sender client_id=%s transactional_id=%s>" % (self.config['client_id'], self.config['transactional_id'])


class SenderMetrics(object):

    def __init__(self, metrics, client, metadata):
        self.metrics = metrics
        self._client = client
        self._metadata = metadata

        sensor_name = 'batch-size'
        self.batch_size_sensor = self.metrics.sensor(sensor_name)
        self.add_metric('batch-size-avg', Avg(),
                        sensor_name=sensor_name,
                        description='The average number of bytes sent per partition per-request.')
        self.add_metric('batch-size-max', Max(),
                        sensor_name=sensor_name,
                        description='The max number of bytes sent per partition per-request.')

        sensor_name = 'compression-rate'
        self.compression_rate_sensor = self.metrics.sensor(sensor_name)
        self.add_metric('compression-rate-avg', Avg(),
                        sensor_name=sensor_name,
                        description='The average compression rate of record batches.')

        sensor_name = 'queue-time'
        self.queue_time_sensor = self.metrics.sensor(sensor_name)
        self.add_metric('record-queue-time-avg', Avg(),
                        sensor_name=sensor_name,
                        description='The average time in ms record batches spent in the record accumulator.')
        self.add_metric('record-queue-time-max', Max(),
                        sensor_name=sensor_name,
                        description='The maximum time in ms record batches spent in the record accumulator.')

        sensor_name = 'records-per-request'
        self.records_per_request_sensor = self.metrics.sensor(sensor_name)
        self.add_metric('record-send-rate', Rate(),
                        sensor_name=sensor_name,
                        description='The average number of records sent per second.')
        self.add_metric('records-per-request-avg', Avg(),
                        sensor_name=sensor_name,
                        description='The average number of records per request.')

        sensor_name = 'bytes'
        self.byte_rate_sensor = self.metrics.sensor(sensor_name)
        self.add_metric('byte-rate', Rate(),
                        sensor_name=sensor_name,
                        description='The average number of bytes sent per second.')

        sensor_name = 'record-retries'
        self.retry_sensor = self.metrics.sensor(sensor_name)
        self.add_metric('record-retry-rate', Rate(),
                        sensor_name=sensor_name,
                        description='The average per-second number of retried record sends')

        sensor_name = 'errors'
        self.error_sensor = self.metrics.sensor(sensor_name)
        self.add_metric('record-error-rate', Rate(),
                        sensor_name=sensor_name,
                        description='The average per-second number of record sends that resulted in errors')

        sensor_name = 'record-size-max'
        self.max_record_size_sensor = self.metrics.sensor(sensor_name)
        self.add_metric('record-size-max', Max(),
                        sensor_name=sensor_name,
                        description='The maximum record size across all batches')
        self.add_metric('record-size-avg', Avg(),
                        sensor_name=sensor_name,
                        description='The average maximum record size per batch')

        self.add_metric('requests-in-flight',
                        AnonMeasurable(lambda *_: self._client.in_flight_request_count()),
                        description='The current number of in-flight requests awaiting a response.')

        self.add_metric('metadata-age',
                        AnonMeasurable(lambda _, now: (now - self._metadata._last_successful_refresh_ms) / 1000),
                        description='The age in seconds of the current producer metadata being used.')

    def add_metric(self, metric_name, measurable, group_name='producer-metrics',
                   description=None, tags=None,
                   sensor_name=None):
        m = self.metrics
        metric = m.metric_name(metric_name, group_name, description, tags)
        if sensor_name:
            sensor = m.sensor(sensor_name)
            sensor.add(metric, measurable)
        else:
            m.add_metric(metric, measurable)

    def maybe_register_topic_metrics(self, topic):

        def sensor_name(name):
            return 'topic.{0}.{1}'.format(topic, name)

        # if one sensor of the metrics has been registered for the topic,
        # then all other sensors should have been registered; and vice versa
        if not self.metrics.get_sensor(sensor_name('records-per-batch')):

            self.add_metric('record-send-rate', Rate(),
                            sensor_name=sensor_name('records-per-batch'),
                            group_name='producer-topic-metrics.' + topic,
                            description= 'Records sent per second for topic ' + topic)

            self.add_metric('byte-rate', Rate(),
                            sensor_name=sensor_name('bytes'),
                            group_name='producer-topic-metrics.' + topic,
                            description='Bytes per second for topic ' + topic)

            self.add_metric('compression-rate', Avg(),
                            sensor_name=sensor_name('compression-rate'),
                            group_name='producer-topic-metrics.' + topic,
                            description='Average Compression ratio for topic ' + topic)

            self.add_metric('record-retry-rate', Rate(),
                            sensor_name=sensor_name('record-retries'),
                            group_name='producer-topic-metrics.' + topic,
                            description='Record retries per second for topic ' + topic)

            self.add_metric('record-error-rate', Rate(),
                            sensor_name=sensor_name('record-errors'),
                            group_name='producer-topic-metrics.' + topic,
                            description='Record errors per second for topic ' + topic)

    def update_produce_request_metrics(self, batches_map):
        for node_batch in batches_map.values():
            records = 0
            total_bytes = 0
            for batch in node_batch:
                # register all per-topic metrics at once
                topic = batch.topic_partition.topic
                self.maybe_register_topic_metrics(topic)

                # per-topic record send rate
                topic_records_count = self.metrics.get_sensor(
                    'topic.' + topic + '.records-per-batch')
                topic_records_count.record(batch.record_count)

                # per-topic bytes send rate
                topic_byte_rate = self.metrics.get_sensor(
                    'topic.' + topic + '.bytes')
                topic_byte_rate.record(batch.records.size_in_bytes())

                # per-topic compression rate
                topic_compression_rate = self.metrics.get_sensor(
                    'topic.' + topic + '.compression-rate')
                topic_compression_rate.record(batch.records.compression_rate())

                # global metrics
                self.batch_size_sensor.record(batch.records.size_in_bytes())
                if batch.drained:
                    self.queue_time_sensor.record(batch.drained - batch.created)
                self.compression_rate_sensor.record(batch.records.compression_rate())
                self.max_record_size_sensor.record(batch.max_record_size)
                records += batch.record_count
                total_bytes += batch.records.size_in_bytes()

            if node_batch:
                self.records_per_request_sensor.record(records)
                self.byte_rate_sensor.record(total_bytes)

    def record_retries(self, topic, count):
        self.retry_sensor.record(count)
        sensor = self.metrics.get_sensor('topic.' + topic + '.record-retries')
        if sensor:
            sensor.record(count)

    def record_errors(self, topic, count):
        self.error_sensor.record(count)
        sensor = self.metrics.get_sensor('topic.' + topic + '.record-errors')
        if sensor:
            sensor.record(count)
