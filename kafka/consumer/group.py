from __future__ import absolute_import

import copy
import logging
import time

from kafka.client_async import KafkaClient
from kafka.consumer.fetcher import Fetcher
from kafka.consumer.subscription_state import SubscriptionState
from kafka.coordinator.consumer import ConsumerCoordinator
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from kafka.protocol.offset import OffsetResetStrategy
from kafka.version import __version__

log = logging.getLogger(__name__)


class KafkaConsumer(object):
    """Consumer for Kafka 0.9"""
    DEFAULT_CONFIG = {
        'bootstrap_servers': 'localhost',
        'client_id': 'kafka-python-' + __version__,
        'group_id': 'kafka-python-default-group',
        'key_deserializer': None,
        'value_deserializer': None,
        'fetch_max_wait_ms': 500,
        'fetch_min_bytes': 1024,
        'max_partition_fetch_bytes': 1 * 1024 * 1024,
        'request_timeout_ms': 40 * 1000,
        'retry_backoff_ms': 100,
        'reconnect_backoff_ms': 50,
        'max_in_flight_requests_per_connection': 5,
        'auto_offset_reset': 'latest',
        'enable_auto_commit': True,
        'auto_commit_interval_ms': 5000,
        'check_crcs': True,
        'metadata_max_age_ms': 5 * 60 * 1000,
        'partition_assignment_strategy': (RoundRobinPartitionAssignor,),
        'heartbeat_interval_ms': 3000,
        'session_timeout_ms': 30000,
        'send_buffer_bytes': 128 * 1024,
        'receive_buffer_bytes': 32 * 1024,
        'connections_max_idle_ms': 9 * 60 * 1000, # not implemented yet
        #'metric_reporters': None,
        #'metrics_num_samples': 2,
        #'metrics_sample_window_ms': 30000,
    }

    def __init__(self, *topics, **configs):
        """A Kafka client that consumes records from a Kafka cluster.

        The consumer will transparently handle the failure of servers in the
        Kafka cluster, and transparently adapt as partitions of data it fetches
        migrate within the cluster. This client also interacts with the server
        to allow groups of consumers to load balance consumption using consumer
        groups.

        Requires Kafka Server >= 0.9.0.0

        Configuration settings can be passed to constructor as kwargs,
        otherwise defaults will be used:

        Keyword Arguments:
            bootstrap_servers: 'host[:port]' string (or list of 'host[:port]'
                strings) that the consumer should contact to bootstrap initial
                cluster metadata. This does not have to be the full node list.
                It just needs to have at least one broker that will respond to a
                Metadata API Request. Default port is 9092. If no servers are
                specified, will default to localhost:9092.
            client_id (str): a name for this client. This string is passed in
                each request to servers and can be used to identify specific
                server-side log entries that correspond to this client. Also
                submitted to GroupCoordinator for logging with respect to
                consumer group administration. Default: 'kafka-python-{version}'
            group_id (str): name of the consumer group to join for dynamic
                partition assignment (if enabled), and to use for fetching and
                committing offsets. Default: 'kafka-python-default-group'
            key_deserializer (callable): Any callable that takes a
                raw message key and returns a deserialized key.
            value_deserializer (callable, optional): Any callable that takes a
                raw message value and returns a deserialized value.
            fetch_min_bytes (int): Minimum amount of data the server should
                return for a fetch request, otherwise wait up to
                fetch_max_wait_ms for more data to accumulate. Default: 1024.
            fetch_max_wait_ms (int): The maximum amount of time in milliseconds
                the server will block before answering the fetch request if
                there isn't sufficient data to immediately satisfy the
                requirement given by fetch_min_bytes. Default: 500.
            max_partition_fetch_bytes (int): The maximum amount of data
                per-partition the server will return. The maximum total memory
                used for a request = #partitions * max_partition_fetch_bytes.
                This size must be at least as large as the maximum message size
                the server allows or else it is possible for the producer to
                send messages larger than the consumer can fetch. If that
                happens, the consumer can get stuck trying to fetch a large
                message on a certain partition. Default: 1048576.
            request_timeout_ms (int): Client request timeout in milliseconds.
                Default: 40000.
            retry_backoff_ms (int): Milliseconds to backoff when retrying on
                errors. Default: 100.
            reconnect_backoff_ms (int): The amount of time in milliseconds to
                wait before attempting to reconnect to a given host.
                Default: 50.
            max_in_flight_requests_per_connection (int): Requests are pipelined
                to kafka brokers up to this number of maximum requests per
                broker connection. Default: 5.
            auto_offset_reset (str): A policy for resetting offsets on
                OffsetOutOfRange errors: 'earliest' will move to the oldest
                available message, 'latest' will move to the most recent. Any
                ofther value will raise the exception. Default: 'latest'.
            enable_auto_commit (bool): If true the consumer's offset will be
                periodically committed in the background. Default: True.
            auto_commit_interval_ms (int): milliseconds between automatic
                offset commits, if enable_auto_commit is True. Default: 5000.
            default_offset_commit_callback (callable): called as
                callback(offsets, response) response will be either an Exception
                or a OffsetCommitResponse struct. This callback can be used to
                trigger custom actions when a commit request completes.
            check_crcs (bool): Automatically check the CRC32 of the records
                consumed. This ensures no on-the-wire or on-disk corruption to
                the messages occurred. This check adds some overhead, so it may
                be disabled in cases seeking extreme performance. Default: True
            metadata_max_age_ms (int): The period of time in milliseconds after
                which we force a refresh of metadata even if we haven't seen any
                partition leadership changes to proactively discover any new
                brokers or partitions. Default: 300000
            partition_assignment_strategy (list): List of objects to use to
                distribute partition ownership amongst consumer instances when
                group management is used. Default: [RoundRobinPartitionAssignor]
            heartbeat_interval_ms (int): The expected time in milliseconds
                between heartbeats to the consumer coordinator when using
                Kafka's group management feature. Heartbeats are used to ensure
                that the consumer's session stays active and to facilitate
                rebalancing when new consumers join or leave the group. The
                value must be set lower than session_timeout_ms, but typically
                should be set no higher than 1/3 of that value. It can be
                adjusted even lower to control the expected time for normal
                rebalances. Default: 3000
            session_timeout_ms (int): The timeout used to detect failures when
                using Kafka's group managementment facilities. Default: 30000
            send_buffer_bytes (int): The size of the TCP send buffer
                (SO_SNDBUF) to use when sending data. Default: 131072
            receive_buffer_bytes (int): The size of the TCP receive buffer
                (SO_RCVBUF) to use when reading data. Default: 32768

        Configuration parameters are described in more detail at
        https://kafka.apache.org/090/configuration.html#newconsumerconfigs
        """
        self.config = copy.copy(self.DEFAULT_CONFIG)
        for key in self.config:
            if key in configs:
                self.config[key] = configs[key]

        self._client = KafkaClient(**self.config)
        self._subscription = SubscriptionState(self.config['auto_offset_reset'])
        self._fetcher = Fetcher(
            self._client, self._subscription, **self.config)
        self._coordinator = ConsumerCoordinator(
            self._client, self._subscription,
            assignors=self.config['partition_assignment_strategy'],
            **self.config)
        self._closed = False

        #self.metrics = None
        if topics:
            self._subscription.subscribe(topics=topics)
            self._client.set_topics(topics)

    def assign(self, partitions):
        """Manually assign a list of TopicPartitions to this consumer.

        This interface does not allow for incremental assignment and will
        replace the previous assignment (if there was one).

        Manual topic assignment through this method does not use the consumer's
        group management functionality. As such, there will be no rebalance
        operation triggered when group membership or cluster and topic metadata
        change. Note that it is not possible to use both manual partition
        assignment with assign() and group assignment with subscribe().

        Arguments:
            partitions (list of TopicPartition): assignment for this instance.

        Raises:
            IllegalStateError: if consumer has already called subscribe()
        """
        self._subscription.assign_from_user(partitions)
        self._client.set_topics([tp.topic for tp in partitions])

    def assignment(self):
        """Get the TopicPartitions currently assigned to this consumer.

        If partitions were directly assigning using assign(), then this will
        simply return the same partitions that were assigned.
        If topics were subscribed to using subscribe(), then this will give the
        set of topic partitions currently assigned to the consumer (which may
        be none if the assignment hasn't happened yet, or the partitions are in
        the process of getting reassigned).

        Returns:
            set: {TopicPartition, ...}
        """
        return self._subscription.assigned_partitions()

    def close(self):
        """Close the consumer, waiting indefinitely for any needed cleanup."""
        if self._closed:
            return
        log.debug("Closing the KafkaConsumer.")
        self._closed = True
        self._coordinator.close()
        #self.metrics.close()
        self._client.close()
        try:
            self.config['key_deserializer'].close()
        except AttributeError:
            pass
        try:
            self.config['value_deserializer'].close()
        except AttributeError:
            pass
        log.debug("The KafkaConsumer has closed.")

    def commit_async(self, offsets=None, callback=None):
        """Commit offsets to kafka asynchronously, optionally firing callback

        This commits offsets only to Kafka. The offsets committed using this API
        will be used on the first fetch after every rebalance and also on
        startup. As such, if you need to store offsets in anything other than
        Kafka, this API should not be used.

        This is an asynchronous call and will not block. Any errors encountered
        are either passed to the callback (if provided) or discarded.

        Arguments:
            offsets (dict, optional): {TopicPartition: OffsetAndMetadata} dict
                to commit with the configured group_id. Defaults to current
                consumed offsets for all subscribed partitions.
            callback (callable, optional): called as callback(offsets, response)
                with response as either an Exception or a OffsetCommitResponse
                struct. This callback can be used to trigger custom actions when
                a commit request completes.

        Returns:
            kafka.future.Future
        """
        if offsets is None:
            offsets = self._subscription.all_consumed_offsets()
        log.debug("Committing offsets: %s", offsets)
        future = self._coordinator.commit_offsets_async(
            offsets, callback=callback)
        return future

    def commit(self, offsets=None):
        """Commit offsets to kafka, blocking until success or error

        This commits offsets only to Kafka. The offsets committed using this API
        will be used on the first fetch after every rebalance and also on
        startup. As such, if you need to store offsets in anything other than
        Kafka, this API should not be used.

        Blocks until either the commit succeeds or an unrecoverable error is
        encountered (in which case it is thrown to the caller).

        Currently only supports kafka-topic offset storage (not zookeeper)

        Arguments:
            offsets (dict, optional): {TopicPartition: OffsetAndMetadata} dict
                to commit with the configured group_id. Defaults to current
                consumed offsets for all subscribed partitions.
        """
        if offsets is None:
            offsets = self._subscription.all_consumed_offsets()
        self._coordinator.commit_offsets_sync(offsets)

    def committed(self, partition):
        """Get the last committed offset for the given partition

        This offset will be used as the position for the consumer
        in the event of a failure.

        This call may block to do a remote call if the partition in question
        isn't assigned to this consumer or if the consumer hasn't yet
        initialized its cache of committed offsets.

        Arguments:
            partition (TopicPartition): the partition to check

        Returns:
            The last committed offset, or None if there was no prior commit.
        """
        if self._subscription.is_assigned(partition):
            committed = self._subscription.assignment[partition].committed
            if committed is None:
                self._coordinator.refresh_committed_offsets_if_needed()
                committed = self._subscription.assignment[partition].committed
        else:
            commit_map = self._coordinator.fetch_committed_offsets([partition])
            if partition in commit_map:
                committed = commit_map[partition].offset
            else:
                committed = None
        return committed

    def topics(self):
        """Get all topic metadata topics the user is authorized to view.

        [Not Implemented Yet]

        Returns:
            {topic: [partition_info]}
        """
        raise NotImplementedError('TODO')

    def partitions_for_topic(self, topic):
        """Get metadata about the partitions for a given topic.

        Arguments:
            topic (str): topic to check

        Returns:
            set: partition ids
        """
        return self._client.cluster.partitions_for_topic(topic)

    def poll(self, timeout_ms=0):
        """
        Fetch data for the topics or partitions specified using one of the
        subscribe/assign APIs. It is an error to not have subscribed to any
        topics or partitions before polling for data.

        On each poll, consumer will try to use the last consumed offset as the
        starting offset and fetch sequentially. The last consumed offset can be
        manually set through seek(partition, offset) or automatically set as
        the last committed offset for the subscribed list of partitions.

        Arguments:
            timeout_ms (int, optional): milliseconds to spend waiting in poll if
                data is not available. If 0, returns immediately with any
                records that are available now. Must not be negative. Default: 0

        Returns:
            dict: topic to deque of records since the last fetch for the
                subscribed list of topics and partitions
        """
        assert timeout_ms >= 0, 'Timeout must not be negative'

        # poll for new data until the timeout expires
        start = time.time()
        remaining = timeout_ms
        while True:
            records = self._poll_once(remaining)
            if records:
                # before returning the fetched records, we can send off the
                # next round of fetches and avoid block waiting for their
                # responses to enable pipelining while the user is handling the
                # fetched records.
                self._fetcher.init_fetches()
                return records

            elapsed_ms = (time.time() - start) * 1000
            remaining = timeout_ms - elapsed_ms

            if remaining <= 0:
                break

    def _poll_once(self, timeout_ms):
        """
        Do one round of polling. In addition to checking for new data, this does
        any needed heart-beating, auto-commits, and offset updates.

        Arguments:
            timeout_ms (int): The maximum time in milliseconds to block

        Returns:
            dict: map of topic to deque of records (may be empty)
        """
        # TODO: Sub-requests should take into account the poll timeout (KAFKA-1894)
        self._coordinator.ensure_coordinator_known()

        # ensure we have partitions assigned if we expect to
        if self._subscription.partitions_auto_assigned():
            self._coordinator.ensure_active_group()

        # fetch positions if we have partitions we're subscribed to that we
        # don't know the offset for
        if not self._subscription.has_all_fetch_positions():
            self._update_fetch_positions(self._subscription.missing_fetch_positions())

        # init any new fetches (won't resend pending fetches)
        records = self._fetcher.fetched_records()

        # if data is available already, e.g. from a previous network client
        # poll() call to commit, then just return it immediately
        if records:
            return records

        self._fetcher.init_fetches()
        self._client.poll(timeout_ms / 1000.0)
        return self._fetcher.fetched_records()

    def position(self, partition):
        """Get the offset of the next record that will be fetched

        Arguments:
            partition (TopicPartition): partition to check
        """
        assert self._subscription.is_assigned(partition)

        offset = self._subscription.assignment[partition].consumed
        if offset is None:
            self._update_fetch_positions(partition)
            offset = self._subscription.assignment[partition].consumed
        return offset

    def pause(self, *partitions):
        """Suspend fetching from the requested partitions.

        Future calls to poll() will not return any records from these partitions
        until they have been resumed using resume(). Note that this method does
        not affect partition subscription. In particular, it does not cause a
        group rebalance when automatic assignment is used.

        Arguments:
            *partitions (TopicPartition): partitions to pause
        """
        for partition in partitions:
            log.debug("Pausing partition %s", partition)
            self._subscription.pause(partition)

    def resume(self, *partitions):
        """Resume fetching from the specified (paused) partitions.

        Arguments:
            *partitions (TopicPartition): partitions to resume
        """
        for partition in partitions:
            log.debug("Resuming partition %s", partition)
            self._subscription.resume(partition)

    def seek(self, partition, offset):
        """Manually specify the fetch offset for a TopicPartition.

        Overrides the fetch offsets that the consumer will use on the next
        poll(). If this API is invoked for the same partition more than once,
        the latest offset will be used on the next poll(). Note that you may
        lose data if this API is arbitrarily used in the middle of consumption,
        to reset the fetch offsets.

        Arguments:
            partition (TopicPartition): partition for seek operation
            offset (int): message offset in partition
        """
        assert offset >= 0
        log.debug("Seeking to offset %s for partition %s", offset, partition)
        self._subscription.assignment[partition].seek(offset)

    def seek_to_beginning(self, *partitions):
        """Seek to the oldest available offset for partitions.

        Arguments:
            *partitions: optionally provide specific TopicPartitions, otherwise
                default to all assigned partitions
        """
        if not partitions:
            partitions = self._subscription.assigned_partitions()
        for tp in partitions:
            log.debug("Seeking to beginning of partition %s", tp)
            self._subscription.need_offset_reset(tp, OffsetResetStrategy.EARLIEST)

    def seek_to_end(self, *partitions):
        """Seek to the most recent available offset for partitions.

        Arguments:
            *partitions: optionally provide specific TopicPartitions, otherwise
                default to all assigned partitions
        """
        if not partitions:
            partitions = self._subscription.assigned_partitions()
        for tp in partitions:
            log.debug("Seeking to end of partition %s", tp)
            self._subscription.need_offset_reset(tp, OffsetResetStrategy.LATEST)

    def subscribe(self, topics=(), pattern=None, listener=None):
        """Subscribe to a list of topics, or a topic regex pattern

        Partitions will be dynamically assigned via a group coordinator.
        Topic subscriptions are not incremental: this list will replace the
        current assignment (if there is one).

        This method is incompatible with assign()

        Arguments:
            topics (list): List of topics for subscription.
            pattern (str): Pattern to match available topics. You must provide
                either topics or pattern, but not both.
            listener (ConsumerRebalanceListener): Optionally include listener
                callback, which will be called before and after each rebalance
                operation.

                As part of group management, the consumer will keep track of the
                list of consumers that belong to a particular group and will
                trigger a rebalance operation if one of the following events
                trigger:

                * Number of partitions change for any of the subscribed topics
                * Topic is created or deleted
                * An existing member of the consumer group dies
                * A new member is added to the consumer group

                When any of these events are triggered, the provided listener
                will be invoked first to indicate that the consumer's assignment
                has been revoked, and then again when the new assignment has
                been received. Note that this listener will immediately override
                any listener set in a previous call to subscribe. It is
                guaranteed, however, that the partitions revoked/assigned
                through this interface are from topics subscribed in this call.
        """
        if not topics:
            self.unsubscribe()
        else:
            self._subscription.subscribe(topics=topics,
                                         pattern=pattern,
                                         listener=listener)
            # regex will need all topic metadata
            if pattern is not None:
                self._client.cluster.need_metadata_for_all = True
                log.debug("Subscribed to topic pattern: %s", topics)
            else:
                self._client.set_topics(self._subscription.group_subscription())
                log.debug("Subscribed to topic(s): %s", topics)

    def subscription(self):
        """Get the current topic subscription.

        Returns:
            set: {topic, ...}
        """
        return self._subscription.subscription

    def unsubscribe(self):
        """Unsubscribe from all topics and clear all assigned partitions."""
        self._subscription.unsubscribe()
        self._coordinator.close()
        self._client.cluster.need_metadata_for_all_topics = False
        log.debug("Unsubscribed all topics or patterns and assigned partitions")

    def _update_fetch_positions(self, partitions):
        """
        Set the fetch position to the committed position (if there is one)
        or reset it using the offset reset policy the user has configured.

        Arguments:
            partitions (List[TopicPartition]): The partitions that need
                updating fetch positions

        Raises:
            NoOffsetForPartitionError: If no offset is stored for a given
                partition and no offset reset policy is defined
        """
        # refresh commits for all assigned partitions
        self._coordinator.refresh_committed_offsets_if_needed()

        # then do any offset lookups in case some positions are not known
        self._fetcher.update_fetch_positions(partitions)

    def __iter__(self):
        while True:
            self._coordinator.ensure_coordinator_known()

            # ensure we have partitions assigned if we expect to
            if self._subscription.partitions_auto_assigned():
                self._coordinator.ensure_active_group()

            # fetch positions if we have partitions we're subscribed to that we
            # don't know the offset for
            if not self._subscription.has_all_fetch_positions():
                self._update_fetch_positions(self._subscription.missing_fetch_positions())

            # init any new fetches (won't resend pending fetches)
            self._fetcher.init_fetches()
            self._client.poll(self.config['request_timeout_ms'] / 1000.0)
            timeout = time.time() + self.config['heartbeat_interval_ms'] / 1000.0
            for msg in self._fetcher:
                yield msg
                if time.time() > timeout:
                    break
