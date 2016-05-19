from __future__ import absolute_import

import copy
import logging
import time

import six

from kafka.client_async import KafkaClient
from kafka.consumer.fetcher import Fetcher
from kafka.consumer.subscription_state import SubscriptionState
from kafka.coordinator.consumer import ConsumerCoordinator
from kafka.coordinator.assignors.range import RangePartitionAssignor
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from kafka.metrics import DictReporter, MetricConfig, Metrics
from kafka.protocol.offset import OffsetResetStrategy
from kafka.structs import TopicPartition
from kafka.version import __version__

log = logging.getLogger(__name__)


class KafkaConsumer(six.Iterator):
    """Consume records from a Kafka cluster.

    The consumer will transparently handle the failure of servers in the Kafka
    cluster, and adapt as topic-partitions are created or migrate between
    brokers. It also interacts with the assigned kafka Group Coordinator node
    to allow multiple consumers to load balance consumption of topics (requires
    kafka >= 0.9.0.0).

    Arguments:
        *topics (str): optional list of topics to subscribe to. If not set,
            call subscribe() or assign() before consuming records.

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
        group_id (str or None): name of the consumer group to join for dynamic
            partition assignment (if enabled), and to use for fetching and
            committing offsets. If None, auto-partition assignment (via
            group coordinator) and offset commits are disabled.
            Default: 'kafka-python-default-group'
        key_deserializer (callable): Any callable that takes a
            raw message key and returns a deserialized key.
        value_deserializer (callable): Any callable that takes a
            raw message value and returns a deserialized value.
        fetch_min_bytes (int): Minimum amount of data the server should
            return for a fetch request, otherwise wait up to
            fetch_max_wait_ms for more data to accumulate. Default: 1.
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
            group management is used.
            Default: [RangePartitionAssignor, RoundRobinPartitionAssignor]
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
            (SO_SNDBUF) to use when sending data. Default: None (relies on
            system defaults). The java client defaults to 131072.
        receive_buffer_bytes (int): The size of the TCP receive buffer
            (SO_RCVBUF) to use when reading data. Default: None (relies on
            system defaults). The java client defaults to 32768.
        consumer_timeout_ms (int): number of millisecond to throw a timeout
            exception to the consumer if no message is available for
            consumption. Default: -1 (dont throw exception)
        security_protocol (str): Protocol used to communicate with brokers.
            Valid values are: PLAINTEXT, SSL. Default: PLAINTEXT.
        ssl_context (ssl.SSLContext): pre-configured SSLContext for wrapping
            socket connections. If provided, all other ssl_* configurations
            will be ignored. Default: None.
        ssl_check_hostname (bool): flag to configure whether ssl handshake
            should verify that the certificate matches the brokers hostname.
            default: true.
        ssl_cafile (str): optional filename of ca file to use in certificate
            veriication. default: none.
        ssl_certfile (str): optional filename of file in pem format containing
            the client certificate, as well as any ca certificates needed to
            establish the certificate's authenticity. default: none.
        ssl_keyfile (str): optional filename containing the client private key.
            default: none.
        ssl_crlfile (str): optional filename containing the CRL to check for
            certificate expiration. By default, no CRL check is done. When
            providing a file, only the leaf certificate will be checked against
            this CRL. The CRL can only be checked with Python 3.4+ or 2.7.9+.
            default: none.
        api_version (str): specify which kafka API version to use.
            0.9 enables full group coordination features; 0.8.2 enables
            kafka-storage offset commits; 0.8.1 enables zookeeper-storage
            offset commits; 0.8.0 is what is left. If set to 'auto', will
            attempt to infer the broker version by probing various APIs.
            Default: auto
        metric_reporters (list): A list of classes to use as metrics reporters.
            Implementing the AbstractMetricsReporter interface allows plugging
            in classes that will be notified of new metric creation. Default: []
        metrics_num_samples (int): The number of samples maintained to compute
            metrics. Default: 2
        metrics_sample_window_ms (int): The number of samples maintained to
            compute metrics. Default: 30000

    Note:
        Configuration parameters are described in more detail at
        https://kafka.apache.org/090/configuration.html#newconsumerconfigs
    """
    DEFAULT_CONFIG = {
        'bootstrap_servers': 'localhost',
        'client_id': 'kafka-python-' + __version__,
        'group_id': 'kafka-python-default-group',
        'key_deserializer': None,
        'value_deserializer': None,
        'fetch_max_wait_ms': 500,
        'fetch_min_bytes': 1,
        'max_partition_fetch_bytes': 1 * 1024 * 1024,
        'request_timeout_ms': 40 * 1000,
        'retry_backoff_ms': 100,
        'reconnect_backoff_ms': 50,
        'max_in_flight_requests_per_connection': 5,
        'auto_offset_reset': 'latest',
        'enable_auto_commit': True,
        'auto_commit_interval_ms': 5000,
        'default_offset_commit_callback': lambda offsets, response: True,
        'check_crcs': True,
        'metadata_max_age_ms': 5 * 60 * 1000,
        'partition_assignment_strategy': (RangePartitionAssignor, RoundRobinPartitionAssignor),
        'heartbeat_interval_ms': 3000,
        'session_timeout_ms': 30000,
        'send_buffer_bytes': None,
        'receive_buffer_bytes': None,
        'consumer_timeout_ms': -1,
        'security_protocol': 'PLAINTEXT',
        'ssl_context': None,
        'ssl_check_hostname': True,
        'ssl_cafile': None,
        'ssl_certfile': None,
        'ssl_keyfile': None,
        'ssl_crlfile': None,
        'api_version': 'auto',
        'connections_max_idle_ms': 9 * 60 * 1000, # not implemented yet
        'metric_reporters': [],
        'metrics_num_samples': 2,
        'metrics_sample_window_ms': 30000,
    }

    def __init__(self, *topics, **configs):
        self.config = copy.copy(self.DEFAULT_CONFIG)
        for key in self.config:
            if key in configs:
                self.config[key] = configs.pop(key)

        # Only check for extra config keys in top-level class
        assert not configs, 'Unrecognized configs: %s' % configs

        deprecated = {'smallest': 'earliest', 'largest': 'latest' }
        if self.config['auto_offset_reset'] in deprecated:
            new_config = deprecated[self.config['auto_offset_reset']]
            log.warning('use auto_offset_reset=%s (%s is deprecated)',
                        new_config, self.config['auto_offset_reset'])
            self.config['auto_offset_reset'] = new_config

        metrics_tags = {'client-id': self.config['client_id']}
        metric_config = MetricConfig(samples=self.config['metrics_num_samples'],
                                     time_window_ms=self.config['metrics_sample_window_ms'],
                                     tags=metrics_tags)
        reporters = [reporter() for reporter in self.config['metric_reporters']]
        reporters.append(DictReporter('kafka.consumer'))
        self._metrics = Metrics(metric_config, reporters)
        metric_group_prefix = 'consumer'
        # TODO _metrics likely needs to be passed to KafkaClient, etc.

        self._client = KafkaClient(**self.config)

        # Check Broker Version if not set explicitly
        if self.config['api_version'] == 'auto':
            self.config['api_version'] = self._client.check_version()
        assert self.config['api_version'] in ('0.10', '0.9', '0.8.2', '0.8.1', '0.8.0'), 'Unrecognized api version'

        # Convert api_version config to tuple for easy comparisons
        self.config['api_version'] = tuple(
            map(int, self.config['api_version'].split('.')))

        self._subscription = SubscriptionState(self.config['auto_offset_reset'])
        self._fetcher = Fetcher(
            self._client, self._subscription, self._metrics, metric_group_prefix, **self.config)
        self._coordinator = ConsumerCoordinator(
            self._client, self._subscription, self._metrics, metric_group_prefix,
            assignors=self.config['partition_assignment_strategy'],
            **self.config)
        self._closed = False
        self._iterator = None
        self._consumer_timeout = float('inf')

        if topics:
            self._subscription.subscribe(topics=topics)
            self._client.set_topics(topics)

    def assign(self, partitions):
        """Manually assign a list of TopicPartitions to this consumer.

        Arguments:
            partitions (list of TopicPartition): assignment for this instance.

        Raises:
            IllegalStateError: if consumer has already called subscribe()

        Warning:
            It is not possible to use both manual partition assignment with
            assign() and group assignment with subscribe().

        Note:
            This interface does not support incremental assignment and will
            replace the previous assignment (if there was one).

        Note:
            Manual topic assignment through this method does not use the
            consumer's group management functionality. As such, there will be
            no rebalance operation triggered when group membership or cluster
            and topic metadata change.
        """
        self._subscription.assign_from_user(partitions)
        self._client.set_topics([tp.topic for tp in partitions])

    def assignment(self):
        """Get the TopicPartitions currently assigned to this consumer.

        If partitions were directly assigned using assign(), then this will
        simply return the same partitions that were previously assigned.
        If topics were subscribed using subscribe(), then this will give the
        set of topic partitions currently assigned to the consumer (which may
        be none if the assignment hasn't happened yet, or if the partitions are
        in the process of being reassigned).

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
        self._metrics.close()
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
        Kafka, this API should not be used. To avoid re-processing the last
        message read if a consumer is restarted, the committed offset should be
        the next message your application should consume, i.e.: last_offset + 1.

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
        assert self.config['api_version'] >= (0, 8, 1), 'Requires >= Kafka 0.8.1'
        assert self.config['group_id'] is not None, 'Requires group_id'
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
        Kafka, this API should not be used. To avoid re-processing the last
        message read if a consumer is restarted, the committed offset should be
        the next message your application should consume, i.e.: last_offset + 1.

        Blocks until either the commit succeeds or an unrecoverable error is
        encountered (in which case it is thrown to the caller).

        Currently only supports kafka-topic offset storage (not zookeeper)

        Arguments:
            offsets (dict, optional): {TopicPartition: OffsetAndMetadata} dict
                to commit with the configured group_id. Defaults to current
                consumed offsets for all subscribed partitions.
        """
        assert self.config['api_version'] >= (0, 8, 1), 'Requires >= Kafka 0.8.1'
        assert self.config['group_id'] is not None, 'Requires group_id'
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
        assert self.config['api_version'] >= (0, 8, 1), 'Requires >= Kafka 0.8.1'
        assert self.config['group_id'] is not None, 'Requires group_id'
        if not isinstance(partition, TopicPartition):
            raise TypeError('partition must be a TopicPartition namedtuple')
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
        """Get all topics the user is authorized to view.

        Returns:
            set: topics
        """
        cluster = self._client.cluster
        if self._client._metadata_refresh_in_progress and self._client._topics:
            future = cluster.request_update()
            self._client.poll(future=future)
        stash = cluster.need_all_topic_metadata
        cluster.need_all_topic_metadata = True
        future = cluster.request_update()
        self._client.poll(future=future)
        cluster.need_all_topic_metadata = stash
        return cluster.topics()

    def partitions_for_topic(self, topic):
        """Get metadata about the partitions for a given topic.

        Arguments:
            topic (str): topic to check

        Returns:
            set: partition ids
        """
        return self._client.cluster.partitions_for_topic(topic)

    def poll(self, timeout_ms=0):
        """Fetch data from assigned topics / partitions.

        Records are fetched and returned in batches by topic-partition.
        On each poll, consumer will try to use the last consumed offset as the
        starting offset and fetch sequentially. The last consumed offset can be
        manually set through seek(partition, offset) or automatically set as
        the last committed offset for the subscribed list of partitions.

        Incompatible with iterator interface -- use one or the other, not both.

        Arguments:
            timeout_ms (int, optional): milliseconds spent waiting in poll if
                data is not available in the buffer. If 0, returns immediately
                with any records that are available currently in the buffer,
                else returns empty. Must not be negative. Default: 0

        Returns:
            dict: topic to list of records since the last fetch for the
                subscribed list of topics and partitions
        """
        assert timeout_ms >= 0, 'Timeout must not be negative'
        assert self._iterator is None, 'Incompatible with iterator interface'

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
                return {}

    def _poll_once(self, timeout_ms):
        """
        Do one round of polling. In addition to checking for new data, this does
        any needed heart-beating, auto-commits, and offset updates.

        Arguments:
            timeout_ms (int): The maximum time in milliseconds to block

        Returns:
            dict: map of topic to list of records (may be empty)
        """
        if self._use_consumer_group():
            self._coordinator.ensure_coordinator_known()
            self._coordinator.ensure_active_group()

        # 0.8.2 brokers support kafka-backed offset storage via group coordinator
        elif self.config['group_id'] is not None and self.config['api_version'] >= (0, 8, 2):
            self._coordinator.ensure_coordinator_known()


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
        self._client.poll(timeout_ms=timeout_ms, sleep=True)
        return self._fetcher.fetched_records()

    def position(self, partition):
        """Get the offset of the next record that will be fetched

        Arguments:
            partition (TopicPartition): partition to check

        Returns:
            int: offset
        """
        if not isinstance(partition, TopicPartition):
            raise TypeError('partition must be a TopicPartition namedtuple')
        assert self._subscription.is_assigned(partition), 'Partition is not assigned'
        offset = self._subscription.assignment[partition].position
        if offset is None:
            self._update_fetch_positions([partition])
            offset = self._subscription.assignment[partition].position
        return offset

    def highwater(self, partition):
        """Last known highwater offset for a partition

        A highwater offset is the offset that will be assigned to the next
        message that is produced. It may be useful for calculating lag, by
        comparing with the reported position. Note that both position and
        highwater refer to the *next* offset -- i.e., highwater offset is
        one greater than the newest available message.

        Highwater offsets are returned in FetchResponse messages, so will
        not be available if not FetchRequests have been sent for this partition
        yet.

        Arguments:
            partition (TopicPartition): partition to check

        Returns:
            int or None: offset if available
        """
        if not isinstance(partition, TopicPartition):
            raise TypeError('partition must be a TopicPartition namedtuple')
        assert self._subscription.is_assigned(partition), 'Partition is not assigned'
        return self._subscription.assignment[partition].highwater

    def pause(self, *partitions):
        """Suspend fetching from the requested partitions.

        Future calls to poll() will not return any records from these partitions
        until they have been resumed using resume(). Note that this method does
        not affect partition subscription. In particular, it does not cause a
        group rebalance when automatic assignment is used.

        Arguments:
            *partitions (TopicPartition): partitions to pause
        """
        if not all([isinstance(p, TopicPartition) for p in partitions]):
            raise TypeError('partitions must be TopicPartition namedtuples')
        for partition in partitions:
            log.debug("Pausing partition %s", partition)
            self._subscription.pause(partition)

    def paused(self):
        """Get the partitions that were previously paused by a call to pause().

        Returns:
            set: {partition (TopicPartition), ...}
        """
        return self._subscription.paused_partitions()

    def resume(self, *partitions):
        """Resume fetching from the specified (paused) partitions.

        Arguments:
            *partitions (TopicPartition): partitions to resume
        """
        if not all([isinstance(p, TopicPartition) for p in partitions]):
            raise TypeError('partitions must be TopicPartition namedtuples')
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

        Raises:
            AssertionError: if offset is not an int >= 0; or if partition is not
                currently assigned.
        """
        if not isinstance(partition, TopicPartition):
            raise TypeError('partition must be a TopicPartition namedtuple')
        assert isinstance(offset, int) and offset >= 0, 'Offset must be >= 0'
        assert partition in self._subscription.assigned_partitions(), 'Unassigned partition'
        log.debug("Seeking to offset %s for partition %s", offset, partition)
        self._subscription.assignment[partition].seek(offset)

    def seek_to_beginning(self, *partitions):
        """Seek to the oldest available offset for partitions.

        Arguments:
            *partitions: optionally provide specific TopicPartitions, otherwise
                default to all assigned partitions

        Raises:
            AssertionError: if any partition is not currently assigned, or if
                no partitions are assigned
        """
        if not all([isinstance(p, TopicPartition) for p in partitions]):
            raise TypeError('partitions must be TopicPartition namedtuples')
        if not partitions:
            partitions = self._subscription.assigned_partitions()
            assert partitions, 'No partitions are currently assigned'
        else:
            for p in partitions:
                assert p in self._subscription.assigned_partitions(), 'Unassigned partition'

        for tp in partitions:
            log.debug("Seeking to beginning of partition %s", tp)
            self._subscription.need_offset_reset(tp, OffsetResetStrategy.EARLIEST)

    def seek_to_end(self, *partitions):
        """Seek to the most recent available offset for partitions.

        Arguments:
            *partitions: optionally provide specific TopicPartitions, otherwise
                default to all assigned partitions

        Raises:
            AssertionError: if any partition is not currently assigned, or if
                no partitions are assigned
        """
        if not all([isinstance(p, TopicPartition) for p in partitions]):
            raise TypeError('partitions must be TopicPartition namedtuples')
        if not partitions:
            partitions = self._subscription.assigned_partitions()
            assert partitions, 'No partitions are currently assigned'
        else:
            for p in partitions:
                assert p in self._subscription.assigned_partitions(), 'Unassigned partition'

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

        Raises:
            IllegalStateError: if called after previously calling assign()
            AssertionError: if neither topics or pattern is provided
            TypeError: if listener is not a ConsumerRebalanceListener
        """
        # SubscriptionState handles error checking
        self._subscription.subscribe(topics=topics,
                                     pattern=pattern,
                                     listener=listener)

        # regex will need all topic metadata
        if pattern is not None:
            self._client.cluster.need_all_topic_metadata = True
            self._client.set_topics([])
            log.debug("Subscribed to topic pattern: %s", pattern)
        else:
            self._client.cluster.need_all_topic_metadata = False
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
        self._client.cluster.need_all_topic_metadata = False
        self._client.set_topics([])
        log.debug("Unsubscribed all topics or patterns and assigned partitions")

    def _use_consumer_group(self):
        """Return True iff this consumer can/should join a broker-coordinated group."""
        if self.config['api_version'] < (0, 9):
            return False
        elif self.config['group_id'] is None:
            return False
        elif not self._subscription.partitions_auto_assigned():
            return False
        return True

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
        if (self.config['api_version'] >= (0, 8, 1)
            and self.config['group_id'] is not None):

            # refresh commits for all assigned partitions
            self._coordinator.refresh_committed_offsets_if_needed()

        # then do any offset lookups in case some positions are not known
        self._fetcher.update_fetch_positions(partitions)

    def _message_generator(self):
        assert self.assignment() or self.subscription() is not None, 'No topic subscription or manual partition assignment'
        while time.time() < self._consumer_timeout:

            if self._use_consumer_group():
                self._coordinator.ensure_coordinator_known()
                self._coordinator.ensure_active_group()

            # 0.8.2 brokers support kafka-backed offset storage via group coordinator
            elif self.config['group_id'] is not None and self.config['api_version'] >= (0, 8, 2):
                self._coordinator.ensure_coordinator_known()

            # fetch offsets for any subscribed partitions that we arent tracking yet
            if not self._subscription.has_all_fetch_positions():
                partitions = self._subscription.missing_fetch_positions()
                self._update_fetch_positions(partitions)

            poll_ms = 1000 * (self._consumer_timeout - time.time())
            if not self._fetcher.in_flight_fetches():
                poll_ms = 0
            self._client.poll(timeout_ms=poll_ms, sleep=True)

            # We need to make sure we at least keep up with scheduled tasks,
            # like heartbeats, auto-commits, and metadata refreshes
            timeout_at = self._next_timeout()

            # Because the consumer client poll does not sleep unless blocking on
            # network IO, we need to explicitly sleep when we know we are idle
            # because we haven't been assigned any partitions to fetch / consume
            if self._use_consumer_group() and not self.assignment():
                sleep_time = max(timeout_at - time.time(), 0)
                if sleep_time > 0 and not self._client.in_flight_request_count():
                    log.debug('No partitions assigned; sleeping for %s', sleep_time)
                    time.sleep(sleep_time)
                    continue

            # Short-circuit the fetch iterator if we are already timed out
            # to avoid any unintentional interaction with fetcher setup
            if time.time() > timeout_at:
                continue

            for msg in self._fetcher:
                yield msg
                if time.time() > timeout_at:
                    log.debug("internal iterator timeout - breaking for poll")
                    break

            # an else block on a for loop only executes if there was no break
            # so this should only be called on a StopIteration from the fetcher
            # and we assume that it is safe to init_fetches when fetcher is done
            # i.e., there are no more records stored internally
            else:
                self._fetcher.init_fetches()

    def _next_timeout(self):
        timeout = min(self._consumer_timeout,
                      self._client._delayed_tasks.next_at() + time.time(),
                      self._client.cluster.ttl() / 1000.0 + time.time())

        # Although the delayed_tasks timeout above should cover processing
        # HeartbeatRequests, it is still possible that HeartbeatResponses
        # are left unprocessed during a long _fetcher iteration without
        # an intermediate poll(). And because tasks are responsible for
        # rescheduling themselves, an unprocessed response will prevent
        # the next heartbeat from being sent. This check should help
        # avoid that.
        if self._use_consumer_group():
            heartbeat = time.time() + self._coordinator.heartbeat.ttl()
            timeout = min(timeout, heartbeat)
        return timeout

    def __iter__(self):  # pylint: disable=non-iterator-returned
        return self

    def __next__(self):
        if not self._iterator:
            self._iterator = self._message_generator()

        self._set_consumer_timeout()
        try:
            return next(self._iterator)
        except StopIteration:
            self._iterator = None
            raise

    def _set_consumer_timeout(self):
        # consumer_timeout_ms can be used to stop iteration early
        if self.config['consumer_timeout_ms'] >= 0:
            self._consumer_timeout = time.time() + (
                self.config['consumer_timeout_ms'] / 1000.0)

    # old KafkaConsumer methods are deprecated
    def configure(self, **configs):
        raise NotImplementedError(
            'deprecated -- initialize a new consumer')

    def set_topic_partitions(self, *topics):
        raise NotImplementedError(
            'deprecated -- use subscribe() or assign()')

    def fetch_messages(self):
        raise NotImplementedError(
            'deprecated -- use poll() or iterator interface')

    def get_partition_offsets(self, topic, partition,
                              request_time_ms, max_num_offsets):
        raise NotImplementedError(
            'deprecated -- send an OffsetRequest with KafkaClient')

    def offsets(self, group=None):
        raise NotImplementedError('deprecated -- use committed(partition)')

    def task_done(self, message):
        raise NotImplementedError(
            'deprecated -- commit offsets manually if needed')
