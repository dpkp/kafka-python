from __future__ import absolute_import

from collections import namedtuple
from copy import deepcopy
import logging
import random
import sys
import time

import six

from kafka.client import KafkaClient
from kafka.common import (
    OffsetFetchRequest, OffsetCommitRequest, OffsetRequest, FetchRequest,
    check_error, NotLeaderForPartitionError, UnknownTopicOrPartitionError,
    OffsetOutOfRangeError, RequestTimedOutError, KafkaMessage, ConsumerTimeout,
    FailedPayloadsError, KafkaUnavailableError, KafkaConfigurationError
)
from kafka.util import kafka_bytestring

logger = logging.getLogger(__name__)

OffsetsStruct = namedtuple("OffsetsStruct", ["fetch", "highwater", "commit", "task_done"])

DEFAULT_CONSUMER_CONFIG = {
    'client_id': __name__,
    'group_id': None,
    'bootstrap_servers': [],
    'socket_timeout_ms': 30 * 1000,
    'fetch_message_max_bytes': 1024 * 1024,
    'auto_offset_reset': 'largest',
    'fetch_min_bytes': 1,
    'fetch_wait_max_ms': 100,
    'refresh_leader_backoff_ms': 200,
    'deserializer_class': lambda msg: msg,
    'auto_commit_enable': False,
    'auto_commit_interval_ms': 60 * 1000,
    'auto_commit_interval_messages': None,
    'consumer_timeout_ms': -1,

    # Currently unused
    'socket_receive_buffer_bytes': 64 * 1024,
    'num_consumer_fetchers': 1,
    'default_fetcher_backoff_ms': 1000,
    'queued_max_message_chunks': 10,
    'rebalance_max_retries': 4,
    'rebalance_backoff_ms': 2000,
}

DEPRECATED_CONFIG_KEYS = {
    'metadata_broker_list': 'bootstrap_servers',
}

class KafkaConsumer(object):
    """A simpler kafka consumer"""

    def __init__(self, *topics, **configs):
        self.configure(**configs)
        self.set_topic_partitions(*topics)

    def configure(self, **configs):
        """Configure the consumer instance

        Configuration settings can be passed to constructor,
        otherwise defaults will be used:

        Keyword Arguments:
            bootstrap_servers (list): List of initial broker nodes the consumer
                should contact to bootstrap initial cluster metadata.  This does
                not have to be the full node list.  It just needs to have at
                least one broker that will respond to a Metadata API Request.
            client_id (str): a unique name for this client.  Defaults to
                'kafka.consumer.kafka'.
            group_id (str): the name of the consumer group to join,
                Offsets are fetched / committed to this group name.
            fetch_message_max_bytes (int, optional): Maximum bytes for each
                topic/partition fetch request.  Defaults to 1024*1024.
            fetch_min_bytes (int, optional): Minimum amount of data the server
                should return for a fetch request, otherwise wait up to
                fetch_wait_max_ms for more data to accumulate.  Defaults to 1.
            fetch_wait_max_ms (int, optional): Maximum time for the server to
                block waiting for fetch_min_bytes messages to accumulate.
                Defaults to 100.
            refresh_leader_backoff_ms (int, optional): Milliseconds to backoff
                when refreshing metadata on errors (subject to random jitter).
                Defaults to 200.
            socket_timeout_ms (int, optional): TCP socket timeout in
                milliseconds.  Defaults to 30*1000.
            auto_offset_reset (str, optional): A policy for resetting offsets on
                OffsetOutOfRange errors. 'smallest' will move to the oldest
                available message, 'largest' will move to the most recent.  Any
                ofther value will raise the exception.  Defaults to 'largest'.
            deserializer_class (callable, optional):  Any callable that takes a
                raw message value and returns a deserialized value.  Defaults to
                 lambda msg: msg.
            auto_commit_enable (bool, optional): Enabling auto-commit will cause
                the KafkaConsumer to periodically commit offsets without an
                explicit call to commit().  Defaults to False.
            auto_commit_interval_ms (int, optional):  If auto_commit_enabled,
                the milliseconds between automatic offset commits.  Defaults to
                60 * 1000.
            auto_commit_interval_messages (int, optional): If
                auto_commit_enabled, a number of messages consumed between
                automatic offset commits.  Defaults to None (disabled).
            consumer_timeout_ms (int, optional): number of millisecond to throw
                a timeout exception to the consumer if no message is available
                for consumption.  Defaults to -1 (dont throw exception).

        Configuration parameters are described in more detail at
        http://kafka.apache.org/documentation.html#highlevelconsumerapi
        """
        configs = self._deprecate_configs(**configs)
        self._config = {}
        for key in DEFAULT_CONSUMER_CONFIG:
            self._config[key] = configs.pop(key, DEFAULT_CONSUMER_CONFIG[key])

        if configs:
            raise KafkaConfigurationError('Unknown configuration key(s): ' +
                                          str(list(configs.keys())))

        if self._config['auto_commit_enable']:
            if not self._config['group_id']:
                raise KafkaConfigurationError(
                    'KafkaConsumer configured to auto-commit '
                    'without required consumer group (group_id)'
                )

        # Check auto-commit configuration
        if self._config['auto_commit_enable']:
            logger.info("Configuring consumer to auto-commit offsets")
            self._reset_auto_commit()

        if not self._config['bootstrap_servers']:
            raise KafkaConfigurationError(
                'bootstrap_servers required to configure KafkaConsumer'
            )

        self._client = KafkaClient(
            self._config['bootstrap_servers'],
            client_id=self._config['client_id'],
            timeout=(self._config['socket_timeout_ms'] / 1000.0)
        )

    def set_topic_partitions(self, *topics):
        """
        Set the topic/partitions to consume
        Optionally specify offsets to start from

        Accepts types:

        * str (utf-8): topic name (will consume all available partitions)
        * tuple: (topic, partition)
        * dict:
            - { topic: partition }
            - { topic: [partition list] }
            - { topic: (partition tuple,) }

        Optionally, offsets can be specified directly:

        * tuple: (topic, partition, offset)
        * dict:  { (topic, partition): offset, ... }

        Example:

        .. code:: python

            kafka = KafkaConsumer()

            # Consume topic1-all; topic2-partition2; topic3-partition0
            kafka.set_topic_partitions("topic1", ("topic2", 2), {"topic3": 0})

            # Consume topic1-0 starting at offset 12, and topic2-1 at offset 45
            # using tuples --
            kafka.set_topic_partitions(("topic1", 0, 12), ("topic2", 1, 45))

            # using dict --
            kafka.set_topic_partitions({ ("topic1", 0): 12, ("topic2", 1): 45 })

        """
        self._topics = []
        self._client.load_metadata_for_topics()

        # Setup offsets
        self._offsets = OffsetsStruct(fetch=dict(),
                                      commit=dict(),
                                      highwater=dict(),
                                      task_done=dict())

        # Handle different topic types
        for arg in topics:

            # Topic name str -- all partitions
            if isinstance(arg, (six.string_types, six.binary_type)):
                topic = kafka_bytestring(arg)

                for partition in self._client.get_partition_ids_for_topic(topic):
                    self._consume_topic_partition(topic, partition)

            # (topic, partition [, offset]) tuple
            elif isinstance(arg, tuple):
                topic = kafka_bytestring(arg[0])
                partition = arg[1]
                self._consume_topic_partition(topic, partition)
                if len(arg) == 3:
                    offset = arg[2]
                    self._offsets.fetch[(topic, partition)] = offset

            # { topic: partitions, ... } dict
            elif isinstance(arg, dict):
                for key, value in six.iteritems(arg):

                    # key can be string (a topic)
                    if isinstance(key, (six.string_types, six.binary_type)):
                        topic = kafka_bytestring(key)

                        # topic: partition
                        if isinstance(value, int):
                            self._consume_topic_partition(topic, value)

                        # topic: [ partition1, partition2, ... ]
                        elif isinstance(value, (list, tuple)):
                            for partition in value:
                                self._consume_topic_partition(topic, partition)
                        else:
                            raise KafkaConfigurationError(
                                'Unknown topic type '
                                '(dict key must be int or list/tuple of ints)'
                            )

                    # (topic, partition): offset
                    elif isinstance(key, tuple):
                        topic = kafka_bytestring(key[0])
                        partition = key[1]
                        self._consume_topic_partition(topic, partition)
                        self._offsets.fetch[(topic, partition)] = value

            else:
                raise KafkaConfigurationError('Unknown topic type (%s)' % type(arg))

        # If we have a consumer group, try to fetch stored offsets
        if self._config['group_id']:
            self._get_commit_offsets()

        # Update missing fetch/commit offsets
        for topic_partition in self._topics:

            # Commit offsets default is None
            if topic_partition not in self._offsets.commit:
                self._offsets.commit[topic_partition] = None

            # Skip if we already have a fetch offset from user args
            if topic_partition not in self._offsets.fetch:

                # Fetch offsets default is (1) commit
                if self._offsets.commit[topic_partition] is not None:
                    self._offsets.fetch[topic_partition] = self._offsets.commit[topic_partition]

                # or (2) auto reset
                else:
                    self._offsets.fetch[topic_partition] = self._reset_partition_offset(topic_partition)

        # highwater marks (received from server on fetch response)
        # and task_done (set locally by user)
        # should always get initialized to None
        self._reset_highwater_offsets()
        self._reset_task_done_offsets()

        # Reset message iterator in case we were in the middle of one
        self._reset_message_iterator()

    def next(self):
        """Return the next available message

        Blocks indefinitely unless consumer_timeout_ms > 0

        Returns:
            a single KafkaMessage from the message iterator

        Raises:
            ConsumerTimeout after consumer_timeout_ms and no message

        Note:
            This is also the method called internally during iteration

        """
        self._set_consumer_timeout_start()
        while True:

            try:
                return six.next(self._get_message_iterator())

            # Handle batch completion
            except StopIteration:
                self._reset_message_iterator()

            self._check_consumer_timeout()

    def fetch_messages(self):
        """Sends FetchRequests for all topic/partitions set for consumption

        Returns:
            Generator that yields KafkaMessage structs
            after deserializing with the configured `deserializer_class`

        Note:
            Refreshes metadata on errors, and resets fetch offset on
            OffsetOutOfRange, per the configured `auto_offset_reset` policy

        See Also:
            Key KafkaConsumer configuration parameters:
            * `fetch_message_max_bytes`
            * `fetch_max_wait_ms`
            * `fetch_min_bytes`
            * `deserializer_class`
            * `auto_offset_reset`

        """

        max_bytes = self._config['fetch_message_max_bytes']
        max_wait_time = self._config['fetch_wait_max_ms']
        min_bytes = self._config['fetch_min_bytes']

        if not self._topics:
            raise KafkaConfigurationError('No topics or partitions configured')

        if not self._offsets.fetch:
            raise KafkaConfigurationError(
                'No fetch offsets found when calling fetch_messages'
            )

        fetches = [FetchRequest(topic, partition,
                                self._offsets.fetch[(topic, partition)],
                                max_bytes)
                   for (topic, partition) in self._topics]

        # send_fetch_request will batch topic/partition requests by leader
        responses = self._client.send_fetch_request(
            fetches,
            max_wait_time=max_wait_time,
            min_bytes=min_bytes,
            fail_on_error=False
        )

        for resp in responses:

            if isinstance(resp, FailedPayloadsError):
                logger.warning('FailedPayloadsError attempting to fetch data')
                self._refresh_metadata_on_error()
                continue

            topic = kafka_bytestring(resp.topic)
            partition = resp.partition
            try:
                check_error(resp)
            except OffsetOutOfRangeError:
                logger.warning('OffsetOutOfRange: topic %s, partition %d, '
                               'offset %d (Highwatermark: %d)',
                               topic, partition,
                               self._offsets.fetch[(topic, partition)],
                               resp.highwaterMark)
                # Reset offset
                self._offsets.fetch[(topic, partition)] = (
                    self._reset_partition_offset((topic, partition))
                )
                continue

            except NotLeaderForPartitionError:
                logger.warning("NotLeaderForPartitionError for %s - %d. "
                               "Metadata may be out of date",
                               topic, partition)
                self._refresh_metadata_on_error()
                continue

            except RequestTimedOutError:
                logger.warning("RequestTimedOutError for %s - %d",
                               topic, partition)
                continue

            # Track server highwater mark
            self._offsets.highwater[(topic, partition)] = resp.highwaterMark

            # Yield each message
            # Kafka-python could raise an exception during iteration
            # we are not catching -- user will need to address
            for (offset, message) in resp.messages:
                # deserializer_class could raise an exception here
                val = self._config['deserializer_class'](message.value)
                msg = KafkaMessage(topic, partition, offset, message.key, val)

                # in some cases the server will return earlier messages
                # than we requested. skip them per kafka spec
                if offset < self._offsets.fetch[(topic, partition)]:
                    logger.debug('message offset less than fetched offset '
                                 'skipping: %s', msg)
                    continue
                # Only increment fetch offset
                # if we safely got the message and deserialized
                self._offsets.fetch[(topic, partition)] = offset + 1

                # Then yield to user
                yield msg

    def get_partition_offsets(self, topic, partition, request_time_ms, max_num_offsets):
        """Request available fetch offsets for a single topic/partition

        Keyword Arguments:
            topic (str): topic for offset request
            partition (int): partition for offset request
            request_time_ms (int): Used to ask for all messages before a
                certain time (ms). There are two special values.
                Specify -1 to receive the latest offset (i.e. the offset of the
                next coming message) and -2 to receive the earliest available
                offset. Note that because offsets are pulled in descending
                order, asking for the earliest offset will always return you a
                single element.
            max_num_offsets (int): Maximum offsets to include in the OffsetResponse

        Returns:
            a list of offsets in the OffsetResponse submitted for the provided
            topic / partition. See:
            https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetAPI
        """
        reqs = [OffsetRequest(topic, partition, request_time_ms, max_num_offsets)]

        (resp,) = self._client.send_offset_request(reqs)

        check_error(resp)

        # Just for sanity..
        # probably unnecessary
        assert resp.topic == topic
        assert resp.partition == partition

        return resp.offsets

    def offsets(self, group=None):
        """Get internal consumer offset values

        Keyword Arguments:
            group: Either "fetch", "commit", "task_done", or "highwater".
                If no group specified, returns all groups.

        Returns:
            A copy of internal offsets struct
        """
        if not group:
            return {
                'fetch': self.offsets('fetch'),
                'commit': self.offsets('commit'),
                'task_done': self.offsets('task_done'),
                'highwater': self.offsets('highwater')
            }
        else:
            return dict(deepcopy(getattr(self._offsets, group)))

    def task_done(self, message):
        """Mark a fetched message as consumed.

        Offsets for messages marked as "task_done" will be stored back
        to the kafka cluster for this consumer group on commit()

        Arguments:
            message (KafkaMessage): the message to mark as complete

        Returns:
            True, unless the topic-partition for this message has not
            been configured for the consumer. In normal operation, this
            should not happen. But see github issue 364.
        """
        topic_partition = (message.topic, message.partition)
        if topic_partition not in self._topics:
            logger.warning('Unrecognized topic/partition in task_done message: '
                           '{0}:{1}'.format(*topic_partition))
            return False

        offset = message.offset

        # Warn on non-contiguous offsets
        prev_done = self._offsets.task_done[topic_partition]
        if prev_done is not None and offset != (prev_done + 1):
            logger.warning('Marking task_done on a non-continuous offset: %d != %d + 1',
                           offset, prev_done)

        # Warn on smaller offsets than previous commit
        # "commit" offsets are actually the offset of the next message to fetch.
        prev_commit = self._offsets.commit[topic_partition]
        if prev_commit is not None and ((offset + 1) <= prev_commit):
            logger.warning('Marking task_done on a previously committed offset?: %d (+1) <= %d',
                           offset, prev_commit)

        self._offsets.task_done[topic_partition] = offset

        # Check for auto-commit
        if self._does_auto_commit_messages():
            self._incr_auto_commit_message_count()

        if self._should_auto_commit():
            self.commit()

        return True

    def commit(self):
        """Store consumed message offsets (marked via task_done())
        to kafka cluster for this consumer_group.

        Returns:
            True on success, or False if no offsets were found for commit

        Note:
            this functionality requires server version >=0.8.1.1
            https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
        """
        if not self._config['group_id']:
            logger.warning('Cannot commit without a group_id!')
            raise KafkaConfigurationError(
                'Attempted to commit offsets '
                'without a configured consumer group (group_id)'
            )

        # API supports storing metadata with each commit
        # but for now it is unused
        metadata = b''

        offsets = self._offsets.task_done
        commits = []
        for topic_partition, task_done_offset in six.iteritems(offsets):

            # Skip if None
            if task_done_offset is None:
                continue

            # Commit offsets as the next offset to fetch
            # which is consistent with the Java Client
            # task_done is marked by messages consumed,
            # so add one to mark the next message for fetching
            commit_offset = (task_done_offset + 1)

            # Skip if no change from previous committed
            if commit_offset == self._offsets.commit[topic_partition]:
                continue

            commits.append(
                OffsetCommitRequest(topic_partition[0], topic_partition[1],
                                    commit_offset, metadata)
            )

        if commits:
            logger.info('committing consumer offsets to group %s', self._config['group_id'])
            resps = self._client.send_offset_commit_request(
                kafka_bytestring(self._config['group_id']), commits,
                fail_on_error=False
            )

            for r in resps:
                check_error(r)
                topic_partition = (r.topic, r.partition)
                task_done = self._offsets.task_done[topic_partition]
                self._offsets.commit[topic_partition] = (task_done + 1)

            if self._config['auto_commit_enable']:
                self._reset_auto_commit()

            return True

        else:
            logger.info('No new offsets found to commit in group %s', self._config['group_id'])
            return False

    #
    # Topic/partition management private methods
    #

    def _consume_topic_partition(self, topic, partition):
        topic = kafka_bytestring(topic)
        if not isinstance(partition, int):
            raise KafkaConfigurationError('Unknown partition type (%s) '
                                          '-- expected int' % type(partition))

        if topic not in self._client.topic_partitions:
            raise UnknownTopicOrPartitionError("Topic %s not found in broker metadata" % topic)
        if partition not in self._client.get_partition_ids_for_topic(topic):
            raise UnknownTopicOrPartitionError("Partition %d not found in Topic %s "
                                               "in broker metadata" % (partition, topic))
        logger.info("Configuring consumer to fetch topic '%s', partition %d", topic, partition)
        self._topics.append((topic, partition))

    def _refresh_metadata_on_error(self):
        refresh_ms = self._config['refresh_leader_backoff_ms']
        jitter_pct = 0.20
        sleep_ms = random.randint(
            int((1.0 - 0.5 * jitter_pct) * refresh_ms),
            int((1.0 + 0.5 * jitter_pct) * refresh_ms)
        )
        while True:
            logger.info("Sleeping for refresh_leader_backoff_ms: %d", sleep_ms)
            time.sleep(sleep_ms / 1000.0)
            try:
                self._client.load_metadata_for_topics()
            except KafkaUnavailableError:
                logger.warning("Unable to refresh topic metadata... cluster unavailable")
                self._check_consumer_timeout()
            else:
                logger.info("Topic metadata refreshed")
                return

    #
    # Offset-managment private methods
    #

    def _get_commit_offsets(self):
        logger.info("Consumer fetching stored offsets")
        for topic_partition in self._topics:
            (resp,) = self._client.send_offset_fetch_request(
                kafka_bytestring(self._config['group_id']),
                [OffsetFetchRequest(topic_partition[0], topic_partition[1])],
                fail_on_error=False)
            try:
                check_error(resp)
            # API spec says server wont set an error here
            # but 0.8.1.1 does actually...
            except UnknownTopicOrPartitionError:
                pass

            # -1 offset signals no commit is currently stored
            if resp.offset == -1:
                self._offsets.commit[topic_partition] = None

            # Otherwise we committed the stored offset
            # and need to fetch the next one
            else:
                self._offsets.commit[topic_partition] = resp.offset

    def _reset_highwater_offsets(self):
        for topic_partition in self._topics:
            self._offsets.highwater[topic_partition] = None

    def _reset_task_done_offsets(self):
        for topic_partition in self._topics:
            self._offsets.task_done[topic_partition] = None

    def _reset_partition_offset(self, topic_partition):
        (topic, partition) = topic_partition
        LATEST = -1
        EARLIEST = -2

        request_time_ms = None
        if self._config['auto_offset_reset'] == 'largest':
            request_time_ms = LATEST
        elif self._config['auto_offset_reset'] == 'smallest':
            request_time_ms = EARLIEST
        else:

            # Let's raise an reasonable exception type if user calls
            # outside of an exception context
            if sys.exc_info() == (None, None, None):
                raise OffsetOutOfRangeError('Cannot reset partition offsets without a '
                                            'valid auto_offset_reset setting '
                                            '(largest|smallest)')

            # Otherwise we should re-raise the upstream exception
            # b/c it typically includes additional data about
            # the request that triggered it, and we do not want to drop that
            raise

        (offset, ) = self.get_partition_offsets(topic, partition,
                                                request_time_ms, max_num_offsets=1)
        return offset

    #
    # Consumer Timeout private methods
    #

    def _set_consumer_timeout_start(self):
        self._consumer_timeout = False
        if self._config['consumer_timeout_ms'] >= 0:
            self._consumer_timeout = time.time() + (self._config['consumer_timeout_ms'] / 1000.0)

    def _check_consumer_timeout(self):
        if self._consumer_timeout and time.time() > self._consumer_timeout:
            raise ConsumerTimeout('Consumer timed out after %d ms' % + self._config['consumer_timeout_ms'])

    #
    # Autocommit private methods
    #

    def _should_auto_commit(self):
        if self._does_auto_commit_ms():
            if time.time() >= self._next_commit_time:
                return True

        if self._does_auto_commit_messages():
            if self._uncommitted_message_count >= self._config['auto_commit_interval_messages']:
                return True

        return False

    def _reset_auto_commit(self):
        self._uncommitted_message_count = 0
        self._next_commit_time = None
        if self._does_auto_commit_ms():
            self._next_commit_time = time.time() + (self._config['auto_commit_interval_ms'] / 1000.0)

    def _incr_auto_commit_message_count(self, n=1):
        self._uncommitted_message_count += n

    def _does_auto_commit_ms(self):
        if not self._config['auto_commit_enable']:
            return False

        conf = self._config['auto_commit_interval_ms']
        if conf is not None and conf > 0:
            return True
        return False

    def _does_auto_commit_messages(self):
        if not self._config['auto_commit_enable']:
            return False

        conf = self._config['auto_commit_interval_messages']
        if conf is not None and conf > 0:
            return True
        return False

    #
    # Message iterator private methods
    #

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def _get_message_iterator(self):
        # Fetch a new batch if needed
        if self._msg_iter is None:
            self._msg_iter = self.fetch_messages()

        return self._msg_iter

    def _reset_message_iterator(self):
        self._msg_iter = None

    #
    # python private methods
    #

    def __repr__(self):
        return '<{0} topics=({1})>'.format(
            self.__class__.__name__,
            '|'.join(["%s-%d" % topic_partition
                      for topic_partition in self._topics])
        )

    #
    # other private methods
    #

    def _deprecate_configs(self, **configs):
        for old, new in six.iteritems(DEPRECATED_CONFIG_KEYS):
            if old in configs:
                logger.warning('Deprecated Kafka Consumer configuration: %s. '
                               'Please use %s instead.', old, new)
                old_value = configs.pop(old)
                if new not in configs:
                    configs[new] = old_value
        return configs
