from collections import defaultdict, namedtuple
from copy import deepcopy
import logging
import sys
import time

from kafka.client import KafkaClient
from kafka.common import (
    OffsetFetchRequest, OffsetCommitRequest, OffsetRequest, FetchRequest,
    check_error, NotLeaderForPartitionError, UnknownTopicOrPartitionError,
    OffsetOutOfRangeError, RequestTimedOutError, KafkaMessage, ConsumerTimeout,
    FailedPayloadsError, KafkaUnavailableError
)

logger = logging.getLogger(__name__)

OffsetsStruct = namedtuple("OffsetsStruct", ["fetch", "highwater", "commit", "task_done"])


class KafkaConsumer(object):
  """
  A simpler kafka consumer

  ```
  # A very basic 'tail' consumer, with no stored offset management
  kafka = KafkaConsumer('topic1')
  for m in kafka:
    print m

  # Alternate interface: next() 
  while True:
    print kafka.next()

  # Alternate interface: batch iteration
  while True:
    for m in kafka.fetch_messages():
      print m
    print "Done with batch - let's do another!"
  ```

  ```
  # more advanced consumer -- multiple topics w/ auto commit offset management
  kafka = KafkaConsumer('topic1', 'topic2',
                        group_id='my_consumer_group',
                        auto_commit_enable=True,
                        auto_commit_interval_ms=30 * 1000,
                        auto_offset_reset='smallest')

  # Infinite iteration
  for m in kafka:
    process_message(m)
    kafka.task_done(m)

  # Alternate interface: next()
  while True:
    m = kafka.next()
    process_message(m)
    kafka.task_done(m)

  # Batch process interface does not auto_commit!
  while True:
    for m in kafka.fetch_messages():
      process_message(m)
      kafka.task_done(m)
    kafka.commit()
  ```

  messages (m) are namedtuples with attributes:
    m.topic: topic name (str)
    m.partition: partition number (int)
    m.offset: message offset on topic-partition log (int)
    m.key: key (bytes - can be None)
    m.value: message (output of deserializer_class - default is event object)

  Configuration settings can be passed to constructor,
  otherwise defaults will be used:
    client_id='kafka.consumer.XXX',
    group_id=None,
    fetch_message_max_bytes=1024*1024,
    fetch_min_bytes=1,
    fetch_wait_max_ms=100,
    refresh_leader_backoff_ms=200,
    metadata_broker_list=None,
    socket_timeout_ms=30*1000,
    auto_offset_reset='largest',
    deserializer_class=Event.from_bytes,
    auto_commit_enable=False,
    auto_commit_interval_ms=60 * 1000,
    consumer_timeout_ms=-1,


  Configuration parameters are described in more detail at
  http://kafka.apache.org/documentation.html#highlevelconsumerapi
  """

  DEFAULT_CONSUMER_CONFIG = {
    'client_id': __name__,
    'group_id': None,
    'metadata_broker_list': None,
    'socket_timeout_ms': 30 * 1000,
    'fetch_message_max_bytes': 1024 * 1024,
    'auto_offset_reset': 'largest',
    'fetch_min_bytes': 1,
    'fetch_wait_max_ms': 100,
    'refresh_leader_backoff_ms': 200,
    'deserializer_class': lambda msg: msg,
    'auto_commit_enable': False,
    'auto_commit_interval_ms': 60 * 1000,
    'consumer_timeout_ms': -1,

    # Currently unused
    'socket_receive_buffer_bytes': 64 * 1024,
    'refresh_leader_backoff_ms': 200,
    'num_consumer_fetchers': 1,
    'default_fetcher_backoff_ms': 1000,
    'queued_max_message_chunks': 10,
    'rebalance_max_retries': 4,
    'rebalance_backoff_ms': 2000,
  }

  def __init__(self, *topics, **configs):
    self.topics = topics
    self.config = configs
    self.client = KafkaClient(self._get_config('metadata_broker_list'),
                              client_id=self._get_config('client_id'),
                              timeout=(self._get_config('socket_timeout_ms') / 1000.0))

    # Get initial topic metadata
    self.client.load_metadata_for_topics()
    for topic in self.topics:
      if topic not in self.client.topic_partitions:
        raise ValueError("Topic %s not found in broker metadata" % topic)
      logger.info("Configuring consumer to fetch topic '%s'", topic)

    # Check auto-commit configuration
    if self._get_config('auto_commit_enable'):
      if not self._get_config('group_id'):
        raise RuntimeError('KafkaConsumer configured to auto-commit without required consumer group (group_id)')

      logger.info("Configuring consumer to auto-commit offsets")
      self._set_next_auto_commit_time()

    # Setup offsets
    self._offsets = OffsetsStruct(fetch=defaultdict(dict),
                                  commit=defaultdict(dict),
                                  highwater= defaultdict(dict),
                                  task_done=defaultdict(dict))

    # If we have a consumer group, try to fetch stored offsets
    if self._get_config('group_id'):
      self._fetch_stored_offsets()
    else:
      self._auto_reset_offsets()

    # highwater marks (received from server on fetch response)
    # and task_done (set locally by user)
    # should always get initialized to None
    self._reset_highwater_offsets()
    self._reset_task_done_offsets()

    # Start the message fetch generator
    self._msg_iter = self.fetch_messages()

  def _fetch_stored_offsets(self):
    logger.info("Consumer fetching stored offsets")
    for topic in self.topics:
      for partition in self.client.topic_partitions[topic]:

        (resp,) = self.client.send_offset_fetch_request(
            self._get_config('group_id'),
            [OffsetFetchRequest(topic, partition)],
            fail_on_error=False)
        try:
          check_error(resp)
        # API spec says server wont set an error here
        # but 0.8.1.1 does actually...
        except UnknownTopicOrPartitionError:
          pass

        # -1 offset signals no commit is currently stored
        if resp.offset == -1:
          self._offsets.commit[topic][partition] = None
          self._offsets.fetch[topic][partition] = self._reset_partition_offset(topic, partition)

        # Otherwise we committed the stored offset
        # and need to fetch the next one
        else:
          self._offsets.commit[topic][partition] = resp.offset
          self._offsets.fetch[topic][partition] = resp.offset

  def _auto_reset_offsets(self):
    logger.info("Consumer auto-resetting offsets")
    for topic in self.topics:
      for partition in self.client.topic_partitions[topic]:

        self._offsets.fetch[topic][partition] = self._reset_partition_offset(topic, partition)
        self._offsets.commit[topic][partition] = None

  def _reset_highwater_offsets(self):
    for topic in self.topics:
      for partition in self.client.topic_partitions[topic]:
        self._offsets.highwater[topic][partition] = None

  def _reset_task_done_offsets(self):
    for topic in self.topics:
      for partition in self.client.topic_partitions[topic]:
        self._offsets.task_done[topic][partition] = None

  def __repr__(self):
    return '<KafkaConsumer topics=(%s)>' % ', '.join(self.topics)

  def __iter__(self):
    return self

  def next(self):
    consumer_timeout = False
    if self._get_config('consumer_timeout_ms') >= 0:
      consumer_timeout = time.time() + (self._get_config('consumer_timeout_ms') / 1000.0)

    while True:

      # Check for auto-commit
      if self.should_auto_commit():
        self.commit()

      try:
        return self._msg_iter.next()

      # If the previous batch finishes, start get new batch
      except StopIteration:
        self._msg_iter = self.fetch_messages()

      if consumer_timeout and time.time() > consumer_timeout:
        raise ConsumerTimeout('Consumer timed out waiting to fetch messages')

  def offsets(self, group=None):
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
    topic = message.topic
    partition = message.partition
    offset = message.offset

    # Warn on non-contiguous offsets
    prev_done = self._offsets.task_done[topic][partition]
    if prev_done is not None and offset != (prev_done + 1):
      logger.warning('Marking task_done on a non-continuous offset: %d != %d + 1',
                     offset, prev_done)

    # Warn on smaller offsets than previous commit
    # "commit" offsets are actually the offset of the next # message to fetch.
    # so task_done should be compared with (commit - 1)
    prev_done = (self._offsets.commit[topic][partition] - 1)
    if prev_done is not None and (offset <= prev_done):
      logger.warning('Marking task_done on a previously committed offset?: %d <= %d',
                     offset, prev_done)

    self._offsets.task_done[topic][partition] = offset

  def should_auto_commit(self):
    if not self._get_config('auto_commit_enable'):
      return False

    if not self._next_commit:
      return False

    return (time.time() >= self._next_commit)

  def _set_next_auto_commit_time(self):
    self._next_commit = time.time() + (self._get_config('auto_commit_interval_ms') / 1000.0)

  def commit(self):
    if not self._get_config('group_id'):
      logger.warning('Cannot commit without a group_id!')
      raise RuntimeError('Attempted to commit offsets without a configured consumer group (group_id)')

    # API supports storing metadata with each commit
    # but for now it is unused
    metadata = ''

    offsets = self._offsets.task_done
    commits = []
    for topic, partitions in offsets.iteritems():
      for partition, task_done in partitions.iteritems():

        # Skip if None
        if task_done is None:
          continue

        # Commit offsets as the next offset to fetch
        # which is consistent with the Java Client
        # task_done is marked by messages consumed,
        # so add one to mark the next message for fetching
        commit_offset = (task_done + 1)

        # Skip if no change from previous committed
        if commit_offset == self._offsets.commit[topic][partition]:
          continue

        commits.append(OffsetCommitRequest(topic, partition, commit_offset, metadata))

    if commits:
      logger.info('committing consumer offsets to group %s', self._get_config('group_id'))
      resps = self.client.send_offset_commit_request(self._get_config('group_id'),
                                                     commits,
                                                     fail_on_error=False)

      for r in resps:
        check_error(r)
        task_done = self._offsets.task_done[r.topic][r.partition]
        self._offsets.commit[r.topic][r.partition] = (task_done + 1)

      if self._get_config('auto_commit_enable'):
        self._set_next_auto_commit_time()

      return True

    else:
      logger.info('No new offsets found to commit in group %s', self._get_config('group_id'))
      return False

  def _get_config(self, key):
    return self.config.get(key, self.DEFAULT_CONSUMER_CONFIG[key])

  def fetch_messages(self):

    max_bytes = self._get_config('fetch_message_max_bytes')
    max_wait_time = self._get_config('fetch_wait_max_ms')
    min_bytes = self._get_config('fetch_min_bytes')

    fetches = []
    offsets = self._offsets.fetch
    for topic, partitions in offsets.iteritems():
      for partition, offset in partitions.iteritems():
        fetches.append(FetchRequest(topic, partition, offset, max_bytes))

    # client.send_fetch_request will collect topic/partition requests by leader
    # and send each group as a single FetchRequest to the correct broker
    try:
      responses = self.client.send_fetch_request(fetches,
                                                 max_wait_time=max_wait_time,
                                                 min_bytes=min_bytes,
                                                 fail_on_error=False)
    except FailedPayloadsError:
      logger.warning('FailedPayloadsError attempting to fetch data from kafka')
      self._refresh_metadata_on_error()
      return

    for resp in responses:
      topic = resp.topic
      partition = resp.partition
      try:
        check_error(resp)
      except OffsetOutOfRangeError:
        logger.warning('OffsetOutOfRange: topic %s, partition %d, offset %d '
                       '(Highwatermark: %d)',
                       topic, partition, offsets[topic][partition],
                       resp.highwaterMark)
        # Reset offset
        self._offsets.fetch[topic][partition] = self._reset_partition_offset(topic, partition)
        continue

      except NotLeaderForPartitionError:
        logger.warning("NotLeaderForPartitionError for %s - %d. "
                       "Metadata may be out of date",
                       topic, partition)
        self._refresh_metadata_on_error()
        continue

      except RequestTimedOutError:
        logger.warning("RequestTimedOutError for %s - %d", topic, partition)
        continue

      # Track server highwater mark
      self._offsets.highwater[topic][partition] = resp.highwaterMark

      # Yield each message
      # Kafka-python could raise an exception during iteration
      # we are not catching -- user will need to address
      for (offset, message) in resp.messages:
        # deserializer_class could raise an exception here
        msg = KafkaMessage(topic, partition, offset, message.key,
                           self._get_config('deserializer_class')(message.value))

        # Only increment fetch offset if we safely got the message and deserialized
        self._offsets.fetch[topic][partition] = offset + 1

        # Then yield to user
        yield msg

  def _reset_partition_offset(self, topic, partition):
    LATEST = -1
    EARLIEST = -2

    RequestTime = None
    if self._get_config('auto_offset_reset') == 'largest':
      RequestTime = LATEST
    elif self._get_config('auto_offset_reset') == 'smallest':
      RequestTime = EARLIEST
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

    (offset, ) = self.get_partition_offsets(topic, partition, RequestTime,
                                            num_offsets=1)
    return offset

  def get_partition_offsets(self, topic, partition, request_time, num_offsets):
    reqs = [OffsetRequest(topic, partition, request_time, num_offsets)]

    (resp,) = self.client.send_offset_request(reqs)

    check_error(resp)

    # Just for sanity..
    # probably unnecessary
    assert resp.topic == topic
    assert resp.partition == partition

    return resp.offsets

  def _refresh_metadata_on_error(self):
    sleep_ms = self._get_config('refresh_leader_backoff_ms')
    while True:
      logger.info("Sleeping for refresh_leader_backoff_ms: %d", sleep_ms)
      time.sleep(sleep_ms / 1000.0)
      try:
        self.client.load_metadata_for_topics()
      except KafkaUnavailableError:
        logger.warning("Unable to refresh topic metadata... cluster unavailable")
      else:
        logger.info("Topic metadata refreshed")
        return
