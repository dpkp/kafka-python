from collections import defaultdict
from itertools import izip_longest, repeat
import logging
import time
from threading import Lock
from multiprocessing import Process, Queue, Event, Value
from Queue import Empty

from kafka.common import (
    ErrorMapping, FetchRequest,
    OffsetRequest, OffsetFetchRequest, OffsetCommitRequest
)

from kafka.util import (
    ReentrantTimer, ConsumerFetchSizeTooSmall
)

log = logging.getLogger("kafka")

AUTO_COMMIT_MSG_COUNT = 100
AUTO_COMMIT_INTERVAL = 5000

FETCH_DEFAULT_BLOCK_TIMEOUT = 1
FETCH_MAX_WAIT_TIME = 100
FETCH_MIN_BYTES = 4096


class FetchContext(object):
    """
    Class for managing the state of a consumer during fetch
    """
    def __init__(self, consumer, block, timeout):
        self.consumer = consumer
        self.block = block

        if block and not timeout:
            timeout = FETCH_DEFAULT_BLOCK_TIMEOUT

        self.timeout = timeout * 1000

    def __enter__(self):
        """Set fetch values based on blocking status"""
        if self.block:
            self.consumer.fetch_max_wait_time = self.timeout
            self.consumer.fetch_min_bytes = 1
        else:
            self.consumer.fetch_min_bytes = 0

    def __exit__(self, type, value, traceback):
        """Reset values to default"""
        self.consumer.fetch_max_wait_time = FETCH_MAX_WAIT_TIME
        self.consumer.fetch_min_bytes = FETCH_MIN_BYTES


class Consumer(object):
    """
    Base class to be used by other consumers. Not to be used directly

    This base class provides logic for
    * initialization and fetching metadata of partitions
    * Auto-commit logic
    * APIs for fetching pending message count
    """
    def __init__(self, client, group, topic, partitions=None, auto_commit=True,
                 auto_commit_every_n=AUTO_COMMIT_MSG_COUNT,
                 auto_commit_every_t=AUTO_COMMIT_INTERVAL):

        self.client = client
        self.topic = topic
        self.group = group
        self.client._load_metadata_for_topics(topic)
        self.offsets = {}

        if not partitions:
            partitions = self.client.topic_partitions[topic]

        # Variables for handling offset commits
        self.commit_lock = Lock()
        self.commit_timer = None
        self.count_since_commit = 0
        self.auto_commit = auto_commit
        self.auto_commit_every_n = auto_commit_every_n
        self.auto_commit_every_t = auto_commit_every_t

        # Set up the auto-commit timer
        if auto_commit is True and auto_commit_every_t is not None:
            self.commit_timer = ReentrantTimer(auto_commit_every_t,
                                               self.commit)
            self.commit_timer.start()

        def get_or_init_offset_callback(resp):
            if resp.error == ErrorMapping.NO_ERROR:
                return resp.offset
            elif resp.error == ErrorMapping.UNKNOWN_TOPIC_OR_PARTITON:
                return 0
            else:
                raise Exception("OffsetFetchRequest for topic=%s, "
                                "partition=%d failed with errorcode=%s" % (
                                    resp.topic, resp.partition, resp.error))

        # Uncomment for 0.8.1
        #
        #for partition in partitions:
        #    req = OffsetFetchRequest(topic, partition)
        #    (offset,) = self.client.send_offset_fetch_request(group, [req],
        #                  callback=get_or_init_offset_callback,
        #                  fail_on_error=False)
        #    self.offsets[partition] = offset

        for partition in partitions:
            self.offsets[partition] = 0

    def commit(self, partitions=None):
        """
        Commit offsets for this consumer

        partitions: list of partitions to commit, default is to commit
                    all of them
        """

        # short circuit if nothing happened. This check is kept outside
        # to prevent un-necessarily acquiring a lock for checking the state
        if self.count_since_commit == 0:
            return

        with self.commit_lock:
            # Do this check again, just in case the state has changed
            # during the lock acquiring timeout
            if self.count_since_commit == 0:
                return

            reqs = []
            if not partitions:  # commit all partitions
                partitions = self.offsets.keys()

            for partition in partitions:
                offset = self.offsets[partition]
                log.debug("Commit offset %d in SimpleConsumer: "
                          "group=%s, topic=%s, partition=%s" %
                          (offset, self.group, self.topic, partition))

                reqs.append(OffsetCommitRequest(self.topic, partition,
                                                offset, None))

            resps = self.client.send_offset_commit_request(self.group, reqs)
            for resp in resps:
                assert resp.error == 0

            self.count_since_commit = 0

    def _auto_commit(self):
        """
        Check if we have to commit based on number of messages and commit
        """

        # Check if we are supposed to do an auto-commit
        if not self.auto_commit or self.auto_commit_every_n is None:
            return

        if self.count_since_commit > self.auto_commit_every_n:
            self.commit()

    def stop(self):
        if self.commit_timer is not None:
            self.commit_timer.stop()
            self.commit()

    def pending(self, partitions=None):
        """
        Gets the pending message count

        partitions: list of partitions to check for, default is to check all
        """
        if not partitions:
            partitions = self.offsets.keys()

        total = 0
        reqs = []

        for partition in partitions:
            reqs.append(OffsetRequest(self.topic, partition, -1, 1))

        resps = self.client.send_offset_request(reqs)
        for resp in resps:
            partition = resp.partition
            pending = resp.offsets[0]
            offset = self.offsets[partition]
            total += pending - offset - (1 if offset > 0 else 0)

        return total


class SimpleConsumer(Consumer):
    """
    A simple consumer implementation that consumes all/specified partitions
    for a topic

    client: a connected KafkaClient
    group: a name for this consumer, used for offset storage and must be unique
    topic: the topic to consume
    partitions: An optional list of partitions to consume the data from

    auto_commit: default True. Whether or not to auto commit the offsets
    auto_commit_every_n: default 100. How many messages to consume
                         before a commit
    auto_commit_every_t: default 5000. How much time (in milliseconds) to
                         wait before commit

    fetch_size_bytes:    number of bytes to request in a FetchRequest

    Auto commit details:
    If both auto_commit_every_n and auto_commit_every_t are set, they will
    reset one another when one is triggered. These triggers simply call the
    commit method on this class. A manual call to commit will also reset
    these triggers
    """
    def __init__(self, client, group, topic, auto_commit=True, partitions=None,
                 auto_commit_every_n=AUTO_COMMIT_MSG_COUNT,
                 auto_commit_every_t=AUTO_COMMIT_INTERVAL,
                 fetch_size_bytes=FETCH_MIN_BYTES):

        self.partition_info = False     # Do not return partition info in msgs
        self.fetch_max_wait_time = FETCH_MAX_WAIT_TIME
        self.fetch_min_bytes = fetch_size_bytes
        self.fetch_started = defaultdict(bool)  # defaults to false

        super(SimpleConsumer, self).__init__(client, group, topic,
                                    partitions=partitions,
                                    auto_commit=auto_commit,
                                    auto_commit_every_n=auto_commit_every_n,
                                    auto_commit_every_t=auto_commit_every_t)

    def provide_partition_info(self):
        """
        Indicates that partition info must be returned by the consumer
        """
        self.partition_info = True

    def seek(self, offset, whence):
        """
        Alter the current offset in the consumer, similar to fseek

        offset: how much to modify the offset
        whence: where to modify it from
                0 is relative to the earliest available offset (head)
                1 is relative to the current offset
                2 is relative to the latest known offset (tail)
        """

        if whence == 1:  # relative to current position
            for partition, _offset in self.offsets.items():
                self.offsets[partition] = _offset + offset
        elif whence in (0, 2):  # relative to beginning or end
            # divide the request offset by number of partitions,
            # distribute the remained evenly
            (delta, rem) = divmod(offset, len(self.offsets))
            deltas = {}
            for partition, r in izip_longest(self.offsets.keys(),
                                             repeat(1, rem), fillvalue=0):
                deltas[partition] = delta + r

            reqs = []
            for partition in self.offsets.keys():
                if whence == 0:
                    reqs.append(OffsetRequest(self.topic, partition, -2, 1))
                elif whence == 2:
                    reqs.append(OffsetRequest(self.topic, partition, -1, 1))

                    # The API returns back the next available offset
                    # For eg: if the current offset is 18, the API will return
                    # back 19. So, if we have to seek 5 points before, we will
                    # end up going back to 14, instead of 13. Adjust this
                    deltas[partition] -= 1
                else:
                    pass

            resps = self.client.send_offset_request(reqs)
            for resp in resps:
                self.offsets[resp.partition] = resp.offsets[0] + \
                                                deltas[resp.partition]
        else:
            raise ValueError("Unexpected value for `whence`, %d" % whence)

    def get_messages(self, count=1, block=True, timeout=0.1):
        """
        Fetch the specified number of messages

        count: Indicates the maximum number of messages to be fetched
        block: If True, the API will block till some messages are fetched.
        timeout: If None, and block=True, the API will block infinitely.
                 If >0, API will block for specified time (in seconds)
        """
        messages = []
        iterator = self.__iter__()

        # HACK: This splits the timeout between available partitions
        timeout = timeout * 1.0 / len(self.offsets)

        with FetchContext(self, block, timeout):
            while count > 0:
                try:
                    messages.append(next(iterator))
                except StopIteration as exp:
                    break
                count -= 1

        return messages

    def __iter__(self):
        """
        Create an iterate per partition. Iterate through them calling next()
        until they are all exhausted.
        """
        iters = {}
        for partition, offset in self.offsets.items():
            iters[partition] = self.__iter_partition__(partition, offset)

        if len(iters) == 0:
            return

        while True:
            if len(iters) == 0:
                break

            for partition, it in iters.items():
                try:
                    if self.partition_info:
                        yield (partition, it.next())
                    else:
                        yield it.next()
                except StopIteration:
                    log.debug("Done iterating over partition %s" % partition)
                    del iters[partition]

                    # skip auto-commit since we didn't yield anything
                    continue

                # Count, check and commit messages if necessary
                self.count_since_commit += 1
                self._auto_commit()

    def __iter_partition__(self, partition, offset):
        """
        Iterate over the messages in a partition. Create a FetchRequest
        to get back a batch of messages, yield them one at a time.
        After a batch is exhausted, start a new batch unless we've reached
        the end of this partition.
        """

        # The offset that is stored in the consumer is the offset that
        # we have consumed. In subsequent iterations, we are supposed to
        # fetch the next message (that is from the next offset)
        # However, for the 0th message, the offset should be as-is.
        # An OffsetFetchRequest to Kafka gives 0 for a new queue. This is
        # problematic, since 0 is offset of a message which we have not yet
        # consumed.
        if self.fetch_started[partition]:
            offset += 1

        fetch_size = self.fetch_min_bytes

        while True:
            req = FetchRequest(self.topic, partition, offset, fetch_size)

            (resp,) = self.client.send_fetch_request([req],
                                    max_wait_time=self.fetch_max_wait_time,
                                    min_bytes=fetch_size)

            assert resp.topic == self.topic
            assert resp.partition == partition

            next_offset = None
            try:
                for message in resp.messages:
                    next_offset = message.offset

                    # update the offset before the message is yielded. This is
                    # so that the consumer state is not lost in certain cases.
                    # For eg: the message is yielded and consumed by the caller,
                    # but the caller does not come back into the generator again.
                    # The message will be consumed but the status will not be
                    # updated in the consumer
                    self.fetch_started[partition] = True
                    self.offsets[partition] = message.offset
                    yield message
            except ConsumerFetchSizeTooSmall, e:
                log.warn("Fetch size is too small, increasing by 1.5x and retrying")
                fetch_size *= 1.5
                continue
            except ConsumerNoMoreData, e:
                log.debug("Iteration was ended by %r", e)

            if next_offset is None:
                break
            else:
                offset = next_offset + 1


class MultiProcessConsumer(Consumer):
    """
    A consumer implementation that consumes partitions for a topic in
    parallel using multiple processes

    client: a connected KafkaClient
    group: a name for this consumer, used for offset storage and must be unique
    topic: the topic to consume

    auto_commit: default True. Whether or not to auto commit the offsets
    auto_commit_every_n: default 100. How many messages to consume
                         before a commit
    auto_commit_every_t: default 5000. How much time (in milliseconds) to
                         wait before commit
    num_procs: Number of processes to start for consuming messages.
               The available partitions will be divided among these processes
    partitions_per_proc: Number of partitions to be allocated per process
               (overrides num_procs)

    Auto commit details:
    If both auto_commit_every_n and auto_commit_every_t are set, they will
    reset one another when one is triggered. These triggers simply call the
    commit method on this class. A manual call to commit will also reset
    these triggers
    """
    def __init__(self, client, group, topic, auto_commit=True,
                 auto_commit_every_n=AUTO_COMMIT_MSG_COUNT,
                 auto_commit_every_t=AUTO_COMMIT_INTERVAL,
                 num_procs=1, partitions_per_proc=0):

        # Initiate the base consumer class
        super(MultiProcessConsumer, self).__init__(client, group, topic,
                                    partitions=None,
                                    auto_commit=auto_commit,
                                    auto_commit_every_n=auto_commit_every_n,
                                    auto_commit_every_t=auto_commit_every_t)

        # Variables for managing and controlling the data flow from
        # consumer child process to master
        self.queue = Queue(1024)    # Child consumers dump messages into this
        self.start = Event()        # Indicates the consumers to start fetch
        self.exit = Event()         # Requests the consumers to shutdown
        self.pause = Event()        # Requests the consumers to pause fetch
        self.size = Value('i', 0)   # Indicator of number of messages to fetch

        partitions = self.offsets.keys()

        # If unspecified, start one consumer per partition
        # The logic below ensures that
        # * we do not cross the num_procs limit
        # * we have an even distribution of partitions among processes
        if not partitions_per_proc:
            partitions_per_proc = round(len(partitions) * 1.0 / num_procs)
            if partitions_per_proc < num_procs * 0.5:
                partitions_per_proc += 1

        # The final set of chunks
        chunker = lambda *x: [] + list(x)
        chunks = map(chunker, *[iter(partitions)] * int(partitions_per_proc))

        self.procs = []
        for chunk in chunks:
            chunk = filter(lambda x: x is not None, chunk)
            proc = Process(target=self._consume, args=(chunk,))
            proc.daemon = True
            proc.start()
            self.procs.append(proc)

    def _consume(self, partitions):
        """
        A child process worker which consumes messages based on the
        notifications given by the controller process
        """

        # Make the child processes open separate socket connections
        self.client.reinit()

        # We will start consumers without auto-commit. Auto-commit will be
        # done by the master controller process.
        consumer = SimpleConsumer(self.client, self.group, self.topic,
                                  partitions=partitions,
                                  auto_commit=False,
                                  auto_commit_every_n=None,
                                  auto_commit_every_t=None)

        # Ensure that the consumer provides the partition information
        consumer.provide_partition_info()

        while True:
            # Wait till the controller indicates us to start consumption
            self.start.wait()

            # If we are asked to quit, do so
            if self.exit.is_set():
                break

            # Consume messages and add them to the queue. If the controller
            # indicates a specific number of messages, follow that advice
            count = 0

            for partition, message in consumer:
                self.queue.put((partition, message))
                count += 1

                # We have reached the required size. The controller might have
                # more than what he needs. Wait for a while.
                # Without this logic, it is possible that we run into a big
                # loop consuming all available messages before the controller
                # can reset the 'start' event
                if count == self.size.value:
                    self.pause.wait()
                    break

            # In case we did not receive any message, give up the CPU for
            # a while before we try again
            if count == 0:
                time.sleep(0.1)

        consumer.stop()

    def stop(self):
        # Set exit and start off all waiting consumers
        self.exit.set()
        self.pause.set()
        self.start.set()

        for proc in self.procs:
            proc.join()
            proc.terminate()

        super(MultiProcessConsumer, self).stop()

    def __iter__(self):
        """
        Iterator to consume the messages available on this consumer
        """
        # Trigger the consumer procs to start off.
        # We will iterate till there are no more messages available
        self.size.value = 0
        self.pause.set()

        while True:
            self.start.set()
            try:
                # We will block for a small while so that the consumers get
                # a chance to run and put some messages in the queue
                # TODO: This is a hack and will make the consumer block for
                # at least one second. Need to find a better way of doing this
                partition, message = self.queue.get(block=True, timeout=1)
            except Empty:
                break

            # Count, check and commit messages if necessary
            self.offsets[partition] = message.offset
            self.start.clear()
            yield message

            self.count_since_commit += 1
            self._auto_commit()

        self.start.clear()

    def get_messages(self, count=1, block=True, timeout=10):
        """
        Fetch the specified number of messages

        count: Indicates the maximum number of messages to be fetched
        block: If True, the API will block till some messages are fetched.
        timeout: If None, and block=True, the API will block infinitely.
                 If >0, API will block for specified time (in seconds)
        """
        messages = []

        # Give a size hint to the consumers. Each consumer process will fetch
        # a maximum of "count" messages. This will fetch more messages than
        # necessary, but these will not be committed to kafka. Also, the extra
        # messages can be provided in subsequent runs
        self.size.value = count
        self.pause.clear()

        while count > 0:
            # Trigger consumption only if the queue is empty
            # By doing this, we will ensure that consumers do not
            # go into overdrive and keep consuming thousands of
            # messages when the user might need only a few
            if self.queue.empty():
                self.start.set()

            try:
                partition, message = self.queue.get(block, timeout)
            except Empty:
                break

            messages.append(message)

            # Count, check and commit messages if necessary
            self.offsets[partition] = message.offset
            self.count_since_commit += 1
            self._auto_commit()
            count -= 1

        self.size.value = 0
        self.start.clear()
        self.pause.set()

        return messages
