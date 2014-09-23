from __future__ import absolute_import

try:
    from itertools import zip_longest as izip_longest, repeat  # pylint: disable-msg=E0611
except ImportError:  # python 2
    from itertools import izip_longest as izip_longest, repeat
import logging
import time

import six

try:
    from Queue import Empty, Queue
except ImportError:  # python 2
    from queue import Empty, Queue

from kafka.common import (
    FetchRequest, OffsetRequest,
    ConsumerFetchSizeTooSmall, ConsumerNoMoreData
)
from .base import (
    Consumer,
    FETCH_DEFAULT_BLOCK_TIMEOUT,
    AUTO_COMMIT_MSG_COUNT,
    AUTO_COMMIT_INTERVAL,
    FETCH_MIN_BYTES,
    FETCH_BUFFER_SIZE_BYTES,
    MAX_FETCH_BUFFER_SIZE_BYTES,
    FETCH_MAX_WAIT_TIME,
    ITER_TIMEOUT_SECONDS,
    NO_MESSAGES_WAIT_TIME_SECONDS
)

log = logging.getLogger("kafka")

class FetchContext(object):
    """
    Class for managing the state of a consumer during fetch
    """
    def __init__(self, consumer, block, timeout):
        self.consumer = consumer
        self.block = block

        if block:
            if not timeout:
                timeout = FETCH_DEFAULT_BLOCK_TIMEOUT
            self.timeout = timeout * 1000

    def __enter__(self):
        """Set fetch values based on blocking status"""
        self.orig_fetch_max_wait_time = self.consumer.fetch_max_wait_time
        self.orig_fetch_min_bytes = self.consumer.fetch_min_bytes
        if self.block:
            self.consumer.fetch_max_wait_time = self.timeout
            self.consumer.fetch_min_bytes = 1
        else:
            self.consumer.fetch_min_bytes = 0

    def __exit__(self, type, value, traceback):
        """Reset values"""
        self.consumer.fetch_max_wait_time = self.orig_fetch_max_wait_time
        self.consumer.fetch_min_bytes = self.orig_fetch_min_bytes


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
    buffer_size:         default 4K. Initial number of bytes to tell kafka we
                         have available. This will double as needed.
    max_buffer_size:     default 16K. Max number of bytes to tell kafka we have
                         available. None means no limit.
    iter_timeout:        default None. How much time (in seconds) to wait for a
                         message in the iterator before exiting. None means no
                         timeout, so it will wait forever.

    Auto commit details:
    If both auto_commit_every_n and auto_commit_every_t are set, they will
    reset one another when one is triggered. These triggers simply call the
    commit method on this class. A manual call to commit will also reset
    these triggers
    """
    def __init__(self, client, group, topic, auto_commit=True, partitions=None,
                 auto_commit_every_n=AUTO_COMMIT_MSG_COUNT,
                 auto_commit_every_t=AUTO_COMMIT_INTERVAL,
                 fetch_size_bytes=FETCH_MIN_BYTES,
                 buffer_size=FETCH_BUFFER_SIZE_BYTES,
                 max_buffer_size=MAX_FETCH_BUFFER_SIZE_BYTES,
                 iter_timeout=None):
        super(SimpleConsumer, self).__init__(
            client, group, topic,
            partitions=partitions,
            auto_commit=auto_commit,
            auto_commit_every_n=auto_commit_every_n,
            auto_commit_every_t=auto_commit_every_t)

        if max_buffer_size is not None and buffer_size > max_buffer_size:
            raise ValueError("buffer_size (%d) is greater than "
                             "max_buffer_size (%d)" %
                             (buffer_size, max_buffer_size))
        self.buffer_size = buffer_size
        self.max_buffer_size = max_buffer_size
        self.partition_info = False     # Do not return partition info in msgs
        self.fetch_max_wait_time = FETCH_MAX_WAIT_TIME
        self.fetch_min_bytes = fetch_size_bytes
        self.fetch_offsets = self.offsets.copy()
        self.iter_timeout = iter_timeout
        self.queue = Queue()

    def __repr__(self):
        return '<SimpleConsumer group=%s, topic=%s, partitions=%s>' % \
            (self.group, self.topic, str(self.offsets.keys()))

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
                else:
                    pass

            resps = self.client.send_offset_request(reqs)
            for resp in resps:
                self.offsets[resp.partition] = \
                    resp.offsets[0] + deltas[resp.partition]
        else:
            raise ValueError("Unexpected value for `whence`, %d" % whence)

        # Reset queue and fetch offsets since they are invalid
        self.fetch_offsets = self.offsets.copy()
        if self.auto_commit:
            self.count_since_commit += 1
            self.commit()

        self.queue = Queue()

    def get_messages(self, count=1, block=True, timeout=0.1):
        """
        Fetch the specified number of messages

        count: Indicates the maximum number of messages to be fetched
        block: If True, the API will block till some messages are fetched.
        timeout: If block is True, the function will block for the specified
                 time (in seconds) until count messages is fetched. If None,
                 it will block forever.
        """
        messages = []
        if timeout is not None:
            max_time = time.time() + timeout

        new_offsets = {}
        while count > 0 and (timeout is None or timeout > 0):
            result = self._get_message(block, timeout, get_partition_info=True,
                                       update_offset=False)
            if result:
                partition, message = result
                if self.partition_info:
                    messages.append(result)
                else:
                    messages.append(message)
                new_offsets[partition] = message.offset + 1
                count -= 1
            else:
                # Ran out of messages for the last request.
                if not block:
                    # If we're not blocking, break.
                    break

            # If we have a timeout, reduce it to the
            # appropriate value
            if timeout is not None:
                timeout = max_time - time.time()

        # Update and commit offsets if necessary
        self.offsets.update(new_offsets)
        self.count_since_commit += len(messages)
        self._auto_commit()
        return messages

    def get_message(self, block=True, timeout=0.1, get_partition_info=None):
        return self._get_message(block, timeout, get_partition_info)

    def _get_message(self, block=True, timeout=0.1, get_partition_info=None,
                     update_offset=True):
        """
        If no messages can be fetched, returns None.
        If get_partition_info is None, it defaults to self.partition_info
        If get_partition_info is True, returns (partition, message)
        If get_partition_info is False, returns message
        """
        if self.queue.empty():
            # We're out of messages, go grab some more.
            with FetchContext(self, block, timeout):
                self._fetch()
        try:
            partition, message = self.queue.get_nowait()

            if update_offset:
                # Update partition offset
                self.offsets[partition] = message.offset + 1

                # Count, check and commit messages if necessary
                self.count_since_commit += 1
                self._auto_commit()

            if get_partition_info is None:
                get_partition_info = self.partition_info
            if get_partition_info:
                return partition, message
            else:
                return message
        except Empty:
            return None

    def __iter__(self):
        if self.iter_timeout is None:
            timeout = ITER_TIMEOUT_SECONDS
        else:
            timeout = self.iter_timeout

        while True:
            message = self.get_message(True, timeout)
            if message:
                yield message
            elif self.iter_timeout is None:
                # We did not receive any message yet but we don't have a
                # timeout, so give up the CPU for a while before trying again
                time.sleep(NO_MESSAGES_WAIT_TIME_SECONDS)
            else:
                # Timed out waiting for a message
                break

    def _fetch(self):
        # Create fetch request payloads for all the partitions
        partitions = dict((p, self.buffer_size)
                      for p in self.fetch_offsets.keys())
        while partitions:
            requests = []
            for partition, buffer_size in six.iteritems(partitions):
                requests.append(FetchRequest(self.topic, partition,
                                             self.fetch_offsets[partition],
                                             buffer_size))
            # Send request
            responses = self.client.send_fetch_request(
                requests,
                max_wait_time=int(self.fetch_max_wait_time),
                min_bytes=self.fetch_min_bytes)

            retry_partitions = {}
            for resp in responses:
                partition = resp.partition
                buffer_size = partitions[partition]
                try:
                    for message in resp.messages:
                        # Put the message in our queue
                        self.queue.put((partition, message))
                        self.fetch_offsets[partition] = message.offset + 1
                except ConsumerFetchSizeTooSmall:
                    if (self.max_buffer_size is not None and
                            buffer_size == self.max_buffer_size):
                        log.error("Max fetch size %d too small",
                                  self.max_buffer_size)
                        raise
                    if self.max_buffer_size is None:
                        buffer_size *= 2
                    else:
                        buffer_size = min(buffer_size * 2,
                                          self.max_buffer_size)
                    log.warn("Fetch size too small, increase to %d (2x) "
                             "and retry", buffer_size)
                    retry_partitions[partition] = buffer_size
                except ConsumerNoMoreData as e:
                    log.debug("Iteration was ended by %r", e)
                except StopIteration:
                    # Stop iterating through this partition
                    log.debug("Done iterating over partition %s" % partition)
            partitions = retry_partitions
