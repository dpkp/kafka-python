from __future__ import absolute_import

import collections
import io
import threading
import time

from ..codec import (has_gzip, has_snappy, has_lz4,
                     gzip_encode, snappy_encode,
                     lz4_encode, lz4_encode_old_kafka)
from .. import errors as Errors
from ..protocol.types import Int32, Int64
from ..protocol.message import MessageSet, Message



class MessageSetBuffer(object):
    """Wrap a buffer for writing MessageSet batches.

    Arguments:
        buf (IO stream): a buffer for writing data. Typically BytesIO.
        batch_size (int): maximum number of bytes to write to the buffer.

    Keyword Arguments:
        compression_type ('gzip', 'snappy', None): compress messages before
            publishing. Default: None.
    """
    _COMPRESSORS = {
        'gzip': (has_gzip, gzip_encode, Message.CODEC_GZIP),
        'snappy': (has_snappy, snappy_encode, Message.CODEC_SNAPPY),
        'lz4': (has_lz4, lz4_encode, Message.CODEC_LZ4),
        'lz4-old-kafka': (has_lz4, lz4_encode_old_kafka, Message.CODEC_LZ4),
    }
    def __init__(self, buf, batch_size, compression_type=None, message_version=0):
        if compression_type is not None:
            assert compression_type in self._COMPRESSORS, 'Unrecognized compression type'

            # Kafka 0.8/0.9 had a quirky lz4...
            if compression_type == 'lz4' and message_version == 0:
                compression_type = 'lz4-old-kafka'

            checker, encoder, attributes = self._COMPRESSORS[compression_type]
            assert checker(), 'Compression Libraries Not Found'
            self._compressor = encoder
            self._compression_attributes = attributes
        else:
            self._compressor = None
            self._compression_attributes = None

        self._message_version = message_version
        self._buffer = buf
        # Init MessageSetSize to 0 -- update on close
        self._buffer.seek(0)
        self._buffer.write(Int32.encode(0))
        self._batch_size = batch_size
        self._closed = False
        self._messages = 0

    def append(self, offset, message):
        """Apend a Message to the MessageSet.

        Arguments:
            offset (int): offset of the message
            message (Message or bytes): message struct or encoded bytes
        """
        if isinstance(message, Message):
            encoded = message.encode()
        else:
            encoded = bytes(message)
        msg = Int64.encode(offset) + Int32.encode(len(encoded)) + encoded
        self._buffer.write(msg)
        self._messages += 1

    def has_room_for(self, key, value):
        if self._closed:
            return False
        if not self._messages:
            return True
        needed_bytes = MessageSet.HEADER_SIZE + Message.HEADER_SIZE
        if key is not None:
            needed_bytes += len(key)
        if value is not None:
            needed_bytes += len(value)
        return self._buffer.tell() + needed_bytes < self._batch_size

    def is_full(self):
        if self._closed:
            return True
        return self._buffer.tell() >= self._batch_size

    def close(self):
        if self._compressor:
            # TODO: avoid copies with bytearray / memoryview
            self._buffer.seek(4)
            msg = Message(self._compressor(self._buffer.read()),
                          attributes=self._compression_attributes,
                          magic=self._message_version)
            encoded = msg.encode()
            self._buffer.seek(4)
            self._buffer.write(Int64.encode(0)) # offset 0 for wrapper msg
            self._buffer.write(Int32.encode(len(encoded)))
            self._buffer.write(encoded)

        # Update the message set size, and return ready for full read()
        size = self._buffer.tell() - 4
        self._buffer.seek(0)
        self._buffer.write(Int32.encode(size))
        self._buffer.seek(0)
        self._closed = True

    def size_in_bytes(self):
        return self._buffer.tell()

    def buffer(self):
        return self._buffer


class SimpleBufferPool(object):
    """A simple pool of BytesIO objects with a weak memory ceiling."""
    def __init__(self, memory, poolable_size):
        """Create a new buffer pool.

        Arguments:
            memory (int): maximum memory that this buffer pool can allocate
            poolable_size (int): memory size per buffer to cache in the free
                list rather than deallocating
        """
        self._poolable_size = poolable_size
        self._lock = threading.RLock()

        buffers = int(memory / poolable_size) if poolable_size else 0
        self._free = collections.deque([io.BytesIO() for _ in range(buffers)])

        self._waiters = collections.deque()
        #self.metrics = metrics;
        #self.waitTime = this.metrics.sensor("bufferpool-wait-time");
        #MetricName metricName = metrics.metricName("bufferpool-wait-ratio", metricGrpName, "The fraction of time an appender waits for space allocation.");
        #this.waitTime.add(metricName, new Rate(TimeUnit.NANOSECONDS));

    def allocate(self, size, max_time_to_block_ms):
        """
        Allocate a buffer of the given size. This method blocks if there is not
        enough memory and the buffer pool is configured with blocking mode.

        Arguments:
            size (int): The buffer size to allocate in bytes [ignored]
            max_time_to_block_ms (int): The maximum time in milliseconds to
                block for buffer memory to be available

        Returns:
            io.BytesIO
        """
        with self._lock:
            # check if we have a free buffer of the right size pooled
            if self._free:
                return self._free.popleft()

            elif self._poolable_size == 0:
                return io.BytesIO()

            else:
                # we are out of buffers and will have to block
                buf = None
                more_memory = threading.Condition(self._lock)
                self._waiters.append(more_memory)
                # loop over and over until we have a buffer or have reserved
                # enough memory to allocate one
                while buf is None:
                    start_wait = time.time()
                    more_memory.wait(max_time_to_block_ms / 1000.0)
                    end_wait = time.time()
                    #this.waitTime.record(endWait - startWait, time.milliseconds());

                    if self._free:
                        buf = self._free.popleft()
                    else:
                        raise Errors.KafkaTimeoutError(
                            "Failed to allocate memory within the configured"
                            " max blocking time")

                # remove the condition for this thread to let the next thread
                # in line start getting memory
                removed = self._waiters.popleft()
                assert removed is more_memory, 'Wrong condition'

                # signal any additional waiters if there is more memory left
                # over for them
                if self._free and self._waiters:
                    self._waiters[0].notify()

                # unlock and return the buffer
                return buf

    def deallocate(self, buf):
        """
        Return buffers to the pool. If they are of the poolable size add them
        to the free list, otherwise just mark the memory as free.

        Arguments:
            buffer_ (io.BytesIO): The buffer to return
        """
        with self._lock:
            # BytesIO.truncate here makes the pool somewhat pointless
            # but we stick with the BufferPool API until migrating to
            # bytesarray / memoryview. The buffer we return must not
            # expose any prior data on read().
            buf.truncate(0)
            self._free.append(buf)
            if self._waiters:
                self._waiters[0].notify()

    def queued(self):
        """The number of threads blocked waiting on memory."""
        with self._lock:
            return len(self._waiters)

'''
class BufferPool(object):
    """
    A pool of ByteBuffers kept under a given memory limit. This class is fairly
    specific to the needs of the producer. In particular it has the following
    properties:

    * There is a special "poolable size" and buffers of this size are kept in a
      free list and recycled
    * It is fair. That is all memory is given to the longest waiting thread
      until it has sufficient memory. This prevents starvation or deadlock when
      a thread asks for a large chunk of memory and needs to block until
      multiple buffers are deallocated.
    """
    def __init__(self, memory, poolable_size):
        """Create a new buffer pool.

        Arguments:
            memory (int): maximum memory that this buffer pool can allocate
            poolable_size (int): memory size per buffer to cache in the free
                list rather than deallocating
        """
        self._poolable_size = poolable_size
        self._lock = threading.RLock()
        self._free = collections.deque()
        self._waiters = collections.deque()
        self._total_memory = memory
        self._available_memory = memory
        #self.metrics = metrics;
        #self.waitTime = this.metrics.sensor("bufferpool-wait-time");
        #MetricName metricName = metrics.metricName("bufferpool-wait-ratio", metricGrpName, "The fraction of time an appender waits for space allocation.");
        #this.waitTime.add(metricName, new Rate(TimeUnit.NANOSECONDS));

    def allocate(self, size, max_time_to_block_ms):
        """
        Allocate a buffer of the given size. This method blocks if there is not
        enough memory and the buffer pool is configured with blocking mode.

        Arguments:
            size (int): The buffer size to allocate in bytes
            max_time_to_block_ms (int): The maximum time in milliseconds to
                block for buffer memory to be available

        Returns:
            buffer

        Raises:
            InterruptedException If the thread is interrupted while blocked
            IllegalArgumentException if size is larger than the total memory
                controlled by the pool (and hence we would block forever)
        """
        assert size <= self._total_memory, (
            "Attempt to allocate %d bytes, but there is a hard limit of %d on"
            " memory allocations." % (size, self._total_memory))

        with self._lock:
            # check if we have a free buffer of the right size pooled
            if (size == self._poolable_size and len(self._free) > 0):
                return self._free.popleft()

            # now check if the request is immediately satisfiable with the
            # memory on hand or if we need to block
            free_list_size = len(self._free) * self._poolable_size
            if self._available_memory + free_list_size >= size:
                # we have enough unallocated or pooled memory to immediately
                # satisfy the request
                self._free_up(size)
                self._available_memory -= size
                raise NotImplementedError()
                #return ByteBuffer.allocate(size)
            else:
                # we are out of memory and will have to block
                accumulated = 0
                buf = None
                more_memory = threading.Condition(self._lock)
                self._waiters.append(more_memory)
                # loop over and over until we have a buffer or have reserved
                # enough memory to allocate one
                while (accumulated < size):
                    start_wait = time.time()
                    if not more_memory.wait(max_time_to_block_ms / 1000.0):
                        raise Errors.KafkaTimeoutError(
                            "Failed to allocate memory within the configured"
                            " max blocking time")
                    end_wait = time.time()
                    #this.waitTime.record(endWait - startWait, time.milliseconds());

                    # check if we can satisfy this request from the free list,
                    # otherwise allocate memory
                    if (accumulated == 0
                        and size == self._poolable_size
                        and self._free):

                        # just grab a buffer from the free list
                        buf = self._free.popleft()
                        accumulated = size
                    else:
                        # we'll need to allocate memory, but we may only get
                        # part of what we need on this iteration
                        self._free_up(size - accumulated)
                        got = min(size - accumulated, self._available_memory)
                        self._available_memory -= got
                        accumulated += got

                # remove the condition for this thread to let the next thread
                # in line start getting memory
                removed = self._waiters.popleft()
                assert removed is more_memory, 'Wrong condition'

                # signal any additional waiters if there is more memory left
                # over for them
                if (self._available_memory > 0 or len(self._free) > 0):
                    if len(self._waiters) > 0:
                        self._waiters[0].notify()

                # unlock and return the buffer
                if buf is None:
                    raise NotImplementedError()
                    #return ByteBuffer.allocate(size)
                else:
                    return buf

    def _free_up(self, size):
        """
        Attempt to ensure we have at least the requested number of bytes of
        memory for allocation by deallocating pooled buffers (if needed)
        """
        while self._free and self._available_memory < size:
            self._available_memory += self._free.pop().capacity

    def deallocate(self, buffer_, size=None):
        """
        Return buffers to the pool. If they are of the poolable size add them
        to the free list, otherwise just mark the memory as free.

        Arguments:
            buffer (io.BytesIO): The buffer to return
            size (int): The size of the buffer to mark as deallocated, note
                that this maybe smaller than buffer.capacity since the buffer
                may re-allocate itself during in-place compression
        """
        with self._lock:
            if size is None:
                size = buffer_.capacity
            if (size == self._poolable_size and size == buffer_.capacity):
                buffer_.seek(0)
                buffer_.truncate()
                self._free.append(buffer_)
            else:
                self._available_memory += size

            if self._waiters:
                more_mem = self._waiters[0]
                more_mem.notify()

    def available_memory(self):
        """The total free memory both unallocated and in the free list."""
        with self._lock:
            return self._available_memory + len(self._free) * self._poolable_size

    def unallocated_memory(self):
        """Get the unallocated memory (not in the free list or in use)."""
        with self._lock:
            return self._available_memory

    def queued(self):
        """The number of threads blocked waiting on memory."""
        with self._lock:
            return len(self._waiters)

    def poolable_size(self):
        """The buffer size that will be retained in the free list after use."""
        return self._poolable_size

    def total_memory(self):
        """The total memory managed by this pool."""
        return self._total_memory
'''
