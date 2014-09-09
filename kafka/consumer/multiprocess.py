from __future__ import absolute_import

import logging
import time
from multiprocessing import Process, Queue as MPQueue, Event, Value

try:
    from Queue import Empty
except ImportError:  # python 2
    from queue import Empty

from .base import (
    AUTO_COMMIT_MSG_COUNT, AUTO_COMMIT_INTERVAL,
    NO_MESSAGES_WAIT_TIME_SECONDS
)
from .simple import Consumer, SimpleConsumer

log = logging.getLogger("kafka")


def _mp_consume(client, group, topic, chunk, queue, start, exit, pause, size):
    """
    A child process worker which consumes messages based on the
    notifications given by the controller process

    NOTE: Ideally, this should have been a method inside the Consumer
    class. However, multiprocessing module has issues in windows. The
    functionality breaks unless this function is kept outside of a class
    """

    # Make the child processes open separate socket connections
    client.reinit()

    # We will start consumers without auto-commit. Auto-commit will be
    # done by the master controller process.
    consumer = SimpleConsumer(client, group, topic,
                              partitions=chunk,
                              auto_commit=False,
                              auto_commit_every_n=None,
                              auto_commit_every_t=None)

    # Ensure that the consumer provides the partition information
    consumer.provide_partition_info()

    while True:
        # Wait till the controller indicates us to start consumption
        start.wait()

        # If we are asked to quit, do so
        if exit.is_set():
            break

        # Consume messages and add them to the queue. If the controller
        # indicates a specific number of messages, follow that advice
        count = 0

        message = consumer.get_message()
        if message:
            queue.put(message)
            count += 1

            # We have reached the required size. The controller might have
            # more than what he needs. Wait for a while.
            # Without this logic, it is possible that we run into a big
            # loop consuming all available messages before the controller
            # can reset the 'start' event
            if count == size.value:
                pause.wait()

        else:
            # In case we did not receive any message, give up the CPU for
            # a while before we try again
            time.sleep(NO_MESSAGES_WAIT_TIME_SECONDS)

    consumer.stop()


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
        super(MultiProcessConsumer, self).__init__(
            client, group, topic,
            partitions=None,
            auto_commit=auto_commit,
            auto_commit_every_n=auto_commit_every_n,
            auto_commit_every_t=auto_commit_every_t)

        # Variables for managing and controlling the data flow from
        # consumer child process to master
        self.queue = MPQueue(1024)  # Child consumers dump messages into this
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
            args = (client.copy(),
                    group, topic, list(chunk),
                    self.queue, self.start, self.exit,
                    self.pause, self.size)

            proc = Process(target=_mp_consume, args=args)
            proc.daemon = True
            proc.start()
            self.procs.append(proc)

    def __repr__(self):
        return '<MultiProcessConsumer group=%s, topic=%s, consumers=%d>' % \
            (self.group, self.topic, len(self.procs))

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
            self.offsets[partition] = message.offset + 1
            self.start.clear()
            self.count_since_commit += 1
            self._auto_commit()
            yield message

        self.start.clear()

    def get_messages(self, count=1, block=True, timeout=10):
        """
        Fetch the specified number of messages

        count: Indicates the maximum number of messages to be fetched
        block: If True, the API will block till some messages are fetched.
        timeout: If block is True, the function will block for the specified
                 time (in seconds) until count messages is fetched. If None,
                 it will block forever.
        """
        messages = []

        # Give a size hint to the consumers. Each consumer process will fetch
        # a maximum of "count" messages. This will fetch more messages than
        # necessary, but these will not be committed to kafka. Also, the extra
        # messages can be provided in subsequent runs
        self.size.value = count
        self.pause.clear()

        if timeout is not None:
            max_time = time.time() + timeout

        new_offsets = {}
        while count > 0 and (timeout is None or timeout > 0):
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
            new_offsets[partition] = message.offset + 1
            count -= 1
            if timeout is not None:
                timeout = max_time - time.time()

        self.size.value = 0
        self.start.clear()
        self.pause.set()

        # Update and commit offsets if necessary
        self.offsets.update(new_offsets)
        self.count_since_commit += len(messages)
        self._auto_commit()

        return messages
