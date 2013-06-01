from itertools import izip_longest, repeat
import logging
from threading import Lock

from kafka.common import (
    ErrorMapping, FetchRequest,
    OffsetRequest, OffsetFetchRequest, OffsetCommitRequest
)

from kafka.util import (
    ReentrantTimer
)

log = logging.getLogger("kafka")

AUTO_COMMIT_MSG_COUNT = 100
AUTO_COMMIT_INTERVAL = 5000

class SimpleConsumer(object):
    """
    A simple consumer implementation that consumes all partitions for a topic

    client: a connected KafkaClient
    group: a name for this consumer, used for offset storage and must be unique
    topic: the topic to consume

    auto_commit: default True. Whether or not to auto commit the offsets
    auto_commit_every_n: default 100. How many messages to consume before a commit
    auto_commit_every_t: default 5000. How much time (in milliseconds) to wait before commit

    Auto commit details:
    If both auto_commit_every_n and auto_commit_every_t are set, they will reset one another
    when one is triggered. These triggers simply call the commit method on this class. A 
    manual call to commit will also reset these triggers

    """
    def __init__(self, client, group, topic, auto_commit=True,
                 auto_commit_every_n=AUTO_COMMIT_MSG_COUNT,
                 auto_commit_every_t=AUTO_COMMIT_INTERVAL):
        self.client = client
        self.topic = topic
        self.group = group
        self.client._load_metadata_for_topics(topic)
        self.offsets = {}

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
                                               self._timed_commit)
            self.commit_timer.start()

        def get_or_init_offset_callback(resp):
            if resp.error == ErrorMapping.NO_ERROR:
                return resp.offset
            elif resp.error == ErrorMapping.UNKNOWN_TOPIC_OR_PARTITON:
                return 0
            else:
                raise Exception("OffsetFetchRequest for topic=%s, partition=%d failed with errorcode=%s" % (
                    resp.topic, resp.partition, resp.error))

        # Uncomment for 0.8.1
        #
        #for partition in self.client.topic_partitions[topic]:
        #    req = OffsetFetchRequest(topic, partition)
        #    (offset,) = self.client.send_offset_fetch_request(group, [req],
        #                  callback=get_or_init_offset_callback, fail_on_error=False)
        #    self.offsets[partition] = offset
        
        for partition in self.client.topic_partitions[topic]:
            self.offsets[partition] = 0

    def stop(self):
        if self.commit_timer is not None:
            self.commit_timer.stop()
            self.commit()

    def seek(self, offset, whence):
        """
        Alter the current offset in the consumer, similar to fseek

        offset: how much to modify the offset
        whence: where to modify it from
                0 is relative to the earliest available offset (head)
                1 is relative to the current offset
                2 is relative to the latest known offset (tail)
        """
        if whence == 1: # relative to current position
            for partition, _offset in self.offsets.items():
                self.offsets[partition] = _offset + offset
        elif whence in (0, 2): # relative to beginning or end
            # divide the request offset by number of partitions, distribute the remained evenly
            (delta, rem) = divmod(offset, len(self.offsets))
            deltas = {}
            for partition, r in izip_longest(self.offsets.keys(), repeat(1, rem), fillvalue=0):
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
                self.offsets[resp.partition] = resp.offsets[0] + deltas[resp.partition] 
        else:
            raise ValueError("Unexpected value for `whence`, %d" % whence)

    def pending(self, partitions=[]):
        """
        Gets the pending message count

        partitions: list of partitions to check for, default is to check all
        """
        if len(partitions) == 0:
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

    def _timed_commit(self):
        """
        Commit offsets as part of timer
        """
        self.commit()

        # Once the commit is done, start the timer again
        self.commit_timer.start()

    def commit(self, partitions=[]):
        """
        Commit offsets for this consumer

        partitions: list of partitions to commit, default is to commit all of them
        """


        # short circuit if nothing happened
        if self.count_since_commit == 0:
            return

        with self.commit_lock:
            reqs = []
            if len(partitions) == 0: # commit all partitions
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

    def __iter__(self):
        """
        Create an iterate per partition. Iterate through them calling next() until they are
        all exhausted.
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
                    yield it.next()
                except StopIteration:
                    log.debug("Done iterating over partition %s" % partition)
                    del iters[partition]
                    continue # skip auto-commit since we didn't yield anything

                # auto commit logic
                self.count_since_commit += 1
                if self.auto_commit is True:
                    if self.auto_commit_every_n is not None and self.count_since_commit > self.auto_commit_every_n:
                        if self.commit_timer is not None:
                            self.commit_timer.stop()
                            self.commit()
                            self.commit_timer.start()
                        else:
                            self.commit()

    def __iter_partition__(self, partition, offset):
        """
        Iterate over the messages in a partition. Create a FetchRequest to get back
        a batch of messages, yield them one at a time. After a batch is exhausted,
        start a new batch unless we've reached the end of ths partition.
        """

        while True:
            req = FetchRequest(self.topic, partition, offset, 1024) # TODO configure fetch size
            (resp,) = self.client.send_fetch_request([req])
            assert resp.topic == self.topic
            assert resp.partition == partition
            next_offset = None
            for message in resp.messages:
                next_offset = message.offset
                yield message
                # update the internal state _after_ we yield the message
                self.offsets[partition] = message.offset
            if next_offset is None:
                break
            else:
                offset = next_offset + 1
