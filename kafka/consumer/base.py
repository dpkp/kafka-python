from __future__ import absolute_import

import logging
import numbers
from threading import Lock

import kafka.common
from kafka.common import (
    OffsetRequest, OffsetCommitRequest, OffsetFetchRequest,
    UnknownTopicOrPartitionError
)

from kafka.util import ReentrantTimer

log = logging.getLogger("kafka")

AUTO_COMMIT_MSG_COUNT = 100
AUTO_COMMIT_INTERVAL = 5000

FETCH_DEFAULT_BLOCK_TIMEOUT = 1
FETCH_MAX_WAIT_TIME = 100
FETCH_MIN_BYTES = 4096
FETCH_BUFFER_SIZE_BYTES = 4096
MAX_FETCH_BUFFER_SIZE_BYTES = FETCH_BUFFER_SIZE_BYTES * 8

ITER_TIMEOUT_SECONDS = 60
NO_MESSAGES_WAIT_TIME_SECONDS = 0.1


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
        self.client.load_metadata_for_topics(topic)
        self.offsets = {}

        if not partitions:
            partitions = self.client.get_partition_ids_for_topic(topic)
        else:
            assert all(isinstance(x, numbers.Integral) for x in partitions)

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

        if auto_commit:
            self.fetch_last_known_offsets(partitions)
        else:
            for partition in partitions:
                self.offsets[partition] = 0

    def fetch_last_known_offsets(self, partitions=None):
        if not partitions:
            partitions = self.client.get_partition_ids_for_topic(self.topic)

        def get_or_init_offset(resp):
            try:
                kafka.common.check_error(resp)
                return resp.offset
            except UnknownTopicOrPartitionError:
                return 0

        for partition in partitions:
            req = OffsetFetchRequest(self.topic, partition)
            (resp,) = self.client.send_offset_fetch_request(self.group, [req],
                          fail_on_error=False)
            self.offsets[partition] = get_or_init_offset(resp)
        self.fetch_offsets = self.offsets.copy()

    def commit(self, partitions=None):
        """
        Commit offsets for this consumer

        Keyword Arguments:
            partitions (list): list of partitions to commit, default is to commit
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
                kafka.common.check_error(resp)

            self.count_since_commit = 0

    def _auto_commit(self):
        """
        Check if we have to commit based on number of messages and commit
        """

        # Check if we are supposed to do an auto-commit
        if not self.auto_commit or self.auto_commit_every_n is None:
            return

        if self.count_since_commit >= self.auto_commit_every_n:
            self.commit()

    def stop(self):
        if self.commit_timer is not None:
            self.commit_timer.stop()
            self.commit()

    def pending(self, partitions=None):
        """
        Gets the pending message count

        Keyword Arguments:
            partitions (list): list of partitions to check for, default is to check all
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
            total += pending - offset

        return total
