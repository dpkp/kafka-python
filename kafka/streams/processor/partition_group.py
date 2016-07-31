"""
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
"""
from __future__ import absolute_import

import collections
import heapq

import kafka.errors as Errors
from kafka.structs import TopicPartition
from .record_queue import RecordQueue


TaskId = collections.namedtuple('TaskId', 'topic_group_id partition_id')


class RecordInfo(object):
    def __init__(self):
        self.queue = RecordQueue()

    def node(self):
        return self.queue.source()

    def partition(self):
        return self.queue.partition()

    def queue(self):
        return self.queue


class PartitionGroup(object):
    """A PartitionGroup is composed from a set of partitions. It also maintains
    the timestamp of this group, hence the associated task as the min timestamp
    across all partitions in the group.
    """
    def __init__(self, partition_queues, timestamp_extractor):
        self._queues_by_time = [] # heapq
        self._partition_queues = partition_queues
        self._timestamp_extractor = timestamp_extractor
        self._total_buffered = 0

    def next_record(self, record_info):
        """Get the next record and queue

        Returns: (timestamp, ConsumerRecord)
        """
        record = None

        if self._queues_by_time:
            _, queue = heapq.heappop(self._queues_by_time)

            # get the first record from this queue.
            record = queue.poll()

            if queue:
                heapq.heappush(self._queues_by_time, (queue.timestamp(), queue))

        record_info.queue = queue

        if record:
            self._total_buffered -= 1

        return record

    def add_raw_records(self, partition, raw_records):
        """Adds raw records to this partition group

        Arguments:
            partition (TopicPartition): the partition
            raw_records (list of ConsumerRecord): the raw records

        Returns: the queue size for the partition
        """
        record_queue = self._partition_queues[partition]

        old_size = record_queue.size()
        new_size = record_queue.add_raw_records(raw_records, self._timestamp_extractor)

        # add this record queue to be considered for processing in the future
        # if it was empty before
        if old_size == 0 and new_size > 0:
            heapq.heappush(self._queues_by_time, (record_queue.timestamp(), record_queue))

        self._total_buffered += new_size - old_size

        return new_size

    def partitions(self):
        return set(self._partition_queues.keys())

    def timestamp(self):
        """Return the timestamp of this partition group
        as the smallest partition timestamp among all its partitions
        """
        # we should always return the smallest timestamp of all partitions
        # to avoid group partition time goes backward
        timestamp = float('inf')
        for queue in self._partition_queues.values():
            if timestamp > queue.timestamp():
                timestamp = queue.timestamp()
        return timestamp

    def num_buffered(self, partition=None):
        if partition is None:
            return self._total_buffered
        record_queue = self._partition_queues.get(partition)
        if not record_queue:
            raise Errors.IllegalStateError('Record partition does not belong to this partition-group.')
        return record_queue.size()

    def top_queue_size(self):
        if not self._queues_by_time:
            return 0
        return self._queues_by_time[0].size()

    def close(self):
        self._queues_by_time = []
        self._partition_queues.clear()


def partition_grouper(topic_groups, metadata):
    """Assign partitions to task/topic groups

    Arguments:
        topic_groups ({topic_group_id: [topics]})
        metadata (kafka.Cluster)

    Returns: {TaskId: set([TopicPartition])}
    """
    groups = {}
    for topic_group_id, topic_group in topic_groups.items():

        partitions = set()
        for topic in topic_group:
            partitions.update(metadata.partitions_for_topic(topic))

        for partition_id in partitions:
            group = set()

            for topic in topic_group:
                if partition_id in metadata.partitions_for_topic(topic):
                    group.add(TopicPartition(topic, partition_id))
            groups[TaskId(topic_group_id, partition_id)] = group

    return groups
