"""
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
"""
from __future__ import absolute_import

from collections import deque

from kafka.consumer.fetcher import ConsumerRecord
import kafka.errors as Errors


class MinTimestampTracker(object):
    """MinTimestampTracker maintains the min timestamp of timestamped elements."""
    def __init__(self):
        self.descending_subsequence = deque()

        # in the case that incoming traffic is very small, the records maybe
        # put and polled within a single iteration, in this case we need to
        # remember the last polled record's timestamp
        self.last_known_time = -1

    def add_element(self, elem):
        if elem is None:
            raise ValueError('elem must not be None')

        while self.descending_subsequence:
            min_elem = self.descending_subsequence[-1]
            if min_elem[0] < elem[0]:
                break
            self.descending_subsequence.pop()
        self.descending_subsequence.append(elem)

    def remove_element(self, elem):
        if elem is not None:
            if self.descending_subsequence:
                if self.descending_subsequence[0] == elem:
                    self.descending_subsequence.popleft()

        if not self.descending_subsequence:
            self.last_known_time = elem[0]

    def size(self):
        return len(self.descending_subsequence)

    def get(self):
        if not self.descending_subsequence:
            return self.last_known_time
        return self.descending_subsequence[0][0]


class RecordQueue(object):
    """
    RecordQueue is a FIFO queue of (timestamp, ConsumerRecord).
    It also keeps track of the partition timestamp defined as the minimum
    timestamp of records in its queue; in addition, its partition timestamp
    is monotonically increasing such that once it is advanced, it will not be
    decremented.
    """

    def __init__(self, partition, source):
        self.partition = partition
        self.source = source

        self.fifo_queue = deque()
        self.time_tracker = MinTimestampTracker()

        self._partition_time = -1

    def add_raw_records(self, raw_records, timestamp_extractor):
        """Add a batch of ConsumerRecord into the queue

        Arguments:
            raw_records (list of ConsumerRecord): the raw records
            timestamp_extractor (callable): given a record, return a timestamp

        Returns: the size of this queue
        """
        for raw_record in raw_records:
            # deserialize the raw record, extract the timestamp and put into the queue
            key = self.source.deserialize_key(raw_record.topic, raw_record.key)
            value = self.source.deserialize_value(raw_record.topic, raw_record.value)

            record = ConsumerRecord(raw_record.topic,
                                    raw_record.partition,
                                    raw_record.offset,
                                    raw_record.timestamp,
                                    0, # TimestampType.CREATE_TIME,
                                    key, value,
                                    raw_record.checksum,
                                    raw_record.serialized_key_size,
                                    raw_record.serialized_value_size)

            timestamp = timestamp_extractor(record)

            # validate that timestamp must be non-negative
            if timestamp < 0:
                raise Errors.StreamsError('Extracted timestamp value is negative, which is not allowed.')

            stamped_record = (timestamp, record)

            self.fifo_queue.append(stamped_record)
            self.time_tracker.add_element(stamped_record)

        # update the partition timestamp if its currently
        # tracked min timestamp has exceeded its value; this will
        # usually only take effect for the first added batch
        timestamp = self.time_tracker.get()

        if timestamp > self._partition_time:
            self._partition_time = timestamp

        return self.size()

    def poll(self):
        """Get the next Record from the queue

        Returns: (timestamp, record)
        """
        if not self.fifo_queue:
            return None, None

        elem = self.fifo_queue.popleft()
        self.time_tracker.remove_element(elem)

        # only advance the partition timestamp if its currently
        # tracked min timestamp has exceeded its value
        timestamp = self.time_tracker.get()

        if timestamp > self._partition_time:
            self._partition_time = timestamp

        return elem

    def size(self):
        """Returns the number of records in the queue

        Returns: the number of records
        """
        return len(self.fifo_queue)

    def is_empty(self):
        """Tests if the queue is empty

        Returns: True if the queue is empty, otherwise False
        """
        return not bool(self.fifo_queue)

    def timestamp(self):
        """Returns the tracked partition timestamp

        Returns: timestamp
        """
        return self._partition_time
