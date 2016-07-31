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

import abc
import logging
import threading

from kafka.consumer.fetcher import ConsumerRecord
import kafka.errors as Errors
from kafka.streams.errors import ProcessorStateError
from kafka.structs import OffsetAndMetadata
from .context import ProcessorContext
from .partition_group import PartitionGroup, RecordInfo
from .punctuation import PunctuationQueue
from .record_collector import RecordCollector
from .record_queue import RecordQueue

log = logging.getLogger(__name__)

NONEXIST_TOPIC = '__null_topic__'
DUMMY_RECORD = ConsumerRecord(NONEXIST_TOPIC, -1, -1, -1, -1, None, None, -1, -1, -1)


class AbstractTask(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, task_id, partitions, topology,
                 consumer, restore_consumer, is_standby, **config):
        """Raises ProcessorStateError if the state manager cannot be created"""
        self.id = task_id
        self.application_id = config['application_id']
        self.partitions = set(partitions)
        self.topology = topology
        self.consumer = consumer
        self.processor_context = None
        self.state_mgr = None

        # create the processor state manager
        """
        try:
            File applicationStateDir = StreamThread.makeStateDir(applicationId, config.getString(StreamsConfig.STATE_DIR_CONFIG));
            File stateFile = new File(applicationStateDir.getCanonicalPath(), id.toString());
            # if partitions is null, this is a standby task
            self.state_mgr = ProcessorStateManager(applicationId, id.partition, partitions, stateFile, restoreConsumer, isStandby);
        except Exception as e:
            raise ProcessorStateError('Error while creating the state manager', e)
        """

    def initialize_state_stores(self):
        # set initial offset limits
        self.initialize_offset_limits()

        for state_store_supplier in self.topology.state_store_suppliers():
            store = state_store_supplier.get()
            store.init(self.processor_context, store)

    @abc.abstractmethod
    def commit(self):
        pass

    def close(self):
        """Close the task.

        Raises ProcessorStateError if there is an error while closing the state manager
        """
        try:
            if self.state_mgr is not None:
                self.state_mgr.close(self.record_collector_offsets())
        except Exception as e:
            raise ProcessorStateError('Error while closing the state manager', e)

    def record_collector_offsets(self):
        return {}

    def initialize_offset_limits(self):
        for partition in self.partitions:
            metadata = self.consumer.committed(partition) # TODO: batch API?
            self.state_mgr.put_offset_limit(partition, metadata.offset if metadata else 0)


class StreamTask(AbstractTask):
    """A StreamTask is associated with a PartitionGroup,
    and is assigned to a StreamThread for processing."""

    def __init__(self, task_id, partitions, topology, consumer, producer, restore_consumer, **config):
        """Create StreamTask with its assigned partitions

        Arguments:
            task_id (str): the ID of this task
            partitions (list of TopicPartition): the assigned partitions
            topology (ProcessorTopology): the instance of ProcessorTopology
            consumer (Consumer): the instance of Consumer
            producer (Producer): the instance of Producer
            restore_consumer (Consumer): the instance of Consumer used when
                restoring state
        """
        super(StreamTask, self).__init__(task_id, partitions, topology,
                                         consumer, restore_consumer, False, **config)
        self._punctuation_queue = PunctuationQueue()
        self._record_info = RecordInfo()

        self.max_buffered_size = config['buffered_records_per_partition']
        self._process_lock = threading.Lock()

        self._commit_requested = False
        self._commit_offset_needed = False
        self._curr_record = None
        self._curr_node = None
        self.requires_poll = True

        # create queues for each assigned partition and associate them
        # to corresponding source nodes in the processor topology
        partition_queues = {}

        for partition in partitions:
            source = self.topology.source(partition.topic)
            queue = self._create_record_queue(partition, source)
            partition_queues[partition] = queue

        self.partition_group = PartitionGroup(partition_queues, config['timestamp_extractor'])

        # initialize the consumed offset cache
        self.consumed_offsets = {}

        # create the RecordCollector that maintains the produced offsets
        self.record_collector = RecordCollector(producer)

        log.info('Creating restoration consumer client for stream task #%s', self.id)

        # initialize the topology with its own context
        self.processor_context = ProcessorContext(self.id, self, self.record_collector, self.state_mgr, **config)

        # initialize the state stores
        #self.initialize_state_stores()

        # initialize the task by initializing all its processor nodes in the topology
        for node in self.topology.processors():
            self._curr_node = node
            try:
                node.init(self.processor_context)
            finally:
                self._curr_node = None

        self.processor_context.initialized()


    def add_records(self, partition, records):
        """Adds records to queues"""
        queue_size = self.partition_group.add_raw_records(partition, records)

        # if after adding these records, its partition queue's buffered size has
        # been increased beyond the threshold, we can then pause the consumption
        # for this partition
        if queue_size > self.max_buffered_size:
            self.consumer.pause(partition)

    def process(self):
        """Process one record

        Returns:
            number of records left in the buffer of this task's partition group after the processing is done
        """
        with self._process_lock:
            # get the next record to process
            timestamp, record = self.partition_group.next_record(self._record_info)

            # if there is no record to process, return immediately
            if record is None:
                self.requires_poll = True
                return 0

            self.requires_poll = False

            try:
                # process the record by passing to the source node of the topology
                self._curr_record = record
                self._curr_node = self._record_info.node()
                partition = self._record_info.partition()

                log.debug('Start processing one record [%s]', self._curr_record)

                self._curr_node.process(self._curr_record.key, self._curr_record.value)

                log.debug('Completed processing one record [%s]', self._curr_record)

                # update the consumed offset map after processing is done
                self.consumed_offsets[partition] = self._curr_record.offset
                self._commit_offset_needed = True

                # after processing this record, if its partition queue's
                # buffered size has been decreased to the threshold, we can then
                # resume the consumption on this partition
                if self._record_info.queue.size() == self.max_buffered_size:
                    self.consumer.resume(partition)
                    self.requires_poll = True

                if self.partition_group.top_queue_size() <= self.max_buffered_size:
                    self.requires_poll = True

            finally:
                self._curr_record = None
                self._curr_node = None

            return self.partition_group.num_buffered()

    def maybe_punctuate(self):
        """Possibly trigger registered punctuation functions if
        current partition group timestamp has reached the defined stamp
        """
        timestamp = self.partition_group.timestamp()

        # if the timestamp is not known yet, meaning there is not enough data
        # accumulated to reason stream partition time, then skip.
        if timestamp == -1:
            return False
        else:
            return self._punctuation_queue.may_punctuate(timestamp, self)

    def punctuate(self, node, timestamp):
        if self._curr_node is not None:
            raise Errors.IllegalStateError('Current node is not null')

        self._curr_node = node
        self._curr_record = (timestamp, DUMMY_RECORD)

        try:
            node.processor().punctuate(timestamp)
        finally:
            self._curr_node = None
            self._curr_record = None

    def record(self):
        return self._curr_record

    def node(self):
        return self._curr_node

    def commit(self):
        """Commit the current task state"""
        # 1) flush local state
        if self.state_mgr is not None:
            self.state_mgr.flush()

        # 2) flush produced records in the downstream and change logs of local states
        self.record_collector.flush()

        # 3) commit consumed offsets if it is dirty already
        if self._commit_offset_needed:
            consumed_offsets_and_metadata = {}
            for partition, offset in self.consumed_offsets.items():
                consumed_offsets_and_metadata[partition] = OffsetAndMetadata(offset + 1, b'')
                if self.state_mgr is not None:
                    self.state_mgr.put_offset_limit(partition, offset + 1)
            self.consumer.commit(consumed_offsets_and_metadata)
            self._commit_offset_needed = False

        self._commit_requested = False

    def commit_needed(self):
        """Whether or not a request has been made to commit the current state"""
        return self._commit_requested

    def need_commit(self):
        """Request committing the current task's state"""
        self._commit_requested = True

    def schedule(self, interval_ms):
        """Schedules a punctuation for the processor

        Arguments:
            interval_ms (int): the interval in milliseconds

        Raises: IllegalStateError if the current node is not None
        """
        if self._curr_node is None:
            raise Errors.IllegalStateError('Current node is null')

        schedule = (0, self._curr_node, interval_ms)
        self._punctuation_queue.schedule(schedule)

    def close(self):
        self.partition_group.close()
        self.consumed_offsets.clear()

        # close the processors
        # make sure close() is called for each node even when there is a RuntimeException
        exception = None
        for node in self.topology.processors():
            self._curr_node = node
            try:
                node.close()
            except RuntimeError as e:
                exception = e
            finally:
                self._curr_node = None

        super(StreamTask, self).close()

        if exception is not None:
            raise exception

    def record_collector_offsets(self):
        return self.record_collector.offsets()

    def _create_record_queue(self, partition, source):
        return RecordQueue(partition, source)

    def forward(self, key, value, child_index=None, child_name=None):
        this_node = self._curr_node
        try:
            children = this_node.children

            if child_index is not None:
                children = [children[child_index]]
            elif child_name is not None:
                children = [child for child in children if child.name == child_name]

            for child_node in children:
                self._curr_node = child_node
                child_node.process(key, value)
        finally:
            self._curr_node = this_node
