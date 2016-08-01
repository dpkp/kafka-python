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
import copy
import logging
from multiprocessing import Process
import os
import time

import six

from kafka import KafkaConsumer, KafkaProducer
from kafka.consumer.subscription_state import ConsumerRebalanceListener
import kafka.errors as Errors
from kafka.streams.errors import StreamsError
from kafka.streams.utils import AtomicInteger
from .partition_group import partition_grouper
from .stream_partition_assignor import StreamPartitionAssignor
from .task import StreamTask

log = logging.getLogger(__name__)

STREAM_THREAD_ID_SEQUENCE = AtomicInteger(0)


class StreamThread(Process):
    DEFAULT_CONFIG = {
        'application_id': None, # required
        'bootstrap_servers': None, # required
        'process_id': None, # required
        'client_id': 'kafka-python-streams',
        'poll_ms': 100,
        'num_stream_threads': 1,
        'commit_interval_ms': 30000,
        'partition_grouper': partition_grouper,
        'key_serializer': None,
        'value_serializer': None,
        'key_deserializer': None,
        'value_deserializer': None,
        'state_dir': '/tmp/kafka-streams',
        #'client_supplier': ....,
        'state_cleanup_delay_ms': 60000,
        'linger_ms': 100,
        'auto_offset_reset': 'earliest',
        'enable_auto_commit': False,
        'max_poll_records': 1000,
        'replication_factor': 1,
        'num_standby_replicas': 0,
        'buffered_records_per_partition': 1000,
        'zookeeper_connect': None,
        'timestamp_extractor': lambda x: x.timestamp,
    }
    PRODUCER_OVERRIDES = {
        'linger_ms': 100
    }
    CONSUMER_OVERRIDES = {
        'max_poll_records': 1000,
        'auto_offset_reset': 'earliest',
        'enable_auto_commit': False,
    }

    def __init__(self, builder, **configs):
        stream_id = STREAM_THREAD_ID_SEQUENCE.increment()
        super(StreamThread, self).__init__(name='StreamThread-' + str(stream_id))

        self.config = copy.copy(self.DEFAULT_CONFIG)
        for key in self.config:
            if key in configs:
                self.config[key] = configs.pop(key)

        for key in ('application_id', 'process_id', 'bootstrap_servers'):
            assert self.config[key], 'Required configuration: ' + key

        # Only check for extra config keys in top-level class
        log.warning('Unrecognized configs: %s', configs.keys())

        self.builder = builder
        self.source_topics = builder.source_topics()
        self.topic_pattern = builder.source_topic_pattern()
        self.partition_assignor = None

        self._rebalance_exception = None
        self._process_standby_records = False
        self._rebalance_listener = KStreamsConsumerRebalanceListener(self)
        self.config['thread_client_id'] = self.config['client_id'] + "-" + self.name
        self._running = False

    @property
    def application_id(self):
        return self.config['application_id']

    @property
    def process_id(self):
        return self.config['process_id']

    @property
    def client_id(self):
        return self.config['client_id']

    @property
    def partition_grouper(self):
        return self.config['partition_grouper']

    def consumer_configs(self, restore=False):
        consumer = {}
        for key in KafkaConsumer.DEFAULT_CONFIG:
            if key in self.config:
                consumer[key] = self.config[key]
            if key in self.CONSUMER_OVERRIDES:
                if (key in consumer
                    and consumer[key] != self.CONSUMER_OVERRIDES[key]):
                    log.warning('Overriding KafkaConsumer configs: %s=%s',
                                key, self.CONSUMER_OVERRIDES[key])
                consumer[key] = self.CONSUMER_OVERRIDES[key]

        assert not consumer['enable_auto_commit'], (
            'Unexpected user-specified consumer config enable_auto_commit,'
            ' as the streams client will always turn off auto committing.')

        if restore:
            if 'group_id' in consumer:
                consumer.pop('group_id')
            restore_id = self.config['thread_client_id'] + '-restore-consumer'
            consumer['client_id'] = restore_id
            return consumer

        consumer['group_id'] = self.config['application_id']
        consumer['client_id'] = self.config['thread_client_id'] + '-consumer'

        consumer['partition_assignment_strategy'] = [
            StreamPartitionAssignor(
                stream_thread_instance=self,
                replication_factor=self.config['replication_factor'],
                num_standby_replicas=self.config['num_standby_replicas'],
                zookeeper_connect=self.config['zookeeper_connect'],
                **consumer)]
        return consumer

    def producer_configs(self):
        producer = {}
        for key in KafkaProducer.DEFAULT_CONFIG:
            if key in self.config:
                producer[key] = self.config[key]
            if key in self.PRODUCER_OVERRIDES:
                if (key in producer
                    and producer[key] != self.PRODUCER_OVERRIDES[key]):
                    log.warning('Overriding KafkaProducer configs: %s=%s',
                                key, self.PRODUCER_OVERRIDES[key])
                producer[key] = self.PRODUCER_OVERRIDES[key]
        producer['client_id'] = self.config['thread_client_id'] + '-producer'
        return producer

    def initialize(self):
        assert not self._running
        log.info('Creating producer client for stream thread [%s]', self.name)
        #client_supplier = self.config['client_supplier']
        self.producer = KafkaProducer(**self.producer_configs())
        log.info('Creating consumer client for stream thread [%s]', self.name)
        self.consumer = KafkaConsumer(**self.consumer_configs())
        log.info('Creating restore consumer client for stream thread [%s]', self.name)
        self.restore_consumer = KafkaConsumer(**self.consumer_configs(restore=True))

        # initialize the task list
        self._active_tasks = {}
        self._standby_tasks = {}
        self._active_tasks_by_partition = {}
        self._standby_tasks_by_partition = {}
        self._prev_tasks = set()

        # standby ktables
        self._standby_records = {}

        # read in task specific config values
        """
        self._state_dir = os.path.join(self.config['state_dir'], self.config['application_id'])
        if not os.path.isdir(self._state_dir):
            os.makedirs(self._state_dir)
        """

        # the cleaning cycle won't start until partition assignment
        self._last_clean_ms = float('inf')
        self._last_commit_ms = time.time() * 1000

        #self._sensors = StreamsMetricsImpl(metrics)

        self._running = True #new AtomicBoolean(true);

    def run(self):
        """Execute the stream processors.

        Raises:
            KafkaError for any Kafka-related exceptions
            Exception for any other non-Kafka exceptions
        """
        self.initialize()
        log.info('Starting stream thread [%s]', self.name)

        try:
            self._run_loop()
        except Errors.KafkaError:
            # just re-throw the exception as it should be logged already
            raise
        except Exception:
            # we have caught all Kafka related exceptions, and other runtime exceptions
            # should be due to user application errors
            log.exception('Streams application error during processing in thread [%s]', self.name)
            raise
        finally:
            self.shutdown()

    def close(self):
        """Shutdown this stream thread."""
        self._running = False #.set(False)

    def tasks(self):
        return self._active_tasks

    def shutdown(self):
        log.info('Shutting down stream thread [%s]', self.name)

        # Exceptions should not prevent this call from going through all shutdown steps
        try:
            self._commit_all()
        except Exception:
            # already logged in commitAll()
            pass

        # Close standby tasks before closing the restore consumer since closing
        # standby tasks uses the restore consumer.
        self._remove_standby_tasks()

        # We need to first close the underlying clients before closing the state
        # manager, for example we need to make sure producer's record sends
        # have all been acked before the state manager records
        # changelog sent offsets
        try:
            self.producer.close()
        except Exception:
            log.exception('Failed to close producer in thread [%s]', self.name)
        try:
            self.consumer.close()
        except Exception:
            log.exception('Failed to close consumer in thread [%s]', self.name)
        try:
            self.restore_consumer.close()
        except Exception:
            log.exception('Failed to close restore consumer in thread [%s]', self.name)

        self._remove_stream_tasks()
        log.info('Stream thread shutdown complete [%s]', self.name)

    def _run_loop(self):
        total_num_buffered = 0
        last_poll = 0
        requires_poll = True

        if self.topic_pattern is not None:
            self.consumer.subscribe(pattern=self.topic_pattern,
                                    listener=self._rebalance_listener)
        else:
            self.consumer.subscribe(topics=self.source_topics,
                                    listener=self._rebalance_listener)

        while self._still_running():
            # try to fetch some records if necessary
            if requires_poll:
                requires_poll = False

                start_poll = time.time() * 1000

                if total_num_buffered == 0:
                    poll_ms = self.config['poll_ms']
                else:
                    poll_ms = 0
                records = self.consumer.poll(poll_ms)
                last_poll = time.time() * 1000

                if self._rebalance_exception is not None:
                    raise StreamsError('Failed to rebalance',
                                              self._rebalance_exception)

                if records:
                    for partition in records:
                        task = self._active_tasks_by_partition[partition]
                        task.add_records(partition, records[partition])

                end_poll = time.time()
                #self._sensors.poll_time_sensor.record(end_poll - start_poll)

            total_num_buffered = 0

            # try to process one fetch record from each task via the topology,
            # and also trigger punctuate functions if necessary, which may
            # result in more records going through the topology in this loop
            if self._active_tasks:
                for task in six.itervalues(self._active_tasks):
                    start_process = time.time()

                    total_num_buffered += task.process()
                    requires_poll = requires_poll or task.requires_poll

                    latency_ms = (time.time() - start_process) * 1000
                    #self._sensors.process_time_sensor.record(latency_ms)

                    self._maybe_punctuate(task)

                    if task.commit_needed():
                        self._commit_one(task)

                # if poll_ms has passed since the last poll, we poll to respond
                # to a possible rebalance even when we paused all partitions.
                if (last_poll + self.config['poll_ms'] < time.time() * 1000):
                    requires_poll = True

            else:
                # even when no task is assigned, we must poll to get a task.
                requires_poll = True

            self._maybe_commit()
            self._maybe_update_standby_tasks()
            self._maybe_clean()

    def _maybe_update_standby_tasks(self):
        if self._standby_tasks:
            if self._process_standby_records:
                if self._standby_records:
                    remaining_standby_records = {}
                    for partition in self._standby_records:
                        remaining = self._standby_records[partition]
                        if remaining:
                            task = self._standby_tasks_by_partition[partition]
                            remaining = task.update(partition, remaining)
                            if remaining:
                                remaining_standby_records[partition] = remaining
                            else:
                                self.restore_consumer.resume(partition)
                    self._standby_records = remaining_standby_records;
                self._process_standby_records = False

            records = self.restore_consumer.poll(0)

            if records:
                for partition in records:
                    task = self._standby_tasks_by_partition.get(partition)

                    if task is None:
                        log.error('missing standby task for partition %s', partition)
                        raise StreamsError('missing standby task for partition %s' % partition)

                    remaining = task.update(partition, records[partition])
                    if remaining:
                        self.restore_consumer.pause(partition)
                        self._standby_records[partition] = remaining

    def _still_running(self):
        if not self._running:
            log.debug('Shutting down at user request.')
            return False
        return True

    def _maybe_punctuate(self, task):
        try:
            now = time.time()

            # check whether we should punctuate based on the task's partition
            # group timestamp which are essentially based on record timestamp.
            if task.maybe_punctuate():
                latency_ms = (time.time() - now) * 1000
                #self._sensors.punctuate_time_sensor.record(latency_ms)

        except Errors.KafkaError:
            log.exception('Failed to punctuate active task #%s in thread [%s]',
                          task.id, self.name)
            raise

    def _maybe_commit(self):
        now_ms = time.time() * 1000

        if (self.config['commit_interval_ms'] >= 0 and
            self._last_commit_ms + self.config['commit_interval_ms'] < now_ms):
            log.log(0, 'Committing processor instances because the commit interval has elapsed.')

            self._commit_all()
            self._last_commit_ms = now_ms

            self._proces_standby_records = True

    def _commit_all(self):
        """Commit the states of all its tasks"""
        for task in six.itervalues(self._active_tasks):
            self._commit_one(task)
        for task in six.itervalues(self._standby_tasks):
            self._commit_one(task)

    def _commit_one(self, task):
        """Commit the state of a task"""
        start = time.time()
        try:
            task.commit()
        except Errors.CommitFailedError:
            # commit failed. Just log it.
            log.warning('Failed to commit %s #%s in thread [%s]',
                        task.__class__.__name__, task.id, self.name,
                        exc_info=True)
        except Errors.KafkaError:
            # commit failed due to an unexpected exception.
            # Log it and rethrow the exception.
            log.exception('Failed to commit %s #%s in thread [%s]',
                          task.__class__.__name__, task.id, self.name)
            raise

        timer_ms = (time.time() - start) * 1000
        #self._sensors.commit_time_sensor.record(timer_ms)

    def _maybe_clean(self):
        """Cleanup any states of the tasks that have been removed from this thread"""
        now_ms = time.time() * 1000

        clean_time_ms = self.config['state_cleanup_delay_ms']
        if now_ms > self._last_clean_ms + clean_time_ms:
            """
            File[] stateDirs = stateDir.listFiles();
            if (stateDirs != null) {
                for (File dir : stateDirs) {
                    try {
                        String dirName = dir.getName();
                        TaskId id = TaskId.parse(dirName.substring(dirName.lastIndexOf("-") + 1)); # task_id as (topic_group_id, partition_id)

                        // try to acquire the exclusive lock on the state directory
                        if (dir.exists()) {
                            FileLock directoryLock = null;
                            try {
                                directoryLock = ProcessorStateManager.lockStateDirectory(dir);
                                if (directoryLock != null) {
                                    log.info("Deleting obsolete state directory {} for task {} after delayed {} ms.", dir.getAbsolutePath(), id, cleanTimeMs);
                                    Utils.delete(dir);
                                }
                            } catch (FileNotFoundException e) {
                                // the state directory may be deleted by another thread
                            } catch (IOException e) {
                                log.error("Failed to lock the state directory due to an unexpected exception", e);
                            } finally {
                                if (directoryLock != null) {
                                    try {
                                        directoryLock.release();
                                        directoryLock.channel().close();
                                    } catch (IOException e) {
                                        log.error("Failed to release the state directory lock");
                                    }
                                }
                            }
                        }
                    } catch (TaskIdFormatException e) {
                        // there may be some unknown files that sits in the same directory,
                        // we should ignore these files instead trying to delete them as well
                    }
                }
            }
            """
            self._last_clean_ms = now_ms

    def prev_tasks(self):
        """Returns ids of tasks that were being executed before the rebalance."""
        return self._prev_tasks

    def cached_tasks(self):
        """Returns ids of tasks whose states are kept on the local storage."""
        # A client could contain some inactive tasks whose states are still
        # kept on the local storage in the following scenarios:
        # 1) the client is actively maintaining standby tasks by maintaining
        #    their states from the change log.
        # 2) the client has just got some tasks migrated out of itself to other
        #    clients while these task states have not been cleaned up yet (this
        #    can happen in a rolling bounce upgrade, for example).

        tasks = set()
        """
        File[] stateDirs = stateDir.listFiles();
        if (stateDirs != null) {
            for (File dir : stateDirs) {
                try {
                    TaskId id = TaskId.parse(dir.getName());
                    // if the checkpoint file exists, the state is valid.
                    if (new File(dir, ProcessorStateManager.CHECKPOINT_FILE_NAME).exists())
                        tasks.add(id);

                } catch (TaskIdFormatException e) {
                    // there may be some unknown files that sits in the same directory,
                    // we should ignore these files instead trying to delete them as well
                }
            }
        }
        """
        return tasks

    def _create_stream_task(self, task_id, partitions):
        #self._sensors.task_creation_sensor.record()

        topology = self.builder.build(self.config['application_id'],
                                      task_id.topic_group_id)

        return StreamTask(task_id, partitions, topology,
                          self.consumer, self.producer, self.restore_consumer,
                          **self.config) # self._sensors

    def _add_stream_tasks(self, assignment):
        if self.partition_assignor is None:
            raise Errors.IllegalStateError(
                'Partition assignor has not been initialized while adding'
                ' stream tasks: this should not happen.')

        partitions_for_task = collections.defaultdict(set)

        for partition in assignment:
            task_ids = self.partition_assignor.tasks_for_partition(partition)
            for task_id in task_ids:
                partitions = partitions_for_task[task_id].add(partition)

        # create the active tasks
        for task_id, partitions in partitions_for_task.items():
            try:
                task = self._create_stream_task(task_id, partitions)
                self._active_tasks[task_id] = task

                for partition in partitions:
                    self._active_tasks_by_partition[partition] = task
            except StreamsError:
                log.exception('Failed to create an active task #%s in thread [%s]',
                              task_id, self.name)
                raise

    def _remove_stream_tasks(self):
        try:
            for task in self._active_tasks.values():
                self._close_one(task)
            self._prev_tasks.clear()
            self._prev_tasks.update(set(self._active_tasks.keys()))

            self._active_tasks.clear()
            self._active_tasks_by_partition.clear()

        except Exception:
            log.exception('Failed to remove stream tasks in thread [%s]', self.name)

    def _close_one(self, task):
        log.info('Removing a task %s', task.id)
        try:
            task.close()
        except StreamsError:
            log.exception('Failed to close a %s #%s in thread [%s]',
                          task.__class__.__name__, task.id, self.name)
        #self._sensors.task_destruction_sensor.record()

    def _create_standby_task(self, task_id, partitions):
        #self._sensors.task_creation_sensor.record()
        raise NotImplementedError('no standby tasks yet')

        topology = self.builder.build(self.config['application_id'],
                                      task_id.topic_group_id)

        """
        if topology.state_store_suppliers():
            return StandbyTask(task_id, partitions, topology,
                               self.consumer, self.restore_consumer,
                               **self.config) # self._sensors
        else:
            return None
        """

    def _add_standby_tasks(self):
        if self.partition_assignor is None:
            raise Errors.IllegalStateError(
                'Partition assignor has not been initialized while adding'
                ' standby tasks: this should not happen.')

        checkpointed_offsets = {}

        # create the standby tasks
        for task_id, partitions in self.partition_assignor.standby_tasks.items():
            task = self._create_standby_task(task_id, partitions)
            if task:
                self._standby_tasks[task_id] = task
                for partition in partitions:
                    self._standby_tasks_by_partition[partition] = task

                # collect checkpointed offsets to position the restore consumer
                # this includes all partitions from which we restore states
                for partition in task.checkpointed_offsets():
                    self._standby_tasks_by_partition[partition] = task

                checkpointed_offsets.update(task.checkpointed_offsets())

        self.restore_consumer.assign(checkpointed_offsets.keys())

        for partition, offset in checkpointed_offsets.items():
            if offset >= 0:
                self.restore_consumer.seek(partition, offset)
            else:
                self.restore_consumer.seek_to_beginning(partition)

    def _remove_standby_tasks(self):
        try:
            for task in self._standby_tasks.values():
                self._close_one(task)
            self._standby_tasks.clear()
            self._standby_tasks_by_partition.clear()
            self._standby_records.clear()

            # un-assign the change log partitions
            self.restore_consumer.assign([])

        except Exception:
            log.exception('Failed to remove standby tasks in thread [%s]', self.name)

"""
    private class StreamsMetricsImpl implements StreamsMetrics {
        final Metrics metrics;
        final String metricGrpName;
        final Map<String, String> metricTags;

        final Sensor commitTimeSensor;
        final Sensor pollTimeSensor;
        final Sensor processTimeSensor;
        final Sensor punctuateTimeSensor;
        final Sensor taskCreationSensor;
        final Sensor taskDestructionSensor;

        public StreamsMetricsImpl(Metrics metrics) {

            this.metrics = metrics;
            this.metricGrpName = "stream-metrics";
            this.metricTags = new LinkedHashMap<>();
            this.metricTags.put("client-id", clientId + "-" + getName());

            this.commitTimeSensor = metrics.sensor("commit-time");
            this.commitTimeSensor.add(metrics.metricName("commit-time-avg", metricGrpName, "The average commit time in ms", metricTags), new Avg());
            this.commitTimeSensor.add(metrics.metricName("commit-time-max", metricGrpName, "The maximum commit time in ms", metricTags), new Max());
            this.commitTimeSensor.add(metrics.metricName("commit-calls-rate", metricGrpName, "The average per-second number of commit calls", metricTags), new Rate(new Count()));

            this.pollTimeSensor = metrics.sensor("poll-time");
            this.pollTimeSensor.add(metrics.metricName("poll-time-avg", metricGrpName, "The average poll time in ms", metricTags), new Avg());
            this.pollTimeSensor.add(metrics.metricName("poll-time-max", metricGrpName, "The maximum poll time in ms", metricTags), new Max());
            this.pollTimeSensor.add(metrics.metricName("poll-calls-rate", metricGrpName, "The average per-second number of record-poll calls", metricTags), new Rate(new Count()));

            this.processTimeSensor = metrics.sensor("process-time");
            this.processTimeSensor.add(metrics.metricName("process-time-avg-ms", metricGrpName, "The average process time in ms", metricTags), new Avg());
            this.processTimeSensor.add(metrics.metricName("process-time-max-ms", metricGrpName, "The maximum process time in ms", metricTags), new Max());
            this.processTimeSensor.add(metrics.metricName("process-calls-rate", metricGrpName, "The average per-second number of process calls", metricTags), new Rate(new Count()));

            this.punctuateTimeSensor = metrics.sensor("punctuate-time");
            this.punctuateTimeSensor.add(metrics.metricName("punctuate-time-avg", metricGrpName, "The average punctuate time in ms", metricTags), new Avg());
            this.punctuateTimeSensor.add(metrics.metricName("punctuate-time-max", metricGrpName, "The maximum punctuate time in ms", metricTags), new Max());
            this.punctuateTimeSensor.add(metrics.metricName("punctuate-calls-rate", metricGrpName, "The average per-second number of punctuate calls", metricTags), new Rate(new Count()));

            this.taskCreationSensor = metrics.sensor("task-creation");
            this.taskCreationSensor.add(metrics.metricName("task-creation-rate", metricGrpName, "The average per-second number of newly created tasks", metricTags), new Rate(new Count()));

            this.taskDestructionSensor = metrics.sensor("task-destruction");
            this.taskDestructionSensor.add(metrics.metricName("task-destruction-rate", metricGrpName, "The average per-second number of destructed tasks", metricTags), new Rate(new Count()));
        }

        @Override
        public void recordLatency(Sensor sensor, long startNs, long endNs) {
            sensor.record((endNs - startNs) / 1000000, endNs);
        }

        /**
         * @throws IllegalArgumentException if tags is not constructed in key-value pairs
         */
        @Override
        public Sensor addLatencySensor(String scopeName, String entityName, String operationName, String... tags) {
            // extract the additional tags if there are any
            Map<String, String> tagMap = new HashMap<>(this.metricTags);
            if ((tags.length % 2) != 0)
                throw new IllegalArgumentException("Tags needs to be specified in key-value pairs");

            for (int i = 0; i < tags.length; i += 2)
                tagMap.put(tags[i], tags[i + 1]);

            String metricGroupName = "stream-" + scopeName + "-metrics";

            // first add the global operation metrics if not yet, with the global tags only
            Sensor parent = metrics.sensor(scopeName + "-" + operationName);
            addLatencyMetrics(metricGroupName, parent, "all", operationName, this.metricTags);

            // add the store operation metrics with additional tags
            Sensor sensor = metrics.sensor(scopeName + "-" + entityName + "-" + operationName, parent);
            addLatencyMetrics(metricGroupName, sensor, entityName, operationName, tagMap);

            return sensor;
        }

        private void addLatencyMetrics(String metricGrpName, Sensor sensor, String entityName, String opName, Map<String, String> tags) {
            maybeAddMetric(sensor, metrics.metricName(entityName + "-" + opName + "-avg-latency-ms", metricGrpName,
                "The average latency in milliseconds of " + entityName + " " + opName + " operation.", tags), new Avg());
            maybeAddMetric(sensor, metrics.metricName(entityName + "-" + opName + "-max-latency-ms", metricGrpName,
                "The max latency in milliseconds of " + entityName + " " + opName + " operation.", tags), new Max());
            maybeAddMetric(sensor, metrics.metricName(entityName + "-" + opName + "-qps", metricGrpName,
                "The average number of occurrence of " + entityName + " " + opName + " operation per second.", tags), new Rate(new Count()));
        }

        private void maybeAddMetric(Sensor sensor, MetricName name, MeasurableStat stat) {
            if (!metrics.metrics().containsKey(name))
                sensor.add(name, stat);
        }
    }
}
    """


class KStreamsConsumerRebalanceListener(ConsumerRebalanceListener):
    def __init__(self, stream_thread):
        self.stream_thread = stream_thread

    def on_partitions_assigned(self, assignment):
        try:
            self.stream_thread._add_stream_tasks(assignment)
            self.stream_thread._add_standby_tasks()
            # start the cleaning cycle
            self.stream_thread._last_clean_ms = time.time() * 1000
        except Exception as e:
            self.stream_thread._rebalance_exception = e
            raise

    def on_partitions_revoked(self, assignment):
        try:
            self.stream_thread._commit_all()
            # stop the cleaning cycle until partitions are assigned 
            self.stream_thread._last_clean_ms = float('inf')
        except Exception as e:
            self.stream_thread._rebalance_exception = e
            raise
        finally:
            # TODO: right now upon partition revocation, we always remove all
            # the tasks; this behavior can be optimized to only remove affected
            # tasks in the future
            self.stream_thread._remove_stream_tasks()
            self.stream_thread._remove_standby_tasks()
