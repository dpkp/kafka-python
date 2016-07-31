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

import logging
import time

import kafka.errors as Errors

log = logging.getLogger(__name__)

STATE_CHANGELOG_TOPIC_SUFFIX = "-changelog"
CHECKPOINT_FILE_NAME = ".checkpoint"


class ProcessorStateManager(object):

    def __init__(self, application_id, task_id, sources, restore_consumer, is_standby, state_directory):

        self.application_id = application_id
        self.default_partition = task_id.partition
        self.task_id = task_id
        self.state_directory = state_directory
        self.partition_for_topic = {}
        for source in sources:
            self.partition_for_topic[source.topic] = source

        self.stores = {}
        self.logging_enabled = set()
        self.restore_consumer = restore_consumer
        self.restored_offsets = {}
        self.is_standby = is_standby
        if is_standby:
            self.restore_callbacks = {}
        else:
            self.restore_callbacks = None
        self.offset_limits = {}
        self.base_dir  = state_directory.directory_for_task(task_id)

        if not state_directory.lock(task_id, 5):
            raise IOError("Failed to lock the state directory: " + self.base_dir)

        """
        # load the checkpoint information
        checkpoint = OffsetCheckpoint(self.base_dir, CHECKPOINT_FILE_NAME)
        self.checkpointed_offsets = checkpoint.read()

        # delete the checkpoint file after finish loading its stored offsets
        checkpoint.delete()
        """


    def store_changelog_topic(self, application_id, store_name):
        return application_id + "-" + store_name + STATE_CHANGELOG_TOPIC_SUFFIX

    def base_dir(self):
        return self.base_dir

    def register(self, store, logging_enabled, state_restore_callback):
        """
         * @throws IllegalArgumentException if the store name has already been registered or if it is not a valid name
         * (e.g., when it conflicts with the names of internal topics, like the checkpoint file name)
         * @throws StreamsException if the store's change log does not contain the partition
        """
        if (store.name == CHECKPOINT_FILE_NAME):
            raise Errors.IllegalArgumentError("Illegal store name: " + CHECKPOINT_FILE_NAME)

        if store.name in self.stores:
            raise Errors.IllegalArgumentError("Store " + store.name + " has already been registered.")

        if logging_enabled:
            self.logging_enabled.add(store.name)

        # check that the underlying change log topic exist or not
        if logging_enabled:
            topic = self.store_changelog_topic(self.application_id, store.name)
        else:
            topic = store.name

        # block until the partition is ready for this state changelog topic or time has elapsed
        partition = self.get_partition(topic)
        partition_not_found = True
        start_time = time.time() * 1000
        wait_time = 5000 # hard-code the value since we should not block after KIP-4

        while True:
            try:
                time.sleep(50)
            except KeyboardInterrupt:
                # ignore
                pass

            partitions = self.restore_consumer.partitions_for_topic(topic)
            if partitions is None:
                raise Errors.StreamsError("Could not find partition info for topic: " + topic)

            if partition in partitions:
                partition_not_found = False
                break

            if partition_not_found and (time.time() * 1000) < (start_time + wait_time):
                continue
            break

        if partition_not_found:
            raise Errors.StreamsError("Store " + store.name + "'s change log (" + topic + ") does not contain partition " + partition)

        self.stores[store.name] = store

        if self.is_standby:
            if store.persistent():
                self.restore_callbacks[topic] = state_restore_callback
        else:
            self.restore_active_state(topic, state_restore_callback)

    """
    def restore_active_state(self, topic_name, state_restore_callback):
        # ---- try to restore the state from change-log ---- //

        # subscribe to the store's partition
        if (!restoreConsumer.subscription().isEmpty()) {
            throw new IllegalStateException("Restore consumer should have not subscribed to any partitions beforehand");
        }
        TopicPartition storePartition = new TopicPartition(topicName, getPartition(topicName));
        restoreConsumer.assign(Collections.singletonList(storePartition));

        try {
            // calculate the end offset of the partition
            // TODO: this is a bit hacky to first seek then position to get the end offset
            restoreConsumer.seekToEnd(singleton(storePartition));
            long endOffset = restoreConsumer.position(storePartition);

            // restore from the checkpointed offset of the change log if it is persistent and the offset exists;
            // restore the state from the beginning of the change log otherwise
            if (checkpointedOffsets.containsKey(storePartition)) {
                restoreConsumer.seek(storePartition, checkpointedOffsets.get(storePartition));
            } else {
                restoreConsumer.seekToBeginning(singleton(storePartition));
            }

            // restore its state from changelog records
            long limit = offsetLimit(storePartition);
            while (true) {
                long offset = 0L;
                for (ConsumerRecord<byte[], byte[]> record : restoreConsumer.poll(100).records(storePartition)) {
                    offset = record.offset();
                    if (offset >= limit) break;
                    stateRestoreCallback.restore(record.key(), record.value());
                }

                if (offset >= limit) {
                    break;
                } else if (restoreConsumer.position(storePartition) == endOffset) {
                    break;
                } else if (restoreConsumer.position(storePartition) > endOffset) {
                    // For a logging enabled changelog (no offset limit),
                    // the log end offset should not change while restoring since it is only written by this thread.
                    throw new IllegalStateException("Log end offset should not change while restoring");
                }
            }

            // record the restored offset for its change log partition
            long newOffset = Math.min(limit, restoreConsumer.position(storePartition));
            restoredOffsets.put(storePartition, newOffset);
        } finally {
            // un-assign the change log partition
            restoreConsumer.assign(Collections.<TopicPartition>emptyList());
        }
    }

    public Map<TopicPartition, Long> checkpointedOffsets() {
        Map<TopicPartition, Long> partitionsAndOffsets = new HashMap<>();

        for (Map.Entry<String, StateRestoreCallback> entry : restoreCallbacks.entrySet()) {
            String topicName = entry.getKey();
            int partition = getPartition(topicName);
            TopicPartition storePartition = new TopicPartition(topicName, partition);

            if (checkpointedOffsets.containsKey(storePartition)) {
                partitionsAndOffsets.put(storePartition, checkpointedOffsets.get(storePartition));
            } else {
                partitionsAndOffsets.put(storePartition, -1L);
            }
        }
        return partitionsAndOffsets;
    }

    public List<ConsumerRecord<byte[], byte[]>> updateStandbyStates(TopicPartition storePartition, List<ConsumerRecord<byte[], byte[]>> records) {
        long limit = offsetLimit(storePartition);
        List<ConsumerRecord<byte[], byte[]>> remainingRecords = null;

        // restore states from changelog records

        StateRestoreCallback restoreCallback = restoreCallbacks.get(storePartition.topic());

        long lastOffset = -1L;
        int count = 0;
        for (ConsumerRecord<byte[], byte[]> record : records) {
            if (record.offset() < limit) {
                restoreCallback.restore(record.key(), record.value());
                lastOffset = record.offset();
            } else {
                if (remainingRecords == null)
                    remainingRecords = new ArrayList<>(records.size() - count);

                remainingRecords.add(record);
            }
            count++;
        }
        // record the restored offset for its change log partition
        restoredOffsets.put(storePartition, lastOffset + 1);

        return remainingRecords;
    }

    public void putOffsetLimit(TopicPartition partition, long limit) {
        offsetLimits.put(partition, limit);
    }

    private long offsetLimit(TopicPartition partition) {
        Long limit = offsetLimits.get(partition);
        return limit != null ? limit : Long.MAX_VALUE;
    }

    public StateStore getStore(String name) {
        return stores.get(name);
    }

    public void flush() {
        if (!this.stores.isEmpty()) {
            log.debug("Flushing stores.");
            for (StateStore store : this.stores.values())
                store.flush();
        }
    }

    /**
     * @throws IOException if any error happens when flushing or closing the state stores
     */
    public void close(Map<TopicPartition, Long> ackedOffsets) throws IOException {
        try {
            // attempting to flush and close the stores, just in case they
            // are not closed by a ProcessorNode yet
            if (!stores.isEmpty()) {
                log.debug("Closing stores.");
                for (Map.Entry<String, StateStore> entry : stores.entrySet()) {
                    log.debug("Closing storage engine {}", entry.getKey());
                    entry.getValue().flush();
                    entry.getValue().close();
                }

                Map<TopicPartition, Long> checkpointOffsets = new HashMap<>();
                for (String storeName : stores.keySet()) {
                    TopicPartition part;
                    if (loggingEnabled.contains(storeName))
                        part = new TopicPartition(storeChangelogTopic(applicationId, storeName), getPartition(storeName));
                    else
                        part = new TopicPartition(storeName, getPartition(storeName));

                    // only checkpoint the offset to the offsets file if it is persistent;
                    if (stores.get(storeName).persistent()) {
                        Long offset = ackedOffsets.get(part);

                        if (offset != null) {
                            // store the last offset + 1 (the log position after restoration)
                            checkpointOffsets.put(part, offset + 1);
                        } else {
                            // if no record was produced. we need to check the restored offset.
                            offset = restoredOffsets.get(part);
                            if (offset != null)
                                checkpointOffsets.put(part, offset);
                        }
                    }
                }

                // write the checkpoint file before closing, to indicate clean shutdown
                OffsetCheckpoint checkpoint = new OffsetCheckpoint(new File(this.baseDir, CHECKPOINT_FILE_NAME));
                checkpoint.write(checkpointOffsets);
            }
        } finally {
            // release the state directory directoryLock
            stateDirectory.unlock(taskId);
        }
    }

    private int getPartition(String topic) {
        TopicPartition partition = partitionForTopic.get(topic);

        return partition == null ? defaultPartition : partition.partition();
    }
}
    """
