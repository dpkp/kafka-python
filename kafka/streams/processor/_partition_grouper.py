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

"""
 * Default implementation of the {@link PartitionGrouper} interface that groups partitions by the partition id.
 *
 * Join operations requires that topics of the joining entities are copartitoned, i.e., being partitioned by the same key and having the same
 * number of partitions. Copartitioning is ensured by having the same number of partitions on
 * joined topics, and by using the serialization and Producer's default partitioner.
"""
class DefaultPartitionGrouper(object):

    """
     * Generate tasks with the assigned topic partitions.
     *
     * @param topicGroups   group of topics that need to be joined together
     * @param metadata      metadata of the consuming cluster
     * @return The map from generated task ids to the assigned partitions
    """
    def partition_groups(self, topic_groups, metadata):
        groups = {}

        for topic_group_id, topic_group in topic_groups.items():

            max_num_partitions = self.max_num_partitions(metadata, topic_group)

            for partition_id in range(max_num_partitions):
                group = set()

                for topic in topic_group:
                    if partition_id < len(metadata.partitions_for_topic(topic)):
                        group.add(TopicPartition(topic, partition_id))
                groups[TaskId(topicGroupId, partitionId)] = group

        return groups

    def max_num_partitions(self, metadata, topics):
        max_num_partitions = 0
        for topic in topics:
            partitions = metadata.partitions_for_topic(topic)

            if not partitions:
                raise Errors.StreamsError("Topic not found during partition assignment: " + topic)

            num_partitions = len(partitions)
            if num_partitions > max_num_partitions:
                max_num_partitions = num_partitions
        return max_num_partitions
