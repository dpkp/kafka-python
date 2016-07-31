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

import collections
import logging
import weakref

from kafka import TopicPartition
from kafka.coordinator.assignors.abstract import AbstractPartitionAssignor
from kafka.coordinator.protocol import (
    ConsumerProtocolMemberMetadata, ConsumerProtocolMemberAssignment)
import kafka.streams.errors as Errors
from .internal_topic_manager import InternalTopicManager
from .partition_group import TaskId
from .assignment.assignment_info import AssignmentInfo
from .assignment.client_state import ClientState
from .assignment.subscription_info import SubscriptionInfo
from .assignment.task_assignor import TaskAssignor

log = logging.getLogger(__name__)


class AssignedPartition(object):
    def __init__(self, task_id, partition):
        self.task_id = task_id
        self.partition = partition

    def __cmp__(self, that):
        return cmp(self.partition, that.partition)


class SubscriptionUpdates(object):
    """
     * Used to capture subscribed topic via Patterns discovered during the
     * partition assignment process.
    """
    def __init__(self):
        self.updated_topic_subscriptions = set()

    def update_topics(self, topic_names):
        self.updatedTopicSubscriptions.clear()
        self.updated_topic_subscriptions.update(topic_names)

    def get_updates(self):
        return self.updated_topic_subscriptions

    def has_updates(self):
        return bool(self.updated_topic_subscriptions)


class StreamPartitionAssignor(AbstractPartitionAssignor):
    name = 'stream'
    version = 0

    def __init__(self, **configs):
        """
        We need to have the PartitionAssignor and its StreamThread to be mutually accessible
        since the former needs later's cached metadata while sending subscriptions,
        and the latter needs former's returned assignment when adding tasks.
        """
        self.stream_thread = None
        self.num_standby_replicas = None
        self.topic_groups = {}
        self.partition_to_task_ids = {}
        self.state_changelog_topic_to_task_ids = {}
        self.internal_source_topic_to_task_ids = {}
        self.standby_tasks = {}
        self.internal_topic_manager = None

        self.num_standby_replicas = configs.get('num_standby_replicas', 0)

        o = configs.get('stream_thread_instance')
        if o is None:
            raise Errors.KafkaError("StreamThread is not specified")

        #if not isinstance(o, StreamThread):
        #    raise Errors.KafkaError(o.__class__.__name__ + " is not an instance of StreamThread")

        self.stream_thread = weakref.proxy(o)
        self.stream_thread.partition_assignor = self

        if 'zookeeper_connect_config' in configs:
            self.internal_topic_manager = InternalTopicManager(configs['zookeeper_connect_config'], configs.get('replication_factor', 1))
        else:
            log.info("Config 'zookeeper_connect_config' isn't supplied and hence no internal topics will be created.")

    def metadata(self, topics):
        """Adds the following information to subscription
        1. Client UUID (a unique id assigned to an instance of KafkaStreams)
        2. Task ids of previously running tasks
        3. Task ids of valid local states on the client's state directory.

        Returns: ConsumerProtocolMemberMetadata
        """

        prev_tasks = self.stream_thread.prev_tasks()
        standby_tasks = self.stream_thread.cached_tasks()
        standby_tasks.difference_update(prev_tasks)
        data = SubscriptionInfo(self.stream_thread.process_id, prev_tasks, standby_tasks)

        return ConsumerProtocolMemberMetadata(self.version, list(topics), data.encode())

    def prepare_topic(self, topic_to_task_ids, compact_topic, post_partition_phase):
        """Internal helper function that creates a Kafka topic

        Arguments:
            topic_to_task_ids (dict): that contains the topic names to be created
            compact_topic (bool): If True, the topic should be a compacted topic.
                This is used for change log topics usually.
            post_partition_phase (bool): If True, the computation for calculating
                the number of partitions is slightly different. Set to True after
                the initial topic-to-partition assignment.

        Returns:
            set([TopicPartition])
        """
        partitions = set()
        # if ZK is specified, prepare the internal source topic before calling partition grouper
        if self.internal_topic_manager is not None:
            log.debug("Starting to validate internal topics in partition assignor.")

            for topic, tasks in topic_to_task_ids.items():
                num_partitions = 0
                if post_partition_phase:
                    # the expected number of partitions is the max value of
                    # TaskId.partition + 1
                    for task in tasks:
                        if num_partitions < task.partition + 1:
                            num_partitions = task.partition + 1
                else:
                    # should have size 1 only
                    num_partitions = -1
                    for task in tasks:
                        num_partitions = task.partition

                self.internal_topic_manager.make_ready(topic, num_partitions, compact_topic)

                # wait until the topic metadata has been propagated to all brokers
                partition_ints = []
                while True:
                    partition_ints = self.stream_thread.restore_consumer.partitions_for_topic(topic)
                    if partition_ints and len(partition_ints) == num_partitions:
                        break

                for partition in partition_ints:
                    partitions.add(TopicPartition(topic, partition))

            log.info("Completed validating internal topics in partition assignor.")
        else:
            missing_topics = []
            for topic in topic_to_task_ids:
                partition_ints = self.stream_thread.restore_consumer.partitions_for_topic(topic)
                if partition_ints is None:
                    missing_topics.append(topic)

            if missing_topics:
                log.warn("Topic {} do not exists but couldn't created as the config '{}' isn't supplied",
                         missing_topics, 'zookeeper_connect_config')

        return partitions

    def assign(self, metadata, subscriptions):
        """Assigns tasks to consumer clients in two steps.

        1. using TaskAssignor to assign tasks to consumer clients.
           - Assign a task to a client which was running it previously.
             If there is no such client, assign a task to a client which has
             its valid local state.
           - A client may have more than one stream threads.
             The assignor tries to assign tasks to a client proportionally to
             the number of threads.
           - We try not to assign the same set of tasks to two different clients
           We do the assignment in one-pass. The result may not satisfy above all.
        2. within each client, tasks are assigned to consumer clients in
           round-robin manner.

        Returns:
            {member_id: ConsumerProtocolMemberAssignment}
        """
        import pdb; pdb.set_trace()
        consumers_by_client = {}
        states = {}
        subscription_updates = SubscriptionUpdates()
        # decode subscription info
        for consumer_id, subscription in subscriptions.items():

            if self.stream_thread.builder.source_topic_pattern() is not None:
               # update the topic groups with the returned subscription list for regex pattern subscriptions
                subscription_updates.update_topics(subscription.topics())

            info = SubscriptionInfo.decode(subscription.user_data)

            consumers = consumers_by_client.get(info.process_id)
            if consumers is None:
                consumers = set()
                consumers_by_client[info.process_id] = consumers
            consumers.add(consumer_id)

            state = states.get(info.process_id)
            if state is None:
                state = ClientState()
                states[info.process_id] = state

            state.prev_active_tasks.update(info.prev_tasks)
            state.prev_assigned_tasks.update(info.prev_tasks)
            state.prev_assigned_tasks.update(info.standby_tasks)
            state.capacity = state.capacity + 1

        self.stream_thread.builder.subscription_updates = subscription_updates
        self.topic_groups = self.stream_thread.builder.topic_groups()

        # ensure the co-partitioning topics within the group have the same
        # number of partitions, and enforce the number of partitions for those
        # internal topics.
        source_topic_groups = {}
        internal_source_topic_groups = {}
        for key, value in self.topic_groups.items():
            source_topic_groups[key] = value.source_topics
            internal_source_topic_groups[key] = value.inter_source_topics

        # for all internal source topics
        # set the number of partitions to the maximum of the depending
        # sub-topologies source topics
        internal_partitions = set()
        all_internal_topic_names = set()
        for topic_group_id, topics_info in self.topic_groups.items():
            internal_topics = topics_info.inter_source_topics
            all_internal_topic_names.update(internal_topics)
            for internal_topic in internal_topics:
                tasks = self.internal_source_topic_to_task_ids.get(internal_topic)

                if tasks is None:
                    num_partitions = -1
                    for other in self.topic_groups.values():
                        other_sink_topics = other.sink_topics

                        if internal_topic in other_sink_topics:
                            for topic in other.source_topics:
                                partitions = None
                                # It is possible the source_topic is another internal topic, i.e,
                                # map().join().join(map())
                                if topic in all_internal_topic_names:
                                    task_ids = self.internal_source_topic_to_task_ids.get(topic)
                                    if task_ids is not None:
                                        for task_id in task_ids:
                                            partitions = task_id.partition
                                else:
                                    partitions = len(metadata.partitions_for_topic(topic))

                                if partitions is not None and partitions > num_partitions:
                                    num_partitions = partitions

                    self.internal_source_topic_to_task_ids[internal_topic] = [TaskId(topic_group_id, num_partitions)]
                    for partition in range(num_partitions):
                        internal_partitions.add(TopicPartition(internal_topic, partition))

        copartition_topic_groups = self.stream_thread.builder.copartition_groups()
        self.ensure_copartitioning(copartition_topic_groups, internal_source_topic_groups,
                                   metadata.with_partitions(internal_partitions))


        internal_partitions = self.prepare_topic(self.internal_source_topic_to_task_ids, False, False);
        self.internal_source_topic_to_task_ids.clear()

        metadata_with_internal_topics = metadata
        if self.internal_topic_manager:
            metadata_with_internal_topics = metadata.with_partitions(internal_partitions)

        # get the tasks as partition groups from the partition grouper
        partitions_for_task = self.stream_thread.partition_grouper(source_topic_groups, metadata_with_internal_topics)

        # add tasks to state change log topic subscribers
        self.state_changelog_topic_to_task_ids = {}
        for task in partitions_for_task:
            for topic_name in self.topic_groups[task.topic_group_id].state_changelog_topics:
                tasks = self.state_changelog_topic_to_task_ids.get(topic_name)
                if tasks is None:
                    tasks = set()
                    self.state_changelog_topic_to_task_ids[topic_name] = tasks

                tasks.add(task)

            for topic_name in self.topic_groups[task.topic_group_id].inter_source_topics:
                tasks = self.internal_source_topic_to_task_ids.get(topic_name)
                if tasks is None:
                    tasks = set()
                    self.internal_source_topic_to_task_ids[topic_name] = tasks

                tasks.add(task)

        # assign tasks to clients
        states = TaskAssignor.assign(states, set(partitions_for_task), self.num_standby_replicas)
        assignment = {}

        for process_id, consumers in consumers_by_client.items():
            state = states[process_id]

            task_ids = []
            num_active_tasks = len(state.active_tasks)
            for task_id in state.active_tasks:
                task_ids.append(task_id)

            for task_id in state.assigned_tasks:
                if task_id not in state.active_tasks:
                    task_ids.append(task_id)

            num_consumers = len(consumers)
            standby = {}

            i = 0
            for consumer in consumers:
                assigned_partitions = []

                num_task_ids = len(task_ids)
                j = i
                while j < num_task_ids:
                    task_id = task_ids[j]
                    if j < num_active_tasks:
                        for partition in partitions_for_task[task_id]:
                            assigned_partitions.append(AssignedPartition(task_id, partition))
                    else:
                        standby_partitions = standby.get(task_id)
                        if standby_partitions is None:
                            standby_partitions = set()
                            standby[task_id] = standby_partitions
                        standby_partitions.update(partitions_for_task[task_id])
                    j += num_consumers

                assigned_partitions.sort()
                active = []
                active_partitions = collections.defaultdict(list)
                for partition in assigned_partitions:
                    active.append(partition.task_id)
                    active_partitions[partition.topic].append(partition.partition)

                data = AssignmentInfo(active, standby)
                assignment[consumer] = ConsumerProtocolMemberAssignment(
                    self.version,
                    sorted(active_partitions.items()),
                    data.encode())
                i += 1

                active.clear()
                standby.clear()

        # if ZK is specified, validate the internal topics again
        self.prepare_topic(self.internal_source_topic_to_task_ids, False, True)
        # change log topics should be compacted
        self.prepare_topic(self.state_changelog_topic_to_task_ids, True, True)

        return assignment

    def on_assignment(self, assignment):
        partitions = [TopicPartition(topic, partition)
                      for topic, topic_partitions in assignment.partition_assignment
                      for partition in topic_partitions]

        partitions.sort()

        info = AssignmentInfo.decode(assignment.user_data)
        self.standby_tasks = info.standby_tasks

        partition_to_task_ids = {}
        task_iter = iter(info.active_tasks)
        for partition in partitions:
            task_ids = self.partition_to_task_ids.get(partition)
            if task_ids is None:
                task_ids = set()
                self.partition_to_task_ids[partition] = task_ids

            try:
                task_ids.add(next(task_iter))
            except StopIteration:
                raise Errors.TaskAssignmentError(
                    "failed to find a task id for the partition=%s"
                    ", partitions=%d, assignment_info=%s"
                    % (partition, len(partitions), info))
        self.partition_to_task_ids = partition_to_task_ids

    def ensure_copartitioning(self, copartition_groups, internal_topic_groups, metadata):
        internal_topics = set()
        for topics in internal_topic_groups.values():
            internal_topics.update(topics)

        for copartition_group in copartition_groups:
            num_partitions = -1

            for topic in copartition_group:
                if topic not in internal_topics:
                    infos = metadata.partitions_for_topic(topic)

                    if infos is None:
                        raise Errors.TopologyBuilderError("External source topic not found: " + topic)

                    if num_partitions == -1:
                        num_partitions = len(infos)
                    elif num_partitions != len(infos):
                        raise Errors.TopologyBuilderError("Topics not copartitioned: [%s]" % copartition_group)

            if num_partitions == -1:
                for topic in internal_topics:
                    if topic in copartition_group:
                        partitions = len(metadata.partitions_for_topic(topic))
                        if partitions is not None and partitions > num_partitions:
                            num_partitions = partitions

            # enforce co-partitioning restrictions to internal topics reusing
            # internalSourceTopicToTaskIds
            for topic in internal_topics:
                if topic in copartition_group:
                    self.internal_source_topic_to_task_ids[topic] = [TaskId(-1, num_partitions)]

    def tasks_for_partition(self, partition):
        return self.partition_to_task_ids.get(partition)

    def standby_tasks(self):
        return self.standby_tasks

    def set_internal_topic_manager(self, internal_topic_manager):
        self.internal_topic_manager = internal_topic_manager
