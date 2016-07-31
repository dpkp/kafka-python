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

import abc
import re

import kafka.streams.errors as Errors
from .node import ProcessorNode, SourceNode, SinkNode
from .processor_state_manager import STATE_CHANGELOG_TOPIC_SUFFIX
from .quick_union import QuickUnion
from .topology import ProcessorTopology


class StateStoreFactory(object):
    def __init__(self, is_internal, supplier):
        self.users = set()
        self.is_internal = is_internal
        self.supplier = supplier

class NodeFactory(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, builder, name):
        self.builder = builder
        self.name = name

    @abc.abstractmethod
    def build(self, application_id):
        pass

class ProcessorNodeFactory(NodeFactory):
    def __init__(self, builder, name, parents, supplier):
        self.builder = builder
        self.name = name
        self.parents = list(parents)
        self.supplier = supplier
        self.state_store_names = set()

    def add_state_store(self, state_store_name):
        self.state_store_names.add(state_store_name)

    def build(self, application_id):
        return ProcessorNode(self.name, self.supplier(), self.state_store_names)

class SourceNodeFactory(NodeFactory):
    def __init__(self, builder, name, topics, pattern, key_deserializer, val_deserializer):
        self.builder = builder
        self.name = name
        self.topics = list(topics) if topics else None
        self.pattern = pattern
        self.key_deserializer = key_deserializer
        self.val_deserializer = val_deserializer

    def get_topics(self):
        return self.topics

    """
    def get_topics(self, subscribed_topics=None):
        if not subscribed_topics:
            return self.topics
        matched_topics = []
        for update in subscribed_topics:
            if self.pattern == topicToPatterns.get(update)) {
                matchedTopics.add(update);
                //not same pattern instance,but still matches not allowed
            } else if (topicToPatterns.containsKey(update) && isMatch(update)) {
                throw new TopologyBuilderException("Topic " + update + " already matched check for overlapping regex patterns");
            } else if (isMatch(update)) {
                topicToPatterns.put(update, this.pattern);
                matchedTopics.add(update);
            }
        }
        return matchedTopics.toArray(new String[matchedTopics.size()]);
    }
    """

    def build(self, application_id):
        return SourceNode(self.name, self.key_deserializer, self.val_deserializer)

    """
    private boolean isMatch(String topic) {
        return this.pattern.matcher(topic).matches();
    """

class SinkNodeFactory(NodeFactory):
    def __init__(self, builder, name, parents, topic, key_serializer, val_serializer, partitioner):
        self.builder = builder
        self.name = name
        self.parents = list(parents)
        self.topic = topic
        self.key_serializer = key_serializer
        self.val_serializer = val_serializer
        self.partitioner = partitioner

    def build(self, application_id):
        if self.topic in self.builder.internal_topics:
            sink_name = application_id + '-' + self.topic
        else:
            sink_name = self.topic
        return SinkNode(self.name, sink_name, self.key_serializer, self.val_serializer, self.partitioner)

class TopicsInfo(object):
    def __init__(self, builder, sink_topics, source_topics, inter_source_topics, state_changelog_topics):
        self.sink_topics = set(sink_topics)
        self.source_topics = set(source_topics)
        self.inter_source_topics = set(inter_source_topics)
        self.state_changelog_topics = set(state_changelog_topics)

    def __eq__(self, other):
        if isinstance(other, TopicsInfo):
            return (other.source_topics == self.source_topics and
                    other.state_changelog_topics == self.state_changelog_topics)
        else:
            return False

    """
    @Override
    public int hashCode() {
        long n = ((long) sourceTopics.hashCode() << 32) | (long) stateChangelogTopics.hashCode();
        return (int) (n % 0xFFFFFFFFL);
    """


class TopologyBuilder(object):
    """TopologyBuilder is used to build a ProcessorTopology.

    A topology contains an acyclic graph of sources, processors, and sinks.

    A source is a node in the graph that consumes one or more Kafka topics
    and forwards them to its child nodes.

    A processor is a node in the graph that receives input records from
    upstream nodes, processes the records, and optionally forwarding new
    records to one or all of its children.

    A sink is a node in the graph that receives records from upstream nodes
    and writes them to a Kafka topic.

    This builder allows you to construct an acyclic graph of these nodes,
    and the builder is then passed into a new KafkaStreams instance that will
    then begin consuming, processing, and producing records.
    """

    def __init__(self):
        """Create a new builder."""
        # node factories in a topological order
        self.node_factories = {}

        # state factories
        self.state_factories = {}

        self.source_topic_names = set()
        self.internal_topic_names = set()
        self.node_grouper = QuickUnion()
        self.copartition_source_groups = []
        self.node_to_source_topics = {}
        self.node_to_source_patterns = {}
        self.topic_to_patterns = {}
        self.node_to_sink_topic = {}
        self.subscription_updates = set()
        self.application_id = None

        self._node_groups = None
        self.topic_pattern = None

    def add_source(self, name, *topics, **kwargs):
        """Add a named source node that consumes records from kafka.

        Source consumes named topics or topics that match a pattern and
        forwards the records to child processor and/or sink nodes.
        The source will use the specified key and value deserializers.

        Arguments:
            name (str): unique name of the source used to reference this node
                when adding processor children
            topics (*str): one or more Kafka topics to consume with this source

        Keyword Arguments:
            topic_pattern (str): pattern to match source topics
            key_deserializer (callable): used when consuming records, if None,
                uses the default key deserializer specified in the stream
                configuration
            val_deserializer (callable): the value deserializer used when
                consuming records; if None, uses the default value
                deserializer specified in the stream configuration.

        Raises: TopologyBuilderError if processor is already added or if topics
            have already been registered by another source

        Returns: self, so methods can be chained together
        """
        topic_pattern = kwargs.get('topic_pattern', None)
        key_deserializer = kwargs.get('key_deserializer', None)
        val_deserializer = kwargs.get('val_deserializer', None)
        if name in self.node_factories:
            raise Errors.TopologyBuilderError("Processor " + name + " is already added.")

        if topic_pattern:
            if topics:
                raise Errors.TopologyBuilderError('Cannot supply both topics and a topic_pattern')

            for source_topic_name in self.source_topic_names:
                if re.match(topic_pattern, source_topic_name):
                    raise Errors.TopologyBuilderError("Pattern  " + topic_pattern + " will match a topic that has already been registered by another source.")

            self.node_to_source_patterns[name] = topic_pattern
            self.node_factories[name] = SourceNodeFactory(self, name, None, topic_pattern, key_deserializer, val_deserializer)
            self.node_grouper.add(name)

            return self

        for topic in topics:
            if topic in self.source_topic_names:
                raise Errors.TopologyBuilderError("Topic " + topic + " has already been registered by another source.")

            for pattern in self.node_to_source_patterns.values():
                if re.match(pattern, topic):
                    raise Errors.TopologyBuilderError("Topic " + topic + " matches a Pattern already registered by another source.")

            self.source_topic_names.add(topic)

        self.node_factories[name] = SourceNodeFactory(self, name, topics, None, key_deserializer, val_deserializer)
        self.node_to_source_topics[name] = list(topics)
        self.node_grouper.add(name)

        return self

    def add_sink(self, name, topic, *parent_names, **kwargs):
        """Add a named sink node that writes records to a named kafka topic.

        The sink node forwards records from upstream parent processor and/or
        source nodes to the named Kafka topic. The sink will use the specified
        key and value serializers, and the supplied partitioner.

        Arguments;
            name (str): unique name of the sink node
            topic (str): name of the output topic for the sink
            parent_names (*str): one or more source or processor nodes whose
                output records should consumed by this sink and written to
                the output topic

        Keyword Arguments:
            key_serializer (callable): the key serializer used when consuming
                records; if None, uses the default key serializer specified in
                the stream configuration.
            val_serializer (callable): the value serializer used when consuming
                records; if None, uses the default value serializer specified
                in the stream configuration.
            partitioner (callable): function used to determine the partition
                for each record processed by the sink

        Raises: TopologyBuilderError if parent processor is not added yet, or
            if this processor's name is equal to the parent's name

        Returns: self, so methods can be chained together
        """
        key_serializer = kwargs.get('key_serializer', None)
        val_serializer = kwargs.get('val_serializer', None)
        partitioner = kwargs.get('partitioner', None)
        if name in self.node_factories:
            raise Errors.TopologyBuilderError("Processor " + name + " is already added.")

        for parent in parent_names:
            if parent == name:
                raise Errors.TopologyBuilderError("Processor " + name + " cannot be a parent of itself.")
            if parent not in self.node_factories:
                raise Errors.TopologyBuilderError("Parent processor " + parent + " is not added yet.")

        self.node_factories[name] = SinkNodeFactory(self, name, parent_names, topic, key_serializer, val_serializer, partitioner)
        self.node_to_sink_topic[name] = topic
        self.node_grouper.add(name)
        self.node_grouper.unite(name, parent_names)
        return self

    def add_processor(self, name, supplier, *parent_names):
        """Add a node to process consumed messages from parent nodes.

        A processor node receives and processes records output by one or more
        parent source or processor nodes. Any new record output by this
        processor will be forwarded to its child processor or sink nodes.

        Arguments:
            name (str): unique name of the processor node
            supplier (callable): factory function that returns a Processor
            parent_names (*str): the name of one or more source or processor
                nodes whose output records this processor should receive
                and process

        Returns: self (so methods can be chained together)

        Raises: TopologyBuilderError if parent processor is not added yet,
            or if this processor's name is equal to the parent's name
        """
        if name in self.node_factories:
            raise Errors.TopologyBuilderError("Processor " + name + " is already added.")

        for parent in parent_names:
            if parent == name:
                raise Errors.TopologyBuilderError("Processor " + name + " cannot be a parent of itself.")
            if not parent in self.node_factories:
                raise Errors.TopologyBuilderError("Parent processor " + parent + " is not added yet.")

        self.node_factories[name] = ProcessorNodeFactory(self, name, parent_names, supplier)
        self.node_grouper.add(name)
        self.node_grouper.unite(name, parent_names)
        return self

    def add_state_store(self, supplier, *processor_names, **kwargs):
        """Adds a state store

        @param supplier the supplier used to obtain this state store {@link StateStore} instance
        @return this builder instance so methods can be chained together; never null
        @throws TopologyBuilderException if state store supplier is already added
        """
        is_internal = kwargs.get('is_internal', True)
        if supplier.name in self.state_factories:
            raise Errors.TopologyBuilderError("StateStore " + supplier.name + " is already added.")

        self.state_factories[supplier.name] = StateStoreFactory(is_internal, supplier)

        for processor_name in processor_names:
            self.connect_processor_and_state_store(processor_name, supplier.name)

        return self

    def connect_processor_and_state_stores(self, processor_name, *state_store_names):
        """
        Connects the processor and the state stores

        @param processorName the name of the processor
        @param stateStoreNames the names of state stores that the processor uses
        @return this builder instance so methods can be chained together; never null
        """
        for state_store_name in state_store_names:
            self.connect_processor_and_state_store(processor_name, state_store_name)

        return self

    def connect_processors(self, *processor_names):
        """
        Connects a list of processors.

        NOTE this function would not needed by developers working with the processor APIs, but only used
        for the high-level DSL parsing functionalities.

        @param processorNames the name of the processors
        @return this builder instance so methods can be chained together; never null
        @throws TopologyBuilderException if less than two processors are specified, or if one of the processors is not added yet
        """
        if len(processor_names) < 2:
            raise Errors.TopologyBuilderError("At least two processors need to participate in the connection.")

        for processor_name in processor_names:
            if processor_name not in self.node_factories:
                raise Errors.TopologyBuilderError("Processor " + processor_name + " is not added yet.")

        self.node_grouper.unite(processor_names[0], processor_names[1:])

        return self

    def add_internal_topic(self, topic_name):
        """
        Adds an internal topic

        @param topicName the name of the topic
        @return this builder instance so methods can be chained together; never null
        """
        self.internal_topic_names.add(topic_name)

        return self

    def connect_processor_and_state_store(self, processor_name, state_store_name):
        if state_store_name not in self.state_factories:
            raise Errors.TopologyBuilderError("StateStore " + state_store_name + " is not added yet.")
        if processor_name not in self.node_factories:
            raise Errors.TopologyBuilderError("Processor " + processor_name + " is not added yet.")

        state_store_factory = self.state_factories.get(state_store_name)
        for user in state_store_factory.users:
            self.node_grouper.unite(user, [processor_name])
        state_store_factory.users.add(processor_name)

        node_factory = self.node_factories.get(processor_name)
        if isinstance(node_factory, ProcessorNodeFactory):
            node_factory.add_state_store(state_store_name)
        else:
            raise Errors.TopologyBuilderError("cannot connect a state store " + state_store_name + " to a source node or a sink node.")

    def topic_groups(self):
        """
        Returns the map of topic groups keyed by the group id.
        A topic group is a group of topics in the same task.

        @return groups of topic names
        """
        topic_groups = {}

        if self.subscription_updates:
            for name in self.node_to_source_patterns:
                source_node = self.node_factories[name]
                # need to update node_to_source_topics with topics matched from given regex
                self.node_to_source_topics[name] = source_node.get_topics(self.subscription_updates)

        if self._node_groups is None:
            self._node_groups = self.make_node_groups()

        for group_id, nodes in self._node_groups.items():
            sink_topics = set()
            source_topics = set()
            internal_source_topics = set()
            state_changelog_topics = set()
            for node in nodes:
                # if the node is a source node, add to the source topics
                topics = self.node_to_source_topics.get(node)
                if topics:
                    # if some of the topics are internal, add them to the internal topics
                    for topic in topics:
                        if topic in self.internal_topic_names:
                            if self.application_id is None:
                                raise Errors.TopologyBuilderError("There are internal topics and"
                                                                  " applicationId hasn't been "
                                                                  "set. Call setApplicationId "
                                                                  "first")
                            # prefix the internal topic name with the application id
                            internal_topic = self.application_id + "-" + topic
                            internal_source_topics.add(internal_topic)
                            source_topics.add(internal_topic)
                        else:
                            source_topics.add(topic)

                # if the node is a sink node, add to the sink topics
                topic = self.node_to_sink_topic.get(node)
                if topic:
                    if topic in self.internal_topic_names:
                        # prefix the change log topic name with the application id
                        sink_topics.add(self.application_id + "-" + topic)
                    else:
                        sink_topics.add(topic)

                # if the node is connected to a state, add to the state topics
                for state_factory in self.state_factories.values():
                    if state_factory.is_internal and node in state_factory.users:
                        # prefix the change log topic name with the application id
                        state_changelog_topics.add(self.application_id + "-" + state_factory.supplier.name + STATE_CHANGELOG_TOPIC_SUFFIX)

            topic_groups[group_id] = TopicsInfo(
                    self,
                    sink_topics,
                    source_topics,
                    internal_source_topics,
                    state_changelog_topics)

        return topic_groups

    def node_groups(self):
        """
        Returns the map of node groups keyed by the topic group id.

        @return groups of node names
        """
        if self._node_groups is None:
            self._node_groups = self.make_node_groups()

        return self._node_groups

    def make_node_groups(self):
        node_groups = {}
        root_to_node_group = {}

        node_group_id = 0

        # Go through source nodes first. This makes the group id assignment easy to predict in tests
        for node_name in sorted(self.node_to_source_topics):
            root = self.node_grouper.root(node_name)
            node_group = root_to_node_group.get(root)
            if node_group is None:
                node_group = set()
                root_to_node_group[root] = node_group
                node_group_id += 1
                node_groups[node_group_id] = node_group
            node_group.add(node_name)

        # Go through non-source nodes
        for node_name in sorted(self.node_factories):
            if node_name not in self.node_to_source_topics:
                root = self.node_grouper.root(node_name)
                node_group = root_to_node_group.get(root)
                if node_group is None:
                    node_group = set()
                    root_to_node_group[root] = node_group
                    node_group_id += 1
                    node_groups[node_group_id] = node_group
                node_group.add(node_name)

        return node_groups

    def copartition_sources(self, source_nodes):
        """
        Asserts that the streams of the specified source nodes must be copartitioned.

        @param sourceNodes a set of source node names
        @return this builder instance so methods can be chained together; never null
        """
        self.copartition_source_groups.append(source_nodes)
        return self

    def copartition_groups(self):
        """
        Returns the copartition groups.
        A copartition group is a group of source topics that are required to be copartitioned.

        @return groups of topic names
        """
        groups = []
        for node_names in self.copartition_source_groups:
            copartition_group = set()
            for node in node_names:
                topics = self.node_to_source_topics.get(node)
                if topics:
                    copartition_group.update(self.convert_internal_topic_names(topics))
            groups.append(copartition_group)
        return groups

    def convert_internal_topic_names(self, *topics):
        topic_names = []
        for topic in topics:
            if topic in self.internal_topic_names:
                if self.application_id is None:
                    raise Errors.TopologyBuilderError("there are internal topics "
                                                      "and applicationId hasn't been set. Call "
                                                      "setApplicationId first")
                topic_names.append(self.application_id + "-" + topic)
            else:
                topic_names.append(topic)
        return topic_names

    def build(self, application_id, topic_group_id=None, node_group=None):
        """
        Build the topology for the specified topic group. This is called automatically when passing this builder into the
        {@link org.apache.kafka.streams.KafkaStreams#KafkaStreams(TopologyBuilder, org.apache.kafka.streams.StreamsConfig)} constructor.

        @see org.apache.kafka.streams.KafkaStreams#KafkaStreams(TopologyBuilder, org.apache.kafka.streams.StreamsConfig)
        """
        if topic_group_id is not None:
            node_group = None
            if topic_group_id is not None:
                node_group = self.node_groups().get(topic_group_id)
            else:
                # when nodeGroup is null, we build the full topology. this is used in some tests.
                node_group = None

        processor_nodes = []
        processor_map = {}
        topic_source_map = {}
        state_store_map = {}

        # create processor nodes in a topological order ("nodeFactories" is already topologically sorted)
        for factory in self.node_factories.values():
            if node_group is None or factory.name in node_group:
                node = factory.build(application_id)
                processor_nodes.append(node)
                processor_map[node.name] = node

                if isinstance(factory, ProcessorNodeFactory):
                    for parent in factory.parents:
                        processor_map[parent].add_child(node)
                    for state_store_name in factory.state_store_names:
                        if state_store_name not in state_store_map:
                            state_store_map[state_store_name] = self.state_factories[state_store_name].supplier
                elif isinstance(factory, SourceNodeFactory):
                    if factory.pattern is not None:
                        topics = factory.get_topics(self.subscription_updates)
                    else:
                        topics = factory.get_topics()
                    for topic in topics:
                        if topic in self.internal_topic_names:
                            # prefix the internal topic name with the application id
                            topic_source_map[application_id + "-" + topic] = node
                        else:
                            topic_source_map[topic] = node
                elif isinstance(factory, SinkNodeFactory):
                    for parent in factory.parents:
                        processor_map[parent].add_child(node)
                else:
                    raise Errors.TopologyBuilderError("Unknown definition class: " + factory.__class__.__name__)

        return ProcessorTopology(processor_nodes, topic_source_map, state_store_map.values())

    def source_topics(self):
        """
        Get the names of topics that are to be consumed by the source nodes created by this builder.
        @return the unmodifiable set of topic names used by source nodes, which changes as new sources are added; never null
        """
        topics = set()
        for topic in self.source_topic_names:
            if topic in self.internal_topic_names:
                if self.application_id is None:
                    raise Errors.TopologyBuilderError("there are internal topics and "
                                                      "applicationId is null. Call "
                                                      "setApplicationId before sourceTopics")
                topics.add(self.application_id + "-" + topic)
            else:
                topics.add(topic)
        return topics

    def source_topic_pattern(self):
        if self.topic_pattern is None and self.node_to_source_patterns:
            topic_pattern = ''
            for pattern in self.node_to_source_patterns.values():
                topic_pattern += pattern
                topic_pattern += "|"
            if self.node_to_source_topics:
                for topics in self.node_to_source_topics.values():
                    for topic in topics:
                        topic_pattern += topic
                        topic_pattern += "|"
            self.topic_pattern = topic_pattern[:-1]
        return self.topic_pattern;

    def set_application_id(self, application_id):
        """
        Set the applicationId. This is required before calling
        {@link #sourceTopics}, {@link #topicGroups} and {@link #copartitionSources}
        @param applicationId   the streams applicationId. Should be the same as set by
        {@link org.apache.kafka.streams.StreamsConfig#APPLICATION_ID_CONFIG}
        """
        self.application_id = application_id
