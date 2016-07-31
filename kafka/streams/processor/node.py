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

import kafka.errors as Errors
from .processor import Processor


class ProcessorNode(object):

    def __init__(self, name, processor=None, state_stores=None):
        self.name = name

        # Could we construct a Processor here if the processor is just a function?
        assert isinstance(processor, Processor), 'processor must subclass Processor'

        self.processor = processor
        self.children = []
        self.state_stores = state_stores

    def add_child(self, child):
        self.children.append(child)

    def init(self, context):
        self.processor.init(context)

    def process(self, key, value):
        self.processor.process(key, value)

    def close(self):
        self.processor.close()


class SourceNode(ProcessorNode):

    def __init__(self, name, key_deserializer, val_deserializer):
        super(SourceNode, self).__init__(name)

        self.key_deserializer = key_deserializer
        self.val_deserializer = val_deserializer
        self.context = None

    def deserialize_key(self, topic, data):
        if self.key_deserializer is None:
            return data
        return self.key_deserializer.deserialize(topic, data)

    def deserialize_value(self, topic, data):
        if self.value_deserializer is None:
            return data
        return self.val_deserializer.deserialize(topic, data)

    def init(self, context):
        self.context = context

        # if deserializers are null, get the default ones from the context
        if self.key_deserializer is None:
            self.key_deserializer = self.context.key_deserializer
        if self.val_deserializer is None:
            self.val_deserializer = self.context.value_deserializer

        """
        // if value deserializers are for {@code Change} values, set the inner deserializer when necessary
        if (this.valDeserializer instanceof ChangedDeserializer &&
                ((ChangedDeserializer) this.valDeserializer).inner() == null)
            ((ChangedDeserializer) this.valDeserializer).setInner(context.valueSerde().deserializer());
        """

    def process(self, key, value):
        self.context.forward(key, value)

    def close(self):
        # do nothing
        pass


class SinkNode(ProcessorNode):

    def __init__(self, name, topic, key_serializer, val_serializer, partitioner):
        super(SinkNode, self).__init__(name)

        self.topic = topic
        self.key_serializer = key_serializer
        self.val_serializer = val_serializer
        self.partitioner = partitioner
        self.context = None

    def add_child(self, child):
        raise Errors.UnsupportedOperationError("sink node does not allow addChild")

    def init(self, context):
        self.context = context

        # if serializers are null, get the default ones from the context
        if self.key_serializer is None:
            self.key_serializer = self.context.key_serializer
        if self.val_serializer is None:
            self.val_serializer = self.context.value_serializer

        """
        // if value serializers are for {@code Change} values, set the inner serializer when necessary
        if (this.valSerializer instanceof ChangedSerializer &&
                ((ChangedSerializer) this.valSerializer).inner() == null)
            ((ChangedSerializer) this.valSerializer).setInner(context.valueSerde().serializer());
        """

    def process(self, key, value):
        # send to all the registered topics
        collector = self.context.record_collector
        collector.send(self.topic, key=key, value=value,
                       timestamp_ms=self.context.timestamp(),
                       key_serializer=self.key_serializer,
                       val_serializer=self.val_serializer,
                       partitioner=self.partitioner)

    def close(self):
        # do nothing
        pass
