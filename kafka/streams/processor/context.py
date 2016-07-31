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

import kafka.errors as Errors

NONEXIST_TOPIC = '__null_topic__'


class ProcessorContext(object):

    def __init__(self, task_id, task, record_collector, state_mgr, **config):
        self.config = config
        self._task = task
        self.record_collector = record_collector
        self.task_id = task_id
        self.state_mgr = state_mgr

        #self.metrics = metrics
        self.key_serializer = config['key_serializer']
        self.value_serializer = config['value_serializer']
        self.key_deserializer = config['key_deserializer']
        self.value_deserializer = config['value_deserializer']
        self._initialized = False

    def initialized(self):
        self._initialized = True

    @property
    def application_id(self):
        return self._task.application_id

    def state_dir(self):
        return self.state_mgr.base_dir()

    def register(self, state_store, logging_enabled, state_restore_callback):
        if self._initialized:
            raise Errors.IllegalStateError('Can only create state stores during initialization.')

        self.state_mgr.register(state_store, logging_enabled, state_restore_callback)

    def get_state_store(self, name):
        """
        Raises TopologyBuilderError if an attempt is made to access this state store from an unknown node
        """
        node = self._task.node()
        if not node:
            raise Errors.TopologyBuilderError('Accessing from an unknown node')

        # TODO: restore this once we fix the ValueGetter initialization issue
        #if (!node.stateStores.contains(name))
        #    throw new TopologyBuilderException("Processor " + node.name() + " has no access to StateStore " + name);

        return self.state_mgr.get_store(name)

    def topic(self):
        if self._task.record() is None:
            raise Errors.IllegalStateError('This should not happen as topic() should only be called while a record is processed')

        topic = self._task.record().topic()
        if topic == NONEXIST_TOPIC:
            return None
        else:
            return topic

    def partition(self):
        if self._task.record() is None:
            raise Errors.IllegalStateError('This should not happen as partition() should only be called while a record is processed')
        return self._task.record().partition()

    def offset(self):
        if self._task.record() is None:
            raise Errors.IllegalStateError('This should not happen as offset() should only be called while a record is processed')
        return self._task.record().offset()

    def timestamp(self):
        if self._task.record() is None:
            raise Errors.IllegalStateError('This should not happen as timestamp() should only be called while a record is processed')
        return self._task.record().timestamp

    def forward(self, key, value, child_index=None, child_name=None):
        self._task.forward(key, value, child_index=child_index, child_name=child_name)

    def commit(self):
        self._task.need_commit()

    def schedule(self, interval):
        self._task.schedule(interval)
