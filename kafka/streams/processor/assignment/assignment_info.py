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

import json
import logging

from kafka import TopicPartition
from kafka.streams.errors import TaskAssignmentError
from kafka.streams.processor.partition_group import TaskId

log = logging.getLogger(__name__)


class AssignmentInfo(object):
    CURRENT_VERSION = 1

    def __init__(self, active_tasks, standby_tasks, version=None):
        self.version = self.CURRENT_VERSION if version is None else version
        self.active_tasks = active_tasks
        self.standby_tasks = standby_tasks

    def encode(self):

        try:
            if self.version == self.CURRENT_VERSION:
                data = {
                    'version': self.version,
                    'active_tasks': [list(task) for task in self.active_tasks],
                    'standby_tasks': [[list(task), [list(tp) for tp in partitions]]
                                      for task, partitions in self.standby_tasks.items()]
                }
                return json.dumps(data).encode('utf-8')

            else:
                raise TaskAssignmentError('Unable to encode assignment data: version=' + str(self.version))

        except Exception as ex:
            raise TaskAssignmentError('Failed to encode AssignmentInfo', ex)

    @classmethod
    def decode(cls, data):
        try:
            decoded = json.loads(data.decode('utf-8'))

            if decoded['version'] == cls.CURRENT_VERSION:
                decoded['active_tasks'] = [TaskId(*task) for task in decoded['active_tasks']]
                decoded['standby_tasks'] = dict([
                    (TaskId(*task), set([TopicPartition(*partition) for partition in partitions]))
                    for task, partitions in decoded['standby_tasks']])

                return AssignmentInfo(decoded['active_tasks'], decoded['standby_tasks'])

            else:
                raise TaskAssignmentError('Unknown assignment data version: ' + str(cls.version))
        except Exception as ex:
            raise TaskAssignmentError('Failed to decode AssignmentInfo', ex)

    def __str__(self):
        return "[version=%d, active_tasks=%d, standby_tasks=%d]" % (
            self.version, len(self.active_tasks), len(self.standby_tasks))
