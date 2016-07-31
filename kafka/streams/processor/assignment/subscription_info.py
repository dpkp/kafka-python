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

from kafka.streams.errors import TaskAssignmentError
from kafka.streams.processor.partition_group import TaskId

log = logging.getLogger(__name__)


class SubscriptionInfo(object):
    CURRENT_VERSION = 1

    def __init__(self, process_id, prev_tasks, standby_tasks, version=None):
        self.version = self.CURRENT_VERSION if version is None else version
        self.process_id = process_id
        self.prev_tasks = prev_tasks
        self.standby_tasks = standby_tasks

    def encode(self):
        if self.version == self.CURRENT_VERSION:
            data = {
                'version': self.version,
                'process_id': self.process_id,
                'prev_tasks': list(self.prev_tasks),
                'standby_tasks': list(self.standby_tasks)
            }
            return json.dumps(data).encode('utf-8')

        else:
            raise TaskAssignmentError('unable to encode subscription data: version=' + str(self.version))

    @classmethod
    def decode(cls, data):
        try:
            decoded = json.loads(data.decode('utf-8'))
            if decoded['version'] != cls.CURRENT_VERSION:
                raise TaskAssignmentError('unable to decode subscription data: version=' + str(cls.version))

            decoded['prev_tasks'] = set([TaskId(*item) for item in decoded['prev_tasks']])
            decoded['standby_tasks'] = set([TaskId(*item) for item in decoded['standby_tasks']])
            return cls(decoded['process_id'], decoded['prev_tasks'], decoded['standby_tasks'], decoded['version'])

        except Exception as e:
            raise TaskAssignmentError('unable to decode subscription data', data, e)
