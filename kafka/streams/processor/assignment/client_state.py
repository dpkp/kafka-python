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

import copy

class ClientState(object):

    COST_ACTIVE = 0.1
    COST_STANDBY = 0.2
    COST_LOAD = 0.5

    def __init__(self, active_tasks=None, assigned_tasks=None,
                 prev_active_tasks=None, prev_assigned_tasks=None,
                 capacity=0.0):
        self.active_tasks = active_tasks if active_tasks else set([])
        self.assigned_tasks = assigned_tasks if assigned_tasks else set([])
        self.prev_active_tasks = prev_active_tasks if prev_active_tasks else set([])
        self.prev_assigned_tasks = prev_assigned_tasks if prev_assigned_tasks else set([])
        self.capacity = capacity
        self.cost = 0.0

    def copy(self):
        return ClientState(copy.deepcopy(self.active_tasks),
                           copy.deepcopy(self.assigned_tasks),
                           copy.deepcopy(self.prev_active_tasks),
                           copy.deepcopy(self.prev_assigned_tasks),
                           self.capacity)

    def assign(self, task_id, active):
        if active:
            self.active_tasks.add(task_id)

        self.assigned_tasks.add(task_id)

        cost = self.COST_LOAD
        try:
            self.prev_assigned_tasks.remove(task_id)
            cost = self.COST_STANDBY
        except KeyError:
            pass
        try:
            self.prev_active_tasks.remove(task_id)
            cost = self.COST_ACTIVE
        except KeyError:
            pass

        self.cost += cost

    def __str__(self):
        return "[active_tasks: (%s) assigned_tasks: (%s) prev_active_tasks: (%s) prev_assigned_tasks: (%s) capacity: (%s) cost: (%s)]" % (
            self.active_tasks, self.assigned_tasks, self.prev_active_tasks, self.prev_assigned_tasks, self.capacity, self.cost)
