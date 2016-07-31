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

import logging
import random

from kafka.streams.errors import TaskAssignmentError
from kafka.streams.processor.assignment.client_state import ClientState

log = logging.getLogger(__name__)


class TaskAssignor(object):

    @classmethod
    def assign(cls, states, tasks, num_standby_replicas):
        assignor = TaskAssignor(states, tasks)
        log.info('Assigning tasks to clients: %s, prev_assignment_balanced: %s,'
                 ' prev_clients_unchangeed: %s, tasks: %s, replicas: %s',
                     states, assignor.prev_assignment_balanced,
                     assignor.prev_clients_unchanged, tasks, num_standby_replicas)

        assignor.assign_tasks()
        if num_standby_replicas > 0:
            assignor.assign_standby_tasks(num_standby_replicas)

        log.info('Assigned with: %s', assignor.states)
        return assignor.states

    def __init__(self, states, tasks):
        self.states = {}
        self.task_pairs = set()
        self.max_num_task_pairs = None
        self.tasks = []
        self.prev_assignment_balanced = True
        self.prev_clients_unchanged = True

        avg_num_tasks = len(tasks) // len(states)
        existing_tasks = set()
        for client, state in states.items():
            self.states[client] = state.copy()
            old_tasks = state.prev_assigned_tasks
            # make sure the previous assignment is balanced
            self.prev_assignment_balanced &= len(old_tasks) < (2 * avg_num_tasks)
            self.prev_assignment_balanced &= len(old_tasks) > (avg_num_tasks / 2)

            for task in old_tasks:
                # Make sure there is no duplicates
                self.prev_clients_unchanged &= task not in existing_tasks
            existing_tasks.update(old_tasks)

        # Make sure the existing assignment didn't miss out any task
        self.prev_clients_unchanged &= existing_tasks == tasks

        self.tasks = list(tasks)

        num_tasks = len(tasks)
        self.max_num_task_pairs = num_tasks * (num_tasks - 1) / 2
        #self.taskPairs = set(range(self.max_num_task_pairs)) # XXX

    def assign_standby_tasks(self, num_standby_replicas):
        num_replicas = min(num_standby_replicas, len(self.states) - 1)
        for _ in range(num_replicas):
            self.assign_tasks(active=False)

    def assign_tasks(self, active=True):
        random.shuffle(self.tasks)

        for task in self.tasks:
            state = self.find_client_for(task)

            if state:
                state.assign(task, active)
            else:
                raise TaskAssignmentError('failed to find an assignable client')

    def find_client_for(self, task):
        check_task_pairs = len(self.task_pairs) < self.max_num_task_pairs

        state = self.find_client_by_addition_cost(task, check_task_pairs)

        if state is None and check_task_pairs:
            state = self.find_client_by_addition_cost(task, False)

        if state:
            self.add_task_pairs(task, state)

        return state

    def find_client_by_addition_cost(self, task, check_task_pairs):
        candidate = None
        candidate_addition_cost = 0.0

        for state in self.states.values():
            if (self.prev_assignment_balanced and self.prev_clients_unchanged
                and task in state.prev_assigned_tasks):
                return state;
            if task not in state.assigned_tasks:
                # if check_task_pairs is True, skip this client if this task doesn't introduce a new task combination
                if (check_task_pairs and state.assigned_tasks
                    and not self.has_new_task_pair(task, state)):
                    continue
                addition_cost = self.compute_addition_cost(task, state)
                if (candidate is None or
                    (addition_cost < candidate_addition_cost or
                     (addition_cost == candidate_addition_cost
                      and state.cost < candidate.cost))):
                    candidate = state
                    candidate_addition_cost = addition_cost
        return candidate

    def add_task_pairs(self, task, state):
        for other in state.assigned_tasks:
            self.task_pairs.add(self.pair(task, other))

    def has_new_task_pair(self, task, state):
        for other in state.assigned_tasks:
            if self.pair(task, other) not in self.task_pairs:
                return True
        return False

    def compute_addition_cost(self, task, state):
        cost = len(state.assigned_tasks) // state.capacity

        if task in state.prev_assigned_tasks:
            if task in state.prev_active_tasks:
                cost += ClientState.COST_ACTIVE
            else:
                cost += ClientState.COST_STANDBY
        else:
            cost += ClientState.COST_LOAD
        return cost

    def pair(self, task1, task2):
        if task1 < task2:
            return self.TaskPair(task1, task2)
        else:
            return self.TaskPair(task2, task1)

    class TaskPair(object):
        def __init__(self, task1, task2):
            self.task1 = task1
            self.task2 = task2

        def __hash__(self):
            return hash(self.task1) ^ hash(self.task2)

        def __eq__(self, other):
            if isinstance(other, type(self)):
                return self.task1 == other.task1 and self.task2 == other.task2
            return False
