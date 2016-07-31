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

import kafka.streams.errors as Errors


class QuickUnion(object):

    def __init__(self):
        self.ids = {}

    def add(self, foo):
        self.ids[foo] = foo

    def exists(self, foo):
        return foo in self.ids

    def root(self, foo):
        """
        @throws NoSuchElementException if the parent of this node is null
        """
        current = foo
        parent = self.ids.get(current)

        if not parent:
            raise Errors.NoSuchElementError("id: " + str(foo))

        while parent != current:
            # do the path compression
            grandparent = self.ids.get(parent)
            self.ids[current] = grandparent

            current = parent
            parent = grandparent

        return current

    def unite(self, foo, foobars):
        for bar in foobars:
            self.unite_pair(foo, bar)

    def unite_pair(self, foo, bar):
        root1 = self.root(foo)
        root2 = self.root(bar)

        if root1 != root2:
            self.ids[root1] = root2
