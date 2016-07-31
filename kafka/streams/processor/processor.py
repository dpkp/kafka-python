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

import abc


class Processor(object):
    """A processor of key-value pair records."""
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def init(self, context):
        """Initialize this processor with the given context.

        The framework ensures this is called once per processor when the
        topology that contains it is initialized.

        If this processor is to be called periodically by the framework,
        (via punctuate) then this method should schedule itself with the
        provided context.

        Arguments:
            context (ProcessorContext): the context; may not be None

        Returns: None
        """
        pass

    @abc.abstractmethod
    def process(self, key, value):
        """Process the record with the given key and value.

        Arguments:
            key: the key for the record after deserialization
            value: the value for the record after deserialization

        Returns: None
        """
        pass

    @abc.abstractmethod
    def punctuate(self, timestamp):
        """Perform any periodic operations
        
        Requires that the processor scheduled itself with the context during
        initialization

        Arguments:
            timestamp (int): stream time in ms when this method is called

        Returns: None
        """
        pass

    @abc.abstractmethod
    def close(self):
        """Close this processor and clean up any resources.

        Be aware that close() is called after an internal cleanup.
        Thus, it is not possible to write anything to Kafka as underlying
        clients are already closed.

        Returns: None
        """
        pass
