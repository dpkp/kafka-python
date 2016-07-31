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

from kafka.structs import TopicPartition

log = logging.getLogger(__name__)


def _handle_send_success(offsets, metadata):
    tp = TopicPartition(metadata.topic, metadata.partition)
    offsets[tp] = metadata.offset


def _handle_send_failure(topic, exception):
    log.error('Error sending record to topic %s: %s', topic, exception)


class RecordCollector(object):
    def __init__(self, producer):
        self.producer = producer
        self.offsets = {}

    def send(self, topic, partition=None, key=None, value=None, timestamp_ms=None,
             key_serializer=None, value_serializer=None, partitioner=None):
        if key_serializer:
            key_bytes = key_serializer(topic, key)
        else:
            key_bytes = key

        if value_serializer:
            val_bytes = value_serializer(topic, value)
        else:
            val_bytes = value

        if partition is None and partitioner is not None:
            partitions = self.producer.partitions_for(topic)
            if partitions is not None:
                partition = partitioner.partition(key, value, len(partitions))

        future = self.producer.send(topic, partition=partition,
                                    key=key_bytes, value=val_bytes,
                                    timestamp_ms=timestamp_ms)

        future.add_callback(_handle_send_success, self.offsets)
        future.add_errback(_handle_send_failure, topic)

    def flush(self):
        self.producer.flush()

    def close(self):
        self.producer.close()
