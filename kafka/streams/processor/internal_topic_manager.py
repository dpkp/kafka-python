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
import json
import logging

"""
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.zookeeper.ZooDefs;
"""

from kafka.streams.errors import StreamsError

log = logging.getLogger(__name__)


class InternalTopicManager(object):

    # TODO: the following ZK dependency should be removed after KIP-4
    ZK_TOPIC_PATH = '/brokers/topics'
    ZK_BROKER_PATH = '/brokers/ids'
    ZK_DELETE_TOPIC_PATH = '/admin/delete_topics'
    ZK_ENTITY_CONFIG_PATH = '/config/topics'
    # TODO: the following LogConfig dependency should be removed after KIP-4
    CLEANUP_POLICY_PROP = 'cleanup.policy'
    COMPACT = 'compact'
    ZK_ENCODING = 'utf-8'

    def __init__(self, zk_connect=None, replication_factor=0):
        if zk_connect:
            #self.zk_client = ZkClient(zk_connect, 30 * 1000, 30 * 1000, self.ZK_ENCODING)
            self.zk_client = None
        else:
            self.zk_client = None
        self.replication_factor = replication_factor

    def make_ready(self, topic, num_partitions, compact_topic):
        topic_not_ready = True

        while topic_not_ready:
            topic_metadata = self.get_topic_metadata(topic)

            if not topic_metadata:
                try:
                    self.create_topic(topic, num_partitions, self.replication_factor, compact_topic)
                except Exception: #ZkNodeExistsError:
                    # ignore and continue
                    pass
            else:
                if len(topic_metadata) > num_partitions:
                    # else if topic exists with more #.partitions than needed, delete in order to re-create it
                    try:
                        self.delete_topic(topic)
                    except Exception: #ZkNodeExistsError:
                        # ignore and continue
                        pass
                elif len(topic_metadata) < num_partitions:
                    # else if topic exists with less #.partitions than needed, add partitions
                    try:
                        self.add_partitions(topic, num_partitions - len(topic_metadata), self.replication_factor, topic_metadata)
                    except Exception: #ZkNoNodeError:
                        # ignore and continue
                        pass
                else:
                    topic_not_ready = False

    def get_brokers(self):
        brokers = []
        for broker in self.zk_client.get_children(self.ZK_BROKER_PATH):
            brokers.append(int(broker))
        brokers.sort()
        log.debug("Read brokers %s from ZK in partition assignor.", brokers)
        return brokers

    def get_topic_metadata(self, topic):
        data = self.zk_client.read_data(self.ZK_TOPIC_PATH + "/" + topic, True)

        if data is None:
            return None

        try:
            partitions = json.loads(data).get('partitions')
            log.debug("Read partitions %s for topic %s from ZK in partition assignor.", partitions, topic)
            return partitions
        except Exception as e:
            raise StreamsError("Error while reading topic metadata from ZK for internal topic " + topic, e)

    def create_topic(self, topic, num_partitions, replication_factor, compact_topic):
        log.debug("Creating topic %s with %s partitions from ZK in partition assignor.", topic, num_partitions)
        brokers = self.get_brokers()
        num_brokers = len(brokers)
        if num_brokers < replication_factor:
            log.warn("Not enough brokers found. The replication factor is reduced from " + replication_factor + " to " +  num_brokers)
            replication_factor = num_brokers

        assignment = {}

        for i in range(num_partitions):
            broker_list = []
            for r in range(replication_factor):
                shift = r * num_brokers / replication_factor
                broker_list.append(brokers[(i + shift) % num_brokers])
            assignment[i] = broker_list

        # write out config first just like in AdminUtils.scala createOrUpdateTopicPartitionAssignmentPathInZK()
        if compact_topic:
            try:
                data_map = {
                    'version': 1,
                    'config': {
                        'cleanup.policy': 'compact'
                    }
                }
                data = json.dumps(data_map)
                #zk_client.create_persistent(self.ZK_ENTITY_CONFIG_PATH + "/" + topic, data, ZooDefs.Ids.OPEN_ACL_UNSAFE)
            except Exception as e:
                raise StreamsError('Error while creating topic config in ZK for internal topic ' + topic, e)

        # try to write to ZK with open ACL
        try:
            data_map = {
                'version': 1,
                'partitions': assignment
            }
            data = json.dumps(data_map)

            #zk_client.create_persistent(self.ZK_TOPIC_PATH + "/" + topic, data, ZooDefs.Ids.OPEN_ACL_UNSAFE)
        except Exception as e:
            raise StreamsError('Error while creating topic metadata in ZK for internal topic ' + topic, e)

    def delete_topic(self, topic):
        log.debug('Deleting topic %s from ZK in partition assignor.', topic)

        #zk_client.create_persistent(self.ZK_DELETE_TOPIC_PATH + "/" + topic, "", ZooDefs.Ids.OPEN_ACL_UNSAFE)

    def add_partitions(self, topic, num_partitions, replication_factor, existing_assignment):
        log.debug('Adding %s partitions topic %s from ZK with existing'
                  ' partitions assigned as %s in partition assignor.',
                  topic, num_partitions, existing_assignment)

        brokers = self.get_brokers()
        num_brokers = len(brokers)
        if (num_brokers < replication_factor):
            log.warning('Not enough brokers found. The replication factor is'
                        ' reduced from %s to %s', replication_factor, num_brokers)
            replication_factor = num_brokers

        start_index = len(existing_assignment)

        new_assignment = copy.deepcopy(existing_assignment)

        for i in range(num_partitions):
            broker_list = []
            for r in range(replication_factor):
                shift = r * num_brokers / replication_factor
                broker_list.append(brokers[(i + shift) % num_brokers])
            new_assignment[i + start_index] = broker_list

        # try to write to ZK with open ACL
        try:
            data_map = {
                'version': 1,
                'partitions': new_assignment
            }
            data = json.dumps(data_map)

            #zk_client.write_data(ZK_TOPIC_PATH + "/" + topic, data)
        except Exception as e:
            raise StreamsError('Error while updating topic metadata in ZK for internal topic ' + topic, e)
