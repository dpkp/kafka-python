from __future__ import absolute_import

import collections
import copy
import logging
import random
import time

import six

import kafka.common as Errors
from kafka.common import BrokerMetadata, TopicPartition
from .future import Future

log = logging.getLogger(__name__)


class ClusterMetadata(object):
    DEFAULT_CONFIG = {
        'retry_backoff_ms': 100,
        'metadata_max_age_ms': 300000,
    }

    def __init__(self, **configs):
        self._brokers = {}
        self._partitions = {}
        self._broker_partitions = collections.defaultdict(set)
        self._groups = {}
        self._version = 0
        self._last_refresh_ms = 0
        self._last_successful_refresh_ms = 0
        self._need_update = False
        self._future = None
        self._listeners = set()
        self.need_all_topic_metadata = False

        self.config = copy.copy(self.DEFAULT_CONFIG)
        for key in self.config:
            if key in configs:
                self.config[key] = configs[key]

    def brokers(self):
        return set(self._brokers.values())

    def broker_metadata(self, broker_id):
        return self._brokers.get(broker_id)

    def partitions_for_topic(self, topic):
        """Return set of all partitions for topic (whether available or not)"""
        if topic not in self._partitions:
            return None
        return set(self._partitions[topic].keys())

    def available_partitions_for_topic(self, topic):
        """Return set of partitions with known leaders"""
        if topic not in self._partitions:
            return None
        return set([partition for partition, leader
                              in six.iteritems(self._partitions[topic])
                              if leader != -1])

    def leader_for_partition(self, partition):
        """Return node_id of leader, -1 unavailable, None if unknown."""
        if partition.topic not in self._partitions:
            return None
        return self._partitions[partition.topic].get(partition.partition)

    def partitions_for_broker(self, broker_id):
        """Return TopicPartitions for which the broker is a leader"""
        return self._broker_partitions.get(broker_id)

    def coordinator_for_group(self, group):
        return self._groups.get(group)

    def ttl(self):
        """Milliseconds until metadata should be refreshed"""
        now = time.time() * 1000
        if self._need_update:
            ttl = 0
        else:
            metadata_age = now - self._last_successful_refresh_ms
            ttl = self.config['metadata_max_age_ms'] - metadata_age

        retry_age = now - self._last_refresh_ms
        next_retry = self.config['retry_backoff_ms'] - retry_age

        return max(ttl, next_retry, 0)

    def request_update(self):
        """Flags metadata for update, return Future()

        Actual update must be handled separately. This method will only
        change the reported ttl()

        Returns: Future (value will be this cluster object after update)
        """
        self._need_update = True
        if not self._future or self._future.is_done:
          self._future = Future()
        return self._future

    def topics(self):
        return set(self._partitions.keys())

    def failed_update(self, exception):
        if self._future:
            self._future.failure(exception)
            self._future = None
        self._last_refresh_ms = time.time() * 1000

    def update_metadata(self, metadata):
        # In the common case where we ask for a single topic and get back an
        # error, we should fail the future
        if len(metadata.topics) == 1 and metadata.topics[0][0] != 0:
            error_code, topic, _ = metadata.topics[0]
            error = Errors.for_code(error_code)(topic)
            return self.failed_update(error)

        if not metadata.brokers:
            log.warning("No broker metadata found in MetadataResponse")

        for node_id, host, port in metadata.brokers:
            self._brokers.update({
                node_id: BrokerMetadata(node_id, host, port)
            })

        # Drop any UnknownTopic, InvalidTopic, and TopicAuthorizationFailed
        # but retain LeaderNotAvailable because it means topic is initializing
        self._partitions.clear()
        self._broker_partitions.clear()

        for error_code, topic, partitions in metadata.topics:
            error_type = Errors.for_code(error_code)
            if error_type is Errors.NoError:
                self._partitions[topic] = {}
                for _, partition, leader, _, _ in partitions:
                    self._partitions[topic][partition] = leader
                    if leader != -1:
                        self._broker_partitions[leader].add(TopicPartition(topic, partition))
            elif error_type is Errors.LeaderNotAvailableError:
                log.error("Topic %s is not available during auto-create"
                          " initialization", topic)
            elif error_type is Errors.UnknownTopicOrPartitionError:
                log.error("Topic %s not found in cluster metadata", topic)
            elif error_type is Errors.TopicAuthorizationFailedError:
                log.error("Topic %s is not authorized for this client", topic)
            elif error_type is Errors.InvalidTopicError:
                log.error("'%s' is not a valid topic name", topic)
            else:
                log.error("Error fetching metadata for topic %s: %s",
                          topic, error_type)

        if self._future:
            self._future.success(self)
        self._future = None
        self._need_update = False
        self._version += 1
        now = time.time() * 1000
        self._last_refresh_ms = now
        self._last_successful_refresh_ms = now
        log.debug("Updated cluster metadata version %d to %s",
                  self._version, self)

        for listener in self._listeners:
            listener(self)

    def add_listener(self, listener):
        """Add a callback function to be called on each metadata update"""
        self._listeners.add(listener)

    def remove_listener(self, listener):
        """Remove a previously added listener callback"""
        self._listeners.remove(listener)

    def add_group_coordinator(self, group, response):
        """Update with metadata for a group coordinator

        group: name of group from GroupCoordinatorRequest
        response: GroupCoordinatorResponse

        returns True if metadata is updated, False on error
        """
        log.debug("Updating coordinator for %s: %s", group, response)
        error_type = Errors.for_code(response.error_code)
        if error_type is not Errors.NoError:
            log.error("GroupCoordinatorResponse error: %s", error_type)
            self._groups[group] = -1
            return False

        node_id = response.coordinator_id
        coordinator = BrokerMetadata(
            response.coordinator_id,
            response.host,
            response.port)

        # Assume that group coordinators are just brokers
        # (this is true now, but could diverge in future)
        if node_id not in self._brokers:
            self._brokers[node_id] = coordinator

        # If this happens, either brokers have moved without
        # changing IDs, or our assumption above is wrong
        elif coordinator != self._brokers[node_id]:
            log.error("GroupCoordinator metadata conflicts with existing"
                      " broker metadata. Coordinator: %s, Broker: %s",
                      coordinator, self._brokers[node_id])
            self._groups[group] = node_id
            return False

        log.info("Group coordinator for %s is %s", group, coordinator)
        self._groups[group] = node_id
        return True

    def __str__(self):
        return 'Cluster(brokers: %d, topics: %d, groups: %d)' % \
               (len(self._brokers), len(self._partitions), len(self._groups))
