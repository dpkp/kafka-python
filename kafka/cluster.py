from __future__ import absolute_import

import collections
import copy
import logging
import threading
import time

import six

from . import errors as Errors
from .future import Future
from .structs import BrokerMetadata, PartitionMetadata, TopicPartition

log = logging.getLogger(__name__)


class ClusterMetadata(object):
    DEFAULT_CONFIG = {
        'retry_backoff_ms': 100,
        'metadata_max_age_ms': 300000,
    }

    def __init__(self, **configs):
        self._brokers = {}  # node_id -> BrokerMetadata
        self._partitions = {}  # topic -> partition -> PartitionMetadata
        self._broker_partitions = collections.defaultdict(set)  # node_id -> {TopicPartition...}
        self._groups = {}  # group_name -> node_id
        self._last_refresh_ms = 0
        self._last_successful_refresh_ms = 0
        self._need_update = False
        self._future = None
        self._listeners = set()
        self._lock = threading.Lock()
        self.need_all_topic_metadata = False
        self.unauthorized_topics = set()

        self.config = copy.copy(self.DEFAULT_CONFIG)
        for key in self.config:
            if key in configs:
                self.config[key] = configs[key]

    def brokers(self):
        """Get all BrokerMetadata

        Returns:
            set: {BrokerMetadata, ...}
        """
        return set(self._brokers.values())

    def broker_metadata(self, broker_id):
        """Get BrokerMetadata

        Arguments:
            broker_id (int): node_id for a broker to check

        Returns:
            BrokerMetadata or None if not found
        """
        return self._brokers.get(broker_id)

    def partitions_for_topic(self, topic):
        """Return set of all partitions for topic (whether available or not)

        Arguments:
            topic (str): topic to check for partitions

        Returns:
            set: {partition (int), ...}
        """
        if topic not in self._partitions:
            return None
        return set(self._partitions[topic].keys())

    def available_partitions_for_topic(self, topic):
        """Return set of partitions with known leaders

        Arguments:
            topic (str): topic to check for partitions

        Returns:
            set: {partition (int), ...}
        """
        if topic not in self._partitions:
            return None
        return set([partition for partition, metadata
                              in six.iteritems(self._partitions[topic])
                              if metadata.leader != -1])

    def leader_for_partition(self, partition):
        """Return node_id of leader, -1 unavailable, None if unknown."""
        if partition.topic not in self._partitions:
            return None
        elif partition.partition not in self._partitions[partition.topic]:
            return None
        return self._partitions[partition.topic][partition.partition].leader

    def partitions_for_broker(self, broker_id):
        """Return TopicPartitions for which the broker is a leader.

        Arguments:
            broker_id (int): node id for a broker

        Returns:
            set: {TopicPartition, ...}
        """
        return self._broker_partitions.get(broker_id)

    def coordinator_for_group(self, group):
        """Return node_id of group coordinator.

        Arguments:
            group (str): name of consumer group

        Returns:
            int: node_id for group coordinator
        """
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

        Returns:
            kafka.future.Future (value will be the cluster object after update)
        """
        with self._lock:
            self._need_update = True
            if not self._future or self._future.is_done:
              self._future = Future()
            return self._future

    def topics(self):
        """Get set of known topics.

        Returns:
            set: {topic (str), ...}
        """
        return set(self._partitions.keys())

    def failed_update(self, exception):
        """Update cluster state given a failed MetadataRequest."""
        f = None
        with self._lock:
            if self._future:
                f = self._future
                self._future = None
        if f:
            f.failure(exception)
        self._last_refresh_ms = time.time() * 1000

    def update_metadata(self, metadata):
        """Update cluster state given a MetadataResponse.

        Arguments:
            metadata (MetadataResponse): broker response to a metadata request

        Returns: None
        """
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

        _new_partitions = {}
        _new_broker_partitions = collections.defaultdict(set)
        _new_unauthorized_topics = set()

        for error_code, topic, partitions in metadata.topics:
            error_type = Errors.for_code(error_code)
            if error_type is Errors.NoError:
                _new_partitions[topic] = {}
                for p_error, partition, leader, replicas, isr in partitions:
                    _new_partitions[topic][partition] = PartitionMetadata(
                        topic=topic, partition=partition, leader=leader,
                        replicas=replicas, isr=isr, error=p_error)
                    if leader != -1:
                        _new_broker_partitions[leader].add(
                            TopicPartition(topic, partition))

            elif error_type is Errors.LeaderNotAvailableError:
                log.warning("Topic %s is not available during auto-create"
                            " initialization", topic)
            elif error_type is Errors.UnknownTopicOrPartitionError:
                log.error("Topic %s not found in cluster metadata", topic)
            elif error_type is Errors.TopicAuthorizationFailedError:
                log.error("Topic %s is not authorized for this client", topic)
                _new_unauthorized_topics.add(topic)
            elif error_type is Errors.InvalidTopicError:
                log.error("'%s' is not a valid topic name", topic)
            else:
                log.error("Error fetching metadata for topic %s: %s",
                          topic, error_type)

        with self._lock:
            self._partitions = _new_partitions
            self._broker_partitions = _new_broker_partitions
            self.unauthorized_topics = _new_unauthorized_topics
            f = None
            if self._future:
                f = self._future
            self._future = None
            self._need_update = False

        now = time.time() * 1000
        self._last_refresh_ms = now
        self._last_successful_refresh_ms = now

        if f:
            f.success(self)
        log.debug("Updated cluster metadata to %s", self)

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

        Arguments:
            group (str): name of group from GroupCoordinatorRequest
            response (GroupCoordinatorResponse): broker response

        Returns:
            bool: True if metadata is updated, False on error
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
