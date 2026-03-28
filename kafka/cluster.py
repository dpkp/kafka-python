import collections
import copy
import logging
import random
import re
import threading
import time

from kafka import errors as Errors
from kafka.conn import get_ip_port_afi
from kafka.future import Future
from kafka.protocol.new.metadata import MetadataRequest, MetadataResponse
from kafka.structs import TopicPartition
from kafka.util import ensure_valid_topic_name

log = logging.getLogger(__name__)


class ClusterMetadata:
    """
    A class to manage kafka cluster metadata.

    This class does not perform any IO. It simply updates internal state
    given API responses (MetadataResponse, FindCoordinatorResponse).

    Keyword Arguments:
        retry_backoff_ms (int): Milliseconds to backoff when retrying on
            errors. Default: 100.
        metadata_max_age_ms (int): The period of time in milliseconds after
            which we force a refresh of metadata even if we haven't seen any
            partition leadership changes to proactively discover any new
            brokers or partitions. Default: 300000
        bootstrap_servers: 'host[:port]' string (or list of 'host[:port]'
            strings) that the client should contact to bootstrap initial
            cluster metadata. This does not have to be the full node list.
            It just needs to have at least one broker that will respond to a
            Metadata API Request. Default port is 9092. If no servers are
            specified, will default to localhost:9092.
        allow_auto_create_topics (bool): Enable/disable auto topic creation
            on metadata request. Only available with api_version >= (0, 11).
            Default: True
    """
    DEFAULT_CONFIG = {
        'retry_backoff_ms': 100,
        'metadata_max_age_ms': 300000,
        'bootstrap_servers': [],
        'allow_auto_create_topics': True,
    }

    def __init__(self, **configs):
        self._topics = set()
        self._brokers = {}  # node_id -> MetadataResponseBroker
        self._partitions = {}  # topic -> partition -> PartitionMetadata
        self._broker_partitions = collections.defaultdict(set)  # node_id -> {TopicPartition...}
        self._coordinators = {}  # (key_type, key) -> node_id
        self._last_refresh_ms = 0
        self._last_successful_refresh_ms = 0
        self._need_update = True
        self._future = None
        self._listeners = set()
        self._lock = threading.Lock()
        self.need_all_topic_metadata = False
        self.unauthorized_topics = set()
        self.internal_topics = set()
        self.controller = None
        self.cluster_id = None

        self.config = copy.copy(self.DEFAULT_CONFIG)
        for key in self.config:
            if key in configs:
                self.config[key] = configs[key]

        self._bootstrap_brokers = self._generate_bootstrap_brokers()
        self._coordinator_brokers = {}

    def _generate_bootstrap_brokers(self):
        # collect_hosts does not perform DNS, so we should be fine to re-use
        bootstrap_hosts = collect_hosts(self.config['bootstrap_servers'])

        brokers = {}
        for i, (host, port, _) in enumerate(bootstrap_hosts):
            node_id = 'bootstrap-%s' % i
            brokers[node_id] = MetadataResponse.MetadataResponseBroker(node_id, host, port, None)
        return brokers

    def is_bootstrap(self, node_id):
        return node_id in self._bootstrap_brokers

    def set_topics(self, topics):
        """Set specific topics to track for metadata.

        Arguments:
            topics (list of str): topics to check for metadata

        Returns:
            Future: resolves after metadata request/response
        """
        for topic in topics:
            ensure_valid_topic_name(topic)
        if set(topics).difference(self._topics):
            future = self.request_update()
        else:
            future = Future().success(set(topics))
        self._topics = set(topics)
        return future

    def add_topic(self, topic):
        """Add a topic to the list of topics tracked via metadata.

        Arguments:
            topic (str): topic to track

        Returns:
            Future: resolves after metadata request/response

        Raises:
            TypeError: if topic is not a string
            ValueError: if topic is invalid: must be chars (a-zA-Z0-9._-), and less than 250 length
        """
        ensure_valid_topic_name(topic)
        if topic in self._topics:
            return Future().success(set(self._topics))
        self._topics.add(topic)
        return self.request_update()

    def brokers(self):
        """Get all MetadataResponseBroker

        Returns:
            set: {MetadataResponseBroker, ...}
        """
        return list(self._brokers.values()) or list(self._bootstrap_brokers.values())

    def broker_metadata(self, broker_id):
        """Get MetadataResponseBroker

        Arguments:
            broker_id (int or str): node_id for a broker to check

        Returns:
            MetadataResponseBroker or None if not found
        """
        return (
            self._brokers.get(broker_id) or
            self._bootstrap_brokers.get(broker_id) or
            self._coordinator_brokers.get(broker_id)
        )

    def partitions_for_topic(self, topic):
        """Return set of all partitions for topic (whether available or not)

        Arguments:
            topic (str): topic to check for partitions

        Returns:
            set: {partition (int), ...}
            None if topic not found.
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
            None if topic not found.
        """
        if topic not in self._partitions:
            return None
        return set([partition for partition, metadata
                              in self._partitions[topic].items()
                              if metadata.leader_id != -1])

    def leader_for_partition(self, partition):
        """Return node_id of leader, -1 unavailable, None if unknown."""
        if partition.topic not in self._partitions:
            return None
        elif partition.partition not in self._partitions[partition.topic]:
            return None
        return self._partitions[partition.topic][partition.partition].leader_id

    def leader_epoch_for_partition(self, partition):
        return self._partitions[partition.topic][partition.partition].leader_epoch

    def partitions_for_broker(self, broker_id):
        """Return TopicPartitions for which the broker is a leader.

        Arguments:
            broker_id (int or str): node id for a broker

        Returns:
            set: {TopicPartition, ...}
            None if the broker either has no partitions or does not exist.
        """
        return self._broker_partitions.get(broker_id)

    def coordinator_for_group(self, group):
        """Return node_id of group coordinator.

        Arguments:
            group (str): name of consumer group

        Returns:
            node_id (int or str) for group coordinator, -1 if coordinator unknown
            None if the group does not exist.
        """
        return self._coordinators.get(('group', group))

    def ttl(self):
        """Milliseconds until metadata should be refreshed"""
        now = time.monotonic() * 1000
        if self._need_update:
            ttl = 0
        else:
            metadata_age = now - self._last_successful_refresh_ms
            ttl = self.config['metadata_max_age_ms'] - metadata_age

        retry_age = now - self._last_refresh_ms
        next_retry = self.config['retry_backoff_ms'] - retry_age

        return max(ttl, next_retry, 0)

    def refresh_backoff(self):
        """Return milliseconds to wait before attempting to retry after failure"""
        return self.config['retry_backoff_ms']

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

    @property
    def need_update(self):
        return self._need_update

    def topics(self, exclude_internal_topics=True):
        """Get set of known topics.

        Arguments:
            exclude_internal_topics (bool): Whether records from internal topics
                (such as offsets) should be exposed to the consumer. If set to
                True the only way to receive records from an internal topic is
                subscribing to it. Default True

        Returns:
            set: {topic (str), ...}
        """
        topics = set(self._partitions.keys())
        if exclude_internal_topics:
            return topics - self.internal_topics
        else:
            return topics

    def metadata_request(self, version):
        if self.need_all_topic_metadata:
            topics = MetadataRequest[version].ALL_TOPICS
        elif not self._topics:
            topics = MetadataRequest[version].NO_TOPICS
        else:
            topics = [MetadataRequest.MetadataRequestTopic(name=topic)
                      for topic in self._topics]
        return MetadataRequest(
            version=version,
            topics=topics,
            allow_auto_topic_creation=self.config['allow_auto_create_topics'],
            include_cluster_authorized_operations=False,
            include_topic_authorized_operations=False,
        )

    def failed_update(self, exception):
        """Update cluster state given a failed MetadataRequest."""
        f = None
        with self._lock:
            if self._future:
                f = self._future
                self._future = None
        if f:
            f.failure(exception)
        self._last_refresh_ms = time.monotonic() * 1000

    def update_metadata(self, metadata):
        """Update cluster state given a MetadataResponse.

        Arguments:
            metadata (MetadataResponse): broker response to a metadata request

        Returns: None
        """
        if not metadata.brokers:
            log.warning("No broker metadata found in MetadataResponse -- ignoring.")
            return self.failed_update(Errors.MetadataEmptyBrokerList(metadata))

        _new_brokers = {}
        for broker in metadata.brokers:
            if metadata.API_VERSION == 0:
                node_id, host, port = broker
                rack = None
            else:
                node_id, host, port, rack = broker
            _new_brokers.update({
                node_id: MetadataResponse.MetadataResponseBroker(node_id, host, port, rack)
            })

        if metadata.API_VERSION == 0:
            _new_controller = None
        else:
            _new_controller = _new_brokers.get(metadata.controller_id)

        if metadata.API_VERSION < 2:
            _new_cluster_id = None
        else:
            _new_cluster_id = metadata.cluster_id

        _new_partitions = {}
        _new_broker_partitions = collections.defaultdict(set)
        _new_unauthorized_topics = set()
        _new_internal_topics = set()

        for t in metadata.topics:
            topic = t.name
            if t.is_internal:
                _new_internal_topics.add(topic)
            error_type = Errors.for_code(t.error_code)
            if error_type is Errors.NoError:
                _new_partitions[topic] = {}
                for p_data in t.partitions:
                    partition = p_data.partition_index
                    _new_partitions[topic][partition] = p_data
                    if p_data.leader_id != -1:
                        _new_broker_partitions[p_data.leader_id].add(
                            TopicPartition(topic, partition))

            # Specific topic errors can be ignored if this is a full metadata fetch
            elif self.need_all_topic_metadata:
                continue

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
            self._brokers = _new_brokers
            self.controller = _new_controller
            self.cluster_id = _new_cluster_id
            self._partitions = _new_partitions
            self._broker_partitions = _new_broker_partitions
            self.unauthorized_topics = _new_unauthorized_topics
            self.internal_topics = _new_internal_topics
            f = None
            if self._future:
                f = self._future
            self._future = None
            if self.need_all_topic_metadata or self._topics.issubset({t.name for t in metadata.topics}):
                self._need_update = False

        now = time.monotonic() * 1000
        self._last_refresh_ms = now
        self._last_successful_refresh_ms = now

        if f:
            # In the common case where we ask for a single topic and get back an
            # error, we should fail the future
            if len(metadata.topics) == 1 and metadata.topics[0][0] != Errors.NoError.errno:
                error_code, topic = metadata.topics[0][:2]
                error = Errors.for_code(error_code)(topic)
                f.failure(error)
            else:
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

    def add_coordinator(self, response, key_type, key):
        """Update with metadata for a group or txn coordinator

        Arguments:
            response (FindCoordinatorResponse): broker response
            key_type (str): 'group' or 'transaction'
            key (str): consumer_group or transactional_id

        Returns:
            string: coordinator node_id if metadata is updated, None on error
        """
        log.debug("Updating coordinator for %s/%s: %s", key_type, key, response)
        error_type = Errors.for_code(response.error_code)
        if error_type is not Errors.NoError:
            log.error("FindCoordinatorResponse error: %s", error_type)
            self._coordinators[(key_type, key)] = -1
            return

        # Use a coordinator-specific node id so that requests
        # get a dedicated connection
        node_id = 'coordinator-{}'.format(response.node_id)
        coordinator = MetadataResponse.MetadataResponseBroker(
            node_id,
            response.host,
            response.port,
            None)

        log.info("Coordinator for %s/%s is %s", key_type, key, coordinator)
        self._coordinator_brokers[node_id] = coordinator
        self._coordinators[(key_type, key)] = node_id
        return node_id

    def __str__(self):
        return 'ClusterMetadata(brokers: %d, topics: %d, coordinators: %d)' % \
               (len(self._brokers), len(self._partitions), len(self._coordinators))


def collect_hosts(hosts, randomize=True):
    """
    Collects a comma-separated set of hosts (host:port) and optionally
    randomize the returned list.
    """

    if isinstance(hosts, str):
        hosts = hosts.strip().split(',')

    result = []
    for host_port in hosts:
        # ignore leading SECURITY_PROTOCOL:// to mimic java client
        host_port = re.sub('^.*://', '', host_port)
        host, port, afi = get_ip_port_afi(host_port)
        result.append((host, port, afi))

    if randomize:
        random.shuffle(result)
    return result
