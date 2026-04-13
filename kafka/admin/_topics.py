"""Topic management mixin for KafkaAdminClient.

Also defines NewTopic and NewPartitions data classes.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

import kafka.errors as Errors
from kafka.errors import IncompatibleBrokerVersion


class NewTopic:
    """A class for new topic creation.

    Arguments:
        name (string): name of the topic
        num_partitions (int): number of partitions, or -1 if
            replica_assignment has been specified
        replication_factor (int): replication factor, or -1 if
            replica assignment is specified
        replica_assignments (dict of int: [int]): A mapping containing
            partition id and replicas to assign to it.
        topic_configs (dict of str: str): A mapping of config key
            and value for the topic.
    """
    def __init__(self, name, num_partitions=-1, replication_factor=-1,
                 replica_assignments=None, topic_configs=None):
        self.name = name
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor
        self.replica_assignments = replica_assignments or {}
        self.topic_configs = topic_configs or {}


class NewPartitions:
    """A class for new partition creation on existing topics.

    Note that the length of new_assignments, if specified, must be the
    difference between the new total number of partitions and the existing
    number of partitions.

    Arguments:
        total_count (int): the total number of partitions that should exist
            on the topic
        new_assignments ([[int]]): an array of arrays of replica assignments
            for new partitions. If not set, broker assigns replicas per an
            internal algorithm.
    """
    def __init__(self, total_count, new_assignments=None):
        self.total_count = total_count
        self.new_assignments = new_assignments
from kafka.protocol.admin import CreateTopicsRequest, DeleteTopicsRequest, CreatePartitionsRequest

if TYPE_CHECKING:
    from kafka.net.manager import KafkaConnectionManager

log = logging.getLogger(__name__)


class TopicAdminMixin:
    """Mixin providing topic management methods for KafkaAdminClient."""
    _manager: KafkaConnectionManager
    _client: object
    config: dict

    @staticmethod
    def _convert_new_topic_request(new_topic):
        return (
            new_topic.name,
            new_topic.num_partitions,
            new_topic.replication_factor,
            [
                (partition_id, replicas) for partition_id, replicas in new_topic.replica_assignments.items()
            ],
            [
                (config_key, config_value) for config_key, config_value in new_topic.topic_configs.items()
            ]
        )

    def create_topics(self, new_topics, timeout_ms=None, validate_only=False, raise_errors=True,
                      wait_for_metadata=False):
        """Create new topics in the cluster.

        Arguments:
            new_topics: A list of NewTopic objects.

        Keyword Arguments:
            timeout_ms (numeric, optional): Milliseconds to wait for new topics to be created
                before the broker returns.
            validate_only (bool, optional): If True, don't actually create new topics.
                Not supported by all versions. Default: False
            raise_errors (bool, optional): Whether to raise errors as exceptions. Default True.
            wait_for_metadata (bool, optional): If True, block until each new topic is visible
                in broker metadata with a leader assigned for every partition. Default: False

        Returns: CreateTopicResponse
        """
        if validate_only and wait_for_metadata:
            raise ValueError('validate_only and wait_for_metadata are mutually exclusive')
        timeout_ms = self._validate_timeout(timeout_ms)
        if validate_only and self._manager.broker_version < (0, 10, 2):
            raise IncompatibleBrokerVersion(
                "validate_only requires CreateTopicsRequest >= v1, which is not supported by Kafka {}."
                .format(self._manager.broker_version))

        request = CreateTopicsRequest(
            topics=[self._convert_new_topic_request(new_topic) for new_topic in new_topics],
            timeout_ms=timeout_ms,
            validate_only=validate_only,
            max_version=3,
        )
        def response_errors(r):
            for topic in r.topics:
                yield Errors.for_code(topic.error_code)
        response = self._manager.run(self._send_request_to_controller, request, response_errors, raise_errors)
        if wait_for_metadata:
            self.wait_for_topics([new_topic.name for new_topic in new_topics])
        return response

    def wait_for_topics(self, topic_names, timeout_ms=10000):
        """Block until each of the given topics is ready to use.

        CreateTopicsResponse only confirms that the broker accepted the create
        request; propagating the new topics into the broker's metadata cache --
        and electing a leader for every partition -- can lag behind, especially
        on KRaft clusters. This method polls :meth:`describe_topics` at a fixed
        interval until every requested topic both:

          - is returned with ``error_code == 0``, and
          - has ``error_code == 0`` and a leader assigned (``leader_id >= 0``)
            for every partition.

        Arguments:
            topic_names ([str]): Topic names to wait for.

        Keyword Arguments:
            timeout_ms (numeric, optional): Maximum milliseconds to wait.
                Default: 10000.

        Raises:
            KafkaTimeoutError: if any topic is still not ready when the
                deadline expires.
        """
        if not topic_names:
            return
        topic_names = list(topic_names)
        deadline = time.monotonic() + (timeout_ms / 1000.0)
        pending = {name: 'not yet queried' for name in topic_names}
        while True:
            try:
                topics = self.describe_topics(topics=topic_names)
            except Exception as exc:
                log.debug('describe_topics failed while waiting for topic visibility: %s', exc)
                topics = []
            by_name = {t.get('name'): t for t in topics}
            pending = {}
            for name in topic_names:
                reason = self._topic_not_ready_reason(by_name.get(name))
                if reason is not None:
                    pending[name] = reason
            if not pending:
                return
            if time.monotonic() >= deadline:
                raise Errors.KafkaTimeoutError(
                    'Topics not ready after %sms: %s' % (timeout_ms, pending))
            time.sleep(0.1)

    @staticmethod
    def _topic_not_ready_reason(topic_info):
        """Return a string reason if ``topic_info`` isn't ready, else None."""
        if topic_info is None:
            return 'missing from metadata response'
        error_code = topic_info.get('error_code', 0)
        if error_code != 0:
            return Errors.for_code(error_code).__name__
        partitions = topic_info.get('partitions') or []
        if not partitions:
            return 'no partitions reported'
        bad = []
        for p in partitions:
            p_err = p.get('error_code', 0)
            idx = p.get('partition_index')
            if p_err != 0:
                bad.append('p%s=%s' % (idx, Errors.for_code(p_err).__name__))
                continue
            if p.get('leader_id', -1) < 0:
                bad.append('p%s=no leader' % idx)
        if bad:
            return ','.join(bad)
        return None

    def delete_topics(self, topics, timeout_ms=None, raise_errors=True):
        """Delete topics from the cluster.

        Arguments:
            topics ([str]): A list of topic name strings.

        Keyword Arguments:
            timeout_ms (numeric, optional): Milliseconds to wait for topics to be deleted
                before the broker returns.
            raise_errors (bool, optional): Whether to raise errors as exceptions. Default True.

        Returns:
            Appropriate version of DeleteTopicsResponse class.
        """
        timeout_ms = self._validate_timeout(timeout_ms)
        request = DeleteTopicsRequest(
            topic_names=topics, timeout_ms=timeout_ms,
            max_version=5,
        )
        def response_errors(r):
            for response in r.responses:
                yield Errors.for_code(response.error_code)
        return self._manager.run(self._send_request_to_controller, request, response_errors, raise_errors)

    def create_partitions(self, topic_partitions, timeout_ms=None, validate_only=False, raise_errors=True):
        """Create additional partitions for an existing topic.

        Arguments:
            topic_partitions: A dict of topic name strings to total partition count (int),
                or a dict of {topic_name: {count: int, assignments: [[broker_ids]]}}
                if manual assignment is desired.
                dict of {topic_name: NewPartition} is deprecated.

        Keyword Arguments:
            timeout_ms (numeric, optional): Milliseconds to wait for new partitions to be
                created before the broker returns.
            validate_only (bool, optional): If True, don't actually create new partitions.
                Default: False
            raise_errors (bool, optional): Whether to raise errors as exceptions. Default True.

        Returns:
            Appropriate version of CreatePartitionsResponse class.
        """
        timeout_ms = self._validate_timeout(timeout_ms)
        _Topic = CreatePartitionsRequest.CreatePartitionsTopic
        _Assignment = CreatePartitionsRequest.CreatePartitionsTopic.CreatePartitionsAssignment
        topics = []
        for topic, count in topic_partitions.items():
            if isinstance(count, int):
                topics.append(_Topic(name=topic, count=count))
            elif isinstance(count, dict):
                topics.append(
                    _Topic(
                        name=topic,
                        count=count['count'],
                        assignments=[_Assignment(broker_ids=broker_ids)
                                     for broker_ids in count['assignments']]))

            else:
                topics.append(
                    _Topic(
                        name=topic,
                        count=count.total_count,
                        assignments=[_Assignment(broker_ids=broker_ids)
                                     for broker_ids in count.new_assignments]))
        request = CreatePartitionsRequest(
            topics=topics,
            timeout_ms=timeout_ms,
            validate_only=validate_only)

        def response_errors(r):
            for result in r.results:
                yield Errors.for_code(result.error_code)
        return self._manager.run(self._send_request_to_controller, request, response_errors, raise_errors)
