from __future__ import absolute_import

import copy
import collections
import logging
import time
import weakref

from kafka.vendor import six

from .base import BaseCoordinator
from .assignors.range import RangePartitionAssignor
from .assignors.roundrobin import RoundRobinPartitionAssignor
from .protocol import ConsumerProtocol
from .. import errors as Errors
from ..future import Future
from ..metrics import AnonMeasurable
from ..metrics.stats import Avg, Count, Max, Rate
from ..protocol.commit import OffsetCommitRequest, OffsetFetchRequest
from ..structs import OffsetAndMetadata, TopicPartition
from ..util import WeakMethod


log = logging.getLogger(__name__)


class ConsumerCoordinator(BaseCoordinator):
    """This class manages the coordination process with the consumer coordinator."""
    DEFAULT_CONFIG = {
        'group_id': 'kafka-python-default-group',
        'enable_auto_commit': True,
        'auto_commit_interval_ms': 5000,
        'default_offset_commit_callback': lambda offsets, response: True,
        'assignors': (RangePartitionAssignor, RoundRobinPartitionAssignor),
        'session_timeout_ms': 30000,
        'heartbeat_interval_ms': 3000,
        'retry_backoff_ms': 100,
        'api_version': (0, 9),
        'exclude_internal_topics': True,
        'metric_group_prefix': 'consumer'
    }

    def __init__(self, client, subscription, metrics, **configs):
        """Initialize the coordination manager.

        Keyword Arguments:
            group_id (str): name of the consumer group to join for dynamic
                partition assignment (if enabled), and to use for fetching and
                committing offsets. Default: 'kafka-python-default-group'
            enable_auto_commit (bool): If true the consumer's offset will be
                periodically committed in the background. Default: True.
            auto_commit_interval_ms (int): milliseconds between automatic
                offset commits, if enable_auto_commit is True. Default: 5000.
            default_offset_commit_callback (callable): called as
                callback(offsets, response) response will be either an Exception
                or a OffsetCommitResponse struct. This callback can be used to
                trigger custom actions when a commit request completes.
            assignors (list): List of objects to use to distribute partition
                ownership amongst consumer instances when group management is
                used. Default: [RangePartitionAssignor, RoundRobinPartitionAssignor]
            heartbeat_interval_ms (int): The expected time in milliseconds
                between heartbeats to the consumer coordinator when using
                Kafka's group management feature. Heartbeats are used to ensure
                that the consumer's session stays active and to facilitate
                rebalancing when new consumers join or leave the group. The
                value must be set lower than session_timeout_ms, but typically
                should be set no higher than 1/3 of that value. It can be
                adjusted even lower to control the expected time for normal
                rebalances. Default: 3000
            session_timeout_ms (int): The timeout used to detect failures when
                using Kafka's group managementment facilities. Default: 30000
            retry_backoff_ms (int): Milliseconds to backoff when retrying on
                errors. Default: 100.
            exclude_internal_topics (bool): Whether records from internal topics
                (such as offsets) should be exposed to the consumer. If set to
                True the only way to receive records from an internal topic is
                subscribing to it. Requires 0.10+. Default: True
        """
        super(ConsumerCoordinator, self).__init__(client, metrics, **configs)

        self.config = copy.copy(self.DEFAULT_CONFIG)
        for key in self.config:
            if key in configs:
                self.config[key] = configs[key]

        if self.config['api_version'] >= (0, 9) and self.config['group_id'] is not None:
            assert self.config['assignors'], 'Coordinator requires assignors'

        self._subscription = subscription
        self._metadata_snapshot = self._build_metadata_snapshot(subscription, client.cluster)
        self._assignment_snapshot = None
        self._cluster = client.cluster
        self._cluster.request_update()
        self._cluster.add_listener(WeakMethod(self._handle_metadata_update))

        self._auto_commit_task = None
        if self.config['enable_auto_commit']:
            if self.config['api_version'] < (0, 8, 1):
                log.warning('Broker version (%s) does not support offset'
                            ' commits; disabling auto-commit.',
                            self.config['api_version'])
                self.config['enable_auto_commit'] = False
            elif self.config['group_id'] is None:
                log.warning('group_id is None: disabling auto-commit.')
                self.config['enable_auto_commit'] = False
            else:
                interval = self.config['auto_commit_interval_ms'] / 1000.0
                self._auto_commit_task = AutoCommitTask(weakref.proxy(self), interval)
                self._auto_commit_task.reschedule()

        self.consumer_sensors = ConsumerCoordinatorMetrics(
            metrics, self.config['metric_group_prefix'], self._subscription)

    def __del__(self):
        if hasattr(self, '_cluster') and self._cluster:
            self._cluster.remove_listener(WeakMethod(self._handle_metadata_update))

    def protocol_type(self):
        return ConsumerProtocol.PROTOCOL_TYPE

    def group_protocols(self):
        """Returns list of preferred (protocols, metadata)"""
        topics = self._subscription.subscription
        assert topics is not None, 'Consumer has not subscribed to topics'
        metadata_list = []
        for assignor in self.config['assignors']:
            metadata = assignor.metadata(topics)
            group_protocol = (assignor.name, metadata)
            metadata_list.append(group_protocol)
        return metadata_list

    def _handle_metadata_update(self, cluster):
        # if we encounter any unauthorized topics, raise an exception
        if cluster.unauthorized_topics:
            raise Errors.TopicAuthorizationFailedError(cluster.unauthorized_topics)

        if self._subscription.subscribed_pattern:
            topics = []
            for topic in cluster.topics(self.config['exclude_internal_topics']):
                if self._subscription.subscribed_pattern.match(topic):
                    topics.append(topic)

            if set(topics) != self._subscription.subscription:
                self._subscription.change_subscription(topics)
                self._client.set_topics(self._subscription.group_subscription())

        # check if there are any changes to the metadata which should trigger
        # a rebalance
        if self._subscription_metadata_changed(cluster):

            if (self.config['api_version'] >= (0, 9)
                and self.config['group_id'] is not None):

                self._subscription.mark_for_reassignment()

            # If we haven't got group coordinator support,
            # just assign all partitions locally
            else:
                self._subscription.assign_from_subscribed([
                    TopicPartition(topic, partition)
                    for topic in self._subscription.subscription
                    for partition in self._metadata_snapshot[topic]
                ])

    def _build_metadata_snapshot(self, subscription, cluster):
        metadata_snapshot = {}
        for topic in subscription.group_subscription():
            partitions = cluster.partitions_for_topic(topic) or []
            metadata_snapshot[topic] = set(partitions)
        return metadata_snapshot

    def _subscription_metadata_changed(self, cluster):
        if not self._subscription.partitions_auto_assigned():
            return False

        metadata_snapshot = self._build_metadata_snapshot(self._subscription, cluster)
        if self._metadata_snapshot != metadata_snapshot:
            self._metadata_snapshot = metadata_snapshot
            return True
        return False

    def _lookup_assignor(self, name):
        for assignor in self.config['assignors']:
            if assignor.name == name:
                return assignor
        return None

    def _on_join_complete(self, generation, member_id, protocol,
                          member_assignment_bytes):
        # if we were the assignor, then we need to make sure that there have
        # been no metadata updates since the rebalance begin. Otherwise, we
        # won't rebalance again until the next metadata change
        if self._assignment_snapshot is not None and self._assignment_snapshot != self._metadata_snapshot:
            self._subscription.mark_for_reassignment()
            return

        assignor = self._lookup_assignor(protocol)
        assert assignor, 'Coordinator selected invalid assignment protocol: %s' % protocol

        assignment = ConsumerProtocol.ASSIGNMENT.decode(member_assignment_bytes)

        # set the flag to refresh last committed offsets
        self._subscription.needs_fetch_committed_offsets = True

        # update partition assignment
        self._subscription.assign_from_subscribed(assignment.partitions())

        # give the assignor a chance to update internal state
        # based on the received assignment
        assignor.on_assignment(assignment)

        # reschedule the auto commit starting from now
        if self._auto_commit_task:
            self._auto_commit_task.reschedule()

        assigned = set(self._subscription.assigned_partitions())
        log.info("Setting newly assigned partitions %s for group %s",
                 assigned, self.group_id)

        # execute the user's callback after rebalance
        if self._subscription.listener:
            try:
                self._subscription.listener.on_partitions_assigned(assigned)
            except Exception:
                log.exception("User provided listener %s for group %s"
                              " failed on partition assignment: %s",
                              self._subscription.listener, self.group_id,
                              assigned)

    def _perform_assignment(self, leader_id, assignment_strategy, members):
        assignor = self._lookup_assignor(assignment_strategy)
        assert assignor, 'Invalid assignment protocol: %s' % assignment_strategy
        member_metadata = {}
        all_subscribed_topics = set()
        for member_id, metadata_bytes in members:
            metadata = ConsumerProtocol.METADATA.decode(metadata_bytes)
            member_metadata[member_id] = metadata
            all_subscribed_topics.update(metadata.subscription) # pylint: disable-msg=no-member

        # the leader will begin watching for changes to any of the topics
        # the group is interested in, which ensures that all metadata changes
        # will eventually be seen
        # Because assignment typically happens within response callbacks,
        # we cannot block on metadata updates here (no recursion into poll())
        self._subscription.group_subscribe(all_subscribed_topics)
        self._client.set_topics(self._subscription.group_subscription())

        # keep track of the metadata used for assignment so that we can check
        # after rebalance completion whether anything has changed
        self._cluster.request_update()
        self._assignment_snapshot = self._metadata_snapshot

        log.debug("Performing assignment for group %s using strategy %s"
                  " with subscriptions %s", self.group_id, assignor.name,
                  member_metadata)

        assignments = assignor.assign(self._cluster, member_metadata)

        log.debug("Finished assignment for group %s: %s", self.group_id, assignments)

        group_assignment = {}
        for member_id, assignment in six.iteritems(assignments):
            group_assignment[member_id] = assignment
        return group_assignment

    def _on_join_prepare(self, generation, member_id):
        # commit offsets prior to rebalance if auto-commit enabled
        self._maybe_auto_commit_offsets_sync()

        # execute the user's callback before rebalance
        log.info("Revoking previously assigned partitions %s for group %s",
                 self._subscription.assigned_partitions(), self.group_id)
        if self._subscription.listener:
            try:
                revoked = set(self._subscription.assigned_partitions())
                self._subscription.listener.on_partitions_revoked(revoked)
            except Exception:
                log.exception("User provided subscription listener %s"
                              " for group %s failed on_partitions_revoked",
                              self._subscription.listener, self.group_id)

        self._assignment_snapshot = None
        self._subscription.mark_for_reassignment()

    def need_rejoin(self):
        """Check whether the group should be rejoined

        Returns:
            bool: True if consumer should rejoin group, False otherwise
        """
        return (self._subscription.partitions_auto_assigned() and
               (super(ConsumerCoordinator, self).need_rejoin() or
                self._subscription.needs_partition_assignment))

    def refresh_committed_offsets_if_needed(self):
        """Fetch committed offsets for assigned partitions."""
        if self._subscription.needs_fetch_committed_offsets:
            offsets = self.fetch_committed_offsets(self._subscription.assigned_partitions())
            for partition, offset in six.iteritems(offsets):
                # verify assignment is still active
                if self._subscription.is_assigned(partition):
                    self._subscription.assignment[partition].committed = offset.offset
            self._subscription.needs_fetch_committed_offsets = False

    def fetch_committed_offsets(self, partitions):
        """Fetch the current committed offsets for specified partitions

        Arguments:
            partitions (list of TopicPartition): partitions to fetch

        Returns:
            dict: {TopicPartition: OffsetAndMetadata}
        """
        if not partitions:
            return {}

        while True:
            self.ensure_coordinator_known()

            # contact coordinator to fetch committed offsets
            future = self._send_offset_fetch_request(partitions)
            self._client.poll(future=future)

            if future.succeeded():
                return future.value

            if not future.retriable():
                raise future.exception # pylint: disable-msg=raising-bad-type

            time.sleep(self.config['retry_backoff_ms'] / 1000.0)

    def close(self, autocommit=True):
        """Close the coordinator, leave the current group,
        and reset local generation / member_id.

        Keyword Arguments:
            autocommit (bool): If auto-commit is configured for this consumer,
                this optional flag causes the consumer to attempt to commit any
                pending consumed offsets prior to close. Default: True
        """
        try:
            if autocommit:
                self._maybe_auto_commit_offsets_sync()
        finally:
            super(ConsumerCoordinator, self).close()

    def commit_offsets_async(self, offsets, callback=None):
        """Commit specific offsets asynchronously.

        Arguments:
            offsets (dict {TopicPartition: OffsetAndMetadata}): what to commit
            callback (callable, optional): called as callback(offsets, response)
                response will be either an Exception or a OffsetCommitResponse
                struct. This callback can be used to trigger custom actions when
                a commit request completes.
        Returns:
            Future: indicating whether the commit was successful or not
        """
        assert self.config['api_version'] >= (0, 8, 1), 'Unsupported Broker API'
        assert all(map(lambda k: isinstance(k, TopicPartition), offsets))
        assert all(map(lambda v: isinstance(v, OffsetAndMetadata),
                       offsets.values()))
        if callback is None:
            callback = self.config['default_offset_commit_callback']
        self._subscription.needs_fetch_committed_offsets = True
        future = self._send_offset_commit_request(offsets)
        future.add_both(callback, offsets)
        return future

    def commit_offsets_sync(self, offsets):
        """Commit specific offsets synchronously.

        This method will retry until the commit completes successfully or an
        unrecoverable error is encountered.

        Arguments:
            offsets (dict {TopicPartition: OffsetAndMetadata}): what to commit

        Raises error on failure
        """
        assert self.config['api_version'] >= (0, 8, 1), 'Unsupported Broker API'
        assert all(map(lambda k: isinstance(k, TopicPartition), offsets))
        assert all(map(lambda v: isinstance(v, OffsetAndMetadata),
                       offsets.values()))
        if not offsets:
            return

        while True:
            self.ensure_coordinator_known()

            future = self._send_offset_commit_request(offsets)
            self._client.poll(future=future)

            if future.succeeded():
                return future.value

            if not future.retriable():
                raise future.exception # pylint: disable-msg=raising-bad-type

            time.sleep(self.config['retry_backoff_ms'] / 1000.0)

    def _maybe_auto_commit_offsets_sync(self):
        if self._auto_commit_task is None:
            return

        try:
            self.commit_offsets_sync(self._subscription.all_consumed_offsets())

        # The three main group membership errors are known and should not
        # require a stacktrace -- just a warning
        except (Errors.UnknownMemberIdError,
                Errors.IllegalGenerationError,
                Errors.RebalanceInProgressError):
            log.warning("Offset commit failed: group membership out of date"
                        " This is likely to cause duplicate message"
                        " delivery.")
        except Exception:
            log.exception("Offset commit failed: This is likely to cause"
                          " duplicate message delivery")

    def _send_offset_commit_request(self, offsets):
        """Commit offsets for the specified list of topics and partitions.

        This is a non-blocking call which returns a request future that can be
        polled in the case of a synchronous commit or ignored in the
        asynchronous case.

        Arguments:
            offsets (dict of {TopicPartition: OffsetAndMetadata}): what should
                be committed

        Returns:
            Future: indicating whether the commit was successful or not
        """
        assert self.config['api_version'] >= (0, 8, 1), 'Unsupported Broker API'
        assert all(map(lambda k: isinstance(k, TopicPartition), offsets))
        assert all(map(lambda v: isinstance(v, OffsetAndMetadata),
                       offsets.values()))
        if not offsets:
            log.debug('No offsets to commit')
            return Future().success(True)

        elif self.coordinator_unknown():
            return Future().failure(Errors.GroupCoordinatorNotAvailableError)

        node_id = self.coordinator_id

        # create the offset commit request
        offset_data = collections.defaultdict(dict)
        for tp, offset in six.iteritems(offsets):
            offset_data[tp.topic][tp.partition] = offset

        if self.config['api_version'] >= (0, 9):
            request = OffsetCommitRequest[2](
                self.group_id,
                self.generation,
                self.member_id,
                OffsetCommitRequest[2].DEFAULT_RETENTION_TIME,
                [(
                    topic, [(
                        partition,
                        offset.offset,
                        offset.metadata
                    ) for partition, offset in six.iteritems(partitions)]
                ) for topic, partitions in six.iteritems(offset_data)]
            )
        elif self.config['api_version'] >= (0, 8, 2):
            request = OffsetCommitRequest[1](
                self.group_id, -1, '',
                [(
                    topic, [(
                        partition,
                        offset.offset,
                        -1,
                        offset.metadata
                    ) for partition, offset in six.iteritems(partitions)]
                ) for topic, partitions in six.iteritems(offset_data)]
            )
        elif self.config['api_version'] >= (0, 8, 1):
            request = OffsetCommitRequest[0](
                self.group_id,
                [(
                    topic, [(
                        partition,
                        offset.offset,
                        offset.metadata
                    ) for partition, offset in six.iteritems(partitions)]
                ) for topic, partitions in six.iteritems(offset_data)]
            )

        log.debug("Sending offset-commit request with %s for group %s to %s",
                  offsets, self.group_id, node_id)

        future = Future()
        _f = self._client.send(node_id, request)
        _f.add_callback(self._handle_offset_commit_response, offsets, future, time.time())
        _f.add_errback(self._failed_request, node_id, request, future)
        return future

    def _handle_offset_commit_response(self, offsets, future, send_time, response):
        # TODO look at adding request_latency_ms to response (like java kafka)
        self.consumer_sensors.commit_latency.record((time.time() - send_time) * 1000)
        unauthorized_topics = set()

        for topic, partitions in response.topics:
            for partition, error_code in partitions:
                tp = TopicPartition(topic, partition)
                offset = offsets[tp]

                error_type = Errors.for_code(error_code)
                if error_type is Errors.NoError:
                    log.debug("Group %s committed offset %s for partition %s",
                              self.group_id, offset, tp)
                    if self._subscription.is_assigned(tp):
                        self._subscription.assignment[tp].committed = offset.offset
                elif error_type is Errors.GroupAuthorizationFailedError:
                    log.error("Not authorized to commit offsets for group %s",
                              self.group_id)
                    future.failure(error_type(self.group_id))
                    return
                elif error_type is Errors.TopicAuthorizationFailedError:
                    unauthorized_topics.add(topic)
                elif error_type in (Errors.OffsetMetadataTooLargeError,
                                    Errors.InvalidCommitOffsetSizeError):
                    # raise the error to the user
                    log.debug("OffsetCommit for group %s failed on partition %s"
                              " %s", self.group_id, tp, error_type.__name__)
                    future.failure(error_type())
                    return
                elif error_type is Errors.GroupLoadInProgressError:
                    # just retry
                    log.debug("OffsetCommit for group %s failed: %s",
                              self.group_id, error_type.__name__)
                    future.failure(error_type(self.group_id))
                    return
                elif error_type in (Errors.GroupCoordinatorNotAvailableError,
                                    Errors.NotCoordinatorForGroupError,
                                    Errors.RequestTimedOutError):
                    log.debug("OffsetCommit for group %s failed: %s",
                              self.group_id, error_type.__name__)
                    self.coordinator_dead(error_type())
                    future.failure(error_type(self.group_id))
                    return
                elif error_type in (Errors.UnknownMemberIdError,
                                    Errors.IllegalGenerationError,
                                    Errors.RebalanceInProgressError):
                    # need to re-join group
                    error = error_type(self.group_id)
                    log.debug("OffsetCommit for group %s failed: %s",
                              self.group_id, error)
                    self._subscription.mark_for_reassignment()
                    future.failure(Errors.CommitFailedError(
                        "Commit cannot be completed since the group has"
                        " already rebalanced and assigned the partitions to"
                        " another member. This means that the time between"
                        " subsequent calls to poll() was longer than the"
                        " configured session.timeout.ms, which typically"
                        " implies that the poll loop is spending too much time"
                        " message processing. You can address this either by"
                        " increasing the session timeout or by reducing the"
                        " maximum size of batches returned in poll() with"
                        " max.poll.records."))
                    return
                else:
                    log.error("Group %s failed to commit partition %s at offset"
                              " %s: %s", self.group_id, tp, offset,
                              error_type.__name__)
                    future.failure(error_type())
                    return

        if unauthorized_topics:
            log.error("Not authorized to commit to topics %s for group %s",
                      unauthorized_topics, self.group_id)
            future.failure(Errors.TopicAuthorizationFailedError(unauthorized_topics))
        else:
            future.success(True)

    def _send_offset_fetch_request(self, partitions):
        """Fetch the committed offsets for a set of partitions.

        This is a non-blocking call. The returned future can be polled to get
        the actual offsets returned from the broker.

        Arguments:
            partitions (list of TopicPartition): the partitions to fetch

        Returns:
            Future: resolves to dict of offsets: {TopicPartition: int}
        """
        assert self.config['api_version'] >= (0, 8, 1), 'Unsupported Broker API'
        assert all(map(lambda k: isinstance(k, TopicPartition), partitions))
        if not partitions:
            return Future().success({})

        elif self.coordinator_unknown():
            return Future().failure(Errors.GroupCoordinatorNotAvailableError)

        node_id = self.coordinator_id

        # Verify node is ready
        if not self._client.ready(node_id):
            log.debug("Node %s not ready -- failing offset fetch request",
                      node_id)
            return Future().failure(Errors.NodeNotReadyError)

        log.debug("Group %s fetching committed offsets for partitions: %s",
                  self.group_id, partitions)
        # construct the request
        topic_partitions = collections.defaultdict(set)
        for tp in partitions:
            topic_partitions[tp.topic].add(tp.partition)

        if self.config['api_version'] >= (0, 8, 2):
            request = OffsetFetchRequest[1](
                self.group_id,
                list(topic_partitions.items())
            )
        else:
            request = OffsetFetchRequest[0](
                self.group_id,
                list(topic_partitions.items())
            )

        # send the request with a callback
        future = Future()
        _f = self._client.send(node_id, request)
        _f.add_callback(self._handle_offset_fetch_response, future)
        _f.add_errback(self._failed_request, node_id, request, future)
        return future

    def _handle_offset_fetch_response(self, future, response):
        offsets = {}
        for topic, partitions in response.topics:
            for partition, offset, metadata, error_code in partitions:
                tp = TopicPartition(topic, partition)
                error_type = Errors.for_code(error_code)
                if error_type is not Errors.NoError:
                    error = error_type()
                    log.debug("Group %s failed to fetch offset for partition"
                              " %s: %s", self.group_id, tp, error)
                    if error_type is Errors.GroupLoadInProgressError:
                        # just retry
                        future.failure(error)
                    elif error_type is Errors.NotCoordinatorForGroupError:
                        # re-discover the coordinator and retry
                        self.coordinator_dead(error_type())
                        future.failure(error)
                    elif error_type in (Errors.UnknownMemberIdError,
                                        Errors.IllegalGenerationError):
                        # need to re-join group
                        self._subscription.mark_for_reassignment()
                        future.failure(error)
                    elif error_type is Errors.UnknownTopicOrPartitionError:
                        log.warning("OffsetFetchRequest -- unknown topic %s"
                                    " (have you committed any offsets yet?)",
                                    topic)
                        continue
                    else:
                        log.error("Unknown error fetching offsets for %s: %s",
                                  tp, error)
                        future.failure(error)
                    return
                elif offset >= 0:
                    # record the position with the offset
                    # (-1 indicates no committed offset to fetch)
                    offsets[tp] = OffsetAndMetadata(offset, metadata)
                else:
                    log.debug("Group %s has no committed offset for partition"
                              " %s", self.group_id, tp)
        future.success(offsets)


class AutoCommitTask(object):
    def __init__(self, coordinator, interval):
        self._coordinator = coordinator
        self._client = coordinator._client
        self._interval = interval

    def reschedule(self, at=None):
        if at is None:
            at = time.time() + self._interval
        self._client.schedule(self, at)

    def __call__(self):
        if self._coordinator.coordinator_unknown():
            log.debug("Cannot auto-commit offsets for group %s because the"
                      " coordinator is unknown", self._coordinator.group_id)
            backoff = self._coordinator.config['retry_backoff_ms'] / 1000.0
            self.reschedule(time.time() + backoff)
            return

        self._coordinator.commit_offsets_async(
            self._coordinator._subscription.all_consumed_offsets(),
            self._handle_commit_response)

    def _handle_commit_response(self, offsets, result):
        if result is True:
            log.debug("Successfully auto-committed offsets for group %s",
                      self._coordinator.group_id)
            next_at = time.time() + self._interval
        elif not isinstance(result, BaseException):
            raise Errors.IllegalStateError(
                'Unrecognized result in _handle_commit_response: %s'
                % result)
        elif hasattr(result, 'retriable') and result.retriable:
            log.debug("Failed to auto-commit offsets for group %s: %s,"
                      " will retry immediately", self._coordinator.group_id,
                      result)
            next_at = time.time()
        else:
            log.warning("Auto offset commit failed for group %s: %s",
                        self._coordinator.group_id, result)
            next_at = time.time() + self._interval

        self.reschedule(next_at)


class ConsumerCoordinatorMetrics(object):
    def __init__(self, metrics, metric_group_prefix, subscription):
        self.metrics = metrics
        self.metric_group_name = '%s-coordinator-metrics' % metric_group_prefix

        self.commit_latency = metrics.sensor('commit-latency')
        self.commit_latency.add(metrics.metric_name(
            'commit-latency-avg', self.metric_group_name,
            'The average time taken for a commit request'), Avg())
        self.commit_latency.add(metrics.metric_name(
            'commit-latency-max', self.metric_group_name,
            'The max time taken for a commit request'), Max())
        self.commit_latency.add(metrics.metric_name(
            'commit-rate', self.metric_group_name,
            'The number of commit calls per second'), Rate(sampled_stat=Count()))

        num_parts = AnonMeasurable(lambda config, now:
                                   len(subscription.assigned_partitions()))
        metrics.add_metric(metrics.metric_name(
            'assigned-partitions', self.metric_group_name,
            'The number of partitions currently assigned to this consumer'),
            num_parts)
