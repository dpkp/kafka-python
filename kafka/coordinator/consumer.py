import collections
import copy
import functools
import inspect
import logging
import time

from kafka.coordinator.base import BaseCoordinator, Generation
from kafka.coordinator.assignors.abstract import RebalanceProtocol, AbstractPartitionAssignor
from kafka.coordinator.assignors.range import RangePartitionAssignor
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from kafka.coordinator.assignors.sticky.sticky_assignor import StickyPartitionAssignor
from kafka.protocol.consumer.metadata import (
    ConsumerProtocolType, ConsumerProtocolSubscription, ConsumerProtocolAssignment,
)
import kafka.errors as Errors
from kafka.future import Future
from kafka.metrics import AnonMeasurable
from kafka.metrics.stats import Avg, Count, Max, Rate
from kafka.protocol.consumer import OffsetCommitRequest, OffsetFetchRequest, IsolationLevel
from kafka.structs import OffsetAndMetadata, TopicPartition
from kafka.util import Timer, WeakMethod


log = logging.getLogger(__name__)


class ConsumerCoordinator(BaseCoordinator):
    """This class manages the coordination process with the consumer coordinator."""
    DEFAULT_CONFIG = BaseCoordinator.DEFAULT_CONFIG.copy()
    DEFAULT_CONFIG.update({
        'enable_auto_commit': True,
        'auto_commit_interval_ms': 5000,
        'default_offset_commit_callback': None,
        'assignors': (RangePartitionAssignor, RoundRobinPartitionAssignor, StickyPartitionAssignor),
        'exclude_internal_topics': True,
        'isolation_level': 'read_uncommitted',
        'metric_group_prefix': 'consumer'
    })

    def __init__(self, client, subscription, **configs):
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
                or None. This callback can be used to trigger custom actions when
                a commit request completes.
            assignors (list): List of objects to use to distribute partition
                ownership amongst consumer instances when group management is
                used. Default: [RangePartitionAssignor, RoundRobinPartitionAssignor, StickyPartitionAssignor]
            retry_backoff_ms (int): Milliseconds to backoff when retrying on
                errors. Default: 100.
            exclude_internal_topics (bool): Whether records from internal topics
                (such as offsets) should be exposed to the consumer. If set to
                True the only way to receive records from an internal topic is
                subscribing to it. Requires 0.10+. Default: True
        """
        super().__init__(client, **configs)

        self.config = copy.copy(self.DEFAULT_CONFIG)
        for key in self.config:
            if key in configs:
                self.config[key] = configs[key]

        try:
            self._isolation_level = IsolationLevel.build_from(self.config['isolation_level'])
        except ValueError:
            raise Errors.KafkaConfigurationError('Unrecognized isolation_level') from None

        self._subscription = subscription
        self._is_leader = False
        self._rebalance_protocol = None
        self._joined_subscription = set()
        self._metadata_snapshot = self._build_metadata_snapshot(subscription, self._cluster)
        self._assignment_snapshot = None
        self.auto_commit_interval = self.config['auto_commit_interval_ms'] / 1000
        self.next_auto_commit_deadline = None
        self.completed_offset_commits = collections.deque()
        self._offset_fetch_futures = dict()
        self._async_commit_fenced = False

        if self.config['default_offset_commit_callback'] is None:
            self.config['default_offset_commit_callback'] = self._default_offset_commit_callback

        if self.config['group_id'] is not None:
            if self._use_group_apis:
                if not self.config['assignors']:
                    raise Errors.KafkaConfigurationError('Coordinator requires assignors')

        if self.config['enable_auto_commit']:
            if not self._use_offset_apis:
                log.warning('Broker version (%s) does not support offset'
                            ' commits; disabling auto-commit.',
                            self.config['api_version'])
                self.config['enable_auto_commit'] = False
            elif self.config['group_id'] is None:
                log.warning('group_id is None: disabling auto-commit.')
                self.config['enable_auto_commit'] = False
            else:
                self.next_auto_commit_deadline = time.monotonic() + self.auto_commit_interval

        if self.config['metrics']:
            self._consumer_sensors = ConsumerCoordinatorMetrics(
                self.config['metrics'], self.config['metric_group_prefix'], self._subscription)
        else:
            self._consumer_sensors = None

        self._assignors = {}
        for klass in self.config['assignors']:
            if isinstance(klass, AbstractPartitionAssignor):
                assignor = klass
            else:
                assignor = klass()
            self._assignors[assignor.name] = assignor
        # KIP-429: all configured assignors must agree on a single
        # RebalanceProtocol mode. Mixing EAGER and COOPERATIVE
        # assignors in the same consumer is unsafe - at JoinGroup time
        # the broker picks one assignor, and the consumer needs to
        # know up front whether to do eager (full) or cooperative
        # (incremental) revocation.
        self._rebalance_protocol = self._validate_rebalance_protocol()
        self._cluster.request_update()
        self._cluster.add_listener(WeakMethod(self._handle_metadata_update))

    def _validate_rebalance_protocol(self):
        """Return the single :class:`RebalanceProtocol` mode that all
        configured assignors support; raise
        :class:`KafkaConfigurationError` if they don't agree.
        """
        if not self._assignors:
            return RebalanceProtocol.EAGER
        common = None
        for assignor in self._assignors.values():
            supported = set(assignor.supported_protocols())
            common = supported if common is None else common & supported
        if not common:
            names = [a.name for a in self._assignors.values()]
            raise Errors.KafkaConfigurationError(
                "Specified partition_assignment_strategy assignors %s do not"
                " support a common RebalanceProtocol. Mixing EAGER and"
                " COOPERATIVE assignors in a single consumer is not"
                " supported." % (names,))
        # Pick the highest mode they all agree on (EAGER < COOPERATIVE).
        return max(common)

    @property
    def _use_offset_apis(self):
        return self.config['api_version'] >= (0, 8, 1)

    def protocol_type(self):
        return ConsumerProtocolType

    def group_protocols(self):
        """Returns list of preferred (protocols, metadata)"""
        if self._subscription.subscription is None:
            raise Errors.IllegalStateError('Consumer has not subscribed to topics')
        # dpkp note: I really dislike this.
        # why? because we are using this strange method group_protocols,
        # which is seemingly innocuous, to set internal state (_joined_subscription)
        # that is later used to check whether metadata has changed since we joined a group
        # but there is no guarantee that this method, group_protocols, will get called
        # in the correct sequence or that it will only be called when we want it to be.
        # So this really should be moved elsewhere, but I don't have the energy to
        # work that out right now. If you read this at some later date after the mutable
        # state has bitten you... I'm sorry! It mimics the java client, and that's the
        # best I've got for now.
        self._joined_subscription = set(self._subscription.subscription)
        metadata_list = []
        for assignor in self._assignors:
            metadata = self._assignors[assignor].metadata(self._joined_subscription)
            group_protocol = (assignor, metadata)
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
                self._cluster.set_topics(self._subscription.group_subscription())

        # check if there are any changes to the metadata which should trigger
        # a rebalance
        if self._subscription.partitions_auto_assigned():
            metadata_snapshot = self._build_metadata_snapshot(self._subscription, cluster)
            if self._metadata_snapshot != metadata_snapshot:
                self._metadata_snapshot = metadata_snapshot

                # If we haven't got group coordinator support,
                # just assign all partitions locally
                if self._auto_assign_all_partitions():
                    self._subscription.assign_from_subscribed([
                        TopicPartition(topic, partition)
                        for topic in self._subscription.subscription
                        for partition in self._metadata_snapshot[topic]
                    ])

    def _auto_assign_all_partitions(self):
        # For users that use "subscribe" without group support,
        # we will simply assign all partitions to this consumer
        if not self._use_group_apis:
            return True
        elif self.config['group_id'] is None:
            return True
        else:
            return False

    def _build_metadata_snapshot(self, subscription, cluster):
        metadata_snapshot = {}
        for topic in subscription.group_subscription():
            partitions = cluster.partitions_for_topic(topic)
            metadata_snapshot[topic] = partitions or set()
        return metadata_snapshot

    def _lookup_assignor(self, name):
        return self._assignors.get(name, None)

    # Threshold above which a rebalance-listener invocation is logged as a
    # warning. Sync listeners on the IO loop will block heartbeats while
    # they run; even async ones delay rebalance progress. 1s is a soft
    # ceiling: well below default heartbeat_interval_ms (3s) and
    # session_timeout_ms (45s).
    _REBALANCE_LISTENER_WARN_SECS = 1.0

    async def _invoke_rebalance_listener_async(self, method_name, arg):
        """Invoke a rebalance-listener method (sync or async), timing the call.

        Awaits if the method is a coroutine function; otherwise calls inline.
        Logs a warning if the call exceeds
        :data:`_REBALANCE_LISTENER_WARN_SECS`. Caller wraps in try/except.
        """
        cb = getattr(self._subscription.rebalance_listener, method_name)
        start = time.monotonic()
        if inspect.iscoroutinefunction(cb):
            await cb(arg)
        else:
            cb(arg)
        elapsed = time.monotonic() - start
        if elapsed > self._REBALANCE_LISTENER_WARN_SECS:
            log.warning(
                "Rebalance listener %s.%s for group %s took %.3fs."
                " Sync listeners block the consumer event loop (including"
                " heartbeats) -- consider AsyncConsumerRebalanceListener or"
                " wrap blocking work in a worker thread.",
                type(self._subscription.rebalance_listener).__name__,
                method_name, self.group_id, elapsed)

    async def _on_join_complete_async(self, generation, member_id, protocol,
                                      member_assignment_bytes):
        # only the leader is responsible for monitoring for metadata changes
        # (i.e. partition changes)
        if not self._is_leader:
            self._assignment_snapshot = None

        assignor = self._lookup_assignor(protocol)
        if not assignor:
            raise ValueError('Coordinator selected invalid assignment protocol: %s' % (protocol,))

        assignment = ConsumerProtocolAssignment.decode(member_assignment_bytes)
        new_assigned = set(assignment.partitions())

        # KIP-429: under COOPERATIVE, compute the diff between what we
        # currently own and what the leader just assigned. Revoke the
        # partitions we lost; only invoke on_partitions_assigned for
        # the newly-added ones. If we lost any partitions, request a
        # follow-up rebalance so the revoked partitions can land on
        # their intended new owner.
        # Listener hook exceptions are captured, cleanup completes,
        # and we re-raise at the end as a KafkaError.
        # In the cooperative branch both listeners (revoked + assigned)
        # are invoked even if the first throws - matches Java's
        # invokePartitionsRevoked / invokePartitionsAssigned pattern
        # where the later call overwrites the captured exception.
        listener_exc = None

        if self._rebalance_protocol == RebalanceProtocol.COOPERATIVE:
            currently_owned = set(self._subscription.assigned_partitions())
            revoked = currently_owned - new_assigned
            added = new_assigned - currently_owned

            try:
                self._subscription.assign_from_subscribed(sorted(new_assigned))
            except ValueError as e:
                log.warning("Cooperative assignment rejected: %s."
                            " Probably due to a deleted topic."
                            " Requesting re-join.", e)
                self.request_rejoin()
                return

            assignor.on_assignment(assignment, generation)
            self.next_auto_commit_deadline = time.monotonic() + self.auto_commit_interval

            log.info("Cooperative rebalance complete for group %s:"
                     " owned=%s, assigned=%s, revoked=%s, added=%s",
                     self.group_id, currently_owned, new_assigned, revoked, added)

            if self._subscription.rebalance_listener:
                if revoked:
                    try:
                        await self._invoke_rebalance_listener_async(
                            'on_partitions_revoked', revoked)
                    except Exception as exc:
                        log.exception(
                            "User provided rebalance listener %s for group %s"
                            " failed on_partitions_revoked: %s",
                            self._subscription.rebalance_listener,
                            self.group_id, revoked)
                        listener_exc = exc
                if added:
                    try:
                        await self._invoke_rebalance_listener_async(
                            'on_partitions_assigned', added)
                    except Exception as exc:
                        log.exception(
                            "User provided rebalance listener %s for group %s"
                            " failed on_partitions_assigned: %s",
                            self._subscription.rebalance_listener,
                            self.group_id, added)
                        listener_exc = exc

            if revoked:
                # Round 2: the partitions we just revoked should now
                # be unowned cluster-wide and can be assigned to
                # their intended new owners. Trigger a follow-up
                # rebalance to surface those assignments.
                log.info("Triggering follow-up rebalance for group %s to"
                         " complete cooperative move of %d partition(s)",
                         self.group_id, len(revoked))
                self.request_rejoin()

            if listener_exc is not None:
                raise Errors.KafkaError(
                    "User rebalance callback throws an error") from listener_exc
            return

        # EAGER mode (legacy): replace the full assignment and invoke
        # on_partitions_assigned with the entire new set.
        try:
            self._subscription.assign_from_subscribed(assignment.partitions())
        except ValueError as e:
            log.warning("Assignment rejected: %s. Probably due to a"
                        " deleted topic. Requesting re-join.", e)
            self.request_rejoin()
            return

        # give the assignor a chance to update internal state
        # based on the received assignment
        assignor.on_assignment(assignment, generation)

        # reschedule the auto commit starting from now
        self.next_auto_commit_deadline = time.monotonic() + self.auto_commit_interval

        assigned = set(self._subscription.assigned_partitions())
        log.info("Setting newly assigned partitions %s for group %s",
                 assigned, self.group_id)

        # execute the user's callback after rebalance
        if self._subscription.rebalance_listener:
            try:
                await self._invoke_rebalance_listener_async(
                    'on_partitions_assigned', assigned)
            except Exception as exc:
                log.exception("User provided rebalance listener %s for group %s"
                              " failed on partition assignment: %s",
                              self._subscription.rebalance_listener, self.group_id,
                              assigned)
                listener_exc = exc

        if listener_exc is not None:
            raise Errors.KafkaError(
                "User rebalance callback throws an error") from listener_exc

    def poll(self, timeout_ms=None):
        """
        Poll for coordinator events. Only applicable if group_id is set, and
        broker version supports GroupCoordinators. This ensures that the
        coordinator is known, and if using automatic partition assignment,
        ensures that the consumer has joined the group. This also handles
        periodic offset commits if they are enabled.
        """
        if self.group_id is None:
            return True

        timer = Timer(timeout_ms)
        try:
            self._invoke_completed_offset_commit_callbacks()
            if not self.ensure_coordinator_ready(timeout_ms=timer.timeout_ms):
                log.debug('coordinator.poll: timeout in ensure_coordinator_ready; returning early')
                return False

            if self._use_group_apis and self._subscription.partitions_auto_assigned():
                if self.need_rejoin():
                    # due to a race condition between the initial metadata fetch and the
                    # initial rebalance, we need to ensure that the metadata is fresh
                    # before joining initially, and then request the metadata update. If
                    # metadata update arrives while the rebalance is still pending (for
                    # example, when the join group is still inflight), then we will lose
                    # track of the fact that we need to rebalance again to reflect the
                    # change to the topic subscription. Without ensuring that the
                    # metadata is fresh, any metadata update that changes the topic
                    # subscriptions and arrives while a rebalance is in progress will
                    # essentially be ignored. See KAFKA-3949 for the complete
                    # description of the problem.
                    if self._subscription.subscribed_pattern:
                        metadata_update = self._cluster.request_update()
                        try:
                            self._net.run(
                                self._manager.wait_for, metadata_update, timer.timeout_ms)
                        except Errors.KafkaTimeoutError:
                            log.debug('coordinator.poll: timeout updating metadata; returning early')
                            return False

                    if not self.ensure_active_group(timeout_ms=timer.timeout_ms):
                        log.debug('coordinator.poll: timeout in ensure_active_group; returning early')
                        return False

                self.poll_heartbeat()

            self._maybe_auto_commit_offsets_async()
            return True

        except Errors.KafkaTimeoutError:
            return False

    def time_to_next_poll(self):
        """Return seconds (float) remaining until :meth:`.poll` should be called again"""
        if not self.config['enable_auto_commit']:
            return self.time_to_next_heartbeat()

        if time.monotonic() > self.next_auto_commit_deadline:
            return 0

        return min(self.next_auto_commit_deadline - time.monotonic(),
                   self.time_to_next_heartbeat())

    def _perform_assignment(self, leader_id, protocol_name, members):
        assignor = self._lookup_assignor(protocol_name)
        if not assignor:
            raise ValueError('Invalid assignment protocol: %s' % (protocol_name,))
        all_subscribed_topics = set()
        for member in members:
            member.metadata = ConsumerProtocolSubscription.decode(member.metadata)
            all_subscribed_topics.update(member.metadata.topics)

        # the leader will begin watching for changes to any of the topics
        # the group is interested in, which ensures that all metadata changes
        # will eventually be seen
        # Because assignment typically happens within response callbacks,
        # we cannot block on metadata updates here (no recursion into poll())
        self._subscription.group_subscribe(all_subscribed_topics)
        self._cluster.set_topics(self._subscription.group_subscription())

        # keep track of the metadata used for assignment so that we can check
        # after rebalance completion whether anything has changed
        self._cluster.request_update()
        self._is_leader = True
        self._assignment_snapshot = self._metadata_snapshot

        log.debug("Performing assignment for group %s using strategy %s"
                  " with subscriptions %s", self.group_id, assignor.name,
                  members)

        assignments = assignor.assign(self._cluster, members)

        log.debug("Finished assignment for group %s: %s", self.group_id, assignments)

        group_assignment = {}
        for member_id, assignment in assignments.items():
            group_assignment[member_id] = assignment
        return group_assignment

    async def _on_join_prepare_async(self, generation, member_id, timeout_ms=None):
        # Exceptions raised by user rebalance-listener callbacks are captured
        # here, do not abort the cleanup, and are re-raised at the end as a
        # KafkaError so the caller (consumer.poll()) sees them. Matches Java.
        listener_exc = None

        if self._generation.is_lost():
            lost = set(self._subscription.assigned_partitions())
            if lost:
                log.info("Group %s lost membership; forcibly revoking %s",
                         self.group_id, lost)
                self._subscription.mark_pending_revocation(lost)
                if self._subscription.rebalance_listener:
                    try:
                        await self._invoke_rebalance_listener_async(
                            'on_partitions_lost', lost)
                    except Exception as exc:
                        log.exception("User provided subscription rebalance listener %s"
                                      " for group %s failed on_partitions_lost",
                                      self._subscription.rebalance_listener, self.group_id)
                        listener_exc = exc
                self._subscription.assign_from_subscribed([])
                self._is_leader = False
                self._subscription.reset_group_subscription()
                if listener_exc is not None:
                    raise Errors.KafkaError(
                        "User rebalance callback throws an error") from listener_exc
                return
            # else: generation is lost but we have no partitions to
            # lose - this is the initial-join case. Fall through to the
            # normal auto-commit + EAGER/COOPERATIVE path.

        # commit offsets prior to rebalance if auto-commit enabled
        if self.config['enable_auto_commit']:
            try:
                await self._commit_offsets_sync_async(
                    self._subscription.all_consumed_offsets(),
                    timeout_ms=timeout_ms)
            except (Errors.UnknownMemberIdError,
                    Errors.IllegalGenerationError,
                    Errors.RebalanceInProgressError):
                log.warning("Pre-rebalance offset commit failed: group membership"
                            " out of date. This is likely to cause duplicate"
                            " message delivery.")
            except Exception:
                log.exception("Pre-rebalance offset commit failed: This is likely"
                              " to cause duplicate message delivery")

        # Under EAGER, notify the user that the full current
        # assignment is about to be revoked so they can flush state /
        # commit offsets before the rebalance. The partitions remain
        # in self._subscription.assignment until _on_join_complete
        # replaces it via assign_from_subscribed - this listener call
        # is a *notification*, not the actual state mutation.
        #
        # Under COOPERATIVE we keep most of the assignment across
        # JoinGroup, but partitions whose topic is no longer in the
        # subscription (e.g. the user just unsubscribed from a topic)
        # are revoked here, before the JoinGroup, so the listener
        # gets to commit those offsets while we're still the
        # recognised owner.
        if self._rebalance_protocol == RebalanceProtocol.EAGER:
            log.info("Revoking previously assigned partitions %s for group %s",
                     self._subscription.assigned_partitions(), self.group_id)
            if self._subscription.rebalance_listener:
                try:
                    revoked = set(self._subscription.assigned_partitions())
                    self._subscription.mark_pending_revocation(revoked)
                    await self._invoke_rebalance_listener_async(
                        'on_partitions_revoked', revoked)
                except Exception as exc:
                    log.exception("User provided subscription rebalance listener %s"
                                  " for group %s failed on_partitions_revoked",
                                  self._subscription.rebalance_listener, self.group_id)
                    listener_exc = exc

        elif self._rebalance_protocol == RebalanceProtocol.COOPERATIVE:
            owned = set(self._subscription.assigned_partitions())
            subscribed_topics = self._subscription.subscription or set()
            revoked = {tp for tp in owned if tp.topic not in subscribed_topics}
            if revoked:
                log.info("Cooperative pre-rebalance for group %s: revoking %s"
                         " (no longer in subscription)", self.group_id, revoked)
                self._subscription.mark_pending_revocation(revoked)
                if self._subscription.rebalance_listener:
                    try:
                        await self._invoke_rebalance_listener_async(
                            'on_partitions_revoked', revoked)
                    except Exception as exc:
                        log.exception("User provided subscription rebalance listener %s"
                                      " for group %s failed on_partitions_revoked",
                                      self._subscription.rebalance_listener, self.group_id)
                        listener_exc = exc
                self._subscription.assign_from_subscribed(sorted(owned - revoked))

        self._is_leader = False
        self._subscription.reset_group_subscription()

        if listener_exc is not None:
            raise Errors.KafkaError(
                "User rebalance callback throws an error") from listener_exc

    def need_rejoin(self):
        """Check whether the group should be rejoined

        Returns:
            bool: True if consumer should rejoin group, False otherwise
        """
        if not self._subscription.partitions_auto_assigned():
            log.debug("need_rejoin: False (partitions not auto-assigned)")
            return False

        if self._auto_assign_all_partitions():
            log.debug("need_rejoin: False (auto-assign all partitions)")
            return False

        # we need to rejoin if we performed the assignment and metadata has changed
        if (self._assignment_snapshot is not None
            and self._assignment_snapshot != self._metadata_snapshot):
            log.debug("need_rejoin: True (assignment_snapshot != metadata_snapshot: %s != %s)",
                      self._assignment_snapshot, self._metadata_snapshot)
            return True

        # we need to join if our subscription has changed since the last join
        if (self._joined_subscription is not None
            and self._joined_subscription != self._subscription.subscription):
            log.debug("need_rejoin: True (joined_subscription != subscription: %s != %s)",
                      self._joined_subscription, self._subscription.subscription)
            return True

        parent = super().need_rejoin()
        log.debug("need_rejoin: %s (from base.rejoin_needed; assignment_snapshot=%s metadata_snapshot=%s joined_subscription=%s)",
                  parent, self._assignment_snapshot, self._metadata_snapshot, self._joined_subscription)
        return parent

    def refresh_committed_offsets_if_needed(self, timeout_ms=None):
        """Fetch committed offsets for assigned partitions."""
        return self._net.run(self.refresh_committed_offsets_if_needed_async, timeout_ms)

    async def refresh_committed_offsets_if_needed_async(self, timeout_ms=None):
        missing_fetch_positions = set(self._subscription.missing_fetch_positions())
        try:
            offsets = await self.fetch_committed_offsets_async(missing_fetch_positions, timeout_ms=timeout_ms)
        except Errors.KafkaTimeoutError:
            return False
        for partition, offset in offsets.items():
            log.debug("Setting offset for partition %s to the committed offset %s", partition, offset.offset)
            self._subscription.seek(partition, offset.offset)
        return True

    def fetch_committed_offsets(self, partitions, timeout_ms=None):
        """Fetch the current committed offsets for specified partitions

        Arguments:
            partitions (list of TopicPartition): partitions to fetch

        Returns:
            dict: {TopicPartition: OffsetAndMetadata}

        Raises:
            KafkaTimeoutError if timeout_ms provided
        """
        if not partitions:
            return {}
        return self._net.run(self.fetch_committed_offsets_async, partitions, timeout_ms)

    async def fetch_committed_offsets_async(self, partitions, timeout_ms=None):
        """Async variant of :meth:`fetch_committed_offsets`."""
        if not partitions:
            return {}

        future_key = frozenset(partitions)
        timer = Timer(timeout_ms)
        while True:
            if not await self.ensure_coordinator_ready_async(timeout_ms=timer.timeout_ms):
                timer.maybe_raise()

            # contact coordinator to fetch committed offsets
            if future_key in self._offset_fetch_futures:
                future = self._offset_fetch_futures[future_key]
            else:
                future = self._manager.call_soon(self._send_offset_fetch_request, partitions)
                self._offset_fetch_futures[future_key] = future

            try:
                await self._manager.wait_for(future, timer.timeout_ms)
            except Errors.KafkaTimeoutError:
                pass
            except BaseException:
                # handled below via future.is_done / retriable; cleanup happens too
                pass

            if future.is_done:
                if future_key in self._offset_fetch_futures:
                    del self._offset_fetch_futures[future_key]

                if future.succeeded():
                    return future.value

                elif not future.retriable():
                    raise future.exception  # pylint: disable-msg=raising-bad-type

            # future failed but is retriable, or is not done yet
            delay_ms = self.config['retry_backoff_ms']
            if timer.timeout_ms is not None:
                delay_ms = min(delay_ms, timer.timeout_ms)
            if delay_ms > 0:
                await self._net.sleep(delay_ms / 1000)
            timer.maybe_raise()

    async def _on_close_prepare_async(self):
        """Notify the rebalance listener that our partitions are being
        revoked because the consumer is closing. Mirrors Java's
        ConsumerCoordinator.onLeavePrepare.

        Only fires for auto-assigned (group) subscriptions that currently
        own partitions. If the group membership has already been lost
        (forced eviction), on_partitions_lost is invoked instead of
        on_partitions_revoked, since the user cannot commit offsets for
        partitions the broker has already reassigned.

        Runs before we leave the group so a sync listener can still commit
        offsets while we are the recognised owner. Listener exceptions are
        captured, the local assignment is cleared regardless, and the
        exception is re-raised as a KafkaError after cleanup.
        """
        if not self._subscription.partitions_auto_assigned():
            return
        revoked = set(self._subscription.assigned_partitions())
        if not revoked:
            return

        if self._generation.is_lost():
            method = 'on_partitions_lost'
        else:
            method = 'on_partitions_revoked'

        log.info("Revoking previously assigned partitions %s for group %s"
                 " on close", revoked, self.group_id)
        self._subscription.mark_pending_revocation(revoked)
        listener_exc = None
        if self._subscription.rebalance_listener:
            try:
                await self._invoke_rebalance_listener_async(method, revoked)
            except Exception as exc:
                log.exception("User provided subscription rebalance listener %s"
                              " for group %s failed %s on close",
                              self._subscription.rebalance_listener,
                              self.group_id, method)
                listener_exc = exc
        self._subscription.assign_from_subscribed([])
        self._is_leader = False
        self._subscription.reset_group_subscription()
        if listener_exc is not None:
            raise Errors.KafkaError(
                "User rebalance callback throws an error") from listener_exc

    def close(self, autocommit=True, timeout_ms=None):
        """Close the coordinator, leave the current group,
        and reset local generation / member_id.

        Keyword Arguments:
            autocommit (bool): If auto-commit is configured for this consumer,
                this optional flag causes the consumer to attempt to commit any
                pending consumed offsets prior to close. Default: True
        """
        try:
            if autocommit:
                self._maybe_auto_commit_offsets_sync(timeout_ms=timeout_ms)
            self._net.run(self._on_close_prepare_async)
        finally:
            self._cluster.remove_listener(WeakMethod(self._handle_metadata_update))
            super().close(timeout_ms=timeout_ms)

    def _invoke_completed_offset_commit_callbacks(self):
        if self._async_commit_fenced:
            raise Errors.FencedInstanceIdError("Got fenced exception for group_instance_id %s" % (self.group_instance_id,))
        while self.completed_offset_commits:
            callback, offsets, res_or_exc = self.completed_offset_commits.popleft()
            callback(offsets, res_or_exc)

    def commit_offsets_async(self, offsets, callback=None):
        """Commit specific offsets asynchronously.

        Arguments:
            offsets (dict {TopicPartition: OffsetAndMetadata}): what to commit
            callback (callable, optional): called as callback(offsets, response)
                response will be either an Exception or a OffsetCommitResponse
                struct. This callback can be used to trigger custom actions when
                a commit request completes.

        Returns:
            kafka.future.Future
        """
        self._invoke_completed_offset_commit_callbacks()
        if not self.coordinator_unknown():
            future = self._do_commit_offsets_async(offsets, callback)
        else:
            # we don't know the current coordinator, so try to find it and then
            # send the commit or fail (we don't want recursive retries which can
            # cause offset commits to arrive out of order). Note that there may
            # be multiple offset commits chained to the same coordinator lookup
            # request. This is fine because the listeners will be invoked in the
            # same order that they were added. Note also that BaseCoordinator
            # prevents multiple concurrent coordinator lookup requests.
            future = self.lookup_coordinator()
            future.add_callback(lambda r: functools.partial(self._do_commit_offsets_async, offsets, callback)())
            if callback:
                future.add_errback(lambda e: self.completed_offset_commits.appendleft((callback, offsets, e)))
        return future

    def _do_commit_offsets_async(self, offsets, callback=None):
        if not self._use_offset_apis:
            raise Errors.UnsupportedVersionError('OffsetCommitRequest requires 0.8.1+ broker')
        if not all(map(lambda k: isinstance(k, TopicPartition), offsets)) or \
           not all(map(lambda v: isinstance(v, OffsetAndMetadata), offsets.values())):
            raise TypeError('offsets must be dict[TopicPartition, OffsetAndMetadata]')
        if callback is None:
            callback = self.config['default_offset_commit_callback']
        future = self._manager.call_soon(self._send_offset_commit_request, offsets)
        future.add_both(lambda res: self.completed_offset_commits.appendleft((callback, offsets, res)))
        def _maybe_set_async_commit_fenced(exc):
            if isinstance(exc, Errors.FencedInstanceIdError):
                self._async_commit_fenced = True
        future.add_errback(_maybe_set_async_commit_fenced)
        return future

    def commit_offsets_sync(self, offsets, timeout_ms=None):
        """Commit specific offsets synchronously.

        This method will retry until the commit completes successfully or an
        unrecoverable error is encountered.

        Arguments:
            offsets (dict {TopicPartition: OffsetAndMetadata}): what to commit

        Raises error on failure
        """
        if not self._use_offset_apis:
            raise Errors.UnsupportedVersionError('OffsetCommitRequest requires 0.8.1+ broker')
        if not all(map(lambda k: isinstance(k, TopicPartition), offsets)) or \
           not all(map(lambda v: isinstance(v, OffsetAndMetadata), offsets.values())):
            raise TypeError('offsets must be dict[TopicPartition, OffsetAndMetadata]')
        self._invoke_completed_offset_commit_callbacks()
        return self._net.run(self._commit_offsets_sync_async, offsets, timeout_ms)

    async def _commit_offsets_sync_async(self, offsets, timeout_ms=None):
        if not offsets:
            return
        # Default to request_timeout_ms, matching offsets_by_times / _reset_offsets_async
        if timeout_ms is None:
            timeout_ms = self.config['request_timeout_ms']
        timer = Timer(timeout_ms)
        while True:
            await self.ensure_coordinator_ready_async(timeout_ms=timer.timeout_ms)

            future = self._manager.call_soon(self._send_offset_commit_request, offsets)
            try:
                await self._manager.wait_for(future, timer.timeout_ms)
            except Errors.KafkaTimeoutError:
                pass
            except BaseException:
                # handled below via future.is_done / retriable
                pass

            if future.is_done:
                if future.succeeded():
                    return future.value

                elif not future.retriable():
                    raise future.exception  # pylint: disable-msg=raising-bad-type

            # future failed but is retriable, or it is still pending
            delay_ms = self.config['retry_backoff_ms']
            if timer.timeout_ms is not None:
                delay_ms = min(delay_ms, timer.timeout_ms)
            if delay_ms > 0:
                await self._net.sleep(delay_ms / 1000)
            timer.maybe_raise()

    def _maybe_auto_commit_offsets_sync(self, timeout_ms=None):
        if self.config['enable_auto_commit']:
            try:
                self.commit_offsets_sync(self._subscription.all_consumed_offsets(), timeout_ms=timeout_ms)

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

    async def _send_offset_commit_request(self, offsets):
        """Commit offsets for the specified list of topics and partitions.

        Arguments:
            offsets (dict of {TopicPartition: OffsetAndMetadata}): what should
                be committed.

        Returns: None on success.
        Raises:
            UnsupportedVersionError if broker is too old.
            CoordinatorNotAvailableError if the coordinator is unknown.
            RebalanceInProgressError / CommitFailedError if generation is
                not stable.
            Other broker-side OffsetCommit errors propagated via
                _handle_offset_commit_response.
        """
        if not self._use_offset_apis:
            raise Errors.UnsupportedVersionError('OffsetCommitRequest requires 0.8.1+ broker')
        if not all(map(lambda k: isinstance(k, TopicPartition), offsets)) or \
           not all(map(lambda v: isinstance(v, OffsetAndMetadata), offsets.values())):
            raise TypeError('offsets must be dict[TopicPartition, OffsetAndMetadata]')
        if not offsets:
            log.debug('No offsets to commit')
            return None

        node_id = self.coordinator()
        if node_id is None:
            raise Errors.CoordinatorNotAvailableError()

        # create the offset commit request
        offset_data = collections.defaultdict(dict)
        for tp, offset in offsets.items():
            offset_data[tp.topic][tp.partition] = offset

        if self._use_group_apis and self._subscription.partitions_auto_assigned():
            generation = self.generation_if_stable()
        else:
            generation = Generation.NO_GENERATION

        # if the generation is None, we are not part of an active group
        # (and we expect to be). The only thing we can do is fail the commit
        # and let the user rejoin the group in poll()
        if generation is None:
            log.info("Failing OffsetCommit request since the consumer is not part of an active group")
            if self.rebalance_in_progress():
                # if the client knows it is already rebalancing, we can use RebalanceInProgressError instead of
                # CommitFailedError to indicate this is not a fatal error
                raise Errors.RebalanceInProgressError(
                    "Offset commit cannot be completed since the"
                    " consumer is undergoing a rebalance for auto partition assignment. You can try completing the rebalance"
                    " by calling poll() and then retry the operation.")
            else:
                raise Errors.CommitFailedError(
                    "Offset commit cannot be completed since the"
                    " consumer is not part of an active group for auto partition assignment; it is likely that the consumer"
                    " was kicked out of the group.")

        _Topic = OffsetCommitRequest.OffsetCommitRequestTopic
        _Partition = _Topic.OffsetCommitRequestPartition
        request = OffsetCommitRequest(
            max_version=8,
            group_id=self.group_id,
            generation_id_or_member_epoch=generation.generation_id,
            member_id=generation.member_id,
            group_instance_id=self.group_instance_id,
            topics=[_Topic(
                name=topic, partitions=[_Partition(
                    partition_index=partition,
                    committed_offset=offset.offset,
                    committed_leader_epoch=offset.leader_epoch,
                    committed_metadata=offset.metadata
                ) for partition, offset in partitions.items()]
            ) for topic, partitions in offset_data.items()]
        )

        log.debug("Sending offset-commit request with %s for group %s to %s",
                  offsets, self.group_id, node_id)

        send_time = time.monotonic()
        try:
            response = await self._manager.send(request, node_id=node_id)
        except Exception as exc:
            self._failed_request(node_id, request, exc)
            raise
        self._handle_offset_commit_response(offsets, send_time, response)

    def _handle_offset_commit_response(self, offsets, send_time, response):
        log.debug("Received OffsetCommitResponse: %s", response)
        # TODO look at adding request_latency_ms to response (like java kafka)
        if self._consumer_sensors:
            self._consumer_sensors.commit_latency.record((time.monotonic() - send_time) * 1000)
        unauthorized_topics = set()

        for topic, partitions in response.topics:
            for partition, error_code in partitions:
                tp = TopicPartition(topic, partition)
                offset = offsets[tp]

                error_type = Errors.for_code(error_code)
                if error_type is Errors.NoError:
                    log.debug("Group %s committed offset %s for partition %s",
                              self.group_id, offset, tp)
                elif error_type is Errors.GroupAuthorizationFailedError:
                    log.error("Not authorized to commit offsets for group %s",
                              self.group_id)
                    raise error_type(self.group_id)
                elif error_type is Errors.TopicAuthorizationFailedError:
                    unauthorized_topics.add(topic)
                elif error_type in (Errors.OffsetMetadataTooLargeError,
                                    Errors.InvalidCommitOffsetSizeError):
                    # raise the error to the user
                    log.debug("OffsetCommit for group %s failed on partition %s"
                              " %s", self.group_id, tp, error_type.__name__)
                    raise error_type()
                elif error_type is Errors.CoordinatorLoadInProgressError:
                    # just retry
                    log.debug("OffsetCommit for group %s failed: %s",
                              self.group_id, error_type.__name__)
                    raise error_type(self.group_id)
                elif error_type in (Errors.CoordinatorNotAvailableError,
                                    Errors.NotCoordinatorError,
                                    Errors.RequestTimedOutError):
                    log.debug("OffsetCommit for group %s failed: %s",
                              self.group_id, error_type.__name__)
                    self.coordinator_dead(error_type())
                    raise error_type(self.group_id)
                elif error_type is Errors.RebalanceInProgressError:
                    # Consumer never tries to commit offset in between join-group and sync-group,
                    # and hence on broker-side it is not expected to see a commit offset request
                    # during CompletingRebalance phase; if it ever happens then broker would return
                    # this error. In this case we should just treat as a fatal CommitFailed exception.
                    # However, we do not need to reset generations and just request re-join, such that
                    # if the caller decides to proceed and poll, it would still try to proceed and re-join normally.
                    self.request_rejoin()
                    raise Errors.CommitFailedError(error_type())
                elif error_type is Errors.FencedInstanceIdError:
                    log.error("OffsetCommit for group %s failed due to fenced id error: %s",
                              self.group_id, self.group_instance_id)
                    raise error_type()
                elif error_type in (Errors.UnknownMemberIdError,
                                    Errors.IllegalGenerationError):
                    # need reset generation and re-join group
                    error = error_type(self.group_id)
                    log.warning("OffsetCommit for group %s failed: %s",
                                self.group_id, error)
                    if error_type is Errors.IllegalGenerationError:
                        self.reset_generation(member_id=self._generation.member_id)
                    else:
                        self.reset_generation()
                    raise Errors.CommitFailedError(error_type())
                else:
                    log.error("Group %s failed to commit partition %s at offset"
                              " %s: %s", self.group_id, tp, offset,
                              error_type.__name__)
                    raise error_type()

        if unauthorized_topics:
            log.error("Not authorized to commit to topics %s for group %s",
                      unauthorized_topics, self.group_id)
            raise Errors.TopicAuthorizationFailedError(unauthorized_topics)

    async def _send_offset_fetch_request(self, partitions):
        """Fetch the committed offsets for a set of partitions.

        Arguments:
            partitions (list[TopicPartition]): the partitions to fetch.

        Returns:
            dict[TopicPartition, OffsetAndMetadata] on success.

        Raises:
            UnsupportedVersionError if broker is too old.
            CoordinatorNotAvailableError if the coordinator is unknown.
            Other broker-side OffsetFetch errors propagated via
                _handle_offset_fetch_response.
        """
        if not self._use_offset_apis:
            raise Errors.UnsupportedVersionError('OffsetFetchRequest requires 0.8.1+ broker')
        if not all(map(lambda k: isinstance(k, TopicPartition), partitions)):
            raise TypeError("partitions must be list[TopicPartition]")
        if not partitions and partitions is not None:
            return {}

        node_id = self.coordinator()
        if node_id is None:
            raise Errors.CoordinatorNotAvailableError()

        log.debug("Group %s fetching committed offsets for partitions: %s",
                  self.group_id, '(all)' if partitions is None else partitions)
        # construct the request
        _Topic = OffsetFetchRequest.OffsetFetchRequestTopic
        _Group = OffsetFetchRequest.OffsetFetchRequestGroup
        _GroupTopic = _Group.OffsetFetchRequestTopics
        if partitions is not None:
            topic_partitions = collections.defaultdict(set)
            for tp in partitions:
                topic_partitions[tp.topic].add(tp.partition)
            topics = [_Topic(name=t, partition_indexes=list(p))
                      for t, p in topic_partitions.items()]
            group_topics = [_GroupTopic(name=t, partition_indexes=list(p))
                            for t, p in topic_partitions.items()]
            min_version = 0
        else:
            topics = None
            group_topics = None
            min_version = 2

        groups = [_Group(group_id=self.group_id, topics=group_topics)]
        require_stable = self._isolation_level == IsolationLevel.READ_COMMITTED
        request = OffsetFetchRequest(
            group_id=self.group_id,
            topics=topics,
            groups=groups,
            require_stable=require_stable,
            min_version=min_version,
            max_version=8,
        )

        try:
            response = await self._manager.send(request, node_id=node_id)
        except Exception as exc:
            self._failed_request(node_id, request, exc)
            raise
        return self._handle_offset_fetch_response(response)

    def _handle_offset_fetch_response(self, response):
        log.debug("Received OffsetFetchResponse: %s", response)
        if response.API_VERSION >= 8:
            group = response.groups[0]
            top_level_error_code = group.error_code
            topics = group.topics
        else:
            top_level_error_code = response.error_code if response.API_VERSION >= 2 else Errors.NoError.errno
            topics = response.topics

        if top_level_error_code != Errors.NoError.errno:
            error_type = Errors.for_code(top_level_error_code)
            log.debug("Offset fetch failed: %s", error_type.__name__)
            error = error_type()
            if error_type is Errors.NotCoordinatorError:
                # re-discover the coordinator and retry
                self.coordinator_dead(error)
                raise error
            elif error_type in (Errors.CoordinatorLoadInProgressError,
                                Errors.GroupAuthorizationFailedError):
                raise error
            else:
                log.error("Unknown error fetching offsets: %s", error)
                raise error

        offsets = {}
        for topic, partitions in ((t.name, t.partitions) for t in topics):
            for partition_data in partitions:
                partition = partition_data.partition_index
                offset = partition_data.committed_offset
                leader_epoch = partition_data.committed_leader_epoch
                metadata = partition_data.metadata
                error_code = partition_data.error_code
                tp = TopicPartition(topic, partition)
                error_type = Errors.for_code(error_code)
                if error_type is not Errors.NoError:
                    error = error_type()
                    log.debug("Group %s failed to fetch offset for partition"
                              " %s: %s", self.group_id, tp, error)
                    if error_type is Errors.NotCoordinatorError:
                        # re-discover the coordinator and retry
                        self.coordinator_dead(error)
                        raise error
                    elif error_type is Errors.CoordinatorLoadInProgressError:
                        raise error
                    elif error_type is Errors.UnknownTopicOrPartitionError:
                        log.warning("OffsetFetchRequest -- unknown topic %s"
                                    " (have you committed any offsets yet?)",
                                    topic)
                        continue
                    else:
                        log.error("Unknown error fetching offsets for %s: %s",
                                  tp, error)
                        raise error
                elif offset >= 0:
                    # record the position with the offset
                    # (-1 indicates no committed offset to fetch)
                    offsets[tp] = OffsetAndMetadata(offset, metadata, leader_epoch)
                else:
                    log.debug("Group %s has no committed offset for partition"
                              " %s", self.group_id, tp)
        return offsets

    def _default_offset_commit_callback(self, offsets, res_or_exc):
        if isinstance(res_or_exc, Exception):
            log.warning("Auto offset commit failed for group %s: %s",
                        self.group_id, res_or_exc)
        else:
            log.debug("Completed autocommit of offsets %s for group %s",
                      offsets, self.group_id)

    def _commit_offsets_async_on_complete(self, offsets, res_or_exc):
        if isinstance(res_or_exc, Errors.RetriableError):
            self.next_auto_commit_deadline = min(time.monotonic() + self.config['retry_backoff_ms'] / 1000, self.next_auto_commit_deadline)
        self.config['default_offset_commit_callback'](offsets, res_or_exc)

    def _maybe_auto_commit_offsets_async(self):
        if self.config['enable_auto_commit']:
            if self.coordinator_unknown():
                self.next_auto_commit_deadline = time.monotonic() + self.config['retry_backoff_ms'] / 1000
            elif time.monotonic() > self.next_auto_commit_deadline:
                self.next_auto_commit_deadline = time.monotonic() + self.auto_commit_interval
                self._do_auto_commit_offsets_async()

    def maybe_auto_commit_offsets_now(self):
        if self.config['enable_auto_commit'] and not self.coordinator_unknown():
            self._do_auto_commit_offsets_async()

    def _do_auto_commit_offsets_async(self):
        self.commit_offsets_async(self._subscription.all_consumed_offsets(),
                                  self._commit_offsets_async_on_complete)


class ConsumerCoordinatorMetrics:
    def __init__(self, metrics, metric_group_prefix, subscription):
        self.metrics = metrics
        self.metric_group_name = '%s-coordinator-metrics' % (metric_group_prefix,)

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
