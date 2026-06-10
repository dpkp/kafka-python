from abc import ABC, abstractmethod
from collections import OrderedDict
from collections.abc import Sequence
from enum import IntEnum
import logging
import random
import re
import threading
import time

import kafka.errors as Errors
from kafka.protocol.consumer import OffsetResetStrategy
from kafka.structs import OffsetAndMetadata
from kafka.util import ensure_valid_topic_name, synchronized

log = logging.getLogger(__name__)


class SubscriptionType(IntEnum):
    NONE = 0
    AUTO_TOPICS = 1
    AUTO_PATTERN = 2
    USER_ASSIGNED = 3


class SubscriptionState:
    """
    A class for tracking the topics, partitions, and offsets for the consumer.
    A partition is "assigned" either directly with assign_from_user() (manual
    assignment) or with assign_from_subscribed() (automatic assignment from
    subscription).

    Once assigned, the partition is not considered "fetchable" until its initial
    position has been set with seek(). Fetchable partitions track a fetch
    position which is used to set the offset of the next fetch, and a consumed
    position which is the last offset that has been returned to the user. You
    can suspend fetching from a partition through pause() without affecting the
    fetched/consumed offsets. The partition will remain unfetchable until the
    resume() is used. You can also query the pause state independently with
    is_paused().

    Note that pause state as well as fetch/consumed positions are not preserved
    when partition assignment is changed whether directly by the user or
    through a group rebalance.
    """
    _SUBSCRIPTION_EXCEPTION_MESSAGE = (
        "You must choose only one way to configure your consumer:"
        " (1) subscribe to specific topics by name,"
        " (2) subscribe to topics matching a regex pattern,"
        " (3) assign itself specific topic-partitions.")

    def __init__(self, offset_reset_strategy='earliest'):
        """Initialize a SubscriptionState instance

        Keyword Arguments:
            offset_reset_strategy: 'earliest' or 'latest', otherwise
                exception will be raised when fetching an offset that is no
                longer available. Default: 'earliest'
        """
        try:
            offset_reset_strategy = getattr(OffsetResetStrategy,
                                            offset_reset_strategy.upper())
        except AttributeError:
            log.warning('Unrecognized offset_reset_strategy, using NONE')
            offset_reset_strategy = OffsetResetStrategy.NONE
        self._default_offset_reset_strategy = offset_reset_strategy

        self.subscription = None # set() or None
        self.subscription_type = SubscriptionType.NONE
        self.subscribed_pattern = None # regex str or None
        self._group_subscription = set()
        self._user_assignment = set()
        self.assignment = OrderedDict()
        self.rebalance_listener = None
        self.listeners = []
        self._lock = threading.RLock()

    def _set_subscription_type(self, subscription_type):
        if not isinstance(subscription_type, SubscriptionType):
            raise ValueError('SubscriptionType enum required')
        if self.subscription_type == SubscriptionType.NONE:
            self.subscription_type = subscription_type
        elif self.subscription_type != subscription_type:
            raise Errors.IllegalStateError(self._SUBSCRIPTION_EXCEPTION_MESSAGE)

    @synchronized
    def subscribe(self, topics=(), pattern=None, listener=None):
        """Subscribe to a list of topics, or a topic regex pattern.

        Partitions will be dynamically assigned via a group coordinator.
        Topic subscriptions are not incremental: this list will replace the
        current assignment (if there is one).

        This method is incompatible with assign_from_user()

        Arguments:
            topics (list): List of topics for subscription.
            pattern (str): Pattern to match available topics. You must provide
                either topics or pattern, but not both.
            listener (ConsumerRebalanceListener): Optionally include listener
                callback, which will be called before and after each rebalance
                operation.

                As part of group management, the consumer will keep track of the
                list of consumers that belong to a particular group and will
                trigger a rebalance operation if one of the following events
                trigger:

                * Number of partitions change for any of the subscribed topics
                * Topic is created or deleted
                * An existing member of the consumer group dies
                * A new member is added to the consumer group

                When any of these events are triggered, the provided listener
                will be invoked first to indicate that the consumer's assignment
                has been revoked, and then again when the new assignment has
                been received. Note that this listener will immediately override
                any listener set in a previous call to subscribe. It is
                guaranteed, however, that the partitions revoked/assigned
                through this interface are from topics subscribed in this call.

        Raises:
            ValueError: if neither topics nor pattern provided.
            IllegalStateError: if both topics and pattern provided.
            TypeError: if topics is not a list/sequence, or listener is not
                a AsyncConsumerRebalanceListener or ConsumerRebalanceListener.
        """
        if not topics and not pattern:
            raise ValueError('Must provide topics or pattern')
        if (topics and pattern):
            raise Errors.IllegalStateError(self._SUBSCRIPTION_EXCEPTION_MESSAGE)

        elif pattern:
            self._set_subscription_type(SubscriptionType.AUTO_PATTERN)
            log.info('Subscribing to pattern: /%s/', pattern)
            self.subscription = set()
            self.subscribed_pattern = re.compile(pattern)
        else:
            if isinstance(topics, str) or not isinstance(topics, Sequence):
                raise TypeError('Topics must be a list (or non-str sequence)')
            self._set_subscription_type(SubscriptionType.AUTO_TOPICS)
            self.change_subscription(topics)

        if listener and not isinstance(
                listener, (ConsumerRebalanceListener, AsyncConsumerRebalanceListener)):
            raise TypeError(
                'listener must be a ConsumerRebalanceListener or AsyncConsumerRebalanceListener')
        self.rebalance_listener = listener

    @synchronized
    def change_subscription(self, topics):
        """Change the topic subscription.

        Arguments:
            topics (list of str): topics for subscription

        Raises:
            IllegalStateError: if assign_from_user has been used already
            TypeError: if a topic is None or a non-str
            ValueError: if a topic is an empty string or
                        - a topic name is '.' or '..' or
                        - a topic name does not consist of ASCII-characters/'-'/'_'/'.'
        """
        if not self.partitions_auto_assigned():
            raise Errors.IllegalStateError(self._SUBSCRIPTION_EXCEPTION_MESSAGE)

        if isinstance(topics, str):
            topics = [topics]

        if self.subscription == set(topics):
            log.warning("subscription unchanged by change_subscription(%s)",
                        topics)
            return

        for t in topics:
            ensure_valid_topic_name(t)

        log.info('Updating subscribed topics to: %s', topics)
        self.subscription = set(topics)
        self._group_subscription.update(topics)

    @synchronized
    def group_subscribe(self, topics):
        """Add topics to the current group subscription.

        This is used by the group leader to ensure that it receives metadata
        updates for all topics that any member of the group is subscribed to.

        Arguments:
            topics (list of str): topics to add to the group subscription
        """
        if not self.partitions_auto_assigned():
            raise Errors.IllegalStateError(self._SUBSCRIPTION_EXCEPTION_MESSAGE)
        self._group_subscription.update(topics)

    @synchronized
    def reset_group_subscription(self):
        """Reset the group's subscription to only contain topics subscribed by this consumer."""
        if not self.partitions_auto_assigned():
            raise Errors.IllegalStateError(self._SUBSCRIPTION_EXCEPTION_MESSAGE)
        if self.subscription is None:
            raise Errors.IllegalStateError('Subscription required')
        self._group_subscription.intersection_update(self.subscription)

    @synchronized
    def assign_from_user(self, partitions):
        """Manually assign a list of TopicPartitions to this consumer.

        The new assignment replaces the previous one (this is not an
        incremental-add API), but ``TopicPartitionState`` is preserved
        for any partition that's present in both the old and new
        assignment. Manual topic assignment through this method does
        not use the consumer's group management functionality. As
        such, there will be no rebalance operation triggered when
        group membership or cluster and topic metadata change. Note
        that it is not possible to use both manual partition
        assignment with assign() and group assignment with subscribe().

        Arguments:
            partitions (list of TopicPartition): assignment for this instance.

        Raises:
            IllegalStateError: if consumer has already called subscribe()
        """
        self._set_subscription_type(SubscriptionType.USER_ASSIGNED)
        if self._user_assignment == set(partitions):
            return
        self._user_assignment = set(partitions)
        self._apply_assignment(partitions)

    @synchronized
    def assign_from_subscribed(self, assignments):
        """Update the assignment to the specified partitions.

        This method is called by the coordinator to dynamically assign
        partitions based on the consumer's topic subscription. Differs
        from :meth:`assign_from_user` which directly sets the assignment
        from a user-supplied TopicPartition list.

        Preserves ``TopicPartitionState`` (position, paused flag,
        preferred read replica, fetch buffers tied to the partition)
        for any partition present in both the prior and new assignments.

        Validation raises ``ValueError`` BEFORE any mutation if a
        partition's topic isn't subscribed.

        Arguments:
            assignments (list of TopicPartition): the *full* new
                assignment (not a diff). Partitions present in both
                the old and new assignment retain their state;
                revoked partitions are dropped; new partitions get
                fresh state.
        """
        if not self.partitions_auto_assigned():
            raise Errors.IllegalStateError(self._SUBSCRIPTION_EXCEPTION_MESSAGE)

        for tp in assignments:
            if tp.topic not in self.subscription:
                raise ValueError("Assigned partition %s for non-subscribed topic." % (tp,))

        # randomize new-partition insertion order so short-lived
        # consumers (CLI tools, one-shot jobs) that re-run from scratch
        # don't keep starting on the same partition. The Fetcher
        # iterates fetchable_partitions() in self.assignment insertion
        # order; without the shuffle, partition 0 of the
        # alphabetically-first topic always wins the first fetch and
        # short-lived runs that bail before exhausting it never see
        # later partitions.
        self._apply_assignment(assignments, randomize=True)
        log.info("Updated partition assignment: %s", assignments)

    def _apply_assignment(self, partitions, randomize=False):
        """Mutate ``self.assignment`` in place to contain exactly
        ``partitions``, preserving the existing ``TopicPartitionState``
        for any partition present in both the old and new sets.

        Shared between :meth:`assign_from_user` and
        :meth:`assign_from_subscribed` - the algorithm is identical;
        only the validation around it differs.

        When ``randomize=True``, the *newly-added* partitions are
        inserted in random order. Kept partitions retain their
        existing position regardless. This is intended for the
        coordinator-driven path (assign_from_subscribed) - see the
        comment at that call site for rationale.
        """
        new_set = set(partitions)
        # Drop revoked partitions (we mutate self.assignment, so list()
        # the keys first to avoid "dict changed size during iteration").
        for tp in list(self.assignment.keys()):
            if tp not in new_set:
                del self.assignment[tp]
        # Add new partitions; kept partitions retain their existing
        # TopicPartitionState (positions, paused flag, KIP-392 cache,
        # etc.).
        new_partitions = [tp for tp in partitions if tp not in self.assignment]
        if randomize:
            random.shuffle(new_partitions)
        for tp in new_partitions:
            self.assignment[tp] = TopicPartitionState()

    @synchronized
    def unsubscribe(self):
        """Clear all topic subscriptions and partition assignments"""
        self.subscription = None
        self._user_assignment.clear()
        self.assignment.clear()
        self.subscribed_pattern = None
        self.subscription_type = SubscriptionType.NONE

    @synchronized
    def group_subscription(self):
        """Get the topic subscription for the group.

        For the leader, this will include the union of all member subscriptions.
        For followers, it is the member's subscription only.

        This is used when querying topic metadata to detect metadata changes
        that would require rebalancing (the leader fetches metadata for all
        topics in the group so that it can do partition assignment).

        Returns:
            set: topics
        """
        return self._group_subscription

    @synchronized
    def seek(self, partition, offset):
        """Manually specify the fetch offset for a TopicPartition.

        Overrides the fetch offsets that the consumer will use on the next
        poll(). If this API is invoked for the same partition more than once,
        the latest offset will be used on the next poll(). Note that you may
        lose data if this API is arbitrarily used in the middle of consumption,
        to reset the fetch offsets.

        Arguments:
            partition (TopicPartition): partition for seek operation
            offset (int or OffsetAndMetadata): message offset in partition
        """
        if not isinstance(offset, (int, OffsetAndMetadata)):
            raise TypeError("offset must be type in or OffsetAndMetadata")
        self.assignment[partition].seek(offset)

    @synchronized
    def assigned_partitions(self):
        """Return set of TopicPartitions in current assignment."""
        return set(self.assignment.keys())

    @synchronized
    def paused_partitions(self):
        """Return current set of paused TopicPartitions."""
        return set(partition for partition in self.assignment
                   if self.is_paused(partition))

    @synchronized
    def fetchable_partitions(self):
        """Return ordered list of TopicPartitions that should be Fetched."""
        fetchable = list()
        for partition, state in self.assignment.items():
            if state.is_fetchable():
                fetchable.append(partition)
        return fetchable

    @synchronized
    def partitions_auto_assigned(self):
        """Return True unless user supplied partitions manually."""
        return self.subscription_type in (SubscriptionType.AUTO_TOPICS, SubscriptionType.AUTO_PATTERN)

    @synchronized
    def all_consumed_offsets(self):
        """Returns consumed offsets as {TopicPartition: OffsetAndMetadata}"""
        all_consumed = {}
        for partition, state in self.assignment.items():
            if state.has_valid_position:
                all_consumed[partition] = state.position
        return all_consumed

    @synchronized
    def request_offset_reset(self, partition, offset_reset_strategy=None):
        """Mark partition for offset reset using specified or default strategy.

        Arguments:
            partition (TopicPartition): partition to mark
            offset_reset_strategy (OffsetResetStrategy, optional)
        """
        if offset_reset_strategy is None:
            offset_reset_strategy = self._default_offset_reset_strategy
        self.assignment[partition].reset(offset_reset_strategy)

    @synchronized
    def set_reset_pending(self, partitions, next_allowed_reset_time):
        for partition in partitions:
            self.assignment[partition].set_reset_pending(next_allowed_reset_time)

    @synchronized
    def has_default_offset_reset_policy(self):
        """Return True if default offset reset policy is Earliest or Latest"""
        return self._default_offset_reset_strategy != OffsetResetStrategy.NONE

    @synchronized
    def is_offset_reset_needed(self, partition):
        return self.assignment[partition].awaiting_reset

    @synchronized
    def has_all_fetch_positions(self):
        for state in self.assignment.values():
            if not state.has_valid_position:
                return False
        return True

    @synchronized
    def missing_fetch_positions(self):
        missing = set()
        for partition, state in self.assignment.items():
            if state.is_missing_position():
                missing.add(partition)
        return missing

    @synchronized
    def has_valid_position(self, partition):
        return partition in self.assignment and self.assignment[partition].has_valid_position

    @synchronized
    def reset_missing_positions(self):
        partitions_with_no_offsets = set()
        for tp, state in self.assignment.items():
            if state.is_missing_position():
                if self._default_offset_reset_strategy == OffsetResetStrategy.NONE:
                    partitions_with_no_offsets.add(tp)
                else:
                    state.reset(self._default_offset_reset_strategy)

        if partitions_with_no_offsets:
            raise Errors.NoOffsetForPartitionError(partitions_with_no_offsets)

    @synchronized
    def partitions_needing_reset(self):
        partitions = set()
        for tp, state in self.assignment.items():
            if state.awaiting_reset and state.is_reset_allowed():
                partitions.add(tp)
        return partitions

    @synchronized
    def next_offset_reset_retry_time(self):
        times = [state.next_allowed_retry_time
                 for state in self.assignment.values()
                 if state.awaiting_reset and state.next_allowed_retry_time is not None]
        return min(times) if times else None

    @synchronized
    def maybe_validate_position_for_current_leader(self, partition, current_leader_epoch):
        if partition not in self.assignment:
            return False
        return self.assignment[partition].maybe_validate_position(current_leader_epoch)

    @synchronized
    def request_position_validation(self, partition):
        if partition not in self.assignment:
            return False
        return self.assignment[partition].request_position_validation()

    @synchronized
    def partitions_needing_validation(self):
        partitions = set()
        for tp, state in self.assignment.items():
            if state.awaiting_validation and state.is_validation_allowed():
                partitions.add(tp)
        return partitions

    @synchronized
    def next_offset_validation_retry_time(self):
        times = [state.next_allowed_retry_time
                 for state in self.assignment.values()
                 if state.awaiting_validation and state.next_allowed_retry_time is not None]
        return min(times) if times else None

    @synchronized
    def set_validation_pending(self, partitions, next_allowed_retry_time):
        for partition in partitions:
            self.assignment[partition].set_validation_pending(next_allowed_retry_time)

    @synchronized
    def validation_failed(self, partitions, next_allowed_retry_time):
        for partition in partitions:
            self.assignment[partition].validation_failed(next_allowed_retry_time)

    @synchronized
    def complete_validation(self, partition, validated_position=None):
        if partition in self.assignment:
            self.assignment[partition].complete_validation(validated_position)

    @synchronized
    def is_offset_validation_needed(self, partition):
        return partition in self.assignment and self.assignment[partition].awaiting_validation

    @synchronized
    def is_assigned(self, partition):
        return partition in self.assignment

    @synchronized
    def is_paused(self, partition):
        return partition in self.assignment and self.assignment[partition].paused

    @synchronized
    def is_fetchable(self, partition):
        return partition in self.assignment and self.assignment[partition].is_fetchable()

    @synchronized
    def pause(self, partition):
        self.assignment[partition].pause()

    @synchronized
    def resume(self, partition):
        self.assignment[partition].resume()

    @synchronized
    def mark_pending_revocation(self, partitions):
        """KIP-429: gate ``is_fetchable()`` for each partition's state
        so the fetcher would skip them while an on_partitions_revoked /
        on_partitions_lost listener runs. Called immediately before
        invoking the listener. The flag is single-shot - the
        surrounding ``assign_from_subscribed`` drops the
        ``TopicPartitionState`` for revoked partitions when the
        listener returns.

        Currently a no-op while the user thread is blocked in ``_net.run``
        during rebalance and so the only path that calls ``send_fetches``
        cannot fire. Kept as a defensive gate in case this changes in
        the future."""
        for tp in partitions:
            if tp in self.assignment:
                self.assignment[tp].mark_pending_revocation()

    @synchronized
    def reset_failed(self, partitions, next_retry_time):
        for partition in partitions:
            self.assignment[partition].reset_failed(next_retry_time)

    @synchronized
    def move_partition_to_end(self, partition):
        if partition in self.assignment:
            try:
                self.assignment.move_to_end(partition)
            except AttributeError:
                state = self.assignment.pop(partition)
                self.assignment[partition] = state

    @synchronized
    def position(self, partition):
        return self.assignment[partition].position


class TopicPartitionState:
    def __init__(self):
        self.paused = False # whether this partition has been paused by the user
        self.reset_strategy = None # the reset strategy if awaiting_reset is set
        self._position = None # OffsetAndMetadata exposed to the user
        self.highwater = None
        self.drop_pending_record_batch = False
        self.next_allowed_retry_time = None
        # KIP-320: offset validation state. _awaiting_validation gates fetches
        # until OffsetForLeaderEpoch confirms the position is consistent with
        # the current leader's log; mutually exclusive with awaiting_reset.
        self._awaiting_validation = False
        # KIP-392: preferred read replica chosen by the broker (rack-aware).
        # ``_preferred_read_replica_expiration`` is a monotonic deadline; after
        # it passes we fall back to the leader and re-learn. Cleared on
        # replica-related errors so the next fetch goes to the leader.
        self._preferred_read_replica = None
        self._preferred_read_replica_expiration = None
        # KIP-429 (Java parity): set while an on_partitions_revoked /
        # on_partitions_lost listener is running for this partition.
        # Gates fetches so records aren't pulled for a partition the
        # user is in the middle of releasing. The TopicPartitionState
        # is dropped from the assignment when the listener returns,
        # so the flag only matters for the listener-call window.
        self._pending_revocation = False

    def _set_position(self, offset):
        if not self.has_valid_position:
            raise Errors.IllegalStateError('Valid position required')
        if not isinstance(offset, OffsetAndMetadata):
            raise TypeError('offset must be OffsetAndMetadata')
        self._position = offset

    def _get_position(self):
        return self._position

    position = property(_get_position, _set_position, None, "last position")

    def reset(self, strategy):
        if strategy is None:
            raise ValueError('strategy cannot be None')
        self.reset_strategy = strategy
        self._position = None
        self.next_allowed_retry_time = None
        self._awaiting_validation = False
        self.clear_preferred_read_replica()

    def is_reset_allowed(self):
        return self.next_allowed_retry_time is None or self.next_allowed_retry_time < time.monotonic()

    @property
    def awaiting_reset(self):
        return self.reset_strategy is not None

    def set_reset_pending(self, next_allowed_retry_time):
        self.next_allowed_retry_time = next_allowed_retry_time

    def reset_failed(self, next_allowed_retry_time):
        self.next_allowed_retry_time = next_allowed_retry_time

    @property
    def has_valid_position(self):
        return self._position is not None

    def is_missing_position(self):
        return not self.has_valid_position and not self.awaiting_reset

    def seek(self, offset):
        self._position = offset if isinstance(offset, OffsetAndMetadata) else OffsetAndMetadata(offset, '', -1)
        self.reset_strategy = None
        self.drop_pending_record_batch = True
        self.next_allowed_retry_time = None
        self._awaiting_validation = False
        self.clear_preferred_read_replica()

    def pause(self):
        self.paused = True

    def resume(self):
        self.paused = False

    def mark_pending_revocation(self):
        """KIP-429: gate fetches while an on_partitions_revoked /
        on_partitions_lost listener is in progress for this partition.
        Single-shot: the surrounding ``assign_from_subscribed`` drops
        the state object once the listener returns."""
        self._pending_revocation = True

    def is_fetchable(self):
        return (not self.paused
                and not self._pending_revocation
                and self.has_valid_position
                and not self._awaiting_validation)

    def preferred_read_replica(self):
        """Return the currently-cached preferred read replica (KIP-392),
        or None if unset/expired. Lazily clears the cache on expiry."""
        if self._preferred_read_replica is None:
            return None
        if (self._preferred_read_replica_expiration is not None
                and time.monotonic() >= self._preferred_read_replica_expiration):
            self.clear_preferred_read_replica()
            return None
        return self._preferred_read_replica

    def update_preferred_read_replica(self, node_id, expiration_time):
        """Cache the broker's chosen preferred read replica until ``expiration_time``
        (monotonic). ``node_id == -1`` (or None) clears the cache.

        Returns True if the cached replica actually changed (caller can log).
        """
        if node_id is None or node_id < 0:
            changed = self._preferred_read_replica is not None
            self.clear_preferred_read_replica()
            return changed
        if node_id == self._preferred_read_replica:
            return False
        self._preferred_read_replica = node_id
        self._preferred_read_replica_expiration = expiration_time
        return True

    def clear_preferred_read_replica(self):
        """Clear the cached preferred read replica. Returns the previously-
        cached node_id (or None) so the caller can log the eviction."""
        previous = self._preferred_read_replica
        self._preferred_read_replica = None
        self._preferred_read_replica_expiration = None
        return previous

    @property
    def awaiting_validation(self):
        return self._awaiting_validation

    def maybe_validate_position(self, current_leader_epoch):
        """Mark for validation if current leader has advanced beyond our position's epoch.

        Returns True if the partition is now awaiting validation.
        """
        if self.awaiting_reset:
            return False
        if self._position is None:
            return False
        if current_leader_epoch is None or current_leader_epoch < 0:
            return False
        # Positions without a known epoch (legacy data, post-seek to bare offset)
        # can't be validated; treat as already-fetchable.
        if self._position.leader_epoch < 0:
            return False
        if self._position.leader_epoch >= current_leader_epoch:
            return False
        self.clear_preferred_read_replica()
        self._awaiting_validation = True
        self.next_allowed_retry_time = None
        return True

    def request_position_validation(self):
        """Force validation (e.g., after FENCED/UNKNOWN epoch errors from the broker)."""
        if self._position is None or self._position.leader_epoch < 0:
            return False
        self._awaiting_validation = True
        self.next_allowed_retry_time = None
        return True

    def is_validation_allowed(self):
        return self.next_allowed_retry_time is None or self.next_allowed_retry_time < time.monotonic()

    def set_validation_pending(self, next_allowed_retry_time):
        self.next_allowed_retry_time = next_allowed_retry_time

    def validation_failed(self, next_allowed_retry_time):
        self.next_allowed_retry_time = next_allowed_retry_time

    def complete_validation(self, validated_position=None):
        self._awaiting_validation = False
        self.next_allowed_retry_time = None
        if validated_position is not None:
            self._position = validated_position


class ConsumerRebalanceListener(ABC):
    """
    A callback interface that the user can implement to trigger custom actions
    when the set of partitions assigned to the consumer changes.

    This is applicable when the consumer is having Kafka auto-manage group
    membership. If the consumer's directly assign partitions, those
    partitions will never be reassigned and this callback is not applicable.

    When Kafka is managing the group membership, a partition re-assignment will
    be triggered any time the members of the group changes or the subscription
    of the members changes. This can occur when processes die, new process
    instances are added or old instances come back to life after failure.
    Rebalances can also be triggered by changes affecting the subscribed
    topics (e.g. when then number of partitions is administratively adjusted).

    There are many uses for this functionality. One common use is saving offsets
    in a custom store. By saving offsets in the on_partitions_revoked(), call we
    can ensure that any time partition assignment changes the offset gets saved.

    Another use is flushing out any kind of cache of intermediate results the
    consumer may be keeping. For example, consider a case where the consumer is
    subscribed to a topic containing user page views, and the goal is to count
    the number of page views per users for each five minute window.  Let's say
    the topic is partitioned by the user id so that all events for a particular
    user will go to a single consumer instance. The consumer can keep in memory
    a running tally of actions per user and only flush these out to a remote
    data store when its cache gets too big. However if a partition is reassigned
    it may want to automatically trigger a flush of this cache, before the new
    owner takes over consumption.

    Threading: callbacks run on the consumer's IO event loop, the same loop
    that drives heartbeats. Sync listener methods must return promptly --
    blocking IO inside a sync listener will block heartbeats for the duration
    and can cause the consumer to be kicked from the group if the listener
    runs longer than ``session_timeout_ms``. For listeners that need to do
    blocking work (e.g. flushing state to a database), prefer
    :class:`AsyncConsumerRebalanceListener`, which lets you ``await`` while
    keeping the loop responsive, or wrap the blocking call in your own
    worker thread.

    It is guaranteed that all consumer processes will invoke
    on_partitions_revoked() prior to any process invoking
    on_partitions_assigned(). So if offsets or other state is saved in the
    on_partitions_revoked() call, it should be saved by the time the process
    taking over that partition has their on_partitions_assigned() callback
    called to load the state.
    """
    @abstractmethod
    def on_partitions_revoked(self, revoked):
        """
        A callback method the user can implement to provide handling of offset
        commits to a customized store on the start of a rebalance operation.
        This method will be called before a rebalance operation starts and
        after the consumer stops fetching data. It is recommended that offsets
        should be committed in this callback to either Kafka or a custom offset
        store to prevent duplicate data.

        NOTE: This method is called before each rebalance and also when the
        consumer is closing (KafkaConsumer.close()), so that offsets / state
        can be committed before the partitions are given up. If the group
        membership has already been lost (forced eviction),
        on_partitions_lost() is called instead.

        Arguments:
            revoked (list of TopicPartition): the partitions that were assigned
                to the consumer on the last rebalance
        """
        pass

    @abstractmethod
    def on_partitions_assigned(self, assigned):
        """
        A callback method the user can implement to provide handling of
        customized offsets on completion of a successful partition
        re-assignment. This method will be called after an offset re-assignment
        completes and before the consumer starts fetching data.

        It is guaranteed that all the processes in a consumer group will execute
        their on_partitions_revoked() callback before any instance executes its
        on_partitions_assigned() callback.

        Arguments:
            assigned (list of TopicPartition): the partitions assigned to the
                consumer (may include partitions that were previously assigned)
        """
        pass

    def on_partitions_lost(self, lost):
        """KIP-429: called when the consumer has been forcibly removed
        from the group (heartbeat session expiry, ``UnknownMemberIdError``,
        ``IllegalGenerationError``, ``FencedInstanceIdError``) and the
        partitions cannot be cleanly committed. ``on_partitions_revoked``
        implies the user *can* still commit; ``on_partitions_lost`` makes
        explicit that the member has been booted and any in-flight state
        for these partitions should be discarded.

        Default behaviour is to delegate to ``on_partitions_revoked`` so
        listeners written before KIP-429 keep working unchanged. Override
        for cleanup that is specific to the forced-eviction case (e.g.
        skipping a commit attempt that will fail anyway).

        Arguments:
            lost (set of TopicPartition): the partitions that were
                assigned but have been lost due to forced eviction.
        """
        return self.on_partitions_revoked(lost)


class AsyncConsumerRebalanceListener(ABC):
    """
    Async variant of :class:`ConsumerRebalanceListener`.

    Implement this when your rebalance hooks need to perform IO that would
    otherwise block the consumer's event loop -- e.g. flushing state to a
    database, calling an external service, or coordinating with other async
    code. The coordinator detects coroutine functions and ``await`` s them
    instead of calling inline, so other tasks on the loop (notably the
    heartbeat coroutine) continue to run while your listener is suspended.

    Same lifecycle and ordering guarantees as the sync listener: all
    consumers in the group invoke ``on_partitions_revoked`` before any
    invokes ``on_partitions_assigned``. Both methods must be defined as
    ``async def``; otherwise use :class:`ConsumerRebalanceListener`.
    """
    @abstractmethod
    async def on_partitions_revoked(self, revoked):
        """Async-callback for the start of a rebalance operation.

        See :meth:`ConsumerRebalanceListener.on_partitions_revoked` for
        semantics. The coordinator awaits this method, so non-blocking IO
        via ``await`` keeps the heartbeat loop responsive during the call.

        Arguments:
            revoked (set of TopicPartition): the partitions that were
                assigned to the consumer on the last rebalance.
        """
        pass

    @abstractmethod
    async def on_partitions_assigned(self, assigned):
        """Async-callback for the completion of a partition re-assignment.

        See :meth:`ConsumerRebalanceListener.on_partitions_assigned` for
        semantics.

        Arguments:
            assigned (set of TopicPartition): the partitions assigned to
                the consumer (may include partitions that were previously
                assigned).
        """
        pass

    async def on_partitions_lost(self, lost):
        """Async variant of
        :meth:`ConsumerRebalanceListener.on_partitions_lost`. Default
        implementation awaits ``on_partitions_revoked`` for backward
        compatibility with listeners written before KIP-429.

        Arguments:
            lost (set of TopicPartition): the partitions that were
                assigned but have been lost due to forced eviction.
        """
        await self.on_partitions_revoked(lost)
