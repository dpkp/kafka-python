from __future__ import absolute_import

import abc
import logging
import re

import six

from kafka.common import IllegalStateError, OffsetAndMetadata
from kafka.protocol.offset import OffsetResetStrategy

log = logging.getLogger(__name__)


class SubscriptionState(object):
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

    This class also maintains a cache of the latest commit position for each of
    the assigned partitions. This is updated through committed() and can be used
    to set the initial fetch position (e.g. Fetcher._reset_offset() ).
    """
    _SUBSCRIPTION_EXCEPTION_MESSAGE = ("Subscription to topics, partitions and"
                                       " pattern are mutually exclusive")

    def __init__(self, offset_reset_strategy='earliest'):
        """Initialize a SubscriptionState instance

        offset_reset_strategy: 'earliest' or 'latest', otherwise
                               exception will be raised when fetching an offset
                               that is no longer available.
                               Defaults to earliest.
        """
        try:
            offset_reset_strategy = getattr(OffsetResetStrategy,
                                            offset_reset_strategy.upper())
        except AttributeError:
            log.warning('Unrecognized offset_reset_strategy, using NONE')
            offset_reset_strategy = OffsetResetStrategy.NONE
        self._default_offset_reset_strategy = offset_reset_strategy

        self.subscription = None # set() or None
        self.subscribed_pattern = None # regex str or None
        self._group_subscription = set()
        self._user_assignment = set()
        self.assignment = dict()
        self.needs_partition_assignment = False
        self.listener = None

        # initialize to true for the consumers to fetch offset upon starting up
        self.needs_fetch_committed_offsets = True

    def subscribe(self, topics=(), pattern=None, listener=None):
        """Subscribe to a list of topics, or a topic regex pattern

        Partitions will be assigned via a group coordinator
        (incompatible with assign_from_user)

        Optionally include listener callback, which must be a
        ConsumerRebalanceListener and will be called before and
        after each rebalance operation.
        """
        if self._user_assignment or (topics and pattern):
            raise IllegalStateError(self._SUBSCRIPTION_EXCEPTION_MESSAGE)
        if not (topics or pattern):
            raise IllegalStateError('Must provide topics or a pattern')

        if pattern:
            log.info('Subscribing to pattern: /%s/', pattern)
            self.subscription = set()
            self.subscribed_pattern = re.compile(pattern)
        else:
            self.change_subscription(topics)

        if listener and not isinstance(listener, ConsumerRebalanceListener):
            raise TypeError('listener must be a ConsumerRebalanceListener')
        self.listener = listener

    def change_subscription(self, topics):
        if self._user_assignment:
            raise IllegalStateError(self._SUBSCRIPTION_EXCEPTION_MESSAGE)

        if self.subscription == set(topics):
            log.warning("subscription unchanged by change_subscription(%s)",
                        topics)
            return

        log.info('Updating subscribed topics to: %s', topics)
        self.subscription = set(topics)
        self._group_subscription.update(topics)
        self.needs_partition_assignment = True

        # Remove any assigned partitions which are no longer subscribed to
        for tp in set(self.assignment.keys()):
            if tp.topic not in self.subscription:
                del self.assignment[tp]

    def group_subscribe(self, topics):
        """Add topics to the current group subscription.

        This is used by the group leader to ensure that it receives metadata
        updates for all topics that any member of the group is subscribed to.

        @param topics list of topics to add to the group subscription
        """
        if self._user_assignment:
            raise IllegalStateError(self._SUBSCRIPTION_EXCEPTION_MESSAGE)
        self._group_subscription.update(topics)

    def mark_for_reassignment(self):
        self._group_subscription.intersection_update(self.subscription)
        self.needs_partition_assignment = True

    def assign_from_user(self, partitions):
        """
        Change the assignment to the specified partitions provided by the user,
        note this is different from assign_from_subscribed()
        whose input partitions are provided from the subscribed topics.

        @param partitions: list (or iterable) of TopicPartition()
        """
        if self.subscription is not None:
            raise IllegalStateError(self._SUBSCRIPTION_EXCEPTION_MESSAGE)

        self._user_assignment.clear()
        self._user_assignment.update(partitions)

        for partition in partitions:
            if partition not in self.assignment:
                self._add_assigned_partition(partition)

        for tp in set(self.assignment.keys()) - self._user_assignment:
            del self.assignment[tp]

        self.needs_partition_assignment = False

    def assign_from_subscribed(self, assignments):
        """
        Change the assignment to the specified partitions returned from the coordinator,
        note this is different from {@link #assignFromUser(Collection)} which directly set the assignment from user inputs
        """
        if self.subscription is None:
            raise IllegalStateError(self._SUBSCRIPTION_EXCEPTION_MESSAGE)

        for tp in assignments:
            if tp.topic not in self.subscription:
                raise ValueError("Assigned partition %s for non-subscribed topic." % tp)
        self.assignment.clear()
        for tp in assignments:
            self._add_assigned_partition(tp)
        self.needs_partition_assignment = False

    def unsubscribe(self):
        self.subscription = None
        self._user_assignment.clear()
        self.assignment.clear()
        self.needs_partition_assignment = True
        self.subscribed_pattern = None

    def group_subscription(self):
        """Get the topic subscription for the group.

        For the leader, this will include the union of all member subscriptions.
        For followers, it is the member's subscription only.

        This is used when querying topic metadata to detect metadata changes
        that would require rebalancing (the leader fetches metadata for all
        topics in the group so that it can do partition assignment).

        @return set of topics
        """
        return self._group_subscription

    def seek(self, partition, offset):
        self.assignment[partition].seek(offset)

    def assigned_partitions(self):
        return set(self.assignment.keys())

    def fetchable_partitions(self):
        fetchable = set()
        for partition, state in six.iteritems(self.assignment):
            if state.is_fetchable():
                fetchable.add(partition)
        return fetchable

    def partitions_auto_assigned(self):
        return self.subscription is not None

    def all_consumed_offsets(self):
        """Returns consumed offsets as {TopicPartition: OffsetAndMetadata}"""
        all_consumed = {}
        for partition, state in six.iteritems(self.assignment):
            if state.has_valid_position:
                all_consumed[partition] = OffsetAndMetadata(state.consumed, '')
        return all_consumed

    def need_offset_reset(self, partition, offset_reset_strategy=None):
        if offset_reset_strategy is None:
            offset_reset_strategy = self._default_offset_reset_strategy
        self.assignment[partition].await_reset(offset_reset_strategy)

    def has_default_offset_reset_policy(self):
        return self._default_offset_reset_strategy != OffsetResetStrategy.NONE

    def is_offset_reset_needed(self, partition):
        return self.assignment[partition].awaiting_reset

    def has_all_fetch_positions(self):
        for state in self.assignment.values():
            if not state.has_valid_position:
                return False
        return True

    def missing_fetch_positions(self):
        missing = set()
        for partition, state in six.iteritems(self.assignment):
            if not state.has_valid_position:
                missing.add(partition)
        return missing

    def is_assigned(self, partition):
        return partition in self.assignment

    def is_paused(self, partition):
        return partition in self.assignment and self.assignment[partition].paused

    def is_fetchable(self, partition):
        return partition in self.assignment and self.assignment[partition].is_fetchable()

    def pause(self, partition):
        self.assignment[partition].pause()

    def resume(self, partition):
        self.assignment[partition].resume()

    def _add_assigned_partition(self, partition):
        self.assignment[partition] = TopicPartitionState()


class TopicPartitionState(object):
    def __init__(self):
        self.committed = None # last committed position
        self.has_valid_position = False # whether we have valid consumed and fetched positions
        self.paused = False # whether this partition has been paused by the user
        self.awaiting_reset = False # whether we are awaiting reset
        self.reset_strategy = None # the reset strategy if awaitingReset is set
        self._consumed = None # offset exposed to the user
        self._fetched = None # current fetch position

    def _set_fetched(self, offset):
        if not self.has_valid_position:
            raise IllegalStateError("Cannot update fetch position without valid consumed/fetched positions")
        self._fetched = offset

    def _get_fetched(self):
        return self._fetched

    fetched = property(_get_fetched, _set_fetched, None, "current fetch position")

    def _set_consumed(self, offset):
        if not self.has_valid_position:
            raise IllegalStateError("Cannot update consumed position without valid consumed/fetched positions")
        self._consumed = offset

    def _get_consumed(self):
        return self._consumed

    consumed = property(_get_consumed, _set_consumed, None, "last consumed position")

    def await_reset(self, strategy):
        self.awaiting_reset = True
        self.reset_strategy = strategy
        self._consumed = None
        self._fetched = None
        self.has_valid_position = False

    def seek(self, offset):
        self._consumed = offset
        self._fetched = offset
        self.awaiting_reset = False
        self.reset_strategy = None
        self.has_valid_position = True

    def pause(self):
        self.paused = True

    def resume(self):
        self.paused = False

    def is_fetchable(self):
        return not self.paused and self.has_valid_position


class ConsumerRebalanceListener(object):
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

    This callback will execute in the user thread as part of the Consumer.poll()
    whenever partition assignment changes.

    It is guaranteed that all consumer processes will invoke
    on_partitions_revoked() prior to any process invoking
    on_partitions_assigned(). So if offsets or other state is saved in the
    on_partitions_revoked() call, it should be saved by the time the process
    taking over that partition has their on_partitions_assigned() callback
    called to load the state.
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def on_partitions_revoked(self, revoked):
        """
        A callback method the user can implement to provide handling of offset
        commits to a customized store on the start of a rebalance operation.
        This method will be called before a rebalance operation starts and
        after the consumer stops fetching data. It is recommended that offsets
        should be committed in this callback to either Kafka or a custom offset
        store to prevent duplicate data.

        NOTE: This method is only called before rebalances. It is not called
        prior to KafkaConsumer.close()

        @param partitions The list of partitions that were assigned to the
                          consumer on the last rebalance
        """
        pass

    @abc.abstractmethod
    def on_partitions_assigned(self, assigned):
        """
        A callback method the user can implement to provide handling of
        customized offsets on completion of a successful partition
        re-assignment. This method will be called after an offset re-assignment
        completes and before the consumer starts fetching data.

        It is guaranteed that all the processes in a consumer group will execute
        their on_partitions_revoked() callback before any instance executes its
        on_partitions_assigned() callback.

        @param partitions The list of partitions that are now assigned to the
                          consumer (may include partitions previously assigned
                          to the consumer)
        """
        pass
