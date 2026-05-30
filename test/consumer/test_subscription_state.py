import time

import pytest

from kafka import TopicPartition
from kafka.consumer.subscription_state import SubscriptionState, TopicPartitionState
from kafka.structs import OffsetAndMetadata


def test_type_error():
    s = SubscriptionState()
    with pytest.raises(TypeError):
        s.subscribe(topics='foo')

    s.subscribe(topics=['foo'])


def test_change_subscription():
    s = SubscriptionState()
    s.subscribe(topics=['foo'])
    assert s.subscription == set(['foo'])
    s.change_subscription(['bar'])
    assert s.subscription == set(['bar'])


def test_group_subscribe():
    s = SubscriptionState()
    s.subscribe(topics=['foo'])
    assert s.subscription == set(['foo'])
    s.group_subscribe(['bar'])
    assert s.subscription == set(['foo'])
    assert s._group_subscription == set(['foo', 'bar'])

    s.reset_group_subscription()
    assert s.subscription == set(['foo'])
    assert s._group_subscription == set(['foo'])


def test_change_subscription_after_assignment():
    s = SubscriptionState()
    s.subscribe(topics=['foo'])
    s.assign_from_subscribed([TopicPartition('foo', 0), TopicPartition('foo', 1)])
    # Changing subscription retains existing assignment until next rebalance
    s.change_subscription(['bar'])
    assert set(s.assignment.keys()) == set([TopicPartition('foo', 0), TopicPartition('foo', 1)])


# ---------------------------------------------------------------------------
# assign_from_subscribed: state-preserving (Java-faithful) assignment update
# ---------------------------------------------------------------------------


class TestAssignFromSubscribed:
    """``assign_from_subscribed`` preserves ``TopicPartitionState`` for
    partitions present in both the old and new assignment. New partitions
    get fresh state; revoked partitions are dropped. This is what
    makes KIP-429 cooperative rebalancing efficient (and what makes
    EAGER rebalances avoid wiping state for partitions that didn't
    move)."""

    def _subscribed(self):
        s = SubscriptionState()
        s.subscribe(topics=['foo'])
        return s

    def test_initial_assignment(self):
        """First-call behaviour (no prior assignment to preserve)."""
        s = self._subscribed()
        with pytest.raises(ValueError):
            s.assign_from_subscribed([TopicPartition('bar', 0)])

        s.assign_from_subscribed([TopicPartition('foo', 0), TopicPartition('foo', 1)])
        assert set(s.assignment.keys()) == set([
            TopicPartition('foo', 0), TopicPartition('foo', 1)])
        assert all(isinstance(tps, TopicPartitionState)
                   for tps in s.assignment.values())
        assert all(not tps.has_valid_position for tps in s.assignment.values())

    def test_preserves_position_on_kept_partition(self):
        s = self._subscribed()
        s.assign_from_subscribed([TopicPartition('foo', 0), TopicPartition('foo', 1)])
        # Seed state on partition 0 that should survive the rebalance.
        s.assignment[TopicPartition('foo', 0)].seek(
            OffsetAndMetadata(offset=100, metadata='', leader_epoch=-1))

        # Rebalance: keep partition 0, drop partition 1, add partition 2.
        s.assign_from_subscribed([
            TopicPartition('foo', 0), TopicPartition('foo', 2)])

        # Kept partition retains its position.
        assert s.assignment[TopicPartition('foo', 0)].position.offset == 100
        # New partition has fresh (no-position) state.
        assert not s.assignment[TopicPartition('foo', 2)].has_valid_position
        # Revoked partition is gone.
        assert TopicPartition('foo', 1) not in s.assignment

    def test_preserves_paused_state_on_kept_partition(self):
        s = self._subscribed()
        s.assign_from_subscribed([TopicPartition('foo', 0)])
        s.pause(TopicPartition('foo', 0))
        assert s.is_paused(TopicPartition('foo', 0))

        s.assign_from_subscribed([
            TopicPartition('foo', 0), TopicPartition('foo', 1)])
        # Pause survived.
        assert s.is_paused(TopicPartition('foo', 0))
        # New partition isn't paused by default.
        assert not s.is_paused(TopicPartition('foo', 1))

    def test_mark_pending_revocation_blocks_fetches(self):
        """KIP-429: a partition flagged for pending revocation must
        report not-fetchable so the fetcher skips it while a revoke
        listener is running."""
        s = self._subscribed()
        s.assign_from_subscribed([TopicPartition('foo', 0)])
        # Give it a valid position so is_fetchable would otherwise be True.
        s.assignment[TopicPartition('foo', 0)].seek(
            OffsetAndMetadata(offset=0, metadata='', leader_epoch=-1))
        assert s.is_fetchable(TopicPartition('foo', 0))

        s.mark_pending_revocation({TopicPartition('foo', 0)})
        assert not s.is_fetchable(TopicPartition('foo', 0))
        # The pending-revocation flag is dropped along with the state
        # object on the next assign_from_subscribed that excludes the
        # partition (single-shot).
        s.assign_from_subscribed([TopicPartition('foo', 1)])
        s.assign_from_subscribed([
            TopicPartition('foo', 0), TopicPartition('foo', 1)])
        s.assignment[TopicPartition('foo', 0)].seek(
            OffsetAndMetadata(offset=0, metadata='', leader_epoch=-1))
        assert s.is_fetchable(TopicPartition('foo', 0))

    def test_mark_pending_revocation_ignores_unknown_partition(self):
        """mark_pending_revocation must not raise if a partition isn't
        currently assigned - the assignment may have shifted between
        the caller's snapshot and the call."""
        s = self._subscribed()
        s.assign_from_subscribed([TopicPartition('foo', 0)])
        s.mark_pending_revocation({
            TopicPartition('foo', 0), TopicPartition('foo', 99)})
        # Still gates the assigned one.
        assert s.assignment[TopicPartition('foo', 0)]._pending_revocation is True

    def test_preserves_preferred_read_replica_on_kept_partition(self):
        s = self._subscribed()
        s.assign_from_subscribed([TopicPartition('foo', 0)])
        s.assignment[TopicPartition('foo', 0)].update_preferred_read_replica(
            5, time.monotonic() + 60)
        assert s.assignment[TopicPartition('foo', 0)].preferred_read_replica() == 5

        s.assign_from_subscribed([
            TopicPartition('foo', 0), TopicPartition('foo', 1)])
        # KIP-392 cache survives.
        assert s.assignment[TopicPartition('foo', 0)].preferred_read_replica() == 5
        # New partition starts with no preferred replica.
        assert s.assignment[TopicPartition('foo', 1)].preferred_read_replica() is None

    def test_object_identity_for_kept_partition_state(self):
        """The actual TopicPartitionState instance must survive - not a
        copy with the same field values. This matters for callers (like
        the Fetcher) that hold references to TopicPartitionState
        instances across rebalances."""
        s = self._subscribed()
        s.assign_from_subscribed([TopicPartition('foo', 0)])
        original = s.assignment[TopicPartition('foo', 0)]
        s.assign_from_subscribed([
            TopicPartition('foo', 0), TopicPartition('foo', 1)])
        assert s.assignment[TopicPartition('foo', 0)] is original

    def test_revoked_partition_state_is_dropped(self):
        s = self._subscribed()
        s.assign_from_subscribed([TopicPartition('foo', 0), TopicPartition('foo', 1)])
        original = s.assignment[TopicPartition('foo', 1)]

        s.assign_from_subscribed([TopicPartition('foo', 0)])
        assert TopicPartition('foo', 1) not in s.assignment
        # The revoked TopicPartitionState is no longer referenced by the
        # subscription (caller may still hold the reference, but the
        # dict has dropped it).
        assert original not in s.assignment.values()

    def test_validates_topic_subscribed_before_mutation(self):
        """A bad topic must raise BEFORE any mutation. Half-applied
        state would leave the consumer in an inconsistent place."""
        s = self._subscribed()
        s.assign_from_subscribed([TopicPartition('foo', 0)])
        original_keys = list(s.assignment.keys())

        with pytest.raises(ValueError):
            s.assign_from_subscribed([
                TopicPartition('foo', 2),
                TopicPartition('not-subscribed', 0)])
        # Nothing was added or removed.
        assert list(s.assignment.keys()) == original_keys

    def test_full_replace_drops_all_old_partitions(self):
        """A new assignment with zero overlap drops every old partition."""
        s = self._subscribed()
        s.assign_from_subscribed([TopicPartition('foo', 0), TopicPartition('foo', 1)])
        s.assign_from_subscribed([TopicPartition('foo', 2)])
        assert set(s.assignment.keys()) == {TopicPartition('foo', 2)}

    def test_no_op_assignment_preserves_everything(self):
        """Same assignment in/out: every state field survives identical."""
        s = self._subscribed()
        s.assign_from_subscribed([TopicPartition('foo', 0), TopicPartition('foo', 1)])
        s.assignment[TopicPartition('foo', 0)].seek(
            OffsetAndMetadata(offset=50, metadata='', leader_epoch=-1))
        s.pause(TopicPartition('foo', 1))

        s.assign_from_subscribed([
            TopicPartition('foo', 0), TopicPartition('foo', 1)])

        assert s.assignment[TopicPartition('foo', 0)].position.offset == 50
        assert s.is_paused(TopicPartition('foo', 1))
