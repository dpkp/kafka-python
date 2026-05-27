"""KIP-429 cooperative sticky partition assignor.

Wraps :class:`StickyPartitionAssignor` (KIP-54) with the two-phase
"incremental cooperative" rebalancing protocol:

  * Members keep their assignment across JoinGroup - no global revoke.
  * The leader runs the sticky algorithm to compute the *ideal* final
    assignment, then identifies any partition that is moving from one
    owner to another and *removes it from the new owner's first-round
    assignment*. The current owner sees its assignment shrink, revokes
    the lost partition, and the broker is signaled (via
    ``request_rejoin``) that another rebalance is needed.
  * Round two: the freshly-revoked partition is owned by nobody; the
    sticky algorithm now gives it to its intended new owner.

This avoids the "stop the world" pause that EAGER mode imposes - each
member only pauses while it's processing the specific partitions
moving in or out of its own assignment.

References:
  * KIP-429: https://cwiki.apache.org/confluence/x/vAclBg
  * Java: org.apache.kafka.clients.consumer.CooperativeStickyAssignor
"""

from collections import defaultdict

from kafka.coordinator.assignors.abstract import RebalanceProtocol
from kafka.coordinator.assignors.sticky.sticky_assignor import (
    StickyAssignmentExecutor,
    StickyAssignorMemberMetadataV1,
    StickyPartitionAssignor,
)
from kafka.protocol.consumer.metadata import (
    ConsumerProtocolAssignment, ConsumerProtocolSubscription,
)
from kafka.structs import TopicPartition


# Wire version 1 of ConsumerProtocolSubscription is what KIP-429 added.
# Members advertise their currently-owned partitions in the
# ``owned_partitions`` field so the leader can compute the diff.
_COOPERATIVE_SUBSCRIPTION_VERSION = 1


class CooperativeStickyAssignor(StickyPartitionAssignor):
    """KIP-429 cooperative variant of the sticky assignor.

    Behaviorally identical to :class:`StickyPartitionAssignor` for
    final partition placement (it inherits the same algorithm) - the
    only difference is that movements are staged across two rebalance
    rounds so no member ever sees a partition assigned to it while
    another member still owns it.
    """

    name = "cooperative-sticky"
    # Bump the wire metadata to v1 so OwnedPartitions is encoded on
    # JoinGroup. The leader reads .owned_partitions to compute the
    # set of partitions that are moving.
    version = _COOPERATIVE_SUBSCRIPTION_VERSION

    def supported_protocols(self):
        # COOPERATIVE only - mixing this assignor with eager assignors
        # in the same consumer is rejected at consumer init time
        # (see KafkaConsumer.__init__ validation).
        return [RebalanceProtocol.COOPERATIVE]

    def metadata(self, topics):
        # Encode OwnedPartitions (v1+) so the leader can compute the
        # cooperative diff. The base class uses StickyAssignorUserData
        # for the same purpose in v0 - under cooperative we surface
        # the owned set via the dedicated schema field instead.
        SubTP = ConsumerProtocolSubscription.TopicPartition
        owned_partitions = []
        if self.member_assignment is not None:
            by_topic = defaultdict(list)
            for tp in self.member_assignment:
                by_topic[tp.topic].append(tp.partition)
            owned_partitions = [
                SubTP(topic=t, partitions=sorted(parts))
                for t, parts in by_topic.items()
            ]
        return ConsumerProtocolSubscription(
            version=self.version,
            topics=sorted(topics),
            user_data=b'',
            owned_partitions=owned_partitions,
        )

    @classmethod
    def parse_member_metadata(cls, metadata):
        """Decode a member's ``ConsumerProtocolSubscription``.

        Cooperative members carry owned partitions in the
        ``owned_partitions`` schema field (v1+) rather than the
        ``user_data`` blob the legacy sticky assignor uses. Returns
        the same ``StickyAssignorMemberMetadataV1`` shape so the
        underlying sticky algorithm can consume it unchanged.
        """
        member_partitions = []
        # owned_partitions is a list of TopicPartition data containers
        # (v1+); on v0 metadata the field is absent - treat as empty.
        for tp in getattr(metadata, 'owned_partitions', None) or ():
            for partition in tp.partitions:
                member_partitions.append(TopicPartition(tp.topic, partition))

        generation = metadata.generation_id
        return StickyAssignorMemberMetadataV1(
            partitions=member_partitions,
            generation=metadata.generation_id, # requires schema v2, defaults to -1
            subscription=list(metadata.topics),
        )

    def assign(self, cluster, members):
        """Cooperative two-phase assignment.

        1. Compute the ideal final sticky assignment.
        2. Build a map of currently-owned partitions across all
           members from their ``OwnedPartitions``.
        3. For any partition whose final owner differs from its
           current owner, remove it from the new owner's first-round
           assignment. The current owner sees its assignment shrink,
           revokes the partition, and re-joins; on round two the
           partition is unowned and the algorithm assigns it.
        """
        members_metadata = {
            member.member_id: self.parse_member_metadata(member.metadata)
            for member in members
        }
        executor = StickyAssignmentExecutor(cluster, members_metadata)
        executor.perform_initial_assignment()
        executor.balance()
        # Expose for diagnostic tests (matches parent behaviour).
        self._latest_partition_movements = executor.partition_movements

        # Map: partition -> current_owner_member_id (None if unowned).
        currently_owned = {}
        for member_id, parsed in members_metadata.items():
            for tp in parsed.partitions:
                currently_owned[tp] = member_id

        # Build the round-1 assignment: drop any partition that's
        # moving (final owner != current owner). The current owner
        # will revoke it in _on_join_complete; the broker will see
        # the consumer re-join and round 2 will land the partition
        # on the intended new owner.
        #
        # ``executor.get_final_assignment`` returns the canonical wire
        # shape: ``[(topic, [partition, ...]), ...]``. We rebuild the
        # same shape after filtering so the encoder is happy.
        cooperative = {}
        for member in members:
            member_id = member.member_id
            kept_by_topic = defaultdict(list)
            for topic, parts in executor.get_final_assignment(member_id):
                for p in parts:
                    tp = TopicPartition(topic, p)
                    current_owner = currently_owned.get(tp)
                    if current_owner is None or current_owner == member_id:
                        # Either nobody owned it, or this member
                        # already owns it. Safe to assign now.
                        kept_by_topic[topic].append(p)
                    # else: partition is moving; defer to round 2.
            assigned_partitions = sorted(
                (t, sorted(ps)) for t, ps in kept_by_topic.items())
            cooperative[member_id] = ConsumerProtocolAssignment(
                self.version, assigned_partitions, b'')
        return cooperative
