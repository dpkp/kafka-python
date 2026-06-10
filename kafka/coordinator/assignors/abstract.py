from abc import ABC, abstractmethod, abstractproperty
from enum import IntEnum

from kafka.protocol.consumer.metadata import (
    ConsumerProtocolSubscription, ConsumerProtocolAssignment,
)


class RebalanceProtocol(IntEnum):
    """KIP-429: rebalance protocol mode for a partition assignor.

    EAGER - pre-KIP-429 behaviour: every member revokes its full
    assignment before JoinGroup, then receives a fresh assignment in
    SyncGroup. Simple but causes a "stop the world" pause on every
    rebalance.

    COOPERATIVE - KIP-429 incremental rebalance: members keep their
    existing assignment across JoinGroup; the leader's assignment
    indicates the partitions that need to move; only revoked
    partitions are released, and only newly-assigned partitions
    invoke the listener. A second rebalance round picks up partitions
    that were revoked in round 1.
    """
    EAGER = 0
    COOPERATIVE = 1


class AbstractPartitionAssignor(ABC):
    """
    Abstract assignor implementation which does some common grunt work (in particular collecting
    partition counts which are always needed in assignors).
    """

    @abstractproperty
    def name(self):
        """.name should be a string identifying the assignor"""
        pass

    def supported_protocols(self):
        """Return the list of :class:`RebalanceProtocol` modes this
        assignor supports, in order of preference.

        Default is ``[EAGER]`` - every legacy assignor (Range,
        RoundRobin, the original Sticky from KIP-54) behaves this
        way. Override in subclasses that participate in KIP-429
        incremental cooperative rebalancing (e.g.
        ``CooperativeStickyAssignor``).
        """
        return [RebalanceProtocol.EAGER]

    @abstractmethod
    def assign(self, cluster, members):
        """Perform group assignment given cluster metadata and member subscriptions

        Arguments:
            cluster (ClusterMetadata): metadata for use in assignment
            members ([JoinGroupResponseMember]): member_id and metadata
                for each member in the group, including group_instance_id
                when available (v5+). metadata is a decoded instance of
                ConsumerProtocolSubscription.

        Returns:
            dict: {member_id: ConsumerProtocolAssignment}
        """
        pass

    @abstractmethod
    def metadata(self, topics):
        """Generate ProtocolMetadata to be submitted via JoinGroupRequest.

        Arguments:
            topics (set): a member's subscribed topics

        Returns:
            ConsumerProtocolSubscription
        """
        pass

    @abstractmethod
    def on_assignment(self, assignment, generation):
        """Callback that runs on each assignment.

        This method can be used to update internal state, if any, of the
        partition assignor.

        Arguments:
            assignment (ConsumerProtocolAssignment): the member's assignment
            generation (int): generation id of assignment
        """
        pass
