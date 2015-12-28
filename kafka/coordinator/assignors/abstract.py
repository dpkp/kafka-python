import abc
import logging

log = logging.getLogger(__name__)


class AbstractPartitionAssignor(object):
    """
    Abstract assignor implementation which does some common grunt work (in particular collecting
    partition counts which are always needed in assignors).
    """

    @abc.abstractproperty
    def name(self):
        """.name should be a string identifying the assignor"""
        pass

    @abc.abstractmethod
    def assign(self, cluster, members):
        """Perform group assignment given cluster metadata and member subscriptions

        @param cluster: cluster metadata
        @param members: {member_id: subscription}
        @return {member_id: MemberAssignment}
        """
        pass

    @abc.abstractmethod
    def metadata(self, topics):
        """return ProtocolMetadata to be submitted via JoinGroupRequest"""
        pass

    @abc.abstractmethod
    def on_assignment(self, assignment):
        pass
