from __future__ import absolute_import

import collections
import itertools
import logging

from kafka.vendor import six

from kafka.coordinator.assignors.abstract import AbstractPartitionAssignor
from kafka.coordinator.protocol import ConsumerProtocolMemberMetadata_v0, ConsumerProtocolMemberAssignment_v0

log = logging.getLogger(__name__)


class RangePartitionAssignor(AbstractPartitionAssignor):
    """
    The range assignor works on a per-topic basis. For each topic, we lay out
    the available partitions in numeric order and the consumers in
    lexicographic order. We then divide the number of partitions by the total
    number of consumers to determine the number of partitions to assign to each
    consumer. If it does not evenly divide, then the first few consumers will
    have one extra partition.

    For example, suppose there are two consumers C0 and C1, two topics t0 and
    t1, and each topic has 3 partitions, resulting in partitions t0p0, t0p1,
    t0p2, t1p0, t1p1, and t1p2.

    The assignment will be:
        C0: [t0p0, t0p1, t1p0, t1p1]
        C1: [t0p2, t1p2]
    """
    name = 'range'
    version = 0

    @classmethod
    def assign(cls, cluster, group_subscriptions):
        consumers_per_topic = collections.defaultdict(list)
        for member_id, subscription in six.iteritems(group_subscriptions):
            for topic in subscription.topics:
                consumers_per_topic[topic].append((subscription.group_instance_id, member_id))

        # construct {member_id: {topic: [partition, ...]}}
        assignment = collections.defaultdict(dict)

        for topic in consumers_per_topic:
            # group by static members (True) v dynamic members (False)
            grouped = {k: list(g) for k, g in itertools.groupby(consumers_per_topic[topic], key=lambda ids: ids[0] is not None)}
            consumers_per_topic[topic] = sorted(grouped.get(True, [])) + sorted(grouped.get(False, [])) # sorted static members first, then sorted dynamic

        for topic, consumers_for_topic in six.iteritems(consumers_per_topic):
            partitions = cluster.partitions_for_topic(topic)
            if partitions is None:
                log.warning('No partition metadata for topic %s', topic)
                continue
            partitions = sorted(partitions)

            partitions_per_consumer = len(partitions) // len(consumers_for_topic)
            consumers_with_extra = len(partitions) % len(consumers_for_topic)

            for i, (_group_instance_id, member_id) in enumerate(consumers_for_topic):
                start = partitions_per_consumer * i
                start += min(i, consumers_with_extra)
                length = partitions_per_consumer
                if not i + 1 > consumers_with_extra:
                    length += 1
                assignment[member_id][topic] = partitions[start:start+length]

        protocol_assignment = {}
        for member_id in group_subscriptions:
            protocol_assignment[member_id] = ConsumerProtocolMemberAssignment_v0(
                cls.version,
                sorted(assignment[member_id].items()),
                b'')
        return protocol_assignment

    @classmethod
    def metadata(cls, topics):
        return ConsumerProtocolMemberMetadata_v0(cls.version, list(topics), b'')

    @classmethod
    def on_assignment(cls, assignment):
        pass
