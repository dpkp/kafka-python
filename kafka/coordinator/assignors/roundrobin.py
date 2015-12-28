import collections
import itertools
import logging

import six

from .abstract import AbstractPartitionAssignor
from ...common import TopicPartition
from ..consumer import ConsumerProtocolMemberMetadata, ConsumerProtocolMemberAssignment

log = logging.getLogger(__name__)


class RoundRobinPartitionAssignor(AbstractPartitionAssignor):
    name = 'roundrobin'
    version = 0

    @classmethod
    def assign(cls, cluster, member_metadata):
        all_topics = set()
        for metadata in six.itervalues(member_metadata):
            all_topics.update(metadata.subscription)

        all_topic_partitions = []
        for topic in all_topics:
            partitions = cluster.partitions_for_topic(topic)
            if partitions is None:
                log.warning('No partition metadata for topic %s', topic)
                continue
            for partition in partitions:
                all_topic_partitions.append(TopicPartition(topic, partition))
        all_topic_partitions.sort()

        # construct {member_id: {topic: [partition, ...]}}
        assignment = collections.defaultdict(lambda: collections.defaultdict(list))

        member_iter = itertools.cycle(sorted(member_metadata.keys()))
        for partition in all_topic_partitions:
            member_id = member_iter.next()

            # Because we constructed all_topic_partitions from the set of
            # member subscribed topics, we should be safe assuming that
            # each topic in all_topic_partitions is in at least one member
            # subscription; otherwise this could yield an infinite loop
            while partition.topic not in member_metadata[member_id].subscription:
                member_id = member_iter.next()
            assignment[member_id][partition.topic].append(partition.partition)

        protocol_assignment = {}
        for member_id in member_metadata:
            protocol_assignment[member_id] = ConsumerProtocolMemberAssignment(
                cls.version,
                assignment[member_id].items(),
                b'')
        return protocol_assignment

    @classmethod
    def metadata(cls, topics):
        return ConsumerProtocolMemberMetadata(cls.version, list(topics), b'')

    @classmethod
    def on_assignment(cls, assignment):
        pass
