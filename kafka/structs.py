""" Other useful structs """

from collections import namedtuple


TopicPartition = namedtuple("TopicPartition",
    ["topic", "partition"])
TopicPartition.__doc__ = """A topic and partition tuple

Keyword Arguments:
    topic (str): A topic name
    partition (int): A partition id
"""


TopicPartitionReplica = namedtuple("TopicPartitionReplica",
    ["topic", "partition", "broker_id"])
TopicPartitionReplica.__doc__ = """A topic / partition / broker replica tuple

Keyword Arguments:
    topic (str): A topic name
    partition (int): A partition id
    broker_id (int): The node_id of the broker hosting the replica
"""


OffsetAndMetadata = namedtuple("OffsetAndMetadata",
    ["offset", "metadata", "leader_epoch"], defaults=[None, '', -1])
OffsetAndMetadata.__doc__ = """Container for committed group offset data.

The Kafka offset commit API allows users to provide additional metadata
(in the form of a string) when an offset is committed. This can be useful
(for example) to store information about which node made the commit,
what time the commit was made, etc.

Keyword Arguments:
    offset (int): The offset to be committed
    metadata (str): Non-null metadata
    leader_epoch (int): The last known epoch from the leader / broker
"""


OffsetAndTimestamp = namedtuple("OffsetAndTimestamp",
    ["offset", "timestamp", "leader_epoch"])
OffsetAndTimestamp.__doc__ = """An offset and timestamp tuple

Keyword Arguments:
    offset (int): An offset
    timestamp (int): The timestamp associated to the offset
    leader_epoch (int): The last known epoch from the leader / broker
"""


ConsumerGroupMetadata = namedtuple("ConsumerGroupMetadata",
    ["group_id", "generation_id", "member_id", "group_instance_id"],
    defaults=[-1, '', None])
ConsumerGroupMetadata.__doc__ = """A snapshot of a consumer's group membership (KIP-447).

Passed to KafkaProducer.send_offsets_to_transaction() so the broker can fence
stale consumer instances when committing offsets inside a transaction. The
broker uses member_id + generation_id + group_instance_id to verify the
producer is acting on behalf of the current group generation.

Keyword Arguments:
    group_id (str): The consumer group id.
    generation_id (int): The current generation id (-1 if unjoined).
    member_id (str): The current member id ('' if unjoined).
    group_instance_id (str): The static membership instance id, or None.
"""
