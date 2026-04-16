""" Other useful structs """

from collections import namedtuple


"""A topic and partition tuple

Keyword Arguments:
    topic (str): A topic name
    partition (int): A partition id
"""
TopicPartition = namedtuple("TopicPartition",
    ["topic", "partition"])


"""The Kafka offset commit API

The Kafka offset commit API allows users to provide additional metadata
(in the form of a string) when an offset is committed. This can be useful
(for example) to store information about which node made the commit,
what time the commit was made, etc.

Keyword Arguments:
    offset (int): The offset to be committed
    metadata (str): Non-null metadata
    leader_epoch (int): The last known epoch from the leader / broker
"""
OffsetAndMetadata = namedtuple("OffsetAndMetadata",
    ["offset", "metadata", "leader_epoch"])


"""An offset and timestamp tuple

Keyword Arguments:
    offset (int): An offset
    timestamp (int): The timestamp associated to the offset
    leader_epoch (int): The last known epoch from the leader / broker
"""
OffsetAndTimestamp = namedtuple("OffsetAndTimestamp",
    ["offset", "timestamp", "leader_epoch"])

"""Define retry policy for async producer

Keyword Arguments:
    Limit (int): Number of retries. limit >= 0, 0 means no retries
    backoff_ms (int): Milliseconds to backoff.
    retry_on_timeouts:
"""
RetryOptions = namedtuple("RetryOptions",
    ["limit", "backoff_ms", "retry_on_timeouts"])
