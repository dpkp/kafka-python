from __future__ import absolute_import

from collections import namedtuple


#  SimpleClient Payload Structs - Deprecated

# https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-MetadataAPI
MetadataRequest = namedtuple("MetadataRequest",
    ["topics"])

MetadataResponse = namedtuple("MetadataResponse",
    ["brokers", "topics"])

# https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ConsumerMetadataRequest
ConsumerMetadataRequest = namedtuple("ConsumerMetadataRequest",
    ["groups"])

ConsumerMetadataResponse = namedtuple("ConsumerMetadataResponse",
    ["error", "nodeId", "host", "port"])

# https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ProduceAPI
ProduceRequestPayload = namedtuple("ProduceRequestPayload",
    ["topic", "partition", "messages"])

ProduceResponsePayload = namedtuple("ProduceResponsePayload",
    ["topic", "partition", "error", "offset"])

# https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-FetchAPI
FetchRequestPayload = namedtuple("FetchRequestPayload",
    ["topic", "partition", "offset", "max_bytes"])

FetchResponsePayload = namedtuple("FetchResponsePayload",
    ["topic", "partition", "error", "highwaterMark", "messages"])

# https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetAPI
OffsetRequestPayload = namedtuple("OffsetRequestPayload",
    ["topic", "partition", "time", "max_offsets"])

ListOffsetRequestPayload = namedtuple("ListOffsetRequestPayload",
    ["topic", "partition", "time"])

OffsetResponsePayload = namedtuple("OffsetResponsePayload",
    ["topic", "partition", "error", "offsets"])

ListOffsetResponsePayload = namedtuple("ListOffsetResponsePayload",
    ["topic", "partition", "error", "timestamp", "offset"])

# https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
OffsetCommitRequestPayload = namedtuple("OffsetCommitRequestPayload",
    ["topic", "partition", "offset", "metadata"])

OffsetCommitResponsePayload = namedtuple("OffsetCommitResponsePayload",
    ["topic", "partition", "error"])

OffsetFetchRequestPayload = namedtuple("OffsetFetchRequestPayload",
    ["topic", "partition"])

OffsetFetchResponsePayload = namedtuple("OffsetFetchResponsePayload",
    ["topic", "partition", "offset", "metadata", "error"])



# Other useful structs
TopicPartition = namedtuple("TopicPartition",
    ["topic", "partition"])

BrokerMetadata = namedtuple("BrokerMetadata",
    ["nodeId", "host", "port", "rack"])

PartitionMetadata = namedtuple("PartitionMetadata",
    ["topic", "partition", "leader", "replicas", "isr", "error"])

OffsetAndMetadata = namedtuple("OffsetAndMetadata",
    ["offset", "metadata"])

OffsetAndTimestamp = namedtuple("OffsetAndTimestamp",
    ["offset", "timestamp"])


# Deprecated structs
OffsetAndMessage = namedtuple("OffsetAndMessage",
    ["offset", "message"])

Message = namedtuple("Message",
    ["magic", "attributes", "key", "value"])

KafkaMessage = namedtuple("KafkaMessage",
    ["topic", "partition", "offset", "key", "value"])


# Define retry policy for async producer
# Limit value: int >= 0, 0 means no retries
RetryOptions = namedtuple("RetryOptions",
    ["limit", "backoff_ms", "retry_on_timeouts"])


# Support legacy imports from kafka.common
from kafka.errors import *
