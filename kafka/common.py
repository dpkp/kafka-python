from collections import namedtuple

###############
#   Structs   #
###############

# Request payloads
ProduceRequest = namedtuple("ProduceRequest",
                            ["topic", "partition", "messages"])

FetchRequest = namedtuple("FetchRequest",
                          ["topic", "partition", "offset", "max_bytes"])

OffsetRequest = namedtuple("OffsetRequest",
                           ["topic", "partition", "time", "max_offsets"])

OffsetCommitRequest = namedtuple("OffsetCommitRequest",
                                 ["topic", "partition", "offset", "metadata"])

MetadataRequest = namedtuple("MetadataRequest",
    ["topics"])

OffsetFetchRequest = namedtuple("OffsetFetchRequest", ["topic", "partition"])

MetadataResponse = namedtuple("MetadataResponse",
    ["brokers", "topics"])

# Response payloads
ProduceResponse = namedtuple("ProduceResponse",
                             ["topic", "partition", "error", "offset"])

FetchResponse = namedtuple("FetchResponse", ["topic", "partition", "error",
                                             "highwaterMark", "messages"])

OffsetResponse = namedtuple("OffsetResponse",
                            ["topic", "partition", "error", "offsets"])

OffsetCommitResponse = namedtuple("OffsetCommitResponse",
                                  ["topic", "partition", "error"])

OffsetFetchResponse = namedtuple("OffsetFetchResponse",
                                 ["topic", "partition", "offset",
                                  "metadata", "error"])



# Other useful structs
BrokerMetadata = namedtuple("BrokerMetadata",
    ["nodeId", "host", "port"])

TopicMetadata = namedtuple("TopicMetadata",
    ["topic", "error", "partitions"])

PartitionMetadata = namedtuple("PartitionMetadata",
    ["topic", "partition", "leader", "replicas", "isr", "error"])

OffsetAndMessage = namedtuple("OffsetAndMessage",
    ["offset", "message"])

Message = namedtuple("Message",
    ["magic", "attributes", "key", "value"])

TopicAndPartition = namedtuple("TopicAndPartition",
    ["topic", "partition"])

KafkaMessage = namedtuple("KafkaMessage",
    ["topic", "partition", "offset", "key", "value"])


#################
#   Exceptions  #
#################


class KafkaError(RuntimeError):
    pass


class BrokerResponseError(KafkaError):
    pass

class NoError(BrokerResponseError):
    errno = 0
    message = 'SUCCESS'

class UnknownError(BrokerResponseError):
    errno = -1
    message = 'UNKNOWN'


class OffsetOutOfRangeError(BrokerResponseError):
    errno = 1
    message = 'OFFSET_OUT_OF_RANGE'


class InvalidMessageError(BrokerResponseError):
    errno = 2
    message = 'INVALID_MESSAGE'


class UnknownTopicOrPartitionError(BrokerResponseError):
    errno = 3
    message = 'UNKNOWN_TOPIC_OR_PARTITON'


class InvalidFetchRequestError(BrokerResponseError):
    errno = 4
    message = 'INVALID_FETCH_SIZE'


class LeaderNotAvailableError(BrokerResponseError):
    errno = 5
    message = 'LEADER_NOT_AVAILABLE'


class NotLeaderForPartitionError(BrokerResponseError):
    errno = 6
    message = 'NOT_LEADER_FOR_PARTITION'


class RequestTimedOutError(BrokerResponseError):
    errno = 7
    message = 'REQUEST_TIMED_OUT'


class BrokerNotAvailableError(BrokerResponseError):
    errno = 8
    message = 'BROKER_NOT_AVAILABLE'


class ReplicaNotAvailableError(BrokerResponseError):
    errno = 9
    message = 'REPLICA_NOT_AVAILABLE'


class MessageSizeTooLargeError(BrokerResponseError):
    errno = 10
    message = 'MESSAGE_SIZE_TOO_LARGE'


class StaleControllerEpochError(BrokerResponseError):
    errno = 11
    message = 'STALE_CONTROLLER_EPOCH'


class OffsetMetadataTooLargeError(BrokerResponseError):
    errno = 12
    message = 'OFFSET_METADATA_TOO_LARGE'


class StaleLeaderEpochCodeError(BrokerResponseError):
    errno = 13
    message = 'STALE_LEADER_EPOCH_CODE'


class KafkaUnavailableError(KafkaError):
    pass


class KafkaTimeoutError(KafkaError):
    pass


class FailedPayloadsError(KafkaError):
    pass


class ConnectionError(KafkaError):
    pass


class BufferUnderflowError(KafkaError):
    pass


class ChecksumError(KafkaError):
    pass


class ConsumerFetchSizeTooSmall(KafkaError):
    pass


class ConsumerNoMoreData(KafkaError):
    pass


class ConsumerTimeout(KafkaError):
    pass


class ProtocolError(KafkaError):
    pass


class UnsupportedCodecError(KafkaError):
    pass


class KafkaConfigurationError(KafkaError):
    pass


kafka_errors = {
    -1 : UnknownError,
    0  : NoError,
    1  : OffsetOutOfRangeError,
    2  : InvalidMessageError,
    3  : UnknownTopicOrPartitionError,
    4  : InvalidFetchRequestError,
    5  : LeaderNotAvailableError,
    6  : NotLeaderForPartitionError,
    7  : RequestTimedOutError,
    8  : BrokerNotAvailableError,
    9  : ReplicaNotAvailableError,
    10 : MessageSizeTooLargeError,
    11 : StaleControllerEpochError,
    12 : OffsetMetadataTooLargeError,
    13 : StaleLeaderEpochCodeError,
}


def check_error(response):
    error = kafka_errors.get(response.error, UnknownError)
    if error is not NoError:
        raise error(response)

