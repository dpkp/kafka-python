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

OffsetFetchRequest = namedtuple("OffsetFetchRequest", ["topic", "partition"])

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

BrokerMetadata = namedtuple("BrokerMetadata", ["nodeId", "host", "port"])

PartitionMetadata = namedtuple("PartitionMetadata",
                               ["topic", "partition", "leader",
                                "replicas", "isr"])

# Other useful structs
OffsetAndMessage = namedtuple("OffsetAndMessage", ["offset", "message"])
Message = namedtuple("Message", ["magic", "attributes", "key", "value"])
TopicAndPartition = namedtuple("TopicAndPartition", ["topic", "partition"])


ErrorStrings = {
    -1 : 'UNKNOWN',
    0 : 'NO_ERROR',
    1 : 'OFFSET_OUT_OF_RANGE',
    2 : 'INVALID_MESSAGE',
    3 : 'UNKNOWN_TOPIC_OR_PARTITON',
    4 : 'INVALID_FETCH_SIZE',
    5 : 'LEADER_NOT_AVAILABLE',
    6 : 'NOT_LEADER_FOR_PARTITION',
    7 : 'REQUEST_TIMED_OUT',
    8 : 'BROKER_NOT_AVAILABLE',
    9 : 'REPLICA_NOT_AVAILABLE',
    10 : 'MESSAGE_SIZE_TOO_LARGE',
    11 : 'STALE_CONTROLLER_EPOCH',
    12 : 'OFFSET_METADATA_TOO_LARGE',
}

class ErrorMapping(object):
    pass

for k, v in ErrorStrings.items():
    setattr(ErrorMapping, v, k)

#################
#   Exceptions  #
#################


class KafkaError(RuntimeError):
    pass


class KafkaUnavailableError(KafkaError):
    pass


class BrokerResponseError(KafkaError):
    pass


class LeaderUnavailableError(KafkaError):
    pass


class PartitionUnavailableError(KafkaError):
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
