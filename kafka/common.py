import inspect
import sys
from collections import namedtuple

###############
#   Structs   #
###############

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
ProduceRequest = namedtuple("ProduceRequest",
    ["topic", "partition", "messages"])

ProduceResponse = namedtuple("ProduceResponse",
    ["topic", "partition", "error", "offset"])

# https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-FetchAPI
FetchRequest = namedtuple("FetchRequest",
    ["topic", "partition", "offset", "max_bytes"])

FetchResponse = namedtuple("FetchResponse",
    ["topic", "partition", "error", "highwaterMark", "messages"])

# https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetAPI
OffsetRequest = namedtuple("OffsetRequest",
    ["topic", "partition", "time", "max_offsets"])

OffsetResponse = namedtuple("OffsetResponse",
    ["topic", "partition", "error", "offsets"])

# https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
OffsetCommitRequest = namedtuple("OffsetCommitRequest",
    ["topic", "partition", "offset", "metadata"])

OffsetCommitResponse = namedtuple("OffsetCommitResponse",
    ["topic", "partition", "error"])

OffsetFetchRequest = namedtuple("OffsetFetchRequest",
    ["topic", "partition"])

OffsetFetchResponse = namedtuple("OffsetFetchResponse",
    ["topic", "partition", "offset", "metadata", "error"])



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

# Define retry policy for async producer
# Limit value: int >= 0, 0 means no retries
RetryOptions = namedtuple("RetryOptions",
    ["limit", "backoff_ms", "retry_on_timeouts"])


#################
#   Exceptions  #
#################


class KafkaError(RuntimeError):
    pass


class BrokerResponseError(KafkaError):
    pass


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


class OffsetsLoadInProgressCode(BrokerResponseError):
    errno = 14
    message = 'OFFSETS_LOAD_IN_PROGRESS_CODE'


class ConsumerCoordinatorNotAvailableCode(BrokerResponseError):
    errno = 15
    message = 'CONSUMER_COORDINATOR_NOT_AVAILABLE_CODE'


class NotCoordinatorForConsumerCode(BrokerResponseError):
    errno = 16
    message = 'NOT_COORDINATOR_FOR_CONSUMER_CODE'


class KafkaUnavailableError(KafkaError):
    pass


class KafkaTimeoutError(KafkaError):
    pass


class FailedPayloadsError(KafkaError):
    def __init__(self, payload, *args):
        super(FailedPayloadsError, self).__init__(*args)
        self.payload = payload


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


class AsyncProducerQueueFull(KafkaError):
    def __init__(self, failed_msgs, *args):
        super(AsyncProducerQueueFull, self).__init__(*args)
        self.failed_msgs = failed_msgs


def _iter_broker_errors():
    for name, obj in inspect.getmembers(sys.modules[__name__]):
        if inspect.isclass(obj) and issubclass(obj, BrokerResponseError) and obj != BrokerResponseError:
            yield obj


kafka_errors = dict([(x.errno, x) for x in _iter_broker_errors()])


def check_error(response):
    if isinstance(response, Exception):
        raise response
    if response.error:
        error_class = kafka_errors.get(response.error, UnknownError)
        raise error_class(response)


RETRY_BACKOFF_ERROR_TYPES = (
    KafkaUnavailableError, LeaderNotAvailableError,
    ConnectionError, FailedPayloadsError
)


RETRY_REFRESH_ERROR_TYPES = (
    NotLeaderForPartitionError, UnknownTopicOrPartitionError,
    LeaderNotAvailableError, ConnectionError
)


RETRY_ERROR_TYPES = RETRY_BACKOFF_ERROR_TYPES + RETRY_REFRESH_ERROR_TYPES
