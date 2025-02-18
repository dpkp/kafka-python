from __future__ import absolute_import

import inspect
import sys


class KafkaError(RuntimeError):
    retriable = False
    # whether metadata should be refreshed on error
    invalid_metadata = False

    def __str__(self):
        if not self.args:
            return self.__class__.__name__
        return '{0}: {1}'.format(self.__class__.__name__,
                               super(KafkaError, self).__str__())


class IllegalStateError(KafkaError):
    pass


class IllegalArgumentError(KafkaError):
    pass


class NoBrokersAvailable(KafkaError):
    retriable = True
    invalid_metadata = True


class NodeNotReadyError(KafkaError):
    retriable = True


class KafkaProtocolError(KafkaError):
    retriable = True


class CorrelationIdError(KafkaProtocolError):
    retriable = True


class Cancelled(KafkaError):
    retriable = True


class TooManyInFlightRequests(KafkaError):
    retriable = True


class StaleMetadata(KafkaError):
    retriable = True
    invalid_metadata = True


class MetadataEmptyBrokerList(KafkaError):
    retriable = True


class UnrecognizedBrokerVersion(KafkaError):
    pass


class IncompatibleBrokerVersion(KafkaError):
    pass


class CommitFailedError(KafkaError):
    def __init__(self, *args, **kwargs):
        super(CommitFailedError, self).__init__(
            """Commit cannot be completed since the group has already
            rebalanced and assigned the partitions to another member.
            This means that the time between subsequent calls to poll()
            was longer than the configured max_poll_interval_ms, which
            typically implies that the poll loop is spending too much
            time message processing. You can address this either by
            increasing the rebalance timeout with max_poll_interval_ms,
            or by reducing the maximum size of batches returned in poll()
            with max_poll_records.
            """, *args, **kwargs)


class AuthenticationMethodNotSupported(KafkaError):
    pass


class AuthenticationFailedError(KafkaError):
    retriable = False


class BrokerResponseError(KafkaError):
    errno = None
    message = None
    description = None

    def __str__(self):
        """Add errno to standard KafkaError str"""
        return '[Error {0}] {1}'.format(
            self.errno,
            super(BrokerResponseError, self).__str__())


class NoError(BrokerResponseError):
    errno = 0
    message = 'NO_ERROR'
    description = 'No error--it worked!'


class UnknownError(BrokerResponseError):
    errno = -1
    message = 'UNKNOWN'
    description = 'An unexpected server error.'


class OffsetOutOfRangeError(BrokerResponseError):
    errno = 1
    message = 'OFFSET_OUT_OF_RANGE'
    description = ('The requested offset is outside the range of offsets'
                   ' maintained by the server for the given topic/partition.')


class CorruptRecordException(BrokerResponseError):
    errno = 2
    message = 'CORRUPT_MESSAGE'
    description = ('This message has failed its CRC checksum, exceeds the'
                   ' valid size, or is otherwise corrupt.')

# Backward compatibility
InvalidMessageError = CorruptRecordException


class UnknownTopicOrPartitionError(BrokerResponseError):
    errno = 3
    message = 'UNKNOWN_TOPIC_OR_PARTITION'
    description = ('This request is for a topic or partition that does not'
                   ' exist on this broker.')
    retriable = True
    invalid_metadata = True


class InvalidFetchRequestError(BrokerResponseError):
    errno = 4
    message = 'INVALID_FETCH_SIZE'
    description = 'The message has a negative size.'


class LeaderNotAvailableError(BrokerResponseError):
    errno = 5
    message = 'LEADER_NOT_AVAILABLE'
    description = ('This error is thrown if we are in the middle of a'
                   ' leadership election and there is currently no leader for'
                   ' this partition and hence it is unavailable for writes.')
    retriable = True
    invalid_metadata = True


class NotLeaderForPartitionError(BrokerResponseError):
    errno = 6
    message = 'NOT_LEADER_FOR_PARTITION'
    description = ('This error is thrown if the client attempts to send'
                   ' messages to a replica that is not the leader for some'
                   ' partition. It indicates that the clients metadata is out'
                   ' of date.')
    retriable = True
    invalid_metadata = True


class RequestTimedOutError(BrokerResponseError):
    errno = 7
    message = 'REQUEST_TIMED_OUT'
    description = ('This error is thrown if the request exceeds the'
                   ' user-specified time limit in the request.')
    retriable = True


class BrokerNotAvailableError(BrokerResponseError):
    errno = 8
    message = 'BROKER_NOT_AVAILABLE'
    description = ('This is not a client facing error and is used mostly by'
                   ' tools when a broker is not alive.')


class ReplicaNotAvailableError(BrokerResponseError):
    errno = 9
    message = 'REPLICA_NOT_AVAILABLE'
    description = ('If replica is expected on a broker, but is not (this can be'
                   ' safely ignored).')
    retriable = True
    invalid_metadata = True

class MessageSizeTooLargeError(BrokerResponseError):
    errno = 10
    message = 'MESSAGE_SIZE_TOO_LARGE'
    description = ('The server has a configurable maximum message size to avoid'
                   ' unbounded memory allocation. This error is thrown if the'
                   ' client attempt to produce a message larger than this'
                   ' maximum.')


class StaleControllerEpochError(BrokerResponseError):
    errno = 11
    message = 'STALE_CONTROLLER_EPOCH'
    description = 'Internal error code for broker-to-broker communication.'


class OffsetMetadataTooLargeError(BrokerResponseError):
    errno = 12
    message = 'OFFSET_METADATA_TOO_LARGE'
    description = ('If you specify a string larger than configured maximum for'
                   ' offset metadata.')


class NetworkExceptionError(BrokerResponseError):
    errno = 13
    message = 'NETWORK_EXCEPTION'
    retriable = True
    invalid_metadata = True


class GroupLoadInProgressError(BrokerResponseError):
    errno = 14
    message = 'OFFSETS_LOAD_IN_PROGRESS'
    description = ('The broker returns this error code for an offset fetch'
                   ' request if it is still loading offsets (after a leader'
                   ' change for that offsets topic partition), or in response'
                   ' to group membership requests (such as heartbeats) when'
                   ' group metadata is being loaded by the coordinator.')
    retriable = True


class GroupCoordinatorNotAvailableError(BrokerResponseError):
    errno = 15
    message = 'CONSUMER_COORDINATOR_NOT_AVAILABLE'
    description = ('The broker returns this error code for group coordinator'
                   ' requests, offset commits, and most group management'
                   ' requests if the offsets topic has not yet been created, or'
                   ' if the group coordinator is not active.')
    retriable = True


class NotCoordinatorForGroupError(BrokerResponseError):
    errno = 16
    message = 'NOT_COORDINATOR_FOR_CONSUMER'
    description = ('The broker returns this error code if it receives an offset'
                   ' fetch or commit request for a group that it is not a'
                   ' coordinator for.')
    retriable = True


class InvalidTopicError(BrokerResponseError):
    errno = 17
    message = 'INVALID_TOPIC'
    description = ('For a request which attempts to access an invalid topic'
                   ' (e.g. one which has an illegal name), or if an attempt'
                   ' is made to write to an internal topic (such as the'
                   ' consumer offsets topic).')


class RecordListTooLargeError(BrokerResponseError):
    errno = 18
    message = 'RECORD_LIST_TOO_LARGE'
    description = ('If a message batch in a produce request exceeds the maximum'
                   ' configured segment size.')


class NotEnoughReplicasError(BrokerResponseError):
    errno = 19
    message = 'NOT_ENOUGH_REPLICAS'
    description = ('Returned from a produce request when the number of in-sync'
                   ' replicas is lower than the configured minimum and'
                   ' requiredAcks is -1.')
    retriable = True


class NotEnoughReplicasAfterAppendError(BrokerResponseError):
    errno = 20
    message = 'NOT_ENOUGH_REPLICAS_AFTER_APPEND'
    description = ('Returned from a produce request when the message was'
                   ' written to the log, but with fewer in-sync replicas than'
                   ' required.')
    retriable = True


class InvalidRequiredAcksError(BrokerResponseError):
    errno = 21
    message = 'INVALID_REQUIRED_ACKS'
    description = ('Returned from a produce request if the requested'
                   ' requiredAcks is invalid (anything other than -1, 1, or 0).')


class IllegalGenerationError(BrokerResponseError):
    errno = 22
    message = 'ILLEGAL_GENERATION'
    description = ('Returned from group membership requests (such as heartbeats)'
                   ' when the generation id provided in the request is not the'
                   ' current generation.')


class InconsistentGroupProtocolError(BrokerResponseError):
    errno = 23
    message = 'INCONSISTENT_GROUP_PROTOCOL'
    description = ('Returned in join group when the member provides a protocol'
                   ' type or set of protocols which is not compatible with the'
                   ' current group.')


class InvalidGroupIdError(BrokerResponseError):
    errno = 24
    message = 'INVALID_GROUP_ID'
    description = 'Returned in join group when the groupId is empty or null.'


class UnknownMemberIdError(BrokerResponseError):
    errno = 25
    message = 'UNKNOWN_MEMBER_ID'
    description = ('Returned from group requests (offset commits/fetches,'
                   ' heartbeats, etc) when the memberId is not in the current'
                   ' generation.')


class InvalidSessionTimeoutError(BrokerResponseError):
    errno = 26
    message = 'INVALID_SESSION_TIMEOUT'
    description = ('Return in join group when the requested session timeout is'
                   ' outside of the allowed range on the broker')


class RebalanceInProgressError(BrokerResponseError):
    errno = 27
    message = 'REBALANCE_IN_PROGRESS'
    description = ('Returned in heartbeat requests when the coordinator has'
                   ' begun rebalancing the group. This indicates to the client'
                   ' that it should rejoin the group.')


class InvalidCommitOffsetSizeError(BrokerResponseError):
    errno = 28
    message = 'INVALID_COMMIT_OFFSET_SIZE'
    description = ('This error indicates that an offset commit was rejected'
                   ' because of oversize metadata.')


class TopicAuthorizationFailedError(BrokerResponseError):
    errno = 29
    message = 'TOPIC_AUTHORIZATION_FAILED'
    description = ('Returned by the broker when the client is not authorized to'
                   ' access the requested topic.')


class GroupAuthorizationFailedError(BrokerResponseError):
    errno = 30
    message = 'GROUP_AUTHORIZATION_FAILED'
    description = ('Returned by the broker when the client is not authorized to'
                   ' access a particular groupId.')


class ClusterAuthorizationFailedError(BrokerResponseError):
    errno = 31
    message = 'CLUSTER_AUTHORIZATION_FAILED'
    description = ('Returned by the broker when the client is not authorized to'
                   ' use an inter-broker or administrative API.')


class InvalidTimestampError(BrokerResponseError):
    errno = 32
    message = 'INVALID_TIMESTAMP'
    description = 'The timestamp of the message is out of acceptable range.'


class UnsupportedSaslMechanismError(BrokerResponseError):
    errno = 33
    message = 'UNSUPPORTED_SASL_MECHANISM'
    description = 'The broker does not support the requested SASL mechanism.'


class IllegalSaslStateError(BrokerResponseError):
    errno = 34
    message = 'ILLEGAL_SASL_STATE'
    description = 'Request is not valid given the current SASL state.'


class UnsupportedVersionError(BrokerResponseError):
    errno = 35
    message = 'UNSUPPORTED_VERSION'
    description = 'The version of API is not supported.'


class TopicAlreadyExistsError(BrokerResponseError):
    errno = 36
    message = 'TOPIC_ALREADY_EXISTS'
    description = 'Topic with this name already exists.'


class InvalidPartitionsError(BrokerResponseError):
    errno = 37
    message = 'INVALID_PARTITIONS'
    description = 'Number of partitions is invalid.'


class InvalidReplicationFactorError(BrokerResponseError):
    errno = 38
    message = 'INVALID_REPLICATION_FACTOR'
    description = 'Replication-factor is invalid.'


class InvalidReplicationAssignmentError(BrokerResponseError):
    errno = 39
    message = 'INVALID_REPLICATION_ASSIGNMENT'
    description = 'Replication assignment is invalid.'


class InvalidConfigurationError(BrokerResponseError):
    errno = 40
    message = 'INVALID_CONFIG'
    description = 'Configuration is invalid.'


class NotControllerError(BrokerResponseError):
    errno = 41
    message = 'NOT_CONTROLLER'
    description = 'This is not the correct controller for this cluster.'
    retriable = True


class InvalidRequestError(BrokerResponseError):
    errno = 42
    message = 'INVALID_REQUEST'
    description = ('This most likely occurs because of a request being'
                   ' malformed by the client library or the message was'
                   ' sent to an incompatible broker. See the broker logs'
                   ' for more details.')


class UnsupportedForMessageFormatError(BrokerResponseError):
    errno = 43
    message = 'UNSUPPORTED_FOR_MESSAGE_FORMAT'
    description = ('The message format version on the broker does not'
                   ' support this request.')


class PolicyViolationError(BrokerResponseError):
    errno = 44
    message = 'POLICY_VIOLATION'
    description = 'Request parameters do not satisfy the configured policy.'
    retriable = False


class OutOfOrderSequenceNumberError(BrokerResponseError):
    errno = 45
    message = 'OUT_OF_ORDER_SEQUENCE_NUMBER'
    description = 'The broker received an out of order sequence number.'
    retriable = False


class DuplicateSequenceNumberError(BrokerResponseError):
    errno = 46
    message = 'DUPLICATE_SEQUENCE_NUMBER'
    description = 'The broker received a duplicate sequence number.'
    retriable = False


class InvalidProducerEpochError(BrokerResponseError):
    errno = 47
    message = 'INVALID_PRODUCER_EPOCH'
    description = 'Producer attempted to produce with an old epoch.'
    retriable = False


class InvalidTxnStateError(BrokerResponseError):
    errno = 48
    message = 'INVALID_TXN_STATE'
    description = 'The producer attempted a transactional operation in an invalid state.'
    retriable = False


class InvalidProducerIdMappingError(BrokerResponseError):
    errno = 49
    message = 'INVALID_PRODUCER_ID_MAPPING'
    description = 'The producer attempted to use a producer id which is not currently assigned to its transactional id.'
    retriable = False


class InvalidTransactionTimeoutError(BrokerResponseError):
    errno = 50
    message = 'INVALID_TRANSACTION_TIMEOUT'
    description = 'The transaction timeout is larger than the maximum value allowed by the broker (as configured by transaction.max.timeout.ms).'
    retriable = False


class ConcurrentTransactionsError(BrokerResponseError):
    errno = 51
    message = 'CONCURRENT_TRANSACTIONS'
    description = 'The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing.'
    retriable = True


class TransactionCoordinatorFencedError(BrokerResponseError):
    errno = 52
    message = 'TRANSACTION_COORDINATOR_FENCED'
    description = 'Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer.'
    retriable = False


class TransactionalIdAuthorizationFailedError(BrokerResponseError):
    errno = 53
    message = 'TRANSACTIONAL_ID_AUTHORIZATION_FAILED'
    description = 'Transactional Id authorization failed.'
    retriable = False


class SecurityDisabledError(BrokerResponseError):
    errno = 54
    message = 'SECURITY_DISABLED'
    description = 'Security features are disabled.'
    retriable = False


class OperationNotAttemptedError(BrokerResponseError):
    errno = 55
    message = 'OPERATION_NOT_ATTEMPTED'
    description = 'The broker did not attempt to execute this operation. This may happen for batched RPCs where some operations in the batch failed, causing the broker to respond without trying the rest.'
    retriable = False


class KafkaStorageError(BrokerResponseError):
    errno = 56
    message = 'KAFKA_STORAGE_ERROR'
    description = 'Disk error when trying to access log file on the disk.'
    retriable = True
    invalid_metadata = True


class LogDirNotFoundError(BrokerResponseError):
    errno = 57
    message = 'LOG_DIR_NOT_FOUND'
    description = 'The user-specified log directory is not found in the broker config.'
    retriable = False


class SaslAuthenticationFailedError(BrokerResponseError):
    errno = 58
    message = 'SASL_AUTHENTICATION_FAILED'
    description = 'SASL Authentication failed.'
    retriable = False


class UnknownProducerIdError(BrokerResponseError):
    errno = 59
    message = 'UNKNOWN_PRODUCER_ID'
    description = 'This exception is raised by the broker if it could not locate the producer metadata associated with the producerId in question. This could happen if, for instance, the producer\'s records were deleted because their retention time had elapsed. Once the last records of the producerId are removed, the producer\'s metadata is removed from the broker, and future appends by the producer will return this exception.'
    retriable = False


class ReassignmentInProgressError(BrokerResponseError):
    errno = 60
    message = 'REASSIGNMENT_IN_PROGRESS'
    description = 'A partition reassignment is in progress.'
    retriable = False


class DelegationTokenAuthDisabledError(BrokerResponseError):
    errno = 61
    message = 'DELEGATION_TOKEN_AUTH_DISABLED'
    description = 'Delegation Token feature is not enabled.'
    retriable = False


class DelegationTokenNotFoundError(BrokerResponseError):
    errno = 62
    message = 'DELEGATION_TOKEN_NOT_FOUND'
    description = 'Delegation Token is not found on server.'
    retriable = False


class DelegationTokenOwnerMismatchError(BrokerResponseError):
    errno = 63
    message = 'DELEGATION_TOKEN_OWNER_MISMATCH'
    description = 'Specified Principal is not valid Owner/Renewer.'
    retriable = False


class DelegationTokenRequestNotAllowedError(BrokerResponseError):
    errno = 64
    message = 'DELEGATION_TOKEN_REQUEST_NOT_ALLOWED'
    description = 'Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels.'
    retriable = False


class DelegationTokenAuthorizationFailedError(BrokerResponseError):
    errno = 65
    message = 'DELEGATION_TOKEN_AUTHORIZATION_FAILED'
    description = 'Delegation Token authorization failed.'
    retriable = False


class DelegationTokenExpiredError(BrokerResponseError):
    errno = 66
    message = 'DELEGATION_TOKEN_EXPIRED'
    description = 'Delegation Token is expired.'
    retriable = False


class InvalidPrincipalTypeError(BrokerResponseError):
    errno = 67
    message = 'INVALID_PRINCIPAL_TYPE'
    description = 'Supplied principalType is not supported.'
    retriable = False


class NonEmptyGroupError(BrokerResponseError):
    errno = 68
    message = 'NON_EMPTY_GROUP'
    description = 'The group is not empty.'
    retriable = False


class GroupIdNotFoundError(BrokerResponseError):
    errno = 69
    message = 'GROUP_ID_NOT_FOUND'
    description = 'The group id does not exist.'
    retriable = False


class FetchSessionIdNotFoundError(BrokerResponseError):
    errno = 70
    message = 'FETCH_SESSION_ID_NOT_FOUND'
    description = 'The fetch session ID was not found.'
    retriable = True


class InvalidFetchSessionEpochError(BrokerResponseError):
    errno = 71
    message = 'INVALID_FETCH_SESSION_EPOCH'
    description = 'The fetch session epoch is invalid.'
    retriable = True


class ListenerNotFoundError(BrokerResponseError):
    errno = 72
    message = 'LISTENER_NOT_FOUND'
    description = 'There is no listener on the leader broker that matches the listener on which metadata request was processed.'
    retriable = True
    invalid_metadata = True


class TopicDeletionDisabledError(BrokerResponseError):
    errno = 73
    message = 'TOPIC_DELETION_DISABLED'
    description = 'Topic deletion is disabled.'
    retriable = False


class FencedLeaderEpochError(BrokerResponseError):
    errno = 74
    message = 'FENCED_LEADER_EPOCH'
    description = 'The leader epoch in the request is older than the epoch on the broker.'
    retriable = True
    invalid_metadata = True


class UnknownLeaderEpochError(BrokerResponseError):
    errno = 75
    message = 'UNKNOWN_LEADER_EPOCH'
    description = 'The leader epoch in the request is newer than the epoch on the broker.'
    retriable = True


class UnsupportedCompressionTypeError(BrokerResponseError):
    errno = 76
    message = 'UNSUPPORTED_COMPRESSION_TYPE'
    description = 'The requesting client does not support the compression type of given partition.'
    retriable = False


class StaleBrokerEpochError(BrokerResponseError):
    errno = 77
    message = 'STALE_BROKER_EPOCH'
    description = 'Broker epoch has changed.'
    retriable = False


class OffsetNotAvailableError(BrokerResponseError):
    errno = 78
    message = 'OFFSET_NOT_AVAILABLE'
    description = 'The leader high watermark has not caught up from a recent leader election so the offsets cannot be guaranteed to be monotonically increasing.'
    retriable = True


class MemberIdRequiredError(BrokerResponseError):
    errno = 79
    message = 'MEMBER_ID_REQUIRED'
    description = 'The group member needs to have a valid member id before actually entering a consumer group.'
    retriable = False


class PreferredLeaderNotAvailableError(BrokerResponseError):
    errno = 80
    message = 'PREFERRED_LEADER_NOT_AVAILABLE'
    description = 'The preferred leader was not available.'
    retriable = True
    invalid_metadata = True


class GroupMaxSizeReachedError(BrokerResponseError):
    errno = 81
    message = 'GROUP_MAX_SIZE_REACHED'
    description = 'The consumer group has reached its max size.'
    retriable = False


class FencedInstanceIdError(BrokerResponseError):
    errno = 82
    message = 'FENCED_INSTANCE_ID'
    description = 'The broker rejected this static consumer since another consumer with the same group.instance.id has registered with a different member.id.'
    retriable = False


class EligibleLeadersNotAvailableError(BrokerResponseError):
    errno = 83
    message = 'ELIGIBLE_LEADERS_NOT_AVAILABLE'
    description = 'Eligible topic partition leaders are not available.'
    retriable = True
    invalid_metadata = True


class ElectionNotNeededError(BrokerResponseError):
    errno = 84
    message = 'ELECTION_NOT_NEEDED'
    description = 'Leader election not needed for topic partition.'
    retriable = True
    invalid_metadata = True


class NoReassignmentInProgressError(BrokerResponseError):
    errno = 85
    message = 'NO_REASSIGNMENT_IN_PROGRESS'
    description = 'No partition reassignment is in progress.'
    retriable = False


class GroupSubscribedToTopicError(BrokerResponseError):
    errno = 86
    message = 'GROUP_SUBSCRIBED_TO_TOPIC'
    description = 'Deleting offsets of a topic is forbidden while the consumer group is actively subscribed to it.'
    retriable = False


class InvalidRecordError(BrokerResponseError):
    errno = 87
    message = 'INVALID_RECORD'
    description = 'This record has failed the validation on broker and hence will be rejected.'
    retriable = False


class UnstableOffsetCommitError(BrokerResponseError):
    errno = 88
    message = 'UNSTABLE_OFFSET_COMMIT'
    description = 'There are unstable offsets that need to be cleared.'
    retriable = True


class ThrottlingQuotaExceededError(BrokerResponseError):
    errno = 89
    message = 'THROTTLING_QUOTA_EXCEEDED'
    description = 'The throttling quota has been exceeded.'
    retriable = True


class ProducerFencedError(BrokerResponseError):
    errno = 90
    message = 'PRODUCER_FENCED'
    description = 'There is a newer producer with the same transactionalId which fences the current one.'
    retriable = False


class ResourceNotFoundError(BrokerResponseError):
    errno = 91
    message = 'RESOURCE_NOT_FOUND'
    description = 'A request illegally referred to a resource that does not exist.'
    retriable = False


class DuplicateResourceError(BrokerResponseError):
    errno = 92
    message = 'DUPLICATE_RESOURCE'
    description = 'A request illegally referred to the same resource twice.'
    retriable = False


class UnacceptableCredentialError(BrokerResponseError):
    errno = 93
    message = 'UNACCEPTABLE_CREDENTIAL'
    description = 'Requested credential would not meet criteria for acceptability.'
    retriable = False


class InconsistentVoterSetError(BrokerResponseError):
    errno = 94
    message = 'INCONSISTENT_VOTER_SET'
    description = 'Indicates that the either the sender or recipient of a voter-only request is not one of the expected voters.'
    retriable = False


class InvalidUpdateVersionError(BrokerResponseError):
    errno = 95
    message = 'INVALID_UPDATE_VERSION'
    description = 'The given update version was invalid.'
    retriable = False


class FeatureUpdateFailedError(BrokerResponseError):
    errno = 96
    message = 'FEATURE_UPDATE_FAILED'
    description = 'Unable to update finalized features due to an unexpected server error.'
    retriable = False


class PrincipalDeserializationFailureError(BrokerResponseError):
    errno = 97
    message = 'PRINCIPAL_DESERIALIZATION_FAILURE'
    description = 'Request principal deserialization failed during forwarding. This indicates an internal error on the broker cluster security setup.'
    retriable = False


class SnapshotNotFoundError(BrokerResponseError):
    errno = 98
    message = 'SNAPSHOT_NOT_FOUND'
    description = 'Requested snapshot was not found.'
    retriable = False


class PositionOutOfRangeError(BrokerResponseError):
    errno = 99
    message = 'POSITION_OUT_OF_RANGE'
    description = 'Requested position is not greater than or equal to zero, and less than the size of the snapshot.'
    retriable = False


class UnknownTopicIdError(BrokerResponseError):
    errno = 100
    message = 'UNKNOWN_TOPIC_ID'
    description = 'This server does not host this topic ID.'
    retriable = True
    invalid_metadata = True


class DuplicateBrokerRegistrationError(BrokerResponseError):
    errno = 101
    message = 'DUPLICATE_BROKER_REGISTRATION'
    description = 'This broker ID is already in use.'
    retriable = False


class BrokerIdNotRegisteredError(BrokerResponseError):
    errno = 102
    message = 'BROKER_ID_NOT_REGISTERED'
    description = 'The given broker ID was not registered.'
    retriable = False


class InconsistentTopicIdError(BrokerResponseError):
    errno = 103
    message = 'INCONSISTENT_TOPIC_ID'
    description = 'The log\'s topic ID did not match the topic ID in the request.'
    retriable = True
    invalid_metadata = True


class InconsistentClusterIdError(BrokerResponseError):
    errno = 104
    message = 'INCONSISTENT_CLUSTER_ID'
    description = 'The clusterId in the request does not match that found on the server.'
    retriable = False


class TransactionalIdNotFoundError(BrokerResponseError):
    errno = 105
    message = 'TRANSACTIONAL_ID_NOT_FOUND'
    description = 'The transactionalId could not be found.'
    retriable = False


class FetchSessionTopicIdError(BrokerResponseError):
    errno = 106
    message = 'FETCH_SESSION_TOPIC_ID_ERROR'
    description = 'The fetch session encountered inconsistent topic ID usage.'
    retriable = True


class IneligibleReplicaError(BrokerResponseError):
    errno = 107
    message = 'INELIGIBLE_REPLICA'
    description = 'The new ISR contains at least one ineligible replica.'
    retriable = False


class NewLeaderElectedError(BrokerResponseError):
    errno = 108
    message = 'NEW_LEADER_ELECTED'
    description = 'The AlterPartition request successfully updated the partition state but the leader has changed.'
    retriable = False


class OffsetMovedToTieredStorageError(BrokerResponseError):
    errno = 109
    message = 'OFFSET_MOVED_TO_TIERED_STORAGE'
    description = 'The requested offset is moved to tiered storage.'
    retriable = False


class FencedMemberEpochError(BrokerResponseError):
    errno = 110
    message = 'FENCED_MEMBER_EPOCH'
    description = 'The member epoch is fenced by the group coordinator. The member must abandon all its partitions and rejoin.'
    retriable = False


class UnreleasedInstanceIdError(BrokerResponseError):
    errno = 111
    message = 'UNRELEASED_INSTANCE_ID'
    description = 'The instance ID is still used by another member in the consumer group. That member must leave first.'
    retriable = False


class UnsupportedAssignorError(BrokerResponseError):
    errno = 112
    message = 'UNSUPPORTED_ASSIGNOR'
    description = 'The assignor or its version range is not supported by the consumer group.'
    retriable = False


class StaleMemberEpochError(BrokerResponseError):
    errno = 113
    message = 'STALE_MEMBER_EPOCH'
    description = 'The member epoch is stale. The member must retry after receiving its updated member epoch via the ConsumerGroupHeartbeat API.'
    retriable = False


class MismatchedEndpointTypeError(BrokerResponseError):
    errno = 114
    message = 'MISMATCHED_ENDPOINT_TYPE'
    description = 'The request was sent to an endpoint of the wrong type.'
    retriable = False


class UnsupportedEndpointTypeError(BrokerResponseError):
    errno = 115
    message = 'UNSUPPORTED_ENDPOINT_TYPE'
    description = 'This endpoint type is not supported yet.'
    retriable = False


class UnknownControllerIdError(BrokerResponseError):
    errno = 116
    message = 'UNKNOWN_CONTROLLER_ID'
    description = 'This controller ID is not known.'
    retriable = False


class UnknownSubscriptionIdError(BrokerResponseError):
    errno = 117
    message = 'UNKNOWN_SUBSCRIPTION_ID'
    description = 'Client sent a push telemetry request with an invalid or outdated subscription ID.'
    retriable = False


class TelemetryTooLargeError(BrokerResponseError):
    errno = 118
    message = 'TELEMETRY_TOO_LARGE'
    description = 'Client sent a push telemetry request larger than the maximum size the broker will accept.'
    retriable = False


class InvalidRegistrationError(BrokerResponseError):
    errno = 119
    message = 'INVALID_REGISTRATION'
    description = 'The controller has considered the broker registration to be invalid.'
    retriable = False


class TransactionAbortableError(BrokerResponseError):
    errno = 120
    message = 'TRANSACTION_ABORTABLE'
    description = 'The server encountered an error with the transaction. The client can abort the transaction to continue using this transactional ID.'
    retriable = False


class InvalidRecordStateError(BrokerResponseError):
    errno = 121
    message = 'INVALID_RECORD_STATE'
    description = 'The record state is invalid. The acknowledgement of delivery could not be completed.'
    retriable = False


class ShareSessionNotFoundError(BrokerResponseError):
    errno = 122
    message = 'SHARE_SESSION_NOT_FOUND'
    description = 'The share session was not found.'
    retriable = True


class InvalidShareSessionEpochError(BrokerResponseError):
    errno = 123
    message = 'INVALID_SHARE_SESSION_EPOCH'
    description = 'The share session epoch is invalid.'
    retriable = True


class FencedStateEpochError(BrokerResponseError):
    errno = 124
    message = 'FENCED_STATE_EPOCH'
    description = 'The share coordinator rejected the request because the share-group state epoch did not match.'
    retriable = False


class InvalidVoterKeyError(BrokerResponseError):
    errno = 125
    message = 'INVALID_VOTER_KEY'
    description = 'The voter key doesn\'t match the receiving replica\'s key.'
    retriable = False


class DuplicateVoterError(BrokerResponseError):
    errno = 126
    message = 'DUPLICATE_VOTER'
    description = 'The voter is already part of the set of voters.'
    retriable = False


class VoterNotFoundError(BrokerResponseError):
    errno = 127
    message = 'VOTER_NOT_FOUND'
    description = 'The voter is not part of the set of voters.'
    retriable = False


class KafkaUnavailableError(KafkaError):
    pass


class KafkaTimeoutError(KafkaError):
    pass


class FailedPayloadsError(KafkaError):
    def __init__(self, payload, *args):
        super(FailedPayloadsError, self).__init__(*args)
        self.payload = payload


class KafkaConnectionError(KafkaError):
    retriable = True
    invalid_metadata = True


class ProtocolError(KafkaError):
    pass


class UnsupportedCodecError(KafkaError):
    pass


class KafkaConfigurationError(KafkaError):
    pass


class QuotaViolationError(KafkaError):
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


def for_code(error_code):
    if error_code in kafka_errors:
        return kafka_errors[error_code]
    else:
        # The broker error code was not found in our list. This can happen when connecting
        # to a newer broker (with new error codes), or simply because our error list is
        # not complete.
        #
        # To avoid dropping the error code, create a dynamic error class w/ errno override.
        return type('UnrecognizedBrokerError', (UnknownError,), {'errno': error_code})
