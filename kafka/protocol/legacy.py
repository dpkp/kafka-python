import logging
import struct

import six

from six.moves import xrange

from kafka.codec import (
    gzip_encode, gzip_decode, snappy_encode, snappy_decode
)
from kafka.common import (
    Message, OffsetAndMessage, TopicAndPartition,
    BrokerMetadata, TopicMetadata, PartitionMetadata,
    MetadataResponse, ProduceResponse, FetchResponse,
    OffsetResponse, OffsetCommitResponse, OffsetFetchResponse,
    ProtocolError, BufferUnderflowError, ChecksumError,
    ConsumerFetchSizeTooSmall, UnsupportedCodecError
)
from kafka.util import (
    crc32, read_short_string, read_int_string, relative_unpack,
    write_short_string, write_int_string, group_by_topic_and_partition
)


log = logging.getLogger(__name__)

ATTRIBUTE_CODEC_MASK = 0x03
CODEC_NONE = 0x00
CODEC_GZIP = 0x01
CODEC_SNAPPY = 0x02
ALL_CODECS = (CODEC_NONE, CODEC_GZIP, CODEC_SNAPPY)


class KafkaProtocol(object):
    """
    Class to encapsulate all of the protocol encoding/decoding.
    This class does not have any state associated with it, it is purely
    for organization.
    """
    PRODUCE_KEY = 0
    FETCH_KEY = 1
    OFFSET_KEY = 2
    METADATA_KEY = 3
    OFFSET_COMMIT_KEY = 8
    OFFSET_FETCH_KEY = 9

    ###################
    #   Private API   #
    ###################

    @classmethod
    def _encode_message_header(cls, client_id, correlation_id, request_key):
        """
        Encode the common request envelope
        """
        return struct.pack('>hhih%ds' % len(client_id),
                           request_key,          # ApiKey
                           0,                    # ApiVersion
                           correlation_id,       # CorrelationId
                           len(client_id),       # ClientId size
                           client_id)            # ClientId

    @classmethod
    def _encode_message_set(cls, messages):
        """
        Encode a MessageSet. Unlike other arrays in the protocol,
        MessageSets are not length-prefixed

        Format
        ======
        MessageSet => [Offset MessageSize Message]
          Offset => int64
          MessageSize => int32
        """
        message_set = []
        for message in messages:
            encoded_message = KafkaProtocol._encode_message(message)
            message_set.append(struct.pack('>qi%ds' % len(encoded_message), 0,
                                           len(encoded_message),
                                           encoded_message))
        return b''.join(message_set)

    @classmethod
    def _encode_message(cls, message):
        """
        Encode a single message.

        The magic number of a message is a format version number.
        The only supported magic number right now is zero

        Format
        ======
        Message => Crc MagicByte Attributes Key Value
          Crc => int32
          MagicByte => int8
          Attributes => int8
          Key => bytes
          Value => bytes
        """
        if message.magic == 0:
            msg = b''.join([
                struct.pack('>BB', message.magic, message.attributes),
                write_int_string(message.key),
                write_int_string(message.value)
            ])
            crc = crc32(msg)
            msg = struct.pack('>I%ds' % len(msg), crc, msg)
        else:
            raise ProtocolError("Unexpected magic number: %d" % message.magic)
        return msg

    @classmethod
    def _decode_message_set_iter(cls, data):
        """
        Iteratively decode a MessageSet

        Reads repeated elements of (offset, message), calling decode_message
        to decode a single message. Since compressed messages contain futher
        MessageSets, these two methods have been decoupled so that they may
        recurse easily.
        """
        cur = 0
        read_message = False
        while cur < len(data):
            try:
                ((offset, ), cur) = relative_unpack('>q', data, cur)
                (msg, cur) = read_int_string(data, cur)
                for (offset, message) in KafkaProtocol._decode_message(msg, offset):
                    read_message = True
                    yield OffsetAndMessage(offset, message)
            except BufferUnderflowError:
                # NOTE: Not sure this is correct error handling:
                # Is it possible to get a BUE if the message set is somewhere
                # in the middle of the fetch response? If so, we probably have
                # an issue that's not fetch size too small.
                # Aren't we ignoring errors if we fail to unpack data by
                # raising StopIteration()?
                # If _decode_message() raises a ChecksumError, couldn't that
                # also be due to the fetch size being too small?
                if read_message is False:
                    # If we get a partial read of a message, but haven't
                    # yielded anything there's a problem
                    raise ConsumerFetchSizeTooSmall()
                else:
                    raise StopIteration()

    @classmethod
    def _decode_message(cls, data, offset):
        """
        Decode a single Message

        The only caller of this method is decode_message_set_iter.
        They are decoupled to support nested messages (compressed MessageSets).
        The offset is actually read from decode_message_set_iter (it is part
        of the MessageSet payload).
        """
        ((crc, magic, att), cur) = relative_unpack('>IBB', data, 0)
        if crc != crc32(data[4:]):
            raise ChecksumError("Message checksum failed")

        (key, cur) = read_int_string(data, cur)
        (value, cur) = read_int_string(data, cur)

        codec = att & ATTRIBUTE_CODEC_MASK

        if codec == CODEC_NONE:
            yield (offset, Message(magic, att, key, value))

        elif codec == CODEC_GZIP:
            gz = gzip_decode(value)
            for (offset, msg) in KafkaProtocol._decode_message_set_iter(gz):
                yield (offset, msg)

        elif codec == CODEC_SNAPPY:
            snp = snappy_decode(value)
            for (offset, msg) in KafkaProtocol._decode_message_set_iter(snp):
                yield (offset, msg)

    ##################
    #   Public API   #
    ##################

    @classmethod
    def encode_produce_request(cls, client_id, correlation_id,
                               payloads=None, acks=1, timeout=1000):
        """
        Encode some ProduceRequest structs

        Arguments:
            client_id: string
            correlation_id: int
            payloads: list of ProduceRequest
            acks: How "acky" you want the request to be
                0: immediate response
                1: written to disk by the leader
                2+: waits for this many number of replicas to sync
                -1: waits for all replicas to be in sync
            timeout: Maximum time the server will wait for acks from replicas.
                This is _not_ a socket timeout

        """
        payloads = [] if payloads is None else payloads
        grouped_payloads = group_by_topic_and_partition(payloads)

        message = []
        message.append(cls._encode_message_header(client_id, correlation_id,
                                                  KafkaProtocol.PRODUCE_KEY))

        message.append(struct.pack('>hii', acks, timeout,
                                   len(grouped_payloads)))

        for topic, topic_payloads in grouped_payloads.items():
            message.append(struct.pack('>h%dsi' % len(topic), len(topic), topic,
                                       len(topic_payloads)))

            for partition, payload in topic_payloads.items():
                msg_set = KafkaProtocol._encode_message_set(payload.messages)
                message.append(struct.pack('>ii%ds' % len(msg_set), partition,
                                           len(msg_set), msg_set))

        msg = b''.join(message)
        return struct.pack('>i%ds' % len(msg), len(msg), msg)

    @classmethod
    def decode_produce_response(cls, data):
        """
        Decode bytes to a ProduceResponse

        Arguments:
            data: bytes to decode

        """
        ((correlation_id, num_topics), cur) = relative_unpack('>ii', data, 0)

        for _ in range(num_topics):
            ((strlen,), cur) = relative_unpack('>h', data, cur)
            topic = data[cur:cur + strlen]
            cur += strlen
            ((num_partitions,), cur) = relative_unpack('>i', data, cur)
            for _ in range(num_partitions):
                ((partition, error, offset), cur) = relative_unpack('>ihq',
                                                                    data, cur)

                yield ProduceResponse(topic, partition, error, offset)

    @classmethod
    def encode_fetch_request(cls, client_id, correlation_id, payloads=None,
                             max_wait_time=100, min_bytes=4096):
        """
        Encodes some FetchRequest structs

        Arguments:
            client_id: string
            correlation_id: int
            payloads: list of FetchRequest
            max_wait_time: int, how long to block waiting on min_bytes of data
            min_bytes: int, the minimum number of bytes to accumulate before
                       returning the response
        """

        payloads = [] if payloads is None else payloads
        grouped_payloads = group_by_topic_and_partition(payloads)

        message = []
        message.append(cls._encode_message_header(client_id, correlation_id,
                                                  KafkaProtocol.FETCH_KEY))

        # -1 is the replica id
        message.append(struct.pack('>iiii', -1, max_wait_time, min_bytes,
                                   len(grouped_payloads)))

        for topic, topic_payloads in grouped_payloads.items():
            message.append(write_short_string(topic))
            message.append(struct.pack('>i', len(topic_payloads)))
            for partition, payload in topic_payloads.items():
                message.append(struct.pack('>iqi', partition, payload.offset,
                                           payload.max_bytes))

        msg = b''.join(message)
        return struct.pack('>i%ds' % len(msg), len(msg), msg)

    @classmethod
    def decode_fetch_response(cls, data):
        """
        Decode bytes to a FetchResponse

        Arguments:
            data: bytes to decode
        """
        ((correlation_id, num_topics), cur) = relative_unpack('>ii', data, 0)

        for _ in range(num_topics):
            (topic, cur) = read_short_string(data, cur)
            ((num_partitions,), cur) = relative_unpack('>i', data, cur)

            for j in range(num_partitions):
                ((partition, error, highwater_mark_offset), cur) = \
                    relative_unpack('>ihq', data, cur)

                (message_set, cur) = read_int_string(data, cur)

                yield FetchResponse(
                    topic, partition, error,
                    highwater_mark_offset,
                    KafkaProtocol._decode_message_set_iter(message_set))

    @classmethod
    def encode_offset_request(cls, client_id, correlation_id, payloads=None):
        payloads = [] if payloads is None else payloads
        grouped_payloads = group_by_topic_and_partition(payloads)

        message = []
        message.append(cls._encode_message_header(client_id, correlation_id,
                                                  KafkaProtocol.OFFSET_KEY))

        # -1 is the replica id
        message.append(struct.pack('>ii', -1, len(grouped_payloads)))

        for topic, topic_payloads in grouped_payloads.items():
            message.append(write_short_string(topic))
            message.append(struct.pack('>i', len(topic_payloads)))

            for partition, payload in topic_payloads.items():
                message.append(struct.pack('>iqi', partition, payload.time,
                                           payload.max_offsets))

        msg = b''.join(message)
        return struct.pack('>i%ds' % len(msg), len(msg), msg)

    @classmethod
    def decode_offset_response(cls, data):
        """
        Decode bytes to an OffsetResponse

        Arguments:
            data: bytes to decode
        """
        ((correlation_id, num_topics), cur) = relative_unpack('>ii', data, 0)

        for _ in range(num_topics):
            (topic, cur) = read_short_string(data, cur)
            ((num_partitions,), cur) = relative_unpack('>i', data, cur)

            for _ in range(num_partitions):
                ((partition, error, num_offsets,), cur) = \
                    relative_unpack('>ihi', data, cur)

                offsets = []
                for k in range(num_offsets):
                    ((offset,), cur) = relative_unpack('>q', data, cur)
                    offsets.append(offset)

                yield OffsetResponse(topic, partition, error, tuple(offsets))

    @classmethod
    def encode_metadata_request(cls, client_id, correlation_id, topics=None,
                                payloads=None):
        """
        Encode a MetadataRequest

        Arguments:
            client_id: string
            correlation_id: int
            topics: list of strings
        """
        if payloads is None:
            topics = [] if topics is None else topics
        else:
            topics = payloads

        message = []
        message.append(cls._encode_message_header(client_id, correlation_id,
                                                  KafkaProtocol.METADATA_KEY))

        message.append(struct.pack('>i', len(topics)))

        for topic in topics:
            message.append(struct.pack('>h%ds' % len(topic), len(topic), topic))

        msg = b''.join(message)
        return write_int_string(msg)

    @classmethod
    def decode_metadata_response(cls, data):
        """
        Decode bytes to a MetadataResponse

        Arguments:
            data: bytes to decode
        """
        ((correlation_id, numbrokers), cur) = relative_unpack('>ii', data, 0)

        # Broker info
        brokers = []
        for _ in range(numbrokers):
            ((nodeId, ), cur) = relative_unpack('>i', data, cur)
            (host, cur) = read_short_string(data, cur)
            ((port,), cur) = relative_unpack('>i', data, cur)
            brokers.append(BrokerMetadata(nodeId, host, port))

        # Topic info
        ((num_topics,), cur) = relative_unpack('>i', data, cur)
        topic_metadata = []

        for _ in range(num_topics):
            ((topic_error,), cur) = relative_unpack('>h', data, cur)
            (topic_name, cur) = read_short_string(data, cur)
            ((num_partitions,), cur) = relative_unpack('>i', data, cur)
            partition_metadata = []

            for _ in range(num_partitions):
                ((partition_error_code, partition, leader, numReplicas), cur) = \
                    relative_unpack('>hiii', data, cur)

                (replicas, cur) = relative_unpack(
                    '>%di' % numReplicas, data, cur)

                ((num_isr,), cur) = relative_unpack('>i', data, cur)
                (isr, cur) = relative_unpack('>%di' % num_isr, data, cur)

                partition_metadata.append(
                    PartitionMetadata(topic_name, partition, leader,
                                      replicas, isr, partition_error_code)
                )

            topic_metadata.append(
                TopicMetadata(topic_name, topic_error, partition_metadata)
            )

        return MetadataResponse(brokers, topic_metadata)

    @classmethod
    def encode_offset_commit_request(cls, client_id, correlation_id,
                                     group, payloads):
        """
        Encode some OffsetCommitRequest structs

        Arguments:
            client_id: string
            correlation_id: int
            group: string, the consumer group you are committing offsets for
            payloads: list of OffsetCommitRequest
        """
        grouped_payloads = group_by_topic_and_partition(payloads)

        message = []
        message.append(cls._encode_message_header(client_id, correlation_id,
                                                  KafkaProtocol.OFFSET_COMMIT_KEY))
        message.append(write_short_string(group))
        message.append(struct.pack('>i', len(grouped_payloads)))

        for topic, topic_payloads in grouped_payloads.items():
            message.append(write_short_string(topic))
            message.append(struct.pack('>i', len(topic_payloads)))

            for partition, payload in topic_payloads.items():
                message.append(struct.pack('>iq', partition, payload.offset))
                message.append(write_short_string(payload.metadata))

        msg = b''.join(message)
        return struct.pack('>i%ds' % len(msg), len(msg), msg)

    @classmethod
    def decode_offset_commit_response(cls, data):
        """
        Decode bytes to an OffsetCommitResponse

        Arguments:
            data: bytes to decode
        """
        ((correlation_id,), cur) = relative_unpack('>i', data, 0)
        ((num_topics,), cur) = relative_unpack('>i', data, cur)

        for _ in xrange(num_topics):
            (topic, cur) = read_short_string(data, cur)
            ((num_partitions,), cur) = relative_unpack('>i', data, cur)

            for _ in xrange(num_partitions):
                ((partition, error), cur) = relative_unpack('>ih', data, cur)
                yield OffsetCommitResponse(topic, partition, error)

    @classmethod
    def encode_offset_fetch_request(cls, client_id, correlation_id,
                                    group, payloads):
        """
        Encode some OffsetFetchRequest structs

        Arguments:
            client_id: string
            correlation_id: int
            group: string, the consumer group you are fetching offsets for
            payloads: list of OffsetFetchRequest
        """
        grouped_payloads = group_by_topic_and_partition(payloads)

        message = []
        message.append(cls._encode_message_header(client_id, correlation_id,
                                                  KafkaProtocol.OFFSET_FETCH_KEY))

        message.append(write_short_string(group))
        message.append(struct.pack('>i', len(grouped_payloads)))

        for topic, topic_payloads in grouped_payloads.items():
            message.append(write_short_string(topic))
            message.append(struct.pack('>i', len(topic_payloads)))

            for partition, payload in topic_payloads.items():
                message.append(struct.pack('>i', partition))

        msg = b''.join(message)
        return struct.pack('>i%ds' % len(msg), len(msg), msg)

    @classmethod
    def decode_offset_fetch_response(cls, data):
        """
        Decode bytes to an OffsetFetchResponse

        Arguments:
            data: bytes to decode
        """

        ((correlation_id,), cur) = relative_unpack('>i', data, 0)
        ((num_topics,), cur) = relative_unpack('>i', data, cur)

        for _ in range(num_topics):
            (topic, cur) = read_short_string(data, cur)
            ((num_partitions,), cur) = relative_unpack('>i', data, cur)

            for _ in range(num_partitions):
                ((partition, offset), cur) = relative_unpack('>iq', data, cur)
                (metadata, cur) = read_short_string(data, cur)
                ((error,), cur) = relative_unpack('>h', data, cur)

                yield OffsetFetchResponse(topic, partition, offset,
                                          metadata, error)


def create_message(payload, key=None):
    """
    Construct a Message

    Arguments:
        payload: bytes, the payload to send to Kafka
        key: bytes, a key used for partition routing (optional)

    """
    return Message(0, 0, key, payload)


def create_gzip_message(payloads, key=None):
    """
    Construct a Gzipped Message containing multiple Messages

    The given payloads will be encoded, compressed, and sent as a single atomic
    message to Kafka.

    Arguments:
        payloads: list(bytes), a list of payload to send be sent to Kafka
        key: bytes, a key used for partition routing (optional)

    """
    message_set = KafkaProtocol._encode_message_set(
        [create_message(payload, pl_key) for payload, pl_key in payloads])

    gzipped = gzip_encode(message_set)
    codec = ATTRIBUTE_CODEC_MASK & CODEC_GZIP

    return Message(0, 0x00 | codec, key, gzipped)


def create_snappy_message(payloads, key=None):
    """
    Construct a Snappy Message containing multiple Messages

    The given payloads will be encoded, compressed, and sent as a single atomic
    message to Kafka.

    Arguments:
        payloads: list(bytes), a list of payload to send be sent to Kafka
        key: bytes, a key used for partition routing (optional)

    """
    message_set = KafkaProtocol._encode_message_set(
        [create_message(payload, pl_key) for payload, pl_key in payloads])

    snapped = snappy_encode(message_set)
    codec = ATTRIBUTE_CODEC_MASK & CODEC_SNAPPY

    return Message(0, 0x00 | codec, key, snapped)


def create_message_set(messages, codec=CODEC_NONE, key=None):
    """Create a message set using the given codec.

    If codec is CODEC_NONE, return a list of raw Kafka messages. Otherwise,
    return a list containing a single codec-encoded message.
    """
    if codec == CODEC_NONE:
        return [create_message(m, k) for m, k in messages]
    elif codec == CODEC_GZIP:
        return [create_gzip_message(messages, key)]
    elif codec == CODEC_SNAPPY:
        return [create_snappy_message(messages, key)]
    else:
        raise UnsupportedCodecError("Codec 0x%02x unsupported" % codec)
