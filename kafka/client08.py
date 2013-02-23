import base64
from collections import namedtuple, defaultdict
from functools import partial
from itertools import groupby, count
import logging
from operator import attrgetter
import socket
import struct
import time
import zlib

from .codec import gzip_encode, gzip_decode
from .codec import snappy_encode, snappy_decode
from .util import read_short_string, read_int_string
from .util import relative_unpack
from .util import write_short_string, write_int_string
from .util import group_by_topic_and_partition
from .util import BufferUnderflowError, ChecksumError

log = logging.getLogger("kafka")

###############
#   Structs   #
###############

# Request payloads
ProduceRequest = namedtuple("ProduceRequest", ["topic", "partition", "messages"])
FetchRequest = namedtuple("FetchRequest", ["topic", "partition", "offset", "max_bytes"])
OffsetRequest = namedtuple("OffsetRequest", ["topic", "partition", "time", "max_offsets"])
OffsetCommitRequest = namedtuple("OffsetCommitRequest", ["topic", "partition", "offset", "metadata"])
OffsetFetchRequest = namedtuple("OffsetFetchRequest", ["topic", "partition"])

# Response payloads
ProduceResponse = namedtuple("ProduceResponse", ["topic", "partition", "error", "offset"])
FetchResponse = namedtuple("FetchResponse", ["topic", "partition", "error", "highwaterMark", "messages"])
OffsetResponse = namedtuple("OffsetResponse", ["topic", "partition", "error", "offsets"])
OffsetCommitResponse = namedtuple("OffsetCommitResponse", ["topic", "partition", "error"])
OffsetFetchResponse = namedtuple("OffsetFetchResponse", ["topic", "partition", "offset", "metadata", "error"])
BrokerMetadata = namedtuple("BrokerMetadata", ["nodeId", "host", "port"])
PartitionMetadata = namedtuple("PartitionMetadata", ["topic", "partition", "leader", "replicas", "isr"])

# Other useful structs 
OffsetAndMessage = namedtuple("OffsetAndMessage", ["offset", "message"])
Message = namedtuple("Message", ["magic", "attributes", "key", "value"])
TopicAndPartition = namedtuple("TopicAndPartition", ["topic", "partition"])

class ErrorMapping(object):
    # Many of these are not actually used by the client
    UNKNOWN                   = -1
    NO_ERROR                  = 0
    OFFSET_OUT_OF_RANGE       = 1
    INVALID_MESSAGE           = 2
    UNKNOWN_TOPIC_OR_PARTITON = 3
    INVALID_FETCH_SIZE        = 4
    LEADER_NOT_AVAILABLE      = 5
    NOT_LEADER_FOR_PARTITION  = 6
    REQUEST_TIMED_OUT         = 7
    BROKER_NOT_AVAILABLE      = 8
    REPLICA_NOT_AVAILABLE     = 9
    MESSAGE_SIZE_TO_LARGE     = 10
    STALE_CONTROLLER_EPOCH    = 11
    OFFSET_METADATA_TOO_LARGE = 12

class KafkaProtocol(object):
    """
    Class to encapsulate all of the protocol encoding/decoding. This class does not
    have any state associated with it, it is purely for organization.
    """
    PRODUCE_KEY       = 0
    FETCH_KEY         = 1
    OFFSET_KEY        = 2
    METADATA_KEY      = 3
    OFFSET_COMMIT_KEY = 6
    OFFSET_FETCH_KEY  = 7

    ATTRIBUTE_CODEC_MASK = 0x03
    CODEC_NONE = 0x00
    CODEC_GZIP = 0x01
    CODEC_SNAPPY = 0x02

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
                           len(client_id),       #
                           client_id)            # ClientId

    @classmethod
    def _encode_message_set(cls, messages):
        """
        Encode a MessageSet. Unlike other arrays in the protocol, MessageSets are 
        not length-prefixed

        Format
        ======
        MessageSet => [Offset MessageSize Message]
          Offset => int64
          MessageSize => int32
        """
        message_set = ""
        for message in messages:
            encoded_message = KafkaProtocol._encode_message(message)
            message_set += struct.pack('>qi%ds' % len(encoded_message), 0, len(encoded_message), encoded_message)
        return message_set

    @classmethod
    def _encode_message(cls, message):
        """
        Encode a single message.

        The magic number of a message is a format version number. The only supported
        magic number right now is zero

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
            msg = struct.pack('>BB', message.magic, message.attributes)
            msg += write_int_string(message.key)
            msg += write_int_string(message.value)
            crc = zlib.crc32(msg)
            msg = struct.pack('>i%ds' % len(msg), crc, msg)
        else:
            raise Exception("Unexpected magic number: %d" % message.magic)
        return msg


    @classmethod
    def _decode_message_set_iter(cls, data):
        """
        Iteratively decode a MessageSet

        Reads repeated elements of (offset, message), calling decode_message to decode a
        single message. Since compressed messages contain futher MessageSets, these two methods
        have been decoupled so that they may recurse easily.
        """
        cur = 0
        while cur < len(data):
            try:
                ((offset, ), cur) = relative_unpack('>q', data, cur)
                (msg, cur) = read_int_string(data, cur)
                for (offset, message) in KafkaProtocol._decode_message(msg, offset):
                    yield OffsetAndMessage(offset, message)
            except BufferUnderflowError: # If we get a partial read of a message, stop
                raise StopIteration()

    @classmethod
    def _decode_message(cls, data, offset):
        """
        Decode a single Message

        The only caller of this method is decode_message_set_iter. They are decoupled to 
        support nested messages (compressed MessageSets). The offset is actually read from
        decode_message_set_iter (it is part of the MessageSet payload).
        """
        ((crc, magic, att), cur) = relative_unpack('>iBB', data, 0)
        if crc != zlib.crc32(data[4:]):
            raise ChecksumError("Message checksum failed")

        (key, cur) = read_int_string(data, cur)
        (value, cur) = read_int_string(data, cur)
        if att & KafkaProtocol.ATTRIBUTE_CODEC_MASK == KafkaProtocol.CODEC_NONE:
            yield (offset, Message(magic, att, key, value))
        elif att & KafkaProtocol.ATTRIBUTE_CODEC_MASK == KafkaProtocol.CODEC_GZIP:
            gz = gzip_decode(value)
            for (offset, message) in KafkaProtocol._decode_message_set_iter(gz):
                yield (offset, message)
        elif att & KafkaProtocol.ATTRIBUTE_CODEC_MASK == KafkaProtocol.CODEC_SNAPPY:
            snp = snappy_decode(value)
            for (offset, message) in KafkaProtocol._decode_message_set_iter(snp):
                yield (offset, message)

    ##################
    #   Public API   #
    ##################

    @classmethod
    def create_message(cls, payload, key=None):
        """
        Construct a Message

        Params
        ======
        payload: bytes, the payload to send to Kafka
        key: bytes, a key used for partition routing (optional)
        """
        return Message(0, 0, key, payload)

    @classmethod
    def create_gzip_message(cls, payloads, key=None):
        """
        Construct a Gzipped Message containing multiple Messages

        The given payloads will be encoded, compressed, and sent as a single atomic
        message to Kafka.

        Params
        ======
        payloads: list(bytes), a list of payload to send be sent to Kafka
        key: bytes, a key used for partition routing (optional)
        """
        message_set = KafkaProtocol._encode_message_set(
                [KafkaProtocol.create_message(payload) for payload in payloads])
        gzipped = gzip_encode(message_set) 
        return Message(0, 0x00 | (KafkaProtocol.ATTRIBUTE_CODEC_MASK & KafkaProtocol.CODEC_GZIP), key, gzipped)

    @classmethod
    def create_snappy_message(cls, payloads, key=None):
        """
        Construct a Snappy Message containing multiple Messages

        The given payloads will be encoded, compressed, and sent as a single atomic
        message to Kafka.

        Params
        ======
        payloads: list(bytes), a list of payload to send be sent to Kafka
        key: bytes, a key used for partition routing (optional)
        """
        message_set = KafkaProtocol._encode_message_set(
                [KafkaProtocol.create_message(payload) for payload in payloads])
        snapped = snappy_encode(message_set) 
        return Message(0, 0x00 | (KafkaProtocol.ATTRIBUTE_CODEC_MASK & KafkaProtocol.CODEC_SNAPPY), key, snapped)

    @classmethod
    def encode_produce_request(cls, client_id, correlation_id, payloads=[], acks=1, timeout=1000):
        """
        Encode some ProduceRequest structs

        Params
        ======
        client_id: string
        correlation_id: string
        payloads: list of ProduceRequest
        acks: How "acky" you want the request to be
            0: immediate response
            1: written to disk by the leader
            2+: waits for this many number of replicas to sync
            -1: waits for all replicas to be in sync
        timeout: Maximum time the server will wait for acks from replicas. This is _not_ a socket timeout
        """
        grouped_payloads = group_by_topic_and_partition(payloads)
        message = cls._encode_message_header(client_id, correlation_id, KafkaProtocol.PRODUCE_KEY)
        message += struct.pack('>hii', acks, timeout, len(grouped_payloads))
        for topic, topic_payloads in grouped_payloads.items():
            message += struct.pack('>h%dsi' % len(topic), len(topic), topic, len(topic_payloads))
            for partition, payload in topic_payloads.items():
                message_set = KafkaProtocol._encode_message_set(payload.messages)
                message += struct.pack('>ii%ds' % len(message_set), partition, len(message_set), message_set)
        return struct.pack('>i%ds' % len(message), len(message), message)

    @classmethod
    def decode_produce_response(cls, data):
        """
        Decode bytes to a ProduceResponse 

        Params
        ======
        data: bytes to decode
        """
        ((correlation_id, num_topics), cur) = relative_unpack('>ii', data, 0)
        for i in range(num_topics):
            ((strlen,), cur) = relative_unpack('>h', data, cur)
            topic = data[cur:cur+strlen]
            cur += strlen
            ((num_partitions,), cur) = relative_unpack('>i', data, cur)
            for i in range(num_partitions):
                ((partition, error, offset), cur) = relative_unpack('>ihq', data, cur)
                yield ProduceResponse(topic, partition, error, offset)

    @classmethod
    def encode_fetch_request(cls, client_id, correlation_id, payloads=[], max_wait_time=100, min_bytes=4096):
        """
        Encodes some FetchRequest structs

        Params
        ======
        client_id: string
        correlation_id: string
        payloads: list of FetchRequest
        max_wait_time: int, how long to block waiting on min_bytes of data
        min_bytes: int, the minimum number of bytes to accumulate before returning the response
        """
        
        grouped_payloads = group_by_topic_and_partition(payloads)
        message = cls._encode_message_header(client_id, correlation_id, KafkaProtocol.FETCH_KEY)
        message += struct.pack('>iiii', -1, max_wait_time, min_bytes, len(grouped_payloads)) # -1 is the replica id
        for topic, topic_payloads in grouped_payloads.items():
            message += write_short_string(topic)
            message += struct.pack('>i', len(topic_payloads))
            for partition, payload in topic_payloads.items():
                message += struct.pack('>iqi', partition, payload.offset, payload.max_bytes)
        return struct.pack('>i%ds' % len(message), len(message), message)

    @classmethod
    def decode_fetch_response_iter(cls, data):
        """
        Decode bytes to a FetchResponse

        Params
        ======
        data: bytes to decode
        """
        ((correlation_id, num_topics), cur) = relative_unpack('>ii', data, 0)
        for i in range(num_topics):
            (topic, cur) = read_short_string(data, cur)
            ((num_partitions,), cur) = relative_unpack('>i', data, cur)
            for i in range(num_partitions):
                ((partition, error, highwater_mark_offset), cur) = relative_unpack('>ihq', data, cur)
                (message_set, cur) = read_int_string(data, cur)
                yield FetchResponse(topic, partition, error, highwater_mark_offset,
                        KafkaProtocol._decode_message_set_iter(message_set))

    @classmethod
    def encode_offset_request(cls, client_id, correlation_id, payloads=[]):
        grouped_payloads = group_by_topic_and_partition(payloads)
        message = cls._encode_message_header(client_id, correlation_id, KafkaProtocol.OFFSET_KEY)
        message += struct.pack('>ii', -1, len(grouped_payloads)) # -1 is the replica id
        for topic, topic_payloads in grouped_payloads.items():
            message += write_short_string(topic)
            message += struct.pack('>i', len(topic_payloads))
            for partition, payload in topic_payloads.items():
                message += struct.pack('>iqi', partition, payload.time, payload.max_offsets)
        return struct.pack('>i%ds' % len(message), len(message), message)

    @classmethod
    def decode_offset_response(cls, data):
        """
        Decode bytes to an OffsetResponse

        Params
        ======
        data: bytes to decode
        """
        ((correlation_id, num_topics), cur) = relative_unpack('>ii', data, 0)
        for i in range(num_topics):
            (topic, cur) = read_short_string(data, cur)
            ((num_partitions,), cur) = relative_unpack('>i', data, cur)
            for i in range(num_partitions):
                ((partition, error, num_offsets,), cur) = relative_unpack('>ihi', data, cur)
                offsets = []
                for j in range(num_offsets):
                    ((offset,), cur) = relative_unpack('>q', data, cur)
                    offsets.append(offset)
                yield OffsetResponse(topic, partition, error, tuple(offsets))

    @classmethod
    def encode_metadata_request(cls, client_id, correlation_id, topics=[]):
        """
        Encode a MetadataRequest

        Params
        ======
        client_id: string
        correlation_id: string
        topics: list of strings
        """
        message = cls._encode_message_header(client_id, correlation_id, KafkaProtocol.METADATA_KEY)
        message += struct.pack('>i', len(topics))
        for topic in topics:
            message += struct.pack('>h%ds' % len(topic), len(topic), topic)
        return write_int_string(message)

    @classmethod
    def decode_metadata_response(cls, data):
        """
        Decode bytes to a MetadataResponse

        Params
        ======
        data: bytes to decode
        """
        ((correlation_id, numBrokers), cur) = relative_unpack('>ii', data, 0)

        # Broker info
        brokers = {}
        for i in range(numBrokers):
            ((nodeId, ), cur) = relative_unpack('>i', data, cur)
            (host, cur) = read_short_string(data, cur)
            ((port,), cur) = relative_unpack('>i', data, cur)
            brokers[nodeId] = BrokerMetadata(nodeId, host, port)

        # Topic info
        ((num_topics,), cur) = relative_unpack('>i', data, cur)
        topicMetadata = {}
        for i in range(num_topics):
            ((topicError,), cur) = relative_unpack('>h', data, cur)
            (topicName, cur) = read_short_string(data, cur)
            ((num_partitions,), cur) = relative_unpack('>i', data, cur)
            partitionMetadata = {}
            for j in range(num_partitions):
                ((partitionErrorCode, partition, leader, numReplicas), cur) = relative_unpack('>hiii', data, cur)
                (replicas, cur) = relative_unpack('>%di' % numReplicas, data, cur)
                ((numIsr,), cur) = relative_unpack('>i', data, cur)
                (isr, cur) = relative_unpack('>%di' % numIsr, data, cur)
                partitionMetadata[partition] = PartitionMetadata(topicName, partition, leader, replicas, isr)
            topicMetadata[topicName] = partitionMetadata
        return (brokers, topicMetadata)

    @classmethod
    def encode_offset_commit_request(cls, client_id, correlation_id, group, payloads):
        """
        Encode some OffsetCommitRequest structs

        Params
        ======
        client_id: string
        correlation_id: string
        group: string, the consumer group you are committing offsets for
        payloads: list of OffsetCommitRequest
        """
        grouped_payloads= group_by_topic_and_partition(payloads)
        message = cls._encode_message_header(client_id, correlation_id, KafkaProtocol.OFFSET_COMMIT_KEY)
        message += write_short_string(group)
        message += struct.pack('>i', len(grouped_payloads))
        for topic, topic_payloads in grouped_payloads.items():
            message += write_short_string(topic)
            message += struct.pack('>i', len(topic_payloads))
            for partition, payload in topic_payloads.items():
                message += struct.pack('>iq', partition, payload.offset)
                message += write_short_string(payload.metadata)
        return struct.pack('>i%ds' % len(message), len(message), message)

    @classmethod
    def decode_offset_commit_response(cls, data):
        """
        Decode bytes to an OffsetCommitResponse

        Params
        ======
        data: bytes to decode
        """
        data = data[2:] # TODO remove me when versionId is removed
        ((correlation_id,), cur) = relative_unpack('>i', data, 0)
        (client_id, cur) = read_short_string(data, cur)
        ((num_topics,), cur) = relative_unpack('>i', data, cur)
        for i in range(num_topics):
            (topic, cur) = read_short_string(data, cur)
            ((num_partitions,), cur) = relative_unpack('>i', data, cur)
            for i in range(num_partitions):
                ((partition, error), cur) = relative_unpack('>ih', data, cur)
                yield OffsetCommitResponse(topic, partition, error)

    @classmethod
    def encode_offset_fetch_request(cls, client_id, correlation_id, group, payloads):
        """
        Encode some OffsetFetchRequest structs

        Params
        ======
        client_id: string
        correlation_id: string
        group: string, the consumer group you are fetching offsets for
        payloads: list of OffsetFetchRequest
        """
        grouped_payloads = group_by_topic_and_partition(payloads)
        message = cls._encode_message_header(client_id, correlation_id, KafkaProtocol.OFFSET_FETCH_KEY)
        message += write_short_string(group)
        message += struct.pack('>i', len(grouped_payloads))
        for topic, topic_payloads in grouped_payloads.items():
            message += write_short_string(topic)
            message += struct.pack('>i', len(topic_payloads))
            for partition, payload in topic_payloads.items():
                message += struct.pack('>i', partition)
        return struct.pack('>i%ds' % len(message), len(message), message)

    @classmethod
    def decode_offset_fetch_response(cls, data):
        """
        Decode bytes to an OffsetFetchResponse

        Params
        ======
        data: bytes to decode
        """
        ((correlation_id,), cur) = relative_unpack('>i', data, 0)
        (client_id, cur) = read_short_string(data, cur)
        ((num_topics,), cur) = relative_unpack('>i', data, cur)
        for i in range(num_topics):
            (topic, cur) = read_short_string(data, cur)
            ((num_partitions,), cur) = relative_unpack('>i', data, cur)
            for i in range(num_partitions):
                ((partition, offset), cur) = relative_unpack('>iq', data, cur)
                (metadata, cur) = read_short_string(data, cur)
                ((error,), cur) = relative_unpack('>h', data, cur)
                yield OffsetFetchResponse(topic, partition, offset, metadata, error)


class KafkaConnection(object):
    """
    A socket connection to a single Kafka broker

    This class is _not_ thread safe. Each call to `send` must be followed
    by a call to `recv` in order to get the correct response. Eventually, 
    we can do something in here to facilitate multiplexed requests/responses
    since the Kafka API includes a correlation id.
    """
    def __init__(self, host, port, bufsize=4096):
        self.host = host
        self.port = port
        self.bufsize = bufsize
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.connect((host, port))
        self._sock.settimeout(10)

    def __str__(self):
        return "<KafkaConnection host=%s port=%d>" % (self.host, self.port)

    ###################
    #   Private API   #
    ###################

    def _consume_response(self):
        """
        Fully consumer the response iterator
        """
        data = ""
        for chunk in self._consume_response_iter():
            data += chunk
        return data

    def _consume_response_iter(self):
        """
        This method handles the response header and error messages. It 
        then returns an iterator for the chunks of the response
        """
        log.debug("Handling response from Kafka")

        # Read the size off of the header
        resp = self._sock.recv(4)
        if resp == "":
            raise Exception("Got no response from Kafka")
        (size,) = struct.unpack('>i', resp)

        messageSize = size - 4
        log.debug("About to read %d bytes from Kafka", messageSize)

        # Read the remainder of the response 
        total = 0
        while total < messageSize:
            resp = self._sock.recv(self.bufsize)
            log.debug("Read %d bytes from Kafka", len(resp))
            if resp == "":
                raise BufferUnderflowError("Not enough data to read this response")
            total += len(resp)
            yield resp

    ##################
    #   Public API   #
    ##################

    # TODO multiplex socket communication to allow for multi-threaded clients

    def send(self, requestId, payload):
        "Send a request to Kafka"
        sent = self._sock.sendall(payload)
        if sent == 0:
            raise RuntimeError("Kafka went away")
        self.data = self._consume_response()

    def recv(self, requestId):
        "Get a response from Kafka"
        return self.data

    def close(self):
        "Close this connection"
        self._sock.close()

class KafkaClient(object):

    CLIENT_ID = "kafka-python"
    ID_GEN = count() 

    def __init__(self, host, port, bufsize=4096):
        # We need one connection to bootstrap 
        self.bufsize = bufsize
        self.conns = {              # (host, port) -> KafkaConnection
            (host, port): KafkaConnection(host, port, bufsize)
        } 
        self.brokers = {}           # broker_id -> BrokerMetadata
        self.topics_to_brokers = {} # topic_id -> broker_id
        self.load_metadata_for_topics()

    def close(self):
        for conn in self.conns.values():
            conn.close()

    def get_conn_for_broker(self, broker):
        "Get or create a connection to a broker"
        if (broker.host, broker.port) not in self.conns:
            self.conns[(broker.host, broker.port)] = KafkaConnection(broker.host, broker.port, self.bufsize)
        return self.conns[(broker.host, broker.port)]

    def next_id(self):
        "Generate a new correlation id"
        return KafkaClient.ID_GEN.next()

    def load_metadata_for_topics(self, *topics):
        """
        Discover brokers and metadata for a set of topics. This method will
        recurse in the event of a retry.
        """
        requestId = self.next_id()
        request = KafkaProtocol.encode_metadata_request(KafkaClient.CLIENT_ID, requestId, topics) 
        response = self.try_send_request(requestId, request)
        if response is None:
            raise Exception("All servers failed to process request")
        (brokers, topics) = KafkaProtocol.decode_metadata_response(response)
        log.debug("Broker metadata: %s", brokers)
        log.debug("Topic metadata: %s", topics)
        self.brokers.update(brokers)
        self.topics_to_brokers = {}
        for topic, partitions in topics.items():
            for partition, meta in partitions.items():
                if meta.leader == -1:
                    log.info("Partition is unassigned, delay for 1s and retry")
                    time.sleep(1)
                    self.load_metadata_for_topics(topic)
                else:
                    self.topics_to_brokers[TopicAndPartition(topic, partition)] = brokers[meta.leader]

    def get_leader_for_partition(self, topic, partition):
        key = TopicAndPartition(topic, partition)
        if key not in self.topics_to_brokers:
            self.load_metadata_for_topics(topic)
        if key not in self.topics_to_brokers:
            raise Exception("Partition does not exist: %s" % str(key))
        return self.topics_to_brokers[key]

    def send_produce_request(self, payloads=[], fail_on_error=True, callback=None):
        """
        Encode and send some ProduceRequests

        ProduceRequests will be grouped by (topic, partition) and then sent to a specific
        broker. Output is a list of responses in the same order as the list of payloads
        specified

        Params
        ======
        payloads: list of ProduceRequest
        fail_on_error: boolean, should we raise an Exception if we encounter an API error?
        callback: function, instead of returning the ProduceResponse, first pass it through this function

        Return
        ======
        list of ProduceResponse or callback(ProduceResponse), in the order of input payloads
        """
        # Group the produce requests by which broker they go to
        original_keys = []
        payloads_by_broker = defaultdict(list)
        for payload in payloads:
            payloads_by_broker[self.get_leader_for_partition(payload.topic, payload.partition)] += payloads
            original_keys.append((payload.topic, payload.partition))

        # Accumulate the responses in a dictionary
        acc = {}

        # For each broker, send the list of request payloads
        for broker, payloads in payloads_by_broker.items():
            conn = self.get_conn_for_broker(broker)
            requestId = self.next_id()
            request = KafkaProtocol.encode_produce_request(KafkaClient.CLIENT_ID, requestId, payloads)
            # Send the request
            conn.send(requestId, request)
            response = conn.recv(requestId)
            for produce_response in KafkaProtocol.decode_produce_response(response):
                # Check for errors
                if fail_on_error == True and produce_response.error != 0:
                    raise Exception("ProduceRequest for %s failed with errorcode=%d", 
                            (TopicAndPartition(produce_response.topic, produce_response.partition), produce_response.error))
                # Run the callback
                if callback is not None:
                    acc[(produce_response.topic, produce_response.partition)] = callback(produce_response)
                else:
                    acc[(produce_response.topic, produce_response.partition)] = produce_response

        # Order the accumulated responses by the original key order
        return (acc[k] for k in original_keys)

    def send_fetch_request(self, payloads=[], fail_on_error=True, callback=None):
        """
        Encode and send a FetchRequest
        
        Payloads are grouped by topic and partition so they can be pipelined to the same
        brokers.
        """
        # Group the produce requests by which broker they go to
        original_keys = []
        payloads_by_broker = defaultdict(list)
        for payload in payloads:
            payloads_by_broker[self.get_leader_for_partition(payload.topic, payload.partition)].append(payload)
            original_keys.append((payload.topic, payload.partition))

        # Accumulate the responses in a dictionary, keyed by topic+partition 
        acc = {}

        # For each broker, send the list of request payloads
        for broker, payloads in payloads_by_broker.items():
            conn = self.get_conn_for_broker(broker)
            requestId = self.next_id()
            request = KafkaProtocol.encode_fetch_request(KafkaClient.CLIENT_ID, requestId, payloads)
            # Send the request
            conn.send(requestId, request)
            response = conn.recv(requestId)
            for fetch_response in KafkaProtocol.decode_fetch_response_iter(response):
                # Check for errors
                if fail_on_error == True and fetch_response.error != 0:
                    raise Exception("FetchRequest %s failed with errorcode=%d" %
                            (TopicAndPartition(fetch_response.topic, fetch_response.partition), fetch_response.error))
                # Run the callback
                if callback is not None:
                    acc[(fetch_response.topic, fetch_response.partition)] = callback(fetch_response)
                else:
                    acc[(fetch_response.topic, fetch_response.partition)] = fetch_response

        # Order the accumulated responses by the original key order
        return (acc[k] for k in original_keys)

    def try_send_request(self, requestId, request):
        """
        Attempt to send a broker-agnostic request to one of the available brokers.
        Keep trying until you succeed.
        """
        for conn in self.conns.values():
            try:
                conn.send(requestId, request)
                response = conn.recv(requestId)
                return response
            except Exception, e:
                log.warning("Could not send request [%r] to server %s, trying next server: %s" % (request, conn, e))
                continue
        return None

    def send_offset_request(self, payloads=[], fail_on_error=True, callback=None):
        requestId = self.next_id()
        request = KafkaProtocol.encode_offset_request(KafkaClient.CLIENT_ID, requestId, payloads)
        response = self.try_send_request(requestId, request)
        if response is None:
            if fail_on_error is True:
                raise Exception("All servers failed to process request")
            else:
                return None
        out = []
        for offset_response in KafkaProtocol.decode_offset_response(response):
            if fail_on_error == True and offset_response.error != 0:
                raise Exception("OffsetRequest failed with errorcode=%s", offset_response.error)
            if callback is not None:
                out.append(callback(offset_response))
            else:
                out.append(offset_response)
        return out

    def send_offset_commit_request(self, group, payloads=[], fail_on_error=True, callback=None):
        requestId = self.next_id()
        request = KafkaProtocol.encode_offset_commit_request(KafkaClient.CLIENT_ID, requestId, group, payloads)
        response = self.try_send_request(requestId, request)
        if response is None:
            if fail_on_error is True:
                raise Exception("All servers failed to process request")
            else:
                return None
        out = []
        for offset_commit_response in KafkaProtocol.decode_offset_commit_response(response):
            if fail_on_error == True and offset_commit_response.error != 0:
                print(offset_commit_response)
                raise Exception("OffsetCommitRequest failed with errorcode=%s", offset_commit_response.error)
            if callback is not None:
                out.append(callback(offset_commit_response))
            else:
                out.append(offset_commit_response)
        return out

    def send_offset_fetch_request(self, group, payloads=[], fail_on_error=True, callback=None):
        requestId = self.next_id()
        request = KafkaProtocol.encode_offset_fetch_request(KafkaClient.CLIENT_ID, requestId, group, payloads)
        response = self.try_send_request(requestId, request)
        if response is None:
            if fail_on_error is True:
                raise Exception("All servers failed to process request")
            else:
                return None
        out = []
        for offset_fetch_response in KafkaProtocol.decode_offset_fetch_response(response):
            if fail_on_error == True and offset_fetch_response.error != 0:
                raise Exception("OffsetFetchRequest for topic=%s, partition=%d failed with errorcode=%s" % (
                    offset_fetch_response.topic, offset_fetch_response.partition, offset_fetch_response.error))
            if callback is not None:
                out.append(callback(offset_fetch_response))
            else:
                out.append(offset_fetch_response)
        return out


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    topic = "foo8"
    # Bootstrap connection
    conn = KafkaClient("localhost", 49720)

    # Create some Messages
    messages = (KafkaProtocol.create_gzip_message(["GZIPPed"]),
                KafkaProtocol.create_message("not-gzipped"))

    produce1 = ProduceRequest(topic=topic, partition=0, messages=messages)
    produce2 = ProduceRequest(topic=topic, partition=1, messages=messages)

    # Send the ProduceRequest
    produce_resp = conn.send_produce_request(payloads=[produce1, produce2])

    # Check for errors
    for resp in produce_resp:
        if resp.error != 0:
            raise Exception("ProduceRequest failed with errorcode=%d", resp.error)
        print resp

    # Offset commit/fetch
    #conn.send_offset_commit_request(group="group", payloads=[OffsetCommitRequest("topic-1", 0, 42, "METADATA?")])
    #conn.send_offset_fetch_request(group="group", payloads=[OffsetFetchRequest("topic-1", 0)])

    def init_offsets(offset_response):
        if offset_response.error not in (ErrorMapping.NO_ERROR, ErrorMapping.UNKNOWN_TOPIC_OR_PARTITON):
            raise Exception("OffsetFetch failed: %s" % (offset_response))
        elif offset_response.error == ErrorMapping.UNKNOWN_TOPIC_OR_PARTITON:
            return 0
        else:
            return offset_response.offset
    # Load offsets
    (offset1, offset2) = conn.send_offset_fetch_request(
        group="group1", 
        payloads=[OffsetFetchRequest(topic, 0),OffsetFetchRequest(topic, 1)],
        fail_on_error=False,
        callback=init_offsets
    )
    print offset1, offset2

    while True:
        for resp in conn.send_fetch_request(payloads=[FetchRequest(topic=topic, partition=0, offset=offset1, max_bytes=4096)]):
            i = 0
            for msg in resp.messages:
                print msg
                offset1 = msg.offset+1
                print offset1, conn.send_offset_commit_request(group="group1", payloads=[OffsetCommitRequest(topic, 0, offset1, "")])
                i += 1
            if i == 0:
                raise StopIteration("no more messages")

        for resp in conn.send_fetch_request(payloads=[FetchRequest(topic=topic, partition=1, offset=offset2, max_bytes=4096)]):
            i = 0
            for msg in resp.messages:
                print msg
                offset2 = msg.offset+1
                print offset2, conn.send_offset_commit_request(group="group1", payloads=[OffsetCommitRequest(topic, 1, offset2, "")])
                i += 1
            if i == 0:
                raise StopIteration("no more messages")

