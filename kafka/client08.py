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

log = logging.getLogger("kafka")

# Request payloads
ProduceRequest = namedtuple("ProduceRequest", ["topic", "partition", "messages"])
FetchRequest = namedtuple("FetchRequest", ["topic", "partition", "offset", "maxBytes"])
OffsetRequest = namedtuple("OffsetRequest", ["topic", "partition", "time", "maxOffsets"])

# Response payloads
ProduceResponse = namedtuple("ProduceResponse", ["topic", "partition", "error", "offset"])
FetchResponse = namedtuple("FetchResponse", ["topic", "partition", "error", "highwaterMark", "messages"])
OffsetResponse = namedtuple("OffsetResponse", ["topic", "partition", "error", "offset"])
BrokerMetadata = namedtuple("BrokerMetadata", ["nodeId", "host", "port"])
PartitionMetadata = namedtuple("PartitionMetadata", ["topic", "partitionId", "leader", "replicas", "isr"])

# Other useful structs 
OffsetAndMessage = namedtuple("OffsetAndMessage", ["offset", "message"])
Message = namedtuple("Message", ["magic", "attributes", "key", "value"])
TopicAndPartition = namedtuple("TopicAndPartition", ["topic", "partitionId"])

class ErrorMapping(object):
    Unknown                 = -1
    NoError                 = 0
    OffsetOutOfRange        = 1
    InvalidMessage          = 2
    UnknownTopicOrPartition = 3
    InvalidFetchSize        = 4
    LeaderNotAvailable      = 5
    NotLeaderForPartition   = 6
    RequestTimedOut         = 7
    BrokerNotAvailable      = 8
    ReplicaNotAvailable     = 9
    MessageSizeTooLarge     = 10
    StaleControllerEpoch    = 11
    OffsetMetadataTooLarge  = 12

class KafkaProtocol(object):
    PRODUCE_KEY      = 0
    FETCH_KEY        = 1
    OFFSET_KEY       = 2
    METADATA_KEY     = 3

    ATTRIBUTE_CODEC_MASK = 0x03

    @classmethod
    def encode_message_header(cls, clientId, correlationId, requestKey):
        return struct.pack('>HHiH%ds' % len(clientId), 
                           requestKey,          # ApiKey
                           0,                   # ApiVersion
                           correlationId,       # CorrelationId
                           len(clientId),       #
                           clientId)            # ClientId

    @classmethod
    def encode_message_set(cls, messages):
        message_set = ""
        for message in messages:
            encoded_message = KafkaProtocol.encode_message(message)
            message_set += struct.pack('>qi%ds' % len(encoded_message), 0, len(encoded_message), encoded_message)
        return message_set

    @classmethod
    def encode_message(cls, message):
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
    def create_message(cls, value):
        return Message(0, 0, "foo", value)

    @classmethod
    def create_gzip_message(cls, value):
        message_set = KafkaProtocol.encode_message_set([KafkaProtocol.create_message(value)])
        gzipped = gzip_encode(message_set) 
        return Message(0, 0x00 | (KafkaProtocol.ATTRIBUTE_CODEC_MASK & 0x01), "foo", gzipped)

    @classmethod
    def decode_message_set_iter(cls, data):
        """
        Decode a MessageSet, iteratively

        Reads repeated elements of (offset, message), calling decode_message to decode a
        single message. Since compressed messages contain futher MessageSets, these two methods
        have been decoupled so that they may recurse easily.
        
        Format
        ======
        MessageSet => [Offset MessageSize Message]
          Offset => int64
          MessageSize => int32

        N.B., the repeating element of the MessageSet is not preceded by an int32 like other
        repeating elements in this protocol
        """
        cur = 0
        while cur < len(data):
            ((offset, ), cur) = relative_unpack('>q', data, cur)
            (msg, cur) = read_int_string(data, cur)
            for (offset, message) in KafkaProtocol.decode_message(msg, offset):
                yield OffsetAndMessage(offset, message)

    @classmethod
    def decode_message(cls, data, offset):
        """
        Decode a single Message

        The only caller of this method is decode_message_set_iter. They are decoupled to 
        support nested messages (compressed MessageSets). The offset is actually read from
        decode_message_set_iter (it is part of the MessageSet payload).

        Format 
        ========
        Message => Crc MagicByte Attributes Key Value
          Crc => int32
          MagicByte => int8
          Attributes => int8
          Key => bytes
          Value => bytes
        """
        ((crc, magic, att), cur) = relative_unpack('>iBB', data, 0)
        assert crc == zlib.crc32(data[4:])
        (key, cur) = read_int_string(data, cur)
        (value, cur) = read_int_string(data, cur)
        if att & KafkaProtocol.ATTRIBUTE_CODEC_MASK == 0:
            yield (offset, Message(magic, att, key, value))
        elif att & KafkaProtocol.ATTRIBUTE_CODEC_MASK == 1:
            gz = gzip_decode(value)
            for (offset, message) in KafkaProtocol.decode_message_set_iter(gz):
                yield (offset, message)
        elif att & KafkaProtocol.ATTRIBUTE_CODEC_MASK == 2:
            snp = snappy_decode(value)
            for (offset, message) in KafkaProtocol.decode_message_set_iter(snp):
                yield (offset, message)

    @classmethod
    def encode_metadata_request(cls, clientId, correlationId, *topics):
        # Header
        message = cls.encode_message_header(clientId, correlationId, KafkaProtocol.METADATA_KEY)

        # TopicMetadataRequest
        message += struct.pack('>i', len(topics))
        for topic in topics:
            message += struct.pack('>H%ds' % len(topic), len(topic), topic)

        # Length-prefix the whole thing
        return write_int_string(message)

    @classmethod
    def decode_metadata_response(cls, data):
        # TopicMetadataResponse
        cur = 0
        ((correlationId, numBrokers), cur) = relative_unpack('>ii', data, cur)
        brokers = {}
        for i in range(numBrokers):
            ((nodeId, ), cur) = relative_unpack('>i', data, cur)
            (host, cur) = read_short_string(data, cur)
            ((port,), cur) = relative_unpack('>i', data, cur)
            brokers[nodeId] = BrokerMetadata(nodeId, host, port)

        ((numTopics,), cur) = relative_unpack('>i', data, cur)
        topicMetadata = {}
        for i in range(numTopics):
            ((topicError,), cur) = relative_unpack('>H', data, cur)
            (topicName, cur) = read_short_string(data, cur)
            ((numPartitions,), cur) = relative_unpack('>i', data, cur)
            partitionMetadata = {}
            for j in range(numPartitions):
                ((partitionErrorCode, partitionId, leader, numReplicas), cur) = relative_unpack('>Hiii', data, cur)
                (replicas, cur) = relative_unpack('>%di' % numReplicas, data, cur)
                ((numIsr,), cur) = relative_unpack('>i', data, cur)
                (isr, cur) = relative_unpack('>%di' % numIsr, data, cur)
                partitionMetadata[partitionId] = PartitionMetadata(topicName, partitionId, leader, replicas, isr)
            topicMetadata[topicName] = partitionMetadata
        return (brokers, topicMetadata)

    @classmethod
    def encode_produce_request(self, clientId, correlationId, payloads=[], acks=1, timeout=1000):
        # Group the payloads by topic
        sorted_payloads = sorted(payloads, key=attrgetter("topic"))
        grouped_payloads = list(groupby(sorted_payloads, key=attrgetter("topic")))

        # Pack the message header
        message = struct.pack('>HHiH%ds' % len(clientId), 
                              KafkaProtocol.PRODUCE_KEY,    # ApiKey
                              0,                            # ApiVersion
                              correlationId,                # CorrelationId
                              len(clientId),                #
                              clientId)                     # ClientId

        # Pack the message sets
        message += struct.pack('>Hii', acks, timeout, len(grouped_payloads))
        for topic, payload in grouped_payloads:
            payloads = list(payloads)
            message += struct.pack('>H%dsi' % len(topic), len(topic), topic, len(payloads))
            for payload in payloads:
                message_set = KafkaProtocol.encode_message_set(payload.messages)
                message += struct.pack('>ii%ds' % len(message_set), payload.partition, len(message_set), message_set)

        # Length-prefix the whole thing
        return struct.pack('>i%ds' % len(message), len(message), message)

    @classmethod
    def decode_produce_response(cls, data):
        ((correlationId, numTopics), cur) = relative_unpack('>ii', data, 0)
        for i in range(numTopics):
            ((strlen,), cur) = relative_unpack('>H', data, cur)
            topic = data[cur:cur+strlen]
            cur += strlen
            ((numPartitions,), cur) = relative_unpack('>i', data, cur)
            for i in range(numPartitions):
                ((partition, error, offset), cur) = relative_unpack('>iHq', data, cur)
                yield ProduceResponse(topic, partition, error, offset)

    @classmethod
    def encode_fetch_request(cls, clientId, correlationId, payloads=[], replicaId=-1, maxWaitTime=100, minBytes=1024):
        # Group the payloads by topic
        sorted_payloads = sorted(payloads, key=attrgetter("topic"))
        grouped_payloads = list(groupby(sorted_payloads, key=attrgetter("topic")))

        # Pack the message header
        message = struct.pack('>HHiH%ds' % len(clientId), 
                              KafkaProtocol.FETCH_KEY,  # ApiKey
                              0,                        # ApiVersion
                              correlationId,            # CorrelationId
                              len(clientId),            #
                              clientId)                 # ClientId

        # Pack the FetchRequest
        message += struct.pack('>iiii',
                               replicaId,   # ReplicaId
                               maxWaitTime, # MaxWaitTime
                               minBytes,    # MinBytes
                               len(grouped_payloads))
        for topic, payload in grouped_payloads:
            payloads = list(payloads)
            message += write_short_string(topic)
            message += struct.pack('>i', len(payloads))
            for payload in payloads:
                message += struct.pack('>iqi', payload.partition, payload.offset, payload.maxBytes)

        # Length-prefix the whole thing
        return struct.pack('>i%ds' % len(message), len(message), message)

    @classmethod
    def decode_fetch_response_iter(cls, data):
        ((correlationId, numTopics), cur) = relative_unpack('>ii', data, 0)
        for i in range(numTopics):
            (topic, cur) = read_short_string(data, cur)
            ((numPartitions,), cur) = relative_unpack('>i', data, cur)
            for i in range(numPartitions):
                ((partition, error, highwaterMarkOffset), cur) = relative_unpack('>iHq', data, cur)
                (messageSet, cur) = read_int_string(data, cur)
                yield FetchResponse(topic, partition, error, highwaterMarkOffset, KafkaProtocol.decode_message_set_iter(messageSet))

    @classmethod
    def encode_offset_request(cls, clientId, correlationId, payloads=[], replicaId=-1):
        # Group the payloads by topic
        sorted_payloads = sorted(payloads, key=attrgetter("topic"))
        grouped_payloads = list(groupby(sorted_payloads, key=attrgetter("topic")))

        # Pack the message header
        message = struct.pack('>HHiH%ds' % len(clientId), 
                              KafkaProtocol.OFFSET_KEY, # ApiKey
                              0,                        # ApiVersion
                              correlationId,            # CorrelationId
                              len(clientId),            #
                              clientId)                 # ClientId

        message += struct.pack('>ii', replicaId, len(grouped_payloads))

        # Pack the OffsetRequest
        for topic, payload in grouped_payloads:
            payloads = list(payloads)
            message += write_short_string(topic)
            message += struct.pack('>i', len(payloads))
            for payload in payloads:
                message += struct.pack('>iqi', payload.partition, payload.time, payload.maxOffsets)

        # Length-prefix the whole thing
        return struct.pack('>i%ds' % len(message), len(message), message)

    @classmethod
    def decode_offset_response(cls, data):
        ((correlationId, numTopics), cur) = relative_unpack('>ii', data, 0)
        for i in range(numTopics):
            (topic, cur) = read_short_string(data, cur)
            ((numPartitions,), cur) = relative_unpack('>i', data, cur)
            for i in range(numPartitions):
                ((partition, error, offset), cur) = relative_unpack('>iHq', data, cur)
                yield OffsetResponse(topic, partition, error, offset)



class Conn(object):
    """
    A socket connection to a single Kafka broker
    """
    def __init__(self, host, port, bufsize=1024):
        self.host = host
        self.port = port
        self.bufsize = bufsize
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.connect((host, port))
        self._sock.settimeout(10)

    def close(self):
        self._sock.close()

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

        # Header
        resp = self._sock.recv(4)
        if resp == "":
            raise Exception("Got no response from Kafka")
        (size,) = struct.unpack('>i', resp)

        messageSize = size - 4
        log.debug("About to read %d bytes from Kafka", messageSize)

        # Response iterator
        total = 0
        while total < messageSize:
            resp = self._sock.recv(self.bufsize)
            log.debug("Read %d bytes from Kafka", len(resp))
            if resp == "":
                raise Exception("Underflow")
            total += len(resp)
            yield resp

    def send(self, requestId, payload):
        #print(repr(payload))
        sent = self._sock.sendall(payload)
        if sent == 0:
            raise RuntimeError("Kafka went away")
        self.data = self._consume_response()
        #print(repr(self.data))

    def recv(self, requestId):
        return self.data

class KafkaConnection(object):
    """
    Low-level API for Kafka 0.8
    """

    # ClientId for Kafka
    CLIENT_ID = "kafka-python"

    # Global correlation ids
    ID_GEN = count() 

    def __init__(self, host, port, bufsize=1024):
        # We need one connection to bootstrap 
        self.bufsize = bufsize
        self.conns = {(host, port): Conn(host, port, bufsize)}
        self.brokers = {}           # broker Id -> BrokerMetadata
        self.topics_to_brokers = {} # topic Id -> broker Id
        self.load_metadata_for_topics()

    def get_conn_for_broker(self, broker):
        "Get or create a connection to a broker"
        if (broker.host, broker.port) not in self.conns:
            self.conns[(broker.host, broker.port)] = Conn(broker.host, broker.port, self.bufsize)
        return self.conns[(broker.host, broker.port)]

    def next_id(self):
        return KafkaConnection.ID_GEN.next()

    def load_metadata_for_topics(self, *topics):
        """
        Discover brokers and metadata for a set of topics
        """
        requestId = self.next_id()
        request = KafkaProtocol.encode_metadata_request(KafkaConnection.CLIENT_ID, requestId, *topics) 
        conn = self.conns.values()[0] # Just get the first one in the list
        conn.send(requestId, request)
        response = conn.recv(requestId)
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
                    return 
                else:
                    self.topics_to_brokers[TopicAndPartition(topic, partition)] = brokers[meta.leader]

    def get_leader_for_partition(self, topic, partition):
        key = TopicAndPartition(topic, partition)
        if key not in self.topics_to_brokers:
            self.load_metadata_for_topics(topic)
        return self.topics_to_brokers[key]

    def send_produce_request(self, payloads=[], fail_on_error=True, callback=None):
        # Group the produce requests by topic+partition
        sorted_payloads = sorted(payloads, key=lambda x: (x.topic, x.partition))
        grouped_payloads = groupby(sorted_payloads, key=lambda x: (x.topic, x.partition))

        # Group the produce requests by which broker they go to
        payloads_by_broker = defaultdict(list)
        for (topic, partition), payload in grouped_payloads:
            payloads_by_broker[self.get_leader_for_partition(topic, partition)] += list(payload)

        out = []
        # For each broker, send the list of request payloads
        for broker, payloads in payloads_by_broker.items():
            conn = self.get_conn_for_broker(broker)
            requestId = self.next_id()
            request = KafkaProtocol.encode_produce_request(KafkaConnection.CLIENT_ID, requestId, payloads)
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
                    out.append(callback(produce_response))
                else:
                    out.append(produce_response)
        return out

    def send_fetch_request(self, payloads=[], fail_on_error=True, callback=None):
        """
        Encode and send a FetchRequest
        
        Payloads are grouped by topic and partition so they can be pipelined to the same
        brokers.
        """
        # Group the produce requests by topic+partition
        sorted_payloads = sorted(payloads, key=lambda x: (x.topic, x.partition))
        grouped_payloads = groupby(sorted_payloads, key=lambda x: (x.topic, x.partition))

        # Group the produce requests by which broker they go to
        payloads_by_broker = defaultdict(list)
        for (topic, partition), payload in grouped_payloads:
            payloads_by_broker[self.get_leader_for_partition(topic, partition)] += list(payload)

        out = []
        # For each broker, send the list of request payloads
        for broker, payloads in payloads_by_broker.items():
            conn = self.get_conn_for_broker(broker)
            requestId = self.next_id()
            request = KafkaProtocol.encode_fetch_request(KafkaConnection.CLIENT_ID, requestId, payloads)
            # Send the request
            conn.send(requestId, request)
            response = conn.recv(requestId)
            for fetch_response in KafkaProtocol.decode_fetch_response_iter(response):
                # Check for errors
                if fail_on_error == True and fetch_response.error != 0:
                    raise Exception("FetchRequest %s failed with errorcode=%d",
                            (TopicAndPartition(fetch_response.topic, fetch_response.partition), fetch_response.error))
                # Run the callback
                if callback is not None:
                    out.append(callback(fetch_response))
                else:
                    out.append(fetch_response)
        return out

if __name__ == "__main__":
    # Bootstrap connection
    conn = KafkaConnection("localhost", 9092)

    # Create some Messages
    messages = (KafkaProtocol.create_gzip_message("GZIPPed"),
                KafkaProtocol.create_message("not-gzipped"))

    # Create a ProduceRequest
    produce = ProduceRequest("foo5", 0, messages)

    # Send the ProduceRequest
    produce_resp = conn.send_produce_request([produce])

    # Check for errors
    for resp in produce_resp:
        if resp.error != 0:
            raise Exception("ProduceRequest failed with errorcode=%d", resp.error)
        print resp
    

