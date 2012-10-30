from collections import namedtuple
from cStringIO import StringIO
import logging
import gzip
import select
import socket
import struct
import zlib

from .codec import gzip_encode, gzip_decode

log = logging.getLogger("kafka")

error_codes = {
   -1: "UnknownError",
    0: None,
    1: "OffsetOutOfRange",
    2: "InvalidMessage",
    3: "WrongPartition",
    4: "InvalidFetchSize"
}

class KafkaException(Exception):
    def __init__(self, errorType):
        self.errorType = errorType
    def __str__(self):
        return str(self.errorType)

Message = namedtuple("Message", ["magic", "attributes", "crc", "payload"])
FetchRequest = namedtuple("FetchRequest", ["topic", "partition", "offset", "size"])
ProduceRequest = namedtuple("ProduceRequest", ["topic", "partition", "messages"])
OffsetRequest = namedtuple("OffsetRequest", ["topic", "partition", "time", "maxOffsets"])

def length_prefix_message(msg):
    """
    Prefix a message with it's length as an int
    """
    return struct.pack('>i', len(msg)) + msg

class KafkaClient(object):
    """
    Request Structure
    =================

    <Request>     ::= <len> <request-key> <payload>
    <len>         ::= <int32>
    <request-key> ::= 0 | 1 | 2 | 3 | 4
    <payload>     ::= <ProduceRequest> | <FetchRequest> | <MultiFetchRequest> | <MultiProduceRequest> | <OffsetRequest>

    Response Structure
    ==================

    <Response>    ::= <len> <err> <payload>
    <len>         ::= <int32>
    <err>         ::= -1 | 0 | 1 | 2 | 3 | 4
    <payload>     ::= <ProduceResponse> | <FetchResponse> | <MultiFetchResponse> | <MultiProduceResponse> | <OffsetResponse>

    Messages are big-endian byte order
    """

    PRODUCE_KEY      = 0
    FETCH_KEY        = 1
    MULTIFETCH_KEY   = 2
    MULTIPRODUCE_KEY = 3
    OFFSET_KEY       = 4

    ATTRIBUTE_CODEC_MASK = 0x03

    def __init__(self, host, port, bufsize=1024):
        self.host = host
        self.port = port
        self.bufsize = bufsize
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.connect((host, port))
        self._sock.settimeout(10)
        log.debug("Connected to %s on %d", host, port)

    ######################
    #   Protocol Stuff   #
    ######################

    def _consume_response_iter(self):
        """
        This method handles the response header and error messages. It 
        then returns an iterator for the chunks of the response
        """
        log.debug("Handling response from Kafka")
        # Header
        resp = self._sock.recv(6)
        if resp == "":
            raise Exception("Got no response from Kafka")
        (size, err) = struct.unpack('>iH', resp)

        log.debug("About to read %d bytes from Kafka", size-2)
        # Handle error
        error = error_codes.get(err)
        if error is not None:
            raise KafkaException(error)

        # Response iterator
        total = 0
        while total < (size-2):
            resp = self._sock.recv(self.bufsize)
            log.debug("Read %d bytes from Kafka", len(resp))
            if resp == "":
                raise Exception("Underflow")
            total += len(resp)
            yield resp

    def _consume_response(self):
        """
        Fully consumer the response iterator
        """
        data = ""
        for chunk in self._consume_response_iter():
            data += chunk
        return data

    @classmethod
    def encode_message(cls, message):
        """
        Encode a Message from a Message tuple

        Params
        ======
        message: Message

        Wire Format
        ===========
        <Message>    ::= <Message-0> | <Message-1> 
        <Message-0>  ::= <N> 0 <header-0> <payload>
        <Message-1>  ::= <N> 1 <header-1> <payload>
        <N>          ::= <int32>
        <header-0>   ::= <crc>
        <header-1>   ::= <attributes><crc>
        <crc>        ::= <int32>
        <payload>    ::= <bytes>
        <attributes> ::= <int8> 

        The crc is a crc32 checksum of the message payload. The attributes are bitmask
        used for indicating the compression algorithm.
        """
        if message.magic == 0:
            msg = struct.pack('>Bi%ds' % len(message.payload),
                               message.magic, message.crc, message.payload)
        elif message.magic == 1:
            msg = struct.pack('>BBi%ds' % len(message.payload),
                               message.magic, message.attributes, message.crc, message.payload)
        else:
            raise Exception("Unexpected magic number: %d" % message.magic)
        msg = length_prefix_message(msg)
        log.debug("Encoded %s as %r" % (message, msg))
        return msg

    @classmethod
    def encode_message_set(cls, messages):
        """
        Encode a MessageSet

        One or more concatenated Messages
        """
        message_set = ""
        for message in messages:
            encoded_message = cls.encode_message(message)
            message_set += encoded_message
        return message_set

    @classmethod
    def encode_produce_request(cls, produceRequest):
        """
        Encode a ProduceRequest

        Wire Format
        ===========
        <ProduceRequest> ::= <request-key> <topic> <partition> <len> <MessageSet>
        <request-key>    ::= 0
        <topic>          ::= <topic-length><string>
        <topic-length>   ::= <int16>
        <partition>      ::= <int32>
        <len>            ::= <int32>

        The request-key (0) is encoded as a short (int16). len is the length of the proceeding MessageSet
        """
        (topic, partition, messages) = produceRequest
        message_set = cls.encode_message_set(messages)
        log.debug("Sending MessageSet: %r" % message_set)
        req = struct.pack('>HH%dsii%ds' % (len(topic), len(message_set)), 
                KafkaClient.PRODUCE_KEY, len(topic), topic, partition, len(message_set), message_set)
        return req

    @classmethod
    def encode_multi_produce_request(cls, produceRequests):
        """
        Encode a MultiProducerRequest

        Params
        ======
        produceRequest: list of ProduceRequest objects

        Returns
        =======
        Encoded request

        Wire Format
        ===========
        <MultiProducerReqeust>  ::= <request-key> <num> <ProduceRequests>
        <num>                   ::= <int16>
        <ProduceRequests>       ::= <ProduceRequest> [ <ProduceRequests> ]
        <ProduceRequest>        ::= <topic> <partition> <len> <MessageSet>
        <topic>                 ::= <topic-length><string>
        <topic-length>          ::= <int16>
        <partition>             ::= <int32>
        <len>                   ::= <int32>

        num is the number of ProduceRequests being encoded
        """
        req = struct.pack('>HH', KafkaClient.MULTIPRODUCE_KEY, len(produceRequests))
        for (topic, partition, messages) in produceRequests:
            message_set = cls.encode_message_set(messages)
            req += struct.pack('>H%dsii%ds' % (len(topic), len(message_set)), 
                               len(topic), topic, partition, len(message_set), message_set)
        return req

    @classmethod
    def encode_fetch_request(cls, fetchRequest):
        """
        Encode a FetchRequest message

        Wire Format
        ===========
        <FetchRequest> ::= <request-key> <topic> <partition> <offset> <size>
        <request-key>  ::= 1
        <topic>        ::= <topic-length><string>
        <topic-length> ::= <int16>
        <partition>    ::= <int32>
        <offset>       ::= <int64>
        <size>         ::= <int32>
    
        The request-key (1) is encoded as a short (int16).
        """
        (topic, partition, offset, size) = fetchRequest
        req = struct.pack('>HH%dsiqi' % len(topic), 
                KafkaClient.FETCH_KEY, len(topic), topic, partition, offset, size)
        return req

    @classmethod
    def encode_multi_fetch_request(cls, fetchRequests):
        """
        Encode the MultiFetchRequest message from a list of FetchRequest objects

        Params
        ======
        fetchRequests: list of FetchRequest

        Returns
        =======
        req: bytes, The message to send to Kafka

        Wire Format
        ===========
        <MultiFetchRequest> ::= <request-key> <num> [ <FetchRequests> ]
        <request-key>       ::= 2
        <num>               ::= <int16>
        <FetchRequests>     ::= <FetchRequest> [ <FetchRequests> ]
        <FetchRequest>      ::= <topic> <partition> <offset> <size>
        <topic>             ::= <topic-length><string>
        <topic-length>      ::= <int16>
        <partition>         ::= <int32>
        <offset>            ::= <int64>
        <size>              ::= <int32>

        The request-key (2) is encoded as a short (int16).
        """
        req = struct.pack('>HH', KafkaClient.MULTIFETCH_KEY, len(fetchRequests))
        for (topic, partition, offset, size) in fetchRequests:
            req += struct.pack('>H%dsiqi' % len(topic), len(topic), topic, partition, offset, size)
        return req

    @classmethod
    def encode_offset_request(cls, offsetRequest):
        """
        Encode an OffsetRequest message

        Wire Format
        ===========
        <OffsetRequest> ::= <request-key> <topic> <partition> <time> <max-offsets>
        <request-key>   ::= 4
        <topic>         ::= <topic-length><string>
        <topic-length>  ::= <int16>
        <partition>     ::= <int32>
        <time>          ::= <epoch>
        <epoch>         ::= <int64>
        <max-offsets>   ::= <int32>

        The request-key (4) is encoded as a short (int16).
        """
        (topic, partition, offset, maxOffsets) = offsetRequest
        req = struct.pack('>HH%dsiqi' % len(topic), KafkaClient.OFFSET_KEY, len(topic), topic, partition, offset, maxOffsets)
        return req

    @classmethod
    def decode_message(cls, data):
        """
        Decode a Message

        Verify crc and decode the Message. A compressed Message's payload is actually
        an encoded MessageSet. This allows Messages to be nested within Messages and 
        as such, this method will recurse.

        Params
        ======
        data, bytes

        Returns
        =======
        Generator of Messages (depth-first)
        """
        N = len(data)
        (magic,) = struct.unpack('>B', data[0:1])
        if magic == 0:
            # version 0
            (crc,) = struct.unpack('>i', data[1:5])
            payload = data[5:N]
            assert zlib.crc32(payload) == crc
            msg = Message(magic, None, crc, payload)
            log.debug("Got v0 Message, %s", msg)
            yield msg
        elif magic == 1:
            # version 1
            (att, crc) = struct.unpack('>Bi', data[1:6])
            payload = data[6:N]
            assert zlib.crc32(payload) == crc
            if att & KafkaClient.ATTRIBUTE_CODEC_MASK == 0:
                # Uncompressed, just a single Message
                msg = Message(magic, att, crc, payload)
                log.debug("Got v1 Message, %s", msg)
                yield msg
            elif att & KafkaClient.ATTRIBUTE_CODEC_MASK == 1:
                # Gzip encoded Message
                gz = gzip_decode(payload)
                (msgs, _) = cls.read_message_set(gz)
                for msg in msgs:
                    yield msg
            elif att & KafkaClient.ATTRIBUTE_CODEC_MASK == 2:
                # Snappy encoded Message
                raise NotImplementedError("Snappy codec is not yet supported")
            else:
                raise RuntimeError("Unsupported compression type: %d" % (att & KafkaClient.ATTRIBUTE_CODEC_MASK))

    @classmethod
    def read_message_set(cls, data):
        """
        Read a MessageSet

        This method reads through the given bytes and decodes the length-prefixed Messages. It will short 
        circuit if there are insufficient bytes to read a whole Message. This allows callers to determine
        the next valid offset
        
        Params
        ======
        data: bytes to read

        Returns
        =======
        tuple of (list(Message), read), where read is how many bytes were read

        Wire Format
        ===========
        <MessageSet> ::= <len> <Message> [ <MessageSet> ]
        <len>        ::= <int32>

        len is the length of the proceeding Message
        """
        
        # Read the MessageSet
        cur = 0
        msgs = []
        size = len(data)
        while cur < size:
            # Make sure we can read this Message
            if (cur + 4) > size:
                # Not enough bytes to read the length
                if len(msgs) == 0:
                    raise Exception("Message underflow. Did not request enough bytes to consume a single message")
                else:
                    log.debug("Not enough data to read header of next message")
                    break
            (N,) = struct.unpack('>i', data[cur:(cur+4)])
            if (cur + N + 4) > size:
                # Not enough bytes to read this whole Message
                log.debug("Not enough data to read next message")
                break
            else:
                cur += 4

            # Decode the message(s)
            for m in cls.decode_message(data[cur:cur+N]):
                msgs.append(m)

            # Advance the cursor
            cur += N
            
        # Return the retrieved messages and the cursor position
        return (msgs, cur)

    #########################
    #   Advanced User API   #  
    #########################

    @classmethod
    def create_message(cls, payload):
        """
        Create a standard Message

        Params
        ======
        payload, bytes

        Returns
        =======
        A Message tuple
        """
        return Message(1, 0, zlib.crc32(payload), payload)

    @classmethod
    def create_gzip_message(cls, *payloads):
        """
        Create a Gzip encoded Message

        Params
        ======
        payloads, list of messages (bytes) to be encoded

        Returns
        =======
        A Message tuple
        """
        messages = [cls.create_message(payload) for payload in payloads]
        message_set = cls.encode_message_set(messages)
        gzipped = gzip_encode(message_set) 
        return Message(1, 0x00 | (KafkaClient.ATTRIBUTE_CODEC_MASK & 0x01), zlib.crc32(gzipped), gzipped)

    def send_message_set(self, produceRequest):
        """
        Send a ProduceRequest
        
        Params
        ======
        produceRequest: ProduceRequest
        """
        req = length_prefix_message(self.encode_produce_request(produceRequest))
        log.debug("Sending %d bytes to Kafka: %r", len(req), req)
        sent = self._sock.sendall(req) 
        if sent == 0:
            raise RuntimeError("Kafka went away")

    def send_multi_message_set(self, produceRequests):
        """
        Send a MultiProduceRequest
        
        Params
        ======
        produceRequests: list of ProduceRequest
        """
        req = length_prefix_message(self.encode_multi_produce_request(produceRequests))
        log.debug("Sending %d bytes to Kafka", len(req))
        sent = self._sock.sendall(req) 
        if sent == 0:
            raise RuntimeError("Kafka went away")

    def get_message_set(self, fetchRequest):
        """
        Send a FetchRequest and return the Messages

        Params
        ======
        fetchRequest: FetchRequest named tuple

        Returns
        =======
        A tuple of (list(Message), FetchRequest). This FetchRequest will have the offset 
        starting at the next message. 
        """

        req = length_prefix_message(self.encode_fetch_request(fetchRequest))
        log.debug("Sending %d bytes to Kafka", len(req))
        sent = self._sock.sendall(req) 
        if sent == 0:
            raise RuntimeError("Kafka went away")
        data = self._consume_response()
        (messages, read) = self.read_message_set(data)

        # Return the retrieved messages and the next FetchRequest
        return (messages, FetchRequest(fetchRequest.topic, fetchRequest.partition, (fetchRequest.offset + read), fetchRequest.size))

    def get_multi_message_set(self, fetchRequests):
        """
        Send several FetchRequests in a single pipelined request.

        Params
        ======
        fetchRequests: list of FetchRequest 

        Returns
        =======
        list of tuples of (list(Message), FetchRequest). This FetchRequest will have the offset 
        starting at the next message. 

        Wire Format
        ===========
        <MultiFetchResponse> ::= <MultiMessageSet>
        <MultiMessageSet>    ::= <MultiMessage> [ <MultiMessageSet> ]
        <MultiMessage>       ::= <len> 0 <MessageSet>
        <len>                ::= <int32>
        """
        req = length_prefix_message(self.encode_multi_fetch_request(fetchRequests))
        log.debug("Sending %d bytes to Kafka", len(req))
        sent = self._sock.sendall(req) 
        if sent == 0:
            raise RuntimeError("Kafka went away")
        data = self._consume_response()
        cur = 0
        responses = []
        for request in fetchRequests:
            (size, _) = struct.unpack('>iH', data[cur:(cur+6)])
            cur += 6
            (messages, read) = self.read_message_set(data[cur:(cur+size-2)])
            cur += size-2
            responses.append((messages, FetchRequest(request.topic, request.partition, request.offset+read, request.size)))
        return responses

    def get_offsets(self, offsetRequest):
        """
        Get the offsets for a topic

        Params
        ======
        offsetRequest: OffsetRequest
    
        Returns
        =======
        offsets: tuple of offsets
        
        Wire Format
        ===========
        <OffsetResponse> ::= <num> [ <offsets> ]
        <num>            ::= <int32>
        <offsets>        ::= <offset> [ <offsets> ]
        <offset>         ::= <int64>

        """
        req = length_prefix_message(self.encode_offset_request(offsetRequest))
        log.debug("Sending OffsetRequest of %d bytes to Kafka", len(req))
        sent = self._sock.sendall(req) 
        if sent == 0:
            raise RuntimeError("Kafka went away")

        data = self._consume_response()
        (num,) = struct.unpack('>i', data[0:4])
        offsets = struct.unpack('>%dq' % num, data[4:])
        return offsets

    #######################
    #   Simple User API   #
    #######################

    def send_messages_simple(self, topic, *payloads):
        """
        Send one or more strings to Kafka

        Params
        ======
        topic: string
        payloads: strings
        """
        messages = tuple([self.create_message(payload) for payload in payloads])
        self.send_message_set(ProduceRequest(topic, -1, messages))

    def iter_messages(self, topic, partition, offset, size, auto=True):
        """
        Helper method that iterates through all messages starting at the offset
        in the given FetchRequest

        Params
        ======
        topic: string
        partition: int
        offset: int, offset to start consuming from
        size: number of bytes to initially fetch
        auto: boolean, indicates whether or not to automatically make the next 
              FetchRequest for more messages

        Returns
        =======
        A generator of Messages
        """
        fetchRequest = FetchRequest(topic, partition, offset, size)
        while True:
            lastOffset = fetchRequest.offset
            (messages, fetchRequest) = self.get_message_set(fetchRequest)
            if fetchRequest.offset == lastOffset:
                break
            for message in messages:
                yield message
            if auto == False:
                break

    def close(self):
        self._sock.close()
