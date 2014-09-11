from contextlib import contextmanager
import struct

import six
from mock import patch, sentinel
from . import unittest

from kafka.codec import has_snappy, gzip_decode, snappy_decode
from kafka.common import (
    OffsetRequest, OffsetCommitRequest, OffsetFetchRequest,
    OffsetResponse, OffsetCommitResponse, OffsetFetchResponse,
    ProduceRequest, FetchRequest, Message, ChecksumError,
    ProduceResponse, FetchResponse, OffsetAndMessage,
    BrokerMetadata, TopicMetadata, PartitionMetadata, TopicAndPartition,
    KafkaUnavailableError, UnsupportedCodecError, ConsumerFetchSizeTooSmall,
    ProtocolError
)
from kafka.protocol import (
    ATTRIBUTE_CODEC_MASK, CODEC_NONE, CODEC_GZIP, CODEC_SNAPPY, KafkaProtocol,
    create_message, create_gzip_message, create_snappy_message,
    create_message_set
)

class TestProtocol(unittest.TestCase):
    def test_create_message(self):
        payload = "test"
        key = "key"
        msg = create_message(payload, key)
        self.assertEqual(msg.magic, 0)
        self.assertEqual(msg.attributes, 0)
        self.assertEqual(msg.key, key)
        self.assertEqual(msg.value, payload)

    def test_create_gzip(self):
        payloads = [b"v1", b"v2"]
        msg = create_gzip_message(payloads)
        self.assertEqual(msg.magic, 0)
        self.assertEqual(msg.attributes, ATTRIBUTE_CODEC_MASK & CODEC_GZIP)
        self.assertEqual(msg.key, None)
        # Need to decode to check since gzipped payload is non-deterministic
        decoded = gzip_decode(msg.value)
        expect = b"".join([
            struct.pack(">q", 0),          # MsgSet offset
            struct.pack(">i", 16),         # MsgSet size
            struct.pack(">i", 1285512130), # CRC
            struct.pack(">bb", 0, 0),      # Magic, flags
            struct.pack(">i", -1),         # -1 indicates a null key
            struct.pack(">i", 2),          # Msg length (bytes)
            b"v1",                         # Message contents

            struct.pack(">q", 0),          # MsgSet offset
            struct.pack(">i", 16),         # MsgSet size
            struct.pack(">i", -711587208), # CRC
            struct.pack(">bb", 0, 0),      # Magic, flags
            struct.pack(">i", -1),         # -1 indicates a null key
            struct.pack(">i", 2),          # Msg length (bytes)
            b"v2",                         # Message contents
        ])

        self.assertEqual(decoded, expect)

    @unittest.skipUnless(has_snappy(), "Snappy not available")
    def test_create_snappy(self):
        payloads = [b"v1", b"v2"]
        msg = create_snappy_message(payloads)
        self.assertEqual(msg.magic, 0)
        self.assertEqual(msg.attributes, ATTRIBUTE_CODEC_MASK & CODEC_SNAPPY)
        self.assertEqual(msg.key, None)
        decoded = snappy_decode(msg.value)
        expect = b"".join([
            struct.pack(">q", 0),          # MsgSet offset
            struct.pack(">i", 16),         # MsgSet size
            struct.pack(">i", 1285512130), # CRC
            struct.pack(">bb", 0, 0),      # Magic, flags
            struct.pack(">i", -1),         # -1 indicates a null key
            struct.pack(">i", 2),          # Msg length (bytes)
            b"v1",                         # Message contents

            struct.pack(">q", 0),          # MsgSet offset
            struct.pack(">i", 16),         # MsgSet size
            struct.pack(">i", -711587208), # CRC
            struct.pack(">bb", 0, 0),      # Magic, flags
            struct.pack(">i", -1),         # -1 indicates a null key
            struct.pack(">i", 2),          # Msg length (bytes)
            b"v2",                         # Message contents
        ])

        self.assertEqual(decoded, expect)

    def test_encode_message_header(self):
        expect = b"".join([
            struct.pack(">h", 10),             # API Key
            struct.pack(">h", 0),              # API Version
            struct.pack(">i", 4),              # Correlation Id
            struct.pack(">h", len("client3")), # Length of clientId
            b"client3",                         # ClientId
        ])

        encoded = KafkaProtocol._encode_message_header(b"client3", 4, 10)
        self.assertEqual(encoded, expect)

    def test_encode_message(self):
        message = create_message(b"test", b"key")
        encoded = KafkaProtocol._encode_message(message)
        expect = b"".join([
            struct.pack(">i", -1427009701), # CRC
            struct.pack(">bb", 0, 0),       # Magic, flags
            struct.pack(">i", 3),           # Length of key
            b"key",                          # key
            struct.pack(">i", 4),           # Length of value
            b"test",                         # value
        ])

        self.assertEqual(encoded, expect)

    def test_decode_message(self):
        encoded = b"".join([
            struct.pack(">i", -1427009701), # CRC
            struct.pack(">bb", 0, 0),       # Magic, flags
            struct.pack(">i", 3),           # Length of key
            b"key",                         # key
            struct.pack(">i", 4),           # Length of value
            b"test",                        # value
        ])

        offset = 10
        (returned_offset, decoded_message) = list(KafkaProtocol._decode_message(encoded, offset))[0]

        self.assertEqual(returned_offset, offset)
        self.assertEqual(decoded_message, create_message(b"test", b"key"))

    def test_encode_message_failure(self):
        with self.assertRaises(ProtocolError):
            KafkaProtocol._encode_message(Message(1, 0, "key", "test"))

    def test_encode_message_set(self):
        message_set = [
            create_message(b"v1", b"k1"),
            create_message(b"v2", b"k2")
        ]

        encoded = KafkaProtocol._encode_message_set(message_set)
        expect = b"".join([
            struct.pack(">q", 0),          # MsgSet Offset
            struct.pack(">i", 18),         # Msg Size
            struct.pack(">i", 1474775406), # CRC
            struct.pack(">bb", 0, 0),      # Magic, flags
            struct.pack(">i", 2),          # Length of key
            b"k1",                          # Key
            struct.pack(">i", 2),          # Length of value
            b"v1",                          # Value

            struct.pack(">q", 0),          # MsgSet Offset
            struct.pack(">i", 18),         # Msg Size
            struct.pack(">i", -16383415),  # CRC
            struct.pack(">bb", 0, 0),      # Magic, flags
            struct.pack(">i", 2),          # Length of key
            b"k2",                          # Key
            struct.pack(">i", 2),          # Length of value
            b"v2",                          # Value
        ])

        self.assertEqual(encoded, expect)

    def test_decode_message_set(self):
        encoded = b"".join([
            struct.pack(">q", 0),          # MsgSet Offset
            struct.pack(">i", 18),         # Msg Size
            struct.pack(">i", 1474775406), # CRC
            struct.pack(">bb", 0, 0),      # Magic, flags
            struct.pack(">i", 2),          # Length of key
            b"k1",                         # Key
            struct.pack(">i", 2),          # Length of value
            b"v1",                         # Value

            struct.pack(">q", 1),          # MsgSet Offset
            struct.pack(">i", 18),         # Msg Size
            struct.pack(">i", -16383415),  # CRC
            struct.pack(">bb", 0, 0),      # Magic, flags
            struct.pack(">i", 2),          # Length of key
            b"k2",                         # Key
            struct.pack(">i", 2),          # Length of value
            b"v2",                         # Value
        ])

        msgs = list(KafkaProtocol._decode_message_set_iter(encoded))
        self.assertEqual(len(msgs), 2)
        msg1, msg2 = msgs

        returned_offset1, decoded_message1 = msg1
        returned_offset2, decoded_message2 = msg2

        self.assertEqual(returned_offset1, 0)
        self.assertEqual(decoded_message1, create_message(b"v1", b"k1"))

        self.assertEqual(returned_offset2, 1)
        self.assertEqual(decoded_message2, create_message(b"v2", b"k2"))

    def test_decode_message_gzip(self):
        gzip_encoded = (b'\xc0\x11\xb2\xf0\x00\x01\xff\xff\xff\xff\x00\x00\x000'
                        b'\x1f\x8b\x08\x00\xa1\xc1\xc5R\x02\xffc`\x80\x03\x01'
                        b'\x9f\xf9\xd1\x87\x18\x18\xfe\x03\x01\x90\xc7Tf\xc8'
                        b'\x80$wu\x1aW\x05\x92\x9c\x11\x00z\xc0h\x888\x00\x00'
                        b'\x00')
        offset = 11
        messages = list(KafkaProtocol._decode_message(gzip_encoded, offset))

        self.assertEqual(len(messages), 2)
        msg1, msg2 = messages

        returned_offset1, decoded_message1 = msg1
        self.assertEqual(returned_offset1, 0)
        self.assertEqual(decoded_message1, create_message(b"v1"))

        returned_offset2, decoded_message2 = msg2
        self.assertEqual(returned_offset2, 0)
        self.assertEqual(decoded_message2, create_message(b"v2"))

    @unittest.skipUnless(has_snappy(), "Snappy not available")
    def test_decode_message_snappy(self):
        snappy_encoded = (b'\xec\x80\xa1\x95\x00\x02\xff\xff\xff\xff\x00\x00'
                          b'\x00,8\x00\x00\x19\x01@\x10L\x9f[\xc2\x00\x00\xff'
                          b'\xff\xff\xff\x00\x00\x00\x02v1\x19\x1bD\x00\x10\xd5'
                          b'\x96\nx\x00\x00\xff\xff\xff\xff\x00\x00\x00\x02v2')
        offset = 11
        messages = list(KafkaProtocol._decode_message(snappy_encoded, offset))
        self.assertEqual(len(messages), 2)

        msg1, msg2 = messages

        returned_offset1, decoded_message1 = msg1
        self.assertEqual(returned_offset1, 0)
        self.assertEqual(decoded_message1, create_message(b"v1"))

        returned_offset2, decoded_message2 = msg2
        self.assertEqual(returned_offset2, 0)
        self.assertEqual(decoded_message2, create_message(b"v2"))

    def test_decode_message_checksum_error(self):
        invalid_encoded_message = b"This is not a valid encoded message"
        iter = KafkaProtocol._decode_message(invalid_encoded_message, 0)
        self.assertRaises(ChecksumError, list, iter)

    # NOTE: The error handling in _decode_message_set_iter() is questionable.
    # If it's modified, the next two tests might need to be fixed.
    def test_decode_message_set_fetch_size_too_small(self):
        with self.assertRaises(ConsumerFetchSizeTooSmall):
            list(KafkaProtocol._decode_message_set_iter('a'))

    def test_decode_message_set_stop_iteration(self):
        encoded = b"".join([
            struct.pack(">q", 0),          # MsgSet Offset
            struct.pack(">i", 18),         # Msg Size
            struct.pack(">i", 1474775406), # CRC
            struct.pack(">bb", 0, 0),      # Magic, flags
            struct.pack(">i", 2),          # Length of key
            b"k1",                         # Key
            struct.pack(">i", 2),          # Length of value
            b"v1",                         # Value

            struct.pack(">q", 1),          # MsgSet Offset
            struct.pack(">i", 18),         # Msg Size
            struct.pack(">i", -16383415),  # CRC
            struct.pack(">bb", 0, 0),      # Magic, flags
            struct.pack(">i", 2),          # Length of key
            b"k2",                         # Key
            struct.pack(">i", 2),          # Length of value
            b"v2",                         # Value
            b"@1$%(Y!",                    # Random padding
        ])

        msgs = list(KafkaProtocol._decode_message_set_iter(encoded))
        self.assertEqual(len(msgs), 2)
        msg1, msg2 = msgs

        returned_offset1, decoded_message1 = msg1
        returned_offset2, decoded_message2 = msg2

        self.assertEqual(returned_offset1, 0)
        self.assertEqual(decoded_message1, create_message(b"v1", b"k1"))

        self.assertEqual(returned_offset2, 1)
        self.assertEqual(decoded_message2, create_message(b"v2", b"k2"))

    def test_encode_produce_request(self):
        requests = [
            ProduceRequest(b"topic1", 0, [
                create_message(b"a"),
                create_message(b"b")
            ]),
            ProduceRequest(b"topic2", 1, [
                create_message(b"c")
            ])
        ]

        msg_a_binary = KafkaProtocol._encode_message(create_message(b"a"))
        msg_b_binary = KafkaProtocol._encode_message(create_message(b"b"))
        msg_c_binary = KafkaProtocol._encode_message(create_message(b"c"))

        header = b"".join([
            struct.pack('>i', 0x94),                   # The length of the message overall
            struct.pack('>h', 0),                      # Msg Header, Message type = Produce
            struct.pack('>h', 0),                      # Msg Header, API version
            struct.pack('>i', 2),                      # Msg Header, Correlation ID
            struct.pack('>h7s', 7, b"client1"),        # Msg Header, The client ID
            struct.pack('>h', 2),                      # Num acks required
            struct.pack('>i', 100),                    # Request Timeout
            struct.pack('>i', 2),                      # The number of requests
        ])

        total_len = len(msg_a_binary) + len(msg_b_binary)
        topic1 = b"".join([
            struct.pack('>h6s', 6, b'topic1'),         # The topic1
            struct.pack('>i', 1),                      # One message set
            struct.pack('>i', 0),                      # Partition 0
            struct.pack('>i', total_len + 24),         # Size of the incoming message set
            struct.pack('>q', 0),                      # No offset specified
            struct.pack('>i', len(msg_a_binary)),      # Length of message
            msg_a_binary,                              # Actual message
            struct.pack('>q', 0),                      # No offset specified
            struct.pack('>i', len(msg_b_binary)),      # Length of message
            msg_b_binary,                              # Actual message
        ])

        topic2 = b"".join([
            struct.pack('>h6s', 6, b'topic2'),         # The topic1
            struct.pack('>i', 1),                      # One message set
            struct.pack('>i', 1),                      # Partition 1
            struct.pack('>i', len(msg_c_binary) + 12), # Size of the incoming message set
            struct.pack('>q', 0),                      # No offset specified
            struct.pack('>i', len(msg_c_binary)),      # Length of message
            msg_c_binary,                              # Actual message
        ])

        expected1 = b"".join([ header, topic1, topic2 ])
        expected2 = b"".join([ header, topic2, topic1 ])

        encoded = KafkaProtocol.encode_produce_request(b"client1", 2, requests, 2, 100)
        self.assertIn(encoded, [ expected1, expected2 ])

    def test_decode_produce_response(self):
        t1 = b"topic1"
        t2 = b"topic2"
        _long = int
        if six.PY2:
            _long = long
        encoded = struct.pack('>iih%dsiihqihqh%dsiihq' % (len(t1), len(t2)),
                              2, 2, len(t1), t1, 2, 0, 0, _long(10), 1, 1, _long(20),
                              len(t2), t2, 1, 0, 0, _long(30))
        responses = list(KafkaProtocol.decode_produce_response(encoded))
        self.assertEqual(responses,
                         [ProduceResponse(t1, 0, 0, _long(10)),
                          ProduceResponse(t1, 1, 1, _long(20)),
                          ProduceResponse(t2, 0, 0, _long(30))])

    def test_encode_fetch_request(self):
        requests = [
            FetchRequest(b"topic1", 0, 10, 1024),
            FetchRequest(b"topic2", 1, 20, 100),
        ]

        header = b"".join([
            struct.pack('>i', 89),             # The length of the message overall
            struct.pack('>h', 1),              # Msg Header, Message type = Fetch
            struct.pack('>h', 0),              # Msg Header, API version
            struct.pack('>i', 3),              # Msg Header, Correlation ID
            struct.pack('>h7s', 7, b"client1"),# Msg Header, The client ID
            struct.pack('>i', -1),             # Replica Id
            struct.pack('>i', 2),              # Max wait time
            struct.pack('>i', 100),            # Min bytes
            struct.pack('>i', 2),              # Num requests
        ])

        topic1 = b"".join([
            struct.pack('>h6s', 6, b'topic1'),# Topic
            struct.pack('>i', 1),             # Num Payloads
            struct.pack('>i', 0),             # Partition 0
            struct.pack('>q', 10),            # Offset
            struct.pack('>i', 1024),          # Max Bytes
        ])

        topic2 = b"".join([
            struct.pack('>h6s', 6, b'topic2'),# Topic
            struct.pack('>i', 1),             # Num Payloads
            struct.pack('>i', 1),             # Partition 0
            struct.pack('>q', 20),            # Offset
            struct.pack('>i', 100),           # Max Bytes
        ])

        expected1 = b"".join([ header, topic1, topic2 ])
        expected2 = b"".join([ header, topic2, topic1 ])

        encoded = KafkaProtocol.encode_fetch_request(b"client1", 3, requests, 2, 100)
        self.assertIn(encoded, [ expected1, expected2 ])

    def test_decode_fetch_response(self):
        t1 = b"topic1"
        t2 = b"topic2"
        msgs = [create_message(msg)
                for msg in [b"message1", b"hi", b"boo", b"foo", b"so fun!"]]
        ms1 = KafkaProtocol._encode_message_set([msgs[0], msgs[1]])
        ms2 = KafkaProtocol._encode_message_set([msgs[2]])
        ms3 = KafkaProtocol._encode_message_set([msgs[3], msgs[4]])

        encoded = struct.pack('>iih%dsiihqi%dsihqi%dsh%dsiihqi%ds' %
                              (len(t1), len(ms1), len(ms2), len(t2), len(ms3)),
                              4, 2, len(t1), t1, 2, 0, 0, 10, len(ms1), ms1, 1,
                              1, 20, len(ms2), ms2, len(t2), t2, 1, 0, 0, 30,
                              len(ms3), ms3)

        responses = list(KafkaProtocol.decode_fetch_response(encoded))
        def expand_messages(response):
            return FetchResponse(response.topic, response.partition,
                                 response.error, response.highwaterMark,
                                 list(response.messages))

        expanded_responses = list(map(expand_messages, responses))
        expect = [FetchResponse(t1, 0, 0, 10, [OffsetAndMessage(0, msgs[0]),
                                               OffsetAndMessage(0, msgs[1])]),
                  FetchResponse(t1, 1, 1, 20, [OffsetAndMessage(0, msgs[2])]),
                  FetchResponse(t2, 0, 0, 30, [OffsetAndMessage(0, msgs[3]),
                                               OffsetAndMessage(0, msgs[4])])]
        self.assertEqual(expanded_responses, expect)

    def test_encode_metadata_request_no_topics(self):
        expected = b"".join([
            struct.pack(">i", 17),         # Total length of the request
            struct.pack('>h', 3),          # API key metadata fetch
            struct.pack('>h', 0),          # API version
            struct.pack('>i', 4),          # Correlation ID
            struct.pack('>h3s', 3, b"cid"),# The client ID
            struct.pack('>i', 0),          # No topics, give all the data!
        ])

        encoded = KafkaProtocol.encode_metadata_request(b"cid", 4)

        self.assertEqual(encoded, expected)

    def test_encode_metadata_request_with_topics(self):
        expected = b"".join([
            struct.pack(">i", 25),         # Total length of the request
            struct.pack('>h', 3),          # API key metadata fetch
            struct.pack('>h', 0),          # API version
            struct.pack('>i', 4),          # Correlation ID
            struct.pack('>h3s', 3, b"cid"),# The client ID
            struct.pack('>i', 2),          # Number of topics in the request
            struct.pack('>h2s', 2, b"t1"), # Topic "t1"
            struct.pack('>h2s', 2, b"t2"), # Topic "t2"
        ])

        encoded = KafkaProtocol.encode_metadata_request(b"cid", 4, [b"t1", b"t2"])

        self.assertEqual(encoded, expected)

    def _create_encoded_metadata_response(self, brokers, topics):
        encoded = []
        encoded.append(struct.pack('>ii', 3, len(brokers)))
        for broker in brokers:
            encoded.append(struct.pack('>ih%dsi' % len(broker.host),
                                       broker.nodeId, len(broker.host),
                                       broker.host, broker.port))

        encoded.append(struct.pack('>i', len(topics)))
        for topic in topics:
            encoded.append(struct.pack('>hh%dsi' % len(topic.topic),
                                       topic.error, len(topic.topic),
                                       topic.topic, len(topic.partitions)))
            for metadata in topic.partitions:
                encoded.append(struct.pack('>hiii', metadata.error,
                                           metadata.partition, metadata.leader,
                                           len(metadata.replicas)))
                if len(metadata.replicas) > 0:
                    encoded.append(struct.pack('>%di' % len(metadata.replicas),
                                               *metadata.replicas))

                encoded.append(struct.pack('>i', len(metadata.isr)))
                if len(metadata.isr) > 0:
                    encoded.append(struct.pack('>%di' % len(metadata.isr),
                                               *metadata.isr))
        return b''.join(encoded)

    def test_decode_metadata_response(self):
        node_brokers = [
            BrokerMetadata(0, b"brokers1.kafka.rdio.com", 1000),
            BrokerMetadata(1, b"brokers1.kafka.rdio.com", 1001),
            BrokerMetadata(3, b"brokers2.kafka.rdio.com", 1000)
        ]

        topic_partitions = [
            TopicMetadata(b"topic1", 0, [
                PartitionMetadata(b"topic1", 0, 1, (0, 2), (2,), 0),
                PartitionMetadata(b"topic1", 1, 3, (0, 1), (0, 1), 1)
            ]),
            TopicMetadata(b"topic2", 1, [
                PartitionMetadata(b"topic2", 0, 0, (), (), 0),
            ]),
        ]
        encoded = self._create_encoded_metadata_response(node_brokers,
                                                         topic_partitions)
        decoded = KafkaProtocol.decode_metadata_response(encoded)
        self.assertEqual(decoded, (node_brokers, topic_partitions))

    def test_encode_offset_request(self):
        expected = b"".join([
            struct.pack(">i", 21),         # Total length of the request
            struct.pack('>h', 2),          # Message type = offset fetch
            struct.pack('>h', 0),          # API version
            struct.pack('>i', 4),          # Correlation ID
            struct.pack('>h3s', 3, b"cid"), # The client ID
            struct.pack('>i', -1),         # Replica Id
            struct.pack('>i', 0),          # No topic/partitions
        ])

        encoded = KafkaProtocol.encode_offset_request(b"cid", 4)

        self.assertEqual(encoded, expected)

    def test_encode_offset_request__no_payload(self):
        expected = b"".join([
            struct.pack(">i", 65),            # Total length of the request

            struct.pack('>h', 2),             # Message type = offset fetch
            struct.pack('>h', 0),             # API version
            struct.pack('>i', 4),             # Correlation ID
            struct.pack('>h3s', 3, b"cid"),   # The client ID
            struct.pack('>i', -1),            # Replica Id
            struct.pack('>i', 1),             # Num topics
            struct.pack(">h6s", 6, b"topic1"),# Topic for the request
            struct.pack(">i", 2),             # Two partitions

            struct.pack(">i", 3),             # Partition 3
            struct.pack(">q", -1),            # No time offset
            struct.pack(">i", 1),             # One offset requested

            struct.pack(">i", 4),             # Partition 3
            struct.pack(">q", -1),            # No time offset
            struct.pack(">i", 1),             # One offset requested
        ])

        encoded = KafkaProtocol.encode_offset_request(b"cid", 4, [
            OffsetRequest(b'topic1', 3, -1, 1),
            OffsetRequest(b'topic1', 4, -1, 1),
        ])

        self.assertEqual(encoded, expected)

    def test_decode_offset_response(self):
        encoded = b"".join([
            struct.pack(">i", 42),            # Correlation ID
            struct.pack(">i", 1),             # One topics
            struct.pack(">h6s", 6, b"topic1"),# First topic
            struct.pack(">i", 2),             # Two partitions

            struct.pack(">i", 2),             # Partition 2
            struct.pack(">h", 0),             # No error
            struct.pack(">i", 1),             # One offset
            struct.pack(">q", 4),             # Offset 4

            struct.pack(">i", 4),             # Partition 4
            struct.pack(">h", 0),             # No error
            struct.pack(">i", 1),             # One offset
            struct.pack(">q", 8),             # Offset 8
        ])

        results = KafkaProtocol.decode_offset_response(encoded)
        self.assertEqual(set(results), set([
            OffsetResponse(topic = b'topic1', partition = 2, error = 0, offsets=(4,)),
            OffsetResponse(topic = b'topic1', partition = 4, error = 0, offsets=(8,)),
        ]))

    def test_encode_offset_commit_request(self):
        header = b"".join([
            struct.pack('>i', 99),               # Total message length

            struct.pack('>h', 8),                # Message type = offset commit
            struct.pack('>h', 0),                # API version
            struct.pack('>i', 42),               # Correlation ID
            struct.pack('>h9s', 9, b"client_id"),# The client ID
            struct.pack('>h8s', 8, b"group_id"), # The group to commit for
            struct.pack('>i', 2),                # Num topics
        ])

        topic1 = b"".join([
            struct.pack(">h6s", 6, b"topic1"),   # Topic for the request
            struct.pack(">i", 2),                # Two partitions
            struct.pack(">i", 0),                # Partition 0
            struct.pack(">q", 123),              # Offset 123
            struct.pack(">h", -1),               # Null metadata
            struct.pack(">i", 1),                # Partition 1
            struct.pack(">q", 234),              # Offset 234
            struct.pack(">h", -1),               # Null metadata
        ])

        topic2 = b"".join([
            struct.pack(">h6s", 6, b"topic2"),   # Topic for the request
            struct.pack(">i", 1),                # One partition
            struct.pack(">i", 2),                # Partition 2
            struct.pack(">q", 345),              # Offset 345
            struct.pack(">h", -1),               # Null metadata
        ])

        expected1 = b"".join([ header, topic1, topic2 ])
        expected2 = b"".join([ header, topic2, topic1 ])

        encoded = KafkaProtocol.encode_offset_commit_request(b"client_id", 42, b"group_id", [
            OffsetCommitRequest(b"topic1", 0, 123, None),
            OffsetCommitRequest(b"topic1", 1, 234, None),
            OffsetCommitRequest(b"topic2", 2, 345, None),
        ])

        self.assertIn(encoded, [ expected1, expected2 ])

    def test_decode_offset_commit_response(self):
        encoded = b"".join([
            struct.pack(">i", 42),            # Correlation ID
            struct.pack(">i", 1),             # One topic
            struct.pack(">h6s", 6, b"topic1"),# First topic
            struct.pack(">i", 2),             # Two partitions

            struct.pack(">i", 2),             # Partition 2
            struct.pack(">h", 0),             # No error

            struct.pack(">i", 4),             # Partition 4
            struct.pack(">h", 0),             # No error
        ])

        results = KafkaProtocol.decode_offset_commit_response(encoded)
        self.assertEqual(set(results), set([
            OffsetCommitResponse(topic = b'topic1', partition = 2, error = 0),
            OffsetCommitResponse(topic = b'topic1', partition = 4, error = 0),
        ]))

    def test_encode_offset_fetch_request(self):
        header = b"".join([
            struct.pack('>i', 69),               # Total message length
            struct.pack('>h', 9),                # Message type = offset fetch
            struct.pack('>h', 0),                # API version
            struct.pack('>i', 42),               # Correlation ID
            struct.pack('>h9s', 9, b"client_id"),# The client ID
            struct.pack('>h8s', 8, b"group_id"), # The group to commit for
            struct.pack('>i', 2),                # Num topics
        ])

        topic1 = b"".join([
            struct.pack(">h6s", 6, b"topic1"),   # Topic for the request
            struct.pack(">i", 2),                # Two partitions
            struct.pack(">i", 0),                # Partition 0
            struct.pack(">i", 1),                # Partition 1
        ])

        topic2 = b"".join([
            struct.pack(">h6s", 6, b"topic2"),   # Topic for the request
            struct.pack(">i", 1),                # One partitions
            struct.pack(">i", 2),                # Partition 2
        ])

        expected1 = b"".join([ header, topic1, topic2 ])
        expected2 = b"".join([ header, topic2, topic1 ])

        encoded = KafkaProtocol.encode_offset_fetch_request(b"client_id", 42, b"group_id", [
            OffsetFetchRequest(b"topic1", 0),
            OffsetFetchRequest(b"topic1", 1),
            OffsetFetchRequest(b"topic2", 2),
        ])

        self.assertIn(encoded, [ expected1, expected2 ])

    def test_decode_offset_fetch_response(self):
        encoded = b"".join([
            struct.pack(">i", 42),            # Correlation ID
            struct.pack(">i", 1),             # One topics
            struct.pack(">h6s", 6, b"topic1"),# First topic
            struct.pack(">i", 2),             # Two partitions

            struct.pack(">i", 2),             # Partition 2
            struct.pack(">q", 4),             # Offset 4
            struct.pack(">h4s", 4, b"meta"),  # Metadata
            struct.pack(">h", 0),             # No error

            struct.pack(">i", 4),             # Partition 4
            struct.pack(">q", 8),             # Offset 8
            struct.pack(">h4s", 4, b"meta"),  # Metadata
            struct.pack(">h", 0),             # No error
        ])

        results = KafkaProtocol.decode_offset_fetch_response(encoded)
        self.assertEqual(set(results), set([
            OffsetFetchResponse(topic = b'topic1', partition = 2, offset = 4, error = 0, metadata = b"meta"),
            OffsetFetchResponse(topic = b'topic1', partition = 4, offset = 8, error = 0, metadata = b"meta"),
        ]))

    @contextmanager
    def mock_create_message_fns(self):
        import kafka.protocol
        with patch.object(kafka.protocol, "create_message",
                               return_value=sentinel.message):
            with patch.object(kafka.protocol, "create_gzip_message",
                                   return_value=sentinel.gzip_message):
                with patch.object(kafka.protocol, "create_snappy_message",
                                       return_value=sentinel.snappy_message):
                    yield

    def test_create_message_set(self):
        messages = [1, 2, 3]

        # Default codec is CODEC_NONE. Expect list of regular messages.
        expect = [sentinel.message] * len(messages)
        with self.mock_create_message_fns():
            message_set = create_message_set(messages)
        self.assertEqual(message_set, expect)

        # CODEC_NONE: Expect list of regular messages.
        expect = [sentinel.message] * len(messages)
        with self.mock_create_message_fns():
            message_set = create_message_set(messages, CODEC_NONE)
        self.assertEqual(message_set, expect)

        # CODEC_GZIP: Expect list of one gzip-encoded message.
        expect = [sentinel.gzip_message]
        with self.mock_create_message_fns():
            message_set = create_message_set(messages, CODEC_GZIP)
        self.assertEqual(message_set, expect)

        # CODEC_SNAPPY: Expect list of one snappy-encoded message.
        expect = [sentinel.snappy_message]
        with self.mock_create_message_fns():
            message_set = create_message_set(messages, CODEC_SNAPPY)
        self.assertEqual(message_set, expect)

        # Unknown codec should raise UnsupportedCodecError.
        with self.assertRaises(UnsupportedCodecError):
            create_message_set(messages, -1)
