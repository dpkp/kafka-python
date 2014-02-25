import os
import random
import struct
import unittest

from kafka.common import (
    ProduceRequest, FetchRequest, Message, ChecksumError,
    ConsumerFetchSizeTooSmall, ProduceResponse, FetchResponse,
    OffsetAndMessage, BrokerMetadata, PartitionMetadata
)
from kafka.codec import (
    has_gzip, has_snappy, gzip_encode, gzip_decode,
    snappy_encode, snappy_decode
)
from kafka.protocol import (
    create_gzip_message, create_message, create_snappy_message, KafkaProtocol
)

ITERATIONS = 1000
STRLEN = 100


def random_string():
    return os.urandom(random.randint(1, STRLEN))


class TestPackage(unittest.TestCase):

    def test_top_level_namespace(self):
        import kafka as kafka1
        self.assertEquals(kafka1.KafkaClient.__name__, "KafkaClient")
        self.assertEquals(kafka1.client.__name__, "kafka.client")
        self.assertEquals(kafka1.codec.__name__, "kafka.codec")

    def test_submodule_namespace(self):
        import kafka.client as client1
        self.assertEquals(client1.__name__, "kafka.client")
        self.assertEquals(client1.KafkaClient.__name__, "KafkaClient")

        from kafka import client as client2
        self.assertEquals(client2.__name__, "kafka.client")
        self.assertEquals(client2.KafkaClient.__name__, "KafkaClient")

        from kafka.client import KafkaClient as KafkaClient1
        self.assertEquals(KafkaClient1.__name__, "KafkaClient")

        from kafka.codec import gzip_encode as gzip_encode1
        self.assertEquals(gzip_encode1.__name__, "gzip_encode")

        from kafka import KafkaClient as KafkaClient2
        self.assertEquals(KafkaClient2.__name__, "KafkaClient")

        from kafka.codec import snappy_encode
        self.assertEquals(snappy_encode.__name__, "snappy_encode")


class TestCodec(unittest.TestCase):

    @unittest.skipUnless(has_gzip(), "Gzip not available")
    def test_gzip(self):
        for i in xrange(ITERATIONS):
            s1 = random_string()
            s2 = gzip_decode(gzip_encode(s1))
            self.assertEquals(s1, s2)

    @unittest.skipUnless(has_snappy(), "Snappy not available")
    def test_snappy(self):
        for i in xrange(ITERATIONS):
            s1 = random_string()
            s2 = snappy_decode(snappy_encode(s1))
            self.assertEquals(s1, s2)

    @unittest.skipUnless(has_snappy(), "Snappy not available")
    def test_snappy_detect_xerial(self):
        import kafka as kafka1
        _detect_xerial_stream = kafka1.codec._detect_xerial_stream

        header = b'\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01Some extra bytes'
        false_header = b'\x01SNAPPY\x00\x00\x00\x01\x00\x00\x00\x01'
        random_snappy = snappy_encode('SNAPPY' * 50)
        short_data = b'\x01\x02\x03\x04'

        self.assertTrue(_detect_xerial_stream(header))
        self.assertFalse(_detect_xerial_stream(b''))
        self.assertFalse(_detect_xerial_stream(b'\x00'))
        self.assertFalse(_detect_xerial_stream(false_header))
        self.assertFalse(_detect_xerial_stream(random_snappy))
        self.assertFalse(_detect_xerial_stream(short_data))

    @unittest.skipUnless(has_snappy(), "Snappy not available")
    def test_snappy_decode_xerial(self):
        header = b'\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01'
        random_snappy = snappy_encode('SNAPPY' * 50)
        block_len = len(random_snappy)
        random_snappy2 = snappy_encode('XERIAL' * 50)
        block_len2 = len(random_snappy2)

        to_test = header \
            + struct.pack('!i', block_len) + random_snappy \
            + struct.pack('!i', block_len2) + random_snappy2 \

        self.assertEquals(snappy_decode(to_test), ('SNAPPY' * 50) + ('XERIAL' * 50))

    @unittest.skipUnless(has_snappy(), "Snappy not available")
    def test_snappy_encode_xerial(self):
        to_ensure = b'\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01' + \
            '\x00\x00\x00\x18' + \
            '\xac\x02\x14SNAPPY\xfe\x06\x00\xfe\x06\x00\xfe\x06\x00\xfe\x06\x00\x96\x06\x00' + \
            '\x00\x00\x00\x18' + \
            '\xac\x02\x14XERIAL\xfe\x06\x00\xfe\x06\x00\xfe\x06\x00\xfe\x06\x00\x96\x06\x00'

        to_test = ('SNAPPY' * 50) + ('XERIAL' * 50)

        compressed = snappy_encode(to_test, xerial_compatible=True, xerial_blocksize=300)
        self.assertEquals(compressed, to_ensure)

class TestProtocol(unittest.TestCase):

    def test_create_message(self):
        payload = "test"
        key = "key"
        msg = create_message(payload, key)
        self.assertEqual(msg.magic, 0)
        self.assertEqual(msg.attributes, 0)
        self.assertEqual(msg.key, key)
        self.assertEqual(msg.value, payload)

    @unittest.skipUnless(has_gzip(), "Snappy not available")
    def test_create_gzip(self):
        payloads = ["v1", "v2"]
        msg = create_gzip_message(payloads)
        self.assertEqual(msg.magic, 0)
        self.assertEqual(msg.attributes, KafkaProtocol.ATTRIBUTE_CODEC_MASK &
                                         KafkaProtocol.CODEC_GZIP)
        self.assertEqual(msg.key, None)
        # Need to decode to check since gzipped payload is non-deterministic
        decoded = gzip_decode(msg.value)
        expect = ("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10L\x9f[\xc2"
                  "\x00\x00\xff\xff\xff\xff\x00\x00\x00\x02v1\x00\x00\x00\x00"
                  "\x00\x00\x00\x00\x00\x00\x00\x10\xd5\x96\nx\x00\x00\xff\xff"
                  "\xff\xff\x00\x00\x00\x02v2")
        self.assertEqual(decoded, expect)

    @unittest.skipUnless(has_snappy(), "Snappy not available")
    def test_create_snappy(self):
        payloads = ["v1", "v2"]
        msg = create_snappy_message(payloads)
        self.assertEqual(msg.magic, 0)
        self.assertEqual(msg.attributes, KafkaProtocol.ATTRIBUTE_CODEC_MASK &
                                         KafkaProtocol.CODEC_SNAPPY)
        self.assertEqual(msg.key, None)
        expect = ("8\x00\x00\x19\x01@\x10L\x9f[\xc2\x00\x00\xff\xff\xff\xff"
                  "\x00\x00\x00\x02v1\x19\x1bD\x00\x10\xd5\x96\nx\x00\x00\xff"
                  "\xff\xff\xff\x00\x00\x00\x02v2")
        self.assertEqual(msg.value, expect)

    def test_encode_message_header(self):
        expect = '\x00\n\x00\x00\x00\x00\x00\x04\x00\x07client3'
        encoded = KafkaProtocol._encode_message_header("client3", 4, 10)
        self.assertEqual(encoded, expect)

    def test_encode_message(self):
        message = create_message("test", "key")
        encoded = KafkaProtocol._encode_message(message)
        expect = "\xaa\xf1\x8f[\x00\x00\x00\x00\x00\x03key\x00\x00\x00\x04test"
        self.assertEqual(encoded, expect)

    def test_encode_message_failure(self):
        self.assertRaises(Exception, KafkaProtocol._encode_message,
                          Message(1, 0, "key", "test"))

    def test_encode_message_set(self):
        message_set = [create_message("v1", "k1"), create_message("v2", "k2")]
        encoded = KafkaProtocol._encode_message_set(message_set)
        expect = ("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x12W\xe7In\x00"
                  "\x00\x00\x00\x00\x02k1\x00\x00\x00\x02v1\x00\x00\x00\x00"
                  "\x00\x00\x00\x00\x00\x00\x00\x12\xff\x06\x02I\x00\x00\x00"
                  "\x00\x00\x02k2\x00\x00\x00\x02v2")
        self.assertEqual(encoded, expect)

    def test_decode_message(self):
        encoded = "\xaa\xf1\x8f[\x00\x00\x00\x00\x00\x03key\x00\x00\x00\x04test"
        offset = 10
        (returned_offset, decoded_message) = \
            list(KafkaProtocol._decode_message(encoded, offset))[0]
        self.assertEqual(returned_offset, offset)
        self.assertEqual(decoded_message, create_message("test", "key"))

    def test_decode_message_set(self):
        encoded = ('\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10L\x9f[\xc2'
                   '\x00\x00\xff\xff\xff\xff\x00\x00\x00\x02v1\x00\x00\x00\x00'
                   '\x00\x00\x00\x00\x00\x00\x00\x10\xd5\x96\nx\x00\x00\xff'
                   '\xff\xff\xff\x00\x00\x00\x02v2')
        iter = KafkaProtocol._decode_message_set_iter(encoded)
        decoded = list(iter)
        self.assertEqual(len(decoded), 2)
        (returned_offset1, decoded_message1) = decoded[0]
        self.assertEqual(returned_offset1, 0)
        self.assertEqual(decoded_message1, create_message("v1"))
        (returned_offset2, decoded_message2) = decoded[1]
        self.assertEqual(returned_offset2, 0)
        self.assertEqual(decoded_message2, create_message("v2"))

    @unittest.skipUnless(has_gzip(), "Gzip not available")
    def test_decode_message_gzip(self):
        gzip_encoded = ('\xc0\x11\xb2\xf0\x00\x01\xff\xff\xff\xff\x00\x00\x000'
                        '\x1f\x8b\x08\x00\xa1\xc1\xc5R\x02\xffc`\x80\x03\x01'
                        '\x9f\xf9\xd1\x87\x18\x18\xfe\x03\x01\x90\xc7Tf\xc8'
                        '\x80$wu\x1aW\x05\x92\x9c\x11\x00z\xc0h\x888\x00\x00'
                        '\x00')
        offset = 11
        decoded = list(KafkaProtocol._decode_message(gzip_encoded, offset))
        self.assertEqual(len(decoded), 2)
        (returned_offset1, decoded_message1) = decoded[0]
        self.assertEqual(returned_offset1, 0)
        self.assertEqual(decoded_message1, create_message("v1"))
        (returned_offset2, decoded_message2) = decoded[1]
        self.assertEqual(returned_offset2, 0)
        self.assertEqual(decoded_message2, create_message("v2"))

    @unittest.skipUnless(has_snappy(), "Snappy not available")
    def test_decode_message_snappy(self):
        snappy_encoded = ('\xec\x80\xa1\x95\x00\x02\xff\xff\xff\xff\x00\x00'
                          '\x00,8\x00\x00\x19\x01@\x10L\x9f[\xc2\x00\x00\xff'
                          '\xff\xff\xff\x00\x00\x00\x02v1\x19\x1bD\x00\x10\xd5'
                          '\x96\nx\x00\x00\xff\xff\xff\xff\x00\x00\x00\x02v2')
        offset = 11
        decoded = list(KafkaProtocol._decode_message(snappy_encoded, offset))
        self.assertEqual(len(decoded), 2)
        (returned_offset1, decoded_message1) = decoded[0]
        self.assertEqual(returned_offset1, 0)
        self.assertEqual(decoded_message1, create_message("v1"))
        (returned_offset2, decoded_message2) = decoded[1]
        self.assertEqual(returned_offset2, 0)
        self.assertEqual(decoded_message2, create_message("v2"))

    def test_decode_message_checksum_error(self):
        invalid_encoded_message = "This is not a valid encoded message"
        iter = KafkaProtocol._decode_message(invalid_encoded_message, 0)
        self.assertRaises(ChecksumError, list, iter)

    # NOTE: The error handling in _decode_message_set_iter() is questionable.
    # If it's modified, the next two tests might need to be fixed.
    def test_decode_message_set_fetch_size_too_small(self):
        iter = KafkaProtocol._decode_message_set_iter('a')
        self.assertRaises(ConsumerFetchSizeTooSmall, list, iter)

    def test_decode_message_set_stop_iteration(self):
        encoded = ('\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10L\x9f[\xc2'
                   '\x00\x00\xff\xff\xff\xff\x00\x00\x00\x02v1\x00\x00\x00\x00'
                   '\x00\x00\x00\x00\x00\x00\x00\x10\xd5\x96\nx\x00\x00\xff'
                   '\xff\xff\xff\x00\x00\x00\x02v2')
        iter = KafkaProtocol._decode_message_set_iter(encoded + "@#$%(Y!")
        decoded = list(iter)
        self.assertEqual(len(decoded), 2)
        (returned_offset1, decoded_message1) = decoded[0]
        self.assertEqual(returned_offset1, 0)
        self.assertEqual(decoded_message1, create_message("v1"))
        (returned_offset2, decoded_message2) = decoded[1]
        self.assertEqual(returned_offset2, 0)
        self.assertEqual(decoded_message2, create_message("v2"))

    def test_encode_produce_request(self):
        requests = [ProduceRequest("topic1", 0, [create_message("a"),
                                                 create_message("b")]),
                    ProduceRequest("topic2", 1, [create_message("c")])]
        expect = ('\x00\x00\x00\x94\x00\x00\x00\x00\x00\x00\x00\x02\x00\x07'
                  'client1\x00\x02\x00\x00\x00d\x00\x00\x00\x02\x00\x06topic1'
                  '\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x006\x00\x00\x00'
                  '\x00\x00\x00\x00\x00\x00\x00\x00\x0fQ\xdf:2\x00\x00\xff\xff'
                  '\xff\xff\x00\x00\x00\x01a\x00\x00\x00\x00\x00\x00\x00\x00'
                  '\x00\x00\x00\x0f\xc8\xd6k\x88\x00\x00\xff\xff\xff\xff\x00'
                  '\x00\x00\x01b\x00\x06topic2\x00\x00\x00\x01\x00\x00\x00\x01'
                  '\x00\x00\x00\x1b\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
                  '\x00\x0f\xbf\xd1[\x1e\x00\x00\xff\xff\xff\xff\x00\x00\x00'
                  '\x01c')
        encoded = KafkaProtocol.encode_produce_request("client1", 2, requests,
                                                       2, 100)
        self.assertEqual(encoded, expect)

    def test_decode_produce_response(self):
        t1 = "topic1"
        t2 = "topic2"
        encoded = struct.pack('>iih%dsiihqihqh%dsiihq' % (len(t1), len(t2)),
                              2, 2, len(t1), t1, 2, 0, 0, 10L, 1, 1, 20L,
                              len(t2), t2, 1, 0, 0, 30L)
        responses = list(KafkaProtocol.decode_produce_response(encoded))
        self.assertEqual(responses,
                         [ProduceResponse(t1, 0, 0, 10L),
                          ProduceResponse(t1, 1, 1, 20L),
                          ProduceResponse(t2, 0, 0, 30L)])

    def test_encode_fetch_request(self):
        requests = [FetchRequest("topic1", 0, 10, 1024),
                    FetchRequest("topic2", 1, 20, 100)]
        expect = ('\x00\x00\x00Y\x00\x01\x00\x00\x00\x00\x00\x03\x00\x07'
                  'client1\xff\xff\xff\xff\x00\x00\x00\x02\x00\x00\x00d\x00'
                  '\x00\x00\x02\x00\x06topic1\x00\x00\x00\x01\x00\x00\x00\x00'
                  '\x00\x00\x00\x00\x00\x00\x00\n\x00\x00\x04\x00\x00\x06'
                  'topic2\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x00\x00'
                  '\x00\x00\x14\x00\x00\x00d')
        encoded = KafkaProtocol.encode_fetch_request("client1", 3, requests, 2,
                                                     100)
        self.assertEqual(encoded, expect)

    def test_decode_fetch_response(self):
        t1 = "topic1"
        t2 = "topic2"
        msgs = map(create_message, ["message1", "hi", "boo", "foo", "so fun!"])
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

        expanded_responses = map(expand_messages, responses)
        expect = [FetchResponse(t1, 0, 0, 10, [OffsetAndMessage(0, msgs[0]),
                                               OffsetAndMessage(0, msgs[1])]),
                  FetchResponse(t1, 1, 1, 20, [OffsetAndMessage(0, msgs[2])]),
                  FetchResponse(t2, 0, 0, 30, [OffsetAndMessage(0, msgs[3]),
                                               OffsetAndMessage(0, msgs[4])])]
        self.assertEqual(expanded_responses, expect)

    def test_encode_metadata_request_no_topics(self):
        encoded = KafkaProtocol.encode_metadata_request("cid", 4)
        self.assertEqual(encoded, '\x00\x00\x00\x11\x00\x03\x00\x00\x00\x00'
                                  '\x00\x04\x00\x03cid\x00\x00\x00\x00')

    def test_encode_metadata_request_with_topics(self):
        encoded = KafkaProtocol.encode_metadata_request("cid", 4, ["t1", "t2"])
        self.assertEqual(encoded, '\x00\x00\x00\x19\x00\x03\x00\x00\x00\x00'
                                  '\x00\x04\x00\x03cid\x00\x00\x00\x02\x00\x02'
                                  't1\x00\x02t2')

    def _create_encoded_metadata_response(self, broker_data, topic_data,
                                          topic_errors, partition_errors):
        encoded = struct.pack('>ii', 3, len(broker_data))
        for node_id, broker in broker_data.iteritems():
            encoded += struct.pack('>ih%dsi' % len(broker.host), node_id,
                                   len(broker.host), broker.host, broker.port)

        encoded += struct.pack('>i', len(topic_data))
        for topic, partitions in topic_data.iteritems():
            encoded += struct.pack('>hh%dsi' % len(topic), topic_errors[topic],
                                   len(topic), topic, len(partitions))
            for partition, metadata in partitions.iteritems():
                encoded += struct.pack('>hiii',
                                       partition_errors[(topic, partition)],
                                       partition, metadata.leader,
                                       len(metadata.replicas))
                if len(metadata.replicas) > 0:
                    encoded += struct.pack('>%di' % len(metadata.replicas),
                                           *metadata.replicas)

                encoded += struct.pack('>i', len(metadata.isr))
                if len(metadata.isr) > 0:
                    encoded += struct.pack('>%di' % len(metadata.isr),
                                           *metadata.isr)

        return encoded

    def test_decode_metadata_response(self):
        node_brokers = {
            0: BrokerMetadata(0, "brokers1.kafka.rdio.com", 1000),
            1: BrokerMetadata(1, "brokers1.kafka.rdio.com", 1001),
            3: BrokerMetadata(3, "brokers2.kafka.rdio.com", 1000)
        }
        topic_partitions = {
            "topic1": {
                0: PartitionMetadata("topic1", 0, 1, (0, 2), (2,)),
                1: PartitionMetadata("topic1", 1, 3, (0, 1), (0, 1))
            },
            "topic2": {
                0: PartitionMetadata("topic2", 0, 0, (), ())
            }
        }
        topic_errors = {"topic1": 0, "topic2": 1}
        partition_errors = {
            ("topic1", 0): 0,
            ("topic1", 1): 1,
            ("topic2", 0): 0
        }
        encoded = self._create_encoded_metadata_response(node_brokers,
                                                         topic_partitions,
                                                         topic_errors,
                                                         partition_errors)
        decoded = KafkaProtocol.decode_metadata_response(encoded)
        self.assertEqual(decoded, (node_brokers, topic_partitions))

    @unittest.skip("Not Implemented")
    def test_encode_offset_request(self):
        pass

    @unittest.skip("Not Implemented")
    def test_decode_offset_response(self):
        pass


    @unittest.skip("Not Implemented")
    def test_encode_offset_commit_request(self):
        pass

    @unittest.skip("Not Implemented")
    def test_decode_offset_commit_response(self):
        pass

    @unittest.skip("Not Implemented")
    def test_encode_offset_fetch_request(self):
        pass

    @unittest.skip("Not Implemented")
    def test_decode_offset_fetch_response(self):
        pass


if __name__ == '__main__':
    unittest.main()
