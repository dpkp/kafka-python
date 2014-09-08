import socket
from time import sleep

from mock import ANY, MagicMock, patch
import six
from . import unittest

from kafka import KafkaClient
from kafka.common import (
    ProduceRequest, MetadataResponse,
    BrokerMetadata, TopicMetadata, PartitionMetadata,
    TopicAndPartition, KafkaUnavailableError,
    LeaderNotAvailableError, NoError,
    UnknownTopicOrPartitionError, KafkaTimeoutError,
    ConnectionError
)
from kafka.conn import KafkaConnection
from kafka.protocol import KafkaProtocol, create_message

from test.testutil import Timer

NO_ERROR = 0
UNKNOWN_TOPIC_OR_PARTITION = 3
NO_LEADER = 5

class TestKafkaClient(unittest.TestCase):
    def test_init_with_list(self):
        with patch.object(KafkaClient, 'load_metadata_for_topics'):
            client = KafkaClient(hosts=['kafka01:9092', 'kafka02:9092', 'kafka03:9092'])

        self.assertEqual(
            sorted([('kafka01', 9092), ('kafka02', 9092), ('kafka03', 9092)]),
            sorted(client.hosts))

    def test_init_with_csv(self):
        with patch.object(KafkaClient, 'load_metadata_for_topics'):
            client = KafkaClient(hosts='kafka01:9092,kafka02:9092,kafka03:9092')

        self.assertEqual(
            sorted([('kafka01', 9092), ('kafka02', 9092), ('kafka03', 9092)]),
            sorted(client.hosts))

    def test_init_with_unicode_csv(self):
        with patch.object(KafkaClient, 'load_metadata_for_topics'):
            client = KafkaClient(hosts=u'kafka01:9092,kafka02:9092,kafka03:9092')

        self.assertEqual(
            sorted([('kafka01', 9092), ('kafka02', 9092), ('kafka03', 9092)]),
            sorted(client.hosts))

    def test_send_broker_unaware_request_fail(self):
        'Tests that call fails when all hosts are unavailable'

        mocked_conns = {
            ('kafka01', 9092): MagicMock(),
            ('kafka02', 9092): MagicMock()
        }

        # inject KafkaConnection side effects
        mocked_conns[('kafka01', 9092)].send.side_effect = RuntimeError("kafka01 went away (unittest)")
        mocked_conns[('kafka02', 9092)].send.side_effect = RuntimeError("Kafka02 went away (unittest)")

        def mock_get_conn(host, port):
            return mocked_conns[(host, port)]

        # patch to avoid making requests before we want it
        with patch.object(KafkaClient, 'load_metadata_for_topics'):
            with patch.object(KafkaClient, '_get_conn', side_effect=mock_get_conn):
                client = KafkaClient(hosts=['kafka01:9092', 'kafka02:9092'])

                req = KafkaProtocol.encode_metadata_request(b'client', 0)
                with self.assertRaises(KafkaUnavailableError):
                    client._send_broker_unaware_request(payloads=['fake request'],
                                                        encoder_fn=MagicMock(return_value='fake encoded message'),
                                                        decoder_fn=lambda x: x)

                for key, conn in six.iteritems(mocked_conns):
                    conn.send.assert_called_with(ANY, 'fake encoded message')

    def test_send_broker_unaware_request(self):
        'Tests that call works when at least one of the host is available'

        mocked_conns = {
            ('kafka01', 9092): MagicMock(),
            ('kafka02', 9092): MagicMock(),
            ('kafka03', 9092): MagicMock()
        }
        # inject KafkaConnection side effects
        mocked_conns[('kafka01', 9092)].send.side_effect = RuntimeError("kafka01 went away (unittest)")
        mocked_conns[('kafka02', 9092)].recv.return_value = 'valid response'
        mocked_conns[('kafka03', 9092)].send.side_effect = RuntimeError("kafka03 went away (unittest)")

        def mock_get_conn(host, port):
            return mocked_conns[(host, port)]

        # patch to avoid making requests before we want it
        with patch.object(KafkaClient, 'load_metadata_for_topics'):
            with patch.object(KafkaClient, '_get_conn', side_effect=mock_get_conn):
                with patch.object(KafkaClient, '_next_id', return_value=1):
                    client = KafkaClient(hosts='kafka01:9092,kafka02:9092')

                    resp = client._send_broker_unaware_request(payloads=['fake request'],
                                                               encoder_fn=MagicMock(),
                                                               decoder_fn=lambda x: x)

                    self.assertEqual('valid response', resp)
                    mocked_conns[('kafka02', 9092)].recv.assert_called_with(1)

    @patch('kafka.client.KafkaConnection')
    @patch('kafka.client.KafkaProtocol')
    def test_load_metadata(self, protocol, conn):

        conn.recv.return_value = 'response'  # anything but None

        brokers = [
            BrokerMetadata(0, 'broker_1', 4567),
            BrokerMetadata(1, 'broker_2', 5678)
        ]

        topics = [
            TopicMetadata('topic_1', NO_ERROR, [
                PartitionMetadata('topic_1', 0, 1, [1, 2], [1, 2], NO_ERROR)
            ]),
            TopicMetadata('topic_noleader', NO_ERROR, [
                PartitionMetadata('topic_noleader', 0, -1, [], [],
                                  NO_LEADER),
                PartitionMetadata('topic_noleader', 1, -1, [], [],
                                  NO_LEADER),
            ]),
            TopicMetadata('topic_no_partitions', NO_LEADER, []),
            TopicMetadata('topic_unknown', UNKNOWN_TOPIC_OR_PARTITION, []),
            TopicMetadata('topic_3', NO_ERROR, [
                PartitionMetadata('topic_3', 0, 0, [0, 1], [0, 1], NO_ERROR),
                PartitionMetadata('topic_3', 1, 1, [1, 0], [1, 0], NO_ERROR),
                PartitionMetadata('topic_3', 2, 0, [0, 1], [0, 1], NO_ERROR)
            ])
        ]
        protocol.decode_metadata_response.return_value = MetadataResponse(brokers, topics)

        # client loads metadata at init
        client = KafkaClient(hosts=['broker_1:4567'])
        self.assertDictEqual({
            TopicAndPartition('topic_1', 0): brokers[1],
            TopicAndPartition('topic_noleader', 0): None,
            TopicAndPartition('topic_noleader', 1): None,
            TopicAndPartition('topic_3', 0): brokers[0],
            TopicAndPartition('topic_3', 1): brokers[1],
            TopicAndPartition('topic_3', 2): brokers[0]},
            client.topics_to_brokers)

        # if we ask for metadata explicitly, it should raise errors
        with self.assertRaises(LeaderNotAvailableError):
            client.load_metadata_for_topics('topic_no_partitions')

        with self.assertRaises(UnknownTopicOrPartitionError):
            client.load_metadata_for_topics('topic_unknown')

        # This should not raise
        client.load_metadata_for_topics('topic_no_leader')

    @patch('kafka.client.KafkaConnection')
    @patch('kafka.client.KafkaProtocol')
    def test_has_metadata_for_topic(self, protocol, conn):

        conn.recv.return_value = 'response'  # anything but None

        brokers = [
            BrokerMetadata(0, 'broker_1', 4567),
            BrokerMetadata(1, 'broker_2', 5678)
        ]

        topics = [
            TopicMetadata('topic_still_creating', NO_LEADER, []),
            TopicMetadata('topic_doesnt_exist', UNKNOWN_TOPIC_OR_PARTITION, []),
            TopicMetadata('topic_noleaders', NO_ERROR, [
                PartitionMetadata('topic_noleaders', 0, -1, [], [], NO_LEADER),
                PartitionMetadata('topic_noleaders', 1, -1, [], [], NO_LEADER),
            ]),
        ]
        protocol.decode_metadata_response.return_value = MetadataResponse(brokers, topics)

        client = KafkaClient(hosts=['broker_1:4567'])

        # Topics with no partitions return False
        self.assertFalse(client.has_metadata_for_topic('topic_still_creating'))
        self.assertFalse(client.has_metadata_for_topic('topic_doesnt_exist'))

        # Topic with partition metadata, but no leaders return True
        self.assertTrue(client.has_metadata_for_topic('topic_noleaders'))

    @patch('kafka.client.KafkaConnection')
    @patch('kafka.client.KafkaProtocol')
    def test_ensure_topic_exists(self, protocol, conn):

        conn.recv.return_value = 'response'  # anything but None

        brokers = [
            BrokerMetadata(0, 'broker_1', 4567),
            BrokerMetadata(1, 'broker_2', 5678)
        ]

        topics = [
            TopicMetadata('topic_still_creating', NO_LEADER, []),
            TopicMetadata('topic_doesnt_exist', UNKNOWN_TOPIC_OR_PARTITION, []),
            TopicMetadata('topic_noleaders', NO_ERROR, [
                PartitionMetadata('topic_noleaders', 0, -1, [], [], NO_LEADER),
                PartitionMetadata('topic_noleaders', 1, -1, [], [], NO_LEADER),
            ]),
        ]
        protocol.decode_metadata_response.return_value = MetadataResponse(brokers, topics)

        client = KafkaClient(hosts=['broker_1:4567'])

        with self.assertRaises(UnknownTopicOrPartitionError):
            client.ensure_topic_exists('topic_doesnt_exist', timeout=1)

        with self.assertRaises(KafkaTimeoutError):
            client.ensure_topic_exists('topic_still_creating', timeout=1)

        # This should not raise
        client.ensure_topic_exists('topic_noleaders', timeout=1)

    @patch('kafka.client.KafkaConnection')
    @patch('kafka.client.KafkaProtocol')
    def test_get_leader_for_partitions_reloads_metadata(self, protocol, conn):
        "Get leader for partitions reload metadata if it is not available"

        conn.recv.return_value = 'response'  # anything but None

        brokers = [
            BrokerMetadata(0, 'broker_1', 4567),
            BrokerMetadata(1, 'broker_2', 5678)
        ]

        topics = [
            TopicMetadata('topic_no_partitions', NO_LEADER, [])
        ]
        protocol.decode_metadata_response.return_value = MetadataResponse(brokers, topics)

        client = KafkaClient(hosts=['broker_1:4567'])

        # topic metadata is loaded but empty
        self.assertDictEqual({}, client.topics_to_brokers)

        topics = [
            TopicMetadata('topic_one_partition', NO_ERROR, [
                PartitionMetadata('topic_no_partition', 0, 0, [0, 1], [0, 1], NO_ERROR)
            ])
        ]
        protocol.decode_metadata_response.return_value = MetadataResponse(brokers, topics)

        # calling _get_leader_for_partition (from any broker aware request)
        # will try loading metadata again for the same topic
        leader = client._get_leader_for_partition('topic_one_partition', 0)

        self.assertEqual(brokers[0], leader)
        self.assertDictEqual({
            TopicAndPartition('topic_one_partition', 0): brokers[0]},
            client.topics_to_brokers)

    @patch('kafka.client.KafkaConnection')
    @patch('kafka.client.KafkaProtocol')
    def test_get_leader_for_unassigned_partitions(self, protocol, conn):

        conn.recv.return_value = 'response'  # anything but None

        brokers = [
            BrokerMetadata(0, 'broker_1', 4567),
            BrokerMetadata(1, 'broker_2', 5678)
        ]

        topics = [
            TopicMetadata('topic_no_partitions', NO_LEADER, []),
            TopicMetadata('topic_unknown', UNKNOWN_TOPIC_OR_PARTITION, []),
        ]
        protocol.decode_metadata_response.return_value = MetadataResponse(brokers, topics)

        client = KafkaClient(hosts=['broker_1:4567'])

        self.assertDictEqual({}, client.topics_to_brokers)

        with self.assertRaises(LeaderNotAvailableError):
            client._get_leader_for_partition('topic_no_partitions', 0)

        with self.assertRaises(UnknownTopicOrPartitionError):
            client._get_leader_for_partition('topic_unknown', 0)

    @patch('kafka.client.KafkaConnection')
    @patch('kafka.client.KafkaProtocol')
    def test_get_leader_exceptions_when_noleader(self, protocol, conn):

        conn.recv.return_value = 'response'  # anything but None

        brokers = [
            BrokerMetadata(0, 'broker_1', 4567),
            BrokerMetadata(1, 'broker_2', 5678)
        ]

        topics = [
            TopicMetadata('topic_noleader', NO_ERROR, [
                PartitionMetadata('topic_noleader', 0, -1, [], [],
                                  NO_LEADER),
                PartitionMetadata('topic_noleader', 1, -1, [], [],
                                  NO_LEADER),
            ]),
        ]
        protocol.decode_metadata_response.return_value = MetadataResponse(brokers, topics)

        client = KafkaClient(hosts=['broker_1:4567'])
        self.assertDictEqual(
            {
                TopicAndPartition('topic_noleader', 0): None,
                TopicAndPartition('topic_noleader', 1): None
            },
            client.topics_to_brokers)

        # No leader partitions -- raise LeaderNotAvailableError
        with self.assertRaises(LeaderNotAvailableError):
            self.assertIsNone(client._get_leader_for_partition('topic_noleader', 0))
        with self.assertRaises(LeaderNotAvailableError):
            self.assertIsNone(client._get_leader_for_partition('topic_noleader', 1))

        # Unknown partitions -- raise UnknownTopicOrPartitionError
        with self.assertRaises(UnknownTopicOrPartitionError):
            self.assertIsNone(client._get_leader_for_partition('topic_noleader', 2))

        topics = [
            TopicMetadata('topic_noleader', NO_ERROR, [
                PartitionMetadata('topic_noleader', 0, 0, [0, 1], [0, 1], NO_ERROR),
                PartitionMetadata('topic_noleader', 1, 1, [1, 0], [1, 0], NO_ERROR)
            ]),
        ]
        protocol.decode_metadata_response.return_value = MetadataResponse(brokers, topics)
        self.assertEqual(brokers[0], client._get_leader_for_partition('topic_noleader', 0))
        self.assertEqual(brokers[1], client._get_leader_for_partition('topic_noleader', 1))

    @patch('kafka.client.KafkaConnection')
    @patch('kafka.client.KafkaProtocol')
    def test_send_produce_request_raises_when_noleader(self, protocol, conn):
        "Send producer request raises LeaderNotAvailableError if leader is not available"

        conn.recv.return_value = 'response'  # anything but None

        brokers = [
            BrokerMetadata(0, 'broker_1', 4567),
            BrokerMetadata(1, 'broker_2', 5678)
        ]

        topics = [
            TopicMetadata('topic_noleader', NO_ERROR, [
                PartitionMetadata('topic_noleader', 0, -1, [], [],
                                  NO_LEADER),
                PartitionMetadata('topic_noleader', 1, -1, [], [],
                                  NO_LEADER),
            ]),
        ]
        protocol.decode_metadata_response.return_value = MetadataResponse(brokers, topics)

        client = KafkaClient(hosts=['broker_1:4567'])

        requests = [ProduceRequest(
            "topic_noleader", 0,
            [create_message("a"), create_message("b")])]

        with self.assertRaises(LeaderNotAvailableError):
            client.send_produce_request(requests)

    @patch('kafka.client.KafkaConnection')
    @patch('kafka.client.KafkaProtocol')
    def test_send_produce_request_raises_when_topic_unknown(self, protocol, conn):

        conn.recv.return_value = 'response'  # anything but None

        brokers = [
            BrokerMetadata(0, 'broker_1', 4567),
            BrokerMetadata(1, 'broker_2', 5678)
        ]

        topics = [
            TopicMetadata('topic_doesnt_exist', UNKNOWN_TOPIC_OR_PARTITION, []),
        ]
        protocol.decode_metadata_response.return_value = MetadataResponse(brokers, topics)

        client = KafkaClient(hosts=['broker_1:4567'])

        requests = [ProduceRequest(
            "topic_doesnt_exist", 0,
            [create_message("a"), create_message("b")])]

        with self.assertRaises(UnknownTopicOrPartitionError):
            client.send_produce_request(requests)

    def test_timeout(self):
        def _timeout(*args, **kwargs):
            timeout = args[1]
            sleep(timeout)
            raise socket.timeout

        with patch.object(socket, "create_connection", side_effect=_timeout):

            with Timer() as t:
                with self.assertRaises(ConnectionError):
                    KafkaConnection("nowhere", 1234, 1.0)
            self.assertGreaterEqual(t.interval, 1.0)
