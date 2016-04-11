import socket
from time import sleep

from mock import ANY, MagicMock, patch
import six
from . import unittest

from kafka import SimpleClient
from kafka.conn import KafkaConnection
from kafka.errors import (
    KafkaUnavailableError, LeaderNotAvailableError, KafkaTimeoutError,
    UnknownTopicOrPartitionError, ConnectionError, FailedPayloadsError)
from kafka.future import Future
from kafka.protocol import KafkaProtocol, create_message
from kafka.protocol.metadata import MetadataResponse
from kafka.structs import ProduceRequestPayload, BrokerMetadata, TopicPartition

from test.testutil import Timer

NO_ERROR = 0
UNKNOWN_TOPIC_OR_PARTITION = 3
NO_LEADER = 5


def mock_conn(conn, success=True):
    mocked = MagicMock()
    mocked.connected.return_value = True
    if success:
        mocked.send.return_value = Future().success(True)
    else:
        mocked.send.return_value = Future().failure(Exception())
    conn.return_value = mocked


class TestSimpleClient(unittest.TestCase):
    def test_init_with_list(self):
        with patch.object(SimpleClient, 'load_metadata_for_topics'):
            client = SimpleClient(hosts=['kafka01:9092', 'kafka02:9092', 'kafka03:9092'])

        self.assertEqual(
            sorted([('kafka01', 9092, socket.AF_UNSPEC), ('kafka02', 9092, socket.AF_UNSPEC),
                    ('kafka03', 9092, socket.AF_UNSPEC)]),
            sorted(client.hosts))

    def test_init_with_csv(self):
        with patch.object(SimpleClient, 'load_metadata_for_topics'):
            client = SimpleClient(hosts='kafka01:9092,kafka02:9092,kafka03:9092')

        self.assertEqual(
            sorted([('kafka01', 9092, socket.AF_UNSPEC), ('kafka02', 9092, socket.AF_UNSPEC),
                    ('kafka03', 9092, socket.AF_UNSPEC)]),
            sorted(client.hosts))

    def test_init_with_unicode_csv(self):
        with patch.object(SimpleClient, 'load_metadata_for_topics'):
            client = SimpleClient(hosts=u'kafka01:9092,kafka02:9092,kafka03:9092')

        self.assertEqual(
            sorted([('kafka01', 9092, socket.AF_UNSPEC), ('kafka02', 9092, socket.AF_UNSPEC),
                    ('kafka03', 9092, socket.AF_UNSPEC)]),
            sorted(client.hosts))

    @patch.object(SimpleClient, '_get_conn')
    @patch.object(SimpleClient, 'load_metadata_for_topics')
    def test_send_broker_unaware_request_fail(self, load_metadata, conn):
        mocked_conns = {
            ('kafka01', 9092): MagicMock(),
            ('kafka02', 9092): MagicMock()
        }
        for val in mocked_conns.values():
            mock_conn(val, success=False)

        def mock_get_conn(host, port, afi):
            return mocked_conns[(host, port)]
        conn.side_effect = mock_get_conn

        client = SimpleClient(hosts=['kafka01:9092', 'kafka02:9092'])

        req = KafkaProtocol.encode_metadata_request()
        with self.assertRaises(KafkaUnavailableError):
            client._send_broker_unaware_request(payloads=['fake request'],
                                                encoder_fn=MagicMock(return_value='fake encoded message'),
                                                decoder_fn=lambda x: x)

        for key, conn in six.iteritems(mocked_conns):
            conn.send.assert_called_with('fake encoded message')

    def test_send_broker_unaware_request(self):
        mocked_conns = {
            ('kafka01', 9092): MagicMock(),
            ('kafka02', 9092): MagicMock(),
            ('kafka03', 9092): MagicMock()
        }
        # inject KafkaConnection side effects
        mock_conn(mocked_conns[('kafka01', 9092)], success=False)
        mock_conn(mocked_conns[('kafka03', 9092)], success=False)
        future = Future()
        mocked_conns[('kafka02', 9092)].send.return_value = future
        mocked_conns[('kafka02', 9092)].recv.side_effect = lambda: future.success('valid response')

        def mock_get_conn(host, port, afi):
            return mocked_conns[(host, port)]

        # patch to avoid making requests before we want it
        with patch.object(SimpleClient, 'load_metadata_for_topics'):
            with patch.object(SimpleClient, '_get_conn', side_effect=mock_get_conn):

                client = SimpleClient(hosts='kafka01:9092,kafka02:9092')
                resp = client._send_broker_unaware_request(payloads=['fake request'],
                                                           encoder_fn=MagicMock(),
                                                           decoder_fn=lambda x: x)

                self.assertEqual('valid response', resp)
                mocked_conns[('kafka02', 9092)].recv.assert_called_once_with()

    @patch('kafka.SimpleClient._get_conn')
    @patch('kafka.client.KafkaProtocol')
    def test_load_metadata(self, protocol, conn):

        mock_conn(conn)

        brokers = [
            BrokerMetadata(0, 'broker_1', 4567),
            BrokerMetadata(1, 'broker_2', 5678)
        ]

        topics = [
            (NO_ERROR, 'topic_1', [
                (NO_ERROR, 0, 1, [1, 2], [1, 2])
            ]),
            (NO_ERROR, 'topic_noleader', [
                (NO_LEADER, 0, -1, [], []),
                (NO_LEADER, 1, -1, [], []),
            ]),
            (NO_LEADER, 'topic_no_partitions', []),
            (UNKNOWN_TOPIC_OR_PARTITION, 'topic_unknown', []),
            (NO_ERROR, 'topic_3', [
                (NO_ERROR, 0, 0, [0, 1], [0, 1]),
                (NO_ERROR, 1, 1, [1, 0], [1, 0]),
                (NO_ERROR, 2, 0, [0, 1], [0, 1])
            ])
        ]
        protocol.decode_metadata_response.return_value = MetadataResponse[0](brokers, topics)

        # client loads metadata at init
        client = SimpleClient(hosts=['broker_1:4567'])
        self.assertDictEqual({
            TopicPartition('topic_1', 0): brokers[1],
            TopicPartition('topic_noleader', 0): None,
            TopicPartition('topic_noleader', 1): None,
            TopicPartition('topic_3', 0): brokers[0],
            TopicPartition('topic_3', 1): brokers[1],
            TopicPartition('topic_3', 2): brokers[0]},
            client.topics_to_brokers)

        # if we ask for metadata explicitly, it should raise errors
        with self.assertRaises(LeaderNotAvailableError):
            client.load_metadata_for_topics('topic_no_partitions')

        with self.assertRaises(UnknownTopicOrPartitionError):
            client.load_metadata_for_topics('topic_unknown')

        # This should not raise
        client.load_metadata_for_topics('topic_no_leader')

    @patch('kafka.SimpleClient._get_conn')
    @patch('kafka.client.KafkaProtocol')
    def test_has_metadata_for_topic(self, protocol, conn):

        mock_conn(conn)

        brokers = [
            BrokerMetadata(0, 'broker_1', 4567),
            BrokerMetadata(1, 'broker_2', 5678)
        ]

        topics = [
            (NO_LEADER, 'topic_still_creating', []),
            (UNKNOWN_TOPIC_OR_PARTITION, 'topic_doesnt_exist', []),
            (NO_ERROR, 'topic_noleaders', [
                (NO_LEADER, 0, -1, [], []),
                (NO_LEADER, 1, -1, [], []),
            ]),
        ]
        protocol.decode_metadata_response.return_value = MetadataResponse[0](brokers, topics)

        client = SimpleClient(hosts=['broker_1:4567'])

        # Topics with no partitions return False
        self.assertFalse(client.has_metadata_for_topic('topic_still_creating'))
        self.assertFalse(client.has_metadata_for_topic('topic_doesnt_exist'))

        # Topic with partition metadata, but no leaders return True
        self.assertTrue(client.has_metadata_for_topic('topic_noleaders'))

    @patch('kafka.SimpleClient._get_conn')
    @patch('kafka.client.KafkaProtocol.decode_metadata_response')
    def test_ensure_topic_exists(self, decode_metadata_response, conn):

        mock_conn(conn)

        brokers = [
            BrokerMetadata(0, 'broker_1', 4567),
            BrokerMetadata(1, 'broker_2', 5678)
        ]

        topics = [
            (NO_LEADER, 'topic_still_creating', []),
            (UNKNOWN_TOPIC_OR_PARTITION, 'topic_doesnt_exist', []),
            (NO_ERROR, 'topic_noleaders', [
                (NO_LEADER, 0, -1, [], []),
                (NO_LEADER, 1, -1, [], []),
            ]),
        ]
        decode_metadata_response.return_value = MetadataResponse[0](brokers, topics)

        client = SimpleClient(hosts=['broker_1:4567'])

        with self.assertRaises(UnknownTopicOrPartitionError):
            client.ensure_topic_exists('topic_doesnt_exist', timeout=1)

        with self.assertRaises(KafkaTimeoutError):
            client.ensure_topic_exists('topic_still_creating', timeout=1)

        # This should not raise
        client.ensure_topic_exists('topic_noleaders', timeout=1)

    @patch('kafka.SimpleClient._get_conn')
    @patch('kafka.client.KafkaProtocol')
    def test_get_leader_for_partitions_reloads_metadata(self, protocol, conn):
        "Get leader for partitions reload metadata if it is not available"

        mock_conn(conn)

        brokers = [
            BrokerMetadata(0, 'broker_1', 4567),
            BrokerMetadata(1, 'broker_2', 5678)
        ]

        topics = [
            (NO_LEADER, 'topic_no_partitions', [])
        ]
        protocol.decode_metadata_response.return_value = MetadataResponse[0](brokers, topics)

        client = SimpleClient(hosts=['broker_1:4567'])

        # topic metadata is loaded but empty
        self.assertDictEqual({}, client.topics_to_brokers)

        topics = [
            (NO_ERROR, 'topic_one_partition', [
                (NO_ERROR, 0, 0, [0, 1], [0, 1])
            ])
        ]
        protocol.decode_metadata_response.return_value = MetadataResponse[0](brokers, topics)

        # calling _get_leader_for_partition (from any broker aware request)
        # will try loading metadata again for the same topic
        leader = client._get_leader_for_partition('topic_one_partition', 0)

        self.assertEqual(brokers[0], leader)
        self.assertDictEqual({
            TopicPartition('topic_one_partition', 0): brokers[0]},
            client.topics_to_brokers)

    @patch('kafka.SimpleClient._get_conn')
    @patch('kafka.client.KafkaProtocol')
    def test_get_leader_for_unassigned_partitions(self, protocol, conn):

        mock_conn(conn)

        brokers = [
            BrokerMetadata(0, 'broker_1', 4567),
            BrokerMetadata(1, 'broker_2', 5678)
        ]

        topics = [
            (NO_LEADER, 'topic_no_partitions', []),
            (UNKNOWN_TOPIC_OR_PARTITION, 'topic_unknown', []),
        ]
        protocol.decode_metadata_response.return_value = MetadataResponse[0](brokers, topics)

        client = SimpleClient(hosts=['broker_1:4567'])

        self.assertDictEqual({}, client.topics_to_brokers)

        with self.assertRaises(LeaderNotAvailableError):
            client._get_leader_for_partition('topic_no_partitions', 0)

        with self.assertRaises(UnknownTopicOrPartitionError):
            client._get_leader_for_partition('topic_unknown', 0)

    @patch('kafka.SimpleClient._get_conn')
    @patch('kafka.client.KafkaProtocol')
    def test_get_leader_exceptions_when_noleader(self, protocol, conn):

        mock_conn(conn)

        brokers = [
            BrokerMetadata(0, 'broker_1', 4567),
            BrokerMetadata(1, 'broker_2', 5678)
        ]

        topics = [
            (NO_ERROR, 'topic_noleader', [
                (NO_LEADER, 0, -1, [], []),
                (NO_LEADER, 1, -1, [], []),
            ]),
        ]
        protocol.decode_metadata_response.return_value = MetadataResponse[0](brokers, topics)

        client = SimpleClient(hosts=['broker_1:4567'])
        self.assertDictEqual(
            {
                TopicPartition('topic_noleader', 0): None,
                TopicPartition('topic_noleader', 1): None
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
            (NO_ERROR, 'topic_noleader', [
                (NO_ERROR, 0, 0, [0, 1], [0, 1]),
                (NO_ERROR, 1, 1, [1, 0], [1, 0])
            ]),
        ]
        protocol.decode_metadata_response.return_value = MetadataResponse[0](brokers, topics)
        self.assertEqual(brokers[0], client._get_leader_for_partition('topic_noleader', 0))
        self.assertEqual(brokers[1], client._get_leader_for_partition('topic_noleader', 1))

    @patch.object(SimpleClient, '_get_conn')
    @patch('kafka.client.KafkaProtocol')
    def test_send_produce_request_raises_when_noleader(self, protocol, conn):
        mock_conn(conn)

        brokers = [
            BrokerMetadata(0, 'broker_1', 4567),
            BrokerMetadata(1, 'broker_2', 5678)
        ]

        topics = [
            (NO_ERROR, 'topic_noleader', [
                (NO_LEADER, 0, -1, [], []),
                (NO_LEADER, 1, -1, [], []),
            ]),
        ]
        protocol.decode_metadata_response.return_value = MetadataResponse[0](brokers, topics)

        client = SimpleClient(hosts=['broker_1:4567'])

        requests = [ProduceRequestPayload(
            "topic_noleader", 0,
            [create_message("a"), create_message("b")])]

        with self.assertRaises(FailedPayloadsError):
            client.send_produce_request(requests)

    @patch('kafka.SimpleClient._get_conn')
    @patch('kafka.client.KafkaProtocol')
    def test_send_produce_request_raises_when_topic_unknown(self, protocol, conn):

        mock_conn(conn)

        brokers = [
            BrokerMetadata(0, 'broker_1', 4567),
            BrokerMetadata(1, 'broker_2', 5678)
        ]

        topics = [
            (UNKNOWN_TOPIC_OR_PARTITION, 'topic_doesnt_exist', []),
        ]
        protocol.decode_metadata_response.return_value = MetadataResponse[0](brokers, topics)

        client = SimpleClient(hosts=['broker_1:4567'])

        requests = [ProduceRequestPayload(
            "topic_doesnt_exist", 0,
            [create_message("a"), create_message("b")])]

        with self.assertRaises(FailedPayloadsError):
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

    def test_correlation_rollover(self):
        with patch.object(SimpleClient, 'load_metadata_for_topics'):
            big_num = 2**31 - 3
            client = SimpleClient(hosts=[], correlation_id=big_num)
            self.assertEqual(big_num + 1, client._next_id())
            self.assertEqual(big_num + 2, client._next_id())
            self.assertEqual(0, client._next_id())

