
from mock import MagicMock, patch
from . import unittest

from kafka import SimpleConsumer, KafkaConsumer, MultiProcessConsumer
from kafka.common import (
    KafkaConfigurationError, FetchResponse, OffsetFetchResponse,
    FailedPayloadsError, OffsetAndMessage,
    NotLeaderForPartitionError, UnknownTopicOrPartitionError
)


class TestKafkaConsumer(unittest.TestCase):
    def test_non_integer_partitions(self):
        with self.assertRaises(AssertionError):
            SimpleConsumer(MagicMock(), 'group', 'topic', partitions = [ '0' ])

    def test_broker_list_required(self):
        with self.assertRaises(KafkaConfigurationError):
            KafkaConsumer()


class TestMultiProcessConsumer(unittest.TestCase):
    def test_partition_list(self):
        client = MagicMock()
        partitions = (0,)
        with patch.object(MultiProcessConsumer, 'fetch_last_known_offsets') as fetch_last_known_offsets:
            MultiProcessConsumer(client, 'testing-group', 'testing-topic', partitions=partitions)
            self.assertEqual(fetch_last_known_offsets.call_args[0], (partitions,) )
        self.assertEqual(client.get_partition_ids_for_topic.call_count, 0) # pylint: disable=no-member

class TestSimpleConsumer(unittest.TestCase):
    def test_simple_consumer_failed_payloads(self):
        client = MagicMock()
        consumer = SimpleConsumer(client, group=None,
                                  topic='topic', partitions=[0, 1],
                                  auto_commit=False)

        def failed_payloads(payload):
            return FailedPayloadsError(payload)

        client.send_fetch_request.side_effect = self.fail_requests_factory(failed_payloads)

        # This should not raise an exception
        consumer.get_messages(5)

    def test_simple_consumer_leader_change(self):
        client = MagicMock()
        consumer = SimpleConsumer(client, group=None,
                                  topic='topic', partitions=[0, 1],
                                  auto_commit=False)

        # Mock so that only the first request gets a valid response
        def not_leader(request):
            return FetchResponse(request.topic, request.partition,
                                 NotLeaderForPartitionError.errno, -1, ())

        client.send_fetch_request.side_effect = self.fail_requests_factory(not_leader)

        # This should not raise an exception
        consumer.get_messages(20)

        # client should have updated metadata
        self.assertGreaterEqual(client.reset_topic_metadata.call_count, 1)
        self.assertGreaterEqual(client.load_metadata_for_topics.call_count, 1)

    def test_simple_consumer_unknown_topic_partition(self):
        client = MagicMock()
        consumer = SimpleConsumer(client, group=None,
                                  topic='topic', partitions=[0, 1],
                                  auto_commit=False)

        # Mock so that only the first request gets a valid response
        def unknown_topic_partition(request):
            return FetchResponse(request.topic, request.partition,
                                 UnknownTopicOrPartitionError.errno, -1, ())

        client.send_fetch_request.side_effect = self.fail_requests_factory(unknown_topic_partition)

        # This should not raise an exception
        with self.assertRaises(UnknownTopicOrPartitionError):
            consumer.get_messages(20)

    def test_simple_consumer_commit_does_not_raise(self):
        client = MagicMock()
        client.get_partition_ids_for_topic.return_value = [0, 1]

        def mock_offset_fetch_request(group, payloads, **kwargs):
            return [OffsetFetchResponse(p.topic, p.partition, 0, b'', 0) for p in payloads]

        client.send_offset_fetch_request.side_effect = mock_offset_fetch_request

        def mock_offset_commit_request(group, payloads, **kwargs):
            raise FailedPayloadsError(payloads[0])

        client.send_offset_commit_request.side_effect = mock_offset_commit_request

        consumer = SimpleConsumer(client, group='foobar',
                                  topic='topic', partitions=[0, 1],
                                  auto_commit=False)

        # Mock internal commit check
        consumer.count_since_commit = 10

        # This should not raise an exception
        self.assertFalse(consumer.commit(partitions=[0, 1]))

    def test_simple_consumer_reset_partition_offset(self):
        client = MagicMock()

        def mock_offset_request(payloads, **kwargs):
            raise FailedPayloadsError(payloads[0])

        client.send_offset_request.side_effect = mock_offset_request

        consumer = SimpleConsumer(client, group='foobar',
                                  topic='topic', partitions=[0, 1],
                                  auto_commit=False)

        # This should not raise an exception
        self.assertEqual(consumer.reset_partition_offset(0), None)

    @staticmethod
    def fail_requests_factory(error_factory):
        # Mock so that only the first request gets a valid response
        def fail_requests(payloads, **kwargs):
            responses = [
                FetchResponse(payloads[0].topic, payloads[0].partition, 0, 0,
                              (OffsetAndMessage(
                                  payloads[0].offset + i,
                                  "msg %d" % (payloads[0].offset + i))
                               for i in range(10))),
            ]
            for failure in payloads[1:]:
                responses.append(error_factory(failure))
            return responses
        return fail_requests
