# pylint: skip-file
from __future__ import absolute_import

import collections
import io
import time

import pytest
try:
    from unittest.mock import call
except ImportError:
    from mock import call

from kafka.vendor import six

from kafka.client_async import KafkaClient
from kafka.cluster import ClusterMetadata
import kafka.errors as Errors
from kafka.protocol.broker_api_versions import BROKER_API_VERSIONS
from kafka.producer.kafka import KafkaProducer
from kafka.protocol.produce import ProduceRequest
from kafka.producer.record_accumulator import RecordAccumulator, ProducerBatch
from kafka.producer.sender import Sender
from kafka.producer.transaction_manager import TransactionManager
from kafka.record.memory_records import MemoryRecordsBuilder
from kafka.structs import TopicPartition


@pytest.fixture
def accumulator():
    return RecordAccumulator()


@pytest.fixture
def sender(client, accumulator):
    return Sender(client, client.cluster, accumulator)


def producer_batch(topic='foo', partition=0, magic=2):
    tp = TopicPartition(topic, partition)
    records = MemoryRecordsBuilder(
        magic=magic, compression_type=0, batch_size=100000)
    batch = ProducerBatch(tp, records)
    batch.try_append(0, None, b'msg', [])
    batch.records.close()
    return batch


@pytest.fixture
def transaction_manager():
    return TransactionManager(
        transactional_id=None,
        transaction_timeout_ms=60000,
        retry_backoff_ms=100,
        api_version=(2, 1),
        metadata=ClusterMetadata())


@pytest.mark.parametrize(("api_version", "produce_version"), [
    ((2, 1), 7),
    ((0, 10, 0), 2),
    ((0, 9), 1),
    ((0, 8, 0), 0)
])
def test_produce_request(sender, api_version, produce_version):
    sender._client._api_versions = BROKER_API_VERSIONS[api_version]
    magic = KafkaProducer.max_usable_produce_magic(api_version)
    batch = producer_batch(magic=magic)
    produce_request = sender._produce_request(0, 0, 0, [batch])
    assert isinstance(produce_request, ProduceRequest[produce_version])


@pytest.mark.parametrize(("api_version", "produce_version"), [
    ((2, 1), 7),
])
def test_create_produce_requests(sender, api_version, produce_version):
    sender._client._api_versions = BROKER_API_VERSIONS[api_version]
    tp = TopicPartition('foo', 0)
    magic = KafkaProducer.max_usable_produce_magic(api_version)
    batches_by_node = collections.defaultdict(list)
    for node in range(3):
        for _ in range(5):
            batches_by_node[node].append(producer_batch(magic=magic))
    produce_requests_by_node = sender._create_produce_requests(batches_by_node)
    assert len(produce_requests_by_node) == 3
    for node in range(3):
        assert isinstance(produce_requests_by_node[node], ProduceRequest[produce_version])


def test_complete_batch_success(sender):
    batch = producer_batch()
    assert not batch.produce_future.is_done

    # No error, base_offset 0
    sender._complete_batch(batch, None, 0, timestamp_ms=123)
    assert batch.is_done
    assert batch.produce_future.is_done
    assert batch.produce_future.succeeded()
    assert batch.produce_future.value == (0, 123)


def test_complete_batch_transaction(sender, transaction_manager):
    sender._transaction_manager = transaction_manager
    batch = producer_batch()
    assert sender._transaction_manager.sequence_number(batch.topic_partition) == 0
    assert sender._transaction_manager.producer_id_and_epoch.producer_id == batch.producer_id

    # No error, base_offset 0
    sender._complete_batch(batch, None, 0)
    assert batch.is_done
    assert sender._transaction_manager.sequence_number(batch.topic_partition) == batch.record_count


@pytest.mark.parametrize(("error", "refresh_metadata"), [
    (Errors.KafkaConnectionError, True),
    (Errors.CorruptRecordError, False),
    (Errors.UnknownTopicOrPartitionError, True),
    (Errors.NotLeaderForPartitionError, True),
    (Errors.MessageSizeTooLargeError, False),
    (Errors.InvalidTopicError, False),
    (Errors.RecordListTooLargeError, False),
    (Errors.NotEnoughReplicasError, False),
    (Errors.NotEnoughReplicasAfterAppendError, False),
    (Errors.InvalidRequiredAcksError, False),
    (Errors.TopicAuthorizationFailedError, False),
    (Errors.UnsupportedForMessageFormatError, False),
    (Errors.InvalidProducerEpochError, False),
    (Errors.ClusterAuthorizationFailedError, False),
    (Errors.TransactionalIdAuthorizationFailedError, False),
])
def test_complete_batch_error(sender, error, refresh_metadata):
    sender._client.cluster._last_successful_refresh_ms = (time.time() - 10) * 1000
    sender._client.cluster._need_update = False
    sender.config['retries'] = 0
    assert sender._client.cluster.ttl() > 0
    batch = producer_batch()
    sender._complete_batch(batch, error, -1)
    if refresh_metadata:
        assert sender._client.cluster.ttl() == 0
    else:
        assert sender._client.cluster.ttl() > 0
    assert batch.is_done
    assert batch.produce_future.failed()
    assert isinstance(batch.produce_future.exception, error)


@pytest.mark.parametrize(("error", "retry"), [
    (Errors.KafkaConnectionError, True),
    (Errors.CorruptRecordError, False),
    (Errors.UnknownTopicOrPartitionError, True),
    (Errors.NotLeaderForPartitionError, True),
    (Errors.MessageSizeTooLargeError, False),
    (Errors.InvalidTopicError, False),
    (Errors.RecordListTooLargeError, False),
    (Errors.NotEnoughReplicasError, True),
    (Errors.NotEnoughReplicasAfterAppendError, True),
    (Errors.InvalidRequiredAcksError, False),
    (Errors.TopicAuthorizationFailedError, False),
    (Errors.UnsupportedForMessageFormatError, False),
    (Errors.InvalidProducerEpochError, False),
    (Errors.ClusterAuthorizationFailedError, False),
    (Errors.TransactionalIdAuthorizationFailedError, False),
])
def test_complete_batch_retry(sender, accumulator, mocker, error, retry):
    sender.config['retries'] = 1
    mocker.spy(sender, '_fail_batch')
    mocker.patch.object(accumulator, 'reenqueue')
    batch = producer_batch()
    sender._complete_batch(batch, error, -1)
    if retry:
        assert not batch.is_done
        accumulator.reenqueue.assert_called_with(batch)
        batch.attempts += 1 # normally handled by accumulator.reenqueue, but it's mocked
        sender._complete_batch(batch, error, -1)
        assert batch.is_done
        assert isinstance(batch.produce_future.exception, error)
    else:
        assert batch.is_done
        assert isinstance(batch.produce_future.exception, error)


def test_complete_batch_producer_id_changed_no_retry(sender, accumulator, transaction_manager, mocker):
    sender._transaction_manager = transaction_manager
    sender.config['retries'] = 1
    mocker.spy(sender, '_fail_batch')
    mocker.patch.object(accumulator, 'reenqueue')
    error = Errors.NotLeaderForPartitionError
    batch = producer_batch()
    sender._complete_batch(batch, error, -1)
    assert not batch.is_done
    accumulator.reenqueue.assert_called_with(batch)
    batch.records._producer_id = 123 # simulate different producer_id
    assert batch.producer_id != sender._transaction_manager.producer_id_and_epoch.producer_id
    sender._complete_batch(batch, error, -1)
    assert batch.is_done
    assert isinstance(batch.produce_future.exception, error)


def test_fail_batch(sender, accumulator, transaction_manager, mocker):
    sender._transaction_manager = transaction_manager
    batch = producer_batch()
    mocker.patch.object(batch, 'done')
    assert sender._transaction_manager.producer_id_and_epoch.producer_id == batch.producer_id
    error = Exception('error')
    sender._fail_batch(batch, base_offset=0, timestamp_ms=None, exception=error)
    batch.done.assert_called_with(base_offset=0, timestamp_ms=None, exception=error)


def test_out_of_order_sequence_number_reset_producer_id(sender, accumulator, transaction_manager, mocker):
    sender._transaction_manager = transaction_manager
    assert transaction_manager.transactional_id is None # this test is for idempotent producer only
    mocker.patch.object(TransactionManager, 'reset_producer_id')
    batch = producer_batch()
    mocker.patch.object(batch, 'done')
    assert sender._transaction_manager.producer_id_and_epoch.producer_id == batch.producer_id
    error = Errors.OutOfOrderSequenceNumberError()
    sender._fail_batch(batch, base_offset=0, timestamp_ms=None, exception=error)
    sender._transaction_manager.reset_producer_id.assert_called_once()
    batch.done.assert_called_with(base_offset=0, timestamp_ms=None, exception=error)


def test_handle_produce_response():
    pass


def test_failed_produce(sender, mocker):
    mocker.patch.object(sender, '_complete_batch')
    mock_batches = ['foo', 'bar', 'fizzbuzz']
    sender._failed_produce(mock_batches, 0, 'error')
    sender._complete_batch.assert_has_calls([
        call('foo', 'error', -1),
        call('bar', 'error', -1),
        call('fizzbuzz', 'error', -1),
    ])


def test_maybe_wait_for_producer_id():
    pass


def test_run_once():
    pass


def test__send_producer_data_expiry_time_reset(sender, accumulator, mocker):
    now = time.time()
    tp = TopicPartition('foo', 0)
    mocker.patch.object(sender, '_failed_produce')
    result = accumulator.append(tp, 0, b'key', b'value', [], now=now)
    poll_timeout_ms = sender._send_producer_data(now=now)
    assert poll_timeout_ms == accumulator.config['delivery_timeout_ms']
    sender._failed_produce.assert_not_called()
    now += accumulator.config['delivery_timeout_ms']
    poll_timeout_ms = sender._send_producer_data(now=now)
    assert poll_timeout_ms > 0
