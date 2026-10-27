import gc
import platform
import threading
from array import array
from ctypes import c_uint32
from unittest.mock import MagicMock

import pytest

from kafka import KafkaProducer
from kafka.errors import KafkaConfigurationError, MessageSizeTooLargeError
from kafka.partitioner import DefaultPartitioner, Partitioner, StickyPartitioner
from kafka.producer.transaction_manager import TransactionManager, ProducerIdAndEpoch
from kafka.record.memory_records import MemoryRecordsBuilder

from test.mock_broker import MockBroker


def test_default_api_timeout_smaller_than_request_timeout_raises():
    # Validation runs before any network/bootstrap, so no broker is needed.
    with pytest.raises(KafkaConfigurationError):
        KafkaProducer(bootstrap_servers='localhost:9092', api_version=(0, 9),
                      request_timeout_ms=70000, default_api_timeout_ms=60000)


def _mock_producer(**configs):
    """A KafkaProducer wired to a fresh MockBroker (no real network).

    Defaults to a non-idempotent producer (no InitProducerId traffic). Use
    as a context manager so close() joins the Sender thread.
    """
    broker = MockBroker(broker_version=(4, 3))
    configs.setdefault('api_version', (4, 3))
    configs.setdefault('enable_idempotence', False)
    return KafkaProducer(
        kafka_client=broker.client_factory(),
        bootstrap_servers=['%s:%d' % (broker.host, broker.port)],
        **configs)


def test_kafka_producer_thread_close():
    # Explicit close() (rather than the context manager) is the behavior under
    # test here -- it must join the Sender thread and return the thread count
    # to baseline.
    threads = threading.active_count()
    producer = _mock_producer()
    assert threading.active_count() == threads + 1
    producer.close()
    assert threading.active_count() == threads


def test_kafka_producer_context_manager_closes_on_exit():
    threads = threading.active_count()
    with _mock_producer() as producer:
        assert threading.active_count() == threads + 1
        assert producer._closed is False
    assert producer._closed is True
    assert threading.active_count() == threads


def test_partition_calls_partitioner_partition_with_cluster():
    """_partition routes through partitioner.partition(topic, key, cluster)
    - the new signature passes the ClusterMetadata directly so the
    partitioner can call available_partitions_for_topic / topics() itself."""
    producer = KafkaProducer.__new__(KafkaProducer)
    producer._metadata = MagicMock()
    producer._metadata.topics.return_value = {'t'}
    producer._metadata.partitions_for_topic.return_value = {0, 1, 2}
    producer._metadata.available_partitions_for_topic.return_value = {0, 1, 2}

    partitioner = MagicMock(spec=Partitioner)
    partitioner.partition.return_value = 1
    producer.config = {'partitioner': partitioner}

    result = producer._partition('t', None, None, None, b'key-bytes', b'val')
    assert result == 1
    partitioner.partition.assert_called_once_with('t', None, b'key-bytes', None, b'val', producer._metadata)


def test_partition_explicit_partition_skips_partitioner():
    """Explicit partition= argument bypasses the partitioner entirely.
    The partition must still be in the topic's known set."""
    producer = KafkaProducer.__new__(KafkaProducer)
    producer._metadata = MagicMock()
    producer._metadata.topics.return_value = {'t'}
    producer._metadata.partitions_for_topic.return_value = {0, 1, 2}
    partitioner = MagicMock()
    producer.config = {'partitioner': partitioner}

    assert producer._partition('t', 1, None, None, b'k', b'v') == 1
    partitioner.partition.assert_not_called()


def test_partition_explicit_partition_rejects_unknown_partition():
    producer = KafkaProducer.__new__(KafkaProducer)
    producer._metadata = MagicMock()
    producer._metadata.topics.return_value = {'t'}
    producer._metadata.partitions_for_topic.return_value = {0, 1, 2}
    producer.config = {'partitioner': MagicMock()}
    with pytest.raises(ValueError):
        producer._partition('t', 99, None, None, b'k', b'v')


def _producer_for_send_test(partitioner):
    """Build a real KafkaProducer but replace the accumulator + sender
    with mocks so ``send()`` doesn't try to actually push data.

    __init__ already starts the real Sender coroutine on the IO thread; we stop
    it and wait on its loop Future before swapping in the mock so it isn't
    orphaned (close() would otherwise act on the mock). MockBroker keeps it off
    the real network."""
    producer = _mock_producer(partitioner=partitioner)
    producer._sender.initiate_close()
    producer._net.wait_for(producer._sender._loop_future, 2000)
    producer._accumulator = MagicMock()
    producer._sender = MagicMock()
    # close() now blocks on the sender's loop Future; give the mock an
    # already-resolved one (and is_running()==False) so teardown doesn't hang.
    from kafka.future import Future
    _done = Future()
    _done.success(None)
    producer._sender._loop_future = _done
    producer._sender.is_running.return_value = False
    producer._metadata = MagicMock()
    producer._metadata.topics.return_value = {'t'}
    producer._metadata.partitions_for_topic.return_value = set(range(20))
    producer._metadata.available_partitions_for_topic.return_value = set(range(20))
    return producer


def _success_result():
    from kafka.producer.future import FutureRecordMetadata, FutureProduceResult
    from kafka.structs import TopicPartition
    return (FutureRecordMetadata(
                FutureProduceResult(TopicPartition('t', 0)),
                0, 0, 0, 0, 0, 0),
            False, True, False)


@pytest.mark.parametrize('magic,api_version', [(0, (0, 9)), (1, (0, 10)), (2, (0, 11))])
@pytest.mark.parametrize('field', ['key', 'value'])
@pytest.mark.parametrize('data_factory', [
    pytest.param(lambda: memoryview(array('H', [0x1234, 0x5678])), id='typed'),
    pytest.param(lambda: memoryview(bytearray(b'abcd')).cast('B', [2, 2]), id='multidimensional'),
    pytest.param(lambda: memoryview(c_uint32(0x12345678)), id='scalar'),
    pytest.param(lambda: memoryview(b'abcdef')[::2], id='strided-bytes'),
    pytest.param(lambda: memoryview(array('H', [1, 2, 3]))[::2], id='strided-typed'),
    pytest.param(lambda: memoryview(b'abcdef')[::-1], id='reversed'),
    pytest.param(lambda: memoryview(b'abcdef')[1:4], id='offset-byte-view'),
    pytest.param(lambda: memoryview(array('H')), id='empty-typed'),
    pytest.param(lambda: memoryview(b'bytes'), id='byte-view'),
    pytest.param(lambda: b'bytes', id='bytes'),
    pytest.param(lambda: bytearray(b'bytes'), id='bytearray'),
    pytest.param(lambda: None, id='none'),
])
def test_send_buffer_payload_matches_its_bytes(data_factory, field, magic, api_version):
    """Accepted views must reach partitioning and record encoding as bytes."""
    original = data_factory()
    data = {'key': b'key', 'value': b'value'}
    data[field] = original
    expected = {name: bytes(value) if value is not None else None for name, value in data.items()}
    partitioner = DefaultPartitioner()
    with _producer_for_send_test(partitioner) as producer:
        producer.config['api_version'] = api_version
        producer._accumulator.append.return_value = _success_result()
        producer.send('t', timestamp_ms=1000, **data)
        tp, timestamp, key, value, headers = producer._accumulator.append.call_args.args

        # Exercise actual wire encoding/decoding after the public send path.
        # The broker and accumulator are test doubles; this isn't a broker test.
        builder = MemoryRecordsBuilder(magic, compression_type=0, batch_size=10000)
        builder.append(timestamp, key, value, headers)
        builder.close()
        batch = builder.records().next_batch()
        assert batch.validate_crc()
        record, = list(batch)
        assert record.key == expected['key']
        assert record.value == expected['value']
        assert producer._estimate_size_in_bytes(key, value) == producer._estimate_size_in_bytes(
            expected['key'], expected['value'])
        if expected['key'] is not None:
            assert tp.partition == partitioner.partition(
                't', data['key'], expected['key'], data['value'], expected['value'], producer._metadata)

        serialized = key if field == 'key' else value
        if isinstance(original, memoryview):
            assert bytes(serialized) == bytes(original)
            if original.c_contiguous:
                assert isinstance(serialized, memoryview)
                assert serialized.obj is original.obj
        else:
            assert serialized is original


@pytest.mark.parametrize('api_version', [(0, 9), (0, 10), (0, 11)])
def test_send_memoryview_checks_byte_size_limit(api_version):
    """Typed views must not bypass the configured serialized record size bound."""
    value = memoryview(array('H', range(100)))
    with _producer_for_send_test(DefaultPartitioner()) as producer:
        producer.config['api_version'] = api_version
        producer.config['max_request_size'] = producer._estimate_size_in_bytes(b'key', bytes(value)) - 1
        producer._accumulator.append.return_value = _success_result()
        with pytest.raises(MessageSizeTooLargeError):
            producer.send('t', key=b'key', value=value)
        producer._accumulator.append.assert_not_called()


def test_send_null_key_triggers_on_new_batch_via_abort_retry():
    """KIP-480 Java-faithful flow: a null-key send whose accumulator has
    no in-progress batch must invoke ``partitioner.on_new_batch`` (rotate
    the sticky) and re-pick the partition before the actual append,
    matching KafkaProducer.doSend's abort-for-new-batch retry path."""
    partitioner = MagicMock(spec=StickyPartitioner)
    partitioner.partition.side_effect = [3, 7]  # initial pick, post-rotate
    abort = (None, False, False, True)

    with _producer_for_send_test(partitioner) as producer:
        producer._accumulator.append.side_effect = [abort, _success_result()]
        producer.send('t', value=b'msg')
        # Initial pick + post-rotate re-pick.
        assert partitioner.partition.call_count == 2
        # on_new_batch fired exactly once, with the cluster metadata and
        # the *initial* sticky.
        partitioner.on_new_batch.assert_called_once_with(
            't', producer._metadata, 3)
        # Two appends: first aborted, second landed the record on partition 7.
        assert producer._accumulator.append.call_count == 2
        second_call = producer._accumulator.append.call_args_list[1]
        tp_arg = second_call.args[0]
        assert tp_arg.partition == 7


def test_send_keyed_skips_on_new_batch():
    """Keyed records bypass the sticky abort-retry path - on_new_batch
    must not fire."""
    partitioner = MagicMock(spec=StickyPartitioner)
    partitioner.partition.return_value = 0

    with _producer_for_send_test(partitioner) as producer:
        producer._accumulator.append.return_value = _success_result()
        producer.send('t', key=b'k', value=b'v')
        partitioner.on_new_batch.assert_not_called()
        # Keyed records pass abort_on_new_batch=False directly - one append.
        assert producer._accumulator.append.call_count == 1
        kwargs = producer._accumulator.append.call_args.kwargs
        assert kwargs.get('abort_on_new_batch') is False


def test_send_with_explicit_partition_skips_on_new_batch():
    """Explicit partition overrides the partitioner entirely - no
    rotation hook should fire."""
    partitioner = MagicMock(spec=StickyPartitioner)

    with _producer_for_send_test(partitioner) as producer:
        producer._accumulator.append.return_value = _success_result()
        producer.send('t', value=b'v', partition=1)
        partitioner.partition.assert_not_called()
        partitioner.on_new_batch.assert_not_called()
        # Explicit partition also goes straight to abort_on_new_batch=False.
        kwargs = producer._accumulator.append.call_args.kwargs
        assert kwargs.get('abort_on_new_batch') is False


def test_idempotent_producer_reset_producer_id(cluster):
    transaction_manager = TransactionManager(
        transactional_id=None,
        transaction_timeout_ms=1000,
        retry_backoff_ms=100,
        api_version=(4, 3),
        metadata=cluster,
    )

    test_producer_id_and_epoch = ProducerIdAndEpoch(123, 456)
    transaction_manager.set_producer_id_and_epoch(test_producer_id_and_epoch)
    assert transaction_manager.producer_id_and_epoch == test_producer_id_and_epoch
    transaction_manager.reset_producer_id()
    assert transaction_manager.producer_id_and_epoch == ProducerIdAndEpoch(-1, -1)
