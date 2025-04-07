# pylint: skip-file
from __future__ import absolute_import

import collections
import io

import pytest
from kafka.vendor import six

from kafka.client_async import KafkaClient
from kafka.protocol.broker_api_versions import BROKER_API_VERSIONS
from kafka.producer.kafka import KafkaProducer
from kafka.protocol.produce import ProduceRequest
from kafka.producer.record_accumulator import RecordAccumulator, ProducerBatch
from kafka.producer.sender import Sender
from kafka.record.memory_records import MemoryRecordsBuilder
from kafka.structs import TopicPartition


@pytest.fixture
def accumulator():
    return RecordAccumulator()


@pytest.fixture
def sender(client, accumulator):
    return Sender(client, client.cluster, accumulator)


@pytest.mark.parametrize(("api_version", "produce_version"), [
    ((2, 1), 7),
    ((0, 10, 0), 2),
    ((0, 9), 1),
    ((0, 8, 0), 0)
])
def test_produce_request(sender, api_version, produce_version):
    sender._client._api_versions = BROKER_API_VERSIONS[api_version]
    tp = TopicPartition('foo', 0)
    magic = KafkaProducer.max_usable_produce_magic(api_version)
    records = MemoryRecordsBuilder(
        magic=1, compression_type=0, batch_size=100000)
    batch = ProducerBatch(tp, records)
    records.close()
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
            records = MemoryRecordsBuilder(
                magic=1, compression_type=0, batch_size=100000)
            batches_by_node[node].append(ProducerBatch(tp, records))
            records.close()

    produce_requests_by_node = sender._create_produce_requests(batches_by_node)
    assert len(produce_requests_by_node) == 3
    for node in range(3):
        assert isinstance(produce_requests_by_node[node], ProduceRequest[produce_version])
