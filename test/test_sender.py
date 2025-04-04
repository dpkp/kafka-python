# pylint: skip-file
from __future__ import absolute_import

import pytest
import io

from kafka.client_async import KafkaClient
from kafka.protocol.broker_api_versions import BROKER_API_VERSIONS
from kafka.protocol.produce import ProduceRequest
from kafka.producer.record_accumulator import RecordAccumulator, ProducerBatch
from kafka.producer.sender import Sender
from kafka.record.memory_records import MemoryRecordsBuilder
from kafka.structs import TopicPartition


@pytest.fixture
def accumulator():
    return RecordAccumulator()


@pytest.fixture
def sender(client, accumulator, metrics, mocker):
    return Sender(client, client.cluster, accumulator, metrics=metrics)


@pytest.mark.parametrize(("api_version", "produce_version"), [
    ((0, 10, 0), 2),
    ((0, 9), 1),
    ((0, 8, 0), 0)
])
def test_produce_request(sender, mocker, api_version, produce_version):
    sender._client._api_versions = BROKER_API_VERSIONS[api_version]
    tp = TopicPartition('foo', 0)
    records = MemoryRecordsBuilder(
        magic=1, compression_type=0, batch_size=100000)
    batch = ProducerBatch(tp, records)
    records.close()
    produce_request = sender._produce_request(0, 0, 0, [batch])
    assert isinstance(produce_request, ProduceRequest[produce_version])
