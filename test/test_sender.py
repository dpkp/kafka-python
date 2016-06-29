# pylint: skip-file
from __future__ import absolute_import

import io

import pytest

from kafka.client_async import KafkaClient
from kafka.cluster import ClusterMetadata
from kafka.producer.buffer import MessageSetBuffer
from kafka.producer.sender import Sender
from kafka.producer.record_accumulator import RecordAccumulator, RecordBatch
import kafka.errors as Errors
from kafka.future import Future
from kafka.protocol.produce import ProduceRequest
from kafka.structs import TopicPartition, OffsetAndMetadata


@pytest.fixture
def client(mocker):
    _cli = mocker.Mock(spec=KafkaClient(bootstrap_servers=[]))
    _cli.cluster = mocker.Mock(spec=ClusterMetadata())
    return _cli


@pytest.fixture
def accumulator():
    return RecordAccumulator()


@pytest.fixture
def sender(client, accumulator):
    return Sender(client, client.cluster, accumulator)


@pytest.mark.parametrize(("api_version", "produce_version"), [
    ((0, 10), 2),
    ((0, 9), 1),
    ((0, 8), 0)
])
def test_produce_request(sender, mocker, api_version, produce_version):
    sender.config['api_version'] = api_version
    tp = TopicPartition('foo', 0)
    records = MessageSetBuffer(io.BytesIO(), 100000)
    batch = RecordBatch(tp, records)
    produce_request = sender._produce_request(0, 0, 0, [batch])
    assert isinstance(produce_request, ProduceRequest[produce_version])
