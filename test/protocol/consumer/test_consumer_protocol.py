import pytest

from kafka.protocol.consumer.metadata import *


@pytest.mark.parametrize('version', range(ConsumerProtocolAssignment.min_version, ConsumerProtocolAssignment.max_version + 1))
def test_consumer_protocol_assignment(version):
    assignment = ConsumerProtocolAssignment(
        assigned_partitions=[('t0', [0, 2]), ('t1', [1])],
        user_data=b'foo\x12',
        version=version,
    )
    encoded = assignment.encode()
    decoded = ConsumerProtocolAssignment.decode(encoded)
    assert decoded == assignment
    assert decoded.version == version
    assert len(decoded.assigned_partitions) == 2
    assert decoded.assigned_partitions[0].topic == 't0'
    assert decoded.assigned_partitions[0].partitions == [0, 2]
    assert decoded.assigned_partitions[1].topic == 't1'
    assert decoded.assigned_partitions[1].partitions == [1]
    assert decoded.user_data == b'foo\x12'


@pytest.mark.parametrize('version', range(ConsumerProtocolSubscription.min_version, ConsumerProtocolSubscription.max_version + 1))
def test_consumer_protocol_subscription(version):
    topics = ['t0', 't1']
    user_data = b'foo\x12'
    subscription = ConsumerProtocolSubscription(
        topics=topics,
        user_data=user_data,
        version=version,
    )
    encoded = subscription.encode()
    decoded = ConsumerProtocolSubscription.decode(encoded)
    assert decoded == subscription
    assert decoded.version == version
    assert decoded.topics == topics
    assert decoded.user_data == user_data


def test_consumer_protocol_type():
    assert ConsumerProtocolType == 'consumer'
