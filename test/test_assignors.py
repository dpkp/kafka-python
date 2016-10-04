# pylint: skip-file
from __future__ import absolute_import

import pytest

from kafka.coordinator.assignors.range import RangePartitionAssignor
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from kafka.coordinator.protocol import (
    ConsumerProtocolMemberMetadata, ConsumerProtocolMemberAssignment)


@pytest.fixture
def cluster(mocker):
    cluster = mocker.MagicMock()
    cluster.partitions_for_topic.return_value = set([0, 1, 2])
    return cluster


def test_assignor_roundrobin(cluster):
    assignor = RoundRobinPartitionAssignor

    member_metadata = {
        'C0': assignor.metadata(set(['t0', 't1'])),
        'C1': assignor.metadata(set(['t0', 't1'])),
    }

    ret = assignor.assign(cluster, member_metadata)
    expected = {
        'C0': ConsumerProtocolMemberAssignment(
            assignor.version, [('t0', [0, 2]), ('t1', [1])], b''),
        'C1': ConsumerProtocolMemberAssignment(
            assignor.version, [('t0', [1]), ('t1', [0, 2])], b'')
    }
    assert ret == expected
    assert set(ret) == set(expected)
    for member in ret:
        assert ret[member].encode() == expected[member].encode()


def test_assignor_range(cluster):
    assignor = RangePartitionAssignor

    member_metadata = {
        'C0': assignor.metadata(set(['t0', 't1'])),
        'C1': assignor.metadata(set(['t0', 't1'])),
    }

    ret = assignor.assign(cluster, member_metadata)
    expected = {
        'C0': ConsumerProtocolMemberAssignment(
            assignor.version, [('t0', [0, 1]), ('t1', [0, 1])], b''),
        'C1': ConsumerProtocolMemberAssignment(
            assignor.version, [('t0', [2]), ('t1', [2])], b'')
    }
    assert ret == expected
    assert set(ret) == set(expected)
    for member in ret:
        assert ret[member].encode() == expected[member].encode()
