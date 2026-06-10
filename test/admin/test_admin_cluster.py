import pytest


def test_describe_cluster(admin):
    """On modern brokers (default fixture is (4, 2)), describe_cluster
    uses DescribeClusterRequest and surfaces is_fenced per broker."""
    result = admin.describe_cluster()
    assert result['cluster_id'] == 'mock-cluster'
    assert result['controller_id'] == 0
    assert 'topics' not in result
    assert 'throttle_time_ms' not in result
    assert 'endpoint_type' not in result
    assert len(result['brokers']) == 1
    broker_info = result['brokers'][0]
    assert broker_info['broker_id'] == 0
    assert broker_info['host'] == 'localhost'
    assert broker_info['port'] == 9092
    assert broker_info['is_fenced'] is False


@pytest.mark.parametrize("broker", [(2, 7)], indirect=True)
def test_describe_cluster_fallback_to_metadata(admin):
    """On brokers that don't advertise DescribeCluster (pre-KIP-700),
    describe_cluster() falls back to MetadataRequest and reshapes the
    result to look like the DescribeCluster path (node_id -> broker_id)."""
    result = admin.describe_cluster()
    assert result['cluster_id'] == 'mock-cluster'
    assert result['controller_id'] == 0
    assert 'topics' not in result
    assert 'throttle_time_ms' not in result
    assert len(result['brokers']) == 1
    broker_info = result['brokers'][0]
    assert broker_info['broker_id'] == 0
    assert 'node_id' not in broker_info
    assert broker_info['host'] == 'localhost'
    assert broker_info['port'] == 9092
