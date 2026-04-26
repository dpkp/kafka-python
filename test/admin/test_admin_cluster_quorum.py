import pytest

import kafka.errors as Errors
from kafka.protocol.admin import DescribeQuorumRequest, DescribeQuorumResponse


def _quorum_response(
    *,
    error_code=0,
    error_message=None,
    leader_id=1,
    leader_epoch=5,
    high_watermark=100,
    voter_ids=(1, 2, 3),
    observer_ids=(),
    nodes=(),
    partition_error_code=0,
    partition_error_message=None,
):
    Topic = DescribeQuorumResponse.TopicData
    Part = Topic.PartitionData
    State = Part.ReplicaState
    voters = [State(replica_id=rid,
                    replica_directory_id='00000000-0000-0000-0000-000000000000',
                    log_end_offset=high_watermark,
                    last_fetch_timestamp=-1,
                    last_caught_up_timestamp=-1)
              for rid in voter_ids]
    observers = [State(replica_id=rid,
                       replica_directory_id='00000000-0000-0000-0000-000000000000',
                       log_end_offset=high_watermark,
                       last_fetch_timestamp=-1,
                       last_caught_up_timestamp=-1)
                 for rid in observer_ids]
    return DescribeQuorumResponse(
        error_code=error_code,
        error_message=error_message,
        topics=[Topic(topic_name='__cluster_metadata', partitions=[
            Part(partition_index=0,
                 error_code=partition_error_code,
                 error_message=partition_error_message,
                 leader_id=leader_id, leader_epoch=leader_epoch,
                 high_watermark=high_watermark,
                 current_voters=voters, observers=observers),
        ])],
        nodes=list(nodes),
    )


class TestDescribeMetadataQuorumMockBroker:

    def test_returns_quorum_state(self, broker, admin):
        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            captured['request'] = DescribeQuorumRequest.decode(
                request_bytes, version=api_version, header=True)
            return _quorum_response(leader_id=2, leader_epoch=7,
                                    high_watermark=42,
                                    voter_ids=(1, 2, 3), observer_ids=(4,))

        broker.respond_fn(DescribeQuorumRequest, handler)

        result = admin.describe_metadata_quorum()

        req = captured['request']
        assert len(req.topics) == 1
        assert req.topics[0].topic_name == '__cluster_metadata'
        assert [p.partition_index for p in req.topics[0].partitions] == [0]

        assert 'error_code' not in result
        assert 'error_message' not in result
        partition = result['topics'][0]['partitions'][0]
        assert partition['error'] is None
        assert partition['leader_id'] == 2
        assert partition['leader_epoch'] == 7
        assert partition['high_watermark'] == 42
        assert [v['replica_id'] for v in partition['current_voters']] == [1, 2, 3]
        assert [o['replica_id'] for o in partition['observers']] == [4]

    def test_top_level_error_raises(self, broker, admin):
        broker.respond(
            DescribeQuorumRequest,
            _quorum_response(
                error_code=Errors.ClusterAuthorizationFailedError.errno,
                error_message='nope'))

        with pytest.raises(Errors.ClusterAuthorizationFailedError):
            admin.describe_metadata_quorum()

    def test_partition_level_error_returned(self, broker, admin):
        broker.respond(
            DescribeQuorumRequest,
            _quorum_response(
                partition_error_code=Errors.ClusterAuthorizationFailedError.errno,
                partition_error_message='nope'))

        result = admin.describe_metadata_quorum()
        assert result['topics'][0]['partitions'][0]['error'] == str(Errors.ClusterAuthorizationFailedError('nope'))
