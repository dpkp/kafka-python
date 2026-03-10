import pytest

from kafka.protocol.parser import KafkaProtocol

from kafka.protocol.api_versions import ApiVersionsRequest, ApiVersionsResponse
from kafka.protocol.commit import OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse
from kafka.protocol.find_coordinator import FindCoordinatorRequest, FindCoordinatorResponse
from kafka.protocol.group import JoinGroupRequest, JoinGroupResponse, SyncGroupRequest, SyncGroupResponse, LeaveGroupRequest, LeaveGroupResponse, HeartbeatRequest, HeartbeatResponse
from kafka.protocol.metadata import MetadataRequest, MetadataResponse
from kafka.version import __version__


TEST_API_VERSIONS_1 = {
    'client_id': '_internal_client_kYVL',
    'messages': (
        {
            'request': (
                ApiVersionsRequest[4](client_software_name='kafka-python', client_software_version=__version__, tags={}),
                b'\x00\x00\x004\x00\x12\x00\x04\x00\x00\x00\x01\x00\x15_internal_client_kYVL\x00\rkafka-python\x062.3.0\x00',
            ),
            'response': (
                b'\x00\x00\x00\x10\x00\x00\x00\x01\x00#\x00\x00\x00\x01\x00\x12\x00\x00\x00\x03',
                1,
                ApiVersionsResponse[0](error_code=35, api_versions=[(18, 0, 3)]) # Note different class on error - special case
            ),
        },
        {
            'request': (
                ApiVersionsRequest[3](client_software_name='kafka-python', client_software_version=__version__, tags={}),
                b'\x00\x00\x004\x00\x12\x00\x03\x00\x00\x00\x02\x00\x15_internal_client_kYVL\x00\rkafka-python\x062.3.0\x00',
            ),
            'response': (
                b'\x00\x00\x01j\x00\x00\x00\x02\x00\x003\x00\x00\x00\x00\x00\x08\x00\x00\x01\x00\x00\x00\x0b\x00\x00\x02\x00\x00\x00\x05\x00\x00\x03\x00\x00\x00\t\x00\x00\x04\x00\x00\x00\x04\x00\x00\x05\x00\x00\x00\x03\x00\x00\x06\x00\x00\x00\x06\x00\x00\x07\x00\x00\x00\x03\x00\x00\x08\x00\x00\x00\x08\x00\x00\t\x00\x00\x00\x07\x00\x00\n\x00\x00\x00\x03\x00\x00\x0b\x00\x00\x00\x07\x00\x00\x0c\x00\x00\x00\x04\x00\x00\r\x00\x00\x00\x04\x00\x00\x0e\x00\x00\x00\x05\x00\x00\x0f\x00\x00\x00\x05\x00\x00\x10\x00\x00\x00\x04\x00\x00\x11\x00\x00\x00\x01\x00\x00\x12\x00\x00\x00\x03\x00\x00\x13\x00\x00\x00\x05\x00\x00\x14\x00\x00\x00\x04\x00\x00\x15\x00\x00\x00\x02\x00\x00\x16\x00\x00\x00\x03\x00\x00\x17\x00\x00\x00\x03\x00\x00\x18\x00\x00\x00\x01\x00\x00\x19\x00\x00\x00\x01\x00\x00\x1a\x00\x00\x00\x01\x00\x00\x1b\x00\x00\x00\x00\x00\x00\x1c\x00\x00\x00\x03\x00\x00\x1d\x00\x00\x00\x02\x00\x00\x1e\x00\x00\x00\x02\x00\x00\x1f\x00\x00\x00\x02\x00\x00 \x00\x00\x00\x03\x00\x00!\x00\x00\x00\x01\x00\x00"\x00\x00\x00\x01\x00\x00#\x00\x00\x00\x02\x00\x00$\x00\x00\x00\x02\x00\x00%\x00\x00\x00\x02\x00\x00&\x00\x00\x00\x02\x00\x00\'\x00\x00\x00\x02\x00\x00(\x00\x00\x00\x02\x00\x00)\x00\x00\x00\x02\x00\x00*\x00\x00\x00\x02\x00\x00+\x00\x00\x00\x02\x00\x00,\x00\x00\x00\x01\x00\x00-\x00\x00\x00\x00\x00\x00.\x00\x00\x00\x00\x00\x00/\x00\x00\x00\x00\x00\x000\x00\x00\x00\x00\x00\x001\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00',
                2,
                ApiVersionsResponse[3](error_code=0, api_versions=[(0, 0, 8, {}), (1, 0, 11, {}), (2, 0, 5, {}), (3, 0, 9, {}), (4, 0, 4, {}), (5, 0, 3, {}), (6, 0, 6, {}), (7, 0, 3, {}), (8, 0, 8, {}), (9, 0, 7, {}), (10, 0, 3, {}), (11, 0, 7, {}), (12, 0, 4, {}), (13, 0, 4, {}), (14, 0, 5, {}), (15, 0, 5, {}), (16, 0, 4, {}), (17, 0, 1, {}), (18, 0, 3, {}), (19, 0, 5, {}), (20, 0, 4, {}), (21, 0, 2, {}), (22, 0, 3, {}), (23, 0, 3, {}), (24, 0, 1, {}), (25, 0, 1, {}), (26, 0, 1, {}), (27, 0, 0, {}), (28, 0, 3, {}), (29, 0, 2, {}), (30, 0, 2, {}), (31, 0, 2, {}), (32, 0, 3, {}), (33, 0, 1, {}), (34, 0, 1, {}), (35, 0, 2, {}), (36, 0, 2, {}), (37, 0, 2, {}), (38, 0, 2, {}), (39, 0, 2, {}), (40, 0, 2, {}), (41, 0, 2, {}), (42, 0, 2, {}), (43, 0, 2, {}), (44, 0, 1, {}), (45, 0, 0, {}), (46, 0, 0, {}), (47, 0, 0, {}), (48, 0, 0, {}), (49, 0, 0, {})], throttle_time_ms=0, tags={}),
            ),
        },
    ),
}


TEST_API_VERSIONS_2 = {
    'client_id': 'admin_client_VIsf',
    'messages': (
        {
            'request': (
                ApiVersionsRequest[4](client_software_name='kafka-python', client_software_version=__version__, tags={}),
                b'\x00\x00\x000\x00\x12\x00\x04\x00\x00\x00\x01\x00\x11admin_client_VIsf\x00\rkafka-python\x062.3.0\x00',
            ),
            'response': (
                b'\x00\x00\x00\x10\x00\x00\x00\x01\x00#\x00\x00\x00\x01\x00\x12\x00\x00\x00\x03',
                1,
                ApiVersionsResponse[0](error_code=35, api_versions=[(18, 0, 3)]) # Note different class on error - special case
            ),
        },
        {
            'request': (
                ApiVersionsRequest[3](client_software_name='kafka-python', client_software_version=__version__, tags={}),
                b'\x00\x00\x000\x00\x12\x00\x03\x00\x00\x00\x02\x00\x11admin_client_VIsf\x00\rkafka-python\x062.3.0\x00',
            ),
            'response': (
                b'\x00\x00\x01j\x00\x00\x00\x02\x00\x003\x00\x00\x00\x00\x00\x08\x00\x00\x01\x00\x00\x00\x0b\x00\x00\x02\x00\x00\x00\x05\x00\x00\x03\x00\x00\x00\t\x00\x00\x04\x00\x00\x00\x04\x00\x00\x05\x00\x00\x00\x03\x00\x00\x06\x00\x00\x00\x06\x00\x00\x07\x00\x00\x00\x03\x00\x00\x08\x00\x00\x00\x08\x00\x00\t\x00\x00\x00\x07\x00\x00\n\x00\x00\x00\x03\x00\x00\x0b\x00\x00\x00\x07\x00\x00\x0c\x00\x00\x00\x04\x00\x00\r\x00\x00\x00\x04\x00\x00\x0e\x00\x00\x00\x05\x00\x00\x0f\x00\x00\x00\x05\x00\x00\x10\x00\x00\x00\x04\x00\x00\x11\x00\x00\x00\x01\x00\x00\x12\x00\x00\x00\x03\x00\x00\x13\x00\x00\x00\x05\x00\x00\x14\x00\x00\x00\x04\x00\x00\x15\x00\x00\x00\x02\x00\x00\x16\x00\x00\x00\x03\x00\x00\x17\x00\x00\x00\x03\x00\x00\x18\x00\x00\x00\x01\x00\x00\x19\x00\x00\x00\x01\x00\x00\x1a\x00\x00\x00\x01\x00\x00\x1b\x00\x00\x00\x00\x00\x00\x1c\x00\x00\x00\x03\x00\x00\x1d\x00\x00\x00\x02\x00\x00\x1e\x00\x00\x00\x02\x00\x00\x1f\x00\x00\x00\x02\x00\x00 \x00\x00\x00\x03\x00\x00!\x00\x00\x00\x01\x00\x00"\x00\x00\x00\x01\x00\x00#\x00\x00\x00\x02\x00\x00$\x00\x00\x00\x02\x00\x00%\x00\x00\x00\x02\x00\x00&\x00\x00\x00\x02\x00\x00\'\x00\x00\x00\x02\x00\x00(\x00\x00\x00\x02\x00\x00)\x00\x00\x00\x02\x00\x00*\x00\x00\x00\x02\x00\x00+\x00\x00\x00\x02\x00\x00,\x00\x00\x00\x01\x00\x00-\x00\x00\x00\x00\x00\x00.\x00\x00\x00\x00\x00\x00/\x00\x00\x00\x00\x00\x000\x00\x00\x00\x00\x00\x001\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00',
                2,
                ApiVersionsResponse[3](error_code=0, api_versions=[(0, 0, 8, {}), (1, 0, 11, {}), (2, 0, 5, {}), (3, 0, 9, {}), (4, 0, 4, {}), (5, 0, 3, {}), (6, 0, 6, {}), (7, 0, 3, {}), (8, 0, 8, {}), (9, 0, 7, {}), (10, 0, 3, {}), (11, 0, 7, {}), (12, 0, 4, {}), (13, 0, 4, {}), (14, 0, 5, {}), (15, 0, 5, {}), (16, 0, 4, {}), (17, 0, 1, {}), (18, 0, 3, {}), (19, 0, 5, {}), (20, 0, 4, {}), (21, 0, 2, {}), (22, 0, 3, {}), (23, 0, 3, {}), (24, 0, 1, {}), (25, 0, 1, {}), (26, 0, 1, {}), (27, 0, 0, {}), (28, 0, 3, {}), (29, 0, 2, {}), (30, 0, 2, {}), (31, 0, 2, {}), (32, 0, 3, {}), (33, 0, 1, {}), (34, 0, 1, {}), (35, 0, 2, {}), (36, 0, 2, {}), (37, 0, 2, {}), (38, 0, 2, {}), (39, 0, 2, {}), (40, 0, 2, {}), (41, 0, 2, {}), (42, 0, 2, {}), (43, 0, 2, {}), (44, 0, 1, {}), (45, 0, 0, {}), (46, 0, 0, {}), (47, 0, 0, {}), (48, 0, 0, {}), (49, 0, 0, {})], throttle_time_ms=0, tags={}),
            ),
        },
        {
            'request': (
                MetadataRequest[8](topics=[], allow_auto_topic_creation=True),
                b'\x00\x00\x00"\x00\x03\x00\x08\x00\x00\x00\x03\x00\x11admin_client_VIsf\x00\x00\x00\x00\x01\x00\x00',
            ),
            'response': (
                b'\x00\x00\x00E\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\tlocalhost\x00\x00\xb9}\xff\xff\x00\x1634wjNp3hRJixCRvMlK9Znw\x00\x00\x00\x00\x00\x00\x00\x00\x80\x00\x00\x00',
                3,
                MetadataResponse[8](throttle_time_ms=0, brokers=[(0, 'localhost', 47485, None)], cluster_id='34wjNp3hRJixCRvMlK9Znw', controller_id=0, topics=[], authorized_operations=None),
            ),
        },
        {
            'request': (
                MetadataRequest[8](topics=None),
                b'\x00\x00\x00"\x00\x03\x00\x08\x00\x00\x00\x04\x00\x11admin_client_VIsf\xff\xff\xff\xff\x00\x00\x00',
            ),
            'response': (
                b'\x00\x00\x00E\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\tlocalhost\x00\x00\xb9}\xff\xff\x00\x1634wjNp3hRJixCRvMlK9Znw\x00\x00\x00\x00\x00\x00\x00\x00\x80\x00\x00\x00',
                4,
                MetadataResponse[8](throttle_time_ms=0, brokers=[(0, 'localhost', 47485, None)], cluster_id='34wjNp3hRJixCRvMlK9Znw', controller_id=0, topics=[], authorized_operations=None),
            ),
        }
    ),
}


TEST_FIND_COORDINATOR = {
    'client_id': 'consumer_thread-1',
    'messages': (
        {
            'request': (
                FindCoordinatorRequest[2]('test-group-CYErjI', 0),
                b'\x00\x00\x00/\x00\n\x00\x02\x00\x00\x00\x01\x00\x11consumer_thread-1\x00\x11test-group-CYErjI\x00',
            ),
            'response': (
                b'\x00\x00\x007\x00\x00\x00\x01\x00\x00\x00\x00\x00\x0f\x00!The coordinator is not available.\xff\xff\xff\xff\x00\x00\xff\xff\xff\xff',
                1,
                FindCoordinatorResponse[2](throttle_time_ms=0, error_code=15, error_message='The coordinator is not available.', coordinator_id=-1, host='', port=-1),
            ),
        },
        {
            'request': (
                FindCoordinatorRequest[2]('test-group-CYErjI', 0),
                b'\x00\x00\x00/\x00\n\x00\x02\x00\x00\x00\x02\x00\x11consumer_thread-1\x00\x11test-group-CYErjI\x00',
            ),
            'response': (
                b'\x00\x00\x00#\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x04NONE\x00\x00\x00\x00\x00\tlocalhost\x00\x00\xe1\t',
                2,
                FindCoordinatorResponse[2](throttle_time_ms=0, error_code=0, error_message='NONE', coordinator_id=0, host='localhost', port=57609),
            ),
        },
    ),
}


TEST_JOIN_GROUP = {
    'client_id': 'consumer_thread-0',
    'messages': (
        {
            'request': (
                JoinGroupRequest[5](group='test-group-IrXFAX', session_timeout=10000, rebalance_timeout=300000, member_id='', group_instance_id=None, protocol_type='consumer', group_protocols=[('range', b'\x00\x00\x00\x00\x00\x01\x00\x15test_group_fNFDXXKzKt\x00\x00\x00\x00'), ('roundrobin', b'\x00\x00\x00\x00\x00\x01\x00\x15test_group_fNFDXXKzKt\x00\x00\x00\x00')]),
                b"\x00\x00\x00\xa5\x00\x0b\x00\x05\x00\x00\x00\x01\x00\x11consumer_thread-0\x00\x11test-group-IrXFAX\x00\x00'\x10\x00\x04\x93\xe0\x00\x00\xff\xff\x00\x08consumer\x00\x00\x00\x02\x00\x05range\x00\x00\x00!\x00\x00\x00\x00\x00\x01\x00\x15test_group_fNFDXXKzKt\x00\x00\x00\x00\x00\nroundrobin\x00\x00\x00!\x00\x00\x00\x00\x00\x01\x00\x15test_group_fNFDXXKzKt\x00\x00\x00\x00",
            ),
            'response': (
                b'\x00\x00\x00N\x00\x00\x00\x01\x00\x00\x00\x00\x00O\xff\xff\xff\xff\x00\x00\x00\x00\x006consumer_thread-0-b3c8845c-6071-48bc-9cf1-372f84c4409b\x00\x00\x00\x00',
                1,
                JoinGroupResponse[5](throttle_time_ms=0, error_code=79, generation_id=-1, group_protocol='', leader_id='', member_id='consumer_thread-0-b3c8845c-6071-48bc-9cf1-372f84c4409b', members=[]),
            ),
        },

        {
            'request': (
                JoinGroupRequest[5](group='test-group-IrXFAX', session_timeout=10000, rebalance_timeout=300000, member_id='consumer_thread-0-b3c8845c-6071-48bc-9cf1-372f84c4409b', group_instance_id=None, protocol_type='consumer', group_protocols=[('range', b'\x00\x00\x00\x00\x00\x01\x00\x15test_group_fNFDXXKzKt\x00\x00\x00\x00'), ('roundrobin', b'\x00\x00\x00\x00\x00\x01\x00\x15test_group_fNFDXXKzKt\x00\x00\x00\x00')]),
                b"\x00\x00\x00\xdb\x00\x0b\x00\x05\x00\x00\x00\x02\x00\x11consumer_thread-0\x00\x11test-group-IrXFAX\x00\x00'\x10\x00\x04\x93\xe0\x006consumer_thread-0-b3c8845c-6071-48bc-9cf1-372f84c4409b\xff\xff\x00\x08consumer\x00\x00\x00\x02\x00\x05range\x00\x00\x00!\x00\x00\x00\x00\x00\x01\x00\x15test_group_fNFDXXKzKt\x00\x00\x00\x00\x00\nroundrobin\x00\x00\x00!\x00\x00\x00\x00\x00\x01\x00\x15test_group_fNFDXXKzKt\x00\x00\x00\x00",
            ),
            'response': (
                b'\x00\x00\x02\x05\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x05range\x006consumer_thread-0-b3c8845c-6071-48bc-9cf1-372f84c4409b\x006consumer_thread-0-b3c8845c-6071-48bc-9cf1-372f84c4409b\x00\x00\x00\x04\x006consumer_thread-1-a1e534ee-01a9-4c45-9dfd-778bbac550b7\xff\xff\x00\x00\x00!\x00\x00\x00\x00\x00\x01\x00\x15test_group_fNFDXXKzKt\x00\x00\x00\x00\x006consumer_thread-3-5c966b06-f0da-4ba9-9d89-38e9da865b8c\xff\xff\x00\x00\x00!\x00\x00\x00\x00\x00\x01\x00\x15test_group_fNFDXXKzKt\x00\x00\x00\x00\x006consumer_thread-0-b3c8845c-6071-48bc-9cf1-372f84c4409b\xff\xff\x00\x00\x00!\x00\x00\x00\x00\x00\x01\x00\x15test_group_fNFDXXKzKt\x00\x00\x00\x00\x006consumer_thread-2-01a02bdb-5425-4bfd-979f-ddff280cecd8\xff\xff\x00\x00\x00!\x00\x00\x00\x00\x00\x01\x00\x15test_group_fNFDXXKzKt\x00\x00\x00\x00',
                2,
                JoinGroupResponse[5](throttle_time_ms=0, error_code=0, generation_id=1, group_protocol='range', leader_id='consumer_thread-0-b3c8845c-6071-48bc-9cf1-372f84c4409b', member_id='consumer_thread-0-b3c8845c-6071-48bc-9cf1-372f84c4409b', members=[('consumer_thread-1-a1e534ee-01a9-4c45-9dfd-778bbac550b7', None, b'\x00\x00\x00\x00\x00\x01\x00\x15test_group_fNFDXXKzKt\x00\x00\x00\x00'), ('consumer_thread-3-5c966b06-f0da-4ba9-9d89-38e9da865b8c', None, b'\x00\x00\x00\x00\x00\x01\x00\x15test_group_fNFDXXKzKt\x00\x00\x00\x00'), ('consumer_thread-0-b3c8845c-6071-48bc-9cf1-372f84c4409b', None, b'\x00\x00\x00\x00\x00\x01\x00\x15test_group_fNFDXXKzKt\x00\x00\x00\x00'), ('consumer_thread-2-01a02bdb-5425-4bfd-979f-ddff280cecd8', None, b'\x00\x00\x00\x00\x00\x01\x00\x15test_group_fNFDXXKzKt\x00\x00\x00\x00')]),
            ),
        },

        {
            'request': (
                SyncGroupRequest[3](group='test-group-IrXFAX', generation_id=1, member_id='consumer_thread-0-b3c8845c-6071-48bc-9cf1-372f84c4409b', group_instance_id=None, group_assignment=[('consumer_thread-1-a1e534ee-01a9-4c45-9dfd-778bbac550b7', b'\x00\x00\x00\x00\x00\x01\x00\x15test_group_fNFDXXKzKt\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x00'), ('consumer_thread-3-5c966b06-f0da-4ba9-9d89-38e9da865b8c', b'\x00\x00\x00\x00\x00\x01\x00\x15test_group_fNFDXXKzKt\x00\x00\x00\x01\x00\x00\x00\x03\x00\x00\x00\x00'), ('consumer_thread-0-b3c8845c-6071-48bc-9cf1-372f84c4409b', b'\x00\x00\x00\x00\x00\x01\x00\x15test_group_fNFDXXKzKt\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00'), ('consumer_thread-2-01a02bdb-5425-4bfd-979f-ddff280cecd8', b'\x00\x00\x00\x00\x00\x01\x00\x15test_group_fNFDXXKzKt\x00\x00\x00\x01\x00\x00\x00\x02\x00\x00\x00\x00')]),
                b'\x00\x00\x02\x04\x00\x0e\x00\x03\x00\x00\x00\x03\x00\x11consumer_thread-0\x00\x11test-group-IrXFAX\x00\x00\x00\x01\x006consumer_thread-0-b3c8845c-6071-48bc-9cf1-372f84c4409b\xff\xff\x00\x00\x00\x04\x006consumer_thread-1-a1e534ee-01a9-4c45-9dfd-778bbac550b7\x00\x00\x00)\x00\x00\x00\x00\x00\x01\x00\x15test_group_fNFDXXKzKt\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x00\x006consumer_thread-3-5c966b06-f0da-4ba9-9d89-38e9da865b8c\x00\x00\x00)\x00\x00\x00\x00\x00\x01\x00\x15test_group_fNFDXXKzKt\x00\x00\x00\x01\x00\x00\x00\x03\x00\x00\x00\x00\x006consumer_thread-0-b3c8845c-6071-48bc-9cf1-372f84c4409b\x00\x00\x00)\x00\x00\x00\x00\x00\x01\x00\x15test_group_fNFDXXKzKt\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x006consumer_thread-2-01a02bdb-5425-4bfd-979f-ddff280cecd8\x00\x00\x00)\x00\x00\x00\x00\x00\x01\x00\x15test_group_fNFDXXKzKt\x00\x00\x00\x01\x00\x00\x00\x02\x00\x00\x00\x00',
            ),
            'response': (
                b'\x00\x00\x007\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00)\x00\x00\x00\x00\x00\x01\x00\x15test_group_fNFDXXKzKt\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00',
                3,
                SyncGroupResponse[3](throttle_time_ms=0, error_code=0, member_assignment=b'\x00\x00\x00\x00\x00\x01\x00\x15test_group_fNFDXXKzKt\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00'),
            ),
        },

        {
            'request': (
                OffsetFetchRequest[5](consumer_group='test-group-IrXFAX', topics=[('test_group_fNFDXXKzKt', [0])]),
                b'\x00\x00\x00Q\x00\t\x00\x05\x00\x00\x00\x04\x00\x11consumer_thread-0\x00\x11test-group-IrXFAX\x00\x00\x00\x01\x00\x15test_group_fNFDXXKzKt\x00\x00\x00\x01\x00\x00\x00\x00',
            ),
            'response': (
                b'\x00\x00\x00=\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x01\x00\x15test_group_fNFDXXKzKt\x00\x00\x00\x01\x00\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00',
                4,
                OffsetFetchResponse[5](throttle_time_ms=0, topics=[('test_group_fNFDXXKzKt', [(0, -1, -1, '', 0)])], error_code=0),
            ),
        },

        {
            'request': (
                HeartbeatRequest[3](group='test-group-IrXFAX', generation_id=1, member_id='consumer_thread-0-b3c8845c-6071-48bc-9cf1-372f84c4409b', group_instance_id=None),
                b'\x00\x00\x00l\x00\x0c\x00\x03\x00\x00\x00\x05\x00\x11consumer_thread-0\x00\x11test-group-IrXFAX\x00\x00\x00\x01\x006consumer_thread-0-b3c8845c-6071-48bc-9cf1-372f84c4409b\xff\xff',
            ),
            'response': (
                b'\x00\x00\x00\n\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00',
                5,
                HeartbeatResponse[3](throttle_time_ms=0, error_code=0),
            ),
        },

        {
            'request': (
                OffsetCommitRequest[7](group_id='test-group-IrXFAX', generation_id=1, member_id='consumer_thread-0-b3c8845c-6071-48bc-9cf1-372f84c4409b', group_instance_id=None, topics=[('test_group_fNFDXXKzKt', [(0, 0, -1, '')])]),
                b'\x00\x00\x00\x9d\x00\x08\x00\x07\x00\x00\x00\x06\x00\x11consumer_thread-0\x00\x11test-group-IrXFAX\x00\x00\x00\x01\x006consumer_thread-0-b3c8845c-6071-48bc-9cf1-372f84c4409b\xff\xff\x00\x00\x00\x01\x00\x15test_group_fNFDXXKzKt\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff\x00\x00',
            ),
            'response': (
                b'\x00\x00\x00-\x00\x00\x00\x06\x00\x00\x00\x00\x00\x00\x00\x01\x00\x15test_group_fNFDXXKzKt\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00',
                6,
                OffsetCommitResponse[7](throttle_time_ms=0, topics=[('test_group_fNFDXXKzKt', [(0, 0)])]),
            ),
        },

        {
            'request': (
                LeaveGroupRequest[3](group='test-group-IrXFAX', members=[('consumer_thread-0-b3c8845c-6071-48bc-9cf1-372f84c4409b', None)]),
                b'\x00\x00\x00l\x00\r\x00\x03\x00\x00\x00\x07\x00\x11consumer_thread-0\x00\x11test-group-IrXFAX\x00\x00\x00\x01\x006consumer_thread-0-b3c8845c-6071-48bc-9cf1-372f84c4409b\xff\xff',
            ),
            'response': (
                b'\x00\x00\x00J\x00\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x006consumer_thread-0-b3c8845c-6071-48bc-9cf1-372f84c4409b\xff\xff\x00\x00',
                7,
                LeaveGroupResponse[3](throttle_time_ms=0, error_code=0, members=[('consumer_thread-0-b3c8845c-6071-48bc-9cf1-372f84c4409b', None, 0)]),
            ),
        },
    ),
}


TEST_CASES = [
    TEST_API_VERSIONS_1,
    TEST_API_VERSIONS_2,
    TEST_FIND_COORDINATOR,
    TEST_JOIN_GROUP,
]


@pytest.mark.parametrize('test_case', TEST_CASES)
def test_parser(test_case):
    parser = KafkaProtocol(client_id=test_case['client_id'])

    for msg in test_case['messages']:
        req, snd = msg['request']

        parser.send_request(req)
        sent_bytes = parser.send_bytes()
        assert sent_bytes == snd

        recv, correlation_id, resp = msg['response']

        responses = parser.receive_bytes(recv)
        assert len(responses) == 1
        assert responses[0][0] == correlation_id
        assert responses[0][1] == resp
