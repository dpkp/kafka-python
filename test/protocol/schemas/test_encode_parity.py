"""Verify that StructField.compiled_encode_into (codegen) matches the
non-optimized StructField.encode() output byte-for-byte, across a variety
of schemas and versions. Does the same for compiled_decode_from vs the
reference decode().
"""
import io
import uuid

import pytest

from kafka.protocol.admin import (
    AlterPartitionReassignmentsRequest,
    ListPartitionReassignmentsResponse,
)
from kafka.protocol.metadata import MetadataRequest, MetadataResponse
from kafka.protocol.schemas.fields.base import BaseField
from kafka.protocol.schemas.fields.codecs.encode_buffer import EncodeBuffer
from kafka.protocol.data_container import DataContainer


def _reference_encode(api_message):
    """Encode using the non-optimized struct.encode() reference path."""
    flexible = api_message.flexible_version_q(api_message.API_VERSION)
    return api_message._struct.encode(
        api_message, version=api_message.API_VERSION,
        compact=flexible, tagged=flexible)


def _codegen_encode(api_message):
    """Encode using the compiled codegen path (what production uses)."""
    flexible = api_message.flexible_version_q(api_message.API_VERSION)
    fn = api_message._struct.compiled_encode_into(
        api_message.API_VERSION, compact=flexible, tagged=flexible)
    out = EncodeBuffer()
    fn(api_message, out)
    return bytes(out.result())


def _reference_decode(cls, data, version):
    flexible = cls.flexible_version_q(version)
    bio = io.BytesIO(data)
    return cls._struct.decode(
        bio, version=version, compact=flexible, tagged=flexible,
        data_class=cls[None])


def _codegen_decode(cls, data, version):
    flexible = cls.flexible_version_q(version)
    fn = cls._struct.compiled_decode_from(
        version, compact=flexible, tagged=flexible, data_class=cls[None])
    obj, _pos = fn(memoryview(data), 0)
    return obj


# ---------------------------------------------------------------------------
# Parity: simple struct, no nullable, no struct array
# ---------------------------------------------------------------------------


def test_parity_metadata_request_v0():
    req = MetadataRequest(version=0, topics=['topic-a', 'topic-b'])
    assert _reference_encode(req) == _codegen_encode(req)


def test_parity_metadata_request_flexible():
    # v9+ is flexible with compact strings and tagged fields
    Topic = MetadataRequest.MetadataRequestTopic
    req = MetadataRequest(
        version=12,
        topics=[Topic(topic_id=uuid.uuid4(), name='t1')],
        allow_auto_topic_creation=True,
        include_topic_authorized_operations=False,
    )
    assert _reference_encode(req) == _codegen_encode(req)


def test_parity_metadata_request_null_topics():
    # ALL_TOPICS is encoded as null topics array
    req = MetadataRequest(version=4, topics=None,
                          allow_auto_topic_creation=False)
    assert _reference_encode(req) == _codegen_encode(req)


# ---------------------------------------------------------------------------
# Parity: struct arrays with nested scalar fields
# ---------------------------------------------------------------------------


def test_parity_metadata_response_with_topics():
    Broker = MetadataResponse.MetadataResponseBroker
    Topic = MetadataResponse.MetadataResponseTopic
    Partition = Topic.MetadataResponsePartition
    resp = MetadataResponse(
        version=7,
        throttle_time_ms=0,
        brokers=[Broker(node_id=1, host='h1', port=9092, rack='r1')],
        cluster_id='c1',
        controller_id=1,
        topics=[
            Topic(
                error_code=0,
                name='t1',
                is_internal=False,
                partitions=[
                    Partition(
                        error_code=0, partition_index=0, leader_id=1,
                        leader_epoch=5,
                        replica_nodes=[1], isr_nodes=[1],
                        offline_replicas=[],
                    ),
                ],
            ),
        ],
    )
    assert _reference_encode(resp) == _codegen_encode(resp)


def test_parity_alter_partition_reassignments_request():
    Topic = AlterPartitionReassignmentsRequest.ReassignableTopic
    Partition = Topic.ReassignablePartition
    req = AlterPartitionReassignmentsRequest(
        version=0,
        timeout_ms=30000,
        topics=[
            Topic(
                name='topic-a',
                partitions=[
                    Partition(partition_index=0, replicas=[1, 2, 3]),
                    # cancel: null replicas
                    Partition(partition_index=1, replicas=None),
                ],
            ),
        ],
    )
    assert _reference_encode(req) == _codegen_encode(req)


# ---------------------------------------------------------------------------
# Parity: nullable error_message (flexible compact nullable string) in a
# list/response with struct arrays
# ---------------------------------------------------------------------------


def test_parity_list_partition_reassignments_response():
    _Topic = ListPartitionReassignmentsResponse.OngoingTopicReassignment
    _Partition = _Topic.OngoingPartitionReassignment
    resp = ListPartitionReassignmentsResponse(
        version=0,
        throttle_time_ms=0,
        error_code=0,
        error_message=None,
        topics=[
            _Topic(
                name='t1',
                partitions=[
                    _Partition(
                        partition_index=0, replicas=[1, 2, 3],
                        adding_replicas=[4], removing_replicas=[1]),
                ],
            ),
        ],
    )
    assert _reference_encode(resp) == _codegen_encode(resp)


# ---------------------------------------------------------------------------
# Decode parity: round-trip the codegen encoder output through both decoders.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize('cls,kwargs,version', [
    (MetadataRequest,
     dict(topics=['a', 'b']),
     0),
    (MetadataRequest,
     dict(topics=None, allow_auto_topic_creation=False),
     4),
])
def test_decode_parity(cls, kwargs, version):
    msg = cls(version=version, **kwargs)
    encoded = _codegen_encode(msg)

    via_codegen = _codegen_decode(cls, encoded, version)
    via_reference = _reference_decode(cls, encoded, version)

    # Compare full field-tree via to_dict (cursor=None case exercises the
    # _to_dict_vals None-guard in DataContainer).
    assert via_codegen.to_dict() == via_reference.to_dict()


# ---------------------------------------------------------------------------
# Targeted: build a minimal struct schema with a nullable nested struct to
# verify the null-prefix byte wire format directly (independent of any real
# protocol schema).
# ---------------------------------------------------------------------------


def _make_struct_with_nullable_child():
    return BaseField.parse_json({
        'name': 'Outer',
        'versions': '0+',
        'validVersions': '0',
        'flexibleVersions': '0+',
        'type': 'Outer',
        'fields': [
            {'name': 'x', 'type': 'int32', 'versions': '0+'},
            {'name': 'child', 'type': 'Child', 'versions': '0+',
             'nullableVersions': '0+',
             'fields': [
                 {'name': 'y', 'type': 'int32', 'versions': '0+'},
             ]},
        ],
    })


def _container_from_struct(struct):
    Outer = type('Outer', (DataContainer,), {'_struct': struct})
    return Outer


def test_nullable_child_wire_format_null():
    struct = _make_struct_with_nullable_child()
    Outer = _container_from_struct(struct)
    # Build inner child class via the field's data_class (set by __init_subclass__).
    item = Outer(version=0, x=7, child=None)
    encoded = struct.encode(item, version=0, compact=True, tagged=None)
    # int32 x=7 (4 bytes), nullable struct prefix 0x00, empty tagged fields 0x00
    assert encoded == b'\x00\x00\x00\x07\x00\x00'


def test_nullable_child_wire_format_present():
    struct = _make_struct_with_nullable_child()
    Outer = _container_from_struct(struct)
    Child = struct.fields['child'].data_class
    item = Outer(version=0, x=7, child=Child(version=0, y=9))
    encoded = struct.encode(item, version=0, compact=True, tagged=None)
    # int32 x=7, prefix 0x01, int32 y=9, inner struct's empty tagged 0x00,
    # outer struct's empty tagged 0x00
    assert encoded == b'\x00\x00\x00\x07\x01\x00\x00\x00\x09\x00\x00'


def test_nullable_child_codegen_matches_reference():
    struct = _make_struct_with_nullable_child()
    Outer = _container_from_struct(struct)
    Child = struct.fields['child'].data_class

    for child_val in (None, Child(version=0, y=42)):
        item = Outer(version=0, x=1, child=child_val)
        ref = struct.encode(item, version=0, compact=True, tagged=None)
        fn = struct.compiled_encode_into(0, compact=True, tagged=None)
        out = EncodeBuffer()
        fn(item, out)
        assert bytes(out.result()) == ref
