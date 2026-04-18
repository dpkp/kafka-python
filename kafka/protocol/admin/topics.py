from enum import IntEnum

from ..api_message import ApiMessage


class CreateTopicsRequest(ApiMessage): pass
class CreateTopicsResponse(ApiMessage): pass

class DeleteTopicsRequest(ApiMessage):
    def encode(self, version=None, header=False, framed=False):
        # convert topics => topic_names for v0-v5
        version = self.API_VERSION if version is None else version
        if version is not None and version <= 5:
            if self.topics and not self.topic_names: # pylint: disable=E0203
                self.topic_names = [topic.name for topic in self.topics]
        return super().encode(version=version, header=header, framed=framed)

class DeleteTopicsResponse(ApiMessage): pass

class CreatePartitionsRequest(ApiMessage): pass
class CreatePartitionsResponse(ApiMessage): pass

class AlterPartitionReassignmentsRequest(ApiMessage): pass
class AlterPartitionReassignmentsResponse(ApiMessage): pass

class ListPartitionReassignmentsRequest(ApiMessage): pass
class ListPartitionReassignmentsResponse(ApiMessage): pass

class DescribeTopicPartitionsRequest(ApiMessage): pass
class DescribeTopicPartitionsResponse(ApiMessage): pass

class DeleteRecordsRequest(ApiMessage): pass
class DeleteRecordsResponse(ApiMessage): pass

class ElectLeadersRequest(ApiMessage): pass
class ElectLeadersResponse(ApiMessage): pass

class ElectionType(IntEnum):
    """Leader election type"""
    PREFERRED = 0
    UNCLEAN = 1


__all__ = [
    'CreateTopicsRequest', 'CreateTopicsResponse',
    'DeleteTopicsRequest', 'DeleteTopicsResponse',
    'CreatePartitionsRequest', 'CreatePartitionsResponse',
    'AlterPartitionReassignmentsRequest', 'AlterPartitionReassignmentsResponse',
    'ListPartitionReassignmentsRequest', 'ListPartitionReassignmentsResponse',
    'DescribeTopicPartitionsRequest', 'DescribeTopicPartitionsResponse',
    'DeleteRecordsRequest', 'DeleteRecordsResponse',
    'ElectLeadersRequest', 'ElectLeadersResponse', 'ElectionType',
]
