from ..api_message import ApiMessage


class CreateTopicsRequest(ApiMessage): pass
class CreateTopicsResponse(ApiMessage): pass

class DeleteTopicsRequest(ApiMessage):
    def encode(self, version=None, header=False, framed=False):
        # convert topics => topic_names
        if self.topics and not self.topic_names:
            self.topic_names = [topic.name for topic in self.topics]
        return super().encode(version=version, header=header, framed=framed)

class DeleteTopicsResponse(ApiMessage): pass

class CreatePartitionsRequest(ApiMessage): pass
class CreatePartitionsResponse(ApiMessage): pass

class AlterPartitionRequest(ApiMessage): pass
class AlterPartitionResponse(ApiMessage): pass

class AlterPartitionReassignmentsRequest(ApiMessage): pass
class AlterPartitionReassignmentsResponse(ApiMessage): pass

class ListPartitionReassignmentsRequest(ApiMessage): pass
class ListPartitionReassignmentsResponse(ApiMessage): pass

class DeleteRecordsRequest(ApiMessage): pass
class DeleteRecordsResponse(ApiMessage): pass


__all__ = [
    'CreateTopicsRequest', 'CreateTopicsResponse',
    'DeleteTopicsRequest', 'DeleteTopicsResponse',
    'CreatePartitionsRequest', 'CreatePartitionsResponse',
    'AlterPartitionRequest', 'AlterPartitionResponse',
    'AlterPartitionReassignmentsRequest', 'AlterPartitionReassignmentsResponse',
    'ListPartitionReassignmentsRequest', 'ListPartitionReassignmentsResponse',
    'DeleteRecordsRequest', 'DeleteRecordsResponse',
]
