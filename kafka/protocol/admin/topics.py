from ..api_message import ApiMessage


class CreateTopicsRequest(ApiMessage): pass
class CreateTopicsResponse(ApiMessage): pass

class DeleteTopicsRequest(ApiMessage): pass
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
