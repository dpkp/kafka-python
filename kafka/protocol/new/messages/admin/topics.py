from ...api_message import ApiMessage


class CreateTopicsRequest(ApiMessage): pass
class CreateTopicsResponse(ApiMessage): pass

class DeleteTopicsRequest(ApiMessage): pass
class DeleteTopicsResponse(ApiMessage): pass

class CreatePartitionsRequest(ApiMessage): pass
class CreatePartitionsResponse(ApiMessage): pass

class DeleteRecordsRequest(ApiMessage): pass
class DeleteRecordsResponse(ApiMessage): pass


__all__ = [
    'CreateTopicsRequest', 'CreateTopicsResponse',
    'DeleteTopicsRequest', 'DeleteTopicsResponse',
    'CreatePartitionsRequest', 'CreatePartitionsResponse',
    'DeleteRecordsRequest', 'DeleteRecordsResponse',
]
