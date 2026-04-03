from ..api_message import ApiMessage


class DescribeGroupsRequest(ApiMessage): pass
class DescribeGroupsResponse(ApiMessage):
    @classmethod
    def json_patch(cls, json):
        # group authorized_operations
        json['fields'][1]['fields'][7]['type'] = 'bitfield'
        return json


class ListGroupsRequest(ApiMessage): pass
class ListGroupsResponse(ApiMessage): pass

class DeleteGroupsRequest(ApiMessage): pass
class DeleteGroupsResponse(ApiMessage): pass


__all__ = [
    'DescribeGroupsRequest', 'DescribeGroupsResponse',
    'ListGroupsRequest', 'ListGroupsResponse',
    'DeleteGroupsRequest', 'DeleteGroupsResponse',
]
