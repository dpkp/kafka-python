from ..api_message import ApiMessage


class DescribeGroupsRequest(ApiMessage): pass
class DescribeGroupsResponse(ApiMessage): pass

class ListGroupsRequest(ApiMessage): pass
class ListGroupsResponse(ApiMessage): pass

class DeleteGroupsRequest(ApiMessage): pass
class DeleteGroupsResponse(ApiMessage): pass


__all__ = [
    'DescribeGroupsRequest', 'DescribeGroupsResponse',
    'ListGroupsRequest', 'ListGroupsResponse',
    'DeleteGroupsRequest', 'DeleteGroupsResponse',
]
