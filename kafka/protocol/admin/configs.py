from ..api_message import ApiMessage


class AlterConfigsRequest(ApiMessage): pass
class AlterConfigsResponse(ApiMessage): pass

class DescribeConfigsRequest(ApiMessage): pass
class DescribeConfigsResponse(ApiMessage): pass


__all__ = [
    'AlterConfigsRequest', 'AlterConfigsResponse',
    'DescribeConfigsRequest', 'DescribeConfigsResponse',
]
