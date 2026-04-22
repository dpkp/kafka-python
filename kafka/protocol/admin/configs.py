from ..api_message import ApiMessage


class AlterConfigsRequest(ApiMessage): pass
class AlterConfigsResponse(ApiMessage): pass

class DescribeConfigsRequest(ApiMessage): pass
class DescribeConfigsResponse(ApiMessage): pass

class IncrementalAlterConfigsRequest(ApiMessage): pass
class IncrementalAlterConfigsResponse(ApiMessage): pass

class ListConfigResourcesRequest(ApiMessage): pass
class ListConfigResourcesResponse(ApiMessage): pass


__all__ = [
    'AlterConfigsRequest', 'AlterConfigsResponse',
    'DescribeConfigsRequest', 'DescribeConfigsResponse',
    'IncrementalAlterConfigsRequest', 'IncrementalAlterConfigsResponse',
    'ListConfigResourcesRequest', 'ListConfigResourcesResponse',
]
