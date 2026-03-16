from enum import IntEnum

from ..api_message import ApiMessage


class DescribeClusterRequest(ApiMessage): pass
class DescribeClusterResponse(ApiMessage): pass

class DescribeConfigsRequest(ApiMessage): pass
class DescribeConfigsResponse(ApiMessage): pass

class AlterConfigsRequest(ApiMessage): pass
class AlterConfigsResponse(ApiMessage): pass

class DescribeLogDirsRequest(ApiMessage): pass
class DescribeLogDirsResponse(ApiMessage): pass

class ElectLeadersRequest(ApiMessage): pass
class ElectLeadersResponse(ApiMessage): pass

class ElectionType(IntEnum):
    """ Leader election type
    """

    PREFERRED = 0,
    UNCLEAN = 1


__all__ = [
    'DescribeClusterRequest', 'DescribeClusterResponse',
    'DescribeConfigsRequest', 'DescribeConfigsResponse',
    'AlterConfigsRequest', 'AlterConfigsResponse',
    'DescribeLogDirsRequest', 'DescribeLogDirsResponse',
    'ElectLeadersRequest', 'ElectLeadersResponse', 'ElectionType',
]
