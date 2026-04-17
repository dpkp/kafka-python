from ..api_message import ApiMessage


class DescribeClusterRequest(ApiMessage): pass
class DescribeClusterResponse(ApiMessage):
    @classmethod
    def json_patch(cls, json):
        json['fields'][7]['type'] = 'bitfield'
        return json

class DescribeLogDirsRequest(ApiMessage): pass
class DescribeLogDirsResponse(ApiMessage): pass


__all__ = [
    'DescribeClusterRequest', 'DescribeClusterResponse',
    'DescribeLogDirsRequest', 'DescribeLogDirsResponse',
]
