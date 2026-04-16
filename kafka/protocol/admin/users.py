from ..api_message import ApiMessage


class AlterUserScramCredentialsRequest(ApiMessage): pass
class AlterUserScramCredentialsResponse(ApiMessage): pass

class DescribeUserScramCredentialsRequest(ApiMessage): pass
class DescribeUserScramCredentialsResponse(ApiMessage): pass


__all__ = [
    'AlterUserScramCredentialsRequest', 'AlterUserScramCredentialsResponse',
    'DescribeUserScramCredentialsRequest', 'DescribeUserScramCredentialsResponse',
]
