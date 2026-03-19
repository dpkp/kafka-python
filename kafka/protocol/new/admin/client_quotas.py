from ..api_message import ApiMessage


class AlterClientQuotasRequest(ApiMessage): pass
class AlterClientQuotasResponse(ApiMessage): pass

class DescribeClientQuotasRequest(ApiMessage): pass
class DescribeClientQuotasResponse(ApiMessage): pass


__all__ = [
    'AlterClientQuotasRequest', 'AlterClientQuotasResponse',
    'DescribeClientQuotasRequest', 'DescribeClientQuotasResponse',
]
