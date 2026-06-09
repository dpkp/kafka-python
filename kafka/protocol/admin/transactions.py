from ..api_message import ApiMessage


class ListTransactionsRequest(ApiMessage): pass
class ListTransactionsResponse(ApiMessage): pass

class DescribeTransactionsRequest(ApiMessage): pass
class DescribeTransactionsResponse(ApiMessage): pass

class DescribeProducersRequest(ApiMessage): pass
class DescribeProducersResponse(ApiMessage): pass


__all__ = [
    'ListTransactionsRequest', 'ListTransactionsResponse',
    'DescribeTransactionsRequest', 'DescribeTransactionsResponse',
    'DescribeProducersRequest', 'DescribeProducersResponse',
]
