from ..api_message import ApiMessage


class ProduceRequest(ApiMessage):
    def expect_response(self):
        if self.acks == 0: # pylint: disable=no-member
            return False
        return True

class ProduceResponse(ApiMessage): pass


__all__ = [
    'ProduceRequest', 'ProduceResponse',
]
