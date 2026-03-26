from __future__ import annotations

from ..api_message import ApiMessage


class ProduceRequest(ApiMessage):
    def expect_response(self) -> bool:
        if self.acks == 0: # pylint: disable=no-member  # ty: ignore[unresolved-attribute]
            return False
        return True

class ProduceResponse(ApiMessage): pass


__all__ = [
    'ProduceRequest', 'ProduceResponse',
]
