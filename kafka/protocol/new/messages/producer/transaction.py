from ...api_message import ApiMessage


class InitProducerIdRequest(ApiMessage): pass
class InitProducerIdResponse(ApiMessage): pass

class AddPartitionsToTxnRequest(ApiMessage): pass
class AddPartitionsToTxnResponse(ApiMessage): pass

class AddOffsetsToTxnRequest(ApiMessage): pass
class AddOffsetsToTxnResponse(ApiMessage): pass

class EndTxnRequest(ApiMessage): pass
class EndTxnResponse(ApiMessage): pass

class TxnOffsetCommitRequest(ApiMessage): pass
class TxnOffsetCommitResponse(ApiMessage): pass


__all__ = [
    'InitProducerIdRequest', 'InitProducerIdResponse',
    'AddPartitionsToTxnRequest', 'AddPartitionsToTxnResponse',
    'AddOffsetsToTxnRequest', 'AddOffsetsToTxnResponse',
    'EndTxnRequest', 'EndTxnResponse',
    'TxnOffsetCommitRequest', 'TxnOffsetCommitResponse',
]
