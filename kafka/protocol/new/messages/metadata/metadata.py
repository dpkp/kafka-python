from ...api_message import ApiMessage


class MetadataRequest(ApiMessage): pass
class MetadataResponse(ApiMessage):
    @classmethod
    def json_patch(cls, json):
        # cluster_authorized_operations
        json['fields'][5]['name'] = 'authorized_operations'
        json['fields'][5]['type'] = 'bitfield'
        # topic_authorized_operations
        json['fields'][4]['fields'][5]['name'] = 'authorized_operations'
        json['fields'][4]['fields'][5]['type'] = 'bitfield'
        return json


__all__ = [
    'MetadataRequest', 'MetadataResponse',
]
