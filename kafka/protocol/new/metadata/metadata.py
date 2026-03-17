from ..api_message import ApiMessage

from kafka.util import classproperty


class MetadataRequest(ApiMessage):
    @classproperty
    def ALL_TOPICS(cls): # pylint: disable=E0213
        if cls._class_version == 0: # pylint: disable=E1101
            return []
        else:
            return None

    @classproperty
    def NO_TOPICS(cls): # pylint: disable=E0213
        return []


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
