from __future__ import annotations

from ..api_message import ApiMessage

from kafka.util import classproperty


class MetadataRequest(ApiMessage):
    ALL_TOPICS = None
    NO_TOPICS = []

    def encode(self, version=None, header=False, framed=False):
        # Fixup v0 ALL_TOPICS => []
        if version == 0 or self.API_VERSION == 0:
            if self.topics is None: # pylint: disable=E0203
                self.topics = []
        return super().encode(version=version, header=header, framed=framed)


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
