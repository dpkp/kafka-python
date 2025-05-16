from __future__ import absolute_import


class Subscription(object):
    __slots__ = ('_metadata', '_group_instance_id')
    def __init__(self, metadata, group_instance_id):
        self._metadata = metadata
        self._group_instance_id = group_instance_id

    @property
    def version(self):
        return self._metadata.version

    @property
    def user_data(self):
        return self._metadata.user_data

    @property
    def topics(self):
        return self._metadata.topics

    # Alias for old interface / name
    subscription = topics

    @property
    def group_instance_id(self):
        return self._group_instance_id

    def encode(self):
        return self._metadata.encode()

    def __eq__(self, other):
        return (
            isinstance(other, Subscription) and
            self._metadata == other._metadata and
            self._group_instance_id == other._group_instance_id
        )
