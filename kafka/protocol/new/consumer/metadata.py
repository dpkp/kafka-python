from ..api_data import ApiData
from kafka.structs import TopicPartition


ConsumerProtocolType = 'consumer'


class ConsumerProtocolSubscription(ApiData): pass
class ConsumerProtocolAssignment(ApiData):

    # Compatibility with old manual protocol definition
    @property
    def assignment(self):
        return self.assigned_partitions

    @assignment.setter
    def assignment(self, value):
        self.assigned_partitions = value

    def partitions(self):
        return [TopicPartition(topic, partition)
                for topic, partitions in self.assigned_partitions
                for partition in partitions]


class StickyAssignorUserData(ApiData):
    def __init__(self, *args, **kw):
        if 'version' not in kw:
            kw['version'] = 1
        super().__init__(*args, **kw)


__all__ = [
    'ConsumerProtocolSubscription', 'ConsumerProtocolAssignment',
    'ConsumerProtocolType', 'StickyAssignorUserData',
]
