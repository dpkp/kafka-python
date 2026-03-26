from __future__ import annotations

from ..api_data import ApiData
from kafka.structs import TopicPartition


ConsumerProtocolType = 'consumer'


class ConsumerProtocolSubscription(ApiData): pass
class ConsumerProtocolAssignment(ApiData):

    # Compatibility with old manual protocol definition
    @property
    def assignment(self) -> list:
        return self.assigned_partitions

    @assignment.setter
    def assignment(self, value: list) -> None:
        self.assigned_partitions = value

    def partitions(self) -> list[TopicPartition]:
        return [TopicPartition(topic, partition)
                for topic, partitions in self.assigned_partitions
                for partition in partitions]


__all__ = [
    'ConsumerProtocolSubscription', 'ConsumerProtocolAssignment', 'ConsumerProtocolType',
]
