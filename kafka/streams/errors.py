from __future__ import absolute_import

from kafka.errors import KafkaError, IllegalStateError


class StreamsError(KafkaError):
    pass


class ProcessorStateError(StreamsError):
    pass


class TopologyBuilderError(StreamsError):
    pass


class NoSuchElementError(StreamsError):
    pass


class TaskAssignmentError(StreamsError):
    pass
