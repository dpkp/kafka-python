from collections import namedtuple

import Queue
from kazoo.handlers.threading import SequentialThreadingHandler
from kazoo.handlers.gevent import SequentialGeventHandler
import multiprocessing
import threading
import socket
import gevent
import gevent.event
import gevent.queue
import gevent.pool
import gevent.socket
import time

###############
#   Structs   #
###############

# Request payloads
ProduceRequest = namedtuple("ProduceRequest",
                            ["topic", "partition", "messages"])

FetchRequest = namedtuple("FetchRequest",
                          ["topic", "partition", "offset", "max_bytes"])

OffsetRequest = namedtuple("OffsetRequest",
                           ["topic", "partition", "time", "max_offsets"])

OffsetCommitRequest = namedtuple("OffsetCommitRequest",
                                 ["topic", "partition", "offset", "metadata"])

OffsetFetchRequest = namedtuple("OffsetFetchRequest", ["topic", "partition"])

# Response payloads
ProduceResponse = namedtuple("ProduceResponse",
                             ["topic", "partition", "error", "offset"])

FetchResponse = namedtuple("FetchResponse", ["topic", "partition", "error",
                                             "highwaterMark", "messages"])

OffsetResponse = namedtuple("OffsetResponse",
                            ["topic", "partition", "error", "offsets"])

OffsetCommitResponse = namedtuple("OffsetCommitResponse",
                                  ["topic", "partition", "error"])

OffsetFetchResponse = namedtuple("OffsetFetchResponse",
                                 ["topic", "partition", "offset",
                                  "metadata", "error"])

BrokerMetadata = namedtuple("BrokerMetadata", ["nodeId", "host", "port"])

PartitionMetadata = namedtuple("PartitionMetadata",
                               ["topic", "partition", "leader",
                                "replicas", "isr"])

# Other useful structs
OffsetAndMessage = namedtuple("OffsetAndMessage", ["offset", "message"])
Message = namedtuple("Message", ["magic", "attributes", "key", "value"])
TopicAndPartition = namedtuple("TopicAndPartition", ["topic", "partition"])


KAFKA_THREAD_DRIVER = 'thread'
KAFKA_PROCESS_DRIVER = 'process'
KAFKA_GEVENT_DRIVER = 'gevent'


class KafkaDriver(object):
    def __init__(self, driver_type):
        self.socket = socket
        self.sleep = time.sleep
        self.driver_type = driver_type

        if driver_type == KAFKA_THREAD_DRIVER:
            self.Queue = Queue.Queue
            self.Event = threading.Event
            self.Proc = threading.Thread
            self.kazoo_handler = SequentialThreadingHandler

        elif driver_type == KAFKA_PROCESS_DRIVER:
            self.Queue = multiprocessing.Queue
            self.Event = multiprocessing.Event
            self.Proc = multiprocessing.Process
            self.kazoo_handler = SequentialThreadingHandler

        elif driver_type == KAFKA_GEVENT_DRIVER:
            self.Queue = gevent.queue.Queue
            self.Event = gevent.event.Event
            self.socket = gevent.socket
            self.Proc = self.gevent_proc
            self.sleep = gevent.sleep
            self.kazoo_handler = SequentialGeventHandler

    def gevent_proc(self, target=None, args=(), kwargs=None):
        kwargs = {} if kwargs is None else kwargs
        return gevent.Greenlet(target, *args, **kwargs)


class ErrorMapping(object):
    # Many of these are not actually used by the client
    UNKNOWN                   = -1
    NO_ERROR                  = 0
    OFFSET_OUT_OF_RANGE       = 1
    INVALID_MESSAGE           = 2
    UNKNOWN_TOPIC_OR_PARTITON = 3
    INVALID_FETCH_SIZE        = 4
    LEADER_NOT_AVAILABLE      = 5
    NOT_LEADER_FOR_PARTITION  = 6
    REQUEST_TIMED_OUT         = 7
    BROKER_NOT_AVAILABLE      = 8
    REPLICA_NOT_AVAILABLE     = 9
    MESSAGE_SIZE_TO_LARGE     = 10
    STALE_CONTROLLER_EPOCH    = 11
    OFFSET_METADATA_TOO_LARGE = 12
