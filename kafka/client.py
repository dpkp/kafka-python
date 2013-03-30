import base64
from collections import defaultdict
from functools import partial
from itertools import count, cycle
import logging
from operator import attrgetter
import struct
import time
import zlib

from kafka.common import *
from kafka.conn import KafkaConnection
from kafka.protocol import KafkaProtocol 

log = logging.getLogger("kafka")

class KafkaClient(object):

    CLIENT_ID = "kafka-python"
    ID_GEN = count() 

    def __init__(self, host, port, bufsize=4096):
        # We need one connection to bootstrap 
        self.bufsize = bufsize
        self.conns = {              # (host, port) -> KafkaConnection
            (host, port): KafkaConnection(host, port, bufsize)
        } 
        self.brokers = {}           # broker_id -> BrokerMetadata
        self.topics_to_brokers = {} # topic_id -> broker_id
        self.topic_partitions = defaultdict(list) # topic_id -> [0, 1, 2, ...]
        self.load_metadata_for_topics()

    def close(self):
        for conn in self.conns.values():
            conn.close()

    def get_conn_for_broker(self, broker):
        "Get or create a connection to a broker"
        if (broker.host, broker.port) not in self.conns:
            self.conns[(broker.host, broker.port)] = KafkaConnection(broker.host, broker.port, self.bufsize)
        return self.conns[(broker.host, broker.port)]

    def next_id(self):
        "Generate a new correlation id"
        return KafkaClient.ID_GEN.next()

    def load_metadata_for_topics(self, *topics):
        """
        Discover brokers and metadata for a set of topics. This method will
        recurse in the event of a retry.
        """
        requestId = self.next_id()
        request = KafkaProtocol.encode_metadata_request(KafkaClient.CLIENT_ID, requestId, topics) 
        response = self.try_send_request(requestId, request)
        if response is None:
            raise Exception("All servers failed to process request")
        (brokers, topics) = KafkaProtocol.decode_metadata_response(response)
        log.debug("Broker metadata: %s", brokers)
        log.debug("Topic metadata: %s", topics)
        self.brokers.update(brokers)
        self.topics_to_brokers = {}
        for topic, partitions in topics.items():
            for partition, meta in partitions.items():
                if meta.leader == -1:
                    log.info("Partition is unassigned, delay for 1s and retry")
                    time.sleep(1)
                    self.load_metadata_for_topics(topic)
                else:
                    self.topics_to_brokers[TopicAndPartition(topic, partition)] = brokers[meta.leader]
                    self.topic_partitions[topic].append(partition)

    def get_leader_for_partition(self, topic, partition):
        key = TopicAndPartition(topic, partition)
        if key not in self.topics_to_brokers:
            self.load_metadata_for_topics(topic)
        if key not in self.topics_to_brokers:
            raise Exception("Partition does not exist: %s" % str(key))
        return self.topics_to_brokers[key]

    def send_produce_request(self, payloads=[], fail_on_error=True, callback=None):
        """
        Encode and send some ProduceRequests

        ProduceRequests will be grouped by (topic, partition) and then sent to a specific
        broker. Output is a list of responses in the same order as the list of payloads
        specified

        Params
        ======
        payloads: list of ProduceRequest
        fail_on_error: boolean, should we raise an Exception if we encounter an API error?
        callback: function, instead of returning the ProduceResponse, first pass it through this function

        Return
        ======
        list of ProduceResponse or callback(ProduceResponse), in the order of input payloads
        """
        # Group the produce requests by which broker they go to
        original_keys = []
        payloads_by_broker = defaultdict(list)
        for payload in payloads:
            payloads_by_broker[self.get_leader_for_partition(payload.topic, payload.partition)] += payloads
            original_keys.append((payload.topic, payload.partition))

        # Accumulate the responses in a dictionary
        acc = {}

        # For each broker, send the list of request payloads
        for broker, payloads in payloads_by_broker.items():
            conn = self.get_conn_for_broker(broker)
            requestId = self.next_id()
            request = KafkaProtocol.encode_produce_request(KafkaClient.CLIENT_ID, requestId, payloads)
            # Send the request
            conn.send(requestId, request)
            response = conn.recv(requestId)
            for produce_response in KafkaProtocol.decode_produce_response(response):
                # Check for errors
                if fail_on_error == True and produce_response.error != ErrorMapping.NO_ERROR:
                    raise Exception("ProduceRequest for %s failed with errorcode=%d" % 
                            (TopicAndPartition(produce_response.topic, produce_response.partition), produce_response.error))
                # Run the callback
                if callback is not None:
                    acc[(produce_response.topic, produce_response.partition)] = callback(produce_response)
                else:
                    acc[(produce_response.topic, produce_response.partition)] = produce_response

        # Order the accumulated responses by the original key order
        return (acc[k] for k in original_keys)

    def send_fetch_request(self, payloads=[], fail_on_error=True, callback=None):
        """
        Encode and send a FetchRequest
        
        Payloads are grouped by topic and partition so they can be pipelined to the same
        brokers.
        """
        # Group the produce requests by which broker they go to
        original_keys = []
        payloads_by_broker = defaultdict(list)
        for payload in payloads:
            payloads_by_broker[self.get_leader_for_partition(payload.topic, payload.partition)].append(payload)
            original_keys.append((payload.topic, payload.partition))

        # Accumulate the responses in a dictionary, keyed by topic+partition 
        acc = {}

        # For each broker, send the list of request payloads
        for broker, payloads in payloads_by_broker.items():
            conn = self.get_conn_for_broker(broker)
            requestId = self.next_id()
            request = KafkaProtocol.encode_fetch_request(KafkaClient.CLIENT_ID, requestId, payloads)
            # Send the request
            conn.send(requestId, request)
            response = conn.recv(requestId)
            for fetch_response in KafkaProtocol.decode_fetch_response_iter(response):
                # Check for errors
                if fail_on_error == True and fetch_response.error != ErrorMapping.NO_ERROR:
                    raise Exception("FetchRequest %s failed with errorcode=%d" %
                            (TopicAndPartition(fetch_response.topic, fetch_response.partition), fetch_response.error))
                # Run the callback
                if callback is not None:
                    acc[(fetch_response.topic, fetch_response.partition)] = callback(fetch_response)
                else:
                    acc[(fetch_response.topic, fetch_response.partition)] = fetch_response

        # Order the accumulated responses by the original key order
        return (acc[k] for k in original_keys)

    def try_send_request(self, requestId, request):
        """
        Attempt to send a broker-agnostic request to one of the available brokers.
        Keep trying until you succeed.
        """
        for conn in self.conns.values():
            try:
                conn.send(requestId, request)
                response = conn.recv(requestId)
                return response
            except Exception, e:
                log.warning("Could not send request [%r] to server %s, trying next server: %s" % (request, conn, e))
                continue
        return None

    def send_offset_request(self, payloads=[], fail_on_error=True, callback=None):
        requestId = self.next_id()
        request = KafkaProtocol.encode_offset_request(KafkaClient.CLIENT_ID, requestId, payloads)
        response = self.try_send_request(requestId, request)
        if response is None:
            if fail_on_error is True:
                raise Exception("All servers failed to process request")
            else:
                return None
        out = []
        for offset_response in KafkaProtocol.decode_offset_response(response):
            if fail_on_error == True and offset_response.error != ErrorMapping.NO_ERROR:
                raise Exception("OffsetRequest failed with errorcode=%s", offset_response.error)
            if callback is not None:
                out.append(callback(offset_response))
            else:
                out.append(offset_response)
        return out

    def send_offset_commit_request(self, group, payloads=[], fail_on_error=True, callback=None):
        requestId = self.next_id()
        request = KafkaProtocol.encode_offset_commit_request(KafkaClient.CLIENT_ID, requestId, group, payloads)
        response = self.try_send_request(requestId, request)
        if response is None:
            if fail_on_error is True:
                raise Exception("All servers failed to process request")
            else:
                return None
        out = []
        for offset_commit_response in KafkaProtocol.decode_offset_commit_response(response):
            log.debug(offset_commit_response)
            if fail_on_error == True and offset_commit_response.error != ErrorMapping.NO_ERROR:
                raise Exception("OffsetCommitRequest failed with errorcode=%s", offset_commit_response.error)
            if callback is not None:
                out.append(callback(offset_commit_response))
            else:
                out.append(offset_commit_response)
        return out

    def send_offset_fetch_request(self, group, payloads=[], fail_on_error=True, callback=None):
        requestId = self.next_id()
        request = KafkaProtocol.encode_offset_fetch_request(KafkaClient.CLIENT_ID, requestId, group, payloads)
        response = self.try_send_request(requestId, request)
        if response is None:
            if fail_on_error is True:
                raise Exception("All servers failed to process request")
            else:
                return None
        out = []
        for offset_fetch_response in KafkaProtocol.decode_offset_fetch_response(response):
            if fail_on_error == True and offset_fetch_response.error != ErrorMapping.NO_ERROR:
                raise Exception("OffsetFetchRequest for topic=%s, partition=%d failed with errorcode=%s" % (
                    offset_fetch_response.topic, offset_fetch_response.partition, offset_fetch_response.error))
            if callback is not None:
                out.append(callback(offset_fetch_response))
            else:
                out.append(offset_fetch_response)
        return out
