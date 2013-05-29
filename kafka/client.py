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
        self._load_metadata_for_topics()

    ##################
    #   Private API  #
    ##################


    def _get_conn_for_broker(self, broker):
        "Get or create a connection to a broker"
        if (broker.host, broker.port) not in self.conns:
            self.conns[(broker.host, broker.port)] = KafkaConnection(broker.host, broker.port, self.bufsize)
        return self.conns[(broker.host, broker.port)]

    def _get_leader_for_partition(self, topic, partition):
        key = TopicAndPartition(topic, partition)
        if key not in self.topics_to_brokers:
            self._load_metadata_for_topics(topic)
        if key not in self.topics_to_brokers:
            raise Exception("Partition does not exist: %s" % str(key))
        return self.topics_to_brokers[key]

    def _load_metadata_for_topics(self, *topics):
        """
        Discover brokers and metadata for a set of topics. This method will
        recurse in the event of a retry.
        """
        requestId = self._next_id()
        request = KafkaProtocol.encode_metadata_request(KafkaClient.CLIENT_ID, requestId, topics) 
        response = self._send_broker_unaware_request(requestId, request)
        if response is None:
            raise Exception("All servers failed to process request")
        (brokers, topics) = KafkaProtocol.decode_metadata_response(response)
        log.debug("Broker metadata: %s", brokers)
        log.debug("Topic metadata: %s", topics)
        self.brokers.update(brokers)
        self.topics_to_brokers = {}
        for topic, partitions in topics.items():
            if not partitions:
                log.info("Partition is unassigned, delay for 1s and retry")
                time.sleep(1)
                self._load_metadata_for_topics(topic)
                break

            for partition, meta in partitions.items():
                if meta.leader == -1:
                    log.info("Partition is unassigned, delay for 1s and retry")
                    time.sleep(1)
                    self._load_metadata_for_topics(topic)
                else:
                    self.topics_to_brokers[TopicAndPartition(topic, partition)] = brokers[meta.leader]
                    self.topic_partitions[topic].append(partition)

    def _next_id(self):
        "Generate a new correlation id"
        return KafkaClient.ID_GEN.next()

    def _send_broker_unaware_request(self, requestId, request):
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

    def _send_broker_aware_request(self, payloads, encoder_fn, decoder_fn):
        """
        Group a list of request payloads by topic+partition and send them to the
        leader broker for that partition using the supplied encode/decode functions

        Params
        ======
        payloads:   list of object-like entities with a topic and partition attribute
        encode_fn:  a method to encode the list of payloads to a request body, must accept
                    client_id, correlation_id, and payloads as keyword arguments
        decode_fn:  a method to decode a response body into response objects. The response
                    objects must be object-like and have topic and partition attributes

        Return
        ======
        List of response objects in the same order as the supplied payloads
        """
        # Group the requests by topic+partition
        original_keys = []
        payloads_by_broker = defaultdict(list)
        for payload in payloads:
            payloads_by_broker[self._get_leader_for_partition(payload.topic, payload.partition)].append(payload)
            original_keys.append((payload.topic, payload.partition))

        # Accumulate the responses in a dictionary
        acc = {}

        # For each broker, send the list of request payloads
        for broker, payloads in payloads_by_broker.items():
            conn = self._get_conn_for_broker(broker)
            requestId = self._next_id()
            request = encoder_fn(client_id=KafkaClient.CLIENT_ID, correlation_id=requestId, payloads=payloads)

            # Send the request, recv the response
            conn.send(requestId, request)
            response = conn.recv(requestId)
            for response in decoder_fn(response):
                acc[(response.topic, response.partition)] = response

        # Order the accumulated responses by the original key order
        return (acc[k] for k in original_keys)

    #################
    #   Public API  #
    #################

    def close(self):
        for conn in self.conns.values():
            conn.close()

    def send_produce_request(self, payloads=[], acks=1, timeout=1000, fail_on_error=True, callback=None):
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
        resps = self._send_broker_aware_request(payloads,
                    partial(KafkaProtocol.encode_produce_request, acks=acks, timeout=timeout),
                    KafkaProtocol.decode_produce_response)
        out = []
        for resp in resps:
            # Check for errors
            if fail_on_error == True and resp.error != ErrorMapping.NO_ERROR:
                raise Exception("ProduceRequest for %s failed with errorcode=%d" % 
                        (TopicAndPartition(resp.topic, resp.partition), resp.error))
            # Run the callback
            if callback is not None:
                out.append(callback(resp))
            else:
                out.append(resp)
        return out

    def send_fetch_request(self, payloads=[], fail_on_error=True, callback=None):
        """
        Encode and send a FetchRequest
        
        Payloads are grouped by topic and partition so they can be pipelined to the same
        brokers.
        """
        resps = self._send_broker_aware_request(payloads,
                                   KafkaProtocol.encode_fetch_request,
                                   KafkaProtocol.decode_fetch_response)
        out = []
        for resp in resps:
            # Check for errors
            if fail_on_error == True and resp.error != ErrorMapping.NO_ERROR:
                raise Exception("FetchRequest for %s failed with errorcode=%d" % 
                        (TopicAndPartition(resp.topic, resp.partition), resp.error))
            # Run the callback
            if callback is not None:
                out.append(callback(resp))
            else:
                out.append(resp)
        return out


    def send_offset_request(self, payloads=[], fail_on_error=True, callback=None):
        resps = self._send_broker_aware_request(payloads,
                                   KafkaProtocol.encode_offset_request,
                                   KafkaProtocol.decode_offset_response)
        out = []
        for resp in resps:
            if fail_on_error == True and resp.error != ErrorMapping.NO_ERROR:
                raise Exception("OffsetRequest failed with errorcode=%s", resp.error)
            if callback is not None:
                out.append(callback(resp))
            else:
                out.append(resp)
        return out

    def send_offset_commit_request(self, group, payloads=[], fail_on_error=True, callback=None):
        resps = self._send_broker_aware_request(payloads,
                                   partial(KafkaProtocol.encode_offset_commit_request, group=group),
                                   KafkaProtocol.decode_offset_commit_response)
        out = []
        for resp in resps:
            if fail_on_error == True and resp.error != ErrorMapping.NO_ERROR:
                raise Exception("OffsetCommitRequest failed with errorcode=%s", resp.error)
            if callback is not None:
                out.append(callback(resp))
            else:
                out.append(resp)
        return out

    def send_offset_fetch_request(self, group, payloads=[], fail_on_error=True, callback=None):
        resps = self._send_broker_aware_request(payloads,
                                   partial(KafkaProtocol.encode_offset_fetch_request, group=group),
                                   KafkaProtocol.decode_offset_fetch_response)
        out = []
        for resp in resps:
            if fail_on_error == True and resp.error != ErrorMapping.NO_ERROR:
                raise Exception("OffsetCommitRequest failed with errorcode=%s", resp.error)
            if callback is not None:
                out.append(callback(resp))
            else:
                out.append(resp)
        return out
