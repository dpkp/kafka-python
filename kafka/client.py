import copy
import logging

from collections import defaultdict
from functools import partial
from itertools import count

from kafka.common import (ErrorMapping, TopicAndPartition,
                          ConnectionError, FailedPayloadsError,
                          BrokerResponseError, PartitionUnavailableError,
                          KafkaUnavailableError, KafkaRequestError)

from kafka.conn import KafkaConnection, DEFAULT_SOCKET_TIMEOUT_SECONDS
from kafka.protocol import KafkaProtocol

log = logging.getLogger("kafka")


class KafkaClient(object):

    CLIENT_ID = "kafka-python"
    ID_GEN = count()

    # NOTE: The timeout given to the client should always be greater than the
    # one passed to SimpleConsumer.get_message(), otherwise you can get a
    # socket timeout.
    def __init__(self, host, port, client_id=CLIENT_ID,
                 timeout=DEFAULT_SOCKET_TIMEOUT_SECONDS):
        # We need one connection to bootstrap
        self.client_id = client_id
        self.timeout = timeout
        self.conns = {               # (host, port) -> KafkaConnection
            (host, port): KafkaConnection(host, port, timeout=timeout)
        }
        self.brokers = {}            # broker_id -> BrokerMetadata
        self.topics_to_brokers = {}  # topic_id -> broker_id
        self.topic_partitions = {}   # topic_id -> [0, 1, 2, ...]
        self.load_metadata_for_topics()  # bootstrap with all metadata

    ##################
    #   Private API  #
    ##################

    def _get_conn_for_broker(self, broker):
        """
        Get or create a connection to a broker
        """
        if (broker.host, broker.port) not in self.conns:
            self.conns[(broker.host, broker.port)] = \
                KafkaConnection(broker.host, broker.port, timeout=self.timeout)

        return self.conns[(broker.host, broker.port)]

    def _get_leader_for_partition(self, topic, partition):
        key = TopicAndPartition(topic, partition)
        if key not in self.topics_to_brokers:
            self.load_metadata_for_topics(topic)

        if key not in self.topics_to_brokers:
            raise KafkaRequestError("Partition does not exist: %s" % str(key))

        return self.topics_to_brokers[key]

    def _next_id(self):
        """
        Generate a new correlation id
        """
        return KafkaClient.ID_GEN.next()

    def _send_broker_unaware_request(self, requestId, request):
        """
        Attempt to send a broker-agnostic request to one of the available
        brokers. Keep trying until you succeed.
        """
        for conn in self.conns.values():
            try:
                conn.send(requestId, request)
                response = conn.recv(requestId)
                return response
            except Exception, e:
                log.warning("Could not send request [%r] to server %s, "
                            "trying next server: %s" % (request, conn, e))
                continue

        raise KafkaUnavailableError("All servers failed to process request")

    def _send_broker_aware_request(self, payloads, encoder_fn, decoder_fn):
        """
        Group a list of request payloads by topic+partition and send them to
        the leader broker for that partition using the supplied encode/decode
        functions

        Params
        ======
        payloads: list of object-like entities with a topic and
                  partition attribute
        encode_fn: a method to encode the list of payloads to a request body,
                   must accept client_id, correlation_id, and payloads as
                   keyword arguments
        decode_fn: a method to decode a response body into response objects.
                   The response objects must be object-like and have topic
                   and partition attributes

        Return
        ======
        List of response objects in the same order as the supplied payloads
        """

        # Group the requests by topic+partition
        original_keys = []
        payloads_by_broker = defaultdict(list)

        for payload in payloads:
            leader = self._get_leader_for_partition(payload.topic,
                                                    payload.partition)
            if leader == -1:
                raise PartitionUnavailableError("Leader is unassigned for %s-%s" % payload.topic, payload.partition)
            payloads_by_broker[leader].append(payload)
            original_keys.append((payload.topic, payload.partition))

        # Accumulate the responses in a dictionary
        acc = {}

        # keep a list of payloads that were failed to be sent to brokers
        failed_payloads = []

        # For each broker, send the list of request payloads
        for broker, payloads in payloads_by_broker.items():
            conn = self._get_conn_for_broker(broker)
            requestId = self._next_id()
            request = encoder_fn(client_id=self.client_id,
                                 correlation_id=requestId, payloads=payloads)

            failed = False
            # Send the request, recv the response
            try:
                conn.send(requestId, request)
                if decoder_fn is None:
                    continue
                try:
                    response = conn.recv(requestId)
                except ConnectionError, e:
                    log.warning("Could not receive response to request [%s] "
                                "from server %s: %s", request, conn, e)
                    failed = True
            except ConnectionError, e:
                log.warning("Could not send request [%s] to server %s: %s",
                            request, conn, e)
                failed = True

            if failed:
                failed_payloads += payloads
                self.reset_all_metadata()
                continue

            for response in decoder_fn(response):
                acc[(response.topic, response.partition)] = response

        if failed_payloads:
            raise FailedPayloadsError(failed_payloads)

        # Order the accumulated responses by the original key order
        return (acc[k] for k in original_keys) if acc else ()

    def __repr__(self):
        return '<KafkaClient client_id=%s>' % (self.client_id)

    def _raise_on_response_error(self, resp):
        if resp.error == ErrorMapping.NO_ERROR:
            return

        if resp.error in (ErrorMapping.UNKNOWN_TOPIC_OR_PARTITON,
                          ErrorMapping.NOT_LEADER_FOR_PARTITION):
            self.reset_topic_metadata(resp.topic)

        raise BrokerResponseError(
            "Request for %s failed with errorcode=%d" %
            (TopicAndPartition(resp.topic, resp.partition), resp.error))

    #################
    #   Public API  #
    #################
    def reset_topic_metadata(self, *topics):
        for topic in topics:
            try:
                partitions = self.topic_partitions[topic]
            except KeyError:
                continue

            for partition in partitions:
                self.topics_to_brokers.pop(TopicAndPartition(topic, partition), None)

            del self.topic_partitions[topic]

    def reset_all_metadata(self):
        self.topics_to_brokers.clear()
        self.topic_partitions.clear()

    def has_metadata_for_topic(self, topic):
        return topic in self.topic_partitions

    def close(self):
        for conn in self.conns.values():
            conn.close()

    def copy(self):
        """
        Create an inactive copy of the client object
        A reinit() has to be done on the copy before it can be used again
        """
        c = copy.deepcopy(self)
        for k, v in c.conns.items():
            c.conns[k] = v.copy()
        return c

    def reinit(self):
        for conn in self.conns.values():
            conn.reinit()

    def load_metadata_for_topics(self, *topics):
        """
        Discover brokers and metadata for a set of topics. This function is called
        lazily whenever metadata is unavailable.
        """
        request_id = self._next_id()
        request = KafkaProtocol.encode_metadata_request(self.client_id,
                                                        request_id, topics)

        response = self._send_broker_unaware_request(request_id, request)

        (brokers, topics) = KafkaProtocol.decode_metadata_response(response)

        log.debug("Broker metadata: %s", brokers)
        log.debug("Topic metadata: %s", topics)

        self.brokers = brokers

        for topic, partitions in topics.items():
            self.reset_topic_metadata(topic)

            if not partitions:
                continue

            self.topic_partitions[topic] = []
            for partition, meta in partitions.items():
                topic_part = TopicAndPartition(topic, partition)
                self.topics_to_brokers[topic_part] = brokers[meta.leader]
                self.topic_partitions[topic].append(partition)

    def send_produce_request(self, payloads=[], acks=1, timeout=1000,
                             fail_on_error=True, callback=None):
        """
        Encode and send some ProduceRequests

        ProduceRequests will be grouped by (topic, partition) and then
        sent to a specific broker. Output is a list of responses in the
        same order as the list of payloads specified

        Params
        ======
        payloads: list of ProduceRequest
        fail_on_error: boolean, should we raise an Exception if we
                       encounter an API error?
        callback: function, instead of returning the ProduceResponse,
                  first pass it through this function

        Return
        ======
        list of ProduceResponse or callback(ProduceResponse), in the
        order of input payloads
        """

        encoder = partial(
            KafkaProtocol.encode_produce_request,
            acks=acks,
            timeout=timeout)

        if acks == 0:
            decoder = None
        else:
            decoder = KafkaProtocol.decode_produce_response

        resps = self._send_broker_aware_request(payloads, encoder, decoder)

        out = []
        for resp in resps:
            if fail_on_error is True:
                self._raise_on_response_error(resp)

            if callback is not None:
                out.append(callback(resp))
            else:
                out.append(resp)
        return out

    def send_fetch_request(self, payloads=[], fail_on_error=True,
                           callback=None, max_wait_time=100, min_bytes=4096):
        """
        Encode and send a FetchRequest

        Payloads are grouped by topic and partition so they can be pipelined
        to the same brokers.
        """

        encoder = partial(KafkaProtocol.encode_fetch_request,
                          max_wait_time=max_wait_time,
                          min_bytes=min_bytes)

        resps = self._send_broker_aware_request(
            payloads, encoder,
            KafkaProtocol.decode_fetch_response)

        out = []
        for resp in resps:
            if fail_on_error is True:
                self._raise_on_response_error(resp)

            if callback is not None:
                out.append(callback(resp))
            else:
                out.append(resp)
        return out

    def send_offset_request(self, payloads=[], fail_on_error=True,
                            callback=None):
        resps = self._send_broker_aware_request(
            payloads,
            KafkaProtocol.encode_offset_request,
            KafkaProtocol.decode_offset_response)

        out = []
        for resp in resps:
            if fail_on_error is True:
                self._raise_on_response_error(resp)
            if callback is not None:
                out.append(callback(resp))
            else:
                out.append(resp)
        return out

    def send_offset_commit_request(self, group, payloads=[],
                                   fail_on_error=True, callback=None):
        encoder = partial(KafkaProtocol.encode_offset_commit_request,
                          group=group)
        decoder = KafkaProtocol.decode_offset_commit_response
        resps = self._send_broker_aware_request(payloads, encoder, decoder)

        out = []
        for resp in resps:
            if fail_on_error is True:
                self._raise_on_response_error(resp)

            if callback is not None:
                out.append(callback(resp))
            else:
                out.append(resp)
        return out

    def send_offset_fetch_request(self, group, payloads=[],
                                  fail_on_error=True, callback=None):

        encoder = partial(KafkaProtocol.encode_offset_fetch_request,
                          group=group)
        decoder = KafkaProtocol.decode_offset_fetch_response
        resps = self._send_broker_aware_request(payloads, encoder, decoder)

        out = []
        for resp in resps:
            if fail_on_error is True:
                self._raise_on_response_error(resp)
            if callback is not None:
                out.append(callback(resp))
            else:
                out.append(resp)
        return out
