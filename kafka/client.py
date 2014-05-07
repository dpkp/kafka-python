import copy
import logging
import collections

import kafka.common

from functools import partial
from itertools import count
from kafka.common import (TopicAndPartition,
                          ConnectionError, FailedPayloadsError,
                          PartitionUnavailableError,
                          LeaderUnavailableError, KafkaUnavailableError,
                          UnknownTopicOrPartitionError, NotLeaderForPartitionError)

from kafka.conn import collect_hosts, KafkaConnection, DEFAULT_SOCKET_TIMEOUT_SECONDS
from kafka.protocol import KafkaProtocol

log = logging.getLogger("kafka")


class KafkaClient(object):

    CLIENT_ID = "kafka-python"
    ID_GEN = count()

    # NOTE: The timeout given to the client should always be greater than the
    # one passed to SimpleConsumer.get_message(), otherwise you can get a
    # socket timeout.
    def __init__(self, hosts, client_id=CLIENT_ID,
                 timeout=DEFAULT_SOCKET_TIMEOUT_SECONDS):
        # We need one connection to bootstrap
        self.client_id = client_id
        self.timeout = timeout
        self.hosts = collect_hosts(hosts)

        # create connections only when we need them
        self.conns = {}
        self.brokers = {}            # broker_id -> BrokerMetadata
        self.topics_to_brokers = {}  # topic_id -> broker_id
        self.topic_partitions = {}   # topic_id -> [0, 1, 2, ...]
        self.load_metadata_for_topics()  # bootstrap with all metadata


    ##################
    #   Private API  #
    ##################

    def _get_conn(self, host, port):
        "Get or create a connection to a broker using host and port"
        host_key = (host, port)
        if host_key not in self.conns:
            self.conns[host_key] = KafkaConnection(
                host,
                port,
                timeout=self.timeout
            )

        return self.conns[host_key]

    def _get_leader_for_partition(self, topic, partition):
        """
        Returns the leader for a partition or None if the partition exists
        but has no leader.

        PartitionUnavailableError will be raised if the topic or partition
        is not part of the metadata.
        """

        key = TopicAndPartition(topic, partition)
        # reload metadata whether the partition is not available
        # or has no leader (broker is None)
        if self.topics_to_brokers.get(key) is None:
            self.load_metadata_for_topics(topic)

        if key not in self.topics_to_brokers:
            raise PartitionUnavailableError("%s not available" % str(key))

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
        for (host, port) in self.hosts:
            try:
                conn = self._get_conn(host, port)
                conn.send(requestId, request)
                response = conn.recv(requestId)
                return response
            except Exception as e:
                log.warning("Could not send request [%r] to server %s:%i, "
                            "trying next server: %s" % (request, host, port, e))

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
        payloads_by_broker = collections.defaultdict(list)

        for payload in payloads:
            leader = self._get_leader_for_partition(payload.topic,
                                                    payload.partition)
            if leader is None:
                raise LeaderUnavailableError(
                    "Leader not available for topic %s partition %s" %
                    (payload.topic, payload.partition))

            payloads_by_broker[leader].append(payload)
            original_keys.append((payload.topic, payload.partition))

        # Accumulate the responses in a dictionary
        acc = {}

        # keep a list of payloads that were failed to be sent to brokers
        failed_payloads = []

        # For each broker, send the list of request payloads
        for broker, payloads in payloads_by_broker.items():
            conn = self._get_conn(broker.host, broker.port)
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
                except ConnectionError as e:
                    log.warning("Could not receive response to request [%s] "
                                "from server %s: %s", request, conn, e)
                    failed = True
            except ConnectionError as e:
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
        try:
            kafka.common.check_error(resp)
        except (UnknownTopicOrPartitionError, NotLeaderForPartitionError) as e:
            self.reset_topic_metadata(resp.topic)
            raise

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
                log.warning('No partitions for %s', topic)
                continue

            self.topic_partitions[topic] = []
            for partition, meta in partitions.items():
                self.topic_partitions[topic].append(partition)
                topic_part = TopicAndPartition(topic, partition)
                if meta.leader == -1:
                    log.warning('No leader for topic %s partition %s', topic, partition)
                    self.topics_to_brokers[topic_part] = None
                else:
                    self.topics_to_brokers[topic_part] = brokers[meta.leader]

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
