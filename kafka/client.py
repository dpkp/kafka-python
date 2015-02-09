import binascii
import collections
import copy
import functools
import itertools
import logging
import time
import kafka.common

from kafka.common import (TopicAndPartition, BrokerMetadata,
                          ConnectionError, FailedPayloadsError,
                          KafkaTimeoutError, KafkaUnavailableError,
                          LeaderNotAvailableError, UnknownTopicOrPartitionError,
                          NotLeaderForPartitionError, ReplicaNotAvailableError)

from kafka.conn import collect_hosts, KafkaConnection, DEFAULT_SOCKET_TIMEOUT_SECONDS
from kafka.protocol import KafkaProtocol

log = logging.getLogger("kafka")


class KafkaClient(object):

    CLIENT_ID = b"kafka-python"
    ID_GEN = itertools.count()

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
        self.topics_to_brokers = {}  # TopicAndPartition -> BrokerMetadata
        self.topic_partitions = {}   # topic -> partition -> PartitionMetadata

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

        UnknownTopicOrPartitionError will be raised if the topic or partition
        is not part of the metadata.

        LeaderNotAvailableError is raised if server has metadata, but there is
        no current leader
        """

        key = TopicAndPartition(topic, partition)

        # Use cached metadata if it is there
        if self.topics_to_brokers.get(key) is not None:
            return self.topics_to_brokers[key]

        # Otherwise refresh metadata

        # If topic does not already exist, this will raise
        # UnknownTopicOrPartitionError if not auto-creating
        # LeaderNotAvailableError otherwise until partitions are created
        self.load_metadata_for_topics(topic)

        # If the partition doesn't actually exist, raise
        if partition not in self.topic_partitions[topic]:
            raise UnknownTopicOrPartitionError(key)

        # If there's no leader for the partition, raise
        meta = self.topic_partitions[topic][partition]
        if meta.leader == -1:
            raise LeaderNotAvailableError(meta)

        # Otherwise return the BrokerMetadata
        return self.brokers[meta.leader]

    def _next_id(self):
        """
        Generate a new correlation id
        """
        return next(KafkaClient.ID_GEN)

    def _send_broker_unaware_request(self, payloads, encoder_fn, decoder_fn):
        """
        Attempt to send a broker-agnostic request to one of the available
        brokers. Keep trying until you succeed.
        """
        for (host, port) in self.hosts:
            requestId = self._next_id()
            try:
                conn = self._get_conn(host, port)
                request = encoder_fn(client_id=self.client_id,
                                     correlation_id=requestId,
                                     payloads=payloads)

                conn.send(requestId, request)
                response = conn.recv(requestId)
                return decoder_fn(response)

            except Exception:
                log.exception("Could not send request [%r] to server %s:%i, "
                              "trying next server" % (requestId, host, port))

        raise KafkaUnavailableError("All servers failed to process request")

    def _send_broker_aware_request(self, payloads, encoder_fn, decoder_fn):
        """
        Group a list of request payloads by topic+partition and send them to
        the leader broker for that partition using the supplied encode/decode
        functions

        Arguments:

        payloads: list of object-like entities with a topic (str) and
            partition (int) attribute

        encode_fn: a method to encode the list of payloads to a request body,
            must accept client_id, correlation_id, and payloads as
            keyword arguments

        decode_fn: a method to decode a response body into response objects.
            The response objects must be object-like and have topic
            and partition attributes

        Returns:

        List of response objects in the same order as the supplied payloads
        """

        # Group the requests by topic+partition
        original_keys = []
        payloads_by_broker = collections.defaultdict(list)

        for payload in payloads:
            leader = self._get_leader_for_partition(payload.topic,
                                                    payload.partition)

            payloads_by_broker[leader].append(payload)
            original_keys.append((payload.topic, payload.partition))

        # Accumulate the responses in a dictionary
        acc = {}

        # keep a list of payloads that were failed to be sent to brokers
        failed_payloads = []

        # For each broker, send the list of request payloads
        for broker, payloads in payloads_by_broker.items():
            conn = self._get_conn(broker.host.decode('utf-8'), broker.port)
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
                                "from server %s: %s", binascii.b2a_hex(request), conn, e)
                    failed = True
            except ConnectionError as e:
                log.warning("Could not send request [%s] to server %s: %s",
                            binascii.b2a_hex(request), conn, e)
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
        except (UnknownTopicOrPartitionError, NotLeaderForPartitionError):
            self.reset_topic_metadata(resp.topic)
            raise

    #################
    #   Public API  #
    #################
    def close(self):
        for conn in self.conns.values():
            conn.close()

    def copy(self):
        """
        Create an inactive copy of the client object
        A reinit() has to be done on the copy before it can be used again
        """
        c = copy.deepcopy(self)
        for key in c.conns:
            c.conns[key] = self.conns[key].copy()
        return c

    def reinit(self):
        for conn in self.conns.values():
            conn.reinit()

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
        return (
          topic in self.topic_partitions
          and len(self.topic_partitions[topic]) > 0
        )

    def get_partition_ids_for_topic(self, topic):
        if topic not in self.topic_partitions:
            return None

        return sorted(list(self.topic_partitions[topic]))

    def ensure_topic_exists(self, topic, timeout = 30):
        start_time = time.time()

        while not self.has_metadata_for_topic(topic):
            if time.time() > start_time + timeout:
                raise KafkaTimeoutError("Unable to create topic {0}".format(topic))
            try:
                self.load_metadata_for_topics(topic)
            except LeaderNotAvailableError:
                pass
            except UnknownTopicOrPartitionError:
                # Server is not configured to auto-create
                # retrying in this case will not help
                raise
            time.sleep(.5)

    def load_metadata_for_topics(self, *topics):
        """
        Fetch broker and topic-partition metadata from the server,
        and update internal data:
        broker list, topic/partition list, and topic/parition -> broker map

        This method should be called after receiving any error

        Arguments:
            *topics (optional): If a list of topics is provided,
                the metadata refresh will be limited to the specified topics only.

        Exceptions:
        ----------
        If the broker is configured to not auto-create topics,
        expect UnknownTopicOrPartitionError for topics that don't exist

        If the broker is configured to auto-create topics,
        expect LeaderNotAvailableError for new topics
        until partitions have been initialized.

        Exceptions *will not* be raised in a full refresh (i.e. no topic list)
        In this case, error codes will be logged as errors

        Partition-level errors will also not be raised here
        (a single partition w/o a leader, for example)
        """
        resp = self.send_metadata_request(topics)

        log.debug("Broker metadata: %s", resp.brokers)
        log.debug("Topic metadata: %s", resp.topics)

        self.brokers = dict([(broker.nodeId, broker)
                             for broker in resp.brokers])

        for topic_metadata in resp.topics:
            topic = topic_metadata.topic
            partitions = topic_metadata.partitions

            self.reset_topic_metadata(topic)

            # Errors expected for new topics
            try:
                kafka.common.check_error(topic_metadata)
            except (UnknownTopicOrPartitionError, LeaderNotAvailableError) as e:

                # Raise if the topic was passed in explicitly
                if topic in topics:
                    raise

                # Otherwise, just log a warning
                log.error("Error loading topic metadata for %s: %s", topic, type(e))
                continue

            self.topic_partitions[topic] = {}
            for partition_metadata in partitions:
                partition = partition_metadata.partition
                leader = partition_metadata.leader

                self.topic_partitions[topic][partition] = partition_metadata

                # Populate topics_to_brokers dict
                topic_part = TopicAndPartition(topic, partition)

                # Check for partition errors
                try:
                    kafka.common.check_error(partition_metadata)

                # If No Leader, topics_to_brokers topic_partition -> None
                except LeaderNotAvailableError:
                    log.error('No leader for topic %s partition %d', topic, partition)
                    self.topics_to_brokers[topic_part] = None
                    continue
                # If one of the replicas is unavailable -- ignore
                # this error code is provided for admin purposes only
                # we never talk to replicas, only the leader
                except ReplicaNotAvailableError:
                    log.warning('Some (non-leader) replicas not available for topic %s partition %d', topic, partition)

                # If Known Broker, topic_partition -> BrokerMetadata
                if leader in self.brokers:
                    self.topics_to_brokers[topic_part] = self.brokers[leader]

                # If Unknown Broker, fake BrokerMetadata so we dont lose the id
                # (not sure how this could happen. server could be in bad state)
                else:
                    self.topics_to_brokers[topic_part] = BrokerMetadata(
                        leader, None, None
                    )

    def send_metadata_request(self, payloads=[], fail_on_error=True,
                              callback=None):

        encoder = KafkaProtocol.encode_metadata_request
        decoder = KafkaProtocol.decode_metadata_response

        return self._send_broker_unaware_request(payloads, encoder, decoder)

    def send_produce_request(self, payloads=[], acks=1, timeout=1000,
                             fail_on_error=True, callback=None):
        """
        Encode and send some ProduceRequests

        ProduceRequests will be grouped by (topic, partition) and then
        sent to a specific broker. Output is a list of responses in the
        same order as the list of payloads specified

        Arguments:
            payloads: list of ProduceRequest
            fail_on_error: boolean, should we raise an Exception if we
                           encounter an API error?
            callback: function, instead of returning the ProduceResponse,
                      first pass it through this function

        Returns:
            list of ProduceResponse or callback(ProduceResponse), in the
            order of input payloads
        """

        encoder = functools.partial(
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

        encoder = functools.partial(KafkaProtocol.encode_fetch_request,
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
        encoder = functools.partial(KafkaProtocol.encode_offset_commit_request,
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

        encoder = functools.partial(KafkaProtocol.encode_offset_fetch_request,
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
