from __future__ import absolute_import

import atexit
import copy
import logging
import threading
import time

from ..client_async import KafkaClient
from ..structs import TopicPartition
from ..partitioner.default import DefaultPartitioner
from ..protocol.message import Message, MessageSet
from .. import errors as Errors
from .future import FutureRecordMetadata, FutureProduceResult
from .record_accumulator import AtomicInteger, RecordAccumulator
from .sender import Sender


log = logging.getLogger(__name__)
PRODUCER_CLIENT_ID_SEQUENCE = AtomicInteger()


class KafkaProducer(object):
    """A Kafka client that publishes records to the Kafka cluster.

    The producer is thread safe and sharing a single producer instance across
    threads will generally be faster than having multiple instances.

    The producer consists of a pool of buffer space that holds records that
    haven't yet been transmitted to the server as well as a background I/O
    thread that is responsible for turning these records into requests and
    transmitting them to the cluster.

    The send() method is asynchronous. When called it adds the record to a
    buffer of pending record sends and immediately returns. This allows the
    producer to batch together individual records for efficiency.

    The 'acks' config controls the criteria under which requests are considered
    complete. The "all" setting will result in blocking on the full commit of
    the record, the slowest but most durable setting.

    If the request fails, the producer can automatically retry, unless
    'retries' is configured to 0. Enabling retries also opens up the
    possibility of duplicates (see the documentation on message
    delivery semantics for details:
    http://kafka.apache.org/documentation.html#semantics
    ).

    The producer maintains buffers of unsent records for each partition. These
    buffers are of a size specified by the 'batch_size' config. Making this
    larger can result in more batching, but requires more memory (since we will
    generally have one of these buffers for each active partition).

    By default a buffer is available to send immediately even if there is
    additional unused space in the buffer. However if you want to reduce the
    number of requests you can set 'linger_ms' to something greater than 0.
    This will instruct the producer to wait up to that number of milliseconds
    before sending a request in hope that more records will arrive to fill up
    the same batch. This is analogous to Nagle's algorithm in TCP. Note that
    records that arrive close together in time will generally batch together
    even with linger_ms=0 so under heavy load batching will occur regardless of
    the linger configuration; however setting this to something larger than 0
    can lead to fewer, more efficient requests when not under maximal load at
    the cost of a small amount of latency.

    The buffer_memory controls the total amount of memory available to the
    producer for buffering. If records are sent faster than they can be
    transmitted to the server then this buffer space will be exhausted. When
    the buffer space is exhausted additional send calls will block.

    The key_serializer and value_serializer instruct how to turn the key and
    value objects the user provides into bytes.

    Keyword Arguments:
        bootstrap_servers: 'host[:port]' string (or list of 'host[:port]'
            strings) that the producer should contact to bootstrap initial
            cluster metadata. This does not have to be the full node list.
            It just needs to have at least one broker that will respond to a
            Metadata API Request. Default port is 9092. If no servers are
            specified, will default to localhost:9092.
        client_id (str): a name for this client. This string is passed in
            each request to servers and can be used to identify specific
            server-side log entries that correspond to this client.
            Default: 'kafka-python-producer-#' (appended with a unique number
            per instance)
        key_serializer (callable): used to convert user-supplied keys to bytes
            If not None, called as f(key), should return bytes. Default: None.
        value_serializer (callable): used to convert user-supplied message
            values to bytes. If not None, called as f(value), should return
            bytes. Default: None.
        acks (0, 1, 'all'): The number of acknowledgments the producer requires
            the leader to have received before considering a request complete.
            This controls the durability of records that are sent. The
            following settings are common:

            0: Producer will not wait for any acknowledgment from the server.
                The message will immediately be added to the socket
                buffer and considered sent. No guarantee can be made that the
                server has received the record in this case, and the retries
                configuration will not take effect (as the client won't
                generally know of any failures). The offset given back for each
                record will always be set to -1.
            1: Wait for leader to write the record to its local log only.
                Broker will respond without awaiting full acknowledgement from
                all followers. In this case should the leader fail immediately
                after acknowledging the record but before the followers have
                replicated it then the record will be lost.
            all: Wait for the full set of in-sync replicas to write the record.
                This guarantees that the record will not be lost as long as at
                least one in-sync replica remains alive. This is the strongest
                available guarantee.
            If unset, defaults to acks=1.
        compression_type (str): The compression type for all data generated by
            the producer. Valid values are 'gzip', 'snappy', 'lz4', or None.
            Compression is of full batches of data, so the efficacy of batching
            will also impact the compression ratio (more batching means better
            compression). Default: None.
        retries (int): Setting a value greater than zero will cause the client
            to resend any record whose send fails with a potentially transient
            error. Note that this retry is no different than if the client
            resent the record upon receiving the error. Allowing retries will
            potentially change the ordering of records because if two records
            are sent to a single partition, and the first fails and is retried
            but the second succeeds, then the second record may appear first.
            Default: 0.
        batch_size (int): Requests sent to brokers will contain multiple
            batches, one for each partition with data available to be sent.
            A small batch size will make batching less common and may reduce
            throughput (a batch size of zero will disable batching entirely).
            Default: 16384
        linger_ms (int): The producer groups together any records that arrive
            in between request transmissions into a single batched request.
            Normally this occurs only under load when records arrive faster
            than they can be sent out. However in some circumstances the client
            may want to reduce the number of requests even under moderate load.
            This setting accomplishes this by adding a small amount of
            artificial delay; that is, rather than immediately sending out a
            record the producer will wait for up to the given delay to allow
            other records to be sent so that the sends can be batched together.
            This can be thought of as analogous to Nagle's algorithm in TCP.
            This setting gives the upper bound on the delay for batching: once
            we get batch_size worth of records for a partition it will be sent
            immediately regardless of this setting, however if we have fewer
            than this many bytes accumulated for this partition we will
            'linger' for the specified time waiting for more records to show
            up. This setting defaults to 0 (i.e. no delay). Setting linger_ms=5
            would have the effect of reducing the number of requests sent but
            would add up to 5ms of latency to records sent in the absense of
            load. Default: 0.
        partitioner (callable): Callable used to determine which partition
            each message is assigned to. Called (after key serialization):
            partitioner(key_bytes, all_partitions, available_partitions).
            The default partitioner implementation hashes each non-None key
            using the same murmur2 algorithm as the java client so that
            messages with the same key are assigned to the same partition.
            When a key is None, the message is delivered to a random partition
            (filtered to partitions with available leaders only, if possible).
        buffer_memory (int): The total bytes of memory the producer should use
            to buffer records waiting to be sent to the server. If records are
            sent faster than they can be delivered to the server the producer
            will block up to max_block_ms, raising an exception on timeout.
            In the current implementation, this setting is an approximation.
            Default: 33554432 (32MB)
        max_block_ms (int): Number of milliseconds to block during send() and
            partitions_for(). These methods can be blocked either because the
            buffer is full or metadata unavailable. Blocking in the
            user-supplied serializers or partitioner will not be counted against
            this timeout. Default: 60000.
        max_request_size (int): The maximum size of a request. This is also
            effectively a cap on the maximum record size. Note that the server
            has its own cap on record size which may be different from this.
            This setting will limit the number of record batches the producer
            will send in a single request to avoid sending huge requests.
            Default: 1048576.
        metadata_max_age_ms (int): The period of time in milliseconds after
            which we force a refresh of metadata even if we haven't seen any
            partition leadership changes to proactively discover any new
            brokers or partitions. Default: 300000
        retry_backoff_ms (int): Milliseconds to backoff when retrying on
            errors. Default: 100.
        request_timeout_ms (int): Client request timeout in milliseconds.
            Default: 30000.
        receive_buffer_bytes (int): The size of the TCP receive buffer
            (SO_RCVBUF) to use when reading data. Default: None (relies on
            system defaults). Java client defaults to 32768.
        send_buffer_bytes (int): The size of the TCP send buffer
            (SO_SNDBUF) to use when sending data. Default: None (relies on
            system defaults). Java client defaults to 131072.
        reconnect_backoff_ms (int): The amount of time in milliseconds to
            wait before attempting to reconnect to a given host.
            Default: 50.
        max_in_flight_requests_per_connection (int): Requests are pipelined
            to kafka brokers up to this number of maximum requests per
            broker connection. Default: 5.
        security_protocol (str): Protocol used to communicate with brokers.
            Valid values are: PLAINTEXT, SSL. Default: PLAINTEXT.
        ssl_context (ssl.SSLContext): pre-configured SSLContext for wrapping
            socket connections. If provided, all other ssl_* configurations
            will be ignored. Default: None.
        ssl_check_hostname (bool): flag to configure whether ssl handshake
            should verify that the certificate matches the brokers hostname.
            default: true.
        ssl_cafile (str): optional filename of ca file to use in certificate
            veriication. default: none.
        ssl_certfile (str): optional filename of file in pem format containing
            the client certificate, as well as any ca certificates needed to
            establish the certificate's authenticity. default: none.
        ssl_keyfile (str): optional filename containing the client private key.
            default: none.
        ssl_crlfile (str): optional filename containing the CRL to check for
            certificate expiration. By default, no CRL check is done. When
            providing a file, only the leaf certificate will be checked against
            this CRL. The CRL can only be checked with Python 3.4+ or 2.7.9+.
            default: none.
        api_version (str): specify which kafka API version to use.
            If set to 'auto', will attempt to infer the broker version by
            probing various APIs. Default: auto

    Note:
        Configuration parameters are described in more detail at
        https://kafka.apache.org/090/configuration.html#producerconfigs
    """
    _DEFAULT_CONFIG = {
        'bootstrap_servers': 'localhost',
        'client_id': None,
        'key_serializer': None,
        'value_serializer': None,
        'acks': 1,
        'compression_type': None,
        'retries': 0,
        'batch_size': 16384,
        'linger_ms': 0,
        'partitioner': DefaultPartitioner(),
        'buffer_memory': 33554432,
        'connections_max_idle_ms': 600000, # not implemented yet
        'max_block_ms': 60000,
        'max_request_size': 1048576,
        'metadata_max_age_ms': 300000,
        'retry_backoff_ms': 100,
        'request_timeout_ms': 30000,
        'receive_buffer_bytes': None,
        'send_buffer_bytes': None,
        'reconnect_backoff_ms': 50,
        'max_in_flight_requests_per_connection': 5,
        'security_protocol': 'PLAINTEXT',
        'ssl_context': None,
        'ssl_check_hostname': True,
        'ssl_cafile': None,
        'ssl_certfile': None,
        'ssl_keyfile': None,
        'ssl_crlfile': None,
        'api_version': 'auto',
    }

    def __init__(self, **configs):
        log.debug("Starting the Kafka producer") # trace
        self.config = copy.copy(self._DEFAULT_CONFIG)
        for key in self.config:
            if key in configs:
                self.config[key] = configs.pop(key)

        # Only check for extra config keys in top-level class
        assert not configs, 'Unrecognized configs: %s' % configs

        if self.config['client_id'] is None:
            self.config['client_id'] = 'kafka-python-producer-%s' % \
                                       PRODUCER_CLIENT_ID_SEQUENCE.increment()

        if self.config['acks'] == 'all':
            self.config['acks'] = -1

        client = KafkaClient(**self.config)

        # Check Broker Version if not set explicitly
        if self.config['api_version'] == 'auto':
            self.config['api_version'] = client.check_version()
        assert self.config['api_version'] in ('0.10', '0.9', '0.8.2', '0.8.1', '0.8.0')

        # Convert api_version config to tuple for easy comparisons
        self.config['api_version'] = tuple(
            map(int, self.config['api_version'].split('.')))

        if self.config['compression_type'] == 'lz4':
            assert self.config['api_version'] >= (0, 8, 2), 'LZ4 Requires >= Kafka 0.8.2 Brokers'

        message_version = 1 if self.config['api_version'] >= (0, 10) else 0
        self._accumulator = RecordAccumulator(message_version=message_version, **self.config)
        self._metadata = client.cluster
        guarantee_message_order = bool(self.config['max_in_flight_requests_per_connection'] == 1)
        self._sender = Sender(client, self._metadata, self._accumulator,
                              guarantee_message_order=guarantee_message_order,
                              **self.config)
        self._sender.daemon = True
        self._sender.start()
        self._closed = False
        atexit.register(self.close, timeout=0)
        log.debug("Kafka producer started")

    def __del__(self):
        self.close(timeout=0)

    def close(self, timeout=None):
        """Close this producer."""
        if not hasattr(self, '_closed') or self._closed:
            log.info('Kafka producer closed')
            return
        if timeout is None:
            timeout = 999999999
        assert timeout >= 0

        log.info("Closing the Kafka producer with %s secs timeout.", timeout)
        #first_exception = AtomicReference() # this will keep track of the first encountered exception
        invoked_from_callback = bool(threading.current_thread() is self._sender)
        if timeout > 0:
            if invoked_from_callback:
                log.warning("Overriding close timeout %s secs to 0 in order to"
                            " prevent useless blocking due to self-join. This"
                            " means you have incorrectly invoked close with a"
                            " non-zero timeout from the producer call-back.",
                            timeout)
            else:
                # Try to close gracefully.
                if self._sender is not None:
                    self._sender.initiate_close()
                    self._sender.join(timeout)

        if self._sender is not None and self._sender.is_alive():

            log.info("Proceeding to force close the producer since pending"
                     " requests could not be completed within timeout %s.",
                     timeout)
            self._sender.force_close()
            # Only join the sender thread when not calling from callback.
            if not invoked_from_callback:
                self._sender.join()

        try:
            self.config['key_serializer'].close()
        except AttributeError:
            pass
        try:
            self.config['value_serializer'].close()
        except AttributeError:
            pass
        self._closed = True
        log.debug("The Kafka producer has closed.")

    def partitions_for(self, topic):
        """Returns set of all known partitions for the topic."""
        max_wait = self.config['max_block_ms'] / 1000.0
        return self._wait_on_metadata(topic, max_wait)

    def send(self, topic, value=None, key=None, partition=None, timestamp_ms=None):
        """Publish a message to a topic.

        Arguments:
            topic (str): topic where the message will be published
            value (optional): message value. Must be type bytes, or be
                serializable to bytes via configured value_serializer. If value
                is None, key is required and message acts as a 'delete'.
                See kafka compaction documentation for more details:
                http://kafka.apache.org/documentation.html#compaction
                (compaction requires kafka >= 0.8.1)
            partition (int, optional): optionally specify a partition. If not
                set, the partition will be selected using the configured
                'partitioner'.
            key (optional): a key to associate with the message. Can be used to
                determine which partition to send the message to. If partition
                is None (and producer's partitioner config is left as default),
                then messages with the same key will be delivered to the same
                partition (but if key is None, partition is chosen randomly).
                Must be type bytes, or be serializable to bytes via configured
                key_serializer.
            timestamp_ms (int, optional): epoch milliseconds (from Jan 1 1970 UTC)
                to use as the message timestamp. Defaults to current time.

        Returns:
            FutureRecordMetadata: resolves to RecordMetadata

        Raises:
            KafkaTimeoutError: if unable to fetch topic metadata, or unable
                to obtain memory buffer prior to configured max_block_ms
        """
        assert value is not None or self.config['api_version'] >= (0, 8, 1), (
            'Null messages require kafka >= 0.8.1')
        assert not (value is None and key is None), 'Need at least one: key or value'
        try:
            # first make sure the metadata for the topic is
            # available
            self._wait_on_metadata(topic, self.config['max_block_ms'] / 1000.0)

            key_bytes, value_bytes = self._serialize(topic, key, value)
            partition = self._partition(topic, partition, key, value,
                                        key_bytes, value_bytes)

            message_size = MessageSet.HEADER_SIZE + Message.HEADER_SIZE
            if key_bytes is not None:
                message_size += len(key_bytes)
            if value_bytes is not None:
                message_size += len(value_bytes)
            self._ensure_valid_record_size(message_size)

            tp = TopicPartition(topic, partition)
            if timestamp_ms is None:
                timestamp_ms = int(time.time() * 1000)
            log.debug("Sending (key=%s value=%s) to %s", key, value, tp)
            result = self._accumulator.append(tp, timestamp_ms,
                                              key_bytes, value_bytes,
                                              self.config['max_block_ms'])
            future, batch_is_full, new_batch_created = result
            if batch_is_full or new_batch_created:
                log.debug("Waking up the sender since %s is either full or"
                           " getting a new batch", tp)
                self._sender.wakeup()

            return future
            # handling exceptions and record the errors;
            # for API exceptions return them in the future,
            # for other exceptions raise directly
        except Errors.KafkaTimeoutError:
            raise
        except AssertionError:
            raise
        except Exception as e:
            log.debug("Exception occurred during message send: %s", e)
            return FutureRecordMetadata(
                FutureProduceResult(
                    TopicPartition(topic, partition)),
                    -1, None
                ).failure(e)

    def flush(self, timeout=None):
        """
        Invoking this method makes all buffered records immediately available
        to send (even if linger_ms is greater than 0) and blocks on the
        completion of the requests associated with these records. The
        post-condition of flush() is that any previously sent record will have
        completed (e.g. Future.is_done() == True). A request is considered
        completed when either it is successfully acknowledged according to the
        'acks' configuration for the producer, or it results in an error.

        Other threads can continue sending messages while one thread is blocked
        waiting for a flush call to complete; however, no guarantee is made
        about the completion of messages sent after the flush call begins.
        """
        log.debug("Flushing accumulated records in producer.") # trace
        self._accumulator.begin_flush()
        self._sender.wakeup()
        self._accumulator.await_flush_completion(timeout=timeout)

    def _ensure_valid_record_size(self, size):
        """Validate that the record size isn't too large."""
        if size > self.config['max_request_size']:
            raise Errors.MessageSizeTooLargeError(
                "The message is %d bytes when serialized which is larger than"
                " the maximum request size you have configured with the"
                " max_request_size configuration" % size)
        if size > self.config['buffer_memory']:
            raise Errors.MessageSizeTooLargeError(
                "The message is %d bytes when serialized which is larger than"
                " the total memory buffer you have configured with the"
                " buffer_memory configuration." % size)

    def _wait_on_metadata(self, topic, max_wait):
        """
        Wait for cluster metadata including partitions for the given topic to
        be available.

        Arguments:
            topic (str): topic we want metadata for
            max_wait (float): maximum time in secs for waiting on the metadata

        Returns:
            set: partition ids for the topic

        Raises:
            TimeoutException: if partitions for topic were not obtained before
                specified max_wait timeout
        """
        # add topic to metadata topic list if it is not there already.
        self._sender.add_topic(topic)
        begin = time.time()
        elapsed = 0.0
        metadata_event = None
        while True:
            partitions = self._metadata.partitions_for_topic(topic)
            if partitions is not None:
                return partitions

            if not metadata_event:
                metadata_event = threading.Event()

            log.debug("Requesting metadata update for topic %s", topic)

            metadata_event.clear()
            future = self._metadata.request_update()
            future.add_both(lambda e, *args: e.set(), metadata_event)
            self._sender.wakeup()
            metadata_event.wait(max_wait - elapsed)
            elapsed = time.time() - begin
            if not metadata_event.is_set():
                raise Errors.KafkaTimeoutError(
                    "Failed to update metadata after %s secs.", max_wait)
            elif topic in self._metadata.unauthorized_topics:
                raise Errors.TopicAuthorizationFailedError(topic)
            else:
                log.debug("_wait_on_metadata woke after %s secs.", elapsed)

    def _serialize(self, topic, key, value):
        # pylint: disable-msg=not-callable
        if self.config['key_serializer']:
            serialized_key = self.config['key_serializer'](key)
        else:
            serialized_key = key
        if self.config['value_serializer']:
            serialized_value = self.config['value_serializer'](value)
        else:
            serialized_value = value
        return serialized_key, serialized_value

    def _partition(self, topic, partition, key, value,
                   serialized_key, serialized_value):
        if partition is not None:
            assert partition >= 0
            assert partition in self._metadata.partitions_for_topic(topic), 'Unrecognized partition'
            return partition

        all_partitions = list(self._metadata.partitions_for_topic(topic))
        available = list(self._metadata.available_partitions_for_topic(topic))
        return self.config['partitioner'](serialized_key,
                                          all_partitions,
                                          available)
