import atexit
import copy
import logging
import selectors
import socket
import threading
import warnings
import weakref

import kafka.errors as Errors
from kafka.net.compat import KafkaNetClient
from kafka.codec import has_gzip, has_snappy, has_lz4, has_zstd
from kafka.metrics import MetricConfig, Metrics
from kafka.partitioner import Partitioner, DefaultPartitioner
from kafka.producer.future import FutureRecordMetadata, FutureProduceResult
from kafka.producer.record_accumulator import AtomicInteger, RecordAccumulator
from kafka.producer.sender import Sender
from kafka.producer.transaction_manager import TransactionManager
from kafka.record.default_records import DefaultRecordBatchBuilder
from kafka.record.legacy_records import LegacyRecordBatchBuilder
from kafka.serializer import Serializer, SerializeWrapper
from kafka.structs import TopicPartition
from kafka.util import Timer


log = logging.getLogger(__name__)

_LOGGED_SERIALIZE_WARNING = False

PRODUCER_CLIENT_ID_SEQUENCE = AtomicInteger()


class KafkaProducer:
    """A Kafka client that publishes records to the Kafka cluster.

    The producer is thread safe and sharing a single producer instance across
    threads will generally be faster than having multiple instances.

    The producer consists of a RecordAccumulator which holds records that
    haven't yet been transmitted to the server, and a Sender background I/O
    thread that is responsible for turning these records into requests and
    transmitting them to the cluster.

    :meth:`~kafka.KafkaProducer.send` is asynchronous. When called it adds the
    record to a buffer of pending record sends and immediately returns. This
    allows the producer to batch together individual records for efficiency.

    The 'acks' config controls the criteria under which requests are considered
    complete. The "all" setting will result in blocking on the full commit of
    the record, the slowest but most durable setting.

    If the request fails, the producer can automatically retry, unless
    'retries' is configured to 0. Enabling retries also opens up the
    possibility of duplicates (see the documentation on message
    delivery semantics for details:
    https://kafka.apache.org/documentation.html#semantics
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

    The key_serializer and value_serializer instruct how to turn the key and
    value objects the user provides into bytes.

    From Kafka 0.11, the KafkaProducer supports two additional modes:
    the idempotent producer and the transactional producer.
    The idempotent producer strengthens Kafka's delivery semantics from
    at least once to exactly once delivery. In particular, producer retries
    will no longer introduce duplicates. The transactional producer allows an
    application to send messages to multiple partitions (and topics!)
    atomically.

    Since KIP-679 (Kafka 3.0), idempotence is enabled by default and `acks`
    defaults to 'all'. If the user explicitly provides a conflicting
    `acks`, `retries=0`, or `max_in_flight_requests_per_connection > 5`, the
    producer silently disables idempotence and emits a warning. Setting
    `enable_idempotence=True` explicitly (or supplying `transactional_id`)
    makes such conflicts raise instead. To opt out of idempotence entirely
    pass `enable_idempotence=False`. There are no API changes for the
    idempotent producer, so existing applications will not need to be
    modified to take advantage of this feature.

    To take advantage of the idempotent producer, it is imperative to avoid
    application level re-sends since these cannot be de-duplicated. As such, if
    an application enables idempotence, it is recommended to leave the
    `retries` config unset, as it will be defaulted to `float('inf')`.
    Additionally, if a :meth:`~kafka.KafkaProducer.send` returns an error even
    with infinite retries (for instance if the message expires in the buffer
    before being sent), then it is recommended to shut down the producer and
    check the contents of the last produced message to ensure that it is not
    duplicated. Finally, the producer can only guarantee idempotence for
    messages sent within a single session.

    To use the transactional producer and the attendant APIs, you must set the
    `transactional_id` configuration property. If the `transactional_id` is
    set, idempotence is automatically enabled along with the producer configs
    which idempotence depends on. Further, topics which are included in
    transactions should be configured for durability. In particular, the
    `replication.factor` should be at least `3`, and the `min.insync.replicas`
    for these topics should be set to 2. Finally, in order for transactional
    guarantees to be realized from end-to-end, the consumers must be
    configured to read only committed messages as well.

    The purpose of the `transactional_id` is to enable transaction recovery
    across multiple sessions of a single producer instance. It would typically
    be derived from the shard identifier in a partitioned, stateful,
    application. As such, it should be unique to each producer instance running
    within a partitioned application.

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
        transactional_id (str): Enable transactional producer with a unique
            identifier. This will be used to identify the same producer
            instance across process restarts. Default: None.
        enable_idempotence (bool): When set to True, the producer will ensure
            that exactly one copy of each message is written in the stream.
            If False, producer retries due to broker failures, etc., may write
            duplicates of the retried message in the stream.
            Default: True (since KIP-679).

            Idempotence requires `acks='all'` (-1), `retries > 0`, and
            `max_in_flight_requests_per_connection <= 5`. When idempotence is
            enabled by default and the user explicitly provides a conflicting
            value for any of those configs, the producer silently disables
            idempotence and logs a warning. When the user explicitly sets
            `enable_idempotence=True` (or supplies `transactional_id`), any
            such conflict raises KafkaConfigurationError instead. Requires
            broker >= 0.11; against older brokers the default-driven
            idempotence is silently disabled, while explicit opt-in raises.

            On Kafka 2.5+ brokers, the idempotent producer automatically
            recovers from transient producer-state errors (OutOfOrderSequence,
            UnknownProducerId, InvalidProducerEpoch) by bumping its producer
            epoch via InitProducerIdRequest v3+ (KIP-360). On older brokers,
            these errors remain fatal for transactional producers and reset
            the producer id for non-transactional idempotent producers.
            Batches that are in-flight at the moment of a bump will have
            their futures fail--their records are lost. Records still in
            the accumulator (not yet drained) are produced under the bumped
            epoch on the next drain.
        delivery_timeout_ms (float): An upper bound on the time to report success
            or failure after producer.send() returns. This limits the total time
            that a record will be delayed prior to sending, the time to await
            acknowledgement from the broker (if expected), and the time allowed
            for retriable send failures. The producer may report failure to send
            a record earlier than this config if either an unrecoverable error is
            encountered, the retries have been exhausted, or the record is added
            to a batch which reached an earlier delivery expiration deadline.
            The value of this config should be greater than or equal to the
            sum of (request_timeout_ms + linger_ms). Default: 120000.
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
            Default: 'all' (-1) (since KIP-679). Setting `acks` to 0 or 1
                while leaving `enable_idempotence` at its default disables
                idempotence silently with a warning.
        compression_type (str): The compression type for all data generated by
            the producer. Valid values are 'gzip', 'snappy', 'lz4', 'zstd' or None.
            Compression is of full batches of data, so the efficacy of batching
            will also impact the compression ratio (more batching means better
            compression). Default: None.
        retries (numeric): Setting a value greater than zero will cause the client
            to resend any record whose send fails with a potentially transient
            error. Note that this retry is no different than if the client
            resent the record upon receiving the error. Allowing retries
            without setting max_in_flight_requests_per_connection to 1 will
            potentially change the ordering of records because if two batches
            are sent to a single partition, and the first fails and is retried
            but the second succeeds, then the records in the second batch may
            appear first. Note additionally that produce requests will be
            failed before the number of retries has been exhausted if the timeout
            configured by delivery_timeout_ms expires first before successful
            acknowledgement. Users should generally prefer to leave this config
            unset and instead use delivery_timeout_ms to control retry behavior.
            Default: float('inf') (infinite)
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
            would add up to 5ms of latency to records sent in the absence of
            load. Default: 0.
        partitioner (kafka.partitioner.Partitioner): Assigns each message
            to a partition (after serialization). The default partitioner
            implementation hashes each non-None serialized key using the same
            algorithm as the java client (murmur2) so that messages with the
            same key are assigned to the same partition.
            When a key is None, the message is delivered to a random partition
            (filtered to partitions with available leaders only, if possible).
            Default: DefaultPartitioner().
        connections_max_idle_ms: Close idle connections after the number of
            milliseconds specified by this config. The broker closes idle
            connections after connections.max.idle.ms, so this avoids hitting
            unexpected socket disconnected errors on the client.
            Default: 540000
        max_block_ms (int): Number of milliseconds to block during
            :meth:`~kafka.KafkaProducer.send` and
            :meth:`~kafka.KafkaProducer.partitions_for`. These methods can be
            blocked either because the buffer is full or metadata unavailable.
            Blocking in the user-supplied serializers or partitioner will not be
            counted against this timeout. Default: 60000.
        max_request_size (int): The maximum size of a request. This is also
            effectively a cap on the maximum record size. Note that the server
            has its own cap on record size which may be different from this.
            This setting will limit the number of record batches the producer
            will send in a single request to avoid sending huge requests.
            Default: 1048576.
        allow_auto_create_topics (bool): Enable/disable auto topic creation
            on metadata request. Only available with api_version >= (0, 11).
            Default: True
        metadata_max_age_ms (int): The period of time in milliseconds after
            which we force a refresh of metadata even if we haven't seen any
            partition leadership changes to proactively discover any new
            brokers or partitions. Default: 300000
        retry_backoff_ms (int): Milliseconds to backoff when retrying on
            errors. Default: 100.
        request_timeout_ms (int): Client request timeout in milliseconds.
            Default: 30000.
        receive_message_max_bytes (int): Maximum allowed network frame size.
            Used to avoid OOM when decoding malformed network message header.
            Default: 100_000_000.
        receive_buffer_bytes (int): The size of the TCP receive buffer
            (SO_RCVBUF) to use when reading data. Default: None (relies on
            system defaults). Java client defaults to 32768.
        send_buffer_bytes (int): The size of the TCP send buffer
            (SO_SNDBUF) to use when sending data. Default: None (relies on
            system defaults). Java client defaults to 131072.
        socket_options (list): List of tuple-arguments to socket.setsockopt
            to apply to broker connection sockets. Default:
            [(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)]
        reconnect_backoff_ms (int): The amount of time in milliseconds to
            wait before attempting to reconnect to a given host.
            Default: 50.
        reconnect_backoff_max_ms (int): The maximum amount of time in
            milliseconds to backoff/wait when reconnecting to a broker that has
            repeatedly failed to connect. If provided, the backoff per host
            will increase exponentially for each consecutive connection
            failure, up to this maximum. Once the maximum is reached,
            reconnection attempts will continue periodically with this fixed
            rate. To avoid connection storms, a randomization factor of 0.2
            will be applied to the backoff resulting in a random range between
            20% below and 20% above the computed value. Default: 30000.
        max_in_flight_requests_per_connection (int): Requests are pipelined
            to kafka brokers up to this number of maximum requests per
            broker connection. Note that if this setting is set to be greater
            than 1 and there are failed sends, there is a risk of message
            re-ordering due to retries (i.e., if retries are enabled).
            Default: 5.
        security_protocol (str): Protocol used to communicate with brokers.
            Valid values are: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL.
            Default: PLAINTEXT.
        ssl_context (ssl.SSLContext): pre-configured SSLContext for wrapping
            socket connections. If provided, all other ssl_* configurations
            will be ignored. Default: None.
        ssl_check_hostname (bool): flag to configure whether ssl handshake
            should verify that the certificate matches the brokers hostname.
            Default: True.
        ssl_cafile (str): optional filename of ca file to use in certificate
            verification. Default: None.
        ssl_certfile (str): optional filename of file in pem format containing
            the client certificate, as well as any ca certificates needed to
            establish the certificate's authenticity. Default: None.
        ssl_keyfile (str): optional filename containing the client private key.
            Default: None.
        ssl_password (str): optional password to be used when loading the
            certificate chain. Default: None.
        ssl_crlfile (str): optional filename containing the CRL to check for
            certificate expiration. By default, no CRL check is done. When
            providing a file, only the leaf certificate will be checked against
            this CRL. Default: None.
        ssl_ciphers (str): optionally set the available ciphers for ssl
            connections. It should be a string in the OpenSSL cipher list
            format. If no cipher can be selected (because compile-time options
            or other configuration forbids use of all the specified ciphers),
            an ssl.SSLError will be raised. See ssl.SSLContext.set_ciphers
        api_version (tuple): Specify which Kafka API version to use. If set to
            None, the client will infer the broker version from the results of
            ApiVersionsRequest API. For brokers earlier than 0.10, which do not
            support the ApiVersionsRequest API, api_version is required.
            Note: Dynamic version checking is performed eagerly during __init__
            and can raise KafkaTimeoutError if no connection can be made before
            timeout (see bootstrap_timeout_ms below).
            Different versions enable different functionality.

            Examples::

                (4, 3) most recent broker release, enable all supported features
                (0, 11) enables message format v2 (internal)
                (0, 10, 0) enables sasl authentication and message format v1
                (0, 9) enables full group coordination features with automatic
                    partition assignment and rebalancing,
                (0, 8, 2) enables kafka-storage offset commits with manual
                    partition assignment only,
                (0, 8, 1) enables zookeeper-storage offset commits with manual
                    partition assignment only,
                (0, 8, 0) enables basic functionality but requires manual
                    partition assignment and offset management.

            Default: None
        bootstrap_timeout_ms (int): number of milliseconds to wait for first
            successful cluster bootstrap. If provided, an attempt to bootstrap
            will raise KafkaTimeoutError if it is unable to fetch cluster
            metadata before the configured timeout. Note that bootstrap will
            be called eagerly from __init__() if api_version is None.
            Default: 30000
        metric_reporters (list): A list of classes to use as metrics reporters.
            Implementing the AbstractMetricsReporter interface allows plugging
            in classes that will be notified of new metric creation. Default: []
        metrics_enabled (bool): Whether to track metrics on this instance. Default True.
        metrics_num_samples (int): The number of samples maintained to compute
            metrics. Default: 2
        metrics_sample_window_ms (int): The maximum age in milliseconds of
            samples used to compute metrics. Default: 30000
        selector (selectors.BaseSelector): Provide a specific selector
            implementation to use for I/O multiplexing.
            Default: selectors.DefaultSelector
        sasl_mechanism (str): Authentication mechanism when security_protocol
            is configured for SASL_PLAINTEXT or SASL_SSL. Valid values are:
            PLAIN, GSSAPI, OAUTHBEARER, SCRAM-SHA-256, SCRAM-SHA-512.
        sasl_plain_username (str): username for sasl PLAIN and SCRAM authentication.
            Required if sasl_mechanism is PLAIN or one of the SCRAM mechanisms.
        sasl_plain_password (str): password for sasl PLAIN and SCRAM authentication.
            Required if sasl_mechanism is PLAIN or one of the SCRAM mechanisms.
        sasl_kerberos_name (str or gssapi.Name): Constructed gssapi.Name for use with
            sasl mechanism handshake. If provided, sasl_kerberos_service_name and
            sasl_kerberos_domain name are ignored. Default: None.
        sasl_kerberos_service_name (str): Service name to include in GSSAPI
            sasl mechanism handshake. Default: 'kafka'
        sasl_kerberos_domain_name (str): kerberos domain name to use in GSSAPI
            sasl mechanism handshake. Default: one of bootstrap servers
        sasl_oauth_token_provider (kafka.net.sasl.oauth.AbstractTokenProvider): OAuthBearer
            token provider instance. Default: None
        proxy_url (str): URL to proxy socket connections through. Supports SOCKS5 only.
            Requires scheme:// (e.g., socks5://foo.bar/). Default: None
        kafka_client (callable): Custom class / callable for creating KafkaNetClient instances

    Note:
        Configuration parameters are described in more detail at
        https://kafka.apache.org/0100/documentation/#producerconfigs
    """
    DEFAULT_CONFIG = {
        'bootstrap_servers': 'localhost',
        'client_id': None,
        'key_serializer': None,
        'value_serializer': None,
        'enable_idempotence': True,
        'transactional_id': None,
        'transaction_timeout_ms': 60000,
        'delivery_timeout_ms': 120000,
        'acks': -1,
        'compression_type': None,
        'retries': float('inf'),
        'batch_size': 16384,
        'linger_ms': 0,
        'partitioner': DefaultPartitioner(),
        'connections_max_idle_ms': 9 * 60 * 1000,
        'max_block_ms': 60000,
        'max_request_size': 1048576,
        'allow_auto_create_topics': True,
        'metadata_max_age_ms': 300000,
        'client_dns_lookup': 'use_all_dns_ips',
        'retry_backoff_ms': 100,
        'request_timeout_ms': 30000,
        'receive_message_max_bytes': 100_000_000,
        'receive_buffer_bytes': None,
        'send_buffer_bytes': None,
        'socket_options': [(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)],
        'reconnect_backoff_ms': 50,
        'reconnect_backoff_max_ms': 30000,
        'max_in_flight_requests_per_connection': 5,
        'security_protocol': 'PLAINTEXT',
        'ssl_context': None,
        'ssl_check_hostname': True,
        'ssl_cafile': None,
        'ssl_certfile': None,
        'ssl_keyfile': None,
        'ssl_crlfile': None,
        'ssl_password': None,
        'ssl_ciphers': None,
        'api_version': None,
        'bootstrap_timeout_ms': 30000,
        'metric_reporters': [],
        'metrics_enabled': True,
        'metrics_num_samples': 2,
        'metrics_sample_window_ms': 30000,
        'selector': selectors.DefaultSelector,
        'sasl_mechanism': None,
        'sasl_plain_username': None,
        'sasl_plain_password': None,
        'sasl_kerberos_name': None,
        'sasl_kerberos_service_name': 'kafka',
        'sasl_kerberos_domain_name': None,
        'sasl_oauth_token_provider': None,
        'proxy_url': None,
        'socks5_proxy': None,  # deprecated
        'kafka_client': KafkaNetClient,
    }

    DEPRECATED_CONFIGS = ()

    _COMPRESSORS = {
        'gzip': (has_gzip, LegacyRecordBatchBuilder.CODEC_GZIP),
        'snappy': (has_snappy, LegacyRecordBatchBuilder.CODEC_SNAPPY),
        'lz4': (has_lz4, LegacyRecordBatchBuilder.CODEC_LZ4),
        'zstd': (has_zstd, DefaultRecordBatchBuilder.CODEC_ZSTD),
        None: (lambda: True, LegacyRecordBatchBuilder.CODEC_NONE),
    }

    def __init__(self, **configs):
        self.config = copy.copy(self.DEFAULT_CONFIG)
        user_provided_configs = set(configs.keys())
        for key in self.config:
            if key in configs:
                self.config[key] = configs.pop(key)

        for key in ('key_serializer', 'value_serializer'):
            if self.config[key] is not None and not isinstance(self.config[key], Serializer):
                warnings.warn('%s does not implement kafka.serializer.Serializer' % (key,), category=DeprecationWarning, stacklevel=3)
                self.config[key] = SerializeWrapper(self.config[key])

        for key in self.DEPRECATED_CONFIGS:
            if key in configs:
                configs.pop(key)
                warnings.warn('Deprecated Producer config: %s' % (key,), category=DeprecationWarning)

        # Only check for extra config keys in top-level class
        if configs:
            raise ValueError('Unrecognized configs: %s' % (configs,))

        if self.config['client_id'] is None:
            self.config['client_id'] = 'kafka-python-producer-%s' % \
                                       (PRODUCER_CLIENT_ID_SEQUENCE.increment(),)

        if self.config['acks'] == 'all':
            self.config['acks'] = -1

        # api_version was previously a str. accept old format for now
        if isinstance(self.config['api_version'], str):
            deprecated = self.config['api_version']
            if deprecated == 'auto':
                self.config['api_version'] = None
            else:
                self.config['api_version'] = tuple(map(int, deprecated.split('.')))
            log.warning('%s: use api_version=%s [tuple] -- "%s" as str is deprecated',
                        str(self), str(self.config['api_version']), deprecated)

        log.debug("%s: Starting Kafka producer", str(self))

        # Configure metrics
        if self.config['metrics_enabled']:
            metrics_tags = {'client-id': self.config['client_id']}
            metric_config = MetricConfig(samples=self.config['metrics_num_samples'],
                                         time_window_ms=self.config['metrics_sample_window_ms'],
                                         tags=metrics_tags)
            reporters = [reporter() for reporter in self.config['metric_reporters']]
            self._metrics = Metrics(metric_config, reporters)
        else:
            self._metrics = None

        client = self.config['kafka_client'](
            metrics=self._metrics, metric_group_prefix='producer',
            wakeup_timeout_ms=self.config['max_block_ms'],
            **self.config)
        manager = client._manager

        # We currently depend on eager-resolution of api_version.
        # If it wasn't provided as a config option, we need to bootstrap
        # to get it.
        if manager.broker_version_data is None:
            manager.bootstrap(self.config['bootstrap_timeout_ms'])
        self.config['api_version'] = manager.broker_version

        if self.config['compression_type'] == 'lz4':
            if self.config['api_version'] < (0, 8, 2):
                raise ValueError('LZ4 Requires >= Kafka 0.8.2 Brokers')

        if self.config['compression_type'] == 'zstd':
            if self.config['api_version'] < (2, 1):
                raise ValueError('Zstd Requires >= Kafka 2.1 Brokers')

        # Check compression_type for library support
        ct = self.config['compression_type']
        if ct not in self._COMPRESSORS:
            raise ValueError("Not supported codec: {}".format(ct))
        else:
            checker, compression_attrs = self._COMPRESSORS[ct]
            if not checker():
                raise RuntimeError("Libraries for {} compression codec not found".format(ct))
            self.config['compression_attrs'] = compression_attrs

        self._metadata = manager.cluster
        self._transaction_manager = None
        self._init_transactions_result = None

        user_set_idempotence = 'enable_idempotence' in user_provided_configs
        user_set_acks = 'acks' in user_provided_configs
        user_set_retries = 'retries' in user_provided_configs
        user_set_inflight = 'max_in_flight_requests_per_connection' in user_provided_configs

        if user_set_idempotence and not self.config['enable_idempotence'] and self.config['transactional_id']:
            raise Errors.KafkaConfigurationError("Cannot set transactional_id without enable_idempotence.")

        if self.config['transactional_id']:
            self.config['enable_idempotence'] = True
            # Transactional path is strict: any conflicting user-provided config must raise.
            user_set_idempotence = True

        if self.config['enable_idempotence']:
            conflicts = []
            if user_set_acks and self.config['acks'] != -1:
                conflicts.append(('acks', self.config['acks']))
            if user_set_retries and self.config['retries'] == 0:
                conflicts.append(('retries', 0))
            if user_set_inflight and self.config['max_in_flight_requests_per_connection'] > 5:
                conflicts.append(('max_in_flight_requests_per_connection',
                                  self.config['max_in_flight_requests_per_connection']))

            if conflicts:
                conflict_str = ', '.join('%s=%r' % kv for kv in conflicts)
                if user_set_idempotence:
                    raise Errors.KafkaConfigurationError(
                        "enable_idempotence=True is incompatible with user-provided %s" % (conflict_str,))
                log.warning(
                    "%s: Idempotence will be disabled because user-provided config conflicts with"
                    " idempotent defaults: %s", str(self), conflict_str)
                self.config['enable_idempotence'] = False

        if self.config['enable_idempotence'] and self.config['api_version'] < (0, 11):
            if user_set_idempotence:
                raise Errors.KafkaConfigurationError(
                    "Idempotent/Transactional producer requires broker >= 0.11 (got api_version=%s)"
                    % (self.config['api_version'],))
            log.warning(
                "%s: Idempotence will be disabled because broker api_version %s < (0, 11)",
                str(self), self.config['api_version'])
            self.config['enable_idempotence'] = False

        if self.config['enable_idempotence']:
            self._transaction_manager = TransactionManager(
                transactional_id=self.config['transactional_id'],
                transaction_timeout_ms=self.config['transaction_timeout_ms'],
                retry_backoff_ms=self.config['retry_backoff_ms'],
                api_version=self.config['api_version'],
                metadata=self._metadata,
            )
            if self._transaction_manager.is_transactional():
                log.info("%s: Instantiated a transactional producer.", str(self))
            else:
                log.info("%s: Instantiated an idempotent producer.", str(self))

            if not user_set_acks:
                self.config['acks'] = -1

        message_version = self.max_usable_produce_magic(self.config['api_version'])
        self._accumulator = RecordAccumulator(
                transaction_manager=self._transaction_manager,
                message_version=message_version,
                **self.config)
        guarantee_message_order = False
        if self.config['enable_idempotence'] or self.config['max_in_flight_requests_per_connection'] == 1:
            guarantee_message_order = True
        self._sender = Sender(client, self._metadata,
                              self._accumulator,
                              metrics=self._metrics,
                              transaction_manager=self._transaction_manager,
                              guarantee_message_order=guarantee_message_order,
                              **self.config)
        self._sender.daemon = True
        self._sender.start()
        self._closed = False

        self._cleanup = self._cleanup_factory()
        atexit.register(self._cleanup)
        log.debug("%s: Kafka producer started", str(self))

    def bootstrap_connected(self):
        """Return True if the bootstrap is connected."""
        return self._sender.bootstrap_connected()

    def _cleanup_factory(self):
        """Build a cleanup closure that doesn't increase our ref count"""
        _self = weakref.proxy(self)
        def wrapper():
            try:
                _self.close(timeout=1, null_logger=True)
            except (ReferenceError, AttributeError):
                pass
        return wrapper

    def _unregister_cleanup(self):
        if getattr(self, '_cleanup', None):
            atexit.unregister(self._cleanup)
        self._cleanup = None

    def __del__(self):
        self.close(timeout=1, null_logger=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self, timeout=None, null_logger=False):
        """Close this producer.

        Arguments:
            timeout (float, optional): timeout in seconds to wait for completion.
        """
        if null_logger:
            # Disable logger during destruction to avoid touching dangling references
            class NullLogger:
                def __getattr__(self, name):
                    return lambda *args: None

            global log
            log = NullLogger()

        # drop our atexit handler now to avoid leaks
        self._unregister_cleanup()

        if not hasattr(self, '_closed') or self._closed:
            log.info('%s: Kafka producer closed', str(self))
            return
        if timeout is None:
            timeout = threading.TIMEOUT_MAX
        if not (0 <= timeout <= threading.TIMEOUT_MAX):
            raise ValueError('Invalid timeout: %s' % timeout)

        log.info("%s: Closing the Kafka producer with %s secs timeout.", str(self), timeout)
        self.flush(timeout)
        invoked_from_callback = bool(threading.current_thread() is self._sender)
        if timeout > 0:
            if invoked_from_callback:
                log.warning("%s: Overriding close timeout %s secs to 0 in order to"
                            " prevent useless blocking due to self-join. This"
                            " means you have incorrectly invoked close with a"
                            " non-zero timeout from the producer call-back.",
                            str(self), timeout)
            else:
                # Try to close gracefully.
                if self._sender is not None:
                    self._sender.initiate_close()
                    self._sender.join(timeout)

        if self._sender is not None and self._sender.is_alive():
            log.info("%s: Proceeding to force close the producer since pending"
                     " requests could not be completed within timeout %s.",
                     str(self), timeout)
            self._sender.force_close()

        if self._metrics:
            self._metrics.close()
        try:
            self.config['key_serializer'].close()
        except AttributeError:
            pass
        try:
            self.config['value_serializer'].close()
        except AttributeError:
            pass
        self._closed = True
        log.debug("%s: The Kafka producer has closed.", str(self))

    def partitions_for(self, topic):
        """Returns set of all known partitions for the topic."""
        return self._wait_on_metadata(topic, self.config['max_block_ms'])

    @classmethod
    def max_usable_produce_magic(cls, api_version):
        if api_version >= (0, 11):
            return 2
        elif api_version >= (0, 10, 0):
            return 1
        else:
            return 0

    def _estimate_size_in_bytes(self, key, value, headers=None):
        if headers is None:
            headers = []
        magic = self.max_usable_produce_magic(self.config['api_version'])
        if magic == 2:
            return DefaultRecordBatchBuilder.estimate_size_in_bytes(
                key, value, headers)
        else:
            return LegacyRecordBatchBuilder.estimate_size_in_bytes(
                magic, self.config['compression_type'], key, value)

    def init_transactions(self):
        """
        Needs to be called before any other methods when the transactional.id is set in the configuration.

        This method does the following:
          1. Ensures any transactions initiated by previous instances of the producer with the same
             transactional_id are completed. If the previous instance had failed with a transaction in
             progress, it will be aborted. If the last transaction had begun completion,
             but not yet finished, this method awaits its completion.
          2. Gets the internal producer id and epoch, used in all future transactional
             messages issued by the producer.

        Note that this method will raise KafkaTimeoutError if the transactional state cannot
        be initialized before expiration of `max_block_ms`.

        Retrying after a KafkaTimeoutError will continue to wait for the prior request to succeed or fail.
        Retrying after any other exception will start a new initialization attempt.
        Retrying after a successful initialization will do nothing.

        Raises:
            IllegalStateError: if no transactional_id has been configured
            AuthorizationError: fatal error indicating that the configured
                transactional_id is not authorized.
            KafkaError: if the producer has encountered a previous fatal error or for any other unexpected error
            KafkaTimeoutError: if the time taken for initialize the transaction has surpassed `max.block.ms`.
        """
        if not self._transaction_manager:
            raise Errors.IllegalStateError("Cannot call init_transactions without setting a transactional_id.")
        if self._init_transactions_result is None:
            self._init_transactions_result = self._transaction_manager.initialize_transactions()
            self._sender.wakeup()

        try:
            if not self._init_transactions_result.wait(timeout_ms=self.config['max_block_ms']):
                raise Errors.KafkaTimeoutError("Timeout expired while initializing transactional state in %s ms." % (self.config['max_block_ms'],))
        finally:
            if self._init_transactions_result.failed:
                self._init_transactions_result = None

    def begin_transaction(self):
        """ Should be called before the start of each new transaction.

        Note that prior to the first invocation of this method,
        you must invoke `init_transactions()` exactly one time.

        Raises:
            ProducerFencedError if another producer is with the same
                transactional_id is active.
        """
        # Set the transactional bit in the producer.
        if not self._transaction_manager:
            raise Errors.IllegalStateError("Cannot use transactional methods without enabling transactions")
        self._transaction_manager.begin_transaction()

    def send_offsets_to_transaction(self, offsets, group_metadata):
        """
        Sends a list of consumed offsets to the consumer group coordinator, and also marks
        those offsets as part of the current transaction. These offsets will be considered
        consumed only if the transaction is committed successfully.

        This method should be used when you need to batch consumed and produced messages
        together, typically in a consume-transform-produce pattern.

        Arguments:
            offsets ({TopicPartition: OffsetAndMetadata}): map of topic-partition -> offsets to commit
                as part of current transaction.
            group_metadata (ConsumerGroupMetadata or str): full group metadata from
                KafkaConsumer.group_metadata() (preferred - enables broker-side fencing
                of stale consumer instances per KIP-447 against Kafka 2.5+ brokers), or
                a bare consumer_group_id str for backwards compatibility.

        Raises:
            IllegalStateError: if no transactional_id, or transaction has not been started.
            ProducerFencedError: fatal error indicating another producer with the same transactional_id is active.
            UnsupportedVersionError: fatal error indicating the broker does not support transactions (i.e. if < 0.11).
            UnsupportedForMessageFormatError: fatal error indicating the message format used for the offsets
                topic on the broker does not support transactions.
            AuthorizationError: fatal error indicating that the configured transactional_id is not authorized.
            KafkaErro:r if the producer has encountered a previous fatal or abortable error, or for any
                other unexpected error
        """
        if not self._transaction_manager:
            raise Errors.IllegalStateError("Cannot use transactional methods without enabling transactions")
        result = self._transaction_manager.send_offsets_to_transaction(offsets, group_metadata)
        self._sender.wakeup()
        result.wait()

    def commit_transaction(self):
        """ Commits the ongoing transaction.

        Raises: ProducerFencedError if another producer with the same
                transactional_id is active.
        """
        if not self._transaction_manager:
            raise Errors.IllegalStateError("Cannot commit transaction since transactions are not enabled")
        result = self._transaction_manager.begin_commit()
        self._sender.wakeup()
        result.wait()

    def abort_transaction(self):
        """ Aborts the ongoing transaction.

        Raises: ProducerFencedError if another producer with the same
                transactional_id is active.
        """
        if not self._transaction_manager:
            raise Errors.IllegalStateError("Cannot abort transaction since transactions are not enabled.")
        result = self._transaction_manager.begin_abort()
        self._sender.wakeup()
        result.wait()

    def send(self, topic, value=None, key=None, headers=None, partition=None, timestamp_ms=None):
        """Publish a message to a topic.

        Arguments:
            topic (str): topic where the message will be published
            value (optional): message value. Must be type bytes, or be
                serializable to bytes via configured value_serializer. If value
                is None, key is required and message acts as a 'delete'.
                See kafka compaction documentation for more details:
                https://kafka.apache.org/documentation.html#compaction
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
            headers (optional): a list of header key value pairs. List items
                are tuples of str key and bytes value.
            timestamp_ms (int, optional): epoch milliseconds (from Jan 1 1970 UTC)
                to use as the message timestamp. Defaults to current time.

        Returns:
            FutureRecordMetadata: resolves to RecordMetadata

        Raises:
            KafkaTimeoutError: if unable to fetch topic metadata, or unable
                to obtain memory buffer prior to configured max_block_ms.
            TypeError: if topic is not a string; if serialized key/value
                are not type bytes/bytearray/memoryview or None; or headers
                is not a list of (str, bytes) items.
            ValueError: if both key and value are None; partitioner fails to
                assign a partition, or topic is invalid (must be chars
                [a-zA-Z0-9._-], and less than 250 length).
            IllegalStateError: if KafkaProducer is already closed.
        """
        if self._closed:
            raise Errors.IllegalStateError('KafkaProducer already closed!')
        if value is None and self.config['api_version'] < (0, 8, 1):
            raise ValueError('Null messages require kafka >= 0.8.1')
        if value is None and key is None:
            raise ValueError('Need at least one: key or value')
        if headers is None:
            headers = []
        if not isinstance(headers, list):
            raise TypeError('headers must be list-type')
        if not all(isinstance(item, tuple) and len(item) == 2 and isinstance(item[0], str) and isinstance(item[1], bytes) for item in headers):
            raise TypeError('All headers items must be (str, bytes) tuples')

        key_bytes = self._serialize(
            self.config['key_serializer'],
            topic, headers, key)
        value_bytes = self._serialize(
            self.config['value_serializer'],
            topic, headers, value)
        if type(key_bytes) not in (bytes, bytearray, memoryview, type(None)):
            raise TypeError("Unsupported type for serialized key: %s" % type(key_bytes))
        if type(value_bytes) not in (bytes, bytearray, memoryview, type(None)):
            raise TypeError("Unsupported type for serialized value: %s" % type(value_bytes))

        if self._metadata.partitions_for_topic(topic) is None:
            try:
                self._wait_on_metadata(topic, self.config['max_block_ms'])
            except Errors.BrokerResponseError as e:
                log.error("%s: Exception occurred waiting for metadata during message send: %s", str(self), e)
                return FutureRecordMetadata(
                    FutureProduceResult(TopicPartition(topic, partition)),
                    -1, None, None,
                    len(key_bytes) if key_bytes is not None else -1,
                    len(value_bytes) if value_bytes is not None else -1,
                    sum(len(h_key.encode("utf-8")) + len(h_value) for h_key, h_value in headers) if headers else -1,
                ).failure(e)

        # Track if the user passed an explicit partition b/c sticky logic does not apply
        explicit_partition = partition is not None
        partition = self._partition(topic, partition, key, value, key_bytes, value_bytes)
        if partition is None:
            raise ValueError(f'Partitioner did not assign a partition for topic {topic}!')

        message_size = self._estimate_size_in_bytes(key_bytes, value_bytes, headers)
        self._ensure_valid_record_size(message_size)

        tp = TopicPartition(topic, partition)
        log.debug("%s: Sending (key=%r value=%r headers=%r) to %s", str(self), key, value, headers, tp)

        if self._transaction_manager and self._transaction_manager.is_transactional():
            self._transaction_manager.maybe_add_partition_to_transaction(tp)

        # KIP-480: when sticky-aware partitioning is in play (no explicit
        # partition, no key), try once with abort_on_new_batch=True. If the
        # accumulator would have to allocate a fresh batch for this partition,
        # rotate the sticky partition first and re-pick. The record that
        # *triggers* the new batch then lands on the rotated partition, not
        # the next one.
        sticky_eligible = not explicit_partition and key_bytes is None
        result = self._accumulator.append(tp, timestamp_ms, key_bytes, value_bytes, headers,
                                          abort_on_new_batch=sticky_eligible)
        future, batch_is_full, new_batch_created, abort_for_new_batch = result
        if abort_for_new_batch:
            prev_partition = partition
            on_new_batch = getattr(self.config['partitioner'], 'on_new_batch', None)
            if on_new_batch is not None:
                on_new_batch(topic, self._metadata, prev_partition)
            # Re-pick - sticky cache may now point at a different partition.
            partition = self._partition(topic, None, key, value, key_bytes, value_bytes)
            tp = TopicPartition(topic, partition)
            if self._transaction_manager and self._transaction_manager.is_transactional():
                self._transaction_manager.maybe_add_partition_to_transaction(tp)
            result = self._accumulator.append(tp, timestamp_ms, key_bytes, value_bytes, headers,
                                              abort_on_new_batch=False)
            future, batch_is_full, new_batch_created, _ = result

        if batch_is_full or new_batch_created:
            log.debug("%s: Waking up the sender since %s is either full or"
                      " getting a new batch", str(self), tp)
            self._sender.wakeup()
        return future

    def flush(self, timeout=None):
        """
        Invoking this method makes all buffered records immediately available
        to send (even if linger_ms is greater than 0) and blocks on the
        completion of the requests associated with these records. The
        post-condition of :meth:`~kafka.KafkaProducer.flush` is that any
        previously sent record will have completed
        (e.g. Future.is_done() == True). A request is considered completed when
        either it is successfully acknowledged according to the 'acks'
        configuration for the producer, or it results in an error.

        Other threads can continue sending messages while one thread is blocked
        waiting for a flush call to complete; however, no guarantee is made
        about the completion of messages sent after the flush call begins.

        Arguments:
            timeout (float, optional): timeout in seconds to wait for completion.

        Raises:
            KafkaTimeoutError: failure to flush buffered records within the
                provided timeout
        """
        log.debug("%s: Flushing accumulated records in producer.", str(self))
        self._accumulator.begin_flush()
        self._sender.wakeup()
        self._accumulator.await_flush_completion(timeout=timeout)

    def _ensure_valid_record_size(self, size):
        """Validate that the record size isn't too large."""
        if size > self.config['max_request_size']:
            raise Errors.MessageSizeTooLargeError(
                "The message is %d bytes when serialized which is larger than"
                " the maximum request size you have configured with the"
                " max_request_size configuration" % (size,))

    def _wait_on_metadata(self, topic, max_wait_ms):
        """
        Wait for cluster metadata including partitions for the given topic to
        be available.

        Arguments:
            topic (str): topic we want metadata for
            max_wait (float): maximum time in secs for waiting on the metadata

        Returns:
            set: partition ids for the topic

        Raises:
            KafkaTimeoutError: if partitions for topic were not obtained before
                specified max_wait timeout
            TopicAuthorizationFailedError: if not authorized to access topic
            Non-retriable errors that cause metadata refresh to fail
        """
        partitions = self._metadata.partitions_for_topic(topic)
        if partitions is not None:
            return partitions
        self._sender.add_topic(topic)
        timer = Timer(max_wait_ms)
        metadata_event = threading.Event()
        while not timer.expired:
            log.debug("%s: Requesting metadata update for topic %s", str(self), topic)
            metadata_event.clear()
            future = self._metadata.request_update()
            future.add_both(lambda e, *args: e.set(), metadata_event)
            self._sender.wakeup()
            metadata_event.wait(timer.timeout_ms / 1000)
            if not future.is_done:
                raise Errors.KafkaTimeoutError(
                    "Failed to update metadata after %.1f secs." % (max_wait_ms / 1000,))
            elif future.failed() and not future.retriable():
                raise future.exception
            elif topic in self._metadata.unauthorized_topics:
                raise Errors.TopicAuthorizationFailedError(set([topic]))
            else:
                log.debug("%s: _wait_on_metadata woke after %s secs.", str(self), timer.elapsed_ms / 1000)
            partitions = self._metadata.partitions_for_topic(topic)
            if partitions is not None:
                return partitions
        else:
            raise Errors.KafkaTimeoutError("Failed to update metadata after %.1f secs." % (max_wait_ms / 1000,))

    def _serialize(self, serializer, topic, headers, data):
        if serializer is None:
            return data
        try:
            return serializer.serialize(topic, headers, data)
        except TypeError:
            global _LOGGED_SERIALIZE_WARNING
            if not _LOGGED_SERIALIZE_WARNING:
                warnings.warn('serializer does not implement serialize(topic, headers, data)', category=DeprecationWarning)
                LOGGED_SERIALIZE_WARNING = True
            return serializer.serialize(topic, data)

    def _partition(self, topic, partition, key, value,
                   serialized_key, serialized_value):
        if topic not in self._metadata.topics():
            return None
        if partition is not None:
            if partition < 0:
                raise ValueError('partition must be >= 0')
            all_partitions = self._metadata.partitions_for_topic(topic)
            if all_partitions is None or partition not in all_partitions:
                raise ValueError('Unrecognized partition %s for topic %s' % (partition, topic))
            return partition

        partitioner = self.config['partitioner']
        if not isinstance(partitioner, Partitioner):
            warnings.warn('partitioner does not implement kafka.partitioner.Partitioner', category=DeprecationWarning)
            return partitioner.partition(topic, serialized_key, self._metadata)
        return partitioner.partition(
            topic, key, serialized_key, value, serialized_value, self._metadata)

    def metrics(self, raw=False):
        """Get metrics on producer performance.

        This is ported from the Java Producer, for details see:
        https://kafka.apache.org/documentation/#producer_monitoring

        Warning:
            This is an unstable interface. It may change in future
            releases without warning.
        """
        if not self._metrics:
            return
        if raw:
            return self._metrics.metrics.copy()

        metrics = {}
        for k, v in self._metrics.metrics.copy().items():
            if k.group not in metrics:
                metrics[k.group] = {}
            if k.name not in metrics[k.group]:
                metrics[k.group][k.name] = {}
            metrics[k.group][k.name] = v.value()
        return metrics

    def __str__(self):
        return "<KafkaProducer client_id=%s transactional_id=%s>" % (self.config.get('client_id', None), self.config.get('transactional_id', None))
