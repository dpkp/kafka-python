"""KafkaAdminClient — high-level Kafka cluster administration."""

import copy
import logging
import selectors
import socket
import time

import kafka.errors as Errors
from kafka.errors import KafkaConfigurationError, UnrecognizedBrokerVersion
from kafka.metrics import MetricConfig, Metrics
from kafka.net.compat import KafkaNetClient
from kafka.protocol.metadata import MetadataRequest, FindCoordinatorRequest
from kafka.version import __version__

from kafka.admin._acls import ACLAdminMixin
from kafka.admin._cluster import ClusterAdminMixin
from kafka.admin._configs import ConfigAdminMixin
from kafka.admin._groups import GroupAdminMixin
from kafka.admin._partitions import PartitionAdminMixin
from kafka.admin._topics import TopicAdminMixin
from kafka.admin._users import UserAdminMixin

log = logging.getLogger(__name__)


class KafkaAdminClient(
    ACLAdminMixin,
    ClusterAdminMixin,
    ConfigAdminMixin,
    GroupAdminMixin,
    PartitionAdminMixin,
    TopicAdminMixin,
    UserAdminMixin,
):
    """A class for administering the Kafka cluster.

    Warning:
        This is an unstable interface that was recently added and is subject to
        change without warning. In particular, many methods currently return
        raw protocol tuples. In future releases, we plan to make these into
        nicer, more pythonic objects. Unfortunately, this will likely break
        those interfaces.

    The KafkaAdminClient class will negotiate for the latest version of each message
    protocol format supported by both the kafka-python client library and the
    Kafka broker. Usage of optional fields from protocol versions that are not
    supported by the broker will result in IncompatibleBrokerVersion exceptions.

    Use of this class requires a minimum broker version >= 0.10.0.0.

    Keyword Arguments:
        bootstrap_servers: 'host[:port]' string (or list of 'host[:port]'
            strings) that the consumer should contact to bootstrap initial
            cluster metadata. This does not have to be the full node list.
            It just needs to have at least one broker that will respond to a
            Metadata API Request. Default port is 9092. If no servers are
            specified, will default to localhost:9092.
        client_id (str): a name for this client. This string is passed in
            each request to servers and can be used to identify specific
            server-side log entries that correspond to this client. Also
            submitted to GroupCoordinator for logging with respect to
            consumer group administration. Default: 'kafka-python-{version}'
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
        request_timeout_ms (int): Client request timeout in milliseconds.
            Default: 30000.
        connections_max_idle_ms: Close idle connections after the number of
            milliseconds specified by this config. The broker closes idle
            connections after connections.max.idle.ms, so this avoids hitting
            unexpected socket disconnected errors on the client.
            Default: 540000
        retry_backoff_ms (int): Milliseconds to backoff when retrying on
            errors. Default: 100.
        max_in_flight_requests_per_connection (int): Requests are pipelined
            to kafka brokers up to this number of maximum requests per
            broker connection. Default: 5.
        receive_buffer_bytes (int): The size of the TCP receive buffer
            (SO_RCVBUF) to use when reading data. Default: None (relies on
            system defaults). Java client defaults to 32768.
        send_buffer_bytes (int): The size of the TCP send buffer
            (SO_SNDBUF) to use when sending data. Default: None (relies on
            system defaults). Java client defaults to 131072.
        socket_options (list): List of tuple-arguments to socket.setsockopt
            to apply to broker connection sockets. Default:
            [(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)]
        metadata_max_age_ms (int): The period of time in milliseconds after
            which we force a refresh of metadata even if we haven't seen any
            partition leadership changes to proactively discover any new
            brokers or partitions. Default: 300000
        security_protocol (str): Protocol used to communicate with brokers.
            Valid values are: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL.
            Default: PLAINTEXT.
        ssl_context (ssl.SSLContext): Pre-configured SSLContext for wrapping
            socket connections. If provided, all other ssl_* configurations
            will be ignored. Default: None.
        ssl_check_hostname (bool): Flag to configure whether SSL handshake
            should verify that the certificate matches the broker's hostname.
            Default: True.
        ssl_cafile (str): Optional filename of CA file to use in certificate
            verification. Default: None.
        ssl_certfile (str): Optional filename of file in PEM format containing
            the client certificate, as well as any CA certificates needed to
            establish the certificate's authenticity. Default: None.
        ssl_keyfile (str): Optional filename containing the client private key.
            Default: None.
        ssl_password (str): Optional password to be used when loading the
            certificate chain. Default: None.
        ssl_crlfile (str): Optional filename containing the CRL to check for
            certificate expiration. By default, no CRL check is done. When
            providing a file, only the leaf certificate will be checked against
            this CRL. Default: None.
        api_version (tuple): Specify which Kafka API version to use. If set
            to None, KafkaConnectionManager will attempt to infer the
            broker version by probing various APIs. Example: (0, 10, 2).
            Default: None
        bootstrap_timeout_ms (int): number of milliseconds to throw a
            timeout exception from the constructor when bootstrapping.
            Default: 2000.
        selector (selectors.BaseSelector): Provide a specific selector
            implementation to use for I/O multiplexing.
            Default: selectors.DefaultSelector
        metrics (kafka.metrics.Metrics): Optionally provide a metrics
            instance for capturing network IO stats. Default: None.
        metric_group_prefix (str): Prefix for metric names. Default: ''
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
        sasl_oauth_token_provider (kafka.sasl.oauth.AbstractTokenProvider): OAuthBearer
            token provider instance. Default: None
        socks5_proxy (str): Socks5 proxy url. Default: None
        kafka_client (callable): Custom class / callable for creating KafkaNetClient instances
    """
    DEFAULT_CONFIG = {
        # client configs
        'bootstrap_servers': 'localhost',
        'client_id': 'kafka-python-' + __version__,
        'request_timeout_ms': 30000,
        'connections_max_idle_ms': 9 * 60 * 1000,
        'reconnect_backoff_ms': 50,
        'reconnect_backoff_max_ms': 30000,
        'max_in_flight_requests_per_connection': 5,
        'receive_buffer_bytes': None,
        'send_buffer_bytes': None,
        'socket_options': [(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)],
        'sock_chunk_bytes': 4096,  # undocumented experimental option
        'sock_chunk_buffer_count': 1000,  # undocumented experimental option
        'retry_backoff_ms': 100,
        'metadata_max_age_ms': 300000,
        'security_protocol': 'PLAINTEXT',
        'ssl_context': None,
        'ssl_check_hostname': True,
        'ssl_cafile': None,
        'ssl_certfile': None,
        'ssl_keyfile': None,
        'ssl_password': None,
        'ssl_crlfile': None,
        'api_version': None,
        'bootstrap_timeout_ms': 2000,
        'selector': selectors.DefaultSelector,
        'sasl_mechanism': None,
        'sasl_plain_username': None,
        'sasl_plain_password': None,
        'sasl_kerberos_name': None,
        'sasl_kerberos_service_name': 'kafka',
        'sasl_kerberos_domain_name': None,
        'sasl_oauth_token_provider': None,
        'socks5_proxy': None,

        # metrics configs
        'metric_reporters': [],
        'metrics_num_samples': 2,
        'metrics_sample_window_ms': 30000,
        'kafka_client': KafkaNetClient,
    }

    def __init__(self, **configs):
        log.debug("Starting KafkaAdminClient with configuration: %s", configs)
        extra_configs = set(configs).difference(self.DEFAULT_CONFIG)
        if extra_configs:
            raise KafkaConfigurationError("Unrecognized configs: {}".format(extra_configs))

        self.config = copy.copy(self.DEFAULT_CONFIG)
        self.config.update(configs)

        # Configure metrics
        metrics_tags = {'client-id': self.config['client_id']}
        metric_config = MetricConfig(samples=self.config['metrics_num_samples'],
                                     time_window_ms=self.config['metrics_sample_window_ms'],
                                     tags=metrics_tags)
        reporters = [reporter() for reporter in self.config['metric_reporters']]
        self._metrics = Metrics(metric_config, reporters)

        self._client = self.config['kafka_client'](
            metrics=self._metrics,
            metric_group_prefix='admin',
            **self.config
        )
        # Goal: migrate all self._client calls -> self._manager (skipping compat layer)
        self._manager = self._client._manager

        # Run all IO on a dedicated background thread; public admin methods
        # block on cross-thread Events via self._manager.run(...).
        self._manager.start()

        # Bootstrap on __init__
        self._manager.run(self._manager.bootstrap, self.config['bootstrap_timeout_ms'])
        self._closed = False
        self._controller_id = None
        self._coordinator_cache = {}  # {group_id: node_id}
        log.debug("KafkaAdminClient started.")

    def close(self):
        """Close the KafkaAdminClient connection to the Kafka broker."""
        if not hasattr(self, '_closed') or self._closed:
            log.info("KafkaAdminClient already closed.")
            return

        self._metrics.close()
        self._client.close()
        self._closed = True
        log.debug("KafkaAdminClient is now closed.")

    def _validate_timeout(self, timeout_ms):
        """Validate the timeout is set or use the configuration default."""
        return timeout_ms or self.config['request_timeout_ms']

    # -- Routing primitives (used by mixins) ----------------------------------

    async def _refresh_controller_id(self, timeout_ms=30000):
        """Determine the Kafka cluster controller."""
        if self._manager.broker_version < (0, 10):
            raise UnrecognizedBrokerVersion(
                "Kafka Admin Client controller requests requires broker version >= (0, 10)")

        request = MetadataRequest()
        timeout_at = time.monotonic() + timeout_ms / 1000
        while time.monotonic() < timeout_at:
            response = await self._manager.send(request)
            controller_id = response.controller_id
            if controller_id == -1:
                log.warning("Controller ID not available, got -1")
                await self._manager._net.sleep(1)
                continue
            return controller_id
        else:
            raise Errors.NodeNotReadyError('controller')

    async def _find_coordinator_id(self, group_id):
        """Find the broker node_id of the coordinator for a consumer group.

        Results are cached; subsequent calls for the same group_id return
        the cached value without a network round-trip.
        """
        cached = self._coordinator_cache.get(group_id)
        if cached is not None:
            return cached
        request = FindCoordinatorRequest(group_id, 0, max_version=2)  # key_type=0 for group
        response = await self._manager.send(request)
        error_type = Errors.for_code(response.error_code)
        if error_type is not Errors.NoError:
            raise error_type(
                "FindCoordinatorRequest failed with response '{}'."
                .format(response))
        self._coordinator_cache[group_id] = response.node_id
        return response.node_id

    async def _send_request_to_controller(self, request, get_errors_fn=lambda r: (), raise_errors=True, ignore_errors=()):
        """Send a Kafka protocol message to the cluster controller.

        Retries once on NotControllerError after refreshing the controller id.
        """
        if self._controller_id is None or self._controller_id == -1:
            self._controller_id = await self._refresh_controller_id()

        response = await self._manager.send(request, node_id=self._controller_id)

        # Refresh controller and retry on NotControllerError
        if Errors.NotControllerError in get_errors_fn(response):
            self._controller_id = await self._refresh_controller_id()
            response = await self._manager.send(request, node_id=self._controller_id)

        for error_type in get_errors_fn(response):
            if error_type is Errors.NoError:
                continue
            elif error_type is Errors.NotControllerError:
                raise RuntimeError("Failed to find active controller id!")
            elif raise_errors and error_type not in ignore_errors:
                raise error_type(
                    "Request '{}' failed with response '{}'."
                    .format(request, response))
        return response
