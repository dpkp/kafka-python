from __future__ import absolute_import

import copy
import logging
import socket
from kafka.client_async import KafkaClient, selectors
from kafka.errors import (
    KafkaConfigurationError, UnsupportedVersionError, NodeNotReadyError, NotControllerError, KafkaConnectionError)
from kafka.metrics import MetricConfig, Metrics
from kafka.protocol.admin import (
    CreateTopicsRequest, DeleteTopicsRequest, DescribeConfigsRequest, AlterConfigsRequest, CreatePartitionsRequest,
    ListGroupsRequest, DescribeGroupsRequest)
from kafka.protocol.metadata import MetadataRequest
from kafka.version import __version__

log = logging.getLogger(__name__)

class KafkaAdmin(object):
    """An class for administering the kafka cluster.

    Warning:
        This is an unstable interface that was recently added and is subject to
        change without warning. In particular, many methods currently return
        raw protocol tuples. In future releases, we plan to make these into
        nicer, more pythonic objects. Unfortunately, this will likely break
        those interfaces.

    The KafkaAdmin class will negotiate for the latest version of each message protocol format supported
    by both the kafka-python client library and the kafka broker.  Usage of optional fields from protocol
    versions that are not supported by the broker will result in UnsupportedVersionError exceptions.

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
            milliseconds to wait when reconnecting to a broker that has
            repeatedly failed to connect. If provided, the backoff per host
            will increase exponentially for each consecutive connection
            failure, up to this maximum. To avoid connection storms, a
            randomization factor of 0.2 will be applied to the backoff
            resulting in a random range between 20% below and 20% above
            the computed value. Default: 1000.
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
            Valid values are: PLAINTEXT, SSL. Default: PLAINTEXT.
        ssl_context (ssl.SSLContext): Pre-configured SSLContext for wrapping
            socket connections. If provided, all other ssl_* configurations
            will be ignored. Default: None.
        ssl_check_hostname (bool): Flag to configure whether SSL handshake
            should verify that the certificate matches the broker's hostname.
            Default: True.
        ssl_cafile (str): Optional filename of CA file to use in certificate
            veriication. Default: None.
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
            this CRL. The CRL can only be checked with Python 3.4+ or 2.7.9+.
            Default: None.
        api_version (tuple): Specify which Kafka API version to use. If set
            to None, KafkaClient will attempt to infer the broker version by
            probing various APIs. Example: (0, 10, 2). Default: None
        api_version_auto_timeout_ms (int): number of milliseconds to throw a
            timeout exception from the constructor when checking the broker
            api version. Only applies if api_version is None
        selector (selectors.BaseSelector): Provide a specific selector
            implementation to use for I/O multiplexing.
            Default: selectors.DefaultSelector
        metrics (kafka.metrics.Metrics): Optionally provide a metrics
            instance for capturing network IO stats. Default: None.
        metric_group_prefix (str): Prefix for metric names. Default: ''
        sasl_mechanism (str): string picking sasl mechanism when security_protocol
            is SASL_PLAINTEXT or SASL_SSL. Currently only PLAIN is supported.
            Default: None
        sasl_plain_username (str): username for sasl PLAIN authentication.
            Default: None
        sasl_plain_password (str): password for sasl PLAIN authentication.
            Default: None
        sasl_kerberos_service_name (str): Service name to include in GSSAPI
            sasl mechanism handshake. Default: 'kafka'

    """
    DEFAULT_CONFIG = {
        # client configs
        'bootstrap_servers': 'localhost',
        'client_id': 'kafka-python-' + __version__,
        'request_timeout_ms': 30000,
        'connections_max_idle_ms': 9 * 60 * 1000,
        'reconnect_backoff_ms': 50,
        'reconnect_backoff_max_ms': 1000,
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
        'api_version_auto_timeout_ms': 2000,
        'selector': selectors.DefaultSelector,
        'sasl_mechanism': None,
        'sasl_plain_username': None,
        'sasl_plain_password': None,
        'sasl_kerberos_service_name': 'kafka',

        # metrics configs
        'metric_reporters' : [],
        'metrics_num_samples': 2,
        'metrics_sample_window_ms': 30000,
    }

    def __init__(self, **configs):
        log.debug("Starting Kafka administration interface")
        extra_configs = set(configs).difference(self.DEFAULT_CONFIG)
        if extra_configs:
            raise KafkaConfigurationError("Unrecognized configs: %s" % (extra_configs,))

        self.config = copy.copy(self.DEFAULT_CONFIG)
        self.config.update(configs)

        # api_version was previously a str. accept old format for now
        if isinstance(self.config['api_version'], str):
            deprecated = self.config['api_version']
            if deprecated == 'auto':
                self.config['api_version'] = None
            else:
                self.config['api_version'] = tuple(map(int, deprecated.split('.')))
            log.warning('use api_version=%s [tuple] -- "%s" as str is deprecated',
                        str(self.config['api_version']), deprecated)

        # Configure metrics
        metrics_tags = {'client-id': self.config['client_id']}
        metric_config = MetricConfig(samples=self.config['metrics_num_samples'],
                                     time_window_ms=self.config['metrics_sample_window_ms'],
                                     tags=metrics_tags)
        reporters = [reporter() for reporter in self.config['metric_reporters']]
        self._metrics = Metrics(metric_config, reporters)

        self._client = KafkaClient(metrics=self._metrics, metric_group_prefix='admin',
                             **self.config)

        # Get auto-discovered version from client if necessary
        if self.config['api_version'] is None:
            self.config['api_version'] = self._client.config['api_version']

        self._closed = False
        self._refresh_controller_id()
        log.debug('Kafka administration interface started')

    def close(self):
        """Close the administration connection to the kafka broker"""
        if not hasattr(self, '_closed') or self._closed:
            log.info('Kafka administration interface already closed')
            return

        self._metrics.close()
        self._client.close()
        self._closed = True
        log.debug('Kafka administration interface has closed')

    def _matching_api_version(self, operation):
        """Find matching api version, the lesser of either the latest api version the library supports, or
        the max version supported by the broker

        :param operation: An operation array from kafka.protocol
        :return: The max matching version number between client and broker
        """
        version = min(len(operation) - 1,
                      self._client.get_api_versions()[operation[0].API_KEY][1])
        if version < self._client.get_api_versions()[operation[0].API_KEY][0]:
            # max library version is less than min broker version.  Not sure any brokers
            # actually set a min version greater than 0 right now, tho.  But maybe in the future?
            raise UnsupportedVersionError(
                "Could not find matching protocol version for {}"
                .format(operation.__name__))
        return version

    def _validate_timeout(self, timeout_ms):
        """Validate the timeout is set or use the configuration default

        :param timeout_ms: The timeout provided by api call, in milliseconds
        :return: The timeout to use for the operation
        """
        return timeout_ms or self.config['request_timeout_ms']

    def _refresh_controller_id(self):
        """Determine the kafka cluster controller
        """
        response = self._send_request_to_node(
            self._client.least_loaded_node(),
            MetadataRequest[1]([])
        )
        self._controller_id = response.controller_id
        version = self._client.check_version(self._controller_id)
        if version < (0, 10, 0):
            raise UnsupportedVersionError(
                "Kafka Admin interface not supported for cluster controller version {} < 0.10.0.0"
                    .format(version))

    def _send_request_to_node(self, node, request):
        """Send a kafka protocol message to a specific broker.  Will block until the message result is received.

        :param node: The broker id to which to send the message
        :param request: The message to send
        :return: The kafka protocol response for the message
        :exception: The exception if the message could not be sent
        """
        while not self._client.ready(node):
            # connection to broker not ready, poll until it is or send will fail with NodeNotReadyError
            self._client.poll()
        future = self._client.send(node, request)
        self._client.poll(future=future)
        if future.succeeded():
            return future.value
        else:
            raise future.exception # pylint: disable-msg=raising-bad-type

    def _send(self, request):
        """Send a kafka protocol message to the cluster controller.  Will block until the message result is received.

        :param request: The message to send
        :return The kafka protocol response for the message
        :exception NodeNotReadyError: If the controller connection can't be established
        """
        remaining_tries = 2
        while remaining_tries > 0:
            remaining_tries = remaining_tries - 1
            try:
                return self._send_request_to_node(self._controller_id, request)
            except (NotControllerError, KafkaConnectionError) as e:
                # controller changed?  refresh it
                self._refresh_controller_id()
        raise NodeNotReadyError(self._controller_id)

    @staticmethod
    def _convert_new_topic_request(new_topic):
        return (
            new_topic.name,
            new_topic.num_partitions,
            new_topic.replication_factor,
            [
                (partition_id, replicas) for partition_id, replicas in new_topic.replica_assignments.items()
            ],
            [
                (config_key, config_value) for config_key, config_value in new_topic.topic_configs.items()
            ]
        )

    def create_topics(self, new_topics, timeout_ms=None, validate_only=None):
        """Create new topics in the cluster.

        :param new_topics: Array of NewTopic objects
        :param timeout_ms: Milliseconds to wait for new topics to be created before broker returns
        :param validate_only: If True, don't actually create new topics.  Not supported by all versions.
        :return: Appropriate version of CreateTopicResponse class
        """
        version = self._matching_api_version(CreateTopicsRequest)
        timeout_ms = self._validate_timeout(timeout_ms)
        if version == 0:
            if validate_only:
                raise UnsupportedVersionError(
                    "validate_only not supported on cluster version {}"
                        .format(self.config['api_version']))
            request = CreateTopicsRequest[version](
                create_topic_requests = [self._convert_new_topic_request(new_topic) for new_topic in new_topics],
                timeout = timeout_ms
            )
        elif version <= 2:
            validate_only = validate_only or False
            request = CreateTopicsRequest[version](
                create_topic_requests = [self._convert_new_topic_request(new_topic) for new_topic in new_topics],
                timeout = timeout_ms,
                validate_only = validate_only
            )
        else:
            raise UnsupportedVersionError(
                "missing implementation of CreateTopics for library supported version {}"
                    .format(version)
            )
        return self._send(request)

    def delete_topics(self, topics, timeout_ms=None):
        """Delete topics from the cluster

        :param topics: Array of topic name strings
        :param timeout_ms: Milliseconds to wait for topics to be deleted before broker returns
        :return: Appropriate version of DeleteTopicsResponse class
        """
        version = self._matching_api_version(DeleteTopicsRequest)
        timeout_ms = self._validate_timeout(timeout_ms)
        if version <= 1:
            request = DeleteTopicsRequest[version](
                topics = topics,
                timeout = timeout_ms
            )
        else:
            raise UnsupportedVersionError(
                "missing implementation of DeleteTopics for library supported version {}"
                    .format(version))
        return self._send(request)

    # list topics functionality is in ClusterMetadata

    # describe topics functionality is in ClusterMetadata

    # describe cluster functionality is in ClusterMetadata

    # describe_acls protocol not implemented

    # create_acls protocol not implemented

    # delete_acls protocol not implemented

    @staticmethod
    def _convert_describe_config_resource_request(config_resource):
        return (
            config_resource.resource_type,
            config_resource.name,
            [
                config_key for config_key, config_value in config_resource.configs.items()
            ] if config_resource.configs else None
        )

    def describe_configs(self, config_resources, include_synonyms=None):
        """Fetch configuration parameters for one or more kafka resources.

        :param config_resources: An array of ConfigResource objects.
            Any keys in ConfigResource.configs dict will be used to filter the result.  The configs dict should be None
            to get all values.  An empty dict will get zero values (as per kafka protocol).
        :param include_synonyms: If True, return synonyms in response.  Not supported by all versions.
        :return: Appropriate version of DescribeConfigsResponse class
        """
        version = self._matching_api_version(DescribeConfigsRequest)
        if version == 0:
            if include_synonyms:
                raise UnsupportedVersionError(
                    "include_synonyms not supported on cluster version {}"
                        .format(self.config['api_version']))
            request = DescribeConfigsRequest[version](
                resources = [self._convert_describe_config_resource_request(config_resource) for config_resource in config_resources]
            )
        elif version <= 1:
            include_synonyms = include_synonyms or False
            request = DescribeConfigsRequest[version](
                resources = [self._convert_describe_config_resource_request(config_resource) for config_resource in config_resources],
                include_synonyms = include_synonyms
            )
        else:
            raise UnsupportedVersionError(
                "missing implementation of DescribeConfigs for library supported version {}"
                    .format(version))
        return self._send(request)

    @staticmethod
    def _convert_alter_config_resource_request(config_resource):
        return (
            config_resource.resource_type,
            config_resource.name,
            [
                (config_key, config_value) for config_key, config_value in config_resource.configs.items()
            ]
        )

    def alter_configs(self, config_resources):
        """Alter configuration parameters of one or more kafka resources.

        :param config_resources: An array of ConfigResource objects.
        :return: Appropriate version of AlterConfigsResponse class
        """
        version = self._matching_api_version(AlterConfigsRequest)
        if version == 0:
            request = AlterConfigsRequest[version](
                resources = [self._convert_alter_config_resource_request(config_resource) for config_resource in config_resources]
            )
        else:
            raise UnsupportedVersionError(
                "missing implementation of AlterConfigs for library supported version {}"
                    .format(version))
        return self._send(request)

    # alter replica logs dir protocol not implemented

    # describe log dirs protocol not implemented

    @staticmethod
    def _convert_create_partitions_request(topic_name, new_partitions):
        return (
            topic_name,
            (
                new_partitions.total_count,
                new_partitions.new_assignments
            )
        )

    def create_partitions(self, topic_partitions, timeout_ms=None, validate_only=None):
        """Create additional partitions for an existing topic.

        :param topic_partitions: A map of topic name strings to NewPartition objects
        :param timeout_ms: Milliseconds to wait for new partitions to be created before broker returns
        :param validate_only: If True, don't actually create new partitions.
        :return: Appropriate version of CreatePartitionsResponse class
        """
        version = self._matching_api_version(CreatePartitionsRequest)
        timeout_ms = self._validate_timeout(timeout_ms)
        validate_only = validate_only or False
        if version == 0:
            request = CreatePartitionsRequest[version](
                topic_partitions = [self._convert_create_partitions_request(topic_name, new_partitions) for topic_name, new_partitions in topic_partitions.items()],
                timeout = timeout_ms,
                validate_only = validate_only
            )
        else:
            raise UnsupportedVersionError(
                "missing implementation of CreatePartitions for library supported version {}"
                    .format(version))
        return self._send(request)

    # delete records protocol not implemented

    # create delegation token protocol not implemented

    # renew delegation token protocol not implemented

    # expire delegation_token protocol not implemented

    # describe delegation_token protocol not implemented

    def describe_consumer_groups(self, group_ids):
        """Describe a set of consumer groups.

        :param group_ids: A list of consumer group id names
        :return: Appropriate version of DescribeGroupsResponse class
        """
        version = self._matching_api_version(DescribeGroupsRequest)
        if version <= 1:
            request = DescribeGroupsRequest[version](
                groups = group_ids
            )
        else:
            raise UnsupportedVersionError(
                "missing implementation of DescribeGroups for library supported version {}"
                    .format(version))
        return self._send(request)

    def list_consumer_groups(self):
        """List all consumer groups known to the cluster.

        :return: Appropriate version of ListGroupsResponse class
        """
        version = self._matching_api_version(ListGroupsRequest)
        if version <= 1:
            request = ListGroupsRequest[version]()
        else:
            raise UnsupportedVersionError(
                "missing implementation of ListGroups for library supported version {}"
                    .format(version))
        return self._send(request)

    # delete groups protocol not implemented
