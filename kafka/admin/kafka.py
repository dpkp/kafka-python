from __future__ import absolute_import

from collections import defaultdict
import copy
import logging
import socket

from kafka.vendor import six

from kafka.client_async import KafkaClient, selectors
import kafka.errors as Errors
from kafka.errors import (
    IncompatibleBrokerVersion, KafkaConfigurationError, NotControllerError,
    UnrecognizedBrokerVersion)
from kafka.metrics import MetricConfig, Metrics
from kafka.protocol.admin import (
    CreateTopicsRequest, DeleteTopicsRequest, DescribeConfigsRequest, AlterConfigsRequest, CreatePartitionsRequest,
    ListGroupsRequest, DescribeGroupsRequest)
from kafka.protocol.commit import GroupCoordinatorRequest, OffsetFetchRequest
from kafka.protocol.metadata import MetadataRequest
from kafka.structs import TopicPartition, OffsetAndMetadata
from kafka.version import __version__


log = logging.getLogger(__name__)


class KafkaAdminClient(object):
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
        'metric_reporters': [],
        'metrics_num_samples': 2,
        'metrics_sample_window_ms': 30000,
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

        self._client = KafkaClient(metrics=self._metrics,
                                   metric_group_prefix='admin',
                                   **self.config)

        # Get auto-discovered version from client if necessary
        if self.config['api_version'] is None:
            self.config['api_version'] = self._client.config['api_version']

        self._closed = False
        self._refresh_controller_id()
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

    def _matching_api_version(self, operation):
        """Find the latest version of the protocol operation supported by both
        this library and the broker.

        This resolves to the lesser of either the latest api version this
        library supports, or the max version supported by the broker.

        :param operation: A list of protocol operation versions from kafka.protocol.
        :return: The max matching version number between client and broker.
        """
        version = min(len(operation) - 1,
                      self._client.get_api_versions()[operation[0].API_KEY][1])
        if version < self._client.get_api_versions()[operation[0].API_KEY][0]:
            # max library version is less than min broker version. Currently,
            # no Kafka versions specify a min msg version. Maybe in the future?
            raise IncompatibleBrokerVersion(
                "No version of the '{}' Kafka protocol is supported by both the client and broker."
                .format(operation.__name__))
        return version

    def _validate_timeout(self, timeout_ms):
        """Validate the timeout is set or use the configuration default.

        :param timeout_ms: The timeout provided by api call, in milliseconds.
        :return: The timeout to use for the operation.
        """
        return timeout_ms or self.config['request_timeout_ms']

    def _refresh_controller_id(self):
        """Determine the Kafka cluster controller."""
        version = self._matching_api_version(MetadataRequest)
        if 1 <= version <= 6:
            request = MetadataRequest[version]()
            response = self._send_request_to_node(self._client.least_loaded_node(), request)
            controller_id = response.controller_id
            # verify the controller is new enough to support our requests
            controller_version = self._client.check_version(controller_id)
            if controller_version < (0, 10, 0):
                raise IncompatibleBrokerVersion(
                    "The controller appears to be running Kafka {}. KafkaAdminClient requires brokers >= 0.10.0.0."
                    .format(controller_version))
            self._controller_id = controller_id
        else:
            raise UnrecognizedBrokerVersion(
                "Kafka Admin interface cannot determine the controller using MetadataRequest_v{}."
                .format(version))

    def _find_group_coordinator_id(self, group_id):
        """Find the broker node_id of the coordinator of the given group.

        Sends a FindCoordinatorRequest message to the cluster. Will block until
        the FindCoordinatorResponse is received. Any errors are immediately
        raised.

        :param group_id: The consumer group ID. This is typically the group
            name as a string.
        :return: The node_id of the broker that is the coordinator.
        """
        # Note: Java may change how this is implemented in KAFKA-6791.
        #
        # TODO add support for dynamically picking version of
        # GroupCoordinatorRequest which was renamed to FindCoordinatorRequest.
        # When I experimented with this, GroupCoordinatorResponse_v1 didn't
        # match GroupCoordinatorResponse_v0 and I couldn't figure out why.
        gc_request = GroupCoordinatorRequest[0](group_id)
        gc_response = self._send_request_to_node(self._client.least_loaded_node(), gc_request)
        # use the extra error checking in add_group_coordinator() rather than
        # immediately returning the group coordinator.
        success = self._client.cluster.add_group_coordinator(group_id, gc_response)
        if not success:
            error_type = Errors.for_code(gc_response.error_code)
            assert error_type is not Errors.NoError
            # Note: When error_type.retriable, Java will retry... see
            # KafkaAdminClient's handleFindCoordinatorError method
            raise error_type(
                "Could not identify group coordinator for group_id '{}' from response '{}'."
                .format(group_id, gc_response))
        group_coordinator = self._client.cluster.coordinator_for_group(group_id)
        # will be None if the coordinator was never populated, which should never happen here
        assert group_coordinator is not None
        # will be -1 if add_group_coordinator() failed... but by this point the
        # error should have been raised.
        assert group_coordinator != -1
        return group_coordinator

    def _send_request_to_node(self, node_id, request):
        """Send a Kafka protocol message to a specific broker.

        Will block until the message result is received.

        :param node_id: The broker id to which to send the message.
        :param request: The message to send.
        :return: The Kafka protocol response for the message.
        :exception: The exception if the message could not be sent.
        """
        while not self._client.ready(node_id):
            # poll until the connection to broker is ready, otherwise send()
            # will fail with NodeNotReadyError
            self._client.poll()
        future = self._client.send(node_id, request)
        self._client.poll(future=future)
        if future.succeeded():
            return future.value
        else:
            raise future.exception  # pylint: disable-msg=raising-bad-type

    def _send_request_to_controller(self, request):
        """Send a Kafka protocol message to the cluster controller.

        Will block until the message result is received.

        :param request: The message to send.
        :return: The Kafka protocol response for the message.
        """
        tries = 2  # in case our cached self._controller_id is outdated
        while tries:
            tries -= 1
            response = self._send_request_to_node(self._controller_id, request)
            # DeleteTopicsResponse returns topic_error_codes rather than topic_errors
            for topic, error_code in getattr(response, "topic_errors", response.topic_error_codes):
                error_type = Errors.for_code(error_code)
                if tries and isinstance(error_type, NotControllerError):
                    # No need to inspect the rest of the errors for
                    # non-retriable errors because NotControllerError should
                    # either be thrown for all errors or no errors.
                    self._refresh_controller_id()
                    break
                elif error_type is not Errors.NoError:
                    raise error_type(
                        "Request '{}' failed with response '{}'."
                        .format(request, response))
            else:
                return response
        raise RuntimeError("This should never happen, please file a bug with full stacktrace if encountered")

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

    def create_topics(self, new_topics, timeout_ms=None, validate_only=False):
        """Create new topics in the cluster.

        :param new_topics: A list of NewTopic objects.
        :param timeout_ms: Milliseconds to wait for new topics to be created
            before the broker returns.
        :param validate_only: If True, don't actually create new topics.
            Not supported by all versions. Default: False
        :return: Appropriate version of CreateTopicResponse class.
        """
        version = self._matching_api_version(CreateTopicsRequest)
        timeout_ms = self._validate_timeout(timeout_ms)
        if version == 0:
            if validate_only:
                raise IncompatibleBrokerVersion(
                    "validate_only requires CreateTopicsRequest >= v1, which is not supported by Kafka {}."
                    .format(self.config['api_version']))
            request = CreateTopicsRequest[version](
                create_topic_requests=[self._convert_new_topic_request(new_topic) for new_topic in new_topics],
                timeout=timeout_ms
            )
        elif version <= 2:
            request = CreateTopicsRequest[version](
                create_topic_requests=[self._convert_new_topic_request(new_topic) for new_topic in new_topics],
                timeout=timeout_ms,
                validate_only=validate_only
            )
        else:
            raise NotImplementedError(
                "Support for CreateTopics v{} has not yet been added to KafkaAdminClient."
                .format(version))
        # TODO convert structs to a more pythonic interface
        # TODO raise exceptions if errors
        return self._send_request_to_controller(request)

    def delete_topics(self, topics, timeout_ms=None):
        """Delete topics from the cluster.

        :param topics: A list of topic name strings.
        :param timeout_ms: Milliseconds to wait for topics to be deleted
            before the broker returns.
        :return: Appropriate version of DeleteTopicsResponse class.
        """
        version = self._matching_api_version(DeleteTopicsRequest)
        timeout_ms = self._validate_timeout(timeout_ms)
        if version <= 1:
            request = DeleteTopicsRequest[version](
                topics=topics,
                timeout=timeout_ms
            )
            response = self._send_request_to_controller(request)
        else:
            raise NotImplementedError(
                "Support for DeleteTopics v{} has not yet been added to KafkaAdminClient."
                .format(version))
        return response

    # list topics functionality is in ClusterMetadata
    # Note: if implemented here, send the request to the least_loaded_node()

    # describe topics functionality is in ClusterMetadata
    # Note: if implemented here, send the request to the controller

    # describe cluster functionality is in ClusterMetadata
    # Note: if implemented here, send the request to the least_loaded_node()

    # describe_acls protocol not yet implemented
    # Note: send the request to the least_loaded_node()

    # create_acls protocol not yet implemented
    # Note: send the request to the least_loaded_node()

    # delete_acls protocol not yet implemented
    # Note: send the request to the least_loaded_node()

    @staticmethod
    def _convert_describe_config_resource_request(config_resource):
        return (
            config_resource.resource_type,
            config_resource.name,
            [
                config_key for config_key, config_value in config_resource.configs.items()
            ] if config_resource.configs else None
        )

    def describe_configs(self, config_resources, include_synonyms=False):
        """Fetch configuration parameters for one or more Kafka resources.

        :param config_resources: An list of ConfigResource objects.
            Any keys in ConfigResource.configs dict will be used to filter the
            result. Setting the configs dict to None will get all values. An
            empty dict will get zero values (as per Kafka protocol).
        :param include_synonyms: If True, return synonyms in response. Not
            supported by all versions. Default: False.
        :return: Appropriate version of DescribeConfigsResponse class.
        """
        version = self._matching_api_version(DescribeConfigsRequest)
        if version == 0:
            if include_synonyms:
                raise IncompatibleBrokerVersion(
                    "include_synonyms requires DescribeConfigsRequest >= v1, which is not supported by Kafka {}."
                    .format(self.config['api_version']))
            request = DescribeConfigsRequest[version](
                resources=[self._convert_describe_config_resource_request(config_resource) for config_resource in config_resources]
            )
        elif version == 1:
            request = DescribeConfigsRequest[version](
                resources=[self._convert_describe_config_resource_request(config_resource) for config_resource in config_resources],
                include_synonyms=include_synonyms
            )
        else:
            raise NotImplementedError(
                "Support for DescribeConfigs v{} has not yet been added to KafkaAdminClient."
                .format(version))
        return self._send_request_to_node(self._client.least_loaded_node(), request)

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
        """Alter configuration parameters of one or more Kafka resources.

        Warning:
            This is currently broken for BROKER resources because those must be
            sent to that specific broker, versus this always picks the
            least-loaded node. See the comment in the source code for details.
            We would happily accept a PR fixing this.

        :param config_resources: A list of ConfigResource objects.
        :return: Appropriate version of AlterConfigsResponse class.
        """
        version = self._matching_api_version(AlterConfigsRequest)
        if version == 0:
            request = AlterConfigsRequest[version](
                resources=[self._convert_alter_config_resource_request(config_resource) for config_resource in config_resources]
            )
        else:
            raise NotImplementedError(
                "Support for AlterConfigs v{} has not yet been added to KafkaAdminClient."
                .format(version))
        # TODO the Java client has the note:
        # // We must make a separate AlterConfigs request for every BROKER resource we want to alter
        # // and send the request to that specific broker. Other resources are grouped together into
        # // a single request that may be sent to any broker.
        #
        # So this is currently broken as it always sends to the least_loaded_node()
        return self._send_request_to_node(self._client.least_loaded_node(), request)

    # alter replica logs dir protocol not yet implemented
    # Note: have to lookup the broker with the replica assignment and send the request to that broker

    # describe log dirs protocol not yet implemented
    # Note: have to lookup the broker with the replica assignment and send the request to that broker

    @staticmethod
    def _convert_create_partitions_request(topic_name, new_partitions):
        return (
            topic_name,
            (
                new_partitions.total_count,
                new_partitions.new_assignments
            )
        )

    def create_partitions(self, topic_partitions, timeout_ms=None, validate_only=False):
        """Create additional partitions for an existing topic.

        :param topic_partitions: A map of topic name strings to NewPartition objects.
        :param timeout_ms: Milliseconds to wait for new partitions to be
            created before the broker returns.
        :param validate_only: If True, don't actually create new partitions.
            Default: False
        :return: Appropriate version of CreatePartitionsResponse class.
        """
        version = self._matching_api_version(CreatePartitionsRequest)
        timeout_ms = self._validate_timeout(timeout_ms)
        if version == 0:
            request = CreatePartitionsRequest[version](
                topic_partitions=[self._convert_create_partitions_request(topic_name, new_partitions) for topic_name, new_partitions in topic_partitions.items()],
                timeout=timeout_ms,
                validate_only=validate_only
            )
        else:
            raise NotImplementedError(
                "Support for CreatePartitions v{} has not yet been added to KafkaAdminClient."
                .format(version))
        return self._send_request_to_controller(request)

    # delete records protocol not yet implemented
    # Note: send the request to the partition leaders

    # create delegation token protocol not yet implemented
    # Note: send the request to the least_loaded_node()

    # renew delegation token protocol not yet implemented
    # Note: send the request to the least_loaded_node()

    # expire delegation_token protocol not yet implemented
    # Note: send the request to the least_loaded_node()

    # describe delegation_token protocol not yet implemented
    # Note: send the request to the least_loaded_node()

    def describe_consumer_groups(self, group_ids, group_coordinator_id=None):
        """Describe a set of consumer groups.

        Any errors are immediately raised.

        :param group_ids: A list of consumer group IDs. These are typically the
            group names as strings.
        :param group_coordinator_id: The node_id of the groups' coordinator
            broker. If set to None, it will query the cluster for each group to
            find that group's coordinator. Explicitly specifying this can be
            useful for avoiding extra network round trips if you already know
            the group coordinator. This is only useful when all the group_ids
            have the same coordinator, otherwise it will error. Default: None.
        :return: A list of group descriptions. For now the group descriptions
            are the raw results from the DescribeGroupsResponse. Long-term, we
            plan to change this to return namedtuples as well as decoding the
            partition assignments.
        """
        group_descriptions = []
        version = self._matching_api_version(DescribeGroupsRequest)
        for group_id in group_ids:
            if group_coordinator_id is not None:
                this_groups_coordinator_id = group_coordinator_id
            else:
                this_groups_coordinator_id = self._find_group_coordinator_id(group_id)
            if version <= 1:
                # Note: KAFKA-6788 A potential optimization is to group the
                # request per coordinator and send one request with a list of
                # all consumer groups. Java still hasn't implemented this
                # because the error checking is hard to get right when some
                # groups error and others don't.
                request = DescribeGroupsRequest[version](groups=(group_id,))
                response = self._send_request_to_node(this_groups_coordinator_id, request)
                assert len(response.groups) == 1
                # TODO need to implement converting the response tuple into
                # a more accessible interface like a namedtuple and then stop
                # hardcoding tuple indices here. Several Java examples,
                # including KafkaAdminClient.java
                group_description = response.groups[0]
                error_code = group_description[0]
                error_type = Errors.for_code(error_code)
                # Java has the note: KAFKA-6789, we can retry based on the error code
                if error_type is not Errors.NoError:
                    raise error_type(
                        "Request '{}' failed with response '{}'."
                        .format(request, response))
                # TODO Java checks the group protocol type, and if consumer
                # (ConsumerProtocol.PROTOCOL_TYPE) or empty string, it decodes
                # the members' partition assignments... that hasn't yet been
                # implemented here so just return the raw struct results
                group_descriptions.append(group_description)
            else:
                raise NotImplementedError(
                    "Support for DescribeGroups v{} has not yet been added to KafkaAdminClient."
                    .format(version))
        return group_descriptions

    def list_consumer_groups(self, broker_ids=None):
        """List all consumer groups known to the cluster.

        This returns a list of Consumer Group tuples. The tuples are
        composed of the consumer group name and the consumer group protocol
        type.

        Only consumer groups that store their offsets in Kafka are returned.
        The protocol type will be an empty string for groups created using
        Kafka < 0.9 APIs because, although they store their offsets in Kafka,
        they don't use Kafka for group coordination. For groups created using
        Kafka >= 0.9, the protocol type will typically be "consumer".

        As soon as any error is encountered, it is immediately raised.

        :param broker_ids: A list of broker node_ids to query for consumer
            groups. If set to None, will query all brokers in the cluster.
            Explicitly specifying broker(s) can be useful for determining which
            consumer groups are coordinated by those broker(s). Default: None
        :return list: List of tuples of Consumer Groups.
        :exception GroupCoordinatorNotAvailableError: The coordinator is not
            available, so cannot process requests.
        :exception GroupLoadInProgressError: The coordinator is loading and
            hence can't process requests.
        """
        # While we return a list, internally use a set to prevent duplicates
        # because if a group coordinator fails after being queried, and its
        # consumer groups move to new brokers that haven't yet been queried,
        # then the same group could be returned by multiple brokers.
        consumer_groups = set()
        if broker_ids is None:
            broker_ids = [broker.nodeId for broker in self._client.cluster.brokers()]
        version = self._matching_api_version(ListGroupsRequest)
        if version <= 2:
            request = ListGroupsRequest[version]()
            for broker_id in broker_ids:
                response = self._send_request_to_node(broker_id, request)
                error_type = Errors.for_code(response.error_code)
                if error_type is not Errors.NoError:
                    raise error_type(
                        "Request '{}' failed with response '{}'."
                        .format(request, response))
                consumer_groups.update(response.groups)
        else:
            raise NotImplementedError(
                "Support for ListGroups v{} has not yet been added to KafkaAdminClient."
                .format(version))
        return list(consumer_groups)

    def list_consumer_group_offsets(self, group_id, group_coordinator_id=None,
                                    partitions=None):
        """Fetch Consumer Group Offsets.

        Note:
        This does not verify that the group_id or partitions actually exist
        in the cluster.

        As soon as any error is encountered, it is immediately raised.

        :param group_id: The consumer group id name for which to fetch offsets.
        :param group_coordinator_id: The node_id of the group's coordinator
            broker. If set to None, will query the cluster to find the group
            coordinator. Explicitly specifying this can be useful to prevent
            that extra network round trip if you already know the group
            coordinator. Default: None.
        :param partitions: A list of TopicPartitions for which to fetch
            offsets. On brokers >= 0.10.2, this can be set to None to fetch all
            known offsets for the consumer group. Default: None.
        :return dictionary: A dictionary with TopicPartition keys and
            OffsetAndMetada values. Partitions that are not specified and for
            which the group_id does not have a recorded offset are omitted. An
            offset value of `-1` indicates the group_id has no offset for that
            TopicPartition. A `-1` can only happen for partitions that are
            explicitly specified.
        """
        group_offsets_listing = {}
        if group_coordinator_id is None:
            group_coordinator_id = self._find_group_coordinator_id(group_id)
        version = self._matching_api_version(OffsetFetchRequest)
        if version <= 3:
            if partitions is None:
                if version <= 1:
                    raise ValueError(
                        """OffsetFetchRequest_v{} requires specifying the
                        partitions for which to fetch offsets. Omitting the
                        partitions is only supported on brokers >= 0.10.2.
                        For details, see KIP-88.""".format(version))
                topics_partitions = None
            else:
                # transform from [TopicPartition("t1", 1), TopicPartition("t1", 2)] to [("t1", [1, 2])]
                topics_partitions_dict = defaultdict(set)
                for topic, partition in partitions:
                    topics_partitions_dict[topic].add(partition)
                topics_partitions = list(six.iteritems(topics_partitions_dict))
            request = OffsetFetchRequest[version](group_id, topics_partitions)
            response = self._send_request_to_node(group_coordinator_id, request)
            if version > 1:  # OffsetFetchResponse_v1 lacks a top-level error_code
                error_type = Errors.for_code(response.error_code)
                if error_type is not Errors.NoError:
                    # optionally we could retry if error_type.retriable
                    raise error_type(
                        "Request '{}' failed with response '{}'."
                        .format(request, response))
            # transform response into a dictionary with TopicPartition keys and
            # OffsetAndMetada values--this is what the Java AdminClient returns
            for topic, partitions in response.topics:
                for partition, offset, metadata, error_code in partitions:
                    error_type = Errors.for_code(error_code)
                    if error_type is not Errors.NoError:
                        raise error_type(
                            "Unable to fetch offsets for group_id {}, topic {}, partition {}"
                            .format(group_id, topic, partition))
                    group_offsets_listing[TopicPartition(topic, partition)] = OffsetAndMetadata(offset, metadata)
        else:
            raise NotImplementedError(
                "Support for OffsetFetch v{} has not yet been added to KafkaAdminClient."
                .format(version))
        return group_offsets_listing

    # delete groups protocol not yet implemented
    # Note: send the request to the group's coordinator.
