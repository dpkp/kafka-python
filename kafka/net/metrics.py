"""Metrics for kafka.net connection manager and connections.
"""
import logging

from kafka.metrics.measurable import AnonMeasurable
from kafka.metrics.stats import Avg, Count, Max, Rate
from kafka.metrics.stats.rate import TimeUnit

log = logging.getLogger(__name__)


class KafkaManagerMetrics:
    """Metrics for KafkaConnectionManager (equivalent to KafkaClientMetrics).
    Note that kafka.net does not track select_time or io_time.
    """
    def __init__(self, metrics, metric_group_prefix, conns):
        self.metrics = metrics
        metric_group_name = metric_group_prefix + '-metrics'

        self.connection_closed = metrics.sensor('connections-closed')
        self.connection_closed.add(metrics.metric_name(
            'connection-close-rate', metric_group_name,
            'Connections closed per second in the window.'), Rate())

        self.connection_created = metrics.sensor('connections-created')
        self.connection_created.add(metrics.metric_name(
            'connection-creation-rate', metric_group_name,
            'New connections established per second in the window.'), Rate())

        metrics.add_metric(metrics.metric_name(
            'connection-count', metric_group_name,
            'The current number of active connections.'), AnonMeasurable(
                lambda config, now: len(conns)))


class KafkaConnectionMetrics:
    """Metrics for a single KafkaConnection (equivalent to BrokerConnectionMetrics)."""
    def __init__(self, metrics, metric_group_prefix, node_id):
        self.metrics = metrics

        # Global aggregate sensors (created once, shared across all connections)
        if not metrics.get_sensor('bytes-sent-received'):
            metric_group_name = metric_group_prefix + '-metrics'

            bytes_transferred = metrics.sensor('bytes-sent-received')
            bytes_transferred.add(metrics.metric_name(
                'network-io-rate', metric_group_name,
                'The average number of network operations (reads or writes) on all'
                ' connections per second.'), Rate(sampled_stat=Count()))

            bytes_sent = metrics.sensor('bytes-sent',
                                        parents=[bytes_transferred])
            bytes_sent.add(metrics.metric_name(
                'outgoing-byte-rate', metric_group_name,
                'The average number of outgoing bytes sent per second to all'
                ' servers.'), Rate())
            bytes_sent.add(metrics.metric_name(
                'request-rate', metric_group_name,
                'The average number of requests sent per second.'),
                Rate(sampled_stat=Count()))
            bytes_sent.add(metrics.metric_name(
                'request-size-avg', metric_group_name,
                'The average size of all requests in the window.'), Avg())
            bytes_sent.add(metrics.metric_name(
                'request-size-max', metric_group_name,
                'The maximum size of any request sent in the window.'), Max())

            bytes_received = metrics.sensor('bytes-received',
                                            parents=[bytes_transferred])
            bytes_received.add(metrics.metric_name(
                'incoming-byte-rate', metric_group_name,
                'Bytes/second read off all sockets'), Rate())
            bytes_received.add(metrics.metric_name(
                'response-rate', metric_group_name,
                'Responses received sent per second.'),
                Rate(sampled_stat=Count()))

            request_latency = metrics.sensor('request-latency')
            request_latency.add(metrics.metric_name(
                'request-latency-avg', metric_group_name,
                'The average request latency in ms.'), Avg())
            request_latency.add(metrics.metric_name(
                'request-latency-max', metric_group_name,
                'The maximum request latency in ms.'), Max())

            throttle_time = metrics.sensor('throttle-time')
            throttle_time.add(metrics.metric_name(
                'throttle-time-avg', metric_group_name,
                'The average throttle time in ms.'), Avg())
            throttle_time.add(metrics.metric_name(
                'throttle-time-max', metric_group_name,
                'The maximum throttle time in ms.'), Max())

        # Per-node sensors (created per connection, parent to global sensors)
        if not metrics.get_sensor(f'node-{node_id}.bytes-sent'):
            metric_group_name = f'{metric_group_prefix}-node-metrics.node-{node_id}'

            bytes_sent = metrics.sensor(
                f'node-{node_id}.bytes-sent',
                parents=[metrics.get_sensor('bytes-sent')])
            bytes_sent.add(metrics.metric_name(
                'outgoing-byte-rate', metric_group_name,
                'The average number of outgoing bytes sent per second.'), Rate())
            bytes_sent.add(metrics.metric_name(
                'request-rate', metric_group_name,
                'The average number of requests sent per second.'),
                Rate(sampled_stat=Count()))
            bytes_sent.add(metrics.metric_name(
                'request-size-avg', metric_group_name,
                'The average size of all requests in the window.'), Avg())
            bytes_sent.add(metrics.metric_name(
                'request-size-max', metric_group_name,
                'The maximum size of any request sent in the window.'), Max())

            bytes_received = metrics.sensor(
                f'node-{node_id}.bytes-received',
                parents=[metrics.get_sensor('bytes-received')])
            bytes_received.add(metrics.metric_name(
                'incoming-byte-rate', metric_group_name,
                'Bytes/second read off node-connection socket'), Rate())
            bytes_received.add(metrics.metric_name(
                'response-rate', metric_group_name,
                'The average number of responses received per second.'),
                Rate(sampled_stat=Count()))

            request_time = metrics.sensor(
                f'node-{node_id}.latency',
                parents=[metrics.get_sensor('request-latency')])
            request_time.add(metrics.metric_name(
                'request-latency-avg', metric_group_name,
                'The average request latency in ms.'), Avg())
            request_time.add(metrics.metric_name(
                'request-latency-max', metric_group_name,
                'The maximum request latency in ms.'), Max())

            throttle_time = metrics.sensor(
                f'node-{node_id}.throttle',
                parents=[metrics.get_sensor('throttle-time')])
            throttle_time.add(metrics.metric_name(
                'throttle-time-avg', metric_group_name,
                'The average throttle time in ms.'), Avg())
            throttle_time.add(metrics.metric_name(
                'throttle-time-max', metric_group_name,
                'The maximum throttle time in ms.'), Max())

        self.bytes_sent = metrics.sensor(f'node-{node_id}.bytes-sent')
        self.bytes_received = metrics.sensor(f'node-{node_id}.bytes-received')
        self.request_time = metrics.sensor(f'node-{node_id}.latency')
        self.throttle_time = metrics.sensor(f'node-{node_id}.throttle')
