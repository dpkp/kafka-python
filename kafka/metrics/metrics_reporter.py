from __future__ import absolute_import

import abc


class AbstractMetricsReporter(object):
    """
    An abstract class to allow things to listen as new metrics
    are created so they can be reported.
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def init(self, metrics):
        """
        This is called when the reporter is first registered
        to initially register all existing metrics

        Arguments:
            metrics (list of KafkaMetric): All currently existing metrics
        """
        raise NotImplementedError

    @abc.abstractmethod
    def metric_change(self, metric):
        """
        This is called whenever a metric is updated or added

        Arguments:
            metric (KafkaMetric)
        """
        raise NotImplementedError

    @abc.abstractmethod
    def metric_removal(self, metric):
        """
        This is called whenever a metric is removed

        Arguments:
            metric (KafkaMetric)
        """
        raise NotImplementedError

    @abc.abstractmethod
    def configure(self, configs):
        """
        Configure this class with the given key-value pairs

        Arguments:
            configs (dict of {str, ?})
        """
        raise NotImplementedError

    @abc.abstractmethod
    def close(self):
        """Called when the metrics repository is closed."""
        raise NotImplementedError

    @abc.abstractmethod
    def get_emitter(self, metric):
        """
        Called to return an instance of an emitter

        Arguments:
            metric (str): the name of the metric
        """

    @abc.abstractmethod
    def record(self, emitter, value, timestamp):
        """
        Called to record and emit metrics

        Arguments:
            emitter: reference to an emitter
            value(float): value to be emitted
            timestamp: the time the value was recorded at
        """
