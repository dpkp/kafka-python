from abc import ABC, abstractmethod


class AbstractMeasurable(ABC):
    """A measurable quantity that can be registered as a metric"""
    @abstractmethod
    def measure(self, config, now):
        """
        Measure this quantity and return the result

        Arguments:
            config (MetricConfig): The configuration for this metric
            now (int): The POSIX time in milliseconds the measurement
                is being taken

        Returns:
            The measured value
        """
        pass


class AnonMeasurable(AbstractMeasurable):
    def __init__(self, measure_fn):
        self._measure_fn = measure_fn

    def measure(self, config, now):
        return float(self._measure_fn(config, now))
