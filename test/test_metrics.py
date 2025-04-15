import sys
import time

import pytest

from kafka.errors import QuotaViolationError
from kafka.metrics import DictReporter, MetricConfig, MetricName, Metrics, Quota
from kafka.metrics.measurable import AbstractMeasurable
from kafka.metrics.stats import (Avg, Count, Max, Min, Percentile, Percentiles,
                                 Rate, Total)
from kafka.metrics.stats.percentiles import BucketSizing
from kafka.metrics.stats.rate import TimeUnit

EPS = 0.000001


@pytest.fixture
def time_keeper():
    return TimeKeeper()


def test_MetricName():
    # The Java test only cover the differences between the deprecated
    # constructors, so I'm skipping them but doing some other basic testing.

    # In short, metrics should be equal IFF their name, group, and tags are
    # the same. Descriptions do not matter.
    name1 = MetricName('name', 'group', 'A metric.', {'a': 1, 'b': 2})
    name2 = MetricName('name', 'group', 'A description.', {'a': 1, 'b': 2})
    assert name1 == name2

    name1 = MetricName('name', 'group', tags={'a': 1, 'b': 2})
    name2 = MetricName('name', 'group', tags={'a': 1, 'b': 2})
    assert name1 == name2

    name1 = MetricName('foo', 'group')
    name2 = MetricName('name', 'group')
    assert name1 != name2

    name1 = MetricName('name', 'foo')
    name2 = MetricName('name', 'group')
    assert name1 != name2

    # name and group must be non-empty. Everything else is optional.
    with pytest.raises(Exception):
        MetricName('', 'group')
    with pytest.raises(Exception):
        MetricName('name', None)
    # tags must be a dict if supplied
    with pytest.raises(Exception):
        MetricName('name', 'group', tags=set())

    # Because of the implementation of __eq__ and __hash__, the values of
    # a MetricName cannot be mutable.
    tags = {'a': 1}
    name = MetricName('name', 'group', 'description', tags=tags)
    with pytest.raises(AttributeError):
        name.name = 'new name'
    with pytest.raises(AttributeError):
        name.group = 'new name'
    with pytest.raises(AttributeError):
        name.tags = {}
    # tags is a copy, so the instance isn't altered
    name.tags['b'] = 2
    assert name.tags == tags


def test_simple_stats(mocker, time_keeper, metrics):
    mocker.patch('time.time', side_effect=time_keeper.time)
    config = metrics._config

    measurable = ConstantMeasurable()

    metrics.add_metric(metrics.metric_name('direct.measurable', 'grp1',
                                            'The fraction of time an appender waits for space allocation.'),
                        measurable)
    sensor = metrics.sensor('test.sensor')
    sensor.add(metrics.metric_name('test.avg', 'grp1'), Avg())
    sensor.add(metrics.metric_name('test.max', 'grp1'), Max())
    sensor.add(metrics.metric_name('test.min', 'grp1'), Min())
    sensor.add(metrics.metric_name('test.rate', 'grp1'), Rate(TimeUnit.SECONDS))
    sensor.add(metrics.metric_name('test.occurences', 'grp1'),Rate(TimeUnit.SECONDS, Count()))
    sensor.add(metrics.metric_name('test.count', 'grp1'), Count())
    percentiles = [Percentile(metrics.metric_name('test.median', 'grp1'), 50.0),
                Percentile(metrics.metric_name('test.perc99_9', 'grp1'), 99.9)]
    sensor.add_compound(Percentiles(100, BucketSizing.CONSTANT, 100, -100,
                        percentiles=percentiles))

    sensor2 = metrics.sensor('test.sensor2')
    sensor2.add(metrics.metric_name('s2.total', 'grp1'), Total())
    sensor2.record(5.0)

    sum_val = 0
    count = 10
    for i in range(count):
        sensor.record(i)
        sum_val += i

    # prior to any time passing
    elapsed_secs = (config.time_window_ms * (config.samples - 1)) / 1000.0
    assert abs(count / elapsed_secs -
            metrics.metrics.get(metrics.metric_name('test.occurences', 'grp1')).value()) \
            < EPS, 'Occurrences(0...%d) = %f' % (count, count / elapsed_secs)

    # pretend 2 seconds passed...
    sleep_time_seconds = 2.0
    time_keeper.sleep(sleep_time_seconds)
    elapsed_secs += sleep_time_seconds

    assert abs(5.0 - metrics.metrics.get(metrics.metric_name('s2.total', 'grp1')).value()) \
            < EPS, 's2 reflects the constant value'
    assert abs(4.5 - metrics.metrics.get(metrics.metric_name('test.avg', 'grp1')).value()) \
            < EPS, 'Avg(0...9) = 4.5'
    assert abs((count - 1) - metrics.metrics.get(metrics.metric_name('test.max', 'grp1')).value()) \
            < EPS, 'Max(0...9) = 9'
    assert abs(0.0 - metrics.metrics.get(metrics.metric_name('test.min', 'grp1')).value()) \
            < EPS, 'Min(0...9) = 0'
    assert abs((sum_val / elapsed_secs) - metrics.metrics.get(metrics.metric_name('test.rate', 'grp1')).value()) \
            < EPS, 'Rate(0...9) = 1.40625'
    assert abs((count / elapsed_secs) - metrics.metrics.get(metrics.metric_name('test.occurences', 'grp1')).value()) \
            < EPS, 'Occurrences(0...%d) = %f' % (count, count / elapsed_secs)
    assert abs(count - metrics.metrics.get(metrics.metric_name('test.count', 'grp1')).value()) \
            < EPS, 'Count(0...9) = 10'


def test_hierarchical_sensors(metrics):
    parent1 = metrics.sensor('test.parent1')
    parent1.add(metrics.metric_name('test.parent1.count', 'grp1'), Count())
    parent2 = metrics.sensor('test.parent2')
    parent2.add(metrics.metric_name('test.parent2.count', 'grp1'), Count())
    child1 = metrics.sensor('test.child1', parents=[parent1, parent2])
    child1.add(metrics.metric_name('test.child1.count', 'grp1'), Count())
    child2 = metrics.sensor('test.child2', parents=[parent1])
    child2.add(metrics.metric_name('test.child2.count', 'grp1'), Count())
    grandchild = metrics.sensor('test.grandchild', parents=[child1])
    grandchild.add(metrics.metric_name('test.grandchild.count', 'grp1'), Count())

    # increment each sensor one time
    parent1.record()
    parent2.record()
    child1.record()
    child2.record()
    grandchild.record()

    p1 = parent1.metrics[0].value()
    p2 = parent2.metrics[0].value()
    c1 = child1.metrics[0].value()
    c2 = child2.metrics[0].value()
    gc = grandchild.metrics[0].value()

    # each metric should have a count equal to one + its children's count
    assert 1.0 == gc
    assert 1.0 + gc == c1
    assert 1.0 == c2
    assert 1.0 + c1 == p2
    assert 1.0 + c1 + c2 == p1
    assert [child1, child2] == metrics._children_sensors.get(parent1)
    assert [child1] == metrics._children_sensors.get(parent2)
    assert metrics._children_sensors.get(grandchild) is None


def test_bad_sensor_hierarchy(metrics):
    parent = metrics.sensor('parent')
    child1 = metrics.sensor('child1', parents=[parent])
    child2 = metrics.sensor('child2', parents=[parent])

    with pytest.raises(ValueError):
        metrics.sensor('gc', parents=[child1, child2])


def test_remove_sensor(metrics):
    size = len(metrics.metrics)
    parent1 = metrics.sensor('test.parent1')
    parent1.add(metrics.metric_name('test.parent1.count', 'grp1'), Count())
    parent2 = metrics.sensor('test.parent2')
    parent2.add(metrics.metric_name('test.parent2.count', 'grp1'), Count())
    child1 = metrics.sensor('test.child1', parents=[parent1, parent2])
    child1.add(metrics.metric_name('test.child1.count', 'grp1'), Count())
    child2 = metrics.sensor('test.child2', parents=[parent2])
    child2.add(metrics.metric_name('test.child2.count', 'grp1'), Count())
    grandchild1 = metrics.sensor('test.gchild2', parents=[child2])
    grandchild1.add(metrics.metric_name('test.gchild2.count', 'grp1'), Count())

    sensor = metrics.get_sensor('test.parent1')
    assert sensor is not None
    metrics.remove_sensor('test.parent1')
    assert metrics.get_sensor('test.parent1') is None
    assert metrics.metrics.get(metrics.metric_name('test.parent1.count', 'grp1')) is None
    assert metrics.get_sensor('test.child1') is None
    assert metrics._children_sensors.get(sensor) is None
    assert metrics.metrics.get(metrics.metric_name('test.child1.count', 'grp1')) is None

    sensor = metrics.get_sensor('test.gchild2')
    assert sensor is not None
    metrics.remove_sensor('test.gchild2')
    assert metrics.get_sensor('test.gchild2') is None
    assert metrics._children_sensors.get(sensor) is None
    assert metrics.metrics.get(metrics.metric_name('test.gchild2.count', 'grp1')) is None

    sensor = metrics.get_sensor('test.child2')
    assert sensor is not None
    metrics.remove_sensor('test.child2')
    assert metrics.get_sensor('test.child2') is None
    assert metrics._children_sensors.get(sensor) is None
    assert metrics.metrics.get(metrics.metric_name('test.child2.count', 'grp1')) is None

    sensor = metrics.get_sensor('test.parent2')
    assert sensor is not None
    metrics.remove_sensor('test.parent2')
    assert metrics.get_sensor('test.parent2') is None
    assert metrics._children_sensors.get(sensor) is None
    assert metrics.metrics.get(metrics.metric_name('test.parent2.count', 'grp1')) is None

    assert size == len(metrics.metrics)


def test_remove_inactive_metrics(mocker, time_keeper, metrics):
    mocker.patch('time.time', side_effect=time_keeper.time)

    s1 = metrics.sensor('test.s1', None, 1)
    s1.add(metrics.metric_name('test.s1.count', 'grp1'), Count())

    s2 = metrics.sensor('test.s2', None, 3)
    s2.add(metrics.metric_name('test.s2.count', 'grp1'), Count())

    purger = Metrics.ExpireSensorTask
    purger.run(metrics)
    assert metrics.get_sensor('test.s1') is not None, \
            'Sensor test.s1 must be present'
    assert metrics.metrics.get(metrics.metric_name('test.s1.count', 'grp1')) is not None, \
            'MetricName test.s1.count must be present'
    assert metrics.get_sensor('test.s2') is not None, \
            'Sensor test.s2 must be present'
    assert metrics.metrics.get(metrics.metric_name('test.s2.count', 'grp1')) is not None, \
            'MetricName test.s2.count must be present'

    time_keeper.sleep(1.001)
    purger.run(metrics)
    assert metrics.get_sensor('test.s1') is None, \
            'Sensor test.s1 should have been purged'
    assert metrics.metrics.get(metrics.metric_name('test.s1.count', 'grp1')) is None, \
            'MetricName test.s1.count should have been purged'
    assert metrics.get_sensor('test.s2') is not None, \
            'Sensor test.s2 must be present'
    assert metrics.metrics.get(metrics.metric_name('test.s2.count', 'grp1')) is not None, \
            'MetricName test.s2.count must be present'

    # record a value in sensor s2. This should reset the clock for that sensor.
    # It should not get purged at the 3 second mark after creation
    s2.record()

    time_keeper.sleep(2)
    purger.run(metrics)
    assert metrics.get_sensor('test.s2') is not None, \
            'Sensor test.s2 must be present'
    assert metrics.metrics.get(metrics.metric_name('test.s2.count', 'grp1')) is not None, \
            'MetricName test.s2.count must be present'

    # After another 1 second sleep, the metric should be purged
    time_keeper.sleep(1)
    purger.run(metrics)
    assert metrics.get_sensor('test.s1') is None, \
            'Sensor test.s2 should have been purged'
    assert metrics.metrics.get(metrics.metric_name('test.s1.count', 'grp1')) is None, \
            'MetricName test.s2.count should have been purged'

    # After purging, it should be possible to recreate a metric
    s1 = metrics.sensor('test.s1', None, 1)
    s1.add(metrics.metric_name('test.s1.count', 'grp1'), Count())
    assert metrics.get_sensor('test.s1') is not None, \
        'Sensor test.s1 must be present'
    assert metrics.metrics.get(metrics.metric_name('test.s1.count', 'grp1')) is not None, \
            'MetricName test.s1.count must be present'


def test_remove_metric(metrics):
    size = len(metrics.metrics)
    metrics.add_metric(metrics.metric_name('test1', 'grp1'), Count())
    metrics.add_metric(metrics.metric_name('test2', 'grp1'), Count())

    assert metrics.remove_metric(metrics.metric_name('test1', 'grp1')) is not None
    assert metrics.metrics.get(metrics.metric_name('test1', 'grp1')) is None
    assert metrics.metrics.get(metrics.metric_name('test2', 'grp1')) is not None

    assert metrics.remove_metric(metrics.metric_name('test2', 'grp1')) is not None
    assert metrics.metrics.get(metrics.metric_name('test2', 'grp1')) is None

    assert size == len(metrics.metrics)


def test_event_windowing(mocker, time_keeper):
    mocker.patch('time.time', side_effect=time_keeper.time)

    count = Count()
    config = MetricConfig(event_window=1, samples=2)
    count.record(config, 1.0, time_keeper.ms())
    count.record(config, 1.0, time_keeper.ms())
    assert 2.0 == count.measure(config, time_keeper.ms())
    count.record(config, 1.0, time_keeper.ms())  # first event times out
    assert 2.0 == count.measure(config, time_keeper.ms())


def test_time_windowing(mocker, time_keeper):
    mocker.patch('time.time', side_effect=time_keeper.time)

    count = Count()
    config = MetricConfig(time_window_ms=1, samples=2)
    count.record(config, 1.0, time_keeper.ms())
    time_keeper.sleep(.001)
    count.record(config, 1.0, time_keeper.ms())
    assert 2.0 == count.measure(config, time_keeper.ms())
    time_keeper.sleep(.001)
    count.record(config, 1.0, time_keeper.ms())  # oldest event times out
    assert 2.0 == count.measure(config, time_keeper.ms())


def test_old_data_has_no_effect(mocker, time_keeper):
    mocker.patch('time.time', side_effect=time_keeper.time)

    max_stat = Max()
    min_stat = Min()
    avg_stat = Avg()
    count_stat = Count()
    window_ms = 100
    samples = 2
    config = MetricConfig(time_window_ms=window_ms, samples=samples)
    max_stat.record(config, 50, time_keeper.ms())
    min_stat.record(config, 50, time_keeper.ms())
    avg_stat.record(config, 50, time_keeper.ms())
    count_stat.record(config, 50, time_keeper.ms())

    time_keeper.sleep(samples * window_ms / 1000.0)
    assert float('-inf') == max_stat.measure(config, time_keeper.ms())
    assert float(sys.maxsize) == min_stat.measure(config, time_keeper.ms())
    assert 0.0 == avg_stat.measure(config, time_keeper.ms())
    assert 0 == count_stat.measure(config, time_keeper.ms())


def test_duplicate_MetricName(metrics):
    metrics.sensor('test').add(metrics.metric_name('test', 'grp1'), Avg())
    with pytest.raises(ValueError):
        metrics.sensor('test2').add(metrics.metric_name('test', 'grp1'), Total())


def test_Quotas(metrics):
    sensor = metrics.sensor('test')
    sensor.add(metrics.metric_name('test1.total', 'grp1'), Total(),
               MetricConfig(quota=Quota.upper_bound(5.0)))
    sensor.add(metrics.metric_name('test2.total', 'grp1'), Total(),
               MetricConfig(quota=Quota.lower_bound(0.0)))
    sensor.record(5.0)
    with pytest.raises(QuotaViolationError):
        sensor.record(1.0)

    assert abs(6.0 - metrics.metrics.get(metrics.metric_name('test1.total', 'grp1')).value()) \
            < EPS

    sensor.record(-6.0)
    with pytest.raises(QuotaViolationError):
        sensor.record(-1.0)


def test_Quotas_equality():
    quota1 = Quota.upper_bound(10.5)
    quota2 = Quota.lower_bound(10.5)
    assert quota1 != quota2, 'Quota with different upper values should not be equal'

    quota3 = Quota.lower_bound(10.5)
    assert quota2 == quota3, 'Quota with same upper and bound values should be equal'


def test_Percentiles(metrics):
    buckets = 100
    _percentiles = [
        Percentile(metrics.metric_name('test.p25', 'grp1'), 25),
        Percentile(metrics.metric_name('test.p50', 'grp1'), 50),
        Percentile(metrics.metric_name('test.p75', 'grp1'), 75),
    ]
    percs = Percentiles(4 * buckets, BucketSizing.CONSTANT, 100.0, 0.0,
                        percentiles=_percentiles)
    config = MetricConfig(event_window=50, samples=2)
    sensor = metrics.sensor('test', config)
    sensor.add_compound(percs)
    p25 = metrics.metrics.get(metrics.metric_name('test.p25', 'grp1'))
    p50 = metrics.metrics.get(metrics.metric_name('test.p50', 'grp1'))
    p75 = metrics.metrics.get(metrics.metric_name('test.p75', 'grp1'))

    # record two windows worth of sequential values
    for i in range(buckets):
        sensor.record(i)

    assert abs(p25.value() - 25) < 1.0
    assert abs(p50.value() - 50) < 1.0
    assert abs(p75.value() - 75) < 1.0

    for i in range(buckets):
        sensor.record(0.0)

    assert p25.value() < 1.0
    assert p50.value() < 1.0
    assert p75.value() < 1.0

def test_rate_windowing(mocker, time_keeper, metrics):
    mocker.patch('time.time', side_effect=time_keeper.time)

    # Use the default time window. Set 3 samples
    config = MetricConfig(samples=3)
    sensor = metrics.sensor('test.sensor', config)
    sensor.add(metrics.metric_name('test.rate', 'grp1'), Rate(TimeUnit.SECONDS))

    sum_val = 0
    count = config.samples - 1
    # Advance 1 window after every record
    for i in range(count):
        sensor.record(100)
        sum_val += 100
        time_keeper.sleep(config.time_window_ms / 1000.0)

    # Sleep for half the window.
    time_keeper.sleep(config.time_window_ms / 2.0 / 1000.0)

    # prior to any time passing
    elapsed_secs = (config.time_window_ms * (config.samples - 1) + config.time_window_ms / 2.0) / 1000.0

    kafka_metric = metrics.metrics.get(metrics.metric_name('test.rate', 'grp1'))
    assert abs((sum_val / elapsed_secs) - kafka_metric.value()) < EPS, \
            'Rate(0...2) = 2.666'
    assert abs(elapsed_secs - (kafka_metric.measurable.window_size(config, time.time() * 1000) / 1000.0)) \
            < EPS, 'Elapsed Time = 75 seconds'


def test_reporter(metrics):
    reporter = DictReporter()
    foo_reporter = DictReporter(prefix='foo')
    metrics.add_reporter(reporter)
    metrics.add_reporter(foo_reporter)
    sensor = metrics.sensor('kafka.requests')
    sensor.add(metrics.metric_name('pack.bean1.avg', 'grp1'), Avg())
    sensor.add(metrics.metric_name('pack.bean2.total', 'grp2'), Total())
    sensor2 = metrics.sensor('kafka.blah')
    sensor2.add(metrics.metric_name('pack.bean1.some', 'grp1'), Total())
    sensor2.add(metrics.metric_name('pack.bean2.some', 'grp1',
                                    tags={'a': 42, 'b': 'bar'}), Total())

    # kafka-metrics-count > count is the total number of metrics and automatic
    expected = {
        'kafka-metrics-count': {'count': 5.0},
        'grp2': {'pack.bean2.total': 0.0},
        'grp1': {'pack.bean1.avg': 0.0, 'pack.bean1.some': 0.0},
        'grp1.a=42,b=bar': {'pack.bean2.some': 0.0},
    }
    assert expected == reporter.snapshot()

    for key in list(expected.keys()):
        metrics = expected.pop(key)
        expected['foo.%s' % (key,)] = metrics
    assert expected == foo_reporter.snapshot()


class ConstantMeasurable(AbstractMeasurable):
    _value = 0.0

    def measure(self, config, now):
        return self._value


class TimeKeeper(object):
    """
    A clock that you can manually advance by calling sleep
    """
    def __init__(self, auto_tick_ms=0):
        self._millis = time.time() * 1000
        self._auto_tick_ms = auto_tick_ms

    def time(self):
        return self.ms() / 1000.0

    def ms(self):
        self.sleep(self._auto_tick_ms)
        return self._millis

    def sleep(self, seconds):
        self._millis += (seconds * 1000)
