from __future__ import absolute_import

import abc

from kafka.metrics.measurable import AbstractMeasurable
from kafka.metrics.stat import AbstractStat
from kafka.vendor.six import add_metaclass


@add_metaclass(abc.ABCMeta)
class AbstractMeasurableStat(AbstractStat, AbstractMeasurable):
    """
    An AbstractMeasurableStat is an AbstractStat that is also
    an AbstractMeasurable (i.e. can produce a single floating point value).
    This is the interface used for most of the simple statistics such
    as Avg, Max, Count, etc.
    """
