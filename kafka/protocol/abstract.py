from __future__ import absolute_import

import abc

from kafka.vendor.six import add_metaclass


@add_metaclass(abc.ABCMeta)
class AbstractType(object):
    @abc.abstractmethod
    def encode(cls, value): # pylint: disable=no-self-argument
        pass

    @abc.abstractmethod
    def decode(cls, data): # pylint: disable=no-self-argument
        pass

    @classmethod
    def repr(cls, value):
        return repr(value)
