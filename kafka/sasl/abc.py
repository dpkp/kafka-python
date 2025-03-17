from __future__ import absolute_import

import abc

from kafka.vendor.six import add_metaclass


@add_metaclass(abc.ABCMeta)
class SaslMechanism(object):
    @abc.abstractmethod
    def __init__(self, **config):
        pass

    @abc.abstractmethod
    def auth_bytes(self):
        pass

    @abc.abstractmethod
    def receive(self, auth_bytes):
        pass

    @abc.abstractmethod
    def is_done(self):
        pass

    @abc.abstractmethod
    def is_authenticated(self):
        pass

    def auth_details(self):
        if not self.is_authenticated:
            raise RuntimeError('Not authenticated yet!')
        return 'Authenticated via SASL'
