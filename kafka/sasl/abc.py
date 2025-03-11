from __future__ import absolute_import

import abc


class SaslMechanism(object):
    __metaclass__ = abc.ABCMeta

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
