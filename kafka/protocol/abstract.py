import abc


class AbstractType(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def encode(cls, value):
        pass

    @abc.abstractmethod
    def decode(cls, data):
        pass
