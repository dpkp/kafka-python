import abc


class AbstractType:
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def encode(cls, value): # pylint: disable=no-self-argument
        pass

    @abc.abstractmethod
    def decode(cls, data): # pylint: disable=no-self-argument
        pass

    @classmethod
    def repr(cls, value):
        return repr(value)
