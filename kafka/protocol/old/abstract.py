import abc


class AbstractType(object, metaclass=abc.ABCMeta):
    @classmethod
    @abc.abstractmethod
    def encode(cls, value): # pylint: disable=no-self-argument
        pass

    @classmethod
    @abc.abstractmethod
    def decode(cls, data): # pylint: disable=no-self-argument
        pass

    @classmethod
    def repr(cls, value):
        return repr(value)
