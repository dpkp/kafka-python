from abc import ABC, abstractmethod


class AbstractType(ABC):
    @classmethod
    @abstractmethod
    def encode(cls, value): # pylint: disable=no-self-argument
        pass

    @classmethod
    @abstractmethod
    def decode(cls, data): # pylint: disable=no-self-argument
        pass

    @classmethod
    def repr(cls, value):
        return repr(value)
