from abc import ABC, abstractmethod


class SaslMechanism(ABC):
    @abstractmethod
    def __init__(self, **config):
        pass

    @abstractmethod
    def auth_bytes(self):
        pass

    @abstractmethod
    def receive(self, auth_bytes):
        pass

    @abstractmethod
    def is_done(self):
        pass

    @abstractmethod
    def is_authenticated(self):
        pass

    def auth_details(self):
        if not self.is_authenticated:
            raise RuntimeError('Not authenticated yet!')
        return 'Authenticated via SASL'
