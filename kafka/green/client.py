from kafka.client import KafkaClient, DEFAULT_SOCKET_TIMEOUT_SECONDS

from .conn import _KafkaConnection

class _KafkaClient(KafkaClient):

    def __init__(self, hosts, client_id=KafkaClient.CLIENT_ID,
                 timeout=DEFAULT_SOCKET_TIMEOUT_SECONDS):
        super(_KafkaClient, self).__init__(hosts=hosts, client_id=client_id, timeout=timeout)

    def copy(self):
        # have to override this since copy.deepcopy cannot serialize
        # a gevent.socket
        return _KafkaClient(self.hosts, self.client_id, self.timeout)

    def create_connection(self, host, port):
        return _KafkaConnection(host, port, timeout=self.timeout)
