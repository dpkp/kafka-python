import gevent.socket as socket

from kafka.conn import KafkaConnection

class _KafkaConnection(KafkaConnection):
    """
    Gevent version of kafka.KafkaConnection class. Uses
    gevent.socket instead of socket.socket.
    """
    def __init__(self, host, port, timeout=10):
        super(_KafkaConnection, self).__init__(host, port, timeout)

    def reinit(self):
        """
        Re-initialize the socket connection
        """
        self.close()
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.connect((self.host, self.port))
        self._sock.settimeout(self.timeout)
        self._dirty = False
