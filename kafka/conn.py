import copy
import logging
import socket
import struct
from random import shuffle
from threading import local

from kafka.common import BufferUnderflowError
from kafka.common import ConnectionError

log = logging.getLogger("kafka")


def collect_hosts(hosts, randomize=True):
    """
    Collects a comma-separated set of hosts (host:port) and optionnaly
    randomize the returned list.
    """

    result = []
    for host_port in hosts.split(","):

        res = host_port.split(':')
        host = res[0]
        port = int(res[1]) if len(res) > 1 else 9092
        result.append((host.strip(), port))

    if randomize:
        shuffle(result)

    return result


class KafkaConnection(local):
    """
    A socket connection to a single Kafka broker

    This class is _not_ thread safe. Each call to `send` must be followed
    by a call to `recv` in order to get the correct response. Eventually,
    we can do something in here to facilitate multiplexed requests/responses
    since the Kafka API includes a correlation id.
    """
    def __init__(self, host, port, bufsize=4096, timeout=10):
        super(KafkaConnection, self).__init__()
        self.host = host
        self.port = port
        self.bufsize = bufsize
        self.timeout = timeout

        self._sock = socket.create_connection((host, port), timeout=timeout)
        self._dirty = False

    def __str__(self):
        return "<KafkaConnection host=%s port=%d>" % (self.host, self.port)

    ###################
    #   Private API   #
    ###################

    def _consume_response(self):
        """
        Fully consumer the response iterator
        """
        data = ""
        for chunk in self._consume_response_iter():
            data += chunk
        return data

    def _consume_response_iter(self):
        """
        This method handles the response header and error messages. It
        then returns an iterator for the chunks of the response
        """
        log.debug("Handling response from Kafka")

        # Read the size off of the header
        resp = self._sock.recv(4)
        if resp == "":
            self._raise_connection_error()
        (size,) = struct.unpack('>i', resp)

        messagesize = size - 4
        log.debug("About to read %d bytes from Kafka", messagesize)

        # Read the remainder of the response
        total = 0
        while total < messagesize:
            resp = self._sock.recv(self.bufsize)
            log.debug("Read %d bytes from Kafka", len(resp))
            if resp == "":
                raise BufferUnderflowError(
                    "Not enough data to read this response")

            total += len(resp)
            yield resp

    def _raise_connection_error(self):
        self._dirty = True
        raise ConnectionError("Kafka @ {}:{} went away".format(self.host, self.port))

    ##################
    #   Public API   #
    ##################

    # TODO multiplex socket communication to allow for multi-threaded clients

    def send(self, request_id, payload):
        "Send a request to Kafka"
        log.debug("About to send %d bytes to Kafka, request %d" % (len(payload), request_id))
        try:
            if self._dirty:
                self.reinit()
            sent = self._sock.sendall(payload)
            if sent is not None:
                self._raise_connection_error()
        except socket.error:
            log.exception('Unable to send payload to Kafka')
            self._raise_connection_error()

    def recv(self, request_id):
        """
        Get a response from Kafka
        """
        log.debug("Reading response %d from Kafka" % request_id)
        self.data = self._consume_response()
        return self.data

    def copy(self):
        """
        Create an inactive copy of the connection object
        A reinit() has to be done on the copy before it can be used again
        """
        c = copy.deepcopy(self)
        c._sock = None
        return c

    def close(self):
        """
        Close this connection
        """
        if self._sock:
            self._sock.close()

    def reinit(self):
        """
        Re-initialize the socket connection
        """
        self.close()
        self._sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        self._dirty = False
