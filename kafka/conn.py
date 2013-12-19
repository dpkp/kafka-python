import copy
import logging
import socket
import struct
from threading import local

from kafka.common import BufferUnderflowError
from kafka.common import ConnectionError

log = logging.getLogger("kafka")


class KafkaConnection(local):
    """
    A socket connection to a single Kafka broker

    This class is _not_ thread safe. Each call to `send` must be followed
    by a call to `recv` in order to get the correct response. Eventually,
    we can do something in here to facilitate multiplexed requests/responses
    since the Kafka API includes a correlation id.
    """
    def __init__(self, host, port, bufsize=4098, timeout=10):
        super(KafkaConnection, self).__init__()
        self.host = host
        self.port = port
        self.bufsize = bufsize
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.connect((host, port))
        self._sock.settimeout(timeout)
        self._dirty = False

    def __str__(self):
        return "<KafkaConnection host=%s port=%d>" % (self.host, self.port)

    ###################
    #   Private API   #
    ###################

    def _consume_response(self):
        """
        Fully consume the response iterator
        """
        return "".join(self._consume_response_iter())

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

        log.debug("About to read %d bytes from Kafka", size)

        # Read the remainder of the response
        total = 0
        while total < size:
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
        if self._dirty:
            self._raise_connection_error()

        try:
            return self._consume_response()
        except socket.error:
            log.exception('Unable to read response from Kafka')
            self._raise_connection_error()

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
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.connect((self.host, self.port))
        self._sock.settimeout(10)
        self._dirty = False
