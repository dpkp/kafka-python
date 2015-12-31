import copy
import logging
from random import shuffle
import socket
import ssl
import struct
from threading import local

import six

from kafka.common import ConnectionError


log = logging.getLogger(__name__)

DEFAULT_SOCKET_TIMEOUT_SECONDS = 120
DEFAULT_KAFKA_PORT = 9092


def collect_hosts(hosts, randomize=True):
    """
    Collects a comma-separated set of hosts (host:port) and optionally
    randomize the returned list.
    """

    if isinstance(hosts, six.string_types):
        hosts = hosts.strip().split(',')

    result = []
    for host_port in hosts:

        res = host_port.split(':')
        host = res[0]
        port = int(res[1]) if len(res) > 1 else DEFAULT_KAFKA_PORT
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

    Arguments:
        host: the host name or IP address of a kafka broker
        port: the port number the kafka broker is listening on
        timeout: default 120. The socket timeout for sending and receiving data
            in seconds. None means no timeout, so a request can block forever.
        certfile: the SSL certificate file of the client (requires Kafka 0.9+)
        keyfile: the SSL key file of the client (requires Kafka 0.9+)
        ca: The SSL CA (requires Kafka 0.9+)
        ssl: Use SSL when communicating with Kafka
        verify_hostname: Whether or not to verify the server's hostname against its cert
    """
    def __init__(self, host, port, timeout=DEFAULT_SOCKET_TIMEOUT_SECONDS,
                 ssl=False, ca=None, certfile=None, keyfile=None, verify_hostname=False):
        super(KafkaConnection, self).__init__()
        self.host = host
        self.port = port
        self.timeout = timeout
        self.ca = ca
        self.certfile = certfile
        self.keyfile = keyfile
        self.ssl = ssl
        self.verify_hostname = verify_hostname
        self._sock = None

        self.reinit()

    def __getnewargs__(self):
        return (self.host, self.port, self.timeout)

    def __repr__(self):
        return "<KafkaConnection host=%s port=%d>" % (self.host, self.port)

    ###################
    #   Private API   #
    ###################

    def _raise_connection_error(self):
        # Cleanup socket if we have one
        if self._sock:
            self.close()

        # And then raise
        raise ConnectionError("Kafka @ {0}:{1} went away".format(self.host, self.port))

    def _read_bytes(self, num_bytes):
        bytes_left = num_bytes
        responses = []

        log.debug("About to read %d bytes from Kafka", num_bytes)

        # Make sure we have a connection
        if not self._sock:
            self.reinit()

        while bytes_left:

            try:
                data = self._sock.recv(min(bytes_left, 4096))

                # Receiving empty string from recv signals
                # that the socket is in error.  we will never get
                # more data from this socket
                if data == b'':
                    raise socket.error("Not enough data to read message -- did server kill socket?")

            except (socket.error, ssl.SSLError):
                log.exception('Unable to receive data from Kafka')
                self._raise_connection_error()

            bytes_left -= len(data)
            log.debug("Read %d/%d bytes from Kafka", num_bytes - bytes_left, num_bytes)
            responses.append(data)

        return b''.join(responses)

    ##################
    #   Public API   #
    ##################

    # TODO multiplex socket communication to allow for multi-threaded clients

    def get_connected_socket(self):
        if not self._sock:
            self.reinit()
        return self._sock

    def send(self, request_id, payload):
        """
        Send a request to Kafka

        Arguments::
            request_id (int): can be any int (used only for debug logging...)
            payload: an encoded kafka packet (see KafkaProtocol)
        """

        log.debug("About to send %d bytes to Kafka, request %d" % (len(payload), request_id))

        # Make sure we have a connection
        if not self._sock:
            self.reinit()

        try:
            self._sock.sendall(payload)
        except (socket.error, ssl.SSLError):
            log.exception('Unable to send payload to Kafka')
            self._raise_connection_error()

    def recv(self, request_id):
        """
        Get a response packet from Kafka

        Arguments:
            request_id: can be any int (only used for debug logging...)

        Returns:
            str: Encoded kafka packet response from server
        """
        log.debug("Reading response %d from Kafka" % request_id)

        # Make sure we have a connection
        if not self._sock:
            self.reinit()

        # Read the size off of the header
        resp = self._read_bytes(4)
        (size,) = struct.unpack('>i', resp)

        # Read the remainder of the response
        resp = self._read_bytes(size)
        return resp

    def copy(self):
        """
        Create an inactive copy of the connection object, suitable for
        passing to a background thread.

        The returned copy is not connected; you must call reinit() before
        using.
        """
        c = copy.deepcopy(self)
        # Python 3 doesn't copy custom attributes of the threadlocal subclass
        c.host = copy.copy(self.host)
        c.port = copy.copy(self.port)
        c.timeout = copy.copy(self.timeout)
        c.ca = copy.copy(self.ca)
        c.certfile = copy.copy(self.certfile)
        c.keyfile = copy.copy(self.keyfile)
        c.ssl = copy.copy(self.ssl)
        c.verify_hostname = copy.copy(self.verify_hostname)
        c._sock = None
        return c

    def close(self):
        """
        Shutdown and close the connection socket
        """
        log.debug("Closing socket connection for %s:%d" % (self.host, self.port))
        if self._sock:
            # Call shutdown to be a good TCP client
            # But expect an error if the socket has already been
            # closed by the server
            try:
                self._sock.shutdown(socket.SHUT_RDWR)
            except (socket.error, ssl.SSLError):
                pass

            # Closing the socket should always succeed
            self._sock.close()
            self._sock = None
        else:
            log.debug("No socket found to close!")

    def reinit(self):
        """
        Re-initialize the socket connection
        close current socket (if open)
        and start a fresh connection
        raise ConnectionError on error
        """
        log.debug("Reinitializing socket connection for %s:%d" % (self.host, self.port))

        if self._sock:
            self.close()

        try:
            self._sock = socket.create_connection((self.host, self.port), self.timeout)
        except socket.error:
            log.exception('Unable to connect to Kafka broker at %s:%r', self.host, self.port)
            self._raise_connection_error()

        if self.ssl:
            # Disallow use of SSLv2 and V3 (meaning we require TLSv1.0+)
            context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)  # pylint: disable=no-member
            context.options |= ssl.OP_NO_SSLv2  # pylint: disable=no-member
            context.options |= ssl.OP_NO_SSLv3  # pylint: disable=no-member
            context.verify_mode = ssl.CERT_OPTIONAL
            if self.verify_hostname:
                context.check_hostname = True
            if self.ca:
                context.load_verify_locations(self.ca)
                context.verify_mode = ssl.CERT_REQUIRED
            if self.certfile and self.keyfile:
                context.load_cert_chain(certfile=self.certfile, keyfile=self.keyfile)
            try:
                self._sock = context.wrap_socket(self._sock, server_hostname=self.host)
            except ssl.SSLError:
                log.exception('Unable to connect to Kafka broker at %s:%r over SSL', self.host, self.port)
                self._raise_connection_error()
