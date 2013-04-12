import logging
import socket
import struct

log = logging.getLogger("kafka")

class KafkaConnection(object):
    """
    A socket connection to a single Kafka broker

    This class is _not_ thread safe. Each call to `send` must be followed
    by a call to `recv` in order to get the correct response. Eventually, 
    we can do something in here to facilitate multiplexed requests/responses
    since the Kafka API includes a correlation id.
    """
    def __init__(self, host, port, bufsize=4096):
        self.host = host
        self.port = port
        self.bufsize = bufsize
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.connect((host, port))
        self._sock.settimeout(10)

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
            raise Exception("Got no response from Kafka")
        (size,) = struct.unpack('>i', resp)

        messageSize = size - 4
        log.debug("About to read %d bytes from Kafka", messageSize)

        # Read the remainder of the response 
        total = 0
        while total < messageSize:
            resp = self._sock.recv(self.bufsize)
            log.debug("Read %d bytes from Kafka", len(resp))
            if resp == "":
                raise BufferUnderflowError("Not enough data to read this response")
            total += len(resp)
            yield resp

    ##################
    #   Public API   #
    ##################

    # TODO multiplex socket communication to allow for multi-threaded clients

    def send(self, requestId, payload):
        "Send a request to Kafka"
        log.debug("About to send %d bytes to Kafka, request %d" % (len(payload), requestId))
        sent = self._sock.sendall(payload)
        if sent != None:
            raise RuntimeError("Kafka went away")
        self.data = self._consume_response()

    def recv(self, requestId):
        "Get a response from Kafka"
        log.debug("Reading response %d from Kafka" % requestId)
        return self.data

    def close(self):
        "Close this connection"
        self._sock.close()
