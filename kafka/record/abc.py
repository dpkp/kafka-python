import abc


class ABCRecord:
    __metaclass__ = abc.ABCMeta
    __slots__ = ()

    @abc.abstractmethod
    def offset(self):
        """ Absolute offset of record
        """

    @abc.abstractmethod
    def timestamp(self):
        """ Epoch milliseconds
        """

    @abc.abstractmethod
    def timestamp_type(self):
        """ CREATE_TIME(0) or APPEND_TIME(1)
        """

    @abc.abstractmethod
    def key(self):
        """ Bytes key or None
        """

    @abc.abstractmethod
    def value(self):
        """ Bytes value or None
        """

    @abc.abstractmethod
    def checksum(self):
        """ Prior to v2 format CRC was contained in every message. This will
            be the checksum for v0 and v1 and None for v2 and above.
        """

    @abc.abstractmethod
    def headers(self):
        """ If supported by version list of key-value tuples, or empty list if
            not supported by format.
        """


class ABCRecordBatchBuilder:
    __metaclass__ = abc.ABCMeta
    __slots__ = ()

    @abc.abstractmethod
    def append(self, offset, timestamp, key, value, headers=None):
        """ Writes record to internal buffer.

        Arguments:
            offset (int): Relative offset of record, starting from 0
            timestamp (int or None): Timestamp in milliseconds since beginning
                of the epoch (midnight Jan 1, 1970 (UTC)). If omitted, will be
                set to current time.
            key (bytes or None): Key of the record
            value (bytes or None): Value of the record
            headers (List[Tuple[str, bytes]]): Headers of the record. Header
                keys can not be ``None``.

        Returns:
            (bytes, int): Checksum of the written record (or None for v2 and
                above) and size of the written record.
        """

    @abc.abstractmethod
    def size_in_bytes(self, offset, timestamp, key, value, headers):
        """ Return the expected size change on buffer (uncompressed) if we add
            this message. This will account for varint size changes and give a
            reliable size.
        """

    @abc.abstractmethod
    def build(self):
        """ Close for append, compress if needed, write size and header and
            return a ready to send buffer object.

            Return:
                bytearray: finished batch, ready to send.
        """


class ABCRecordBatch:
    """ For v2 encapsulates a RecordBatch, for v0/v1 a single (maybe
        compressed) message.
    """
    __metaclass__ = abc.ABCMeta
    __slots__ = ()

    @abc.abstractmethod
    def __iter__(self):
        """ Return iterator over records (ABCRecord instances). Will decompress
            if needed.
        """


class ABCRecords:
    __metaclass__ = abc.ABCMeta
    __slots__ = ()

    @abc.abstractmethod
    def __init__(self, buffer):
        """ Initialize with bytes-like object conforming to the buffer
            interface (ie. bytes, bytearray, memoryview etc.).
        """

    @abc.abstractmethod
    def size_in_bytes(self):
        """ Returns the size of inner buffer.
        """

    @abc.abstractmethod
    def next_batch(self):
        """ Return next batch of records (ABCRecordBatch instances).
        """

    @abc.abstractmethod
    def has_next(self):
        """ True if there are more batches to read, False otherwise.
        """
