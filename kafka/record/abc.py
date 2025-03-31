from __future__ import absolute_import

import abc

from kafka.vendor.six import add_metaclass


@add_metaclass(abc.ABCMeta)
class ABCRecord(object):
    __slots__ = ()

    @abc.abstractproperty
    def size_in_bytes(self):
        """ Number of total bytes in record
        """

    @abc.abstractproperty
    def offset(self):
        """ Absolute offset of record
        """

    @abc.abstractproperty
    def timestamp(self):
        """ Epoch milliseconds
        """

    @abc.abstractproperty
    def timestamp_type(self):
        """ CREATE_TIME(0) or APPEND_TIME(1)
        """

    @abc.abstractproperty
    def key(self):
        """ Bytes key or None
        """

    @abc.abstractproperty
    def value(self):
        """ Bytes value or None
        """

    @abc.abstractproperty
    def checksum(self):
        """ Prior to v2 format CRC was contained in every message. This will
            be the checksum for v0 and v1 and None for v2 and above.
        """

    @abc.abstractmethod
    def validate_crc(self):
        """ Return True if v0/v1 record matches checksum. noop/True for v2 records
        """

    @abc.abstractproperty
    def headers(self):
        """ If supported by version list of key-value tuples, or empty list if
            not supported by format.
        """


@add_metaclass(abc.ABCMeta)
class ABCRecordBatchBuilder(object):
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


@add_metaclass(abc.ABCMeta)
class ABCRecordBatch(object):
    """ For v2 encapsulates a RecordBatch, for v0/v1 a single (maybe
        compressed) message.
    """
    __slots__ = ()

    @abc.abstractmethod
    def __iter__(self):
        """ Return iterator over records (ABCRecord instances). Will decompress
            if needed.
        """

    @abc.abstractproperty
    def base_offset(self):
        """ Return base offset for batch
        """

    @abc.abstractproperty
    def size_in_bytes(self):
        """ Return size of batch in bytes (includes header overhead)
        """

    @abc.abstractproperty
    def magic(self):
        """ Return magic value (0, 1, 2) for batch.
        """


@add_metaclass(abc.ABCMeta)
class ABCRecords(object):
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
