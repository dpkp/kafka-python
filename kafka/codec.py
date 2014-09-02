from io import BytesIO
import gzip
import struct

import six
from six.moves import xrange

_XERIAL_V1_HEADER = (-126, b'S', b'N', b'A', b'P', b'P', b'Y', 0, 1, 1)
_XERIAL_V1_FORMAT = 'bccccccBii'

try:
    import snappy
    _has_snappy = True
except ImportError:
    _has_snappy = False


def has_gzip():
    return True


def has_snappy():
    return _has_snappy


def gzip_encode(payload):
    buffer = BytesIO()
    handle = gzip.GzipFile(fileobj=buffer, mode="w")
    handle.write(payload)
    handle.close()
    buffer.seek(0)
    result = buffer.read()
    buffer.close()
    return result


def gzip_decode(payload):
    buffer = BytesIO(payload)
    handle = gzip.GzipFile(fileobj=buffer, mode='r')
    result = handle.read()
    handle.close()
    buffer.close()
    return result


def snappy_encode(payload, xerial_compatible=False, xerial_blocksize=32 * 1024):
    """Encodes the given data with snappy if xerial_compatible is set then the
       stream is encoded in a fashion compatible with the xerial snappy library

       The block size (xerial_blocksize) controls how frequent the blocking occurs
       32k is the default in the xerial library.

       The format winds up being
        +-------------+------------+--------------+------------+--------------+
        |   Header    | Block1 len | Block1 data  | Blockn len | Blockn data  |
        |-------------+------------+--------------+------------+--------------|
        |  16 bytes   |  BE int32  | snappy bytes |  BE int32  | snappy bytes |
        +-------------+------------+--------------+------------+--------------+

        It is important to not that the blocksize is the amount of uncompressed
        data presented to snappy at each block, whereas the blocklen is the
        number of bytes that will be present in the stream, that is the
        length will always be <= blocksize.
    """

    if not _has_snappy:
        raise NotImplementedError("Snappy codec is not available")

    if xerial_compatible:
        def _chunker():
            for i in xrange(0, len(payload), xerial_blocksize):
                yield payload[i:i+xerial_blocksize]

        out = BytesIO()

        header = b''.join([struct.pack('!' + fmt, dat) for fmt, dat
            in zip(_XERIAL_V1_FORMAT, _XERIAL_V1_HEADER)])

        out.write(header)
        for chunk in _chunker():
            block = snappy.compress(chunk)
            block_size = len(block)
            out.write(struct.pack('!i', block_size))
            out.write(block)

        out.seek(0)
        return out.read()

    else:
        return snappy.compress(payload)


def _detect_xerial_stream(payload):
    """Detects if the data given might have been encoded with the blocking mode
        of the xerial snappy library.

        This mode writes a magic header of the format:
            +--------+--------------+------------+---------+--------+
            | Marker | Magic String | Null / Pad | Version | Compat |
            |--------+--------------+------------+---------+--------|
            |  byte  |   c-string   |    byte    |  int32  | int32  |
            |--------+--------------+------------+---------+--------|
            |  -126  |   'SNAPPY'   |     \0     |         |        |
            +--------+--------------+------------+---------+--------+

        The pad appears to be to ensure that SNAPPY is a valid cstring
        The version is the version of this format as written by xerial,
        in the wild this is currently 1 as such we only support v1.

        Compat is there to claim the miniumum supported version that
        can read a xerial block stream, presently in the wild this is
        1.
    """

    if len(payload) > 16:
        header = header = struct.unpack('!' + _XERIAL_V1_FORMAT, bytes(payload)[:16])
        return header == _XERIAL_V1_HEADER
    return False


def snappy_decode(payload):
    if not _has_snappy:
        raise NotImplementedError("Snappy codec is not available")

    if _detect_xerial_stream(payload):
        # TODO ? Should become a fileobj ?
        out = BytesIO()
        byt = payload[16:]
        length = len(byt)
        cursor = 0

        while cursor < length:
            block_size = struct.unpack_from('!i', byt[cursor:])[0]
            # Skip the block size
            cursor += 4
            end = cursor + block_size
            out.write(snappy.decompress(byt[cursor:end]))
            cursor = end

        out.seek(0)
        return out.read()
    else:
        return snappy.decompress(payload)
