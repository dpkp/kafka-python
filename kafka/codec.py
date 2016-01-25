import gzip
from io import BytesIO
import struct

from six.moves import xrange

_XERIAL_V1_HEADER = (-126, b'S', b'N', b'A', b'P', b'P', b'Y', 0, 1, 1)
_XERIAL_V1_FORMAT = 'bccccccBii'

try:
    import snappy
except ImportError:
    snappy = None

try:
    import lz4
    from lz4 import compress as lz4_encode
    from lz4 import decompress as lz4_decode
except ImportError:
    lz4 = None
    lz4_encode = None
    lz4_decode = None


def has_gzip():
    return True


def has_snappy():
    return snappy is not None


def has_lz4():
    return lz4 is not None


def gzip_encode(payload, compresslevel=None):
    if not compresslevel:
        compresslevel = 9

    with BytesIO() as buf:

        # Gzip context manager introduced in python 2.6
        # so old-fashioned way until we decide to not support 2.6
        gzipper = gzip.GzipFile(fileobj=buf, mode="w", compresslevel=compresslevel)
        try:
            gzipper.write(payload)
        finally:
            gzipper.close()

        result = buf.getvalue()

    return result


def gzip_decode(payload):
    with BytesIO(payload) as buf:

        # Gzip context manager introduced in python 2.6
        # so old-fashioned way until we decide to not support 2.6
        gzipper = gzip.GzipFile(fileobj=buf, mode='r')
        try:
            result = gzipper.read()
        finally:
            gzipper.close()

    return result


def snappy_encode(payload, xerial_compatible=True, xerial_blocksize=32*1024):
    """Encodes the given data with snappy compression.

    If xerial_compatible is set then the stream is encoded in a fashion
    compatible with the xerial snappy library.

    The block size (xerial_blocksize) controls how frequent the blocking occurs
    32k is the default in the xerial library.

    The format winds up being:


        +-------------+------------+--------------+------------+--------------+
        |   Header    | Block1 len | Block1 data  | Blockn len | Blockn data  |
        +-------------+------------+--------------+------------+--------------+
        |  16 bytes   |  BE int32  | snappy bytes |  BE int32  | snappy bytes |
        +-------------+------------+--------------+------------+--------------+


    It is important to note that the blocksize is the amount of uncompressed
    data presented to snappy at each block, whereas the blocklen is the number
    of bytes that will be present in the stream; so the length will always be
    <= blocksize.

    """

    if not has_snappy():
        raise NotImplementedError("Snappy codec is not available")

    if not xerial_compatible:
        return snappy.compress(payload)

    out = BytesIO()
    for fmt, dat in zip(_XERIAL_V1_FORMAT, _XERIAL_V1_HEADER):
        out.write(struct.pack('!' + fmt, dat))

    # Chunk through buffers to avoid creating intermediate slice copies
    for chunk in (buffer(payload, i, xerial_blocksize)
                  for i in xrange(0, len(payload), xerial_blocksize)):

        block = snappy.compress(chunk)
        block_size = len(block)
        out.write(struct.pack('!i', block_size))
        out.write(block)

    return out.getvalue()


def _detect_xerial_stream(payload):
    """Detects if the data given might have been encoded with the blocking mode
        of the xerial snappy library.

        This mode writes a magic header of the format:
            +--------+--------------+------------+---------+--------+
            | Marker | Magic String | Null / Pad | Version | Compat |
            +--------+--------------+------------+---------+--------+
            |  byte  |   c-string   |    byte    |  int32  | int32  |
            +--------+--------------+------------+---------+--------+
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
        header = struct.unpack('!' + _XERIAL_V1_FORMAT, bytes(payload)[:16])
        return header == _XERIAL_V1_HEADER
    return False


def snappy_decode(payload):
    if not has_snappy():
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
