from cStringIO import StringIO
import gzip

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
    buffer = StringIO()
    handle = gzip.GzipFile(fileobj=buffer, mode="w")
    handle.write(payload)
    handle.close()
    buffer.seek(0)
    result = buffer.read()
    buffer.close()
    return result


def gzip_decode(payload):
    buffer = StringIO(payload)
    handle = gzip.GzipFile(fileobj=buffer, mode='r')
    result = handle.read()
    handle.close()
    buffer.close()
    return result


def snappy_encode(payload):
    if not _has_snappy:
        raise NotImplementedError("Snappy codec is not available")
    return snappy.compress(payload)


def snappy_decode(payload):
    if not _has_snappy:
        raise NotImplementedError("Snappy codec is not available")
    return snappy.decompress(payload)
