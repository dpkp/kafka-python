from cStringIO import StringIO
import gzip
import logging

log = logging.getLogger("kafka.codec")

def gzip_encode(payload):
    buf = StringIO()
    f = gzip.GzipFile(fileobj=buf, mode='w', compresslevel=6)
    f.write(payload)
    f.close()
    buf.seek(0)
    out = buf.read()
    buf.close()
    return out

def gzip_decode(payload):
    buf = StringIO(payload)
    f = gzip.GzipFile(fileobj=buf, mode='r')
    out = f.read()
    f.close()
    buf.close()
    return out
