import binascii
import collections
import struct
import sys
from threading import Thread, Event

import six

from kafka.common import BufferUnderflowError


def crc32(data):
    return binascii.crc32(data) & 0xffffffff


def write_int_string(s):
    if s is not None and not isinstance(s, six.binary_type):
        raise TypeError('Expected "%s" to be bytes\n'
                        'data=%s' % (type(s), repr(s)))
    if s is None:
        return struct.pack('>i', -1)
    else:
        return struct.pack('>i%ds' % len(s), len(s), s)


def write_short_string(s):
    if s is not None and not isinstance(s, six.binary_type):
        raise TypeError('Expected "%s" to be bytes\n'
                        'data=%s' % (type(s), repr(s)))
    if s is None:
        return struct.pack('>h', -1)
    elif len(s) > 32767 and sys.version_info < (2, 7):
        # Python 2.6 issues a deprecation warning instead of a struct error
        raise struct.error(len(s))
    else:
        return struct.pack('>h%ds' % len(s), len(s), s)


def read_short_string(data, cur):
    if len(data) < cur + 2:
        raise BufferUnderflowError("Not enough data left")

    (strlen,) = struct.unpack('>h', data[cur:cur + 2])
    if strlen == -1:
        return None, cur + 2

    cur += 2
    if len(data) < cur + strlen:
        raise BufferUnderflowError("Not enough data left")

    out = data[cur:cur + strlen]
    return out, cur + strlen


def read_int_string(data, cur):
    if len(data) < cur + 4:
        raise BufferUnderflowError(
            "Not enough data left to read string len (%d < %d)" %
            (len(data), cur + 4))

    (strlen,) = struct.unpack('>i', data[cur:cur + 4])
    if strlen == -1:
        return None, cur + 4

    cur += 4
    if len(data) < cur + strlen:
        raise BufferUnderflowError("Not enough data left")

    out = data[cur:cur + strlen]
    return out, cur + strlen


def relative_unpack(fmt, data, cur):
    size = struct.calcsize(fmt)
    if len(data) < cur + size:
        raise BufferUnderflowError("Not enough data left")

    out = struct.unpack(fmt, data[cur:cur + size])
    return out, cur + size


def group_by_topic_and_partition(tuples):
    out = collections.defaultdict(dict)
    for t in tuples:
        out[t.topic][t.partition] = t
    return out


def kafka_bytestring(s):
    """
    Takes a string or bytes instance
    Returns bytes, encoding strings in utf-8 as necessary
    """
    if isinstance(s, six.binary_type):
        return s
    if isinstance(s, six.string_types):
        return s.encode('utf-8')
    raise TypeError(s)


class ReentrantTimer(object):
    """
    A timer that can be restarted, unlike threading.Timer
    (although this uses threading.Timer)

    t: timer interval in milliseconds
    fn: a callable to invoke
    args: tuple of args to be passed to function
    kwargs: keyword arguments to be passed to function
    """
    def __init__(self, t, fn, *args, **kwargs):

        if t <= 0:
            raise ValueError('Invalid timeout value')

        if not callable(fn):
            raise ValueError('fn must be callable')

        self.thread = None
        self.t = t / 1000.0
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.active = None

    def _timer(self, active):
        while not active.wait(self.t):
            self.fn(*self.args, **self.kwargs)

    def start(self):
        if self.thread is not None:
            self.stop()

        self.active = Event()
        self.thread = Thread(target=self._timer, args=(self.active,))
        self.thread.daemon = True  # So the app exits when main thread exits
        self.thread.start()

    def stop(self):
        if self.thread is None:
            return

        self.active.set()
        self.thread.join(self.t + 1)
        # noinspection PyAttributeOutsideInit
        self.timer = None
