import binascii
import collections
import struct
import sys
from threading import Thread, Event

import six

from kafka.common import BufferUnderflowError


def crc32(data):
    crc = binascii.crc32(data)
    # py2 and py3 behave a little differently
    # CRC is encoded as a signed int in kafka protocol
    # so we'll convert the py3 unsigned result to signed
    if six.PY3 and crc >= 2**31:
        crc -= 2**32
    return crc


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
        assert t.topic not in out or t.partition not in out[t.topic], \
               'Duplicate {0}s for {1} {2}'.format(t.__class__.__name__,
                                                   t.topic, t.partition)
        out[t.topic][t.partition] = t
    return out


class ReentrantTimer(object):
    """
    A timer that can be restarted, unlike threading.Timer
    (although this uses threading.Timer)

    Arguments:

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
        # python2.6 Event.wait() always returns None
        # python2.7 and greater returns the flag value (true/false)
        # we want the flag value, so add an 'or' here for python2.6
        # this is redundant for later python versions (FLAG OR FLAG == FLAG)
        while not (active.wait(self.t) or active.is_set()):
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
        self.fn = None

    def __del__(self):
        self.stop()

class EventRegistrar(object):
    """
    Handles registration of callable event handlers which are
    executed in sequence when events are emitted.
    """
    def __init__(self):
        self.handlers = collections.defaultdict(list)

    def register(self, event, handler):
        self.handlers[event].append(handler)

    def emit(self, event, *args, **kwargs):
        for handler in self.handlers[event]:
            handler(*args, **kwargs)
