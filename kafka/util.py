from __future__ import absolute_import

import atexit
import binascii
import collections
import struct
from threading import Thread, Event
import weakref

from kafka.vendor import six

from kafka.errors import BufferUnderflowError


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


class WeakMethod(object):
    """
    Callable that weakly references a method and the object it is bound to. It
    is based on http://stackoverflow.com/a/24287465.

    Arguments:

        object_dot_method: A bound instance method (i.e. 'object.method').
    """
    def __init__(self, object_dot_method):
        try:
            self.target = weakref.ref(object_dot_method.__self__)
        except AttributeError:
            self.target = weakref.ref(object_dot_method.im_self)
        self._target_id = id(self.target())
        try:
            self.method = weakref.ref(object_dot_method.__func__)
        except AttributeError:
            self.method = weakref.ref(object_dot_method.im_func)
        self._method_id = id(self.method())

    def __call__(self, *args, **kwargs):
        """
        Calls the method on target with args and kwargs.
        """
        return self.method()(self.target(), *args, **kwargs)

    def __hash__(self):
        return hash(self.target) ^ hash(self.method)

    def __eq__(self, other):
        if not isinstance(other, WeakMethod):
            return False
        return self._target_id == other._target_id and self._method_id == other._method_id


def try_method_on_system_exit(obj, method, *args, **kwargs):
    def wrapper(_obj, _meth, *args, **kwargs):
        try:
            getattr(_obj, _meth)(*args, **kwargs)
        except (ReferenceError, AttributeError):
            pass
    atexit.register(wrapper, weakref.proxy(obj), method, *args, **kwargs)
