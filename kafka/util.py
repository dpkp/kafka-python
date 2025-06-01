from __future__ import absolute_import, division

import binascii
import functools
import re
import time
import weakref

from kafka.errors import KafkaTimeoutError
from kafka.vendor import six


if six.PY3:
    MAX_INT = 2 ** 31
    TO_SIGNED = 2 ** 32

    def crc32(data):
        crc = binascii.crc32(data)
        # py2 and py3 behave a little differently
        # CRC is encoded as a signed int in kafka protocol
        # so we'll convert the py3 unsigned result to signed
        if crc >= MAX_INT:
            crc -= TO_SIGNED
        return crc
else:
    from binascii import crc32 # noqa: F401


class Timer:
    __slots__ = ('_start_at', '_expire_at', '_timeout_ms', '_error_message')

    def __init__(self, timeout_ms, error_message=None, start_at=None):
        self._timeout_ms = timeout_ms
        self._start_at = start_at or time.time()
        if timeout_ms is not None:
            self._expire_at = self._start_at + timeout_ms / 1000
        else:
            self._expire_at = float('inf')
        self._error_message = error_message

    @property
    def expired(self):
        return time.time() >= self._expire_at

    @property
    def timeout_ms(self):
        if self._timeout_ms is None:
            return None
        elif self._expire_at == float('inf'):
            return float('inf')
        remaining = self._expire_at - time.time()
        if remaining < 0:
            return 0
        else:
            return int(remaining * 1000)

    @property
    def elapsed_ms(self):
        return int(1000 * (time.time() - self._start_at))

    def maybe_raise(self):
        if self.expired:
            raise KafkaTimeoutError(self._error_message)

    def __str__(self):
        return "Timer(%s ms remaining)" % (self.timeout_ms)

# Taken from: https://github.com/apache/kafka/blob/39eb31feaeebfb184d98cc5d94da9148c2319d81/clients/src/main/java/org/apache/kafka/common/internals/Topic.java#L29
TOPIC_MAX_LENGTH = 249
TOPIC_LEGAL_CHARS = re.compile('^[a-zA-Z0-9._-]+$')

def ensure_valid_topic_name(topic):
    """ Ensures that the topic name is valid according to the kafka source. """

    # See Kafka Source:
    # https://github.com/apache/kafka/blob/39eb31feaeebfb184d98cc5d94da9148c2319d81/clients/src/main/java/org/apache/kafka/common/internals/Topic.java
    if topic is None:
        raise TypeError('All topics must not be None')
    if not isinstance(topic, six.string_types):
        raise TypeError('All topics must be strings')
    if len(topic) == 0:
        raise ValueError('All topics must be non-empty strings')
    if topic == '.' or topic == '..':
        raise ValueError('Topic name cannot be "." or ".."')
    if len(topic) > TOPIC_MAX_LENGTH:
        raise ValueError('Topic name is illegal, it can\'t be longer than {0} characters, topic: "{1}"'.format(TOPIC_MAX_LENGTH, topic))
    if not TOPIC_LEGAL_CHARS.match(topic):
        raise ValueError('Topic name "{0}" is illegal, it contains a character other than ASCII alphanumerics, ".", "_" and "-"'.format(topic))


class WeakMethod(object):
    """
    Callable that weakly references a method and the object it is bound to. It
    is based on https://stackoverflow.com/a/24287465.

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


class Dict(dict):
    """Utility class to support passing weakrefs to dicts

    See: https://docs.python.org/2/library/weakref.html
    """
    pass


def synchronized(func):
    def wrapper(self, *args, **kwargs):
        with self._lock:
            return func(self, *args, **kwargs)
    functools.update_wrapper(wrapper, func)
    return wrapper
