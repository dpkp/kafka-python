import binascii
import weakref
from typing import Callable, Optional


MAX_INT = 2 ** 31
TO_SIGNED = 2 ** 32

def crc32(data: bytes) -> int:
    crc = binascii.crc32(data)
    # py2 and py3 behave a little differently
    # CRC is encoded as a signed int in kafka protocol
    # so we'll convert the py3 unsigned result to signed
    if crc >= MAX_INT:
        crc -= TO_SIGNED
    return crc


class WeakMethod:
    """
    Callable that weakly references a method and the object it is bound to. It
    is based on https://stackoverflow.com/a/24287465.

    Arguments:

        object_dot_method: A bound instance method (i.e. 'object.method').
    """
    def __init__(self, object_dot_method: Callable) -> None:
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

    def __call__(self, *args, **kwargs) -> Optional[bytes]:
        """
        Calls the method on target with args and kwargs.
        """
        return self.method()(self.target(), *args, **kwargs)

    def __hash__(self) -> int:
        return hash(self.target) ^ hash(self.method)

    def __eq__(self, other: "WeakMethod") -> bool:
        if not isinstance(other, WeakMethod):
            return False
        return self._target_id == other._target_id and self._method_id == other._method_id


class Dict(dict):
    """Utility class to support passing weakrefs to dicts

    See: https://docs.python.org/2/library/weakref.html
    """
    pass
