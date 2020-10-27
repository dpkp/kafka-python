from __future__ import absolute_import

import binascii
import json
import os
import re
import weakref

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
    from binascii import crc32


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


def parse_flexible_versions(root_dir):
    """Reads all the json files in a given folder and outputs a dictionary
    mapping api keys to minimum flexible version. To be used on the resources/common/message
    folder in the kafka java repository"""
    valid_version_re = re.compile(r"(\d+)\+?")
    all_files = os.listdir(root_dir)
    ret = {}
    for f in all_files:
        full_path = os.path.join(root_dir, f)
        if not os.path.isfile(full_path):
            continue
        if not full_path.endswith(".json"):
            continue
        with open(full_path) as data_file:
            data = data_file.read()
        data = re.sub(r'\\\n', '', data)
        data = re.sub(r'//.*\n', '\n', data)
        data = json.loads(data)
        if "apiKey" not in data or "flexibleVersions" not in data:
            continue
        api_key = data["apiKey"]
        flexible_versions = data["flexibleVersions"]
        matches = valid_version_re.findall(flexible_versions)
        if matches:
            ret[int(api_key)] = int(matches[0])
    return ret
