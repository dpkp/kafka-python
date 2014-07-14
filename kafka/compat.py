import sys
import string
import codecs


is_py3 = sys.version_info[0] == 3


if is_py3:
    from io import BytesIO as StringIO
    from queue import Empty, Queue
    from urllib.parse import urlparse
    from itertools import zip_longest as izip_longest

    basestring = str
    xrange = range
    letters = string.ascii_letters
    long = int
    buffer = memoryview

    def iter_next(iterable):
        return next(iterable)

    def dict_items(d):
        return d.items()

    def bytes(x):
        return codecs.latin_1_encode(x)[0]

    def str(x):
        return codecs.unicode_escape_decode(x)[0]

else:
    from cStringIO import StringIO
    from Queue import Empty, Queue
    from urlparse import urlparse
    from itertools import izip_longest

    basestring = basestring
    xrange = xrange
    letters = string.letters
    long = long
    buffer = buffer

    def iter_next(iterable):
        return iterable.next()

    def dict_items(d):
        return d.iteritems()

    def bytes(x):
        return x

    def str(x):
        return x

