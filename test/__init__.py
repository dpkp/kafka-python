from __future__ import absolute_import

import sys

if sys.version_info < (2, 7):
    import unittest2 as unittest  # pylint: disable=import-error
else:
    import unittest

# Set default logging handler to avoid "No handler found" warnings.
import logging
try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

logging.getLogger(__name__).addHandler(NullHandler())

from kafka.future import Future
Future.error_on_callbacks = True  # always fail during testing
