from __future__ import absolute_import

# Set default logging handler to avoid "No handler found" warnings.
import logging
logging.basicConfig(level=logging.INFO)

from kafka.future import Future
Future.error_on_callbacks = True  # always fail during testing
