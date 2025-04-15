from __future__ import absolute_import

import gc
import platform
import threading

import pytest

from kafka import KafkaProducer

@pytest.mark.skipif(platform.python_implementation() != 'CPython',
                    reason='Test relies on CPython-specific gc policies')
def test_kafka_producer_gc_cleanup():
    gc.collect()
    threads = threading.active_count()
    producer = KafkaProducer(api_version=(2, 1)) # set api_version explicitly to avoid auto-detection
    assert threading.active_count() == threads + 1
    del(producer)
    gc.collect()
    assert threading.active_count() == threads



