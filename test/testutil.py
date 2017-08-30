from __future__ import absolute_import

import os
import random
import string
import time


def random_string(length):
    return "".join(random.choice(string.ascii_letters) for i in range(length))


def env_kafka_version():
    """Return the Kafka version set in the OS environment as a tuple.

     Example: '0.8.1.1' --> (0, 8, 1, 1)
    """
    if 'KAFKA_VERSION' not in os.environ:
        return ()
    return tuple(map(int, os.environ['KAFKA_VERSION'].split('.')))


def assert_message_count(messages, num_messages):
    """Check that we received the expected number of messages with no duplicates."""
    # Make sure we got them all
    assert len(messages) == num_messages
    # Make sure there are no duplicates
    # Note: Currently duplicates are identified only using key/value. Other attributes like topic, partition, headers,
    # timestamp, etc are ignored... this could be changed if necessary, but will be more tolerant of dupes.
    unique_messages = {(m.key, m.value) for m in messages}
    assert len(unique_messages) == num_messages


class Timer(object):
    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.interval = self.end - self.start
