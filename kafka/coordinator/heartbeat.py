from __future__ import absolute_import, division

import copy
import logging
import time

from kafka.errors import KafkaConfigurationError

log = logging.getLogger(__name__)


class Heartbeat(object):
    DEFAULT_CONFIG = {
        'group_id': None,
        'heartbeat_interval_ms': 3000,
        'session_timeout_ms': 10000,
        'max_poll_interval_ms': 300000,
        'retry_backoff_ms': 100,
    }

    def __init__(self, **configs):
        self.config = copy.copy(self.DEFAULT_CONFIG)
        for key in self.config:
            if key in configs:
                self.config[key] = configs[key]

        if self.config['group_id'] is not None:
            if self.config['heartbeat_interval_ms'] >= self.config['session_timeout_ms']:
                raise KafkaConfigurationError('Heartbeat interval must be lower than the session timeout (%s v %s)' % (
                    self.config['heartbeat_interval_ms'], self.config['session_timeout_ms']))
            if self.config['heartbeat_interval_ms'] > (self.config['session_timeout_ms'] / 3):
                log.warning('heartbeat_interval_ms is high relative to session_timeout_ms (%s v %s).'
                            ' Recommend heartbeat interval less than 1/3rd of session timeout',
                            self.config['heartbeat_interval_ms'], self.config['session_timeout_ms'])

        self.last_send = -1 * float('inf')
        self.last_receive = -1 * float('inf')
        self.last_poll = -1 * float('inf')
        self.last_reset = time.time()
        self.heartbeat_failed = None

    def poll(self):
        self.last_poll = time.time()

    def sent_heartbeat(self):
        self.last_send = time.time()
        self.heartbeat_failed = False

    def fail_heartbeat(self):
        self.heartbeat_failed = True

    def received_heartbeat(self):
        self.last_receive = time.time()

    def time_to_next_heartbeat(self):
        """Returns seconds (float) remaining before next heartbeat should be sent"""
        time_since_last_heartbeat = time.time() - max(self.last_send, self.last_reset)
        if self.heartbeat_failed:
            delay_to_next_heartbeat = self.config['retry_backoff_ms'] / 1000
        else:
            delay_to_next_heartbeat = self.config['heartbeat_interval_ms'] / 1000
        return max(0, delay_to_next_heartbeat - time_since_last_heartbeat)

    def should_heartbeat(self):
        return self.time_to_next_heartbeat() == 0

    def session_timeout_expired(self):
        last_recv = max(self.last_receive, self.last_reset)
        return (time.time() - last_recv) > (self.config['session_timeout_ms'] / 1000)

    def reset_timeouts(self):
        self.last_reset = time.time()
        self.last_poll = time.time()
        self.heartbeat_failed = False

    def poll_timeout_expired(self):
        return (time.time() - self.last_poll) > (self.config['max_poll_interval_ms'] / 1000)

    def __str__(self):
        return ("<Heartbeat group_id={group_id}"
                " heartbeat_interval_ms={heartbeat_interval_ms}"
                " session_timeout_ms={session_timeout_ms}"
                " max_poll_interval_ms={max_poll_interval_ms}"
                " retry_backoff_ms={retry_backoff_ms}>").format(**self.config)
