from __future__ import absolute_import

import heapq
import threading


class PunctuationQueue(object):

    def __init__(self):
        self._pq = []
        self._lock = threading.Lock()

    def schedule(self, sched):
        with self._lock:
            heapq.heappush(self._pq, sched)

    def close(self):
        with self._lock:
            self._pq = []

    def may_punctuate(self, timestamp, punctuator):
        with self._lock:
            punctuated = False
            while (self._pq and self._pq[0][0] <= timestamp):
                old_ts, node, interval_ms = heapq.heappop(self._pq)
                if old_ts == 0:
                    old_ts = timestamp
                punctuator.punctuate(node, timestamp)
                sched = (old_ts + interval_ms, node, interval_ms)
                heapq.heappush(self._pq, sched)
                punctuated = True
            return punctuated
