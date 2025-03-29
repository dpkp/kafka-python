from __future__ import absolute_import, division

import collections
import threading
import time

from kafka.errors import IllegalStateError


NO_PRODUCER_ID = -1
NO_PRODUCER_EPOCH = -1


class ProducerIdAndEpoch(object):
    __slots__ = ('producer_id', 'epoch')

    def __init__(self, producer_id, epoch):
        self.producer_id = producer_id
        self.epoch = epoch

    @property
    def is_valid(self):
        return NO_PRODUCER_ID < self.producer_id

    def __str__(self):
        return "ProducerIdAndEpoch(producer_id={}, epoch={})".format(self.producer_id, self.epoch)

class TransactionState(object):
    __slots__ = ('producer_id_and_epoch', '_sequence_numbers', '_lock')

    def __init__(self):
        self.producer_id_and_epoch = ProducerIdAndEpoch(NO_PRODUCER_ID, NO_PRODUCER_EPOCH)
        self._sequence_numbers = collections.defaultdict(lambda: 0)
        self._lock = threading.Condition()

    def has_pid(self):
        return self.producer_id_and_epoch.is_valid


    def await_producer_id_and_epoch(self, max_wait_time_ms):
        """
        A blocking call to get the pid and epoch for the producer. If the PID and epoch has not been set, this method
        will block for at most maxWaitTimeMs. It is expected that this method be called from application thread
        contexts (ie. through Producer.send). The PID it self will be retrieved in the background thread.

        Arguments:
            max_wait_time_ms (numeric): The maximum time to block.

        Returns:
            ProducerIdAndEpoch object. Callers must check the 'is_valid' property of the returned object to ensure that a
                valid pid and epoch is actually returned.
        """
        with self._lock:
            start = time.time()
            elapsed = 0
            while not self.has_pid() and elapsed < max_wait_time_ms:
                self._lock.wait(max_wait_time_ms / 1000)
                elapsed = time.time() - start
            return self.producer_id_and_epoch

    def set_producer_id_and_epoch(self, producer_id, epoch):
        """
        Set the pid and epoch atomically. This method will signal any callers blocked on the `pidAndEpoch` method
        once the pid is set. This method will be called on the background thread when the broker responds with the pid.
        """
        with self._lock:
            self.producer_id_and_epoch = ProducerIdAndEpoch(producer_id, epoch)
            if self.producer_id_and_epoch.is_valid:
                self._lock.notify_all()

    def reset_producer_id(self):
        """
        This method is used when the producer needs to reset it's internal state because of an irrecoverable exception
        from the broker.
       
        We need to reset the producer id and associated state when we have sent a batch to the broker, but we either get
        a non-retriable exception or we run out of retries, or the batch expired in the producer queue after it was already
        sent to the broker.
       
        In all of these cases, we don't know whether batch was actually committed on the broker, and hence whether the
        sequence number was actually updated. If we don't reset the producer state, we risk the chance that all future
        messages will return an OutOfOrderSequenceException.
        """
        with self._lock:
            self.producer_id_and_epoch = ProducerIdAndEpoch(NO_PRODUCER_ID, NO_PRODUCER_EPOCH)
            self._sequence_numbers.clear()

    def sequence_number(self, tp):
        with self._lock:
            return self._sequence_numbers[tp]

    def increment_sequence_number(self, tp, increment):
        with self._lock:
            if tp not in self._sequence_numbers:
                raise IllegalStateError("Attempt to increment sequence number for a partition with no current sequence.")
            self._sequence_numbers[tp] += increment
