from kafka.producer import Producer, _send_upstream, STOP_ASYNC_PRODUCER, BATCH_SEND_MSG_COUNT, BATCH_SEND_DEFAULT_INTERVAL, SimpleProducer, KeyedProducer

import gevent
from gevent.queue import Queue

class _ProducerMixin(object):

    def _setup_async(self, batch_send_every_n, batch_send_every_t):
        if self.async:
            self.queue = Queue()  # Messages are sent through this queue
            self.job = gevent.spawn(_send_upstream,
                                    self.queue,
                                    self.client.copy(),
                                    batch_send_every_t,
                                    batch_send_every_n,
                                    self.req_acks,
                                    self.ack_timeout)

    def stop(self, timeout=1):
        if self.async:
            self.queue.put((STOP_ASYNC_PRODUCER, None))
            self.job.join(timeout)
            if self.job.dead is False:
                self.job.kill()

class _Producer(_ProducerMixin, Producer):
    pass

class _SimpleProducer(_ProducerMixin, SimpleProducer):
    pass

class _KeyedProducer(_ProducerMixin, KeyedProducer):
    pass
