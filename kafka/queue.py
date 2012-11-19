from copy import copy
import logging
from multiprocessing import Process, Queue, Event
from Queue import Empty
import time

from .client import KafkaClient, FetchRequest, ProduceRequest

log = logging.getLogger("kafka")

class KafkaConsumerProcess(Process):
    def __init__(self, client, topic, partition, out_queue, barrier):
        self.client = copy(client)
        self.topic = topic
        self.partition = partition
        self.out_queue = out_queue
        self.barrier = barrier
        self.consumer_sleep = 0.2
        Process.__init__(self)

    def config(self, consumer_sleep):
        self.consumer_sleep = consumer_sleep / 1000.

    def run(self):
        self.barrier.wait()
        log.info("Starting Consumer")
        fetchRequest = FetchRequest(self.topic, self.partition, offset=0, size=self.client.bufsize)
        while True:
            if self.barrier.is_set() == False:
                self.client.close()
                break
            lastOffset = fetchRequest.offset
            (messages, fetchRequest) = self.client.get_message_set(fetchRequest)
            if fetchRequest.offset == lastOffset:
                log.debug("No more data for this partition, sleeping a bit (200ms)")
                time.sleep(self.consumer_sleep)
                continue
            for message in messages:
                self.out_queue.put(message)

class KafkaProducerProcess(Process):
    def __init__(self, client, topic, in_queue, barrier):
        self.client = copy(client)
        self.topic = topic
        self.in_queue = in_queue
        self.barrier = barrier
        self.producer_flush_buffer = 100
        self.producer_flush_timeout = 2.0
        self.producer_timeout = 0.1
        Process.__init__(self)

    def config(self, producer_flush_buffer, producer_flush_timeout, producer_timeout):
        self.producer_flush_buffer = producer_flush_buffer
        self.producer_flush_timeout = producer_flush_timeout / 1000.
        self.producer_timeout = producer_timeout / 1000.

    def run(self):
        self.barrier.wait()
        log.info("Starting Producer")
        messages = []
        last_produce = time.time()

        def flush(messages):
            self.client.send_message_set(ProduceRequest(self.topic, -1, messages))
            del messages[:]

        while True:
            if self.barrier.is_set() == False:
                log.info("Producer shut down. Flushing messages")
                flush(messages)
                self.client.close()
                break
            if len(messages) > self.producer_flush_buffer:
                log.debug("Message count threashold reached. Flushing messages")
                flush(messages)
            elif (time.time() - last_produce) > self.producer_flush_timeout:
                log.debug("Producer timeout reached. Flushing messages")
                flush(messages)
                last_produce = time.time()
            try:
                messages.append(KafkaClient.create_message(self.in_queue.get(True, self.producer_timeout)))
            except Empty:
                continue

class KafkaQueue(object):
    def __init__(self, client, topic, partitions):
        self.in_queue = Queue()
        self.out_queue = Queue()
        self.consumers = []
        self.barrier = Event()

        # Initialize and start consumer threads
        for partition in partitions:
            consumer = KafkaConsumerProcess(client, topic, partition, self.in_queue, self.barrier)
            consumer.config(consumer_sleep=200)
            consumer.start()
            self.consumers.append(consumer)

        # Initialize and start producer thread
        self.producer = KafkaProducerProcess(client, topic, self.out_queue, self.barrier)
        self.producer.config(producer_flush_buffer=500, producer_flush_timeout=2000, producer_timeout=100)
        self.producer.start()

        # Trigger everything to start
        self.barrier.set()

    def get(self, block=True, timeout=None):
        return self.in_queue.get(block, timeout).payload

    def put(self, msg, block=True, timeout=None):
        return self.out_queue.put(msg, block, timeout)

    def close(self):
        self.in_queue.close()
        self.out_queue.close()
        self.barrier.clear()
        self.producer.join()
        for consumer in self.consumers:
            consumer.join()
