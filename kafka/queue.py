from copy import copy
import logging
from multiprocessing import Process, Queue, Event
from Queue import Empty
import time

from kafka.client import KafkaClient, FetchRequest, ProduceRequest

log = logging.getLogger("kafka")

raise NotImplementedError("Still need to refactor this class")


class KafkaConsumerProcess(Process):
    def __init__(self, client, topic, partition, out_queue, barrier,
                 consumer_fetch_size=1024, consumer_sleep=200):
        self.client = copy(client)
        self.topic = topic
        self.partition = partition
        self.out_queue = out_queue
        self.barrier = barrier
        self.consumer_fetch_size = consumer_fetch_size
        self.consumer_sleep = consumer_sleep / 1000.
        log.info("Initializing %s" % self)
        Process.__init__(self)

    def __str__(self):
        return "[KafkaConsumerProcess: topic=%s, \
            partition=%s, sleep=%s]" % \
            (self.topic, self.partition, self.consumer_sleep)

    def run(self):
        self.barrier.wait()
        log.info("Starting %s" % self)
        fetchRequest = FetchRequest(self.topic, self.partition,
                                    offset=0, size=self.consumer_fetch_size)

        while True:
            if self.barrier.is_set() is False:
                log.info("Shutdown %s" % self)
                self.client.close()
                break

            lastOffset = fetchRequest.offset
            (messages, fetchRequest) = self.client.get_message_set(fetchRequest)

            if fetchRequest.offset == lastOffset:
                log.debug("No more data for this partition, "
                          "sleeping a bit (200ms)")
                time.sleep(self.consumer_sleep)
                continue

            for message in messages:
                self.out_queue.put(message)


class KafkaProducerProcess(Process):
    def __init__(self, client, topic, in_queue, barrier,
                 producer_flush_buffer=500,
                 producer_flush_timeout=2000,
                 producer_timeout=100):

        self.client = copy(client)
        self.topic = topic
        self.in_queue = in_queue
        self.barrier = barrier
        self.producer_flush_buffer = producer_flush_buffer
        self.producer_flush_timeout = producer_flush_timeout / 1000.
        self.producer_timeout = producer_timeout / 1000.
        log.info("Initializing %s" % self)
        Process.__init__(self)

    def __str__(self):
        return "[KafkaProducerProcess: topic=%s, \
            flush_buffer=%s, flush_timeout=%s, timeout=%s]" % \
            (self.topic,
                self.producer_flush_buffer,
                self.producer_flush_timeout,
                self.producer_timeout)

    def run(self):
        self.barrier.wait()
        log.info("Starting %s" % self)
        messages = []
        last_produce = time.time()

        def flush(messages):
            self.client.send_message_set(ProduceRequest(self.topic, -1,
                                                        messages))
            del messages[:]

        while True:
            if self.barrier.is_set() is False:
                log.info("Shutdown %s, flushing messages" % self)
                flush(messages)
                self.client.close()
                break

            if len(messages) > self.producer_flush_buffer:
                log.debug("Message count threshold reached. Flushing messages")
                flush(messages)
                last_produce = time.time()

            elif (time.time() - last_produce) > self.producer_flush_timeout:
                log.debug("Producer timeout reached. Flushing messages")
                flush(messages)
                last_produce = time.time()

            try:
                msg = KafkaClient.create_message(
                    self.in_queue.get(True, self.producer_timeout))
                messages.append(msg)

            except Empty:
                continue


class KafkaQueue(object):
    def __init__(self, client, topic, partitions,
                 producer_config=None, consumer_config=None):
        """
        KafkaQueue a Queue-like object backed by a Kafka producer and some
        number of consumers

        Messages are eagerly loaded by the consumer in batches of size
        consumer_fetch_size.
        Messages are buffered in the producer thread until
        producer_flush_timeout or producer_flush_buffer is reached.

        Params
        ======
        client: KafkaClient object
        topic: str, the topic name
        partitions: list of ints, the partions to consume from
        producer_config: dict, see below
        consumer_config: dict, see below

        Consumer Config
        ===============
        consumer_fetch_size: int, number of bytes to fetch in one call
                             to Kafka. Default is 1024
        consumer_sleep: int, time in milliseconds a consumer should sleep
                        when it reaches the end of a partition. Default is 200

        Producer Config
        ===============
        producer_timeout: int, time in milliseconds a producer should
                          wait for messages to enqueue for producing.
                          Default is 100
        producer_flush_timeout: int, time in milliseconds a producer
                                should allow messages to accumulate before
                                sending to Kafka. Default is 2000
        producer_flush_buffer: int, number of messages a producer should
                               allow to accumulate. Default is 500

        """
        producer_config = {} if producer_config is None else producer_config
        consumer_config = {} if consumer_config is None else consumer_config

        self.in_queue = Queue()
        self.out_queue = Queue()
        self.consumers = []
        self.barrier = Event()

        # Initialize and start consumer threads
        for partition in partitions:
            consumer = KafkaConsumerProcess(client, topic, partition,
                                            self.in_queue, self.barrier,
                                            **consumer_config)
            consumer.start()
            self.consumers.append(consumer)

        # Initialize and start producer thread
        self.producer = KafkaProducerProcess(client, topic, self.out_queue,
                                             self.barrier, **producer_config)
        self.producer.start()

        # Trigger everything to start
        self.barrier.set()

    def get(self, block=True, timeout=None):
        """
        Consume a message from Kafka

        Params
        ======
        block: boolean, default True
        timeout: int, number of seconds to wait when blocking, default None

        Returns
        =======
        msg: str, the payload from Kafka
        """
        return self.in_queue.get(block, timeout).payload

    def put(self, msg, block=True, timeout=None):
        """
        Send a message to Kafka

        Params
        ======
        msg: std, the message to send
        block: boolean, default True
        timeout: int, number of seconds to wait when blocking, default None
        """
        self.out_queue.put(msg, block, timeout)

    def close(self):
        """
        Close the internal queues and Kafka consumers/producer
        """
        self.in_queue.close()
        self.out_queue.close()
        self.barrier.clear()
        self.producer.join()
        for consumer in self.consumers:
            consumer.join()
