# Zookeeper support for kafka clients

import logging
import os
import random
import sys
import time

from functools import partial
from Queue import Empty

from kafka.client import KafkaClient
from kafka.common import KafkaDriver
from kafka.producer import Producer, SimpleProducer, KeyedProducer
from kafka.consumer import SimpleConsumer
from kazoo.client import KazooClient


BROKER_IDS_PATH = '/brokers/ids/'
PARTITIONER_PATH = '/python/kafka/'
DEFAULT_TIME_BOUNDARY = 5
CHECK_INTERVAL = 30

log = logging.getLogger("kafka")


def _get_brokers(zkclient):
    """
    Get the list of available brokers registered in zookeeper
    """
    brokers = []

    for broker_id in zkclient.get_children(BROKER_IDS_PATH):
        path = os.path.join(BROKER_IDS_PATH, broker_id)
        info, _ = zkclient.get(path)
        info = simplejson.loads(info)
        brokers.append((info['host'], info['port']))

    log.debug("List of brokers fetched" + str(brokers))

    random.shuffle(brokers)
    return brokers


def _get_client(zkclient):
    """
    Given a zookeeper client, return a KafkaClient instance for use
    """
    brokers = _get_brokers(zkclient)
    client = None

    for host, port in brokers:
        try:
            return KafkaClient(host, port)
        except Exception as exp:
            log.error("Error while connecting to %s:%d" % (host, port),
                      exc_info=sys.exc_info())

    raise exp


class ZProducer(Producer):
    """
    A base Zookeeper producer to be used by other producer classes
    """
    producer_kls = None

    def __init__(self, hosts, topic, **kwargs):

        if self.producer_kls is None:
            raise NotImplemented("Producer class needs to be mentioned")

        self.zkclient = KazooClient(hosts=hosts)
        self.zkclient.start()

        # Start the producer instance
        client = _get_client(self.zkclient)
        self.producer = self.producer_kls(client, topic, **kwargs)

        # Stop Zookeeper
        self.zkclient.stop()
        self.zkclient.close()
        self.zkclient = None

    @staticmethod
    def retry(fnc, retries=2, retry_after=2):
        """
        A decorator for attemtping retries in sending messages

        retries - Number of times we must attempt a retry
        retry_after - Delay in between retries
        """
        def retry_send(self, *args, **kwargs):
            count = retries
            while count > 0:
                try:
                    return fnc(self, *args, **kwargs)
                except Exception as exp:
                    log.error("Error in callback", exc_info=sys.exc_info())
                    self.producer.driver.sleep(retry_after)
                    count -= 1

        return retry_send


class ZSimpleProducer(ZProducer):
    """
    A simple, round-robbin producer. Each message goes to exactly one partition

    Args:
    hosts: Comma-separated list of hosts to connect to
           (e.g. 127.0.0.1:2181,127.0.0.1:2182)
    topic - The kafka topic to send messages to
    """
    producer_kls = SimpleProducer

    @ZProducer.retry
    def send_messages(self, *msg):
        self.producer.send_messages(*msg)


class ZKeyedProducer(Producer):
    """
    A producer which distributes messages to partitions based on a
    partitioner function (class) and the key

    hosts: Comma-separated list of hosts to connect to
           (e.g. 127.0.0.1:2181,127.0.0.1:2182)
    topic - The kafka topic to send messages to
    partitioner - A partitioner class that will be used to get the partition
        to send the message to. Must be derived from Partitioner
    """
    producer_kls = KeyedProducer

    @ZProducer.retry
    def send(self, key, msg):
        self.producer.send(key, msg)


class ZSimpleConsumer(object):
    """
    A consumer that uses Zookeeper to co-ordinate and share the partitions
    of a queue with other consumers

    hosts: Comma-separated list of hosts to connect to
           (e.g. 127.0.0.1:2181,127.0.0.1:2182)
    group: a name for this consumer, used for offset storage and must be unique
    topic: the topic to consume
    driver_type: The driver type to use for the consumer
    time_boundary: The time interval to wait out before deciding on consumer
        changes in zookeeper
    ignore_non_allocation: If set to True, the consumer will ignore the
        case where no partitions were allocated to it

    auto_commit: default True. Whether or not to auto commit the offsets
    auto_commit_every_n: default 100. How many messages to consume
                         before a commit
    auto_commit_every_t: default 5000. How much time (in milliseconds) to
                         wait before commit

    Auto commit details:
    If both auto_commit_every_n and auto_commit_every_t are set, they will
    reset one another when one is triggered. These triggers simply call the
    commit method on this class. A manual call to commit will also reset
    these triggers
    """
    def __init__(self, hosts, group, topic,
                 driver_type=KAFKA_PROCESS_DRIVER,
                 time_boundary=DEFAULT_TIME_BOUNDARY,
                 ignore_non_allocation=False,
                 **kwargs):

        if 'partitions' in kwargs:
            raise ValueError("Partitions cannot be specified")

        self.driver = KafkaDriver(driver_type)
        self.ignore_non_allocation = ignore_non_allocation

        self.zkclient = KazooClient(hosts, handler=self.driver.kazoo_handler())
        self.zkclient.start()

        self.client = _get_client(self.zkclient)
        self.client._load_metadata_for_topics(topic)
        self.partitions = set(self.client.topic_partitions[topic])

        # create consumer id
        hostname = self.driver.socket.gethostname()
        consumer_id = "%s-%s-%s-%d" % (topic, group, hostname, os.getpid())
        path = os.path.join(PARTITIONER_PATH, topic, group)

        self.partitioner = self.zkclient.SetPartitioner(
                                            path=path,
                                            set=self.partitions,
                                            identifier=consumer_id,
                                            time_boundary=time_boundary)

        self.consumer = None
        self.consumer_fact = partial(SimpleConsumer,
                                     client, group, topic,
                                     driver_type=driver_type, **kwargs)

        # Keep monitoring for changes
        self.exit = self.driver.Event()
        self.changed = self.driver.Event()

        self.proc = self.driver.Proc(target=_check_and_allocate,
                            args=(CHECK_INTERVAL, self.exit, self.changed))
        self.proc.daemon = True
        self.proc.start()

        # Do the setup once
        self._set_consumer(block=True, timeout=None)

    def _set_consumer(self, block=False, timeout=0):
        """
        Check if a new consumer has to be created because of a re-balance
        """
        try:
            partitions = self.changed.get(block=block, timeout=timeout)
        except Empty:
            return

        # Reset the consumer
        self.consumer = None

        if partitions == -1:
            raise RuntimeError("Error in partition allocation")

        # Check if we must change the consumer
        if not partitions:
            if not self.ignore_non_allocation:
                raise RuntimeError("Did not get any partition allocation")
            else:
                log.info("No partitions allocated. Ignoring")
        else:
            self.consumer = self.consumer_fact(partitions=partitions)

    def _check_and_allocate(self, sleep_time, exit, changed):
        """
        Checks if a new allocation is needed for the partitions.
        If so, co-ordinates with Zookeeper to get a set of partitions
        allocated for the consumer
        """
        old = []

        while not exit.is_set():
            if self.partitioner.acquired:
                new = list(self.partitioner)

                if new != old:
                    old = new
                    changed.put(new)

                log.info("Acquired partitions: %s", new)
                exit.wait(sleep_time)

            elif self.partitioner.release:
                log.info("Releasing partitions for reallocation")
                self.partitioner.release_set()
                old = []

            elif self.partitioner.failed:
                raise RuntimeError("Error in partition allocation")
                change.put(-1)

            elif self.partitioner.allocating:
                log.info("Waiting for partition allocation")
                self.partitioner.wait_for_acquire()

    def __iter__(self):
        """
        Iterate through data available in partitions allocated to this
        instance
        """
        self._set_consumer()

        for msg in self.consumer:
            yield msg

            if not self.change.empty():
                self._set_consumer()
                break

    def get_messages(self, count=1, block=True, timeout=0.1):
        """
        Fetch the specified number of messages

        count: Indicates the maximum number of messages to be fetched
        block: If True, the API will block till some messages are fetched.
        timeout: If None, and block=True, the API will block infinitely.
                 If >0, API will block for specified time (in seconds)
        """
        self._set_consumer(block, timeout=0.1)
        return self.consumer.get_messages(count, block, timeout)

    def stop():
        self.exit.set()
        self.proc.join()
        self.consumer.stop()
        self.client.stop()
        self.partitioner.finish()
        self.zkclient.close()
