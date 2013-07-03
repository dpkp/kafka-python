# Zookeeper support for kafka clients

import logging
import os
import random
import sys
import time
import simplejson

from functools import partial
from Queue import Empty

from kafka.client import KafkaClient
from kafka.producer import SimpleProducer, KeyedProducer
from kafka.consumer import SimpleConsumer
from kazoo.client import KazooClient
from kafka.common import (
    KAFKA_PROCESS_DRIVER, KAFKA_THREAD_DRIVER, KAFKA_GEVENT_DRIVER,
    KafkaDriver
)


BROKER_IDS_PATH = '/brokers/ids/'      # Path where kafka stores broker info
PARTITIONER_PATH = '/python/kafka/'    # Path to use for consumer co-ordination
DEFAULT_TIME_BOUNDARY = 5
CHECK_INTERVAL = 30
ALLOCATION_CHANGING = -1
ALLOCATION_FAILED = -2


log = logging.getLogger("kafka")
random.seed()


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


def get_client(zkclient):
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


# TODO: Make this a subclass of Producer later
class ZProducer(object):
    """
    A base Zookeeper producer to be used by other producer classes

    Args
    hosts: Comma-separated list of hosts to connect to
           (e.g. 127.0.0.1:2181,127.0.0.1:2182)
    topic - The kafka topic to send messages to
    """
    producer_kls = None

    def __init__(self, hosts, topic, **kwargs):

        if self.producer_kls is None:
            raise NotImplemented("Producer class needs to be mentioned")

        self.zkclient = KazooClient(hosts=hosts)
        self.zkclient.start()

        # Start the producer instance
        self.client = get_client(self.zkclient)
        self.producer = self.producer_kls(self.client, topic, **kwargs)

        # Stop Zookeeper
        self.zkclient.stop()
        self.zkclient.close()
        self.zkclient = None

    def stop():
        self.producer.stop()
        self.client.close()


class ZSimpleProducer(ZProducer):
    """
    A simple, round-robbin producer. Each message goes to exactly one partition

    Args:
    hosts: Comma-separated list of hosts to connect to
           (e.g. 127.0.0.1:2181,127.0.0.1:2182)
    topic - The kafka topic to send messages to
    """
    producer_kls = SimpleProducer

    def send_messages(self, *msg):
        self.producer.send_messages(*msg)


class ZKeyedProducer(ZProducer):
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

    def send(self, key, msg):
        self.producer.send(key, msg)


class ZSimpleConsumer(object):
    """
    A consumer that uses Zookeeper to co-ordinate and share the partitions
    of a topic with other consumers

    hosts: Comma-separated list of hosts to connect to
           (e.g. 127.0.0.1:2181,127.0.0.1:2182)
    group: a name for this consumer, used for offset storage and must be unique
    topic: the topic to consume
    driver_type: The driver type to use for the consumer
    time_boundary: The time interval, in seconds, to wait out before deciding
        on consumer changes in zookeeper. A higher value will ensure that a
        consumer restart will not cause two re-balances.
        (Default 10s)
    ignore_non_allocation: If set to True, the consumer will ignore the
        case where no partitions were allocated to it.
        This can be used to keep consumers in stand-by. They will take over
        when another consumer fails. (Default False)

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

    Partition allocation details
    * When the consumer is initialized, it blocks till it gets an allocation
    * If ignore_non_allocation is False, the consumer will throw an error
      in init or during other operations
    * During re-balancing of partitions, the consumer will not return any
      messages (iteration or get_messages)
    * After re-balancing, if the consumer does not get any partitions,
      ignore_non_allocation will control it's behaviour
    """
    def __init__(self, hosts, group, topic,
                 driver_type=KAFKA_PROCESS_DRIVER,
                 time_boundary=DEFAULT_TIME_BOUNDARY,
                 ignore_non_allocation=False,
                 **kwargs):

        # User is not allowed to specify partitions
        if 'partitions' in kwargs:
            raise ValueError("Partitions cannot be specified")

        self.driver = KafkaDriver(driver_type)
        self.ignore_non_allocation = ignore_non_allocation

        self.zkclient = KazooClient(hosts, handler=self.driver.kazoo_handler())
        self.zkclient.start()

        self.client = get_client(self.zkclient)
        self.client._load_metadata_for_topics(topic)
        self.partitions = set(self.client.topic_partitions[topic])

        # create consumer id
        hostname = self.driver.socket.gethostname()
        consumer_id = "%s-%s-%s-%d" % (topic, group, hostname, os.getpid())
        path = os.path.join(PARTITIONER_PATH, topic, group)

        log.debug("Consumer id set to: %s" % consumer_id)
        log.debug("Using path %s for co-ordination" % path)

        self.partitioner = self.zkclient.SetPartitioner(
                                            path=path,
                                            set=self.partitions,
                                            identifier=consumer_id,
                                            time_boundary=time_boundary)

        # Create a function which can be used for creating consumers
        self.consumer_fact = partial(SimpleConsumer,
                                     client, group, topic,
                                     driver_type=driver_type, **kwargs)

        # Keep monitoring for changes
        self.exit = self.driver.Event()         # Notify worker to exit
        self.changed = self.driver.Event()      # Notify of partition changes

        self.proc = self.driver.Proc(target=_check_and_allocate,
                                     args=(CHECK_INTERVAL,))
        self.proc.daemon = True
        self.proc.start()

        # Do the setup once and block till we get an allocation
        self._set_consumer(block=True, timeout=None)

    def _set_consumer(self, block=False, timeout=0):
        """
        Check if a new consumer has to be created because of a re-balance
        """
        try:
            partitions = self.changed.get(block=block, timeout=timeout)
        except Empty:
            return

        # If we have a consumer clean it up
        if self.consumer:
            self.consumer.close()

        self.consumer = None

        if partitions == ALLOCATION_FAILED:
            # Allocation has failed. Nothing we can do about it
            raise RuntimeError("Error in partition allocation")

        elif partitions == ALLOCATION_CHANGING:
            # Allocation is changing because of consumer changes
            self.consumer = []
            log.info("Partitions are being reassigned")
            return

        elif not partitions:
            # Check if we must change the consumer
            if not self.ignore_non_allocation:
                raise RuntimeError("Did not get any partition allocation")
            else:
                log.info("No partitions allocated. Ignoring")
                self.consumer = []
        else:
            # Create a new consumer
            self.consumer = self.consumer_fact(partitions=partitions)

    def _check_and_allocate(self, sleep_time):
        """
        Checks if a new allocation is needed for the partitions.
        If so, co-ordinates with Zookeeper to get a set of partitions
        allocated for the consumer

        sleep_time: Time interval between each check after getting allocation
        """
        old = []

        while not self.exit.is_set():
            if self.partitioner.acquired:
                # A new set of partitions has been acquired
                new = list(self.partitioner)

                # If there is a change, notify for a consumer change
                if new != old:
                    log.info("Acquired partitions: %s", new)
                    old = new
                    self.changed.put(new)

                # Wait for a while before checking again. In the meantime
                # wake up if the user calls for exit
                self.exit.wait(sleep_time)

            elif self.partitioner.release:
                log.info("Releasing partitions for reallocation")
                self.change.put(ALLOCATION_CHANGING)
                self.partitioner.release_set()
                old = []

            elif self.partitioner.failed:
                self.change.put(ALLOCATION_FAILED)
                raise RuntimeError("Error in partition allocation")

            elif self.partitioner.allocating:
                log.info("Waiting for partition allocation")
                self.partitioner.wait_for_acquire()

    def __iter__(self):
        """
        Iterate through data available in partitions allocated to this
        instance
        """
        self._set_consumer(block=False)

        if self.consumer is None:
            raise RuntimeError("Error in partition allocation")

        for msg in self.consumer:
            yield msg

            if not self.change.empty():
                self._set_consumer(block=False)
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

        if not self.consumer:
            raise RuntimeError("Error in partition allocation")

        return self.consumer.get_messages(count, block, timeout)

    def stop():
        self.exit.set()
        self.proc.join()

        if self.consumer:
            self.consumer.stop()

        self.client.stop()
        self.partitioner.finish()
        self.zkclient.close()

    def commit(self):
        if self.consumer:
            self.consumer.commit()
        self._set_consumer(block=False)

    def seek(self, *args, **kwargs):
        self._set_consumer()

        if self.consumer is None:
            raise RuntimeError("Partition allocation failed")
        elif not self.consumer:
            raise RuntimeError("Waiting for partition allocation")

        return self.consumer.seek(*args, **kwargs)

    def pending(self):
        if self.consumer is None:
            raise RuntimeError("Error in partition allocation")
        elif not self.consumer:
            # We are in a transition/suspended state
            return 0

        return self.consumer.pending()
