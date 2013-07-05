# Zookeeper support for kafka clients

import logging
import multiprocessing
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
from kazoo.exceptions import NoNodeError
from kafka.common import (
    KAFKA_PROCESS_DRIVER, KAFKA_THREAD_DRIVER, KAFKA_GEVENT_DRIVER,
    KafkaDriver
)


BROKER_IDS_PATH = '/brokers/ids/'      # Path where kafka stores broker info
PARTITIONER_PATH = '/python/kafka/'    # Path to use for consumer co-ordination
DEFAULT_TIME_BOUNDARY = 5
CHECK_INTERVAL = 30

# Allocation states
ALLOCATION_COMPLETED = -1
ALLOCATION_CHANGING = -2
ALLOCATION_FAILED = -3
ALLOCATION_MISSED = -4
ALLOCATION_INACTIVE = -5


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


class ZOffsetManagingConsumer(SimpleConsumer):
    """
    A SimpleConsumer instance that stores/retrieves the offset in Zookeeper
    itself. This is useful for clients using older version of Kafka which
    do not support the offset fetch API

    Args:
    hosts: Comma-separated list of hosts to connect to
           (e.g. 127.0.0.1:2181,127.0.0.1:2182)
    offset_path: The zookeeper path where offsets must be stored
    """
    def __init__(self, hosts, offset_path, *args, **kwargs):
        driver = KafkaDriver(kwargs['driver_type'])
        self.offset_path = offset_path
        self.hosts = hosts
        self.zkclient = None    # Will be initialized later in commit thread

        zkclient = KazooClient(hosts, handler=driver.kazoo_handler())
        zkclient.start()
        zkclient.ensure_path(offset_path)

        super(ZOffsetManagingConsumer, self).__init__(*args, **kwargs)

        # Fetch the offsets for Zookeeper and store it
        for partition in self.offsets.keys():
            try:
                self.offsets[partition] = self._get_offset(zkclient, partition)

                # The partition has been iterated before.
                self.fetch_started[partition] = True
            except NoNodeError:
                pass

        zkclient.stop()
        zkclient.close()

    def close(self):
        super(ZOffsetManagingConsumer, self).close()

        if self.zkclient:
            self.zkclient.stop()
            self.zkclient.close()

    def _get_offset(self, zkclient, partition):
        """
        Get the stored offset for the partition
        """
        path = os.path.join(self.offset_path, str(partition))

        try:
            val, info = zkclient.get(path)
            return int(val) if val else 0
        except NoNodeError:
            zkclient.create(path)
            raise

    def _commit(self, partitions=None, client=None):
        """
        Commit the data to Zookeeper
        """

        # Initialize the zookeeper client once. If the committer is
        # running in a different process, this is required for it to work
        if not self.zkclient:
            self.zkclient = KazooClient(self.hosts,
                                        handler=self.driver.kazoo_handler())
            self.zkclient.start()

        # short circuit if nothing happened.
        if self.count_since_commit.value == 0:
            return

        for partition, offset in self.offsets.shareditems(keys=partitions):
            path = os.path.join(self.offset_path, str(partition))
            self.zkclient.set(path, b'%s' % str(offset))

        self.count_since_commit.value = 0


class ZSimpleConsumer(object):
    """
    A consumer that uses Zookeeper to co-ordinate and share the partitions
    of a topic with other consumers

    hosts: Comma-separated list of hosts to connect to
           (e.g. 127.0.0.1:2181,127.0.0.1:2182)
    group: a name for this consumer, used for offset storage and must be unique
    topic: the topic to consume
    driver_type: The driver type to use for the consumer
    block_init: If True, the init method will block till the allocation is
        completed. If not, it will return immediately and user can invoke
        consumer.status() to check the status. Default True.
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
                 block_init=True,
                 time_boundary=DEFAULT_TIME_BOUNDARY,
                 ignore_non_allocation=False,
                 manage_offsets=False,
                 **kwargs):

        # User is not allowed to specify partitions
        if 'partitions' in kwargs:
            raise ValueError("Partitions cannot be specified")

        self.driver = KafkaDriver(driver_type)
        self.ignore_non_allocation = ignore_non_allocation
        self.time_boundary = time_boundary

        zkclient = KazooClient(hosts, handler=self.driver.kazoo_handler())
        zkclient.start()

        self.client = get_client(zkclient)
        self.client._load_metadata_for_topics(topic)
        partitions = set(self.client.topic_partitions[topic])

        # create consumer id
        hostname = self.driver.socket.gethostname()
        self.identifier = "%s-%s-%s-%d" % (topic, group, hostname, os.getpid())
        log.debug("Consumer id set to: %s" % self.identifier)

        base_path = os.path.join(PARTITIONER_PATH, topic, group)

        # Create a function which can be used for creating consumers
        self.consumer = []
        if manage_offsets:
            offset_path = os.path.join(base_path, "offsets")
            self.consumer_fact = partial(ZOffsetManagingConsumer,
                                         hosts, offset_path,
                                         self.client, group, topic,
                                         driver_type=driver_type,
                                         **kwargs)
        else:
            self.consumer_fact = partial(SimpleConsumer,
                                         self.client, group, topic,
                                         driver_type=driver_type,
                                         **kwargs)

        # Keep monitoring for changes

        # Design:
        # * We will have a worker which will keep monitoring for rebalance
        # * The worker and main consumer will share data via shared memory
        #   protected by a lock
        # * If the worker gets new allocations, it will SET an Event()
        # * The main consumer will check this event to change itself
        # * Main consumer will SET another Event() to indicate worker to exit

        # This event will notify the worker to exit
        self.exit = self.driver.Event()

        # Used by the worker to indicate that allocation has changed
        self.changed = self.driver.Event()

        # The shared memory and lock used for sharing allocation info
        self.lock = self.driver.Lock()
        self.allocated = multiprocessing.Array('i', len(partitions))

        # Initialize the array
        self._set_partitions(self.allocated, [], ALLOCATION_CHANGING)
        self.consumer_state = ALLOCATION_CHANGING

        # Start the worker
        path = os.path.join(base_path, "partitions")
        log.debug("Using path %s for co-ordination" % path)

        self.proc = self.driver.Proc(target=self._check_and_allocate,
                                     args=(hosts, path, partitions,
                                           self.allocated, CHECK_INTERVAL))
        self.proc.daemon = True
        self.proc.start()

        # Stop the Zookeeper client (worker will create one itself)
        zkclient.stop()
        zkclient.close()

        # Do the setup once and block till we get an allocation
        self._set_consumer(block=block_init, timeout=None)

    def __repr__(self):
        """
        Give a string representation of the consumer
        """
        partitions = filter(lambda x: x >= 0, self.allocated)
        message = ','.join([str(i) for i in partitions])

        if not message:
            message = self.status()

        return u'ZSimpleConsumer<%s>' % message

    def status(self):
        """
        Returns the status of the consumer
        """
        self._set_consumer(block=False)

        if self.consumer_state == ALLOCATION_COMPLETED:
            return 'ALLOCATED'
        elif self.consumer_state == ALLOCATION_CHANGING:
            return 'ALLOCATING'
        elif self.consumer_state == ALLOCATION_FAILED:
            return 'FAILED'
        elif self.consumer_state == ALLOCATION_MISSED:
            return 'MISSED'
        elif self.consumer_state == ALLOCATION_INACTIVE:
            return 'INACTIVE'

    def _set_partitions(self, array, partitions, filler):
        """
        Update partition info in the shared memory array
        """
        i = 0
        for partition in partitions:
            array[i] = partition
            i += 1

        while i < len(array):
            array[i] = filler
            i += 1

    def _set_consumer(self, block=False, timeout=0):
        """
        Check if a new consumer has to be created because of a re-balance
        """
        if not block:
            timeout = 0

        if not self.changed.wait(timeout=timeout):
            return

        # There is a change. Get our new partitions
        with self.lock:
            partitions = [p for p in self.allocated]
            self.changed.clear()

        # If we have a consumer clean it up
        if self.consumer:
            self.consumer.stop()

        self.consumer = None
        self.consumer_state = partitions[0]

        if self.consumer_state == ALLOCATION_MISSED:
            # Check if we must change the consumer
            if not self.ignore_non_allocation:
                raise RuntimeError("Did not get any partition allocation")
            else:
                log.info("No partitions allocated. Ignoring")
                self.consumer = []

        elif self.consumer_state == ALLOCATION_FAILED:
            # Allocation has failed. Nothing we can do about it
            raise RuntimeError("Error in partition allocation")

        elif self.consumer_state == ALLOCATION_CHANGING:
            # Allocation is changing because of consumer changes
            self.consumer = []
            log.info("Partitions are being reassigned")
            return

        elif self.consumer_state == ALLOCATION_INACTIVE:
            log.info("Consumer is inactive")
        else:
            # Create a new consumer
            partitions = filter(lambda x: x >= 0, partitions)
            self.consumer = self.consumer_fact(partitions=partitions)
            self.consumer_state = ALLOCATION_COMPLETED
            log.info("Reinitialized consumer with %s" % partitions)

    def _check_and_allocate(self, hosts, path, partitions, array, sleep_time):
        """
        Checks if a new allocation is needed for the partitions.
        If so, co-ordinates with Zookeeper to get a set of partitions
        allocated for the consumer
        """

        old = None

        # Start zookeeper connection again
        zkclient = KazooClient(hosts, handler=self.driver.kazoo_handler())
        zkclient.start()

        # Set up the partitioner
        partitioner = zkclient.SetPartitioner(path=path, set=partitions,
                                              identifier=self.identifier,
                                              time_boundary=self.time_boundary)

        # Keep running the allocation logic till we are asked to exit
        while not self.exit.is_set():
            if partitioner.acquired:
                # A new set of partitions has been acquired
                new = list(partitioner)

                # If there is a change, notify for a consumer change
                if new != old:
                    log.info("Acquired partitions: %s", new)
                    old = new
                    with self.lock:
                        self._set_partitions(array, new, ALLOCATION_MISSED)
                        self.changed.set()

                # Wait for a while before checking again. In the meantime
                # wake up if the user calls for exit
                self.exit.wait(sleep_time)

            elif partitioner.release:
                # We have been asked to release the partitions
                log.info("Releasing partitions for reallocation")
                old = []
                with self.lock:
                    self._set_partitions(array, old, ALLOCATION_CHANGING)
                    self.changed.set()

                partitioner.release_set()

            elif partitioner.failed:
                # Partition allocation failed
                old = []
                with self.lock:
                    self._set_partitions(array, old, ALLOCATION_FAILED)
                    self.changed.set()
                break

            elif partitioner.allocating:
                # We have to wait till the partition is allocated
                log.info("Waiting for partition allocation")
                partitioner.wait_for_acquire()

        # Clean up
        partitioner.finish()

        with self.lock:
            self._set_partitions(array, [], ALLOCATION_INACTIVE)
            self.changed.set()

        zkclient.stop()
        zkclient.close()

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
            self._set_consumer(block=False)

    def get_messages(self, count=1, block=True, timeout=0.1):
        """
        Fetch the specified number of messages

        count: Indicates the maximum number of messages to be fetched
        block: If True, the API will block till some messages are fetched.
        timeout: If None, and block=True, the API will block infinitely.
                 If >0, API will block for specified time (in seconds)
        """
        self._set_consumer(block, timeout)

        if self.consumer is None:
            raise RuntimeError("Error in partition allocation")
        elif not self.consumer:
            return []

        return self.consumer.get_messages(count, block, timeout)

    def stop(self):
        self.exit.set()
        self.proc.join()
        self._set_consumer(block=True)
        self.client.close()

    def commit(self):
        if self.consumer:
            self.consumer.commit()
        self._set_consumer(block=False)

    def seek(self, *args, **kwargs):
        self._set_consumer()

        if self.consumer is None:
            raise RuntimeError("Error in partition allocation")
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
