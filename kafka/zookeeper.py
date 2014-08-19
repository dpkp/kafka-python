"""
This is originally from:

https://github.com/mahendra/kafka-python/blob/zookeeper/kafka/zookeeper.py

It is modified in a few places to work with more recent KafkaClient.

Also, multiprocess is substituted for threading. Since threading
is gevent friendly, where multiprocess is not.
"""
import logging
import threading
import os
import random
import socket
import uuid
import json
from functools import partial
from kafka.client import KafkaClient
from kafka.producer import SimpleProducer, KeyedProducer
from kafka.consumer import SimpleConsumer
from kazoo.client import KazooClient
from kazoo.handlers.gevent import SequentialGeventHandler

BROKER_IDS_PATH = 'brokers/ids/'      # Path where kafka stores broker info
PARTITIONER_PATH = 'python/kafka/'    # Path to use for consumer co-ordination
DEFAULT_TIME_BOUNDARY = 5

# Allocation states
ALLOCATION_COMPLETED = -1
ALLOCATION_CHANGING = -2
ALLOCATION_FAILED = -3
ALLOCATION_MISSED = -4
ALLOCATION_INACTIVE = -5


log = logging.getLogger("kafka")
random.seed()


def _get_brokers(zkclient, chroot='/'):
    """
    Get the list of available brokers registered in zookeeper
    """
    brokers = []
    root = os.path.join(chroot, BROKER_IDS_PATH)

    for broker_id in zkclient.get_children(root):
        path = os.path.join(root, broker_id)
        info, _ = zkclient.get(path)
        info = json.loads(info)
        brokers.append((info['host'], info['port']))

    log.debug("List of brokers fetched" + str(brokers))

    random.shuffle(brokers)
    return brokers


def get_client(zkclient, chroot='/'):
    """
    Given a zookeeper client, return a KafkaClient instance for use
    """
    brokers = _get_brokers(zkclient, chroot=chroot)
    brokers = ["%s:%s"%(host, port) for (host, port) in brokers]
    return KafkaClient(brokers)


# TODO: Make this a subclass of Producer later
class ZProducer(object):
    """
    A base Zookeeper producer to be used by other producer classes

    Args
    hosts: Comma-separated list of hosts to connect to
           (e.g. 127.0.0.1:2181,127.0.0.1:2182)
    topic - The kafka topic to send messages to
    chroot - The kafka subdirectory to search for brokers
    """
    producer_kls = None

    def __init__(self, hosts, topic, chroot='/', **kwargs):

        if self.producer_kls is None:
            raise NotImplemented("Producer class needs to be mentioned")

        self.zkclient = KazooClient(hosts=hosts)
        self.zkclient.start()

        # Start the producer instance
        self.client = get_client(self.zkclient, chroot=chroot)
        self.producer = self.producer_kls(self.client, topic, **kwargs)

        # Stop Zookeeper
        self.zkclient.stop()
        self.zkclient.close()
        self.zkclient = None

    def stop(self):
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
    chroot - The kafka subdirectory to search for brokers
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
    def __init__(self,
                 hosts,
                 group,
                 topic,
                 chroot='/',
                 block_init=True,
                 time_boundary=DEFAULT_TIME_BOUNDARY,
                 ignore_non_allocation=False,
                 **kwargs):

        # User is not allowed to specify partitions
        if 'partitions' in kwargs:
            raise ValueError("Partitions cannot be specified")

        # self.driver = KafkaDriver(driver_type)
        self.ignore_non_allocation = ignore_non_allocation
        self.time_boundary = time_boundary

        zkclient = KazooClient(hosts, handler=SequentialGeventHandler())
        zkclient.start()

        self.client = get_client(zkclient, chroot=chroot)
        self.client.load_metadata_for_topics(topic)
        partitions = set(self.client.topic_partitions[topic])

        # create consumer id
        hostname = socket.gethostname()
        self.identifier = "%s-%s-%s" % (topic, group, hostname)
        log.debug("Consumer id set to: %s" % self.identifier)

        path = os.path.join(chroot, PARTITIONER_PATH, topic, group)
        log.debug("Using path %s for co-ordination" % path)

        # Create a function which can be used for creating consumers
        self.consumer = []
        self.consumer_fact = partial(SimpleConsumer,
                                     self.client,
                                     group,
                                     topic,
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
        self.exit = threading.Event()

        # Used by the worker to indicate that allocation has changed
        self.changed = threading.Event()


        # The shared memory and lock used for sharing allocation info
        self.lock = threading.Lock()
        #self.allocated = multiprocessing.Array('i', len(partitions))
        self.allocated = [0] * len(partitions)

        # Initialize the array
        self._set_partitions(self.allocated, [], ALLOCATION_CHANGING)
        self.consumer_state = ALLOCATION_CHANGING

        # Start the worker
        self.proc = threading.Thread(target=self._check_and_allocate,
                                     args=(hosts,
                                           path,
                                           partitions,
                                           self.allocated))
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
        partitions = filter(lambda x: x>=0, self.allocated)
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

    def _set_consumer(self, block=False, timeout=None):
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


    def _check_and_allocate(self, hosts, path, partitions, array):
        """
        Checks if a new allocation is needed for the partitions.
        If so, co-ordinates with Zookeeper to get a set of partitions
        allocated for the consumer
        """

        old = None

        # Start zookeeper connection again
        zkclient = KazooClient(hosts, handler=SequentialGeventHandler())
        zkclient.start()

        identifier = '%s-%d-%s' % (self.identifier,
                                   os.getpid(),
                                   uuid.uuid4().hex)

        # Set up the partitioner
        partitioner = zkclient.SetPartitioner(path=path, set=partitions,
                                              identifier=identifier,
                                              time_boundary=self.time_boundary)

        # Once allocation is done, sleep for some time between each checks
        sleep_time = self.time_boundary / 2.0

        # Keep running the allocation logic till we are asked to exit
        while not self.exit.is_set():
            if partitioner.acquired:
                # A new set of partitions has been acquired
                new = list(partitioner)

                # If there is a change, notify for a consumer change
                if new != old:
                    log.info("Acquired partitions: %s" % str(new))
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
                old = None
                with self.lock:
                    self._set_partitions(array, [], ALLOCATION_CHANGING)
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
