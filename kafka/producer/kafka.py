from __future__ import absolute_import

from collections import namedtuple
import logging
try:
    from Queue import Empty, Full, Queue
except ImportError:  # python 3
    from queue import Empty, Full, Queue
import random
import threading
import time

import six

from kafka.client import KafkaClient
from kafka.common import (
    ProduceRequest, ConnectionError, RequestTimedOutError,
    LeaderNotAvailableError, UnknownTopicOrPartitionError,
    FailedPayloadsError, KafkaUnavailableError, KafkaConfigurationError
)
from kafka.protocol import (
    CODEC_NONE, CODEC_GZIP, CODEC_SNAPPY,
    create_message_set
)
from kafka.serializer import NoopSerializer
from kafka.util import kafka_bytestring

ProducerRecord = namedtuple(
    'ProducerRecord',
    'topic partition key value callback'
)

logger = logging.getLogger(__name__)

DEFAULT_PRODUCER_CONFIG = {
    # These configuration names are borrowed directly from
    # upstream java kafka producer:
    'bootstrap_servers': [],
    'acks': 1,
    'buffer_memory_messages': 50000, 
    'compression_type': None,
    'retries': 0,
    'client_id': __name__,
    'timeout_ms': 30000,
    'block_on_buffer_full': True,
    'metadata_fetch_timeout_ms': 60000,
    'metadata_max_age_ms': 300000,
    'retry_backoff_ms': 100,
    'key_serializer': NoopSerializer(is_key=True),
    'value_serializer': NoopSerializer(is_key=False),

    # currently unused upstream configs
    'batch_size_bytes': 16384,
    'linger_ms': 0,
    'max_request_size_bytes': 1048576,
    'receive_buffer_bytes': 32768,
    'send_buffer_bytes': 131072,
    'metric_reporters': [],
    'metrics_num_samples': 2,
    'metrics_sample_window_ms': 30000,
    'reconnect_backoff_ms': 10,

    # These configuration parameters are kafka-python specific -- not borrowed
    'socket_timeout_ms': 30 * 1000,
    'producer_loop_idle_wait_ms': 1000,
}

class KafkaProducer(object):
    """High-level, asynchronous kafka producer
    
    This producer has a simple interface modeled after the new upstream
    java client in version 0.8.2
    http://kafka.apache.org/documentation.html#producerapi

    The primary interface is KafkaProducer.send(), which places messages on an
    internal FIFO queue.  Messages are consumed from the queue asynchronously by
    a background worker thread.

    """
    def __init__(self, **configs):
        """
        KafkaProducer instance constructor takes keyword arguments as
        configuration.  The configuration keywords generally follow the upstream
        java client settings.

        Keyword Arguments:
            bootstrap_servers (list of str): list of kafka brokers to use for
                bootstrapping initial cluster metadata.  broker strings should
                be formatted as `host:port`
            acks (int): number of acks required for each produce request
                defaults to 1.  See Kafka Protocol documentation for more
                information.
            buffer_memory_messages (int): number of unsent messages to buffer
                internally before send() will block or raise. defaults to 50000
            compression_type (str): type of compression to apply to messages.
                options are 'none', 'gzip', or 'snappy'; defaults to 'none'
            retries (int): Number of times to retry a failed produce request.
                defaults to 0.
            client_id (str): a unique string identifier for this client, sent
                to the kafka cluster for bookkeeping.  Defaults to the module
                name ('kafka.producer.kafka').
            timeout_ms (int): milliseconds to wait for the kafka broker to get
                the required number of `acks`.  defaults to 30000.
            block_on_buffer_full (bool): configure whether send() should block
                when there is insufficient buffer_memory to handle the next
                message, or instead raise a Queue.Full exception.
                defaults to True.
            metadata_fetch_timeout_ms (int): milliseconds to wait before raising
                when refreshing cluster metadata.  defaults to 60000.
            metadata_max_age_ms (int): milliseconds to retain cluster metadata
                before requiring a refresh.  (cluster metadata will also be 
                refreshed when a server error is received suggesting that the
                metadata has changed). defaults to 300000.
            retry_backoff_ms (int): milliseconds to wait between retries.  only
                used for produce requests if `retries` > 0, but also applies to
                cluster metadata request retries.  defaults to 100.
            key_serializer (Serializer): an instance of a Serializer class used
                to convert a message key, passed to `send(key=foo)`, into raw
                bytes that will be encoded into the kafka message. Defaults to
                no serialization [NoopSerializer]
            value_serializer (Serializer): an instance of a Serializer class
                used to convert a message value into raw bytes that will be
                encoded into the kafka message.  Defaults to no serialization
                [NoopSerializer]
            socket_timeout_ms (int): client socket TCP timeout, used by
                KafkaClient instance.  defaults to 30000.
            producer_loop_idle_wait_ms (int): milliseconds to sleep when the
                internal producer queue is empty.  defaults to 1000.
            batch_size_bytes (int): currently unused.
            linger_ms (int): currently unusued.
            max_request_size_bytes (int): currently unused.
            receive_buffer_bytes (int): currently unused.
            send_buffer_bytes (int): currently unused.
            metric_reporters (list): currently unused.
            metrics_num_samples (int): currently unused.
            metrics_sample_window_ms (int): currently unused.
            reconnect_backoff_ms (int): currently unused.

        See Also
            http://kafka.apache.org/documentation.html#newproducerconfigs
            http://kafka.apache.org/082/javadoc/org/apache/kafka/clients/producer/ProducerConfig.html

        """
        self._thread_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._producer_queue = None
        self._producer_thread = None
        self._local_data = threading.local()
        self._client = None
        self._configure(**configs)

    def _configure(self, **configs):
        self._config = {}
        for key in DEFAULT_PRODUCER_CONFIG:
            self._config[key] = configs.pop(key, DEFAULT_PRODUCER_CONFIG[key])

        if configs:
            raise KafkaConfigurationError('Unknown configuration key(s): ' +
                                          str(list(configs.keys())))

        self._client = KafkaClient(
            self._config['bootstrap_servers'],
            client_id=self._config['client_id'],
            timeout=(self._config['socket_timeout_ms'] / 1000.0)
        )

        self._producer_queue = Queue(self._config['buffer_memory_messages'])

    def _start_producer_thread(self):
        with self._thread_lock:
            if self._producer_thread_is_running():
                logger.warning("Attempted to start async Kafka producer "
                               "when it was already running.")
                return False

            if self._producer_thread:
                logger.warning("starting a new _producer_thread...  "
                               "previous one crashed?")

            self._stop_event.clear()
            self._producer_thread = threading.Thread(target=self._producer_loop)
            self._producer_thread.daemon = True
            self._producer_thread.start()

    def _producer_thread_is_running(self):
        t = self._producer_thread
        return t is not None and t.is_alive()

    def _producer_loop(self):
        while not self._stop_event.is_set():
            try:
                record = self._producer_queue.get(block=False)
            except Empty:
                wait_time = self._config['producer_loop_idle_wait_ms'] / 1000.0
                self._stop_event.wait(wait_time)
                continue

            self._metadata_max_age_refresh()
            produce_request = self._encode_produce_request(record)

            # send produce request to kafka, optionally w/ retries
            retries = self._config['retries']
            while not self._stop_event.is_set():
                try:
                    responses = self._client.send_produce_request(
                        [produce_request],
                        acks=self._config['acks'],
                        timeout=self._config['timeout_ms']
                    )

                    # kafka server does not respond if acks == 0
                    if self._config['acks'] != 0:
                        
                        # Handle Callbacks -- swallow errors
                        self._handle_callback(record, responses[0])

                    # break marks the while loop as successful
                    # else block below will not run
                    break

                # Catch errors that indicate we need to refresh topic metadata
                except (LeaderNotAvailableError, UnknownTopicOrPartitionError,
                        FailedPayloadsError, ConnectionError) as e:
                    logger.warning("_producer_loop caught exception type %s, "
                                   "refreshing metadata", type(e))
                    try:
                        self.refresh_topic_metadata()
                        # We re-encode the produce request, in case it was
                        # affected by changed metadata
                        produce_request = self._encode_produce_request(record)
                    except RequestTimedOutError:
                        self._handle_dropped_message(record)
                        break

                # If the whole cluster is unavailable, we should backoff
                except KafkaUnavailableError:
                    logger.warning("KafkaUnavailableError error -- "
                                   "will retry after retry_backoff_ms")

                # ProduceRequests set acks and a timeout value to wait for the
                # required acks.  If the server cant get acks before timeout, we
                # get this error and need to decide whether to retry or skip
                except RequestTimedOutError:
                    logger.warning("RequestTimedOut error attempting to produce "
                                   "messages with %s acks required and %s ms "
                                   "timeout" % (self._config['acks'],
                                                self._config['timeout_ms']))

                # If another exception is raised, there's a problem....
                # Please open an issue on github!
                except BaseException:
                    logger.exception("_producer_loop unrecoverable exception"
                                     " - please report on github!")
                    raise

                # If we aren't retrying then we log the message
                # as (potentially) dropped,
                # then break from the inner while loop
                # and move to the next message in the queue
                if not retries:
                    self._handle_dropped_message(record)
                    break

                # Otherwise, backoff before retrying
                self._stop_event.wait(self._config['retry_backoff_ms'] / 1000.0)
                retries -= 1

            # If the loop did not successfully break before _stop_event was set
            # we need to log the message as failed
            # messages still on the queue will be handled separately
            else:
                self._handle_dropped_message(record)

            # Whether the message delivery was successful or not,
            # we ack it in the queue to allow others to join() the queue
            self._producer_queue.task_done()

    def _get_partition_for_record(self, record):
        if record.partition is not None:
            return record.partition

        else:
            partitions = self.partitions_for_topic(record.topic)
            if not partitions:
                self.refresh_topic_metadata()
                partitions = self.partitions_for_topic(record.topic)
                if not partitions: raise UnknownTopicOrPartitionError(record.topic)

            size = len(partitions)

            # hash the index
            if record.key is not None:
                idx = hash(record.key) % size

            # or round-robin it, if no key available
            else:
                try:
                    indices = self._local_data.partition_indices
                except AttributeError:
                    indices = dict()
                    self._local_data.partition_indices = indices

                idx = indices.get(record.topic)

                # if this is the first record of the round-robin
                # we select a random index in order to smooth
                # message distribution when there are many
                # short-lived producer instances
                if idx is None:
                    idx = random.randint(0, size - 1)
                else:
                    idx = (idx + 1) % size
                indices[record.topic] = idx

            partition = partitions[idx]

    def _encode_produce_request(self, record):
        value_serializer = self._config['value_serializer']
        key_serializer = self._config['key_serializer']
        codec = self._get_codec()
        produce_request = ProduceRequest(
            kafka_bytestring(record.topic),
            self._get_partition_for_record(record),
            create_message_set(
                [value_serializer.serialize(record.topic, record.value)],
                codec,
                key_serializer.serialize(record.topic, record.key)
            )
        )
        return produce_request

    def _handle_dropped_message(self, record):
        serializer = self._config['value_serializer']
        logger.error('DROPPED MESSAGE: %s',
                     serializer.serialize(record.topic, record.value))

    def _handle_callback(self, record, response):
        """
        Private method to handle message callbacks
        """
        # If no callback registered, do nothing
        if not record.callback:
            return

        # wrap callback in blanket try / except to avoid crashing the thread
        try:
            record.callback.__call__(record, response)
        except:
            logger.exception('Caught exception during callback (ignoring)')

    def send(self, topic=None, partition=None, key=None, value=None, callback=None):
        """Send a message to a kafka topic

        Messages are sent asynchronously via a background worker thread
        optionally specify specific partition and/or key and/or callback

        Keyword Arguments:
            topic (str): the topic to which the message should be sent.
            partition (int, optional): a specific partition to which the message
                should be routed.  if no partition is specified for a record,
                the partition will be determined by hashing the record key, or,
                if no key is provided, by round-robin across all partitions for
                the topic.
            key (any, optional): a key to use for partitioning.  keys are
                serialized with the configured `key_serializer` and are stored
                along with the value in encoded kafka message.
            value (any): the value to put in the kafka message.  values are
                serialized with the configured `value_serializer`.
            callback (callable, optional): a callback function that will be
                called asynchronously for each produced message with the
                partition and offset the message was routed to.  the callback
                signature should be `callback(record, response)`.  Note that the
                callback will not be called if the configured `acks` is 0.

        Returns:
            Nothing, KafkaProducer always sends messages asynchronously

        """
        if not self._producer_thread_is_running():
            self._start_producer_thread()

        record = ProducerRecord(topic, partition, key, value, callback)
        self._producer_queue.put(record, block=self._config['block_on_buffer_full'])
        return None

    def partitions_for_topic(self, topic):
        """Get a list of partition ids for a topic

        The method relies on cached cluster metadata and may return
        stale values if the cluster state has changed.  Metadata will
        be automatically refreshed if the producer detects leadership changes
        or other server errors suggesting the cached cluster state is out of
        date.  Configure `metadata_max_age_ms` to adjust how long cached
        metadata is kept before refreshing without any detected changes.

        Parameters:
            topic (str): the topic name

        Returns:
            a list of partition ids (ints) that are available for the topic
            based on the last fetched cluster metadata

        """
        return self._client.get_partition_ids_for_topic(topic) 

    def refresh_topic_metadata(self):
        """Refresh kafka broker / topic metadata

        Will retry until _stop_event is set or `metadata_fetch_timeout_ms`
        elapses.  Retries use the `retry_backoff_ms` configuration
        
        Catches KafkaUnavailableError, but any other exception will raise
        (and crash the thread!).

        Returns:
            True if successful, otherwise False

        """
        backoff = self._config['retry_backoff_ms'] / 1000.0
        timeout = self._config['metadata_fetch_timeout_ms'] / 1000.0
        start = time.time()
        while not self._stop_event.is_set() and time.time() < (start + timeout):
            try:
                self._client.load_metadata_for_topics()
                self._local_data.last_metadata_refresh = time.time()
                return True
            except KafkaUnavailableError:
                logger.warning("KafkaUnavailableError attempting "
                               "to refresh topic metadata")
                self._stop_event.wait(backoff)

        if time.time() >= (start + timeout):
            raise RequestTimedOutError()
        else:
            return False

    def _metadata_max_age_refresh(self):
        try:
            last_refresh = self._local_data.last_metadata_refresh
        except AttributeError:
            return True

        refresh_interval = self._config['metadata_max_age_ms'] / 1000.0

        if time.time() >= last_refresh + refresh_interval:
            return self.refresh_topic_metadata()
        return True

    def _get_codec(self):
        compression = self._config['compression_type']
        if compression is None or compression == 'none':
            return CODEC_NONE
        elif compression == 'gzip':
            return CODEC_GZIP
        elif compression == 'snappy':
            return CODEC_SNAPPY
        raise KafkaConfigurationError('Unknown compression type: %s' %
                                      compression)

    def close(self):
        """Shutdown the KafkaProducer instance"""
        with self._thread_lock:
            if self._producer_thread_is_running():
                self._stop_event.set()
                self._producer_thread.join()
                self._producer_thread = None
            else:
                logger.warning("producer thread not running")

            if not self._producer_queue.empty():
                logger.warning("producer thread stopped "
                               "with %d unsent messages",
                               self._producer_queue.qsize())
                while self._producer_queue.qsize() > 0:
                    self._handle_dropped_message(self._producer_queue.get())
                    self._producer_queue.task_done()

        return True

    def metrics(self):
        raise NotImplementedError()
