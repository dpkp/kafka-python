"""
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
"""
from __future__ import absolute_import

import copy
import logging
import uuid

import kafka.streams.errors as Errors

from .processor.stream_thread import StreamThread
from .utils import AtomicInteger

log = logging.getLogger(__name__)


# container states
CREATED = 0
RUNNING = 1
STOPPED = 2


class KafkaStreams(object):
    """
    Kafka Streams allows for performing continuous computation on input coming
    from one or more input topics and sends output to zero or more output
    topics.

    The computational logic can be specified either by using the TopologyBuilder
    class to define the a DAG topology of Processors or by using the
    KStreamBuilder class which provides the high-level KStream DSL to define
    the transformation.

    The KafkaStreams class manages the lifecycle of a Kafka Streams instance.
    One stream instance can contain one or more threads specified in the configs
    for the processing work.

    A KafkaStreams instance can co-ordinate with any other instances with the
    same application ID (whether in this same process, on other processes on
    this machine, or on remote machines) as a single (possibly distributed)
    stream processing client. These instances will divide up the work based on
    the assignment of the input topic partitions so that all partitions are
    being consumed. If instances are added or failed, all instances will
    rebalance the partition assignment among themselves to balance processing
    load.

    Internally the KafkaStreams instance contains a normal KafkaProducer and
    KafkaConsumer instance that is used for reading input and writing output.

    A simple example might look like this:

        builder = (KStreamBuilder().stream('my-input-topic')
                                   .map_values(lambda value: str(len(value))
                                   .to('my-output-topic'))

        streams = KafkaStreams(builder,
                               application_id='my-stream-processing-application',
                               bootstrap_servers=['localhost:9092'],
                               key_serializer=json.dumps,
                               key_deserializer=json.loads,
                               value_serializer=json.dumps,
                               value_deserializer=json.loads)
        streams.start()
    """
    STREAM_CLIENT_ID_SEQUENCE = AtomicInteger(0)
    METRICS_PREFIX = 'kafka.streams'

    DEFAULT_CONFIG = {
        'application_id': None,
        'bootstrap_servers': None,
        'num_stream_threads': 1,
    }

    def __init__(self, builder, **configs):
        """Construct the stream instance.

        Arguments:
            builder (...): The processor topology builder specifying the computational logic
        """
        self.config = copy.copy(self.DEFAULT_CONFIG)
        for key in self.config:
            if key in configs:
                self.config[key] = configs.pop(key)

        # Only check for extra config keys in top-level class
        log.warning('Unrecognized configs: %s', configs.keys())

        self._state = CREATED

        # processId is expected to be unique across JVMs and to be used
        # in userData of the subscription request to allow assignor be aware
        # of the co-location of stream thread's consumers. It is for internal
        # usage only and should not be exposed to users at all.
        self.config['process_id'] = uuid.uuid4().hex

        # The application ID is a required config and hence should always have value
        if 'application_id' not in self.config:
            raise Errors.StreamsError('application_id is a required parameter')

        builder.set_application_id(self.config['application_id'])

        if 'client_id' not in self.config:
            next_id = self.STREAM_CLIENT_ID_SEQUENCE.increment()
            self.config['client_id'] = self.config['application_id'] + "-" + str(next_id)

        # reporters = self.config['metric_reporters']

        #MetricConfig metricConfig = new MetricConfig().samples(config.getInt(StreamsConfig.METRICS_NUM_SAMPLES_CONFIG))
        #    .timeWindow(config.getLong(StreamsConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG),
        #        TimeUnit.MILLISECONDS);

        #self._metrics = new Metrics(metricConfig, reporters, time);

        self._threads = [StreamThread(builder, **self.config)
                         for _ in range(self.config['num_stream_threads'])]

    #synchronized
    def start(self):
        """Start the stream instance by starting all its threads.

        Raises:
            IllegalStateException if process was already started
        """
        log.debug('Starting Kafka Stream process')

        if self._state == CREATED:
            for thread in self._threads:
                thread.start()

            self._state = RUNNING

            log.info('Started Kafka Stream process')
        elif self._state == RUNNING:
            raise Errors.IllegalStateError('This process was already started.')
        else:
            raise Errors.IllegalStateError('Cannot restart after closing.')

    #synchronized
    def close(self):
        """Shutdown this stream instance.
        
        Signals all the threads to stop, and then waits for them to join.

        Raises:
            IllegalStateException if process has not started yet
        """
        log.debug('Stopping Kafka Stream process')

        if self._state == RUNNING:
            # signal the threads to stop and wait
            for thread in self._threads:
                thread.close()

            for thread in self._threads:
                thread.join()

        if self._state != STOPPED:
            #metrics.close()
            self._state = STOPPED
            log.info('Stopped Kafka Stream process')

    def setUncaughtExceptionHandler(self, handler):
        """Sets the handler invoked when a stream thread abruptly terminates
        due to an uncaught exception.

        Arguments:
            handler: the object to use as this thread's uncaught exception handler.
            If None then this thread has no explicit handler.
        """
        for thread in self._threads:
            thread.set_uncaught_exception_handler(handler)
