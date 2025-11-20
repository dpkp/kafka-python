Usage
*****


CLI
===

The kafka module provides a simple command-line interface for consumer, producer,
and admin apis.

python -m kafka.consumer
------------------------

.. code:: bash

    ❯ python -m kafka.consumer --help
    usage: python -m kafka.consumer [-h] -b BOOTSTRAP_SERVERS -t TOPICS -g GROUP [-c EXTRA_CONFIG] [-l LOG_LEVEL] [-f FORMAT] [--encoding ENCODING]

    Kafka console consumer

    options:
      -h, --help            show this help message and exit
      -b BOOTSTRAP_SERVERS, --bootstrap-servers BOOTSTRAP_SERVERS
                            host:port for cluster bootstrap servers
      -t TOPICS, --topic TOPICS
                            subscribe to topic
      -g GROUP, --group GROUP
                            consumer group
      -c EXTRA_CONFIG, --extra-config EXTRA_CONFIG
                            additional configuration properties for kafka consumer
      -l LOG_LEVEL, --log-level LOG_LEVEL
                            logging level, passed to logging.basicConfig
      -f FORMAT, --format FORMAT
                            output format: str|raw|full
      --encoding ENCODING   encoding to use for str output decode()


python -m kafka.producer
------------------------

.. code:: bash

    ❯ python -m kafka.producer --help
    usage: python -m kafka.producer [-h] -b BOOTSTRAP_SERVERS -t TOPIC [-c EXTRA_CONFIG] [-l LOG_LEVEL] [--encoding ENCODING]

    Kafka console producer

    options:
      -h, --help            show this help message and exit
      -b BOOTSTRAP_SERVERS, --bootstrap-servers BOOTSTRAP_SERVERS
                            host:port for cluster bootstrap servers
      -t TOPIC, --topic TOPIC
                            publish to topic
      -c EXTRA_CONFIG, --extra-config EXTRA_CONFIG
                            additional configuration properties for kafka producer
      -l LOG_LEVEL, --log-level LOG_LEVEL
                            logging level, passed to logging.basicConfig
      --encoding ENCODING   byte encoding for produced messages


python -m kafka.admin
---------------------

.. code:: bash

    ❯ python -m kafka.admin --help
    usage: python -m kafka.admin [-h] -b BOOTSTRAP_SERVERS [-c EXTRA_CONFIG] [-l LOG_LEVEL] [-f FORMAT] {cluster,configs,log-dirs,topics,consumer-groups} ...

    Kafka admin client

    positional arguments:
      {cluster,configs,log-dirs,topics,consumer-groups}
                            subcommands
        cluster             Manage Kafka Cluster
        configs             Manage Kafka Configuration
        log-dirs            Manage Kafka Topic/Partition Log Directories
        topics              List/Describe/Create/Delete Kafka Topics
        consumer-groups     Manage Kafka Consumer Groups

    options:
      -h, --help            show this help message and exit
      -b BOOTSTRAP_SERVERS, --bootstrap-servers BOOTSTRAP_SERVERS
                            host:port for cluster bootstrap servers
      -c EXTRA_CONFIG, --extra-config EXTRA_CONFIG
                            additional configuration properties for admin client
      -l LOG_LEVEL, --log-level LOG_LEVEL
                            logging level, passed to logging.basicConfig
      -f FORMAT, --format FORMAT
                            output format: raw|json


KafkaConsumer
==============

.. code:: python

    from kafka import KafkaConsumer
    import json
    import msgpack

    # To consume latest messages and auto-commit offsets
    consumer = KafkaConsumer('my-topic',
                             group_id='my-group',
                             bootstrap_servers=['localhost:9092'])
    for message in consumer:
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                              message.offset, message.key,
                                              message.value))

    # consume earliest available messages, don't commit offsets
    KafkaConsumer(auto_offset_reset='earliest', enable_auto_commit=False)

    # consume json messages
    KafkaConsumer(value_deserializer=lambda m: json.loads(m.decode('ascii')))

    # consume msgpack
    KafkaConsumer(value_deserializer=msgpack.unpackb)

    # StopIteration if no message after 1sec
    KafkaConsumer(consumer_timeout_ms=1000)

    # Subscribe to a regex topic pattern
    consumer = KafkaConsumer()
    consumer.subscribe(pattern='^awesome.*')

    # Use multiple consumers in parallel w/ 0.9 kafka brokers
    # typically you would run each on a different server / process / CPU
    consumer1 = KafkaConsumer('my-topic',
                              group_id='my-group',
                              bootstrap_servers='my.server.com')
    consumer2 = KafkaConsumer('my-topic',
                              group_id='my-group',
                              bootstrap_servers='my.server.com')


There are many configuration options for the consumer class. See
:class:`~kafka.KafkaConsumer` API documentation for more details.


KafkaProducer
==============

.. code:: python

    from kafka import KafkaProducer
    from kafka.errors import KafkaError
    import msgpack
    import json

    producer = KafkaProducer(bootstrap_servers=['broker1:1234'])

    # Asynchronous by default
    future = producer.send('my-topic', b'raw_bytes')

    # Block for 'synchronous' sends
    try:
        record_metadata = future.get(timeout=10)
    except KafkaError:
        # Decide what to do if produce request failed...
        log.exception()
        pass

    # Successful result returns assigned partition and offset
    print (record_metadata.topic)
    print (record_metadata.partition)
    print (record_metadata.offset)

    # produce keyed messages to enable hashed partitioning
    producer.send('my-topic', key=b'foo', value=b'bar')

    # encode objects via msgpack
    producer = KafkaProducer(value_serializer=msgpack.dumps)
    producer.send('msgpack-topic', {'key': 'value'})

    # produce json messages
    producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'))
    producer.send('json-topic', {'key': 'value'})

    # produce asynchronously
    for _ in range(100):
        producer.send('my-topic', b'msg')

    def on_send_success(record_metadata):
        print(record_metadata.topic)
        print(record_metadata.partition)
        print(record_metadata.offset)

    def on_send_error(excp):
        log.error('I am an errback', exc_info=excp)
        # handle exception

    # produce asynchronously with callbacks
    producer.send('my-topic', b'raw_bytes').add_callback(on_send_success).add_errback(on_send_error)

    # block until all async messages are sent
    producer.flush()

    # configure multiple retries
    producer = KafkaProducer(retries=5)


ClusterMetadata
=============
.. code:: python

    from kafka.cluster import ClusterMetadata

    clusterMetadata = ClusterMetadata(bootstrap_servers=['broker1:1234'])

    # get all brokers metadata
    print(clusterMetadata.brokers())

    # get specific broker metadata
    print(clusterMetadata.broker_metadata('bootstrap-0'))

    # get all partitions of a topic
    print(clusterMetadata.partitions_for_topic("topic"))

    # list topics
    print(clusterMetadata.topics())


KafkaAdminClient
=============
.. code:: python
    from kafka import KafkaAdminClient
    from kafka.admin import NewTopic

    admin = KafkaAdminClient(bootstrap_servers=['broker1:1234'])

    # create a new topic
    topics_list = []
    topics_list.append(NewTopic(name="testtopic", num_partitions=1, replication_factor=1))
    admin.create_topics(topics_list,timeout_ms=None, validate_only=False)

    # delete a topic
    admin.delete_topics(['testtopic'])

    # list consumer groups
    print(admin.list_consumer_groups())

    # get consumer group details
    print(admin.describe_consumer_groups('cft-plt-qa.connect'))

    # get consumer group offset
    print(admin.list_consumer_group_offsets('cft-plt-qa.connect'))


