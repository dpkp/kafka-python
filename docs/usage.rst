Usage
*****


KafkaConsumer
==============

.. code:: python

    from kafka import KafkaConsumer, OffsetAndMetadata, TopicPartition, JsonSerializer, DefaultSerializer

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
    
    # Manually commit offsets (disable auto-commit)
    consumer = KafkaConsumer('my-topic',
                             group_id='my-group',
                             enable_auto_commit=False,
                             bootstrap_servers=['localhost:9092'])
    for message in consumer:
        # process message
        process_message(message)
        # TopicPartition for this record
        tp = TopicPartition(message.topic, message.partition)
        # Note: When committing offsets manually, commit the next offset the consumer
        # should read. For example, after successfully processing a message at
        # offset 42, commit offset 43.
        consumer.commit({
            tp: OffsetAndMetadata(message.offset + 1, '', -1)
        })
    
    # consume earliest available messages, don't commit offsets
    KafkaConsumer(auto_offset_reset='earliest', enable_auto_commit=False)

    # consume json messages
    KafkaConsumer(value_deserializer=JsonSerializer())

    # consume utf-8
    KafkaConsumer(value_deserializer=DefaultSerializer())

    # consume utf-16
    KafkaConsumer(value_deserializer=DefaultSerializer('utf-16'))

    # StopIteration if no message after 1sec
    KafkaConsumer(consumer_timeout_ms=1000)

    # Subscribe to a regex topic pattern
    consumer = KafkaConsumer()
    consumer.subscribe(pattern='^awesome.*')

    # Use multiple consumers in parallel
    # (run each on a different server / process / CPU)
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

    from kafka import KafkaProducer, JsonSerializer, DefaultSerializer
    from kafka.errors import KafkaError

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
    producer.send('keyed-topic', key=b'foo', value=b'bar')

    # encode str with utf-8 encoding
    producer = KafkaProducer(value_serializer=DefaultSerializer())
    producer.send('utf-8-topic', 'value_str')

    # encode str with utf-16 encoding
    producer = KafkaProducer(value_serializer=DefaultSerializer('utf-16'))
    producer.send('utf-16-topic', '懂不懂')

    # produce json messages
    producer = KafkaProducer(value_serializer=JsonSerializer())
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


KafkaAdminClient
================

.. code:: python

    from kafka import KafkaAdminClient

    admin = KafkaAdminClient(bootstrap_servers=['broker1:1234'])

    # create topics with defaults (requires kafka 2.4+)
    admin.create_topics(['testtopic1'], timeout_ms=None, validate_only=False)
    # create a new topic with details
    new_topics = {
        'num_partitions': 1,
        'replication_factor': 1,
        'assignments': {0: [1]},                      # assign partition 0 to broker id 1
        'configs': {'max_message_bytes': '1000000'},  # set non-default configs
    }
    admin.create_topics(new_topics, timeout_ms=None, validate_only=False)

    # delete a topic
    admin.delete_topics(['testtopic1'])

    # list consumer groups
    print(admin.list_groups())

    # get consumer group details
    print(admin.describe_groups(['cft-plt-qa.connect']))

    # get consumer group offset
    print(admin.list_group_offsets(['cft-plt-qa.connect']))


CLI
===

The kafka module provides a simple command-line interface for consumer, producer,
and admin apis.

kafka-python consumer
---------------------

.. code:: bash

    > kafka-python consumer --help

    usage: kafka-python consumer [-h] -b BOOTSTRAP_SERVERS [-S SECURITY_PROTOCOL] [-M SASL_MECHANISM] [-U SASL_USER] [-P SASL_PASSWORD] [-l LOG_LEVEL] [-L ENABLE_LOGGER] [-D DISABLE_LOGGER] [-C EXTRA_CONFIG] -t TOPICS [-g GROUP] [-i GROUP_INSTANCE_ID] [-f FORMAT]
                                    [--encoding ENCODING]

    Kafka console consumer

    options:
      -h, --help            show this help message and exit

    connection:
      -b, --bootstrap-servers BOOTSTRAP_SERVERS
                            host:port for cluster bootstrap server. Can be provided multiple times.
      -S, --security-protocol SECURITY_PROTOCOL
                            PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
      -M, --sasl-mechanism SASL_MECHANISM
                            PLAIN, GSSAPI, OAUTHBEARER, SCRAM-SHA-256, SCRAM-SHA-512
      -U, --sasl-user SASL_USER
      -P, --sasl-password SASL_PASSWORD

    logging:
      -l, --log-level LOG_LEVEL
                            logging level, passed to logging.basicConfig
      -L, --enable-logger ENABLE_LOGGER
                            enable a specific logger. Can be provided multiple times. If not provided, all loggers are enabled
      -D, --disable-logger DISABLE_LOGGER
                            disable a specific logger. Can be provided multiple times.

    extended:
      -C, --extra-config EXTRA_CONFIG
                            additional configuration properties for client in "key=val" format. Can be provided multiple times.

    consumer options:
      -t, --topic TOPICS    subscribe to topic
      -g, --group GROUP     consumer group
      -i, --group-instance-id GROUP_INSTANCE_ID
                            static group membership identifier
      -f, --format FORMAT   output format: str|raw|full
      --encoding ENCODING   encoding to use for str output decode()


kafka-python producer
---------------------

.. code:: bash

    > kafka-python producer --help

    usage: kafka-python producer [-h] -b BOOTSTRAP_SERVERS [-S SECURITY_PROTOCOL] [-M SASL_MECHANISM] [-U SASL_USER] [-P SASL_PASSWORD] [-l LOG_LEVEL] [-L ENABLE_LOGGER] [-D DISABLE_LOGGER] [-C EXTRA_CONFIG] -t TOPIC [--encoding ENCODING]

    Kafka console producer

    options:
      -h, --help            show this help message and exit

    connection:
      -b, --bootstrap-servers BOOTSTRAP_SERVERS
                            host:port for cluster bootstrap server. Can be provided multiple times.
      -S, --security-protocol SECURITY_PROTOCOL
                            PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
      -M, --sasl-mechanism SASL_MECHANISM
                            PLAIN, GSSAPI, OAUTHBEARER, SCRAM-SHA-256, SCRAM-SHA-512
      -U, --sasl-user SASL_USER
      -P, --sasl-password SASL_PASSWORD

    logging:
      -l, --log-level LOG_LEVEL
                            logging level, passed to logging.basicConfig
      -L, --enable-logger ENABLE_LOGGER
                            enable a specific logger. Can be provided multiple times. If not provided, all loggers are enabled
      -D, --disable-logger DISABLE_LOGGER
                            disable a specific logger. Can be provided multiple times.

    extended:
      -C, --extra-config EXTRA_CONFIG
                            additional configuration properties for client in "key=val" format. Can be provided multiple times.

    producer options:
      -t, --topic TOPIC     publish to topic
      --encoding ENCODING   byte encoding for produced messages


kafka-python admin
------------------

.. code:: bash

    > kafka-python admin --help

    usage: kafka-python admin [-h] [-b BOOTSTRAP_SERVERS] [-S SECURITY_PROTOCOL] [-M SASL_MECHANISM] [-U SASL_USER] [-P SASL_PASSWORD] [-l LOG_LEVEL] [-L ENABLE_LOGGER] [-D DISABLE_LOGGER] [-C EXTRA_CONFIG] [--format FORMAT] GROUP ...

    Kafka Admin Client

    options:
      -h, --help            show this help message and exit

    connection:
      -b, --bootstrap-servers BOOTSTRAP_SERVERS
                            host:port for cluster bootstrap server. Can be provided multiple times.
      -S, --security-protocol SECURITY_PROTOCOL
                            PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
      -M, --sasl-mechanism SASL_MECHANISM
                            PLAIN, GSSAPI, OAUTHBEARER, SCRAM-SHA-256, SCRAM-SHA-512
      -U, --sasl-user SASL_USER
      -P, --sasl-password SASL_PASSWORD

    logging:
      -l, --log-level LOG_LEVEL
                            logging level, passed to logging.basicConfig
      -L, --enable-logger ENABLE_LOGGER
                            enable a specific logger. Can be provided multiple times. If not provided, all loggers are enabled
      -D, --disable-logger DISABLE_LOGGER
                            disable a specific logger. Can be provided multiple times.

    extended:
      -C, --extra-config EXTRA_CONFIG
                            additional configuration properties for client in "key=val" format. Can be provided multiple times.

    output:
      --format FORMAT       output format: raw|json

    Available command groups:
      GROUP
        acls                Manage Kafka ACLs
        cluster             Manage Kafka Cluster
        configs             Manage Kafka Configuration
        topics              Manage Kafka Topics
        partitions          Manage Kafka Partitions
        groups              Manage Kafka Groups
        transactions        Inspect and recover from hanging transactions (KIP-664)
        users               Manage Kafka Users


