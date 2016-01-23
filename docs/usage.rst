Usage
*****


KafkaConsumer
=============

.. code:: python

    from kafka import KafkaConsumer

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

    # consume smallest available messages, dont commit offsets
    KafkaConsumer(auto_offset_reset='smallest', auto_commit_enable=False)

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


SimpleProducer
==============

Asynchronous Mode
-----------------

.. code:: python

    from kafka import SimpleProducer, SimpleClient

    # To send messages asynchronously
    client = SimpleClient('localhost:9092')
    producer = SimpleProducer(client, async=True)
    producer.send_messages('my-topic', b'async message')

    # To send messages in batch. You can use any of the available
    # producers for doing this. The following producer will collect
    # messages in batch and send them to Kafka after 20 messages are
    # collected or every 60 seconds
    # Notes:
    # * If the producer dies before the messages are sent, there will be losses
    # * Call producer.stop() to send the messages and cleanup
    producer = SimpleProducer(client,
                              async=True,
                              batch_send_every_n=20,
                              batch_send_every_t=60)

Synchronous Mode
----------------

.. code:: python

    from kafka import SimpleProducer, SimpleClient

    # To send messages synchronously
    client = SimpleClient('localhost:9092')
    producer = SimpleProducer(client, async=False)

    # Note that the application is responsible for encoding messages to type bytes
    producer.send_messages('my-topic', b'some message')
    producer.send_messages('my-topic', b'this method', b'is variadic')

    # Send unicode message
    producer.send_messages('my-topic', u'你怎么样?'.encode('utf-8'))

    # To wait for acknowledgements
    # ACK_AFTER_LOCAL_WRITE : server will wait till the data is written to
    #                         a local log before sending response
    # ACK_AFTER_CLUSTER_COMMIT : server will block until the message is committed
    #                            by all in sync replicas before sending a response
    producer = SimpleProducer(client,
                              async=False,
                              req_acks=SimpleProducer.ACK_AFTER_LOCAL_WRITE,
                              ack_timeout=2000,
                              sync_fail_on_error=False)

    responses = producer.send_messages('my-topic', b'another message')
    for r in responses:
        logging.info(r.offset)


KeyedProducer
=============

.. code:: python

    from kafka import (
        SimpleClient, KeyedProducer,
        Murmur2Partitioner, RoundRobinPartitioner)

    kafka = SimpleClient('localhost:9092')

    # HashedPartitioner is default (currently uses python hash())
    producer = KeyedProducer(kafka)
    producer.send_messages(b'my-topic', b'key1', b'some message')
    producer.send_messages(b'my-topic', b'key2', b'this methode')

    # Murmur2Partitioner attempts to mirror the java client hashing
    producer = KeyedProducer(kafka, partitioner=Murmur2Partitioner)

    # Or just produce round-robin (or just use SimpleProducer)
    producer = KeyedProducer(kafka, partitioner=RoundRobinPartitioner)
