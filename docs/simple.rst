Simple APIs (DEPRECATED)
************************


SimpleConsumer
==============

.. code:: python

    from kafka import SimpleProducer, SimpleClient

    # To consume messages
    client = SimpleClient('localhost:9092')
    consumer = SimpleConsumer(client, "my-group", "my-topic")
    for message in consumer:
        # message is raw byte string -- decode if necessary!
        # e.g., for unicode: `message.decode('utf-8')`
        print(message)


    # Use multiprocessing for parallel consumers
    from kafka import MultiProcessConsumer

    # This will split the number of partitions among two processes
    consumer = MultiProcessConsumer(client, "my-group", "my-topic", num_procs=2)

    # This will spawn processes such that each handles 2 partitions max
    consumer = MultiProcessConsumer(client, "my-group", "my-topic",
                                    partitions_per_proc=2)

    for message in consumer:
        print(message)

    for message in consumer.get_messages(count=5, block=True, timeout=4):
        print(message)

    client.close()


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


SimpleClient
============


.. code:: python

    from kafka import SimpleClient, create_message
    from kafka.protocol import KafkaProtocol
    from kafka.common import ProduceRequest

    kafka = SimpleClient("localhost:9092")

    req = ProduceRequest(topic="my-topic", partition=1,
                         messages=[create_message("some message")])
    resps = kafka.send_produce_request(payloads=[req], fail_on_error=True)
    kafka.close()

    resps[0].topic      # "my-topic"
    resps[0].partition  # 1
    resps[0].error      # 0 (hopefully)
    resps[0].offset     # offset of the first message sent in this request
