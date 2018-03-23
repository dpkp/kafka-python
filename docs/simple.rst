Simple APIs (DEPRECATED)
************************


SimpleConsumer (DEPRECATED)
===========================

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


SimpleProducer (DEPRECATED)
===========================

Asynchronous Mode
-----------------

.. code:: python

    from kafka import SimpleProducer, SimpleClient

    # To send messages asynchronously
    client = SimpleClient('localhost:9092')
    producer = SimpleProducer(client, async_send=True)
    producer.send_messages('my-topic', b'async message')

    # To send messages in batch. You can use any of the available
    # producers for doing this. The following producer will collect
    # messages in batch and send them to Kafka after 20 messages are
    # collected or every 60 seconds
    # Notes:
    # * If the producer dies before the messages are sent, there will be losses
    # * Call producer.stop() to send the messages and cleanup
    producer = SimpleProducer(client,
                              async_send=True,
                              batch_send_every_n=20,
                              batch_send_every_t=60)

Synchronous Mode
----------------

.. code:: python

    from kafka import SimpleProducer, SimpleClient

    # To send messages synchronously
    client = SimpleClient('localhost:9092')
    producer = SimpleProducer(client, async_send=False)

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
                              async_send=False,
                              req_acks=SimpleProducer.ACK_AFTER_LOCAL_WRITE,
                              ack_timeout=2000,
                              sync_fail_on_error=False)

    responses = producer.send_messages('my-topic', b'another message')
    for r in responses:
        logging.info(r.offset)


KeyedProducer (DEPRECATED)
==========================

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


SimpleClient (DEPRECATED)
=========================


.. code:: python

    import time
    from kafka import SimpleClient
    from kafka.errors import LeaderNotAvailableError, NotLeaderForPartitionError
    from kafka.protocol import create_message
    from kafka.structs import ProduceRequestPayload

    kafka = SimpleClient('localhost:9092')
    payload = ProduceRequestPayload(topic='my-topic', partition=0,
                                    messages=[create_message("some message")])

    retries = 5
    resps = []
    while retries and not resps:
        retries -= 1
        try:
            resps = kafka.send_produce_request(
                payloads=[payload], fail_on_error=True)
        except LeaderNotAvailableError, NotLeaderForPartitionError:
            kafka.load_metadata_for_topics()
            time.sleep(1)

        # Other exceptions you might consider handling:
        # UnknownTopicOrPartitionError, TopicAuthorizationFailedError,
        # RequestTimedOutError, MessageSizeTooLargeError, InvalidTopicError,
        # RecordListTooLargeError, InvalidRequiredAcksError,
        # NotEnoughReplicasError, NotEnoughReplicasAfterAppendError

    kafka.close()

    resps[0].topic      # 'my-topic'
    resps[0].partition  # 0
    resps[0].error      # 0
    resps[0].offset     # offset of the first message sent in this request
