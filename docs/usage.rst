Usage
=====

High level
----------

.. code:: python

    from kafka import SimpleProducer, KafkaClient, KafkaConsumer

    # To send messages synchronously
    kafka = KafkaClient("localhost:9092")
    producer = SimpleProducer(kafka)

    # Note that the application is responsible for encoding messages to type str
    producer.send_messages("my-topic", "some message")
    producer.send_messages("my-topic", "this method", "is variadic")

    # Send unicode message
    producer.send_messages("my-topic", u'你怎么样?'.encode('utf-8'))

    # To send messages asynchronously
    # WARNING: current implementation does not guarantee message delivery on failure!
    # messages can get dropped! Use at your own risk! Or help us improve with a PR!
    producer = SimpleProducer(kafka, async=True)
    producer.send_messages("my-topic", "async message")

    # To wait for acknowledgements
    # ACK_AFTER_LOCAL_WRITE : server will wait till the data is written to
    #                         a local log before sending response
    # ACK_AFTER_CLUSTER_COMMIT : server will block until the message is committed
    #                            by all in sync replicas before sending a response
    producer = SimpleProducer(kafka, async=False,
                              req_acks=SimpleProducer.ACK_AFTER_LOCAL_WRITE,
                              ack_timeout=2000)

    response = producer.send_messages("my-topic", "another message")

    if response:
        print(response[0].error)
        print(response[0].offset)

    # To send messages in batch. You can use any of the available
    # producers for doing this. The following producer will collect
    # messages in batch and send them to Kafka after 20 messages are
    # collected or every 60 seconds
    # Notes:
    # * If the producer dies before the messages are sent, there will be losses
    # * Call producer.stop() to send the messages and cleanup
    producer = SimpleProducer(kafka, batch_send=True,
                              batch_send_every_n=20,
                              batch_send_every_t=60)

    # To consume messages
    consumer = KafkaConsumer("my-topic", group_id="my_group",
                             metadata_broker_list=["localhost:9092"])
    for message in consumer:
        # message is raw byte string -- decode if necessary!
        # e.g., for unicode: `message.decode('utf-8')`
        print(message)

    kafka.close()


Keyed messages
--------------

.. code:: python

    from kafka import (KafkaClient, KeyedProducer, HashedPartitioner,
                       RoundRobinPartitioner)

    kafka = KafkaClient("localhost:9092")

    # HashedPartitioner is default
    producer = KeyedProducer(kafka)
    producer.send("my-topic", "key1", "some message")
    producer.send("my-topic", "key2", "this methode")

    producer = KeyedProducer(kafka, partitioner=RoundRobinPartitioner)


Multiprocess consumer
---------------------

.. code:: python

    from kafka import KafkaClient, MultiProcessConsumer

    kafka = KafkaClient("localhost:9092")

    # This will split the number of partitions among two processes
    consumer = MultiProcessConsumer(kafka, "my-group", "my-topic", num_procs=2)

    # This will spawn processes such that each handles 2 partitions max
    consumer = MultiProcessConsumer(kafka, "my-group", "my-topic",
                                    partitions_per_proc=2)

    for message in consumer:
        print(message)

    for message in consumer.get_messages(count=5, block=True, timeout=4):
        print(message)

Low level
---------

.. code:: python

    from kafka import KafkaClient, create_message
    from kafka.protocol import KafkaProtocol
    from kafka.common import ProduceRequest

    kafka = KafkaClient("localhost:9092")

    req = ProduceRequest(topic="my-topic", partition=1,
        messages=[create_message("some message")])
    resps = kafka.send_produce_request(payloads=[req], fail_on_error=True)
    kafka.close()

    resps[0].topic      # "my-topic"
    resps[0].partition  # 1
    resps[0].error      # 0 (hopefully)
    resps[0].offset     # offset of the first message sent in this request
