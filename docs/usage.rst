Usage
=====

SimpleProducer
--------------

.. code:: python

    from kafka import SimpleProducer, KafkaClient

    # To send messages synchronously
    kafka = KafkaClient('localhost:9092')
    producer = SimpleProducer(kafka)

    # Note that the application is responsible for encoding messages to type bytes
    producer.send_messages(b'my-topic', b'some message')
    producer.send_messages(b'my-topic', b'this method', b'is variadic')

    # Send unicode message
    producer.send_messages(b'my-topic', u'你怎么样?'.encode('utf-8'))

Asynchronous Mode
-----------------

.. code:: python

    # To send messages asynchronously
    producer = SimpleProducer(kafka, async=True)
    producer.send_messages(b'my-topic', b'async message')

    # To wait for acknowledgements
    # ACK_AFTER_LOCAL_WRITE : server will wait till the data is written to
    #                         a local log before sending response
    # ACK_AFTER_CLUSTER_COMMIT : server will block until the message is committed
    #                            by all in sync replicas before sending a response
    producer = SimpleProducer(kafka, async=False,
                              req_acks=SimpleProducer.ACK_AFTER_LOCAL_WRITE,
                              ack_timeout=2000,
                              sync_fail_on_error=False)

    responses = producer.send_messages(b'my-topic', b'another message')
    for r in responses:
        logging.info(r.offset)

    # To send messages in batch. You can use any of the available
    # producers for doing this. The following producer will collect
    # messages in batch and send them to Kafka after 20 messages are
    # collected or every 60 seconds
    # Notes:
    # * If the producer dies before the messages are sent, there will be losses
    # * Call producer.stop() to send the messages and cleanup
    producer = SimpleProducer(kafka, async=True,
                              batch_send_every_n=20,
                              batch_send_every_t=60)

Keyed messages
--------------

.. code:: python

    from kafka import (
        KafkaClient, KeyedProducer,
        Murmur2Partitioner, RoundRobinPartitioner)

    kafka = KafkaClient('localhost:9092')

    # HashedPartitioner is default (currently uses python hash())
    producer = KeyedProducer(kafka)
    producer.send_messages(b'my-topic', b'key1', b'some message')
    producer.send_messages(b'my-topic', b'key2', b'this methode')

    # Murmur2Partitioner attempts to mirror the java client hashing
    producer = KeyedProducer(kafka, partitioner=Murmur2Partitioner)

    # Or just produce round-robin (or just use SimpleProducer)
    producer = KeyedProducer(kafka, partitioner=RoundRobinPartitioner)



KafkaConsumer
-------------

.. code:: python

    from kafka import KafkaConsumer

    # To consume messages
    consumer = KafkaConsumer('my-topic',
                             group_id='my_group',
                             bootstrap_servers=['localhost:9092'])
    for message in consumer:
        # message value is raw byte string -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                             message.offset, message.key,
                                             message.value))


messages (m) are namedtuples with attributes:

  * `m.topic`: topic name (str)
  * `m.partition`: partition number (int)
  * `m.offset`: message offset on topic-partition log (int)
  * `m.key`: key (bytes - can be None)
  * `m.value`: message (output of deserializer_class - default is raw bytes)


.. code:: python

    from kafka import KafkaConsumer

    # more advanced consumer -- multiple topics w/ auto commit offset
    # management
    consumer = KafkaConsumer('topic1', 'topic2',
                             bootstrap_servers=['localhost:9092'],
                             group_id='my_consumer_group',
                             auto_commit_enable=True,
                             auto_commit_interval_ms=30 * 1000,
                             auto_offset_reset='smallest')

    # Infinite iteration
    for m in consumer:
      do_some_work(m)

      # Mark this message as fully consumed
      # so it can be included in the next commit
      #
      # **messages that are not marked w/ task_done currently do not commit!
      consumer.task_done(m)

    # If auto_commit_enable is False, remember to commit() periodically
    consumer.commit()

    # Batch process interface
    while True:
      for m in kafka.fetch_messages():
        process_message(m)
        consumer.task_done(m)


  Configuration settings can be passed to constructor,
  otherwise defaults will be used:

.. code:: python

      client_id='kafka.consumer.kafka',
      group_id=None,
      fetch_message_max_bytes=1024*1024,
      fetch_min_bytes=1,
      fetch_wait_max_ms=100,
      refresh_leader_backoff_ms=200,
      bootstrap_servers=[],
      socket_timeout_ms=30*1000,
      auto_offset_reset='largest',
      deserializer_class=lambda msg: msg,
      auto_commit_enable=False,
      auto_commit_interval_ms=60 * 1000,
      consumer_timeout_ms=-1

  Configuration parameters are described in more detail at
  http://kafka.apache.org/documentation.html#highlevelconsumerapi

Multiprocess consumer
---------------------

.. code:: python

    from kafka import KafkaClient, MultiProcessConsumer

    kafka = KafkaClient('localhost:9092')

    # This will split the number of partitions among two processes
    consumer = MultiProcessConsumer(kafka, b'my-group', b'my-topic', num_procs=2)

    # This will spawn processes such that each handles 2 partitions max
    consumer = MultiProcessConsumer(kafka, b'my-group', b'my-topic',
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

    kafka = KafkaClient('localhost:9092')

    req = ProduceRequest(topic=b'my-topic', partition=1,
        messages=[create_message(b'some message')])
    resps = kafka.send_produce_request(payloads=[req], fail_on_error=True)
    kafka.close()

    resps[0].topic      # b'my-topic'
    resps[0].partition  # 1
    resps[0].error      # 0 (hopefully)
    resps[0].offset     # offset of the first message sent in this request
