kafka-python
############

.. image:: https://img.shields.io/pypi/v/kafka-python.svg
    :target: https://pypi.org/project/kafka-python
.. image:: https://img.shields.io/badge/kafka-4.3--0.8-brightgreen.svg
    :target: https://kafka-python.readthedocs.io/en/master/compatibility.html

.. image:: https://img.shields.io/badge/license-Apache%202-blue.svg
    :target: https://github.com/dpkp/kafka-python/blob/master/LICENSE
.. image:: https://img.shields.io/pypi/pyversions/kafka-python.svg
    :target: https://pypi.python.org/pypi/kafka-python


kafka-python is a pure-python client library for Apache Kafka, the distributed
stream processing engine. It has no external dependencies and no Cython/C/rust
core, making installation across a wide variety of environments simple and easy
to manage.

kafka-python can also be used as a simple alternative to the apache kafka admin
scripts, which require an installed/compatible jvm. A simple CLI interface for
admin commands is provided as ``kafka-python admin`` / ``python -m kafka.admin``.

Users looking to add more raw throughput can ``pip install crc32c`` as
an optional dependency, offloading one of the most CPU intensive subsystems
to an optimized C library.


.. code-block:: bash

    pip install kafka-python

    # callable as module or as cli-script
    kafka-python admin -b localhost:9092 cluster describe

    # Create a topic with the admin cli
    python -m kafka.admin -b localhost:9092 topics create -t foo-topic

    # Produce messages
    echo "foo message" | python -m kafka.producer -b localhost:9092 -t foo-topic

    # Consume messages
    python -m kafka.consumer -b localhost:9092 -C auto_offset_reset=earliest -C consumer_timeout_ms=1000 -g foo-group -t foo-topic


What's New in 3.0
*****************

- Protocol Stack dynamically generated from Apache Kafka json message schemas.
- Encode/decode performance optimizations with compiled/cached python bytecode.
- Expanded KIP feature support, including Cooperative Rebalance (KIP-429),
  Rack-aware Fetch (KIP-392), Log-Truncation detection (KIP-320), Transactional
  Producer improvements (KIP-360, KIP-447, KIP-654), Sticky Partitioner (KIP-480),
  and splittting oversized producer batches (KIP-126).
- Full refactor and expansion of KafkaAdminClient.
- Networking changes to leverage kafka.net event-loop and async/await syntax.
- Python 3.8+ required

KafkaConsumer
*************

KafkaConsumer is a high-level message consumer, intended to operate as similarly
as possible to the official java client.
See https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html
for API and configuration details.

The consumer iterator returns ConsumerRecords, which are simple namedtuples
that expose basic message attributes: topic, partition, offset, key, and value:

.. code-block:: python

    from kafka import KafkaConsumer
    consumer = KafkaConsumer('my_favorite_topic')
    for msg in consumer:
        print (msg)

.. code-block:: python

    # join a consumer group for dynamic partition assignment and offset commits
    from kafka import KafkaConsumer
    consumer = KafkaConsumer('my_favorite_topic', group_id='my_favorite_group')
    for msg in consumer:
        print (msg)

.. code-block:: python

    # manually assign the partition list for the consumer
    from kafka import TopicPartition
    consumer = KafkaConsumer(bootstrap_servers='localhost:1234')
    consumer.assign([TopicPartition('foobar', 2)])
    msg = next(consumer)

.. code-block:: python

    # Deserialize msgpack-encoded values
    consumer = KafkaConsumer(value_deserializer=msgpack.loads)
    consumer.subscribe(['msgpackfoo'])
    for msg in consumer:
        assert isinstance(msg.value, dict)

.. code-block:: python

    # Access record headers. The returned value is a list of tuples
    # with str, bytes for key and value
    for msg in consumer:
        print (msg.headers)

.. code-block:: python

    # Read only committed messages from transactional topic
    consumer = KafkaConsumer(isolation_level='read_committed')
    consumer.subscribe(['txn_topic'])
    for msg in consumer:
        print(msg)

.. code-block:: python

    # Get consumer metrics
    metrics = consumer.metrics()


KafkaProducer
*************

KafkaProducer is a high-level, asynchronous message producer. The class is
intended to operate as similarly as possible to the official java client.
See https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html
for more details.

.. code-block:: python

    from kafka import KafkaProducer
    producer = KafkaProducer(bootstrap_servers='localhost:1234')
    for _ in range(100):
        producer.send('foobar', b'some_message_bytes')

.. code-block:: python

    # Block until a single message is sent (or timeout)
    future = producer.send('foobar', b'another_message')
    result = future.get(timeout=60)

.. code-block:: python

    # Block until all pending messages are at least put on the network
    # NOTE: This does not guarantee delivery or success! It is really
    # only useful if you configure internal batching using linger_ms
    producer.flush()

.. code-block:: python

    # Use a key for hashed-partitioning
    producer.send('foobar', key=b'foo', value=b'bar')

.. code-block:: python

    # Serialize json messages
    import json
    producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    producer.send('fizzbuzz', {'foo': 'bar'})

.. code-block:: python

    # Serialize string keys
    producer = KafkaProducer(key_serializer=str.encode)
    producer.send('flipflap', key='ping', value=b'1234')

.. code-block:: python

    # Compress messages
    producer = KafkaProducer(compression_type='gzip')
    for i in range(1000):
        producer.send('foobar', b'msg %d' % i)

.. code-block:: python

    # Use transactions
    producer = KafkaProducer(transactional_id='fizzbuzz')
    producer.init_transactions()
    producer.begin_transaction()
    future = producer.send('txn_topic', value=b'yes')
    future.get() # wait for successful produce
    producer.commit_transaction() # commit the transaction

    producer.begin_transaction()
    future = producer.send('txn_topic', value=b'no')
    future.get() # wait for successful produce
    producer.abort_transaction() # abort the transaction

.. code-block:: python

    # Include record headers. The format is list of tuples with string key
    # and bytes value.
    producer.send('foobar', value=b'c29tZSB2YWx1ZQ==', headers=[('content-encoding', b'base64')])

.. code-block:: python

    # Get producer performance metrics
    metrics = producer.metrics()


Module CLI Interface
********************

kafka-python also provides simple command-line interfaces for consumer, producer, and admin clients.
Access via ``python -m kafka.consumer``, ``python -m kafka.producer``, and ``python -m kafka.admin``.
See https://kafka-python.readthedocs.io/en/master/usage.html for more details.


Compression
***********

kafka-python supports the following compression formats:

- gzip (via stdlib)
- LZ4 (via `python-lz4`, `lz4tools`, or `py-lz4framed`)
- Snappy (via `python-snappy`)
- Zstandard (via `python-zstandard`)

gzip is supported natively, the others require installing additional libraries.
See https://kafka-python.readthedocs.io/en/master/install.html for more information.


Optimized CRC32 Validation
**************************

Kafka uses CRC32 checksums to validate messages. kafka-python includes a pure
python implementation for compatibility. To improve performance for high-throughput
applications, kafka-python will use `crc32c` for optimized native code if installed.
See https://kafka-python.readthedocs.io/en/master/install.html for installation instructions.
See https://pypi.org/project/crc32c/ for details on the underlying crc32c lib.


Protocol
********

A secondary goal of kafka-python is to provide an easy-to-use protocol layer
for interacting with kafka brokers via the python repl. This is useful for
testing, probing, and general experimentation. In version 3.0 the protocol
layer was re-written to generate encoder/decoder classes using json message
definitions imported directly from the Apache Kafka project source.


Debugging
*********

Use python's `logging` module to view internal operational events.
See https://docs.python.org/3/howto/logging.html for overview / howto.

.. code-block:: python

    import logging
    logging.basicConfig(level=logging.DEBUG)
