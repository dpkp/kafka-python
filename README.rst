Kafka Python client
------------------------

.. image:: https://img.shields.io/badge/kafka-0.11%2C%200.10%2C%200.9%2C%200.8-brightgreen.svg
    :target: https://kafka-python.readthedocs.io/compatibility.html
.. image:: https://img.shields.io/pypi/pyversions/kafka-python.svg
    :target: https://pypi.python.org/pypi/kafka-python
.. image:: https://coveralls.io/repos/dpkp/kafka-python/badge.svg?branch=master&service=github
    :target: https://coveralls.io/github/dpkp/kafka-python?branch=master
.. image:: https://travis-ci.org/dpkp/kafka-python.svg?branch=master
    :target: https://travis-ci.org/dpkp/kafka-python
.. image:: https://img.shields.io/badge/license-Apache%202-blue.svg
    :target: https://github.com/dpkp/kafka-python/blob/master/LICENSE

Python client for the Apache Kafka distributed stream processing system.
kafka-python is designed to function much like the official java client, with a
sprinkling of pythonic interfaces (e.g., consumer iterators).

kafka-python is best used with newer brokers (0.9+), but is backwards-compatible with
older versions (to 0.8.0). Some features will only be enabled on newer brokers.
For example, fully coordinated consumer groups -- i.e., dynamic partition
assignment to multiple consumers in the same group -- requires use of 0.9+ kafka
brokers. Supporting this feature for earlier broker releases would require
writing and maintaining custom leadership election and membership / health
check code (perhaps using zookeeper or consul). For older brokers, you can
achieve something similar by manually assigning different partitions to each
consumer instance with config management tools like chef, ansible, etc. This
approach will work fine, though it does not support rebalancing on failures.
See <https://kafka-python.readthedocs.io/en/master/compatibility.html>
for more details.

Please note that the master branch may contain unreleased features. For release
documentation, please see readthedocs and/or python's inline help.

>>> pip install kafka-python

KafkaConsumer
*************

KafkaConsumer is a high-level message consumer, intended to operate as similarly
as possible to the official java client. Full support for coordinated
consumer groups requires use of kafka brokers that support the Group APIs: kafka v0.9+.

See <https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html>
for API and configuration details.

The consumer iterator returns ConsumerRecords, which are simple namedtuples
that expose basic message attributes: topic, partition, offset, key, and value:

>>> from kafka import KafkaConsumer
>>> consumer = KafkaConsumer('my_favorite_topic')
>>> for msg in consumer:
...     print (msg)

>>> # join a consumer group for dynamic partition assignment and offset commits
>>> from kafka import KafkaConsumer
>>> consumer = KafkaConsumer('my_favorite_topic', group_id='my_favorite_group')
>>> for msg in consumer:
...     print (msg)

>>> # manually assign the partition list for the consumer
>>> from kafka import TopicPartition
>>> consumer = KafkaConsumer(bootstrap_servers='localhost:1234')
>>> consumer.assign([TopicPartition('foobar', 2)])
>>> msg = next(consumer)

>>> # Deserialize msgpack-encoded values
>>> consumer = KafkaConsumer(value_deserializer=msgpack.loads)
>>> consumer.subscribe(['msgpackfoo'])
>>> for msg in consumer:
...     assert isinstance(msg.value, dict)

>>> # Get consumer metrics
>>> metrics = consumer.metrics()

KafkaProducer
*************

KafkaProducer is a high-level, asynchronous message producer. The class is
intended to operate as similarly as possible to the official java client.
See <https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html>
for more details.

>>> from kafka import KafkaProducer
>>> producer = KafkaProducer(bootstrap_servers='localhost:1234')
>>> for _ in range(100):
...     producer.send('foobar', b'some_message_bytes')

>>> # Block until a single message is sent (or timeout)
>>> future = producer.send('foobar', b'another_message')
>>> result = future.get(timeout=60)

>>> # Block until all pending messages are at least put on the network
>>> # NOTE: This does not guarantee delivery or success! It is really
>>> # only useful if you configure internal batching using linger_ms
>>> producer.flush()

>>> # Use a key for hashed-partitioning
>>> producer.send('foobar', key=b'foo', value=b'bar')

>>> # Serialize json messages
>>> import json
>>> producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))
>>> producer.send('fizzbuzz', {'foo': 'bar'})

>>> # Serialize string keys
>>> producer = KafkaProducer(key_serializer=str.encode)
>>> producer.send('flipflap', key='ping', value=b'1234')

>>> # Compress messages
>>> producer = KafkaProducer(compression_type='gzip')
>>> for i in range(1000):
...     producer.send('foobar', b'msg %d' % i)

>>> # Get producer performance metrics
>>> metrics = producer.metrics()

Thread safety
*************

The KafkaProducer can be used across threads without issue, unlike the
KafkaConsumer which cannot.

While it is possible to use the KafkaConsumer in a thread-local manner,
multiprocessing is recommended.

Compression
***********

kafka-python supports gzip compression/decompression natively. To produce or consume lz4
compressed messages, you should install python-lz4 (pip install lz4).
To enable snappy compression/decompression install python-snappy (also requires snappy library).
See <https://kafka-python.readthedocs.io/en/master/install.html#optional-snappy-install>
for more information.

Protocol
********

A secondary goal of kafka-python is to provide an easy-to-use protocol layer
for interacting with kafka brokers via the python repl. This is useful for
testing, probing, and general experimentation. The protocol support is
leveraged to enable a KafkaClient.check_version() method that
probes a kafka broker and attempts to identify which version it is running
(0.8.0 to 0.11).

Low-level
*********

Legacy support is maintained for low-level consumer and producer classes,
SimpleConsumer and SimpleProducer. See
<https://kafka-python.readthedocs.io/en/master/simple.html?highlight=SimpleProducer> for API details.
