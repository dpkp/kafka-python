Kafka Python client
------------------------

.. image:: https://img.shields.io/badge/kafka-0.9%2C%200.8.2%2C%200.8.1%2C%200.8-brightgreen.svg
    :target: https://kafka-python.readthedocs.org/compatibility.html
.. image:: https://img.shields.io/pypi/pyversions/kafka-python.svg
    :target: https://pypi.python.org/pypi/kafka-python
.. image:: https://coveralls.io/repos/dpkp/kafka-python/badge.svg?branch=master&service=github
    :target: https://coveralls.io/github/dpkp/kafka-python?branch=master
.. image:: https://travis-ci.org/dpkp/kafka-python.svg?branch=master
    :target: https://travis-ci.org/dpkp/kafka-python
.. image:: https://img.shields.io/badge/license-Apache%202-blue.svg
    :target: https://github.com/dpkp/kafka-python/blob/master/LICENSE

>>> pip install kafka-python

kafka-python is a client for the Apache Kafka distributed stream processing
system. It is designed to function much like the official java client, with a
sprinkling of pythonic interfaces (e.g., iterators).


KafkaConsumer
*************

>>> from kafka import KafkaConsumer
>>> consumer = KafkaConsumer('my_favorite_topic')
>>> for msg in consumer:
...     print (msg)

KafkaConsumer is a full-featured,
high-level message consumer class that is similar in design and function to the
new 0.9 java consumer. Most configuration parameters defined by the official
java client are supported as optional kwargs, with generally similar behavior.
Gzip and Snappy compressed messages are supported transparently.

In addition to the standard KafkaConsumer.poll() interface (which returns
micro-batches of messages, grouped by topic-partition), kafka-python supports
single-message iteration, yielding ConsumerRecord namedtuples, which include
the topic, partition, offset, key, and value of each message.

By default, KafkaConsumer will attempt to auto-commit
message offsets every 5 seconds. When used with 0.9 kafka brokers,
KafkaConsumer will dynamically assign partitions using
the kafka GroupCoordinator APIs and a RoundRobinPartitionAssignor
partitioning strategy, enabling relatively straightforward parallel consumption
patterns. See `ReadTheDocs <http://kafka-python.readthedocs.org/master/>`_
for examples.


KafkaProducer
*************

<`in progress - see SimpleProducer for legacy producer implementation`>


Protocol
********

A secondary goal of kafka-python is to provide an easy-to-use protocol layer
for interacting with kafka brokers via the python repl. This is useful for
testing, probing, and general experimentation. The protocol support is
leveraged to enable a KafkaClient.check_version() method that
probes a kafka broker and attempts to identify which version it is running
(0.8.0 to 0.9).


Low-level
*********

Legacy support is maintained for low-level consumer and producer classes,
SimpleConsumer and SimpleProducer. See
`ReadTheDocs <http://kafka-python.readthedocs.org/master/>`_ for API details.
