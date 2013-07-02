# Kafka Python client

This module provides low-level protocol support for Apache Kafka as well as
high-level consumer and producer classes. Request batching is supported by the
protocol as well as broker-aware request routing. Gzip and Snappy compression
is also supported for message sets.

Compatible with Apache Kafka 0.8.1

http://kafka.apache.org/

# License

Copyright 2013, David Arthur under Apache License, v2.0. See `LICENSE`

# Status

I'm following the version numbers of Kafka, plus one number to indicate the 
version of this project. The current version is 0.8.1-1. This version is under
development, APIs are subject to change.

# Usage

## High level

```python
from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer

kafka = KafkaClient("localhost", 9092)

producer = SimpleProducer(kafka, "my-topic")
producer.send_messages("some message")
producer.send_messages("this method", "is variadic")

consumer = SimpleConsumer(kafka, "my-group", "my-topic")
for message in consumer:
    print(message)

# Gevent based consumer
from kafka import KAFKA_GEVENT_DRIVER
consumer = SimpleConsumer(kafka, "my-group", "my-topic",
                          driver_type=KAFKA_GEVENT_DRIVER)

# Threaded consumer
from kafka import KAFKA_THREAD_DRIVER
consumer = SimpleConsumer(kafka, "my-group", "my-topic",
                          driver_type=KAFKA_THREAD_DRIVER)

kafka.close()
```

## Keyed messages
```python
from kafka.client import KafkaClient
from kafka.producer import KeyedProducer
from kafka.partitioner import HashedPartitioner, RoundRobinPartitioner

kafka = KafkaClient("localhost", 9092)

# HashedPartitioner is default
producer = KeyedProducer(kafka, "my-topic")
producer.send("key1", "some message")
producer.send("key2", "this methode")

producer = KeyedProducer(kafka, "my-topic", partitioner=RoundRobinPartitioner)
```

## Multiprocess consumer
```python
from kafka.consume import MultiConsumer

# This will split the number of partitions among two processes (drivers)
consumer = MultiConsumer(kafka, "my-topic", "my-group", num_drivers=2)

# This will spawn processes such that each handles 2 partitions max
consumer = MultiConsumer(kafka, "my-topic", "my-group",
                         partitions_per_driver=2)

for message in consumer:
    print(message)

for message in consumer.get_messages(count=5, block=True, timeout=4):
    print(message)

# Gevent based consumer
from kafka import KAFKA_GEVENT_DRIVER
consumer = MultiConsumer(kafka, "my-group", "my-topic", num_drivers=2,
                         driver_type=KAFKA_GEVENT_DRIVER)

# Threaded consumer
from kafka import KAFKA_THREAD_DRIVER
consumer = MultiConsumer(kafka, "my-group", "my-topic",
                         partitions_per_driver=2,
                         driver_type=KAFKA_THREAD_DRIVER)
```

## Low level

```python
from kafka.client import KafkaClient
kafka = KafkaClient("localhost", 9092)
req = ProduceRequest(topic="my-topic", partition=1,
    messages=[KafkaProdocol.encode_message("some message")])
resps = kafka.send_produce_request(payloads=[req], fail_on_error=True)
kafka.close()

resps[0].topic      # "my-topic"
resps[0].partition  # 1
resps[0].error      # 0 (hopefully)
resps[0].offset     # offset of the first message sent in this request
```

# Install

Install with your favorite package manager

Pip:

```shell
git clone https://github.com/mumrah/kafka-python
pip install ./kafka-python
```

Setuptools:
```shell
git clone https://github.com/mumrah/kafka-python
easy_install ./kafka-python
```

Using `setup.py` directly:
```shell
git clone https://github.com/mumrah/kafka-python
cd kafka-python
python setup.py install
```

## Optional Snappy install

Download and build Snappy from http://code.google.com/p/snappy/downloads/list

```shell
wget http://snappy.googlecode.com/files/snappy-1.0.5.tar.gz
tar xzvf snappy-1.0.5.tar.gz
cd snappy-1.0.5
./configure
make
sudo make install
```

Install the `python-snappy` module
```shell
pip install python-snappy
```

# Tests

## Run the unit tests

_These are broken at the moment_

```shell
tox ./test/test_unit.py
```

## Run the integration tests

First, checkout the Kafka source

```shell
git submodule init
git submodule update
cd kafka-src
./sbt update
./sbt package
```

And then run the tests. This will actually start up real local Zookeeper
instance and Kafka brokers, and send messages in using the client.

```shell
tox ./test/test_integration.py
```
