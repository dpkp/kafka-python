# Kafka Python client

[![Build Status](https://travis-ci.org/mumrah/kafka-python.png)](https://travis-ci.org/mumrah/kafka-python)

This module provides low-level protocol support for Apache Kafka as well as
high-level consumer and producer classes. Request batching is supported by the
protocol as well as broker-aware request routing. Gzip and Snappy compression
is also supported for message sets.

Compatible with Apache Kafka 0.8.1

http://kafka.apache.org/

# License

Copyright 2013, David Arthur under Apache License, v2.0. See `LICENSE`

# Status

The current version of this package is **0.9.0** and is compatible with
Kafka brokers running version **0.8.1**.

# Usage

## High level

```python
from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer, KeyedProducer

kafka = KafkaClient("localhost:9092")

# To send messages synchronously
producer = SimpleProducer(kafka)
producer.send_messages("my-topic", "some message")
producer.send_messages("my-topic", "this method", "is variadic")

# To send messages asynchronously
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

response = producer.send_messages("my-topic", "async message")

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
consumer = SimpleConsumer(kafka, "my-group", "my-topic")
for message in consumer:
    print(message)

kafka.close()
```

## Keyed messages
```python
from kafka.client import KafkaClient
from kafka.producer import KeyedProducer
from kafka.partitioner import HashedPartitioner, RoundRobinPartitioner

kafka = KafkaClient("localhost:9092")

# HashedPartitioner is default
producer = KeyedProducer(kafka)
producer.send("my-topic", "key1", "some message")
producer.send("my-topic", "key2", "this methode")

producer = KeyedProducer(kafka, partitioner=RoundRobinPartitioner)
```

## Multiprocess consumer
```python
from kafka.client import KafkaClient
from kafka.consumer import MultiProcessConsumer

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
```

## Low level

```python
from kafka.client import KafkaClient
kafka = KafkaClient("localhost:9092")
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

Linux:
```shell
wget http://snappy.googlecode.com/files/snappy-1.0.5.tar.gz
tar xzvf snappy-1.0.5.tar.gz
cd snappy-1.0.5
./configure
make
sudo make install
```

OSX:
```shell
brew install snappy
```

Install the `python-snappy` module
```shell
pip install python-snappy
```

# Tests

## Run the unit tests

```shell
tox
```

## Run the integration tests

The integration tests will actually start up real local Zookeeper
instance and Kafka brokers, and send messages in using the client.

Note that you may want to add this to your global gitignore:
```shell
.gradle/
clients/build/
contrib/build/
contrib/hadoop-consumer/build/
contrib/hadoop-producer/build/
core/build/
core/data/
examples/build/
perf/build/
```

First, check out and the Kafka source:
```shell
git submodule update --init
./build_integration.sh
```

Then run the tests against supported Kafka versions:
```shell
    KAFKA_VERSION=0.8.0 tox
    KAFKA_VERSION=0.8.1 tox
```
