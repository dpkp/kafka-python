# Kafka Python client

[![Build Status](https://api.travis-ci.org/mumrah/kafka-python.png?branch=master)](https://travis-ci.org/mumrah/kafka-python)

This module provides low-level protocol support for Apache Kafka as well as
high-level consumer and producer classes. Request batching is supported by the
protocol as well as broker-aware request routing. Gzip and Snappy compression
is also supported for message sets.

http://kafka.apache.org/

On Freenode IRC at #kafka-python, as well as #apache-kafka

For general discussion of kafka-client design and implementation (not python specific),
see https://groups.google.com/forum/m/#!forum/kafka-clients

# License

Copyright 2014, David Arthur under Apache License, v2.0. See `LICENSE`

# Status

The current stable version of this package is [**0.9.2**](https://github.com/mumrah/kafka-python/releases/tag/v0.9.2) and is compatible with

Kafka broker versions
- 0.8.0
- 0.8.1
- 0.8.1.1

Python versions
- 2.6 (tested on 2.6.9)
- 2.7 (tested on 2.7.8)
- pypy (tested on pypy 2.3.1 / python 2.7.6)
- (Python 3.3 and 3.4 support has been added to trunk and will be available the next release)

# Usage

## High level

```python
from kafka import KafkaClient, SimpleProducer, SimpleConsumer

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
consumer = SimpleConsumer(kafka, "my-group", "my-topic")
for message in consumer:
    # message is raw byte string -- decode if necessary!
    # e.g., for unicode: `message.decode('utf-8')`
    print(message)

kafka.close()
```

## Keyed messages
```python
from kafka import KafkaClient, KeyedProducer, HashedPartitioner, RoundRobinPartitioner

kafka = KafkaClient("localhost:9092")

# HashedPartitioner is default
producer = KeyedProducer(kafka)
producer.send("my-topic", "key1", "some message")
producer.send("my-topic", "key2", "this methode")

producer = KeyedProducer(kafka, partitioner=RoundRobinPartitioner)
```

## Multiprocess consumer
```python
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
```

## Low level

```python
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
```

# Install

Install with your favorite package manager

## Latest Release
Pip:

```shell
pip install kafka-python
```

Releases are also listed at https://github.com/mumrah/kafka-python/releases


## Bleeding-Edge
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

### Install Development Libraries
Download and build Snappy from http://code.google.com/p/snappy/downloads/list

Ubuntu:
```shell
apt-get install libsnappy-dev
```

OSX:
```shell
brew install snappy
```

From Source:
```shell
wget http://snappy.googlecode.com/files/snappy-1.0.5.tar.gz
tar xzvf snappy-1.0.5.tar.gz
cd snappy-1.0.5
./configure
make
sudo make install
```

### Install Python Module
Install the `python-snappy` module
```shell
pip install python-snappy
```

# Tests

## Run the unit tests

```shell
tox
```

## Run a subset of unit tests
```shell
# run protocol tests only
tox -- -v test.test_protocol
```

```shell
# test with pypy only
tox -e pypy
```

```shell
# Run only 1 test, and use python 2.7
tox -e py27 -- -v --with-id --collect-only
# pick a test number from the list like #102
tox -e py27 -- -v --with-id 102
```

## Run the integration tests

The integration tests will actually start up real local Zookeeper
instance and Kafka brokers, and send messages in using the client.

First, get the kafka binaries for integration testing:
```shell
./build_integration.sh
```
By default, the build_integration.sh script will download binary
distributions for all supported kafka versions.
To test against the latest source build, set KAFKA_VERSION=trunk
and optionally set SCALA_VERSION (defaults to 2.8.0, but 2.10.1 is recommended)
```shell
SCALA_VERSION=2.10.1 KAFKA_VERSION=trunk ./build_integration.sh
```

Then run the tests against supported Kafka versions, simply set the `KAFKA_VERSION`
env variable to the server build you want to use for testing:
```shell
KAFKA_VERSION=0.8.0 tox
KAFKA_VERSION=0.8.1 tox
KAFKA_VERSION=0.8.1.1 tox
KAFKA_VERSION=trunk tox
```
