# Kafka Python client

This module provides low-level protocol support for Apache Kafka as well as
high-level consumer and producer classes. Request batching is supported by the
protocol as well as broker-aware request routing. Gzip and Snappy compression
is also supported for message sets.

Compatible with Apache Kafka 0.8.0

http://kafka.apache.org/

# License

Copyright 2013, David Arthur under Apache License, v2.0. See `LICENSE`

# Status

I'm following the version numbers of Kafka, plus one number to indicate the 
version of this project. The current version is 0.8.0-1. This version is under
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

kafka.close()
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

Some of the tests will fail if Snappy is not installed. These tests will throw
NotImplementedError. If you see other failures, they might be bugs - so please
report them!

## Run the unit tests

_These are broken at the moment_

```shell
python -m test.unit
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

Next start up a ZooKeeper server on localhost:2181

```shell
/opt/zookeeper/bin/zkServer.sh start
```

This will actually start up real Kafka brokers and send messages in using the
client.
