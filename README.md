# Kakfa Python client

This module provides low-level protocol support Apache Kafka. It implements the five basic request types 
(and their responses): Produce, Fetch, MultiFetch, MultiProduce, and Offsets. Gzip and Snappy compression
is also supported.

Compatible with Apache Kafka 0.7x. Tested against 0.7.0, 0.7.1, and 0.7.2

http://incubator.apache.org/kafka/

# License

Copyright 2012, David Arthur under Apache License, v2.0. See `LICENSE`

# Status

Current version is 0.1-alpha. The current API should be pretty stable.

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

Some of the tests will fail if Snappy is not installed. These tests will throw NotImplementedError. If you see other failures,
they might be bugs - so please report them!

## Run the unit tests

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

Then from the root directory, run the integration tests

```shell
python -m test.integration
```

# Usage

## Send a message to a topic

```python
from kafka.client import KafkaClient
kafka = KafkaClient("localhost", 9092)
kafka.send_messages_simple("my-topic", "some message")
kafka.close()
```

## Send several messages to a topic

Same as before, just add more arguments to `send_simple`

```python
kafka = KafkaClient("localhost", 9092)
kafka.send_messages_simple("my-topic", "some message", "another message", "and another")
kafka.close()
```

## Recieve some messages from a topic

Supply `get_message_set` with a `FetchRequest`, get back the messages and new `FetchRequest`

```python
kafka = KafkaClient("localhost", 9092)
req = FetchRequest("my-topic", 0, 0, 1024*1024)
(messages, req1) = kafka.get_message_set(req)
kafka.close()
```

The returned `FetchRequest` includes the offset of the next message. This makes 
paging through the queue very simple.

## Send multiple messages to multiple topics

For this we use the `send_multi_message_set` method along with `ProduceRequest` objects.

```python
kafka = KafkaClient("localhost", 9092)
req1 = ProduceRequest("my-topic-1", 0, [
    create_message_from_string("message one"),
    create_message_from_string("message two")
])
req2 = ProduceRequest("my-topic-2", 0, [
    create_message_from_string("nachricht ein"),
    create_message_from_string("nachricht zwei")
])
kafka.sent_multi_message_set([req1, req1])
kafka.close()
```

## Iterate through all messages from an offset

The `iter_messages` method will make the underlying calls to `get_message_set`
to provide a generator that returns every message available.

```python
kafka = KafkaClient("localhost", 9092)
for msg in kafka.iter_messages(FetchRequest("my-topic", 0, 0, 1024*1024)):
    print(msg.payload)
kafka.close()
```

An optional `auto` argument will control auto-paging through results

```python
kafka = KafkaClient("localhost", 9092)
for msg in kafka.iter_messages(FetchRequest("my-topic", 0, 0, 1024*1024), False):
    print(msg.payload)
kafka.close()
```
This will only iterate through messages in the byte range of (0, 1024\*1024)

## Create some compressed messages

```python
kafka = KafkaClient("localhost", 9092)
messages = [kafka.create_snappy_message("testing 1"),
            kafka.create_snappy_message("testing 2")]
req = ProduceRequest(topic, 1, messages)
kafka.send_message_set(req)
kafka.close()
```

## Use Kafka like a FIFO queue

Simple API: `get`, `put`, `close`.

```python
kafka = KafkaClient("localhost", 9092)
q = KafkaQueue(kafka, "my-topic", [0,1])
q.put("first")
q.put("second")
q.get() # first
q.get() # second
q.close()
kafka.close()
```

Since the producer and consumers are backed by actual `multiprocessing.Queue`, you can 
do blocking or non-blocking puts and gets.

```python
q.put("first", block=False)
q.get(block=True, timeout=10)
```
