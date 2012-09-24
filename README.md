# Kakfa Python client

This module provides low-level protocol support Apache Kafka. It implements the five basic request types (and their responses): Produce, Fetch, MultiFetch, MultiProduce, and Offsets. 

Compatible with Apache Kafka 0.7x.

# License

Copyright 2012, David Arthur under Apache License, v2.0. See `LICENSE`

# Usage

## Send a message to a topic

You need to specify the topic and partition

```python
    kafka = KafkaClient("localhost", 9092)
    kafka.send_messages_simple("my-topic", 0, "some message")
    kafka.close()
```

## Send several messages to a topic

Same as before, just add more arguments to `send_simple`

```python
    kafka = KafkaClient("localhost", 9092)
    kafka.send_messages_simple("my-topic", 0, "some message", "another message", "and another")
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

This will only iterate through messages in the byte range of 
(0, 1024\*1024)
