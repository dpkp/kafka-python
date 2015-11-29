For 0.8, we have correlation id so we can potentially interleave requests/responses

There are a few levels of abstraction:

* Protocol support: encode/decode the requests/responses
* Socket support: send/recieve messages
* API support: higher level APIs such as: get_topic_metadata


# Methods of producing

* Round robbin (each message to the next partition)
* All-to-one (each message to one partition)
* All-to-all? (each message to every partition)
* Partitioned (run each message through a partitioning function)
** HashPartitioned
** FunctionPartition

# Possible API

    client = KafkaClient("localhost:9092")

    producer = KafkaProducer(client, "topic")
    producer.send_string("hello")

    consumer = KafkaConsumer(client, "group", "topic")
    consumer.seek(10, 2) # seek to beginning (lowest offset)
    consumer.commit() # commit it
    for msg in consumer.iter_messages():
        print msg

# SSL-support
    # see https://docs.python.org/3/library/ssl.html for details:
    client = KafkaClient('localhost:9093', sslopts={
     'security.protocol': 'SSL',
     'keyfile': 'mykeyfile',
     'certfile': 'mycertfile',
     'ca_certs': 'myca_certs',
     'cert_reqs': ssl.CERT_NONE,
     'ssl_version': ssl.PROTOCOL_TLSv1,
     'ciphers': '<check the ssl-docs>',
     })
