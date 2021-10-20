#!/usr/bin/env python
import threading, time

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
import sys
from os import environ

BOOTSTRAP_SERVERS = environ.get("KAFKA_BROKERS").split(',')
AWS_ACCESS_KEY_ID = environ.get("KAFKA_AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = environ.get("KAFKA_AWS_SECRET_ACCESS_KEY")
AWS_REGION = environ.get("KAFKA_AWS_REGION")

TOPIC_NAME = 'data.sandbox'
GROUP_NAME = 'data.sandbox'
SASL_MECHANISM = 'AWSMSKIAM'
SASL_PROTOCOL = 'SSL'
SASL_PROTOCOL = 'SASL_SSL'

class Producer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS,
                                 sasl_aws_msk_iam_access_key_id=AWS_ACCESS_KEY_ID,
                                 sasl_aws_msk_iam_secret_access_key=AWS_SECRET_ACCESS_KEY,
                                 sasl_aws_msk_region=AWS_REGION,
                                 security_protocol=SASL_PROTOCOL,
                                 sasl_mechanism=SASL_MECHANISM,
                                 )

        while not self.stop_event.is_set():
            producer.send(TOPIC_NAME, b"test")
            producer.send(TOPIC_NAME, b"\xc2Hola, mundo!")
            time.sleep(1)

        producer.close()


class Consumer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP_SERVERS,
                                 auto_offset_reset='earliest',
                                 consumer_timeout_ms=1000,
                                 sasl_aws_msk_iam_access_key_id=AWS_ACCESS_KEY_ID,
                                 sasl_aws_msk_iam_secret_access_key=AWS_SECRET_ACCESS_KEY,
                                 sasl_aws_msk_region=AWS_REGION,
                                 security_protocol=SASL_PROTOCOL,
                                 group_id=GROUP_NAME,
                                 sasl_mechanism=SASL_MECHANISM,
                                 )
        consumer.subscribe([TOPIC_NAME])

        while not self.stop_event.is_set():
            for message in consumer:
                print(f"consumer: {message}")
                if self.stop_event.is_set():
                    break

        consumer.close()


def main():
    # Create 'TOPIC_NAME' topic
    try:
        admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS,
                                 sasl_aws_msk_iam_access_key_id=AWS_ACCESS_KEY_ID,
                                 sasl_aws_msk_iam_secret_access_key=AWS_SECRET_ACCESS_KEY,
                                 sasl_aws_msk_region=AWS_REGION,
                                 security_protocol=SASL_PROTOCOL,
                                 sasl_mechanism=SASL_MECHANISM,
                                 )

        topic = NewTopic(name=TOPIC_NAME,
                         num_partitions=1,
                         replication_factor=1)
        #admin.create_topics([topic])
    except Exception as e:
        print(str(e), file=sys.stderr)

    tasks = [
        Producer(),
        #Consumer()
    ]

    # Start threads of a publisher/producer and a subscriber/consumer to 'my-topic' Kafka topic
    for t in tasks:
        t.start()

    time.sleep(10)

    # Stop threads
    for task in tasks:
        task.stop()

    for task in tasks:
        task.join()


if __name__ == "__main__":
    main()
