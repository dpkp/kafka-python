#!/usr/bin/env python
import threading, logging, time

from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer

class Producer(threading.Thread):
    daemon = True

    def run(self):
        client = KafkaClient("localhost:9092")
        producer = SimpleProducer(client)

        while True:
            producer.send_messages('my-topic', "test")
            producer.send_messages('my-topic', "\xc2Hola, mundo!")

            time.sleep(1)


class Consumer(threading.Thread):
    daemon = True

    def run(self):
        client = KafkaClient("localhost:9092")
        consumer = SimpleConsumer(client, "test-group", "my-topic")

        for message in consumer:
            print(message)

def main():
    threads = [
        Producer(),
        Consumer()
    ]

    for t in threads:
        t.start()

    time.sleep(5)

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.DEBUG
        )
    main()
