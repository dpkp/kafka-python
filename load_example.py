#!/usr/bin/env python
import threading, logging, time, collections

from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer

msg_size = 524288

class Producer(threading.Thread):
    daemon = True
    big_msg = "1" * msg_size

    def run(self):
        client = KafkaClient("localhost:9092")
        producer = SimpleProducer(client)
        self.sent = 0

        while True:
            producer.send_messages('my-topic', self.big_msg)
            self.sent += 1


class Consumer(threading.Thread):
    daemon = True

    def run(self):
        client = KafkaClient("localhost:9092")
        consumer = SimpleConsumer(client, "test-group", "my-topic",
            max_buffer_size = None,
        )
        self.valid = 0
        self.invalid = 0

        for message in consumer:
            if len(message.message.value) == msg_size:
                self.valid += 1
            else:
                self.invalid += 1

def main():
    threads = [
        Producer(),
        Consumer()
    ]

    for t in threads:
        t.start()

    time.sleep(10)
    print 'Messages sent: %d' % threads[0].sent
    print 'Messages recvd: %d' % threads[1].valid
    print 'Messages invalid: %d' % threads[1].invalid

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.DEBUG
        )
    main()
