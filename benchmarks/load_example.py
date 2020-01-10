#!/usr/bin/env python
from __future__ import print_function
import threading, logging, time

from kafka import KafkaConsumer, KafkaProducer

msg_size = 524288

producer_stop = threading.Event()
consumer_stop = threading.Event()

class Producer(threading.Thread):
    big_msg = b'1' * msg_size

    def run(self):
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        self.sent = 0

        while not producer_stop.is_set():
            producer.send('my-topic', self.big_msg)
            self.sent += 1
        producer.flush()


class Consumer(threading.Thread):

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                                 auto_offset_reset='earliest')
        consumer.subscribe(['my-topic'])
        self.valid = 0
        self.invalid = 0

        for message in consumer:
            if len(message.value) == msg_size:
                self.valid += 1
            else:
                self.invalid += 1

            if consumer_stop.is_set():
                break

        consumer.close()

def main():
    threads = [
        Producer(),
        Consumer()
    ]

    for t in threads:
        t.start()

    time.sleep(10)
    producer_stop.set()
    consumer_stop.set()
    print('Messages sent: %d' % threads[0].sent)
    print('Messages recvd: %d' % threads[1].valid)
    print('Messages invalid: %d' % threads[1].invalid)

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
    main()
