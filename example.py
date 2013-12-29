import logging

from kafka.client import KafkaClient, FetchRequest, ProduceRequest
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer

def produce_example(client):
    producer = SimpleProducer(client, "my-topic")
    producer.send_messages("test")

def consume_example(client):
    consumer = SimpleConsumer(client, "test-group", "my-topic")
    for message in consumer:
        print(message)

def main():
    client = KafkaClient("localhost:9092")
    produce_example(client)
    consume_example(client)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
