import logging

from kafka import Kafka081Client

def produce_example(client):
    producer = client.simple_producer()
    producer.send_messages('my-topic', "test")

def consume_example(client):
    consumer = client.simple_consumer("test-group", "my-topic")
    for message in consumer:
        print(message)

def main():
    client = Kafka081Client("localhost:9092")
    produce_example(client)
    consume_example(client)


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARN)
    main()
