
from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer


def main():
    kafka = KafkaClient("localhost:9092")

    producer = SimpleProducer(kafka)
    consumer = SimpleConsumer(kafka, "my-group", "activity.stream", max_buffer_size=None)

    producer.send_messages("activity.stream", "some message test")
    for message in consumer:
        print(message)

    kafka.close()

if __name__ == '__main__':
    main()
