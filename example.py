import logging

from kafka import KafkaClient, FetchRequest, ProduceRequest

def produce_example(kafka):
    message = kafka.create_message("testing")
    request = ProduceRequest("my-topic", -1, [message])
    kafka.send_message_set(request)

def consume_example(kafka):
    request = FetchRequest("my-topic", 0, 0, 1024)
    (messages, nextRequest) = kafka.get_message_set(request)
    for message in messages:
        print("Got Message: %s" % (message,))
    print(nextRequest)

def produce_gz_example(kafka):
    message = kafka.create_gzip_message("this message was gzipped", "along with this one")
    request = ProduceRequest("my-topic", 0, [message])
    kafka.send_message_set(request)

def main():
    kafka = KafkaClient("localhost", 9092)
    produce_example(kafka)
    produce_gz_example(kafka)
    consume_example(kafka)
    kafka.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
