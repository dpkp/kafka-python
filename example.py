import logging

from kafka import KafkaClient, FetchRequest, ProduceRequest
from kafka import create_message_from_string

def produce_example(kafka):
    message = create_message_from_string("test")
    request = ProduceRequest("my-topic", 0, [message])
    print("Sending %s" % str(request))
    kafka.send_message_set(request)

def consume_example(kafka):
    request = FetchRequest("my-topic", 0, 0, 64)
    print("Sending %s" % str(request))
    (messages, nextRequest) = kafka.get_message_set(request)
    print("Got %d messages:" % len(messages))
    for message in messages:
        print("\t%s" % message.payload)
    print("Next request %s" % str(nextRequest))

def main():
    kafka = KafkaClient("localhost", 9092)
    produce_example(kafka)
    consume_example(kafka)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
