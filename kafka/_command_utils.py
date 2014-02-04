import logging
from optparse import OptionParser, Option, OptionValueError

from kafka import KafkaClient

logging.basicConfig()

BROKER_OPTION = Option("-b", "--broker", dest="broker",
                       help="Address of a kafka broker")
TOPIC_OPTION = Option("-t", "--topic", dest="topic",
                      help="The topic to consume from")

def parse_options(*extra_options):
    parser = OptionParser()
    parser.add_options([BROKER_OPTION, TOPIC_OPTION] + list(extra_options))
    (opts, args) = parser.parse_args()
    return opts

def get_client(broker, client_id=KafkaClient.CLIENT_ID):
    try:
        (host, port) = broker.split(':')
    except ValueError:
        raise OptionValueError("Broker should be in the form 'host:port'")

    return KafkaClient(host, int(port), client_id)
