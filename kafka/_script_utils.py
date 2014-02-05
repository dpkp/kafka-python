import logging
from optparse import OptionParser, Option, OptionValueError

from kafka import KafkaClient

logging.basicConfig()

# Add this attribute to easily check if the option is required
Option.ATTRS.append("required")

BROKER_OPTION = Option("-b", "--broker", dest="broker", required=True,
                       help="Required: The address of a kafka broker")
TOPIC_OPTION = Option("-t", "--topic", dest="topic", required=True,
                      help="Required: The topic to consume from")

def parse_options(*extra_options):
    parser = OptionParser()
    options = [BROKER_OPTION, TOPIC_OPTION] + list(extra_options)
    parser.add_options(options)
    (opts, args) = parser.parse_args()

    missing = [o._long_opts[0] for o in options
               if o.required and getattr(opts, o.dest) is None]
    if missing:
        parser.error("Missing required option(s) %s" % ", ".join(missing))

    return opts

def get_client(broker, client_id=KafkaClient.CLIENT_ID):
    try:
        (host, port) = broker.split(':')
    except ValueError:
        raise OptionValueError("Broker should be in the form 'host:port'")

    return KafkaClient(host, int(port), client_id)
