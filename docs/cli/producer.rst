kafka-python producer
*********************

A line-oriented console producer. Reads lines from standard input and
publishes each one as a record to the configured topic.

Examples
========

.. code-block:: bash

    # Publish a single message
    echo "hello kafka" | kafka-python producer -b localhost:9092 -t my-topic

    # Stream lines from a file
    kafka-python producer -b localhost:9092 -t my-topic < messages.txt


Reference
=========

.. argparse::
   :module: kafka.cli.producer
   :func: main_parser
   :prog: kafka-python producer
