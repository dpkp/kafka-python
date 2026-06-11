kafka-python consumer
*********************

A line-oriented console consumer. Subscribes to one or more topics and
prints each received record to standard output.

Examples
========

.. code-block:: bash

    # Read every record on a topic until interrupted
    kafka-python consumer -b localhost:9092 -t my-topic

    # Join a consumer group and commit offsets automatically
    kafka-python consumer -b localhost:9092 -g my-group -t my-topic

    # Read from the beginning, then exit after 1s of idle
    kafka-python consumer -b localhost:9092 -t my-topic \
        -C auto_offset_reset=earliest \
        -C consumer_timeout_ms=1000

    # Print full ConsumerRecord (topic, partition, offset, key, value, ...)
    kafka-python consumer -b localhost:9092 -t my-topic -f full


Reference
=========

.. argparse::
   :module: kafka.cli.consumer
   :func: main_parser
   :prog: kafka-python consumer
