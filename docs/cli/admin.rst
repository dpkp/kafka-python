kafka-python admin
******************

``kafka-python admin`` exposes :class:`~kafka.KafkaAdminClient`
operations as a command-line tool. Commands are grouped by the kind of
resource they act on (topics, partitions, configs, ...). Each group
contains one or more subcommands.

Output is printed to stdout. ``--format raw`` (the default) uses
``pprint``; ``--format json`` emits a single JSON document, useful for
piping to ``jq`` or other tooling.

.. code-block:: bash

    kafka-python admin -b localhost:9092 cluster describe
    kafka-python admin -b localhost:9092 --format json topics list
    python -m kafka.admin -b localhost:9092 topics create -t foo --num-partitions 3


Global options
==============

.. argparse::
   :module: kafka.cli.admin
   :func: main_parser
   :prog: kafka-python admin
   :nosubcommands:


acls
====

.. argparse::
   :module: kafka.cli.admin
   :func: main_parser
   :prog: kafka-python admin
   :path: acls


cluster
=======

.. argparse::
   :module: kafka.cli.admin
   :func: main_parser
   :prog: kafka-python admin
   :path: cluster


configs
=======

.. argparse::
   :module: kafka.cli.admin
   :func: main_parser
   :prog: kafka-python admin
   :path: configs


topics
======

.. argparse::
   :module: kafka.cli.admin
   :func: main_parser
   :prog: kafka-python admin
   :path: topics


partitions
==========

.. argparse::
   :module: kafka.cli.admin
   :func: main_parser
   :prog: kafka-python admin
   :path: partitions


groups
======

.. argparse::
   :module: kafka.cli.admin
   :func: main_parser
   :prog: kafka-python admin
   :path: groups


transactions
============

KIP-664 administrative tools for inspecting and recovering from hanging
transactions. ``list``, ``describe``, and ``describe-producers`` require
broker >= 3.0 (``describe-producers`` works against broker >= 2.8).

.. argparse::
   :module: kafka.cli.admin
   :func: main_parser
   :prog: kafka-python admin
   :path: transactions


users
=====

SCRAM credential management. See KIP-554.

.. argparse::
   :module: kafka.cli.admin
   :func: main_parser
   :prog: kafka-python admin
   :path: users
