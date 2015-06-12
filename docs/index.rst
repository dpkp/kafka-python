kafka-python
============

This module provides low-level protocol support for Apache Kafka as well as
high-level consumer and producer classes. Request batching is supported by the
protocol as well as broker-aware request routing. Gzip and Snappy compression
is also supported for message sets.

http://kafka.apache.org/

On Freenode IRC at #kafka-python, as well as #apache-kafka

For general discussion of kafka-client design and implementation (not python specific),
see https://groups.google.com/forum/m/#!forum/kafka-clients

Status
------

The current stable version of this package is `0.9.4 <https://github.com/mumrah/kafka-python/releases/tag/v0.9.4>`_ and is compatible with:

Kafka broker versions

* 0.8.2.1 [offset management currently ZK only -- does not support ConsumerCoordinator offset management APIs]
* 0.8.1.1
* 0.8.1
* 0.8.0

Python versions

* 2.6 (tested on 2.6.9)
* 2.7 (tested on 2.7.9)
* 3.3 (tested on 3.3.5)
* 3.4 (tested on 3.4.2)
* pypy (tested on pypy 2.5.0 / python 2.7.8)

License
-------

Copyright 2015, David Arthur under Apache License, v2.0. See `LICENSE <https://github.com/mumrah/kafka-python/blob/master/LICENSE>`_.


Contents
--------

.. toctree::
   :maxdepth: 2

   install
   tests
   usage
   API reference </apidoc/modules>

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
