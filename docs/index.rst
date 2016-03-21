kafka-python
============

This module provides low-level protocol support for Apache Kafka as well as
high-level consumer and producer classes. Request batching is supported by the
protocol as well as broker-aware request routing. Gzip and Snappy compression
is also supported for message sets.

Coordinated Consumer Group support is under development - see Issue #38.

On Freenode IRC at #kafka-python, as well as #apache-kafka

For general discussion of kafka-client design and implementation (not python specific),
see https://groups.google.com/forum/m/#!forum/kafka-clients

For information about Apache Kafka generally, see https://kafka.apache.org/

Status
------

The current stable version of this package is `0.9.5 <https://github.com/dpkp/kafka-python/releases/tag/v0.9.5>`_ and is compatible with:

Kafka broker versions

* 0.9.0.0
* 0.8.2.2
* 0.8.2.1
* 0.8.1.1
* 0.8.1
* 0.8.0

Python versions

* 3.5 (tested on 3.5.0)
* 3.4 (tested on 3.4.2)
* 3.3 (tested on 3.3.5)
* 2.7 (tested on 2.7.9)
* 2.6 (tested on 2.6.9)
* pypy (tested on pypy 2.5.0 / python 2.7.8)

License
-------

Apache License, v2.0. See `LICENSE <https://github.com/dpkp/kafka-python/blob/master/LICENSE>`_.

Copyright 2015, David Arthur, Dana Powers, and Contributors
(See `AUTHORS <https://github.com/dpkp/kafka-python/blob/master/AUTHORS.md>`_).


Contents
--------

.. toctree::
   :maxdepth: 2

   usage
   install
   tests
   API reference </apidoc/modules>

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
