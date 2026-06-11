Command-Line Interface
**********************

kafka-python ships simple command-line interfaces for consumer, producer,
and admin clients. They can be invoked either as the ``kafka-python``
console script or as module entry points:

.. code-block:: bash

    kafka-python consumer -b localhost:9092 -t my-topic
    kafka-python producer -b localhost:9092 -t my-topic
    kafka-python admin    -b localhost:9092 cluster describe

    # equivalent module invocations
    python -m kafka.consumer -b localhost:9092 -t my-topic
    python -m kafka.producer -b localhost:9092 -t my-topic
    python -m kafka.admin    -b localhost:9092 cluster describe

The ``kafka-python admin`` command, in particular, is a convenient
alternative to the apache kafka ``bin/`` scripts when a compatible JVM is
not available.

.. toctree::
   :maxdepth: 2

   consumer
   producer
   admin


Common Options
==============

All three commands share a common set of connection, logging, and
configuration options. They are documented in full on the individual
command pages; the summary below highlights the most commonly used
flags.

Connection
----------

``-b/--bootstrap-servers HOST:PORT``
    One or more bootstrap servers used to discover the rest of the
    cluster. May be supplied multiple times.

``-S/--security-protocol``
    One of ``PLAINTEXT``, ``SSL``, ``SASL_PLAINTEXT``, ``SASL_SSL``.
    Defaults to ``PLAINTEXT``.

``-M/--sasl-mechanism``
    One of ``PLAIN``, ``GSSAPI``, ``OAUTHBEARER``, ``SCRAM-SHA-256``,
    ``SCRAM-SHA-512``. Defaults to ``PLAIN``.

``-U/--sasl-user`` / ``-P/--sasl-password``
    Credentials for SASL ``PLAIN`` and ``SCRAM-*`` mechanisms.

Logging
-------

``-l/--log-level``
    Python ``logging`` level (``DEBUG``, ``INFO``, ...). Defaults to
    ``CRITICAL`` so the CLI is quiet by default.

``-L/--enable-logger`` / ``-D/--disable-logger``
    Selectively turn on or off a single logger by name. Both flags may
    be supplied multiple times.

Extended Configuration
----------------------

``-C/--extra-config key=value``
    Pass arbitrary keyword arguments through to the underlying client
    constructor (:class:`~kafka.KafkaConsumer`,
    :class:`~kafka.KafkaProducer`, or
    :class:`~kafka.KafkaAdminClient`). Values that parse as ``int``,
    ``True``, ``False``, or ``None`` are converted; everything else is
    passed through as a string. May be supplied multiple times.

    .. code-block:: bash

        kafka-python consumer -b localhost:9092 -t foo \
            -C auto_offset_reset=earliest \
            -C consumer_timeout_ms=1000
