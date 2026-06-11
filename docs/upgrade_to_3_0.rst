Upgrading from 2.3 to 3.0
=========================

kafka-python 3.0 is a major release with several breaking changes. This
guide walks through everything an application upgrading from 2.3 will
typically need to look at, in roughly the order of how likely it is to
matter.

Most applications using the public ``KafkaProducer`` / ``KafkaConsumer`` /
``KafkaAdminClient`` APIs at default settings should still work without
code changes. The biggest change is that kafka-python no longer works on
Python 2. Python 3.8 is now the minimum supported python interpreter.
Other use cases that will need changes are: catching
``NoBrokersAvailableError`` (error removed), implementing a custom
``'kafka_client'`` (internals have changed substantially), using a
non-default ``Serializer`` / ``Deserializer``, producer ``Partitioner``,
or consumer ``AbstractPartitionAssignor`` (minor abstract interface changes),
or using the ``'sasl_oauth_token_provider'`` configuration (import rename).

A full list of changes is in the :doc:`changelog`. This page covers only
the user-visible breaking changes and the most useful additions.


Python compatibility
--------------------

**Python 2 is no longer supported.** 3.0 requires Python 3.8 or newer. If
you still need Python 2.7, pin to ``kafka-python<3.0``; the 2.3.x line
remains compatible.


Configuration changes
---------------------

Producer defaults
^^^^^^^^^^^^^^^^^

``KafkaProducer`` now enables idempotence and full acks by default
(`KIP-679 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-679>`_):

- ``enable_idempotence`` now defaults to ``True`` (was ``False``).
- ``acks`` now effectively defaults to ``'all'`` (``-1``) (was ``1``).

These together mean the producer waits for full ISR acknowledgment and
deduplicates retries on the broker side, giving exactly-once-per-broker
delivery semantics out of the box.

To restore 2.3 behavior, pass ``enable_idempotence=False`` and let
``acks`` default, or set ``acks=1`` explicitly::

    KafkaProducer(
        bootstrap_servers=...,
        enable_idempotence=False,
        acks=1,
    )

If you explicitly pass ``acks=0`` or ``acks=1`` (or
``max_in_flight_requests_per_connection > 5``) without also passing
``enable_idempotence=False``, ``KafkaProducer`` raises
``KafkaConfigurationError`` rather than silently dropping idempotence.

Consumer ``session_timeout_ms``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The default consumer ``session_timeout_ms`` is now **45000** (45s),
up from 10000 (10s), tracking the upstream change in
`KIP-735 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-735>`_.
This reduces spurious rebalances under transient broker / network
disruptions. Set ``session_timeout_ms=10000`` to restore the old
default.

``api_version_auto_timeout_ms`` renamed to ``bootstrap_timeout_ms``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The config previously named ``api_version_auto_timeout_ms`` is now
``bootstrap_timeout_ms``, applies to the entire bootstrap process
(not just the API-version probe), and defaults to 30000 (30s).
The old name is no longer accepted.

``sasl_oauth_token_provider`` abstract baseclass / ``kafka.sasl`` module moved
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``kafka.sasl`` module has been moved to ``kafka.net.sasl``. Users that have
implemented an ``AbstractTokenProvider`` (or implemented a custom SASL mechanism)
will need to modify imports::

    from kafka.net.sasl.oauth import AbstractTokenProvider

``buffer_memory`` removed
^^^^^^^^^^^^^^^^^^^^^^^^^

The ``buffer_memory`` config has been removed from ``KafkaProducer``;
the deprecation warning emitted in 2.x is now a hard error. Use
``max_request_size`` (per-request bound) and let the accumulator manage
in-flight memory naturally.


Serializer / Deserializer interface
-----------------------------------

The ``Serializer`` and ``Deserializer`` abstract interfaces now receive
``headers`` alongside the topic and the payload::

    # old
    class MySerializer(Serializer):
        def serialize(self, topic, data):
            ...

    # new
    class MySerializer(Serializer):
        def serialize(self, topic, headers, data):
            ...

Old single-argument callables (or two-argument ``(topic, data)``
serializers) still work - ``KafkaProducer`` / ``KafkaConsumer`` wrap
them automatically - but emit a ``DeprecationWarning``. Update your
classes to accept the ``headers`` arg.

Two helper classes are now shipped: ``DefaultSerializer`` (a UTF-8
serializer/deserializer) and ``JsonSerializer``. Importable from
``kafka.serializer``. Both serialize/deserialize None<->None to
maintain expected key partitioning behavior.


Partitioner interface
---------------------

The partitioner contract changed shape. In 2.3 a partitioner was a
callable invoked as ``partitioner(serialized_key, all_partitions, available)``.
In 3.0 partitioners are instances of the ``kafka.partitioner.Partitioner``
ABC with a ``partition(...)`` method that receives the topic, both the
raw and serialized key / value, and the live ``cluster`` snapshot::

    # old (2.3) - a callable
    class MyPartitioner:
        def __call__(self, key, all_partitions, available):
            ...

    # new (3.0)
    from kafka.partitioner import Partitioner

    class MyPartitioner(Partitioner):
        def partition(self, topic, key, serialized_key, value, serialized_value, cluster):
            partitions = sorted(cluster.partitions_for_topic(topic))
            available = list(cluster.available_partitions_for_topic(topic))
            ...

Legacy callables that match the old shape still work (with a
``DeprecationWarning``); subclasses of the new ABC must implement the
new signature.

The default partitioner is still ``DefaultPartitioner`` (murmur2 hash
on the serialized key; null keys go to a random available partition) -
**unchanged** routing behavior for callers using the default.

A **sticky partitioner**
(`KIP-480 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-480>`_)
is now shipped and is opt-in: for records with a null key, it sticks to
one partition per topic until the current batch fills, then rotates.
Enable it by passing an instance::

    from kafka.partitioner import StickyPartitioner

    KafkaProducer(
        bootstrap_servers=...,
        partitioner=StickyPartitioner(),
    )

Keyed-record routing is unchanged under either partitioner.


Admin client API
----------------

Response shapes
^^^^^^^^^^^^^^^

Admin client methods now return plain ``dict`` / ``list`` structures
derived from the protocol response, instead of namedtuples or custom
response classes. If your code accesses fields by attribute
(``response.topics[0].name``), switch to dict access
(``response['topics'][0]['name']``).

``create_topics`` / ``create_partitions``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``create_topics`` now accepts any of the following:

- a list of topic name strings (uses broker defaults for partitions /
  replication on broker >= 2.4)
- a dict ``{name: {num_partitions, replication_factor, assignments, configs}}``
- a list of ``NewTopic`` instances (deprecated)

The dict form is the recommended shape going forward. ``NewTopic`` and
``NewPartitions`` are still exported and still work, but emit
deprecation warnings.

Group APIs renamed
^^^^^^^^^^^^^^^^^^

The group management APIs were renamed and expanded; consult the
:doc:`apidoc/KafkaAdminClient` reference for the current method names.
The most common 2.x names (``describe_consumer_groups``,
``list_consumer_groups``, ``delete_consumer_groups``) continue to work,
but were renamed to ``describe_groups``, ``list_groups``, etc...
New: ``list_group_offsets``, ``list_group_members``,
``reset_group_offsets`` (with extended options),
``remove_group_members``.


Consumer API
------------

``partition_assignment_strategy``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Assignor classes passed as ``partition_assignment_strategy`` are now
always instantiated before use. If you have a custom AbstractPartitionAssignor
class that uses @classmethod you will need to drop the decorators and update
to instance method definitions.

Incremental cooperative rebalance
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

KIP-429 cooperative rebalancing is now supported. Existing
``ConsumerRebalanceListener`` callbacks (``on_partitions_revoked`` /
``on_partitions_assigned``) continue to work. New:
``on_partitions_lost`` hook, ``AsyncConsumerRebalanceListener`` base
class for listeners that need to ``await`` rather than block, and
``RebalanceListener`` is now called on ``consumer.close()``.

Exceptions raised from a rebalance listener now propagate, instead of
being swallowed. Audit any custom listener code for accidental raises.


Error hierarchy
---------------

- ``KafkaError`` now subclasses ``Exception`` (was ``RuntimeError``).
  Code that catches ``RuntimeError`` to handle kafka-python errors will
  no longer match; catch ``KafkaError`` (or ``Exception``).
- ``IncompatibleBrokerVersion`` is now a subclass of
  ``UnsupportedVersionError``. Code catching either will keep working;
  code that distinguished them by type will not.
- ``NoBrokersAvailableError`` has been **removed**. Connection-failure
  scenarios that previously raised it now raise ``KafkaTimeoutError``
  (during bootstrap) or specific connection / authentication errors at
  send time.
- ``KafkaProtocolError`` is no longer marked retriable; if you used
  ``err.retriable`` to drive retry loops, this changes behavior on
  protocol-level errors.
- New base classes ``RetriableError`` and ``InvalidMetadataError`` are
  available for catching whole categories of errors at once.


Broker version checks
---------------------

kafka-python now relies exclusively on ApiVersionsRequest to determine
broker verions. For early brokers older than 0.10 you must now pass
an explicit ``api_version`` in order to connect.

In addition the configuration ``api_version_auto_timeout_ms=`` which
previously was used to manage timeouts during the broker version check
process has been removed. It has been replaced with the more general
``bootstrap_timeout_ms`` (see above).


Removed and relocated internals
-------------------------------

If your code imported from kafka-python internals (not the public top-
level API), several modules have moved or been removed:

- ``kafka.client_async`` and most of ``kafka.conn`` are **removed**.
  Their responsibilities now live in ``kafka.net`` (selector / event
  loop, connection pool, transport). A compatibility shim
  ``kafka.net.compat.KafkaNetClient`` exposes the legacy
  ``client.poll()`` / ``client.send()`` shape for callers that have
  not migrated.
- Users that implemented a custom ``kafka_client`` will need to evaluate
  independently. Due to the migration to ``kafka.net`` most internals
  no longer directly rely on the ``kafka_client`` interface, instead
  using the simpler ``kafka.net.manager`` connection manager and
  ``kafka.net.selector`` IO event loop.
- ``HeartbeatThread`` is gone; consumer heartbeats now run as an
  ``async def`` coroutine on a shared IO thread.
- The hand-written protocol classes in ``kafka.protocol.*`` have been
  replaced by classes generated from the upstream Apache Kafka JSON
  schemas. The legacy hand-written protocol types are still available
  under ``kafka.protocol.old`` for the small number of places that
  reach into protocol internals.

Version probes for pre-0.10 brokers have been removed. ``kafka-python``
3.0 always sends ``ApiVersionsRequest`` on connect and uses the result
to negotiate per-API versions. Brokers older than 0.10 no longer
auto-detect; pass ``api_version=(0, 9)`` (or your specific version)
explicitly if you need to talk to one.


New conveniences
----------------

A few additions that may be useful:

- **Context managers.** ``KafkaProducer``, ``KafkaConsumer``, and
  ``KafkaAdminClient`` all now support ``with`` syntax::

      with KafkaProducer(bootstrap_servers=...) as producer:
          producer.send('my-topic', b'hello')

- **CLI.** A single ``kafka-python`` entry point wraps the admin /
  consumer / producer subcommands (also runnable as
  ``python -m kafka.admin`` etc.). See :doc:`cli/index` for examples.
- **New public exports.** ``OffsetSpec`` and ``IsolationLevel`` are
  now importable directly from ``kafka``.
- **HTTP CONNECT proxy support.** Pass ``proxy_url='http://...'`` to
  any client to tunnel through an HTTP CONNECT proxy
  (`RFC 7231 S4.3.6 <https://datatracker.ietf.org/doc/html/rfc7231#section-4.3.6>`_).
- **TCP keepalive** is now enabled by default on broker sockets.
- **TLS minimum version** is now TLS 1.2; the default ``SSLContext``
  uses ``PROTOCOL_TLS_CLIENT``.


Upgrade checklist
-----------------

In order, the things most likely to bite an upgrading 2.3 application:

1. Make sure you are using Python 3.8+.
2. If you pass ``buffer_memory=`` to ``KafkaProducer``, remove it.
3. If you pass ``api_version_auto_timeout_ms=``, rename to
   ``bootstrap_timeout_ms``.
4. If your application can't tolerate idempotent / ``acks=all``
   semantics - typically because you explicitly want ``acks=0`` /
   ``acks=1`` - pass ``enable_idempotence=False`` to ``KafkaProducer``.
5. If you catch ``NoBrokersAvailableError``, replace it with
   ``KafkaTimeoutError``.
6. If you catch ``RuntimeError`` from kafka-python code, switch to
   ``KafkaError`` (or ``Exception``).
7. If you implement a custom ``Serializer`` / ``Deserializer``, update
   the signature to ``(self, topic, headers, data)``.
8. If you implement a custom ``Partitioner``, update the signature to
   ``(self, topic, key, serialized_key, value, serialized_value, cluster)``.
9. If you consume admin client responses by attribute access, switch to
   dict access.
10. If you import from ``kafka.client_async`` or ``kafka.conn``,
    migrate to ``kafka.net`` (or use the ``KafkaNetClient`` compat
    shim).
