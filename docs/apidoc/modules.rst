kafka-python API
****************

The :mod:`kafka` package exposes a small set of public classes for working
with Apache Kafka. The following sections group them by role; each class
links to its own API reference page.


kafka
=====

Application code typically interacts with one of three top-level clients.
Each owns a background IO thread and a shared async networking layer.

- :class:`~kafka.KafkaConsumer` — high-level, group-aware message consumer.
  Iterable, with manual or automatic offset commits, cooperative rebalance,
  pluggable deserializers, and transactional-read isolation.
- :class:`~kafka.KafkaProducer` — high-level, asynchronous message producer.
  Batches records into a background sender thread, with optional
  idempotence, transactions, compression, and pluggable serializers.
- :class:`~kafka.KafkaAdminClient` — admin operations: topic, ACL, config,
  consumer group, partition, quota, log-directory, and quorum management.

.. toctree::
   :maxdepth: 1
   :hidden:

   KafkaConsumer <KafkaConsumer>
   KafkaProducer <KafkaProducer>
   KafkaAdminClient <KafkaAdminClient>


kafka.net
=========

The clients share a single async networking layer (``kafka.net``). These
classes are exposed for advanced use cases — embedding the connection
pool, building a custom client on top of the kafka.net event loop, or
driving the protocol layer directly from the REPL.

- :mod:`~kafka.net.manager` — connection pool and
  high-level facade over the shared IO event loop. Each top-level client
  owns one.
- :mod:`~kafka.net.connection` — per-broker async
  connection: state machine, request/response correlation, and SASL
  handshake.
- :mod:`~kafka.net.transport` - Async socket I/O with write buffering,
  pause/resume hooks, and the asyncio-shaped protocol callback surface.
- :mod:`~kafka.net.inet` - DNS lookup + non-blocking connect, plus a
  URL-scheme registry that resolves ``proxy_url`` to socket factories.
- :mod:`~kafka.net.http_connect` - Tunnels broker connections through
  an HTTP CONNECT proxy (RFC 7231).
- :mod:`~kafka.net.socks5` - SOCKS5 client with optional username/password
  authentication.

.. toctree::
   :maxdepth: 1
   :hidden:

   manager <net/manager>
   connection <net/connection>
   transport <net/transport>
   inet <net/inet>
   http_connect <net/http_connect>
   socks5 <net/socks5>


other / misc
============

Lightweight data types used throughout the client APIs (and useful when
working with the lower-level protocol layer).

- :class:`~kafka.cluster.ClusterMetadata` — in-memory cache of brokers,
  topics, partitions, and the active controller. Refreshes itself on the
  shared IO thread.
- :class:`~kafka.TopicPartition` — namedtuple identifying a partition as
  ``(topic, partition)``.
- :class:`~kafka.OffsetAndMetadata` — committed-offset record
  ``(offset, metadata, leader_epoch)``.
- :class:`~kafka.OffsetSpec` - enum for partition offset queries.
- :class:`~kafka.IsolationLevel` - enum for transactional isolation.

.. toctree::
   :maxdepth: 1
   :hidden:

   ClusterMetadata <misc/ClusterMetadata>
   TopicPartition <misc/TopicPartition>
   OffsetAndMetadata <misc/OffsetAndMetadata>
   OffsetSpec <misc/OffsetSpec>
   IsolationLevel <misc/IsolationLevel>
