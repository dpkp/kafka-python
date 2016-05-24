# 1.2.0 (May 24, 2016)

This release officially adds support for Kafka 0.10
* Add protocol support for ApiVersionRequest (dpkp PR 678)
* KAFKA-3025: Message v1 -- add timetamp and relative offsets (dpkp PR 693)
* Use Fetch/Produce API v2 for brokers >= 0.10 (uses message format v1) (dpkp PR 694)
* Use standard LZ4 framing for v1 messages / kafka 0.10 (dpkp PR 695)

Consumers
* Update SimpleConsumer / legacy protocol to handle compressed messages (paulcavallaro PR 684)

Producers
* KAFKA-3388: Fix expiration of batches sitting in the accumulator (dpkp PR 699)
* KAFKA-3197: when max.in.flight.request.per.connection = 1, attempt to guarantee ordering (dpkp PR 698)
* Dont use soon-to-be-reserved keyword await as function name (FutureProduceResult) (dpkp PR 697)

Clients
* Fix socket leaks in KafkaClient (dpkp PR 696)

Documentation
<none>

Internals
* Support SSL CRL [requires python 2.7.9+ / 3.4+] (vincentbernat PR 683)
* Use original hostname for SSL checks (vincentbernat PR 682)
* Always pass encoded message bytes to MessageSet.encode()
* Raise ValueError on protocol encode/decode errors
* Supplement socket.gaierror exception in BrokerConnection.connect() (erikbeebe PR 687)
* BrokerConnection check_version: expect 0.9 to fail with CorrelationIdError
* Fix small bug in Sensor (zackdever PR 679)


# 1.1.1 (Apr 26, 2016)

quick bugfixes
* fix throttle_time_ms sensor handling (zackdever pr 667)
* improve handling of disconnected sockets (easypost pr 666 / dpkp)
* disable standard metadata refresh triggers during bootstrap (dpkp)
* more predictable future callback/errback exceptions (zackdever pr 670)
* avoid some exceptions in coordinator.__del__ (dpkp pr 668)


# 1.1.0 (Apr 25, 2016)

Consumers
* Avoid resending FetchRequests that are pending on internal queue
* Log debug messages when skipping fetched messages due to offset checks
* KAFKA-3013: Include topic-partition in exception for expired batches
* KAFKA-3318: clean up consumer logging and error messages
* Improve unknown coordinator error handling
* Improve auto-commit error handling when group_id is None
* Add paused() API (zackdever PR 602)
* Add default_offset_commit_callback to KafkaConsumer DEFAULT_CONFIGS

Producers
<none>

Clients
* Support SSL connections
* Use selectors module for non-blocking IO
* Refactor KafkaClient connection management
* Fix AttributeError in __del__
* SimpleClient: catch errors thrown by _get_leader_for_partition (zackdever PR 606)

Documentation
* Fix serializer/deserializer examples in README
* Update max.block.ms docstring
* Remove errant next(consumer) from consumer documentation
* Add producer.flush() to usage docs

Internals
* Add initial metrics implementation (zackdever PR 637)
* KAFKA-2136: support Fetch and Produce v1 (throttle_time_ms)
* Use version-indexed lists for request/response protocol structs (dpkp PR 630)
* Split kafka.common into kafka.structs and kafka.errors
* Handle partial socket send() (dpkp PR 611)
* Fix windows support (dpkp PR 603)
* IPv6 support (TimEvens PR 615; Roguelazer PR 642)


# 1.0.2 (Mar 14, 2016)

Consumers
* Improve KafkaConsumer Heartbeat handling (dpkp PR 583)
* Fix KafkaConsumer.position bug (stefanth PR 578)
* Raise TypeError when partition is not a TopicPartition (dpkp PR 587)
* KafkaConsumer.poll should sleep to prevent tight-loops (dpkp PR 597)

Producers
* Fix producer threading bug that can crash sender (dpkp PR 590)
* Fix bug in producer buffer pool reallocation (dpkp PR 585)
* Remove spurious warnings when closing sync SimpleProducer (twm PR 567)
* Fix FutureProduceResult.await() on python2.6 (dpkp)
* Add optional timeout parameter to KafkaProducer.flush() (dpkp)
* KafkaProducer Optimizations (zackdever PR 598)

Clients
* Improve error handling in SimpleClient.load_metadata_for_topics (dpkp)
* Improve handling of KafkaClient.least_loaded_node failure (dpkp PR 588)

Documentation
* Fix KafkaError import error in docs (shichao-an PR 564)
* Fix serializer / deserializer examples (scribu PR 573)

Internals
* Update to Kafka 0.9.0.1 for integration testing
* Fix ifr.future.failure in conn.py (mortenlj PR 566)
* Improve Zookeeper / Kafka Fixture management (dpkp)


# 1.0.1 (Feb 19, 2016)

Consumers
* Add RangePartitionAssignor (and use as default); add assignor tests (dpkp PR 550)
* Make sure all consumers are in same generation before stopping group test
* Verify node ready before sending offset fetch request from coordinator
* Improve warning when offset fetch request returns unknown topic / partition

Producers
* Warn if pending batches failed during flush
* Fix concurrency bug in RecordAccumulator.ready()
* Fix bug in SimpleBufferPool memory condition waiting / timeout
* Support batch_size = 0 in producer buffers (dpkp PR 558)
* Catch duplicate batch.done() calls [e.g., maybe_expire then a response errback]

Clients

Documentation
* Improve kafka.cluster docstrings
* Migrate load_example.py to KafkaProducer / KafkaConsumer

Internals
* Dont override system rcvbuf or sndbuf unless configured explicitly (dpkp PR 557)
* Some attributes may not exist in __del__ if we failed assertions
* Break up some circular references and close client wake pipes on __del__ (aisch PR 554)


# 1.0.0 (Feb 15, 2016)

This release includes significant code changes. Users of older kafka-python
versions are encouraged to test upgrades before deploying to production as
some interfaces and configuration options have changed.

Users of SimpleConsumer / SimpleProducer / SimpleClient (formerly KafkaClient)
from prior releases should migrate to KafkaConsumer / KafkaProducer. Low-level
APIs (Simple*) are no longer being actively maintained and will be removed in a
future release.

For comprehensive API documentation, please see python help() / docstrings,
kafka-python.readthedocs.org, or run `tox -e docs` from source to build
documentation locally.

Consumers
* KafkaConsumer re-written to emulate the new 0.9 kafka consumer (java client)
  and support coordinated consumer groups (feature requires >= 0.9.0.0 brokers)

  * Methods no longer available:

    * configure [initialize a new consumer instead]
    * set_topic_partitions [use subscribe() or assign()]
    * fetch_messages [use poll() or iterator interface]
    * get_partition_offsets
    * offsets [use committed(partition)]
    * task_done [handled internally by auto-commit; or commit offsets manually]

  * Configuration changes (consistent with updated java client):

    * lots of new configuration parameters -- see docs for details
    * auto_offset_reset: previously values were 'smallest' or 'largest', now
      values are 'earliest' or 'latest'
    * fetch_wait_max_ms is now fetch_max_wait_ms
    * max_partition_fetch_bytes is now max_partition_fetch_bytes
    * deserializer_class is now value_deserializer and key_deserializer
    * auto_commit_enable is now enable_auto_commit
    * auto_commit_interval_messages was removed
    * socket_timeout_ms was removed
    * refresh_leader_backoff_ms was removed

* SimpleConsumer and MultiProcessConsumer are now deprecated and will be removed
  in a future release. Users are encouraged to migrate to KafkaConsumer.

Producers
* new producer class: KafkaProducer. Exposes the same interface as official java client.
  Async by default; returned future.get() can be called for synchronous blocking
* SimpleProducer is now deprecated and will be removed in a future release. Users are
  encouraged to migrate to KafkaProducer.

Clients
* synchronous KafkaClient renamed to SimpleClient. For backwards compatibility, you
  will get a SimpleClient via `from kafka import KafkaClient`. This will change in
  a future release.
* All client calls use non-blocking IO under the hood.
* Add probe method check_version() to infer broker versions.

Documentation
* Updated README and sphinx documentation to address new classes.
* Docstring improvements to make python help() easier to use.

Internals
* Old protocol stack is deprecated. It has been moved to kafka.protocol.legacy
  and may be removed in a future release.
* Protocol layer re-written using Type classes, Schemas and Structs (modeled on
  the java client).
* Add support for LZ4 compression (including broken framing header checksum).


# 0.9.5 (Dec 6, 2015)

Consumers
* Initial support for consumer coordinator: offsets only (toddpalino PR 420)
* Allow blocking until some messages are received in SimpleConsumer (saaros PR 457)
* Support subclass config changes in KafkaConsumer (zackdever PR 446)
* Support retry semantics in MultiProcessConsumer (barricadeio PR 456)
* Support partition_info in MultiProcessConsumer (scrapinghub PR 418)
* Enable seek() to an absolute offset in SimpleConsumer (haosdent PR 412)
* Add KafkaConsumer.close() (ucarion PR 426)

Producers
* Catch client.reinit() exceptions in async producer (dpkp)
* Producer.stop() now blocks until async thread completes (dpkp PR 485)
* Catch errors during load_metadata_for_topics in async producer (bschopman PR 467)
* Add compression-level support for codecs that support it (trbs PR 454)
* Fix translation of Java murmur2 code, fix byte encoding for Python 3 (chrischamberlin PR 439)
* Only call stop() on not-stopped producer objects (docker-hub PR 435)
* Allow null payload for deletion feature (scrapinghub PR 409)

Clients
* Use non-blocking io for broker aware requests (ecanzonieri PR 473)
* Use debug logging level for metadata request (ecanzonieri PR 415)
* Catch KafkaUnavailableError in _send_broker_aware_request (mutability PR 436)
* Lower logging level on replica not available and commit (ecanzonieri PR 415)

Documentation
* Update docs and links wrt maintainer change (mumrah -> dpkp)

Internals
* Add py35 to tox testing
* Update travis config to use container infrastructure
* Add 0.8.2.2 and 0.9.0.0 resources for integration tests; update default official releases
* new pylint disables for pylint 1.5.1 (zackdever PR 481)
* Fix python3 / python2 comments re queue/Queue (dpkp)
* Add Murmur2Partitioner to kafka __all__ imports (dpkp Issue 471)
* Include LICENSE in PyPI sdist (koobs PR 441)

# 0.9.4 (June 11, 2015)

Consumers
* Refactor SimpleConsumer internal fetch handling (dpkp PR 399)
* Handle exceptions in SimpleConsumer commit() and reset_partition_offset() (dpkp PR 404)
* Improve FailedPayloadsError handling in KafkaConsumer (dpkp PR 398)
* KafkaConsumer: avoid raising KeyError in task_done (dpkp PR 389)
* MultiProcessConsumer -- support configured partitions list (dpkp PR 380)
* Fix SimpleConsumer leadership change handling (dpkp PR 393) 
* Fix SimpleConsumer connection error handling (reAsOn2010 PR 392)
* Improve Consumer handling of 'falsy' partition values (wting PR 342)
* Fix _offsets call error in KafkaConsumer (hellais PR 376)
* Fix str/bytes bug in KafkaConsumer (dpkp PR 365)
* Register atexit handlers for consumer and producer thread/multiprocess cleanup (dpkp PR 360)
* Always fetch commit offsets in base consumer unless group is None (dpkp PR 356)
* Stop consumer threads on delete (dpkp PR 357)
* Deprecate metadata_broker_list in favor of bootstrap_servers in KafkaConsumer (dpkp PR 340)
* Support pass-through parameters in multiprocess consumer (scrapinghub PR 336)
* Enable offset commit on SimpleConsumer.seek (ecanzonieri PR 350)
* Improve multiprocess consumer partition distribution (scrapinghub PR 335)
* Ignore messages with offset less than requested (wkiser PR 328)
* Handle OffsetOutOfRange in SimpleConsumer (ecanzonieri PR 296)

Producers
* Add Murmur2Partitioner (dpkp PR 378)
* Log error types in SimpleProducer and SimpleConsumer (dpkp PR 405)
* SimpleProducer support configuration of fail_on_error (dpkp PR 396)
* Deprecate KeyedProducer.send() (dpkp PR 379)
* Further improvements to async producer code (dpkp PR 388)
* Add more configuration parameters for async producer (dpkp)
* Deprecate SimpleProducer batch_send=True in favor of async (dpkp)
* Improve async producer error handling and retry logic (vshlapakov PR 331)
* Support message keys in async producer (vshlapakov PR 329)
* Use threading instead of multiprocessing for Async Producer (vshlapakov PR 330)
* Stop threads on __del__ (chmduquesne PR 324)
* Fix leadership failover handling in KeyedProducer (dpkp PR 314)

KafkaClient
* Add .topics property for list of known topics (dpkp)
* Fix request / response order guarantee bug in KafkaClient (dpkp PR 403)
* Improve KafkaClient handling of connection failures in _get_conn (dpkp)
* Client clears local metadata cache before updating from server (dpkp PR 367)
* KafkaClient should return a response or error for each request - enable better retry handling (dpkp PR 366)
* Improve str/bytes conversion in KafkaClient and KafkaConsumer (dpkp PR 332)
* Always return sorted partition ids in client.get_partition_ids_for_topic() (dpkp PR 315)

Documentation
* Cleanup Usage Documentation
* Improve KafkaConsumer documentation (dpkp PR 341)
* Update consumer documentation (sontek PR 317)
* Add doc configuration for tox (sontek PR 316)
* Switch to .rst doc format (sontek PR 321)
* Fixup google groups link in README (sontek PR 320)
* Automate documentation at kafka-python.readthedocs.org

Internals
* Switch integration testing from 0.8.2.0 to 0.8.2.1 (dpkp PR 402)
* Fix most flaky tests, improve debug logging, improve fixture handling (dpkp)
* General style cleanups (dpkp PR 394)
* Raise error on duplicate topic-partition payloads in protocol grouping (dpkp)
* Use module-level loggers instead of simply 'kafka' (dpkp)
* Remove pkg_resources check for __version__ at runtime (dpkp PR 387)
* Make external API consistently support python3 strings for topic (kecaps PR 361)
* Fix correlation id overflow (dpkp PR 355)
* Cleanup kafka/common structs (dpkp PR 338)
* Use context managers in gzip_encode / gzip_decode (dpkp PR 337)
* Save failed request as FailedPayloadsError attribute (jobevers PR 302)
* Remove unused kafka.queue (mumrah)

# 0.9.3 (Feb 3, 2015)

* Add coveralls.io support (sontek PR 307)
* Fix python2.6 threading.Event bug in ReentrantTimer (dpkp PR 312)
* Add kafka 0.8.2.0 to travis integration tests (dpkp PR 310)
* Auto-convert topics to utf-8 bytes in Producer (sontek PR 306)
* Fix reference cycle between SimpleConsumer and ReentrantTimer (zhaopengzp PR 309)
* Add Sphinx API docs (wedaly PR 282)
* Handle additional error cases exposed by 0.8.2.0 kafka server (dpkp PR 295)
* Refactor error class management (alexcb PR 289)
* Expose KafkaConsumer in __all__ for easy imports (Dinoshauer PR 286)
* SimpleProducer starts on random partition by default (alexcb PR 288)
* Add keys to compressed messages (meandthewallaby PR 281)
* Add new high-level KafkaConsumer class based on java client api (dpkp PR 234)
* Add KeyedProducer.send_messages api (pubnub PR 277)
* Fix consumer pending() method (jettify PR 276)
* Update low-level demo in README (sunisdown PR 274)
* Include key in KeyedProducer messages (se7entyse7en PR 268)
* Fix SimpleConsumer timeout behavior in get_messages (dpkp PR 238)
* Fix error in consumer.py test against max_buffer_size (rthille/wizzat PR 225/242)
* Improve string concat performance on pypy / py3 (dpkp PR 233)
* Reorg directory layout for consumer/producer/partitioners (dpkp/wizzat PR 232/243)
* Add OffsetCommitContext (locationlabs PR 217)
* Metadata Refactor (dpkp  PR 223)
* Add Python 3 support (brutasse/wizzat - PR 227)
* Minor cleanups - imports / README / PyPI classifiers (dpkp - PR 221)
* Fix socket test (dpkp - PR 222)
* Fix exception catching bug in test_failover_integration (zever - PR 216)

# 0.9.2 (Aug 26, 2014)

* Warn users that async producer does not reliably handle failures (dpkp - PR 213)
* Fix spurious ConsumerFetchSizeTooSmall error in consumer (DataDog - PR 136)
* Use PyLint for static error checking (dpkp - PR 208)
* Strictly enforce str message type in producer.send_messages (dpkp - PR 211)
* Add test timers via nose-timer plugin; list 10 slowest timings by default (dpkp)
* Move fetching last known offset logic to a stand alone function (zever - PR 177)
* Improve KafkaConnection and add more tests (dpkp - PR 196)
* Raise TypeError if necessary when encoding strings (mdaniel - PR 204) 
* Use Travis-CI to publish tagged releases to pypi (tkuhlman / mumrah)
* Use official binary tarballs for integration tests and parallelize travis tests (dpkp - PR 193)
* Improve new-topic creation handling (wizzat - PR 174)

# 0.9.1 (Aug 10, 2014)

* Add codec parameter to Producers to enable compression (patricklucas - PR 166)
* Support IPv6 hosts and network (snaury - PR 169)
* Remove dependency on distribute (patricklucas - PR 163)
* Fix connection error timeout and improve tests (wizzat - PR 158)
* SimpleProducer randomization of initial round robin ordering (alexcb - PR 139)
* Fix connection timeout in KafkaClient and KafkaConnection (maciejkula - PR 161)
* Fix seek + commit behavior (wizzat - PR 148) 


# 0.9.0 (Mar 21, 2014)

* Connection refactor and test fixes (wizzat - PR 134)
* Fix when partition has no leader (mrtheb - PR 109)
* Change Producer API to take topic as send argument, not as instance variable (rdiomar - PR 111)
* Substantial refactor and Test Fixing (rdiomar - PR 88)
* Fix Multiprocess Consumer on windows (mahendra - PR 62)
* Improve fault tolerance; add integration tests (jimjh)
* PEP8 / Flakes / Style cleanups (Vetoshkin Nikita; mrtheb - PR 59)
* Setup Travis CI (jimjh - PR 53/54)
* Fix import of BufferUnderflowError (jimjh - PR 49)
* Fix code examples in README (StevenLeRoux - PR 47/48)

# 0.8.0

* Changing auto_commit to False in [SimpleConsumer](kafka/consumer.py), until 0.8.1 is release offset commits are unsupported
* Adding fetch_size_bytes to SimpleConsumer constructor to allow for user-configurable fetch sizes
* Allow SimpleConsumer to automatically increase the fetch size if a partial message is read and no other messages were read during that fetch request. The increase factor is 1.5
* Exception classes moved to kafka.common
