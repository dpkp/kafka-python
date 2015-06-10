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
