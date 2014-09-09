# 0.9.3 (Unreleased)

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
