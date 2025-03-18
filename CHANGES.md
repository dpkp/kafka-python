# 2.1.2 (Mar 17, 2025)

Fixes
* Simplify consumer.poll send fetches logic
* Fix crc validation in consumer / fetcher
* Lazy `_unpack_records` in PartitionRecords to fix premature fetch offset advance in consumer.poll() (#2555)
* Debug log fetch records return; separate offsets update log
* Fix Fetcher retriable error handling (#2554)
* Use six.add_metaclass for py2/py3 compatible abc (#2551)

Improvements
* Add FetchMetrics class; move topic_fetch_metrics inside aggregator
* DefaultRecordsBatchBuilder: support empty batch
* MemoryRecordsBuilder: support arbitrary offset, skipping offsets
* Add record.validate_crc() for v0/v1 crc checks
* Remove fetcher message_generator / iterator interface
* Add size_in_bytes to ABCRecordBatch and implement for Legacy and Default
* Add magic property to ABCRecord and implement for LegacyRecord

# 2.1.1 (Mar 16, 2025)

Fixes
* Fix packaging of 2.1.0 in Fedora: testing requires "pytest-timeout". (#2550)
* Improve connection error handling when try_api_versions_check fails all attempts (#2548)
* Add lock synchronization to Future success/failure (#2549)
* Fix StickyPartitionAssignor encode

# 2.1.0 (Mar 15, 2025)

Support Kafka Broker 2.1 API Baseline
* Add baseline leader_epoch support for ListOffsets v4 / FetchRequest v10 (#2511)
* Support OffsetFetch v5 / OffsetCommit v6 (2.1 baseline) (#2505)
* Support 2.1 baseline consumer group apis (#2503)
* Support FindCoordinatorRequest v2 in consumer and admin client (#2502)
* Support ListOffsets v3 in consumer (#2501)
* Support Fetch Request/Response v6 in consumer (#2500)
* Add support for Metadata Request/Response v7 (#2497)
* Implement Incremental Fetch Sessions / KIP-227 (#2508)
* Implement client-side connection throttling / KIP-219 (#2510)
* Add KafkaClient.api_version(operation) for best available from api_versions (#2495)

Consumer
* Timeout coordinator poll / ensure_coordinator_ready / ensure_active_group (#2526)
* Add optional timeout_ms kwarg to remaining consumer/coordinator methods (#2544)
* Check for coordinator.poll failure in KafkaConsumer
* Only mark coordinator dead if connection_delay > 0 (#2530)
* Delay group coordinator until after bootstrap (#2539)
* KAFKA-4160: Ensure rebalance listener not called with coordinator lock (#1438)
* Call default_offset_commit_callback after `_maybe_auto_commit_offsets_async` (#2546)
* Remove legacy/v1 consumer message iterator (#2543)
* Log warning when attempting to list offsets for unknown topic/partition (#2540)
* Add heartbeat thread id to debug logs on start
* Add inner_timeout_ms handler to fetcher; add fallback (#2529)

Producer
* KafkaProducer: Flush pending records before close() (#2537)
* Raise immediate error on producer.send after close (#2542)
* Limit producer close timeout to 1sec in __del__; use context managers to close in test_producer
* Use NullLogger in producer atexit cleanup
* Attempt to fix metadata race condition when partitioning in producer.send (#2523)
* Remove unused partial KIP-467 implementation (ProduceResponse batch error details) (#2524)

AdminClient
* Implement perform leader election (#2536)
* Support delete_records (#2535)

Networking
* Call ApiVersionsRequest during connection, prior to Sasl Handshake (#2493)
* Fake api_versions for old brokers, rename to ApiVersionsRequest, and handle error decoding (#2494)
* Debug log when skipping api_versions request with pre-configured api_version
* Only refresh metadata if connection fails all dns records (#2532)
* Support connections through SOCKS5 proxies (#2531)
* Fix OverflowError when connection_max_idle_ms is 0 or inf (#2538)
* socket.setblocking for eventlet/gevent compatibility
* Support custom per-request timeouts (#2498)
* Include request_timeout_ms in request debug log
* Support client.poll with future and timeout_ms
* mask unused afi var
* Debug log if check_version connection attempt fails

SASL Modules
* Refactor Sasl authentication with SaslMechanism abstract base class; support SaslAuthenticate (#2515)
* Add SSPI (Kerberos for Windows) authentication mechanism (#2521)
* Support AWS_MSK_IAM authentication (#2519)
* Cleanup sasl mechanism configuration checks; fix gssapi bugs; add sasl_kerberos_name config (#2520)
* Move kafka.oauth.AbstractTokenProvider -> kafka.sasl.oauth.AbstractTokenProvider (#2525)

Testing
* Bump default python to 3.13 in CI tests (#2541)
* Update pytest log_format: use logger instead of filename; add thread id
* Improve test_consumer_group::test_group logging before group stabilized (#2534)
* Limit test duration to 5mins w/ pytest-timeout
* Fix external kafka/zk fixtures for testing (#2533)
* Disable zookeeper admin server to avoid port conflicts
* Set default pytest log level to debug
* test_group: shorter timeout, more logging, more sleep
* Cache servers/dist in github actions workflow (#2527)
* Remove tox.ini; update testing docs
* Use thread-specific client_id in test_group
* Fix subprocess log warning; specify timeout_ms kwarg in consumer.poll tests
* Only set KAFKA_JVM_PERFORMANCE_OPTS in makefile if unset; add note re: 2.0-2.3 broker testing
* Add kafka command to test.fixtures; raise FileNotFoundError if version not installed

Documentation
* Improve ClusterMetadata docs re: node_id/broker_id str/int types
* Document api_version_auto_timeout_ms default; override in group tests

Fixes
* Signal close to metrics expire_loop
* Add kafka.util timeout_ms_fn
* fixup TopicAuthorizationFailedError construction
* Fix lint issues via ruff check (#2522)
* Make the "mock" dependency optional (only used in Python < 3.3). (#2518)

# 2.0.6 (Mar 4, 2025)

Networking
* Improve error handling in `client._maybe_connect` (#2504)
* Client connection / `maybe_refresh_metadata` changes (#2507)
* Improve too-large timeout handling in client poll
* Default `client.check_version` timeout to `api_version_auto_timeout_ms` (#2496)

Fixes
* Decode and skip transactional control records in consumer (#2499)
* try / except in consumer coordinator `__del__`

Testing
* test_conn fixup for py2

Project Maintenance
* Add 2.0 branch for backports

# 2.0.5 (Feb 25, 2025)

Networking
* Remove unused client bootstrap backoff code
* 200ms timeout for client.poll in ensure_active_group and admin client

Fixes
* Admin client: check_version only if needed, use node_id kwarg for controller
* Check for -1 controller_id in admin client
* Only acquire coordinator lock in heartbeat thread close if not self thread

Testing
* Also sleep when waiting for consumers in test_describe_consumer_group_exists
* Refactor sasl_integration test_client - wait for node ready; use send future
* Add timeout to test_kafka_consumer
* Add error str to assert_message_count checks
* Retry on error in test fixture create_topic_via_metadata
* Fixup variable interpolation in test fixture error

Documentation
* Update compatibility docs
* Include client_id in BrokerConnection __str__ output

Project Maintenance
* Add make targets `servers/*/api_versions` and `servers/*/messages`

# 2.0.4 (Feb 21, 2025)

Networking
* Check for wakeup socket errors on read and close and reinit to reset (#2482)
* Improve client networking backoff / retry (#2480)
* Check for socket and unresolved futures before creating selector in conn.check_version (#2477)
* Handle socket init errors, e.g., when IPv6 is disabled (#2476)

Fixes
* Avoid self-join in heartbeat thread close (#2488)

Error Handling
* Always log broker errors in producer.send (#2478)
* Retain unrecognized broker response error codes with dynamic error class (#2481)
* Update kafka.errors with latest types (#2485)

Compatibility
* Do not validate snappy xerial header version and compat fields (for redpanda) (#2483)

Documentation
* Added missing docstrings in admin/client.py (#2487)

Testing
* Update kafka broker test matrix; test against 3.9.0 (#2486)
* Add default resources for new kafka server fixtures (#2484)
* Drop make test-local; add PYTESTS configuration var
* Fix pytest runs when KAFKA_VERSION is not set

Project Maintenance
* Migrate to pyproject.toml / PEP-621
* Remove old travis files; update compatibility tests link to gha

# 2.0.3 (Feb 12, 2025)

Improvements
* Add optional compression libs to extras_require (#2123, #2387)
* KafkaConsumer: Exit poll if consumer is closed (#2152)
* Support configuration of custom kafka client for Admin/Consumer/Producer (#2144)
* Core Protocol: Add support for flexible versions (#2151)
* (Internal) Allow disabling thread wakeup in _send_request_to_node (#2335)
* Change loglevel of cancelled errors to info (#2467)
* Strip trailing dot off hostname for SSL validation. (#2472)
* Log connection close(error) at ERROR level (#2473)
* Support DescribeLogDirs admin api (#2475)

Compatibility
* Support for python 3.12 (#2379, #2382)
* Kafka 2.5 / 2.6 (#2162)
* Try collections.abc imports in vendored selectors34 (#2394)
* Catch OSError when checking for gssapi import for windows compatibility (#2407)
* Update vendored six to 1.16.0 (#2398)

Documentation
* Update usage.rst (#2308, #2334)
* Fix typos (#2319, #2207, #2178)
* Fix links to the compatibility page (#2295, #2226)
* Cleanup install instructions for optional libs (#2139)
* Update license_file to license_files (#2462)
* Update some RST documentation syntax (#2463)
* Add .readthedocs.yaml; update copyright date (#2474)

Fixes
* Use isinstance in builtin crc32 (#2329)
* Use six.viewitems instead of six.iteritems to avoid encoding problems in StickyPartitionAssignor (#2154)
* Fix array encoding TypeError: object of type 'dict_itemiterator' has no len() (#2167)
* Only try to update sensors fetch lag if the unpacked list contains elements (#2158)
* Avoid logging errors during test fixture cleanup (#2458)
* Release coordinator lock before calling maybe_leave_group (#2460)
* Dont raise RuntimeError for dead process in SpawnedService.wait_for() (#2461)
* Cast the size of a MemoryRecordsBuilder object (#2438)
* Fix DescribeConfigsResponse_v1 config_source (#2464)
* Fix base class of DescribeClientQuotasResponse_v0 (#2465)
* Update socketpair w/ CVE-2024-3219 fix (#2468)

Testing
* Transition CI/CD to GitHub Workflows (#2378, #2392, #2381, #2406, #2419, #2418, #2417, #2456)
* Refactor Makefile (#2457)
* Use assert_called_with in client_async tests (#2375)
* Cover sticky assignor's metadata method with tests (#2161)
* Update fixtures.py to check "127.0.0.1" for auto port assignment (#2384)
* Use -Djava.security.manager=allow for Java 23 sasl tests (#2469)
* Test with Java 23 (#2470)
* Update kafka properties template; disable group rebalance delay (#2471)

# 2.0.2 (Sep 29, 2020)

Consumer
* KIP-54: Implement sticky partition assignment strategy (aynroot / PR #2057)
* Fix consumer deadlock when heartbeat thread request timeout (huangcuiyang / PR #2064)

Compatibility
* Python 3.8 support (Photonios / PR #2088)

Cleanups
* Bump dev requirements (jeffwidman / PR #2129)
* Fix crc32c deprecation warning (crc32c==2.1) (jeffwidman / PR #2128)
* Lint cleanup (jeffwidman / PR #2126)
* Fix initialization order in KafkaClient (pecalleja / PR #2119)
* Allow installing crc32c via extras (mishas / PR #2069)
* Remove unused imports (jameslamb / PR #2046)

Admin Client
* Merge _find_coordinator_id methods (jeffwidman / PR #2127)
* Feature: delete consumergroups (swenzel / PR #2040)
* Allow configurable timeouts in admin client check version (sunnyakaxd / PR #2107)
* Enhancement for Kafka Admin Client's "Describe Consumer Group" (Apurva007 / PR #2035)

Protocol
* Add support for zstd compression (gabriel-tincu / PR #2021)
* Add protocol support for brokers 1.1.0 - 2.5.0 (gabriel-tincu / PR #2038)
* Add ProduceRequest/ProduceResponse v6/v7/v8 (gabriel-tincu / PR #2020)
* Fix parsing NULL header values (kvfi / PR #2024)

Tests
* Add 2.5.0 to automated CI tests (gabriel-tincu / PR #2038)
* Add 2.1.1 to build_integration (gabriel-tincu / PR #2019)

Documentation / Logging / Errors
* Disable logging during producer object gc (gioele / PR #2043)
* Update example.py; use threading instead of multiprocessing (Mostafa-Elmenbawy / PR #2081)
* Fix typo in exception message (haracejacob / PR #2096)
* Add kafka.structs docstrings (Mostafa-Elmenbawy / PR #2080)
* Fix broken compatibility page link (anuragrana / PR #2045)
* Rename README to README.md (qhzxc0015 / PR #2055)
* Fix docs by adding SASL mention (jeffwidman / #1990)

# 2.0.1 (Feb 19, 2020)

Admin Client
* KAFKA-8962: Use least_loaded_node() for AdminClient.describe_topics() (jeffwidman / PR #2000)
* Fix AdminClient topic error parsing in MetadataResponse (jtribble / PR #1997)

# 2.0.0 (Feb 10, 2020)

This release includes breaking changes for any application code that has not
migrated from older Simple-style classes to newer Kafka-style classes.

Deprecation
* Remove deprecated SimpleClient, Producer, Consumer, Unittest (jeffwidman / PR #1196)

Admin Client
* Use the controller for topic metadata requests (TylerLubeck / PR #1995)
* Implement list_topics, describe_topics, and describe_cluster (TylerLubeck / PR #1993)
* Implement __eq__ and __hash__ for ACL objects (TylerLubeck / PR #1955)
* Fixes KafkaAdminClient returning `IncompatibleBrokerVersion` when passing an `api_version` (ian28223 / PR #1953)
* Admin protocol updates (TylerLubeck / PR #1948)
* Fix describe config for multi-broker clusters (jlandersen  / PR #1869)

Miscellaneous Bugfixes / Improvements
* Enable SCRAM-SHA-256 and SCRAM-SHA-512 for sasl (swenzel / PR #1918)
* Fix slots usage and use more slots (carsonip / PR #1987)
* Optionally return OffsetAndMetadata from consumer.committed(tp) (dpkp / PR #1979)
* Reset conn configs on exception in conn.check_version() (dpkp / PR #1977)
* Do not block on sender thread join after timeout in producer.close() (dpkp / PR #1974)
* Implement methods to convert a Struct object to a pythonic object (TylerLubeck / PR #1951)

Test Infrastructure / Documentation / Maintenance
* Update 2.4.0 resource files for sasl integration (dpkp)
* Add kafka 2.4.0 to CI testing (vvuibert / PR #1972)
* convert test_admin_integration to pytest (ulrikjohansson / PR #1923)
* xfail test_describe_configs_topic_resource_returns_configs (dpkp / Issue #1929)
* Add crc32c to README and docs (dpkp)
* Improve docs for reconnect_backoff_max_ms (dpkp / PR #1976)
* Fix simple typo: managementment -> management (timgates42 / PR #1966)
* Fix typos (carsonip / PR #1938)
* Fix doc import paths (jeffwidman / PR #1933)
* Update docstring to match conn.py's (dabcoder / PR #1921)
* Do not log topic-specific errors in full metadata fetch (dpkp / PR #1980)
* Raise AssertionError if consumer closed in poll() (dpkp / PR #1978)
* Log retriable coordinator NodeNotReady, TooManyInFlightRequests as debug not error (dpkp / PR #1975)
* Remove unused import (jeffwidman)
* Remove some dead code (jeffwidman)
* Fix a benchmark to Use print() function in both Python 2 and Python 3 (cclauss / PR #1983)
* Fix a test to use ==/!= to compare str, bytes, and int literals (cclauss / PR #1984)
* Fix benchmarks to use pyperf (carsonip / PR #1986)
* Remove unused/empty .gitsubmodules file (jeffwidman / PR #1928)
* Remove deprecated `ConnectionError` (jeffwidman / PR #1816)


# 1.4.7 (Sep 30, 2019)

This is a minor release focused on KafkaConsumer performance, Admin Client
improvements, and Client concurrency. The KafkaConsumer iterator implementation
has been greatly simplified so that it just wraps consumer.poll(). The prior
implementation will remain available for a few more releases using the optional
KafkaConsumer config: `legacy_iterator=True` . This is expected to improve
consumer throughput substantially and help reduce heartbeat failures / group
rebalancing.

Client
* Send socket data via non-blocking IO with send buffer (dpkp / PR #1912)
* Rely on socket selector to detect completed connection attempts (dpkp / PR #1909)
* Improve connection lock handling; always use context manager (melor,dpkp / PR #1895)
* Reduce client poll timeout when there are no in-flight requests (dpkp / PR #1823)

KafkaConsumer
* Do not use wakeup when sending fetch requests from consumer (dpkp / PR #1911)
* Wrap `consumer.poll()` for KafkaConsumer iteration (dpkp / PR #1902)
* Allow the coordinator to auto-commit on old brokers (justecorruptio / PR #1832)
* Reduce internal client poll timeout for (legacy) consumer iterator interface (dpkp / PR #1824)
* Use dedicated connection for group coordinator (dpkp / PR #1822)
* Change coordinator lock acquisition order (dpkp / PR #1821)
* Make `partitions_for_topic` a read-through cache (Baisang / PR #1781,#1809)
* Fix consumer hanging indefinitely on topic deletion while rebalancing (commanderdishwasher / PR #1782)

Miscellaneous Bugfixes / Improvements
* Fix crc32c avilability on non-intel architectures (ossdev07 / PR #1904)
* Load system default SSL CAs if `ssl_cafile` is not provided (iAnomaly / PR #1883)
* Catch py3 TimeoutError in BrokerConnection send/recv (dpkp / PR #1820)
* Added a function to determine if bootstrap is successfully connected (Wayde2014 / PR #1876)

Admin Client
* Add ACL api support to KafkaAdminClient (ulrikjohansson / PR #1833)
* Add `sasl_kerberos_domain_name` config to KafkaAdminClient (jeffwidman / PR #1852)
* Update `security_protocol` config documentation for KafkaAdminClient (cardy31 / PR #1849)
* Break FindCoordinator into request/response methods in KafkaAdminClient (jeffwidman / PR #1871)
* Break consumer operations into request / response methods in KafkaAdminClient (jeffwidman / PR #1845)
* Parallelize calls to `_send_request_to_node()` in KafkaAdminClient (davidheitman / PR #1807)

Test Infrastructure / Documentation / Maintenance
* Add Kafka 2.3.0 to test matrix and compatibility docs (dpkp / PR #1915)
* Convert remaining `KafkaConsumer` tests to `pytest` (jeffwidman / PR #1886)
* Bump integration tests to 0.10.2.2 and 0.11.0.3 (jeffwidman / #1890)
* Cleanup handling of `KAFKA_VERSION` env var in tests (jeffwidman / PR #1887)
* Minor test cleanup (jeffwidman / PR #1885)
* Use `socket.SOCK_STREAM` in test assertions (iv-m / PR #1879)
* Sanity test for `consumer.topics()` and `consumer.partitions_for_topic()` (Baisang / PR #1829)
* Cleanup seconds conversion in client poll timeout calculation (jeffwidman / PR #1825)
* Remove unused imports (jeffwidman / PR #1808)
* Cleanup python nits in RangePartitionAssignor (jeffwidman / PR #1805)
* Update links to kafka consumer config docs (jeffwidman)
* Fix minor documentation typos (carsonip / PR #1865)
* Remove unused/weird comment line (jeffwidman / PR #1813)
* Update docs for `api_version_auto_timeout_ms` (jeffwidman / PR #1812)


# 1.4.6 (Apr 2, 2019)

This is a patch release primarily focused on bugs related to concurrency,
SSL connections and testing, and SASL authentication:

Client Concurrency Issues (Race Conditions / Deadlocks)
* Fix race condition in `protocol.send_bytes` (isamaru / PR #1752)
* Do not call `state_change_callback` with lock (dpkp / PR #1775)
* Additional BrokerConnection locks to synchronize protocol/IFR state (dpkp / PR #1768)
* Send pending requests before waiting for responses (dpkp / PR #1762)
* Avoid race condition on `client._conns` in send() (dpkp / PR #1772)
* Hold lock during `client.check_version` (dpkp / PR #1771)

Producer Wakeup / TimeoutError
* Dont wakeup during `maybe_refresh_metadata` -- it is only called by poll() (dpkp / PR #1769)
* Dont do client wakeup when sending from sender thread (dpkp / PR #1761)

SSL - Python3.7 Support / Bootstrap Hostname Verification / Testing
* Wrap SSL sockets after connecting for python3.7 compatibility (dpkp / PR #1754)
* Allow configuration of SSL Ciphers (dpkp / PR #1755)
* Maintain shadow cluster metadata for bootstrapping (dpkp / PR #1753)
* Generate SSL certificates for local testing (dpkp / PR #1756)
* Rename ssl.keystore.location and ssl.truststore.location config files (dpkp)
* Reset reconnect backoff on SSL connection (dpkp / PR #1777)

SASL - OAuthBearer support / api version bugfix
* Fix 0.8.2 protocol quick detection / fix SASL version check (dpkp / PR #1763)
* Update sasl configuration docstrings to include supported mechanisms (dpkp)
* Support SASL OAuthBearer Authentication (pt2pham / PR #1750)

Miscellaneous Bugfixes
* Dont force metadata refresh when closing unneeded bootstrap connections (dpkp / PR #1773)
* Fix possible AttributeError during conn._close_socket (dpkp / PR #1776)
* Return connection state explicitly after close in connect() (dpkp / PR #1778)
* Fix flaky conn tests that use time.time (dpkp / PR #1758)
* Add py to requirements-dev (dpkp)
* Fixups to benchmark scripts for py3 / new KafkaFixture interface (dpkp)


# 1.4.5 (Mar 14, 2019)

This release is primarily focused on addressing lock contention
and other coordination issues between the KafkaConsumer and the
background heartbeat thread that was introduced in the 1.4 release.

Consumer
* connections_max_idle_ms must be larger than request_timeout_ms (jeffwidman / PR #1688)
* Avoid race condition during close() / join heartbeat thread (dpkp / PR #1735)
* Use last offset from fetch v4 if available to avoid getting stuck in compacted topic (keithks / PR #1724)
* Synchronize puts to KafkaConsumer protocol buffer during async sends (dpkp / PR #1733)
* Improve KafkaConsumer join group / only enable Heartbeat Thread during stable group (dpkp / PR #1695)
* Remove unused `skip_double_compressed_messages` (jeffwidman / PR #1677)
* Fix commit_offsets_async() callback (Faqa / PR #1712)

Client
* Retry bootstrapping after backoff when necessary (dpkp / PR #1736)
* Recheck connecting nodes sooner when refreshing metadata (dpkp / PR #1737)
* Avoid probing broker versions twice on newer brokers (dpkp / PR #1738)
* Move all network connections and writes to KafkaClient.poll() (dpkp / PR #1729)
* Do not require client lock for read-only operations (dpkp / PR #1730)
* Timeout all unconnected conns (incl SSL) after request_timeout_ms (dpkp / PR #1696)

Admin Client
* Fix AttributeError in response topic error codes checking (jeffwidman)
* Fix response error checking in KafkaAdminClient send_to_controller (jeffwidman)
* Fix NotControllerError check (jeffwidman)

Core/Protocol
* Fix default protocol parser version / 0.8.2 version probe (dpkp / PR #1740)
* Make NotEnoughReplicasError/NotEnoughReplicasAfterAppendError retriable (le-linh / PR #1722)

Bugfixes
* Use copy() in metrics() to avoid thread safety issues (emeric254 / PR #1682)

Test Infrastructure
* Mock dns lookups in test_conn (dpkp / PR #1739)
* Use test.fixtures.version not test.conftest.version to avoid warnings (dpkp / PR #1731)
* Fix test_legacy_correct_metadata_response on x86 arch (stanislavlevin / PR #1718)
* Travis CI: 'sudo' tag is now deprecated in Travis (cclauss / PR #1698)
* Use Popen.communicate() instead of Popen.wait() (Baisang / PR #1689)

Compatibility
* Catch thrown OSError by python 3.7 when creating a connection (danjo133 / PR #1694)
* Update travis test coverage: 2.7, 3.4, 3.7, pypy2.7 (jeffwidman, dpkp / PR #1614)
* Drop dependency on sphinxcontrib-napoleon (stanislavlevin / PR #1715)
* Remove unused import from kafka/producer/record_accumulator.py (jeffwidman / PR #1705)
* Fix SSL connection testing in Python 3.7 (seanthegeek, silentben / PR #1669)


# 1.4.4 (Nov 20, 2018)

Bugfixes
* (Attempt to) Fix deadlock between consumer and heartbeat (zhgjun / dpkp #1628)
* Fix Metrics dict memory leak (kishorenc #1569)

Client
* Support Kafka record headers (hnousiainen #1574)
* Set socket timeout for the write-side of wake socketpair (Fleurer #1577)
* Add kerberos domain name config for gssapi sasl mechanism handshake (the-sea #1542)
* Support smaller topic metadata fetch during bootstrap (andyxning #1541)
* Use TypeError for invalid timeout type (jeffwidman #1636)
* Break poll if closed (dpkp)

Admin Client
* Add KafkaAdminClient class (llamahunter #1540)
* Fix list_consumer_groups() to query all brokers (jeffwidman #1635)
* Stop using broker-errors for client-side problems (jeffwidman #1639)
* Fix send to controller (jeffwidman #1640)
* Add group coordinator lookup (jeffwidman #1641)
* Fix describe_groups (jeffwidman #1642)
* Add list_consumer_group_offsets() (jeffwidman #1643)
* Remove support for api versions as strings from KafkaAdminClient (jeffwidman #1644)
* Set a clear default value for `validate_only`/`include_synonyms` (jeffwidman #1645)
* Bugfix: Always set this_groups_coordinator_id (jeffwidman #1650)

Consumer
* Fix linter warning on import of ConsumerRebalanceListener (ben-harack #1591)
* Remove ConsumerTimeout (emord #1587)
* Return future from commit_offsets_async() (ekimekim #1560)

Core / Protocol
* Add protocol structs for {Describe,Create,Delete} Acls (ulrikjohansson #1646/partial)
* Pre-compile pack/unpack function calls (billyevans / jeffwidman #1619)
* Don't use `kafka.common` internally (jeffwidman #1509)
* Be explicit with tuples for %s formatting (jeffwidman #1634)

Documentation
* Document connections_max_idle_ms (jeffwidman #1531)
* Fix sphinx url (jeffwidman #1610)
* Update remote urls: snappy, https, etc (jeffwidman #1603)
* Minor cleanup of testing doc (jeffwidman #1613)
* Various docstring / pep8 / code hygiene cleanups (jeffwidman #1647)

Test Infrastructure
* Stop pinning `pylint` (jeffwidman #1611)
* (partial) Migrate from `Unittest` to `pytest` (jeffwidman #1620)
* Minor aesthetic cleanup of partitioner tests (jeffwidman #1618)
* Cleanup fixture imports (jeffwidman #1616)
* Fix typo in test file name (jeffwidman)
* Remove unused ivy_root variable (jeffwidman)
* Add test fixtures for kafka versions 1.0.2 -> 2.0.1 (dpkp)
* Bump travis test for 1.x brokers to 1.1.1 (dpkp)

Logging / Error Messages
* raising logging level on messages signalling data loss (sibiryakov #1553)
* Stop using deprecated log.warn() (jeffwidman #1615)
* Fix typo in logging message (jeffwidman)

Compatibility
* Vendor enum34 (jeffwidman #1604)
* Bump vendored `six` to `1.11.0` (jeffwidman #1602)
* Vendor `six` consistently (jeffwidman #1605)
* Prevent `pylint` import errors on `six.moves` (jeffwidman #1609)


# 1.4.3 (May 26, 2018)

Compatibility
* Fix for python 3.7 support: remove 'async' keyword from SimpleProducer (dpkp #1454)

Client
* Improve BrokerConnection initialization time (romulorosa #1475)
* Ignore MetadataResponses with empty broker list (dpkp #1506)
* Improve connection handling when bootstrap list is invalid (dpkp #1507)

Consumer
* Check for immediate failure when looking up coordinator in heartbeat thread (dpkp #1457)

Core / Protocol
* Always acquire client lock before coordinator lock to avoid deadlocks (dpkp #1464)
* Added AlterConfigs and DescribeConfigs apis (StephenSorriaux #1472)
* Fix CreatePartitionsRequest_v0 (StephenSorriaux #1469)
* Add codec validators to record parser and builder for all formats (tvoinarovskyi #1447)
* Fix MemoryRecord bugs re error handling and add test coverage (tvoinarovskyi #1448)
* Force lz4 to disable Kafka-unsupported block linking when encoding (mnito #1476)
* Stop shadowing `ConnectionError` (jeffwidman #1492)

Documentation
* Document methods that return None (jeffwidman #1504)
* Minor doc capitalization cleanup (jeffwidman)
* Adds add_callback/add_errback example to docs (Berkodev #1441)
* Fix KafkaConsumer docstring for request_timeout_ms default (dpkp #1459)

Test Infrastructure
* Skip flakey SimpleProducer test (dpkp)
* Fix skipped integration tests if KAFKA_VERSION unset (dpkp #1453)

Logging / Error Messages
* Stop using deprecated log.warn() (jeffwidman)
* Change levels for some heartbeat thread logging (dpkp #1456)
* Log Heartbeat thread start / close for debugging (dpkp)


# 1.4.2 (Mar 10, 2018)

Bugfixes
* Close leaked selector in version check (dpkp #1425)
* Fix `BrokerConnection.connection_delay()` to return milliseconds (dpkp #1414)
* Use local copies in `Fetcher._fetchable_partitions` to avoid mutation errors (dpkp #1400)
* Fix error var name in `_unpack` (j2gg0s #1403)
* Fix KafkaConsumer compacted offset handling (dpkp #1397)
* Fix byte size estimation with kafka producer (blakeembrey #1393)
* Fix coordinator timeout in consumer poll interface (braedon #1384)

Client
* Add `BrokerConnection.connect_blocking()` to improve bootstrap to multi-address hostnames (dpkp #1411)
* Short-circuit `BrokerConnection.close()` if already disconnected (dpkp #1424)
* Only increase reconnect backoff if all addrinfos have been tried (dpkp #1423)
* Make BrokerConnection .host / .port / .afi immutable to avoid incorrect 'metadata changed' checks (dpkp #1422)
* Connect with sockaddrs to support non-zero ipv6 scope ids (dpkp #1433)
* Check timeout type in KafkaClient constructor (asdaraujo #1293)
* Update string representation of SimpleClient (asdaraujo #1293)
* Do not validate `api_version` against known versions (dpkp #1434)

Consumer
* Avoid tight poll loop in consumer when brokers are down (dpkp #1415)
* Validate `max_records` in KafkaConsumer.poll (dpkp #1398)
* KAFKA-5512: Awake heartbeat thread when it is time to poll (dpkp #1439)

Producer
* Validate that serializers generate bytes-like (or None) data (dpkp #1420)

Core / Protocol
* Support alternative lz4 package: lz4framed (everpcpc #1395)
* Use hardware accelerated CRC32C function if available (tvoinarovskyi #1389)
* Add Admin CreatePartitions API call (alexef #1386)

Test Infrastructure
* Close KafkaConsumer instances during tests (dpkp #1410)
* Introduce new fixtures to prepare for migration to pytest (asdaraujo #1293)
* Removed pytest-catchlog dependency (asdaraujo #1380)
* Fixes racing condition when message is sent to broker before topic logs are created (asdaraujo #1293)
* Add kafka 1.0.1 release to test fixtures (dpkp #1437)

Logging / Error Messages
* Re-enable logging during broker version check (dpkp #1430)
* Connection logging cleanups (dpkp #1432)
* Remove old CommitFailed error message from coordinator (dpkp #1436)


# 1.4.1 (Feb 9, 2018)

Bugfixes
* Fix consumer poll stuck error when no available partition (ckyoog #1375)
* Increase some integration test timeouts (dpkp #1374)
* Use raw in case string overriden (jeffwidman #1373)
* Fix pending completion IndexError bug caused by multiple threads (dpkp #1372)


# 1.4.0 (Feb 6, 2018)

This is a substantial release. Although there are no known 'showstopper' bugs as of release,
we do recommend you test any planned upgrade to your application prior to running in production.

Some of the major changes include:
* We have officially dropped python 2.6 support
* The KafkaConsumer now includes a background thread to handle coordinator heartbeats
* API protocol handling has been separated from networking code into a new class, KafkaProtocol
* Added support for kafka message format v2
* Refactored DNS lookups during kafka broker connections
* SASL authentication is working (we think)
* Removed several circular references to improve gc on close()

Thanks to all contributors -- the state of the kafka-python community is strong!

Detailed changelog are listed below:

Client
* Fixes for SASL support
  * Refactor SASL/gssapi support (dpkp #1248 #1249 #1257 #1262 #1280)
  * Add security layer negotiation to the GSSAPI authentication (asdaraujo #1283)
  * Fix overriding sasl_kerberos_service_name in KafkaConsumer / KafkaProducer (natedogs911 #1264)
  * Fix typo in _try_authenticate_plain (everpcpc #1333)
  * Fix for Python 3 byte string handling in SASL auth (christophelec #1353)
* Move callback processing from BrokerConnection to KafkaClient (dpkp #1258)
* Use socket timeout of request_timeout_ms to prevent blocking forever on send (dpkp #1281)
* Refactor dns lookup in BrokerConnection (dpkp #1312)
* Read all available socket bytes (dpkp #1332)
* Honor reconnect_backoff in conn.connect() (dpkp #1342)

Consumer
* KAFKA-3977: Defer fetch parsing for space efficiency, and to raise exceptions to user (dpkp #1245)
* KAFKA-4034: Avoid unnecessary consumer coordinator lookup (dpkp #1254)
* Handle lookup_coordinator send failures (dpkp #1279)
* KAFKA-3888 Use background thread to process consumer heartbeats (dpkp #1266)
* Improve KafkaConsumer cleanup (dpkp #1339)
* Fix coordinator join_future race condition (dpkp #1338)
* Avoid KeyError when filtering fetchable partitions (dpkp #1344)
* Name heartbeat thread with group_id; use backoff when polling (dpkp #1345)
* KAFKA-3949: Avoid race condition when subscription changes during rebalance (dpkp #1364)
* Fix #1239 regression to avoid consuming duplicate compressed messages from mid-batch (dpkp #1367)

Producer
* Fix timestamp not passed to RecordMetadata (tvoinarovskyi #1273)
* Raise non-API exceptions (jeffwidman #1316)
* Fix reconnect_backoff_max_ms default config bug in KafkaProducer (YaoC #1352)

Core / Protocol
* Add kafka.protocol.parser.KafkaProtocol w/ receive and send (dpkp #1230)
* Refactor MessageSet and Message into LegacyRecordBatch to later support v2 message format (tvoinarovskyi #1252)
* Add DefaultRecordBatch implementation aka V2 message format parser/builder. (tvoinarovskyi #1185)
* optimize util.crc32 (ofek #1304)
* Raise better struct pack/unpack errors (jeffwidman #1320)
* Add Request/Response structs for kafka broker 1.0.0 (dpkp #1368)

Bugfixes
* use python standard max value (lukekingbru #1303)
* changed for to use enumerate() (TheAtomicOption #1301)
* Explicitly check for None rather than falsey (jeffwidman #1269)
* Minor Exception cleanup (jeffwidman #1317)
* Use non-deprecated exception handling (jeffwidman a699f6a)
* Remove assertion with side effect in client.wakeup() (bgedik #1348)
* use absolute imports everywhere (kevinkjt2000 #1362)

Test Infrastructure
* Use 0.11.0.2 kafka broker for integration testing (dpkp #1357 #1244)
* Add a Makefile to help build the project, generate docs, and run tests (tvoinarovskyi #1247)
* Add fixture support for 1.0.0 broker (dpkp #1275)
* Add kafka 1.0.0 to travis integration tests (dpkp #1365)
* Change fixture default host to localhost (asdaraujo #1305)
* Minor test cleanups (dpkp #1343)
* Use latest pytest 3.4.0, but drop pytest-sugar due to incompatibility (dpkp #1361)

Documentation
* Expand metrics docs (jeffwidman #1243)
* Fix docstring (jeffwidman #1261)
* Added controlled thread shutdown to example.py (TheAtomicOption #1268)
* Add license to wheel (jeffwidman #1286)
* Use correct casing for MB (jeffwidman #1298)

Logging / Error Messages
* Fix two bugs in printing bytes instance (jeffwidman #1296)


# 1.3.5 (Oct 7, 2017)

Bugfixes
* Fix partition assignment race condition (jeffwidman #1240)
* Fix consumer bug when seeking / resetting to the middle of a compressed messageset (dpkp #1239)
* Fix traceback sent to stderr not logging (dbgasaway #1221)
* Stop using mutable types for default arg values (jeffwidman #1213)
* Remove a few unused imports (jameslamb #1188)

Client
* Refactor BrokerConnection to use asynchronous receive_bytes pipe (dpkp #1032)

Consumer
* Drop unused sleep kwarg to poll (dpkp #1177)
* Enable KafkaConsumer beginning_offsets() and end_offsets() with older broker versions (buptljy #1200)
* Validate consumer subscription topic strings (nikeee #1238)

Documentation
* Small fixes to SASL documentation and logging; validate security_protocol (dpkp #1231)
* Various typo and grammar fixes (jeffwidman)


# 1.3.4 (Aug 13, 2017)

Bugfixes
* Avoid multiple connection attempts when refreshing metadata (dpkp #1067)
* Catch socket.errors when sending / recving bytes on wake socketpair (dpkp #1069)
* Deal with brokers that reappear with different IP address (originsmike #1085)
* Fix join-time-max and sync-time-max metrics to use Max() measure function (billyevans #1146)
* Raise AssertionError when decompression unsupported (bts-webber #1159)
* Catch ssl.EOFErrors on Python3.3 so we close the failing conn (Ormod #1162)
* Select on sockets to avoid busy polling during bootstrap (dpkp #1175)
* Initialize metadata_snapshot in group coordinator to avoid unnecessary rebalance (dpkp #1174)

Client
* Timeout idle connections via connections_max_idle_ms (dpkp #1068)
* Warn, dont raise, on DNS lookup failures (dpkp #1091)
* Support exponential backoff for broker reconnections -- KIP-144 (dpkp #1124)
* Add gssapi support (Kerberos) for SASL (Harald-Berghoff #1152)
* Add private map of api key -> min/max versions to BrokerConnection (dpkp #1169)

Consumer
* Backoff on unavailable group coordinator retry (dpkp #1125)
* Only change_subscription on pattern subscription when topics change (Artimi #1132)
* Add offsets_for_times, beginning_offsets and end_offsets APIs (tvoinarovskyi #1161)

Producer
* Raise KafkaTimeoutError when flush times out (infecto)
* Set producer atexit timeout to 0 to match del (Ormod #1126)

Core / Protocol
* 0.11.0.0 protocol updates (only - no client support yet) (dpkp #1127)
* Make UnknownTopicOrPartitionError retriable error (tvoinarovskyi)

Test Infrastructure
* pylint 1.7.0+ supports python 3.6 and merge py36 into common testenv (jianbin-wei #1095)
* Add kafka 0.10.2.1 into integration testing version (jianbin-wei #1096)
* Disable automated tests for python 2.6 and kafka 0.8.0 and 0.8.1.1 (jianbin-wei #1096)
* Support manual py26 testing; dont advertise 3.3 support (dpkp)
* Add 0.11.0.0 server resources, fix tests for 0.11 brokers (dpkp)
* Use fixture hostname, dont assume localhost (dpkp)
* Add 0.11.0.0 to travis test matrix, remove 0.10.1.1; use scala 2.11 artifacts (dpkp #1176)

Logging / Error Messages
* Improve error message when expiring batches in KafkaProducer (dpkp #1077)
* Update producer.send docstring -- raises KafkaTimeoutError (infecto)
* Use logging's built-in string interpolation (jeffwidman)
* Fix produce timeout message (melor #1151)
* Fix producer batch expiry messages to use seconds (dnwe)

Documentation
* Fix typo in KafkaClient docstring (jeffwidman #1054)
* Update README: Prefer python-lz4 over lz4tools (kiri11 #1057)
* Fix poll() hyperlink in KafkaClient (jeffwidman)
* Update RTD links with https / .io (jeffwidman #1074)
* Describe consumer thread-safety (ecksun)
* Fix typo in consumer integration test (jeffwidman)
* Note max_in_flight_requests_per_connection > 1 may change order of messages (tvoinarovskyi #1149)


# 1.3.3 (Mar 14, 2017)

Core / Protocol
* Derive all api classes from Request / Response base classes (dpkp 1030)
* Prefer python-lz4 if available (dpkp 1024)
* Fix kwarg handing in kafka.protocol.struct.Struct (dpkp 1025)
* Fixed couple of "leaks" when gc is disabled (Mephius 979)
* Added `max_bytes` option and FetchRequest_v3 usage. (Drizzt1991 962)
* CreateTopicsRequest / Response v1 (dpkp 1012)
* Add MetadataRequest_v2 and MetadataResponse_v2 structures for KIP-78 (Drizzt1991 974)
* KIP-88 / KAFKA-3853: OffsetFetch v2 structs (jeffwidman 971)
* DRY-up the MetadataRequest_v1 struct (jeffwidman 966)
* Add JoinGroup v1 structs (jeffwidman 965)
* DRY-up the OffsetCommitResponse Structs (jeffwidman 970)
* DRY-up the OffsetFetch structs (jeffwidman 964)
* time --> timestamp to match Java API (jeffwidman 969)
* Add support for offsetRequestV1 messages (jlafaye 951)
* Add FetchRequest/Response_v3 structs (jeffwidman 943)
* Add CreateTopics / DeleteTopics Structs (jeffwidman 944)

Test Infrastructure
* Add python3.6 to travis test suite, drop python3.3 (exponea 992)
* Update to 0.10.1.1 for integration testing (dpkp 953)
* Update vendored berkerpeksag/selectors34 to ff61b82 (Mephius 979)
* Remove dead code (jeffwidman 967)
* Update pytest fixtures to new yield syntax (jeffwidman 919)

Consumer
* Avoid re-encoding message for crc check (dpkp 1027)
* Optionally skip auto-commit during consumer.close (dpkp 1031)
* Return copy of consumer subscription set (dpkp 1029)
* Short-circuit group coordinator requests when NodeNotReady (dpkp 995)
* Avoid unknown coordinator after client poll (dpkp 1023)
* No longer configure a default consumer group (dpkp 1016)
* Dont refresh metadata on failed group coordinator request unless needed (dpkp 1006)
* Fail-fast on timeout constraint violations during KafkaConsumer creation (harelba 986)
* Default max_poll_records to Java default of 500 (jeffwidman 947)
* For 0.8.2, only attempt connection to coordinator if least_loaded_node succeeds (dpkp)

Producer
* change default timeout of KafkaProducer.close() to threading.TIMEOUT_MAX on py3 (mmyjona 991)

Client
* Add optional kwarg to ready/is_ready to disable metadata-priority logic (dpkp 1017)
* When closing a broker connection without error, fail in-flight-requests with Cancelled (dpkp 1010)
* Catch socket errors during ssl handshake (dpkp 1007)
* Drop old brokers when rebuilding broker metadata (dpkp 1005)
* Drop bad disconnect test -- just use the mocked-socket test (dpkp 982)
* Add support for Python built without ssl (minagawa-sho 954)
* Do not re-close a disconnected connection (dpkp)
* Drop unused last_failure time from BrokerConnection (dpkp)
* Use connection state functions where possible (dpkp)
* Pass error to BrokerConnection.close() (dpkp)

Bugfixes
* Free lz4 decompression context to avoid leak (dpkp 1024)
* Fix sasl reconnect bug: auth future must be reset on close (dpkp 1003)
* Fix raise exception from SubscriptionState.assign_from_subscribed (qntln 960)
* Fix blackout calculation: mark last_attempt time during connection close (dpkp 1008)
* Fix buffer pool reallocation after raising timeout (dpkp 999)

Logging / Error Messages
* Add client info logging re bootstrap; log connection attempts to balance with close (dpkp)
* Minor additional logging for consumer coordinator (dpkp)
* Add more debug-level connection logging (dpkp)
* Do not need str(self) when formatting to %s (dpkp)
* Add new broker response errors (dpkp)
* Small style fixes in kafka.errors (dpkp)
* Include the node id in BrokerConnection logging (dpkp 1009)
* Replace %s with %r in producer debug log message (chekunkov 973)

Documentation
* Sphinx documentation updates (jeffwidman 1019)
* Add sphinx formatting to hyperlink methods (jeffwidman 898)
* Fix BrokerConnection api_version docs default (jeffwidman 909)
* PEP-8: Spacing & removed unused imports (jeffwidman 899)
* Move BrokerConnection docstring to class (jeffwidman 968)
* Move docstring so it shows up in Sphinx/RTD (jeffwidman 952)
* Remove non-pip install instructions (jeffwidman 940)
* Spelling and grammar changes (melissacrawford396 923)
* Fix typo: coorelation --> correlation (jeffwidman 929)
* Make SSL warning list the correct Python versions (jeffwidman 924)
* Fixup comment reference to _maybe_connect (dpkp)
* Add ClusterMetadata sphinx documentation (dpkp)

Legacy Client
* Add send_list_offset_request for searching offset by timestamp (charsyam 1001)
* Use select to poll sockets for read to reduce CPU usage (jianbin-wei 958)
* Use select.select without instance bounding (adamwen829 949)


# 1.3.2 (Dec 28, 2016)

Core
* Add kafka.serializer interfaces (dpkp 912)
* from kafka import ConsumerRebalanceListener, OffsetAndMetadata
* Use 0.10.0.1 for integration tests (dpkp 803)

Consumer
* KAFKA-3007: KafkaConsumer max_poll_records (dpkp 831)
* Raise exception if given a non-str topic (ssaamm 824)
* Immediately update metadata for pattern subscription (laz2 915)

Producer
* Update Partitioners for use with KafkaProducer (barrotsteindev 827)
* Sort partitions before calling partitioner (ms7s 905)
* Added ssl_password config option to KafkaProducer class (kierkegaard13 830)

Client
* Always check for request timeouts (dpkp 887)
* When hostname lookup is necessary, do every connect (benauthor 812)

Bugfixes
* Fix errorcode check when socket.connect_ex raises an exception (guojh 907)
* Fix fetcher bug when processing offset out of range (sibiryakov 860)
* Fix possible request draining in ensure_active_group (dpkp 896)
* Fix metadata refresh handling with 0.10+ brokers when topic list is empty (sibiryakov 867)
* KafkaProducer should set timestamp in Message if provided (Drizzt1991 875)
* Fix murmur2 bug handling python2 bytes that do not ascii encode (dpkp 815)
* Monkeypatch max_in_flight_requests_per_connection when checking broker version (dpkp 834)
* Fix message timestamp_type (qix 828)

Logging / Error Messages
* Always include an error for logging when the coordinator is marked dead (dpkp 890)
* Only string-ify BrokerResponseError args if provided (dpkp 889)
* Update warning re advertised.listeners / advertised.host.name (jeffwidman 878)
* Fix unrecognized sasl_mechanism error message (sharego 883)

Documentation
* Add docstring for max_records (jeffwidman 897)
* Fixup doc references to max_in_flight_requests_per_connection
* Fix typo: passowrd --> password (jeffwidman 901)
* Fix documentation typo 'Defualt' -> 'Default'. (rolando 895)
* Added doc for `max_poll_records` option (Drizzt1991 881)
* Remove old design notes from Kafka 8 era (jeffwidman 876)
* Fix documentation typos (jeffwidman 874)
* Fix quota violation exception message (dpkp 809)
* Add comment for round robin partitioner with different subscriptions
* Improve KafkaProducer docstring for retries configuration


# 1.3.1 (Aug 8, 2016)

Bugfixes
* Fix AttributeError in BrokerConnectionMetrics after reconnecting


# 1.3.0 (Aug 4, 2016)

Incompatible Changes
* Delete KafkaConnection class (dpkp 769)
* Rename partition_assignment -> assignment in MemberMetadata for consistency
* Move selectors34 and socketpair to kafka.vendor (dpkp 785)
* Change api_version config to tuple; deprecate str with warning (dpkp 761)
* Rename _DEFAULT_CONFIG -> DEFAULT_CONFIG in KafkaProducer (dpkp 788)

Improvements
* Vendor six 1.10.0 to eliminate runtime dependency (dpkp 785)
* Add KafkaProducer and KafkaConsumer.metrics() with instrumentation similar to java client (dpkp 754 / 772 / 794)
* Support Sasl PLAIN authentication (larsjsol PR 779)
* Add checksum and size to RecordMetadata and ConsumerRecord (KAFKA-3196 / 770 / 594)
* Use MetadataRequest v1 for 0.10+ api_version (dpkp 762)
* Fix KafkaConsumer autocommit for 0.8 brokers (dpkp 756 / 706)
* Improve error logging (dpkp 760 / 759)
* Adapt benchmark scripts from https://github.com/mrafayaleem/kafka-jython (dpkp 754)
* Add api_version config to KafkaClient (dpkp 761)
* New Metadata method with_partitions() (dpkp 787)
* Use socket_options configuration to setsockopts(). Default TCP_NODELAY (dpkp 783)
* Expose selector type as config option (dpkp 764)
* Drain pending requests to the coordinator before initiating group rejoin (dpkp 798)
* Send combined size and payload bytes to socket to avoid potentially split packets with TCP_NODELAY (dpkp 797)

Bugfixes
* Ignore socket.error when checking for protocol out of sync prior to socket close (dpkp 792)
* Fix offset fetch when partitions are manually assigned (KAFKA-3960 / 786)
* Change pickle_method to use python3 special attributes (jpaulodit 777)
* Fix ProduceResponse v2 throttle_time_ms
* Always encode size with MessageSet (#771)
* Avoid buffer overread when compressing messageset in KafkaProducer
* Explicit format string argument indices for python 2.6 compatibility
* Simplify RecordMetadata; short circuit callbacks (#768)
* Fix autocommit when partitions assigned manually (KAFKA-3486 / #767 / #626)
* Handle metadata updates during consumer rebalance (KAFKA-3117 / #766 / #701)
* Add a consumer config option to exclude internal topics (KAFKA-2832 / #765)
* Protect writes to wakeup socket with threading lock (#763 / #709)
* Fetcher spending unnecessary time during metrics recording (KAFKA-3785)
* Always use absolute_import (dpkp)

Test / Fixtures
* Catch select errors while capturing test fixture logs
* Fix consumer group test race condition (dpkp 795)
* Retry fixture failures on a different port (dpkp 796)
* Dump fixture logs on failure

Documentation
* Fix misspelling of password (ssaamm 793)
* Document the ssl_password config option (ssaamm 780)
* Fix typo in KafkaConsumer documentation (ssaamm 775)
* Expand consumer.fetcher inline comments
* Update kafka configuration links -> 0.10.0.0 docs
* Fixup metrics_sample_window_ms docstring in consumer


# 1.2.5 (July 15, 2016)

Bugfixes
* Fix bug causing KafkaProducer to double-compress message batches on retry
* Check for double-compressed messages in KafkaConsumer, log warning and optionally skip
* Drop recursion in _unpack_message_set; only decompress once


# 1.2.4 (July 8, 2016)

Bugfixes
* Update consumer_timeout_ms docstring - KafkaConsumer raises StopIteration, no longer ConsumerTimeout
* Use explicit subscription state flag to handle seek() during message iteration
* Fix consumer iteration on compacted topics (dpkp PR 752)
* Support ssl_password config when loading cert chains (amckemie PR 750)


# 1.2.3 (July 2, 2016)

Patch Improvements
* Fix gc error log: avoid AttributeError in _unregister_cleanup (dpkp PR 747)
* Wakeup socket optimizations (dpkp PR 740)
* Assert will be disabled by "python -O" (tyronecai PR 736)
* Randomize order of topics/partitions processed by fetcher to improve balance (dpkp PR 732)
* Allow client.check_version timeout to be set in Producer and Consumer constructors (eastlondoner PR 647)


# 1.2.2 (June 21, 2016)

Bugfixes
* Clarify timeout unit in KafkaProducer close and flush (ms7s PR 734)
* Avoid busy poll during metadata refresh failure with retry_backoff_ms (dpkp PR 733)
* Check_version should scan nodes until version found or timeout (dpkp PR 731)
* Fix bug which could cause least_loaded_node to always return the same unavailable node (dpkp PR 730)
* Fix producer garbage collection with weakref in atexit handler (dpkp PR 728)
* Close client selector to fix fd leak (msmith PR 729)
* Tweak spelling mistake in error const (steve8918 PR 719)
* Rearrange connection tests to separate legacy KafkaConnection


# 1.2.1 (June 1, 2016)

Bugfixes
* Fix regression in MessageSet decoding wrt PartialMessages (#716)
* Catch response decode errors and log details (#715)
* Fix Legacy support url (#712 - JonasGroeger)
* Update sphinx docs re 0.10 broker support


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
* Don't use soon-to-be-reserved keyword await as function name (FutureProduceResult) (dpkp PR 697)

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
* Don't override system rcvbuf or sndbuf unless configured explicitly (dpkp PR 557)
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
