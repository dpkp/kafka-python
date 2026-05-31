Supported KIPs
==============

`Kafka Improvement Proposals (KIPs) <https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Improvement+Proposals>`_
describe the protocol and feature changes that ship in each Apache Kafka
release. This page tracks how kafka-python covers the client-facing KIPs. The
KIP set and the Kafka-release column follow the excellent list maintained by
the `franz-go <https://github.com/twmb/franz-go>`_ project.

Because the wire protocol is generated from the upstream Kafka JSON schemas
(``kafka/protocol/schemas/resources/``), the protocol classes track many KIPs
automatically. The **Status** column reflects client-facing behavior in
kafka-python -- whether the feature is actually usable through
``KafkaProducer``, ``KafkaConsumer``, or ``KafkaAdminClient`` -- not merely
whether the wire format exists. Note that kafka-python is a *client* library:
broker- and controller-internal protocols (KRaft, inter-broker replication,
etc.) are intentionally out of scope.

This table layout was borrowed from `franz-go <https://github.com/twmb/franz-go#supported-kips>`__
because we liked it so much! It was originally constructed here by Claude,
but it is maintained with a caring human touch.

Status legend
-------------

* **Supported** -- usable through the kafka-python client or admin API.
* **Partial** -- partially implemented; the status links to the notes below.
* **Protocol only** -- wire/protocol classes exist under ``kafka/protocol/``
  but the client does not yet drive or expose the feature.
* ``--`` -- not implemented / not supported.

KIPs
----

.. list-table::
   :header-rows: 1
   :widths: 12 38 14 24 12
   :class: kip-table

   * - KIP
     - Title
     - Kafka release
     - Status
     - kafka-python release

   * - `KIP-1 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-1+-+Remove+support+of+request.required.acks>`__
     - Disallow acks > 1
     - 0.8.3
     - Supported
     - 1.0.0
   * - `KIP-8 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-8+-+Add+a+flush+method+to+the+producer+API>`__
     - Flush method on Producer
     - 0.8.3
     - Supported
     - 1.0.0
   * - `KIP-4 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-4+-+Command+line+and+centralized+administrative+operations>`__
     - Basic Admin APIs
     - 0.9.0
     - Supported
     - 1.0.0
   * - `KIP-12 <https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=51809888>`__
     - SASL & SSL
     - 0.9.0
     - Supported
     - 1.1.0
   * - `KIP-13 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-13+-+Quotas>`__
     - Throttling (on broker)
     - 0.9.0
     - Supported
     - 2.1.0
   * - `KIP-15 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-15+-+Add+a+close+method+with+a+timeout+in+the+producer>`__
     - Producer close with a timeout
     - 0.9.0
     - Supported
     - 1.0.0
   * - `KIP-19 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-19+-+Add+a+request+timeout+to+NetworkClient>`__
     - Request timeouts
     - 0.9.0
     - Supported
     - 1.0.0
   * - `KIP-22 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-22+-+Expose+a+Partitioner+interface+in+the+new+producer>`__
     - Custom partitioners
     - 0.9.0
     - Supported
     - 1.0.0
   * - `KIP-40 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-40%3A+ListGroups+and+DescribeGroup>`__
     - ListGroups and DescribeGroups
     - 0.9.0
     - Supported
     - 1.4.4
   * - `KIP-31 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-31+-+Move+to+relative+offsets+in+compressed+message+sets>`__
     - Relative offsets in message sets
     - 0.10.0
     - Supported
     - 1.2.0
   * - `KIP-32 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-32+-+Add+timestamps+to+Kafka+message>`__
     - Timestamps in message set v1
     - 0.10.0
     - Supported
     - 1.2.0
   * - `KIP-35 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-35+-+Retrieving+protocol+version>`__
     - ApiVersion
     - 0.10.0
     - Supported
     - 1.3.0
   * - `KIP-41 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-41%3A+KafkaConsumer+Max+Records>`__
     - max.poll.records
     - 0.10.0
     - Supported
     - 1.0.0
   * - `KIP-42 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-42%3A+Add+Producer+and+Consumer+Interceptors>`__
     - Producer & consumer interceptors
     - 0.10.0
     - --
     - --
   * - `KIP-43 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-43%3A+Kafka+SASL+enhancements>`__
     - SASL PLAIN & handshake
     - 0.10.0
     - Supported
     - 1.3.0
   * - `KIP-57 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-57+-+Interoperable+LZ4+Framing>`__
     - Interoperable lz4 framing
     - 0.10.0
     - Supported
     - 1.2.0
   * - `KIP-62 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-62%3A+Allow+consumer+to+send+heartbeats+from+a+background+thread>`__
     - background heartbeats & improvements
     - 0.10.1
     - Supported
     - 1.4.0
   * - `KIP-70 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-70%3A+Revise+Partition+Assignment+Semantics+on+New+Consumer%27s+Subscription+Change>`__
     - On{Assigned,Revoked}
     - 0.10.1
     - Supported
     - 2.1.3
   * - `KIP-74 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-74%3A+Add+Fetch+Response+Size+Limit+in+Bytes>`__
     - Fetch response size limits
     - 0.10.1
     - Supported
     - 2.1.3
   * - `KIP-78 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-78%3A+Cluster+Id>`__
     - ClusterID in Metadata
     - 0.10.1
     - Supported
     - 1.3.3
   * - `KIP-79 <https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=65868090>`__
     - List offsets for times
     - 0.10.1
     - Supported
     - 1.3.4
   * - `KIP-81 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-81%3A+Bound+Fetch+memory+usage+in+the+consumer>`__
     - Bound fetch memory usage
     - 0.10.1
     - :ref:`Partial <partial-entries>`
     - 1.0.0
   * - `KIP-84 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-84%3A+Support+SASL+SCRAM+mechanisms>`__
     - SASL SCRAM
     - 0.10.2
     - Supported
     - 2.0.0
   * - `KIP-86 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-86%3A+Configurable+SASL+callback+handlers>`__
     - SASL Callbacks
     - 0.10.2
     - :ref:`Partial <partial-entries>`
     - 1.4.6
   * - `KIP-88 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-88%3A+OffsetFetch+Protocol+Update>`__
     - OffsetFetch for admins
     - 0.10.2
     - Supported
     - 1.3.3
   * - `KIP-97 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-97%3A+Improved+Kafka+Client+RPC+Compatibility+Policy>`__
     - Backwards compat for old brokers
     - 0.10.2
     - Supported
     - 1.0.0
   * - `KIP-102 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-102+-+Add+close+with+timeout+for+consumers>`__
     - Consumer close timeouts
     - 0.10.2
     - Supported
     - 1.0.0
   * - `KIP-108 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-108%3A+Create+Topic+Policy>`__
     - CreateTopic validate only field
     - 0.10.2
     - Supported
     - 2.0.0
   * - `KIP-54 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-54+-+Sticky+Partition+Assignment+Strategy>`__
     - Sticky partitioning
     - 0.11.0
     - Supported
     - 2.0.2
   * - `KIP-82 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-82+-+Add+Record+Headers>`__
     - Record headers
     - 0.11.0
     - Supported
     - 1.4.0
   * - `KIP-98 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging>`__
     - EOS
     - 0.11.0
     - Supported
     - 2.2.0
   * - `KIP-101 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-101+-+Alter+Replication+Protocol+to+use+Leader+Epoch+rather+than+High+Watermark+for+Truncation>`__
     - OffsetForLeaderEpoch v0
     - 0.11.0
     - Supported
     - 1.4.0
   * - `KIP-107 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-107%3A+Add+deleteRecordsBefore%28%29+API+in+AdminClient>`__
     - DeleteRecords
     - 0.11.0
     - Supported
     - 2.1.0
   * - `KIP-117 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-117%3A+Add+a+public+AdminClient+API+for+Kafka+admin+operations>`__
     - Admin client
     - 0.11.0
     - Supported
     - 1.4.4
   * - `KIP-124 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-124+-+Request+rate+quotas>`__
     - Request rate quotas
     - 0.11.0
     - Supported
     - 2.1.0
   * - `KIP-126 <https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=68715855>`__
     - Ensure proper batch size after compression
     - 0.11.0
     - Supported
     - 1.0.0
   * - `KIP-133 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-133%3A+Describe+and+Alter+Configs+Admin+APIs>`__
     - Describe & Alter configs
     - 0.11.0
     - Supported
     - 2.0.0
   * - `KIP-140 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-140%3A+Add+administrative+RPCs+for+adding%2C+deleting%2C+and+listing+ACLs>`__
     - ACLs
     - 0.11.0
     - Supported
     - 1.4.7
   * - `KIP-144 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-144%3A+Exponential+backoff+for+broker+reconnect+attempts>`__
     - Broker reconnect backoff
     - 0.11.0
     - Supported
     - 1.3.4
   * - `KIP-112 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-112%3A+Handle+disk+failure+for+JBOD>`__
     - Broker request protocol changes
     - 1.0
     - Supported
     - 1.4.4
   * - `KIP-113 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-113%3A+Support+replicas+movement+between+log+directories>`__
     - LogDir requests
     - 1.0
     - Supported
     - 1.4.4
   * - `KIP-152 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-152+-+Improve+diagnostics+for+SASL+authentication+failures>`__
     - More SASL; SASLAuthenticate
     - 1.0
     - Supported
     - 2.1.0
   * - `KIP-185 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-185%3A+Make+exactly+once+in+order+delivery+per+partition+the+default+Producer+setting>`__
     - Idempotency is default
     - 1.0
     - Supported
     - 2.2.0
   * - `KIP-192 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-192+%3A+Provide+cleaner+semantics+when+idempotence+is+enabled>`__
     - Cleaner idempotence semantics
     - 1.0
     - Supported
     - 2.2.0
   * - `KIP-195 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-195%3A+AdminClient.createPartitions>`__
     - CreatePartitions
     - 1.0
     - Supported
     - 1.4.4
   * - `KIP-48 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-48+Delegation+token+support+for+Kafka>`__
     - Delegation tokens
     - 1.1
     - --
     - --
   * - `KIP-204 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-204+%3A+Adding+records+deletion+operation+to+the+new+Admin+Client+API>`__
     - DeleteRecords via admin API
     - 1.1
     - Supported
     - 2.1.0
   * - `KIP-226 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-226+-+Dynamic+Broker+Configuration>`__
     - Describe configs v1
     - 1.1
     - Supported
     - 2.0.0
   * - `KIP-227 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-227%3A+Introduce+Incremental+FetchRequests+to+Increase+Partition+Scalability>`__
     - Incremental fetch
     - 1.1
     - Supported
     - 2.1.0
   * - `KIP-229 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-229%3A+DeleteGroups+API>`__
     - DeleteGroups
     - 1.1
     - Supported
     - 2.0.2
   * - `KIP-219 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+throttle+communication>`__
     - Client-side throttling
     - 2.0
     - Supported
     - 2.1.0
   * - `KIP-222 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-222+-+Add+Consumer+Group+operations+to+Admin+API>`__
     - Group operations via admin API
     - 2.0
     - Supported
     - 1.4.4
   * - `KIP-249 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-249%3A+Add+Delegation+Token+Operations+to+KafkaAdminClient>`__
     - Delegation tokens in admin API
     - 2.0
     - --
     - --
   * - `KIP-255 <https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=75968876>`__
     - SASL OAUTHBEARER
     - 2.0
     - Supported
     - 1.4.6
   * - `KIP-266 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-266%3A+Fix+consumer+indefinite+blocking+behavior>`__
     - Fix indefinite consumer timeouts
     - 2.0
     - :ref:`Partial <partial-entries>`
     - 1.0.0
   * - `KIP-279 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-279%3A+Fix+log+divergence+between+leader+and+follower+after+fast+leader+fail+over>`__
     - OffsetForLeaderEpoch bump
     - 2.0
     - Supported
     - 3.0.0
   * - `KIP-294 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-294+-+Enable+TLS+hostname+verification+by+default>`__
     - TLS verification
     - 2.0
     - Supported
     - 1.4.6
   * - `KIP-91 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-91+Provide+Intuitive+User+Timeouts+in+The+Producer>`__
     - Intuitive producer timeouts
     - 2.1
     - Supported
     - 2.2.0
   * - `KIP-110 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-110%3A+Add+Codec+for+ZStandard+Compression>`__
     - zstd
     - 2.1
     - Supported
     - 2.0.2
   * - `KIP-302 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-302+-+Enable+Kafka+clients+to+use+all+DNS+resolved+IP+addresses>`__
     - Use multiple addrs for resolved hostnames
     - 2.1
     - Supported
     - 2.0.0
   * - `KIP-320 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-320%3A+Allow+fetchers+to+detect+and+handle+log+truncation>`__
     - Fetcher: detect log truncation
     - 2.1
     - Supported
     - 3.0.0
   * - `KIP-322 <https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=87295558>`__
     - DeleteTopics disabled error code
     - 2.1
     - Supported
     - 2.1.0
   * - `KIP-342 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-342%3A+Add+support+for+Custom+SASL+extensions+in+OAuthBearer+authentication>`__
     - OAUTHBEARER extensions
     - 2.1
     - Supported
     - 1.4.6
   * - `KIP-357 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-357%3A++Add+support+to+list+ACLs+per+principal>`__
     - List ACLs per principal via admin API
     - 2.1
     - Supported
     - 1.4.7
   * - `KIP-390 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-390%3A+Support+Compression+Level>`__
     - Configurable compression level
     - 2.1
     - --
     - --
   * - `KIP-183 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-183+-+Change+PreferredReplicaLeaderElectionCommand+to+use+AdminClient>`__
     - Elect preferred leaders
     - 2.2
     - Supported
     - 2.1.0
   * - `KIP-207 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-207%3A+Offsets+returned+by+ListOffsetsResponse+should+be+monotonically+increasing+even+during+a+partition+leader+change>`__
     - New error in ListOffsets
     - 2.2
     - Supported
     - 2.3.0
   * - `KIP-289 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-289%3A+Improve+the+default+group+id+behavior+in+KafkaConsumer>`__
     - Default group.id to null
     - 2.2
     - Supported
     - 2.2.0
   * - `KIP-368 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-368%3A+Allow+SASL+Connections+to+Periodically+Re-Authenticate>`__
     - Periodically reauthenticate SASL
     - 2.2
     - Supported
     - 3.0.0
   * - `KIP-389 <https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=89070828>`__
     - Group max size error
     - 2.2
     - Supported
     - 2.1.0
   * - `KIP-394 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-394%3A+Require+member.id+for+initial+join+group+request>`__
     - Require member.id for initial join request
     - 2.2
     - Supported
     - 2.2.0
   * - `KIP-339 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-339%3A+Create+a+new+IncrementalAlterConfigs+API>`__
     - IncrementalAlterConfigs
     - 2.3
     - Supported
     - 3.0.0
   * - `KIP-341 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-341%3A+Update+Sticky+Assignor%27s+User+Data+Protocol>`__
     - Sticky group bugfix
     - 2.3
     - Supported
     - 2.0.3
   * - `KIP-361 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-361%3A+Add+Consumer+Configuration+to+Disable+Auto+Topic+Creation>`__
     - Allow disable auto topic creation
     - 2.3
     - Supported
     - 2.1.0
   * - `KIP-430 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-430+-+Return+Authorized+Operations+in+Describe+Responses>`__
     - Authorized ops in DescribeGroups
     - 2.3
     - Supported
     - 2.3.0
   * - `KIP-345 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-345%3A+Introduce+static+membership+protocol+to+reduce+consumer+rebalances>`__
     - Static group membership
     - 2.4
     - Supported
     - 2.3.0
   * - `KIP-369 <https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=89070828>`__
     - An always round robin produce partitioner
     - 2.4
     - --
     - --
   * - `KIP-392 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-392%3A+Allow+consumers+to+fetch+from+closest+replica>`__
     - Closest replica fetching w/ rack
     - 2.4
     - Supported
     - 3.0.0
   * - `KIP-396 <https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=97551484>`__
     - Commit offsets manually
     - 2.4
     - Supported
     - 2.0.0
   * - `KIP-412 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-412%3A+Extend+Admin+API+to+support+dynamic+application+log+levels>`__
     - Dynamic log levels w/ IncrementalAlterConfigs
     - 2.4
     - Supported
     - 3.0.0
   * - `KIP-429 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol>`__
     - Consumer incremental rebalance
     - 2.4
     - Supported
     - 3.0.0
   * - `KIP-455 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-455%3A+Create+an+Administrative+API+for+Replica+Reassignment>`__
     - Replica reassignment API
     - 2.4
     - Supported
     - 3.0.0
   * - `KIP-460 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-460%3A+Admin+Leader+Election+RPC>`__
     - Leader election API
     - 2.4
     - Supported
     - 2.1.0
   * - `KIP-464 <https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=113708722>`__
     - CreateTopic defaults
     - 2.4
     - Supported
     - 2.0.0
   * - `KIP-467 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-467%3A+Augment+ProduceResponse+error+messaging+for+specific+culprit+records>`__
     - Per-record error codes when producing
     - 2.4
     - Supported
     - 2.3.0
   * - `KIP-480 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-480%3A+Sticky+Partitioner>`__
     - Sticky partition producing
     - 2.4
     - Supported
     - 3.0.0
   * - `KIP-482 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A+The+Kafka+Protocol+should+Support+Optional+Tagged+Fields>`__
     - Tagged fields (KAFKA-8885)
     - 2.4
     - Supported
     - 3.0.0
   * - `KIP-496 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-496%3A+Administrative+API+to+delete+consumer+offsets>`__
     - OffsetDelete admin command
     - 2.4
     - Supported
     - 3.0.0
   * - `KIP-511 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-511%3A+Collect+and+Expose+Client%27s+Name+and+Version+in+the+Brokers>`__
     - Client name/version in ApiVersions request
     - 2.4
     - Supported
     - 2.1.3
   * - `KIP-514 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-514%3A+Add+a+bounded+flush%28%29+API+to+Kafka+Producer>`__
     - Bounded Flush
     - 2.4
     - Supported
     - 1.0.0
   * - `KIP-525 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-525+-+Return+topic+metadata+and+configs+in+CreateTopics+response>`__
     - CreateTopics v5 returns configs
     - 2.4
     - Supported
     - 2.0.0
   * - `KIP-360 <https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=89068820>`__
     - Safe epoch bumping for ``UNKNOWN_PRODUCER_ID``
     - 2.5
     - Supported
     - 3.0.0
   * - `KIP-447 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics>`__
     - Producer scalability for EOS
     - 2.5
     - Supported
     - 3.0.0
   * - `KIP-526 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-526%3A+Reduce+Producer+Metadata+Lookups+for+Large+Number+of+Topics>`__
     - Reduce metadata lookups
     - 2.5
     - Supported
     - 2.0.0
   * - `KIP-533 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-533%3A+Add+default+api+timeout+to+AdminClient>`__
     - Default API timeout (total time, not per request)
     - 2.5
     - :ref:`Partial <partial-entries>`
     - 1.0.0
   * - `KIP-546 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-546%3A+Add+Client+Quota+APIs+to+the+Admin+Client>`__
     - Client Quota APIs
     - 2.5
     - Protocol only
     - --
   * - `KIP-559 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-559%3A+Make+the+Kafka+Protocol+Friendlier+with+L7+Proxies>`__
     - Protocol info in sync/join
     - 2.5
     - Supported
     - 3.0.0
   * - `KIP-518 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-518%3A+Allow+listing+consumer+groups+per+state>`__
     - List groups by state
     - 2.6
     - Supported
     - 3.0.0
   * - `KIP-519 <https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=128650952>`__
     - Configurable SSL "engine"
     - 2.6
     - Supported
     - 2.0.0
   * - `KIP-568 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-568%3A+Explicit+rebalance+triggering+on+the+Consumer>`__
     - Explicit rebalance triggering on the consumer
     - 2.6
     - --
     - --
   * - `KIP-569 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-569%3A+DescribeConfigsResponse+-+Update+the+schema+to+include+additional+metadata+information+of+the+field>`__
     - Docs & type in DescribeConfigs
     - 2.6
     - Supported
     - 3.0.0
   * - `KIP-580 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-580%3A+Exponential+Backoff+for+Kafka+Clients>`__
     - Exponential backoff
     - 2.6
     - Supported
     - 3.0.0
   * - `KIP-584 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-584%3A+Versioning+scheme+for+features>`__
     - Versioning scheme for features
     - 2.6
     - Supported
     - 3.0.0
   * - `KIP-602 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-602%3A+Change+default+value+for+client.dns.lookup>`__
     - Use all resolved addrs by default
     - 2.6
     - Supported
     - 2.0.0
   * - `KIP-554 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-554%3A+Add+Broker-side+SCRAM+Config+API>`__
     - Broker side SCRAM APIs
     - 2.7
     - Supported
     - 3.0.0
   * - `KIP-588 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-588%3A+Allow+producers+to+recover+gracefully+from+transaction+timeouts>`__
     - Producer recovery from txn timeout
     - 2.7
     - Supported
     - 3.0.0
   * - `KIP-595 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-595%3A+A+Raft+Protocol+for+the+Metadata+Quorum>`__
     - New APIs for raft protocol
     - 2.7
     - Supported
     - 3.0.0
   * - `KIP-599 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-599%3A+Throttle+Create+Topic%2C+Create+Partition+and+Delete+Topic+Operations>`__
     - Throttling on create/delete topic/partition
     - 2.7
     - Supported
     - 2.1.0
   * - `KIP-601 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-601%3A+Configurable+socket+connection+timeout>`__
     - Configurable socket connection timeout
     - 2.7
     - Supported
     - 3.0.0
   * - `KIP-651 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-651+-+Support+PEM+format+for+SSL+certificates+and+private+key>`__
     - Support PEM
     - 2.7
     - Supported
     - 2.0.0
   * - `KIP-654 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-654%3A+Aborted+transaction+with+non-flushed+data+should+throw+a+non-fatal+exception>`__
     - Aborted txns with unflushed data is not fatal
     - 2.7
     - Supported
     - 2.3.0
   * - `KIP-516 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-516%3A+Topic+Identifiers>`__
     - Topic IDs
     - 2.8
     - Supported
     - 3.0.0
   * - `KIP-700 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-700%3A+Add+Describe+Cluster+API>`__
     - DescribeCluster
     - 2.8
     - Supported
     - 3.0.0
   * - `KIP-664 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-664%3A+Provide+tooling+to+detect+and+abort+hanging+transactions>`__
     - Admin API for DescribeProducers
     - 3.0
     - Supported
     - 3.0.0
   * - `KIP-679 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-679%3A+Producer+will+enable+the+strongest+delivery+guarantee+by+default>`__
     - Strongest producer guarantee by default
     - 3.0
     - Supported
     - 3.0.0
   * - `KIP-699 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-699%3A+Update+FindCoordinator+to+resolve+multiple+Coordinators+at+a+time>`__
     - Batch FindCoordinators
     - 3.0
     - Supported
     - 3.0.0
   * - `KIP-709 <https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=173084258>`__
     - Batch OffsetFetch
     - 3.0
     - Supported
     - 3.0.0
   * - `KIP-734 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-734:+Improve+AdminClient.listOffsets+to+return+timestamp+and+offset+for+the+record+with+the+largest+timestamp>`__
     - Support MaxTimestamp in ListOffsets
     - 3.0
     - Supported
     - 3.0.0
   * - `KIP-735 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-735%3A+Increase+default+consumer+session+timeout>`__
     - Bump default session timeout
     - 3.0
     - Supported
     - 3.0.0
   * - `KIP-768 <https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=186877575>`__
     - Extend SASL/OAUTHBEARER support for OIDC
     - 3.1
     - :ref:`Partial <partial-entries>`
     - 1.4.6
   * - `KIP-784 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-784%3A+Add+top-level+error+code+field+to+DescribeLogDirsResponse>`__
     - Add ErrorCode to DescribeLogDirs response
     - 3.1
     - Supported
     - 3.0.0
   * - `KIP-800 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-800%3A+Add+reason+to+JoinGroupRequest+and+LeaveGroupRequest>`__
     - Reason in Join/Leave group
     - 3.1
     - Supported
     - 3.0.0
   * - `KIP-814 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-814%3A+Static+membership+protocol+should+let+the+leader+skip+assignment>`__
     - SkipAssignment for static group leaders
     - 3.1
     - :ref:`Partial <partial-entries>`
     - 3.0.0
   * - `KIP-373 <https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=93324147>`__
     - Users can create delegation tokens for others
     - 3.3
     - --
     - --
   * - `KIP-794 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-794%3A+Strictly+Uniform+Sticky+Partitioner>`__
     - Better sticky partitioning
     - 3.3
     - :ref:`Partial <partial-entries>`
     - 3.0.0
   * - `KIP-827 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-827%3A+Expose+logdirs+total+and+usable+space+via+Kafka+API>`__
     - ``DescribeLogDirs.{Total,Usable}Bytes``
     - 3.3
     - Supported
     - 3.0.0
   * - `KIP-836 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-836%3A+Addition+of+Information+in+DescribeQuorumResponse+about+Voter+Lag>`__
     - ``DescribeQuorum`` voter lag info
     - 3.3
     - Supported
     - 3.0.0
   * - `KIP-851 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-851%3A+Add+requireStable+flag+into+ListConsumerGroupOffsetsOptions>`__
     - ``RequireStable`` on OffsetFetch
     - 3.3
     - :ref:`Partial <partial-entries>`
     - 3.0.0
   * - `KIP-792 <https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=191336614>`__
     - Generation field in consumer group protocol
     - 3.4
     - Supported
     - 3.0.0
   * - `KIP-405 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-405%3A+Kafka+Tiered+Storage>`__
     - Kafka Tiered Storage
     - 3.5
     - Supported
     - 3.0.0
   * - `KIP-881 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-881%3A+Rack-aware+Partition+Assignment+for+Kafka+Consumers>`__
     - Rack-aware consumer partition assignment
     - 3.5
     - --
     - --
   * - `KIP-893 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-893%3A+The+Kafka+protocol+should+support+nullable+structs>`__
     - Nullable structs in the protocol
     - 3.5
     - Supported
     - 3.0.0
   * - `KIP-903 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-903%3A+Replicas+with+stale+broker+epoch+should+not+be+allowed+to+join+the+ISR>`__
     - Stale broker epoch fencing
     - 3.5
     - Protocol only
     - --
   * - `KIP-714 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-714%3A+Client+metrics+and+observability>`__
     - Client Metrics
     - 3.7
     - --
     - --
   * - `KIP-848 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol>`__
     - Next gen consumer rebalance protocol
     - 3.7
     - --
     - --
   * - `KIP-919 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-919%3A+Allow+AdminClient+to+Talk+Directly+with+the+KRaft+Controller+Quorum+and+add+Controller+Registration>`__
     - Admin client to KRaft, Controller registration
     - 3.7
     - --
     - --
   * - `KIP-951 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-951%3A+Leader+discovery+optimisations+for+the+client>`__
     - Leader discovery optimizations
     - 3.7
     - Protocol only
     - --
   * - `KIP-966 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-966%3A+Eligible+Leader+Replicas>`__
     - Eligible leader replicas (protocol)
     - 3.7
     - Supported
     - 3.0.0
   * - `KIP-1000 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-1000%3A+List+Client+Metrics+Configuration+Resources>`__
     - ListClientMetricsResources
     - 3.7
     - --
     - --
   * - `KIP-890 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-890%3A+Transactions+Server-Side+Defense>`__
     - Transactions server side defense
     - 3.8, 4.0
     - :ref:`Partial <partial-entries>`
     - --
   * - `KIP-899 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-899%3A+Allow+producer+and+consumer+clients+to+rebootstrap>`__
     - Allow clients to rebootstrap
     - 3.8
     - --
     - --
   * - `KIP-994 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-994%3A+Minor+Enhancements+to+ListTransactions+and+DescribeTransactions+APIs>`__
     - List/Describe transactions enhancements
     - 3.8
     - --
     - --
   * - `KIP-853 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-853%3A+KRaft+Controller+Membership+Changes>`__
     - Add replica directory ID for replica fetchers
     - 3.9
     - Supported
     - 3.0.0
   * - `KIP-1005 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-1005%3A+Expose+EarliestLocalOffset+and+TieredOffset>`__
     - ListOffsets w. Timestamp -5
     - 3.9
     - Supported
     - 3.0.0
   * - `KIP-1025 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-1025%3A+Optionally+URL-encode+clientID+and+clientSecret+in+authorization+header>`__
     - URL-encode clientID/secret in OAuth auth header
     - 3.9
     - :ref:`Partial <partial-entries>`
     - 1.4.6
   * - `KIP-1022 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-1022%3A+Formatting+and+Updating+Features>`__
     - Formatting changes for features
     - 4.0
     - Supported
     - 3.0.0
   * - `KIP-1043 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-1043%3A+Administration+of+groups>`__
     - Administration of groups
     - 4.0
     - Supported
     - 3.0.0
   * - `KIP-1073 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-1073:+Return+fenced+brokers+in+DescribeCluster+response>`__
     - DescribeCluster.IsFenced
     - 4.0
     - Protocol only
     - --
   * - `KIP-1075 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-1075%3A+Introduce+delayed+remote+list+offsets+purgatory+to+make+LIST_OFFSETS+async>`__
     - TimeoutMillis on ListOffsets
     - 4.0
     - Protocol only
     - --
   * - `KIP-1076 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-1076%3A++Metrics+for+client+applications+KIP-714+extension>`__
     - User provided client metrics
     - 4.0
     - --
     - --
   * - `KIP-1082 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-1082%3A+Require+Client-Generated+IDs+over+the+ConsumerGroupHeartbeat+RPC>`__
     - ClientID in the next-gen rebalancer
     - 4.0
     - --
     - --
   * - `KIP-1102 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-1102%3A+Enable+clients+to+rebootstrap+based+on+timeout+or+error+code>`__
     - RebootstrapRequired
     - 4.0
     - --
     - --
   * - `KIP-1139 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-1139%3A+Add+support+for+OAuth+jwt-bearer+grant+type>`__
     - Oauth JWT bearer grant
     - 4.0
     - :ref:`Partial <partial-entries>`
     - 1.4.6
   * - `KIP-860 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-860%3A+Add+client-provided+option+to+guard+against+replication+factor+change+during+partition+reassignments>`__
     - Client side AlterPartitionAssignments RF change guard
     - 4.1
     - Supported
     - 3.0.0
   * - `KIP-932 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka>`__
     - Share groups (queues)
     - 4.1
     - --
     - --
   * - `KIP-1142 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-1142%3A+Allow+to+list+non-existent+group+which+has+dynamic+config>`__
     - ListConfigResources
     - 4.1
     - Supported
     - 3.0.0
   * - `KIP-1152 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-1152%3A+Add+transactional+ID+pattern+filter+to+ListTransactions+API>`__
     - ListTransactions.TransactionalIDPattern
     - 4.1
     - --
     - --
   * - `KIP-1023 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-1023%3A+Follower+fetch+from+tiered+offset>`__
     - ListOffsets earliest pending upload offset
     - 4.2
     - --
     - --
   * - `KIP-1071 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-1071%3A+Streams+Rebalance+Protocol>`__
     - Streams group protocol
     - 4.2
     - --
     - --
   * - `KIP-1160 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-1160%3A+Enable+returning+supported+features+from+a+specific+broker>`__
     - Per-broker supported features in ApiVersions
     - 4.2
     - Protocol only
     - --
   * - `KIP-1206 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-1206%3A+Strict+max+fetch+records+in+share+fetch>`__
     - ShareFetch strict record limit
     - 4.2
     - --
     - --
   * - `KIP-1222 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-1222%3A+Acquisition+lock+timeout+renewal+in+share+consumer+explicit+mode>`__
     - Share consumer renew acknowledgements
     - 4.2
     - --
     - --
   * - `KIP-1226 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-1226%3A+Introducing+Share+Partition+Lag+Persistence+and+Retrieval>`__
     - Share partition lag in DescribeShareGroupOffsets
     - 4.2
     - --
     - --
   * - `KIP-1227 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-1227%3A+Expose+Rack+ID+in+MemberDescription+and+ShareMemberDescription>`__
     - Rack ID in (Share)MemberDescription
     - 4.2
     - --
     - --
   * - `KIP-1258 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-1258%3A+Add+Support+for+OAuth+Client+Assertion+to+client_credentials+Grant+Type>`__
     - OAuth client assertion in client_credentials grant
     - 4.3
     - :ref:`Partial <partial-entries>`
     - 1.4.6
   * - `KIP-498 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-498%3A+Add+client-side+configuration+for+maximum+response+size+to+protect+against+OOM>`__
     - Max bound on reads
     - WIP
     - Supported
     - 3.0.0


Notes
-----

.. _partial-entries:

* **Partial entries** -- KIP-81 (fetch memory is bounded by ``fetch_max_bytes``
  / ``max_partition_fetch_bytes`` rather than a global buffer); KIP-86 and
  KIP-768 / KIP-1025 / KIP-1139 / KIP-1258 (SASL/OAUTHBEARER is driven by a
  user-supplied ``AbstractTokenProvider``, so OIDC, JWT-bearer, and client
  assertion grants are the application's responsibility); KIP-266 / KIP-533
  (per-call timeouts exist, but there is no ``default.api.timeout.ms``);
  KIP-794 / KIP-814 (the sticky producer partitioner and cooperative-sticky
  assignor are implemented, but not these specific refinements); KIP-890
  (transaction RPCs are capped at
  ``AddPartitionsToTxn``/``EndTxn``/``TxnOffsetCommit`` v3 and ``Produce`` v9,
  below the KIP-890 versions).
* **Not supported** (shown as ``--`` in the table) -- notable client-facing gaps
  include KIP-42 (interceptors), KIP-48 / KIP-249 / KIP-373 (delegation tokens),
  KIP-369 (round-robin partitioner), KIP-568 (explicit rebalance trigger),
  KIP-714 / KIP-1000 / KIP-1076 (client telemetry), KIP-848 / KIP-1082 (next-generation consumer
  group protocol), KIP-881 (rack-aware assignment), KIP-899 / KIP-1102 (client
  re-bootstrap), and KIP-932 / KIP-1071 / KIP-1206 / KIP-1222 (share and streams
  groups).
