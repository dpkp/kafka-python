KafkaAdminClient
================

.. autoclass:: kafka.KafkaAdminClient
    :members:

:class:`~kafka.KafkaAdminClient` is composed of functional mixins. The
sections below group its methods by area.


Topics
------

.. automethod:: kafka.KafkaAdminClient.list_topics
.. automethod:: kafka.KafkaAdminClient.describe_topics
.. automethod:: kafka.KafkaAdminClient.create_topics
.. automethod:: kafka.KafkaAdminClient.wait_for_topics
.. automethod:: kafka.KafkaAdminClient.delete_topics


Partitions
----------

.. automethod:: kafka.KafkaAdminClient.create_partitions
.. automethod:: kafka.KafkaAdminClient.delete_records
.. automethod:: kafka.KafkaAdminClient.elect_leaders
.. automethod:: kafka.KafkaAdminClient.alter_partition_reassignments
.. automethod:: kafka.KafkaAdminClient.list_partition_reassignments
.. automethod:: kafka.KafkaAdminClient.describe_topic_partitions
.. automethod:: kafka.KafkaAdminClient.list_partition_offsets


Configs
-------

.. automethod:: kafka.KafkaAdminClient.describe_configs
.. automethod:: kafka.KafkaAdminClient.list_config_resources
.. automethod:: kafka.KafkaAdminClient.alter_configs
.. automethod:: kafka.KafkaAdminClient.reset_configs


Consumer Groups
---------------

.. automethod:: kafka.KafkaAdminClient.list_groups
.. automethod:: kafka.KafkaAdminClient.describe_groups
.. automethod:: kafka.KafkaAdminClient.list_group_offsets
.. automethod:: kafka.KafkaAdminClient.alter_group_offsets
.. automethod:: kafka.KafkaAdminClient.reset_group_offsets
.. automethod:: kafka.KafkaAdminClient.delete_group_offsets
.. automethod:: kafka.KafkaAdminClient.delete_groups
.. automethod:: kafka.KafkaAdminClient.remove_group_members


ACLs
----

.. automethod:: kafka.KafkaAdminClient.describe_acls
.. automethod:: kafka.KafkaAdminClient.create_acls
.. automethod:: kafka.KafkaAdminClient.delete_acls


Cluster
-------

.. automethod:: kafka.KafkaAdminClient.describe_cluster
.. automethod:: kafka.KafkaAdminClient.describe_log_dirs
.. automethod:: kafka.KafkaAdminClient.alter_replica_log_dirs
.. automethod:: kafka.KafkaAdminClient.describe_metadata_quorum
.. automethod:: kafka.KafkaAdminClient.get_broker_version_data
.. automethod:: kafka.KafkaAdminClient.api_versions
.. automethod:: kafka.KafkaAdminClient.describe_features
.. automethod:: kafka.KafkaAdminClient.update_features


Transactions
------------

.. automethod:: kafka.KafkaAdminClient.list_transactions
.. automethod:: kafka.KafkaAdminClient.describe_transactions
.. automethod:: kafka.KafkaAdminClient.describe_producers
.. automethod:: kafka.KafkaAdminClient.abort_transaction
.. automethod:: kafka.KafkaAdminClient.find_hanging_transactions


Users
-----

.. automethod:: kafka.KafkaAdminClient.alter_user_scram_credentials
.. automethod:: kafka.KafkaAdminClient.describe_user_scram_credentials
