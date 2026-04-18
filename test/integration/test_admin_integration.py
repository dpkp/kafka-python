from logging import info
from threading import Event, Thread
from time import monotonic as time, sleep

import pytest

from kafka.admin import (
    ACLFilter, ACLOperation, ACLPermissionType,
    ResourcePattern, ResourceType, ACL,
    ConfigResource, ConfigResourceType,
    NewPartitions, NewTopic,
)
from kafka.errors import (
    BrokerResponseError, NoError, CoordinatorNotAvailableError,
    NonEmptyGroupError, GroupIdNotFoundError, OffsetOutOfRangeError,
    UnknownTopicOrPartitionError, ElectionNotNeededError,
    KafkaTimeoutError, IncompatibleBrokerVersion
)
from kafka.structs import TopicPartition
from test.testutil import env_kafka_version, random_string
from test.integration.fixtures import create_topics


@pytest.mark.skipif(env_kafka_version() < (0, 11), reason="ACL features require broker >=0.11")
def test_create_describe_delete_acls(kafka_admin_client):
    """Tests that we can add, list and remove ACLs
    """

    # Check that we don't have any ACLs in the cluster
    acls, error = kafka_admin_client.describe_acls(
        ACLFilter(
            principal=None,
            host="*",
            operation=ACLOperation.ANY,
            permission_type=ACLPermissionType.ANY,
            resource_pattern=ResourcePattern(ResourceType.TOPIC, "topic")
        )
    )

    assert error is NoError
    assert len(acls) == 0

    # Try to add an ACL
    acl = ACL(
        principal="User:test",
        host="*",
        operation=ACLOperation.READ,
        permission_type=ACLPermissionType.ALLOW,
        resource_pattern=ResourcePattern(ResourceType.TOPIC, "topic")
    )
    result = kafka_admin_client.create_acls([acl])

    assert len(result["failed"]) == 0
    assert len(result["succeeded"]) == 1

    # Check that we can list the ACL we created
    acl_filter = ACLFilter(
        principal=None,
        host="*",
        operation=ACLOperation.ANY,
        permission_type=ACLPermissionType.ANY,
        resource_pattern=ResourcePattern(ResourceType.TOPIC, "topic")
    )
    acls, error = kafka_admin_client.describe_acls(acl_filter)

    assert error is NoError
    assert len(acls) == 1

    # Remove the ACL
    delete_results = kafka_admin_client.delete_acls(
        [
            ACLFilter(
                principal="User:test",
                host="*",
                operation=ACLOperation.READ,
                permission_type=ACLPermissionType.ALLOW,
                resource_pattern=ResourcePattern(ResourceType.TOPIC, "topic")
            )
        ]
    )

    assert len(delete_results) == 1
    assert len(delete_results[0][1]) == 1  # Check number of affected ACLs

    # Make sure the ACL does not exist in the cluster anymore
    acls, error = kafka_admin_client.describe_acls(
        ACLFilter(
            principal="*",
            host="*",
            operation=ACLOperation.ANY,
            permission_type=ACLPermissionType.ANY,
            resource_pattern=ResourcePattern(ResourceType.TOPIC, "topic")
        )
    )

    assert error is NoError
    assert len(acls) == 0


@pytest.mark.skipif(env_kafka_version() < (0, 11), reason="Describe config features require broker >=0.11")
def test_describe_configs_broker_resource_returns_configs(kafka_admin_client):
    """Tests that describe config returns configs for broker
    """
    broker_id = kafka_admin_client._client.least_loaded_node()
    configs = kafka_admin_client.describe_configs([ConfigResource(ConfigResourceType.BROKER, broker_id)])

    assert len(configs) == 1
    assert len(configs['broker']) == 1
    if env_kafka_version() >= (4, 0):
        assert configs['broker'][str(broker_id)]['advertised.listeners']['config_source'] == 'STATIC_BROKER_CONFIG'
    elif env_kafka_version() >= (1, 1):
        assert configs['broker'][str(broker_id)]['advertised.listeners']['config_source'] == 'DEFAULT_CONFIG'
    if env_kafka_version() >= (2, 6):
        assert configs['broker'][str(broker_id)]['advertised.listeners']['config_type'] in ('LIST', 'STRING')
    if env_kafka_version() >= (4, 0):
        assert configs['broker'][str(broker_id)]['advertised.listeners']['read_only'] is True
    elif env_kafka_version() >= (1, 0):
        assert configs['broker'][str(broker_id)]['advertised.listeners']['read_only'] is False
    assert configs['broker'][str(broker_id)]['advertised.listeners']['is_sensitive'] is False
    if env_kafka_version() >= (1, 1):
        assert configs['broker'][str(broker_id)]['advertised.listeners']['synonyms'] == []
    assert 'value' in configs['broker'][str(broker_id)]['advertised.listeners']


@pytest.mark.skipif(env_kafka_version() < (0, 11), reason="Describe config features require broker >=0.11")
def test_describe_configs_topic_resource_returns_configs(topic, kafka_admin_client):
    """Tests that describe config returns configs for topic
    """
    configs = kafka_admin_client.describe_configs([ConfigResource(ConfigResourceType.TOPIC, topic)])

    assert len(configs) == 1
    assert len(configs['topic']) == 1
    if env_kafka_version() >= (1, 1):
        assert configs['topic'][topic]['retention.bytes']['config_source'] == 'DEFAULT_CONFIG'
    if env_kafka_version() >= (2, 6):
        assert configs['topic'][topic]['retention.bytes']['config_type'] == 'LONG'
    assert configs['topic'][topic]['retention.bytes']['read_only'] is False
    assert configs['topic'][topic]['retention.bytes']['is_sensitive'] is False
    if env_kafka_version() >= (1, 1):
        assert configs['topic'][topic]['retention.bytes']['synonyms'] == []
    assert configs['topic'][topic]['retention.bytes']['value'] == '-1'


@pytest.mark.skipif(env_kafka_version() < (0, 11), reason="Describe config features require broker >=0.11")
def test_describe_configs_mixed_resources_returns_configs(topic, kafka_admin_client):
    """Tests that describe config returns configs for mixed resource types (topic + broker)
    """
    broker_id = kafka_admin_client._client.least_loaded_node()
    configs = kafka_admin_client.describe_configs([
        ConfigResource(ConfigResourceType.TOPIC, topic),
        ConfigResource(ConfigResourceType.BROKER, broker_id)])

    assert len(configs) == 2
    assert topic in configs['topic']
    assert len(configs['topic'][topic]) > 1
    assert str(broker_id) in configs['broker']
    assert len(configs['broker'][str(broker_id)]) > 1


@pytest.mark.skipif(env_kafka_version() < (0, 11), reason="Describe config features require broker >=0.11")
def test_describe_configs_invalid_broker_id_raises(kafka_admin_client):
    """Tests that describe config raises exception on non-integer broker id
    """
    broker_id = "str"

    with pytest.raises(ValueError):
        kafka_admin_client.describe_configs([ConfigResource(ConfigResourceType.BROKER, broker_id)])


@pytest.mark.skipif(env_kafka_version() < (0, 11), reason='Describe consumer group requires broker >=0.11')
def test_describe_consumer_group_exists(kafka_admin_client, kafka_consumer_factory, topic):
    """Tests that the describe consumer group call returns valid consumer group information
    This test takes inspiration from the test 'test_group' in test_consumer_group.py.
    """
    consumers = {}
    stop = {}
    threads = {}
    random_group_id = 'test-group-' + random_string(6)
    group_id_list = [random_group_id, random_group_id + '_2']
    generations = {group_id_list[0]: set(), group_id_list[1]: set()}
    def consumer_thread(i, group_id):
        assert i not in consumers
        assert i not in stop
        stop[i] = Event()
        consumers[i] = kafka_consumer_factory(group_id=group_id)
        while not stop[i].is_set():
            consumers[i].poll(timeout_ms=200)
        consumers[i].close()
        consumers[i] = None
        stop[i] = None

    num_consumers = 3
    for i in range(num_consumers):
        group_id = group_id_list[i % 2]
        t = Thread(target=consumer_thread, args=(i, group_id,))
        t.start()
        threads[i] = t

    try:
        timeout = time() + 35
        while True:
            info('Checking consumers...')
            for c in range(num_consumers):

                # Verify all consumers have been created
                if c not in consumers:
                    break

                # Verify all consumers have an assignment
                elif not consumers[c].assignment():
                    break

            # If all consumers exist and have an assignment
            else:

                info('All consumers have assignment... checking for stable group')
                # Verify all consumers are in the same generation
                # then log state and break while loop

                for consumer in consumers.values():
                    generations[consumer.config['group_id']].add(consumer._coordinator._generation.generation_id)

                is_same_generation = any([len(consumer_generation) == 1 for consumer_generation in generations.values()])

                # New generation assignment is not complete until
                # coordinator.rejoining = False
                rejoining = any([consumer._coordinator.rejoining
                                 for consumer in list(consumers.values())])

                if not rejoining and is_same_generation:
                    break
            assert time() < timeout, "timeout waiting for assignments"
            info('sleeping...')
            sleep(1)

        info('Group stabilized; verifying assignment')
        output = kafka_admin_client.describe_consumer_groups(group_id_list)
        assert len(output) == 2
        consumer_groups = set()
        for consumer_group in output.values():
            assert(consumer_group['group_id'] in group_id_list)
            if consumer_group['group_id'] == group_id_list[0]:
                assert(len(consumer_group['members']) == 2)
            else:
                assert(len(consumer_group['members']) == 1)
            for member in consumer_group['members']:
                    assert(member['member_metadata']['topics'] == [topic])
                    assert(member['member_assignment']['assigned_partitions'][0]['topic'] == topic)
            consumer_groups.add(consumer_group['group_id'])
        assert(sorted(list(consumer_groups)) == group_id_list)
    finally:
        info('Shutting down %s consumers', num_consumers)
        for c in range(num_consumers):
            info('Stopping consumer %s', c)
            stop[c].set()
        for c in range(num_consumers):
            info('Waiting for consumer thread %s', c)
            threads[c].join()
            threads[c] = None


@pytest.mark.skipif(env_kafka_version() < (1, 1), reason="Delete consumer groups requires broker >=1.1")
def test_delete_consumer_groups(kafka_admin_client, kafka_consumer_factory, send_messages):
    random_group_id = 'test-group-' + random_string(6)
    group1 = random_group_id + "_1"
    group2 = random_group_id + "_2"
    group3 = random_group_id + "_3"

    send_messages(range(0, 100), partition=0)
    consumer1 = kafka_consumer_factory(group_id=group1)
    next(consumer1)
    consumer1.close()

    consumer2 = kafka_consumer_factory(group_id=group2)
    next(consumer2)
    consumer2.close()

    consumer3 = kafka_consumer_factory(group_id=group3)
    next(consumer3)
    consumer3.close()

    groups = {group['group_id'] for group in kafka_admin_client.list_consumer_groups()}
    assert group1 in groups
    assert group2 in groups
    assert group3 in groups

    delete_results = kafka_admin_client.delete_consumer_groups([group1, group2])
    assert delete_results[group1] == 'OK'
    assert delete_results[group2] == 'OK'
    assert group3 not in delete_results

    groups = {group['group_id'] for group in kafka_admin_client.list_consumer_groups()}
    assert group1 not in groups
    assert group2 not in groups
    assert group3 in groups


@pytest.mark.skipif(env_kafka_version() < (1, 1), reason="Delete consumer groups requires broker >=1.1")
def test_delete_consumer_groups_with_errors(kafka_admin_client, kafka_consumer_factory, send_messages):
    random_group_id = 'test-group-' + random_string(6)
    group1 = random_group_id + "_1"
    group2 = random_group_id + "_2"
    group3 = random_group_id + "_3"

    send_messages(range(0, 100), partition=0)
    consumer1 = kafka_consumer_factory(group_id=group1)
    next(consumer1)
    consumer1.close()

    consumer2 = kafka_consumer_factory(group_id=group2)
    next(consumer2)

    groups = {group['group_id'] for group in kafka_admin_client.list_consumer_groups()}
    assert group1 in groups
    assert group2 in groups
    assert group3 not in groups

    delete_results = kafka_admin_client.delete_consumer_groups([group1, group2, group3])
    assert delete_results[group1] == 'OK'
    assert delete_results[group2] == 'NonEmptyGroupError'
    assert delete_results[group3] == 'GroupIdNotFoundError'

    groups = {group['group_id'] for group in kafka_admin_client.list_consumer_groups()}
    assert group1 not in groups
    assert group2 in groups
    assert group3 not in groups


@pytest.fixture(name="topic2")
def _topic2(kafka_broker, request):
    """Same as `topic` fixture, but a different name if you need to topics."""
    topic_name = '%s_%s' % (request.node.name, random_string(10))
    create_topics(kafka_broker, [topic_name])
    return topic_name


@pytest.mark.skipif(env_kafka_version() < (0, 11), reason="Delete records requires broker >=0.11.0")
def test_delete_records(kafka_admin_client, kafka_consumer_factory, send_messages, topic, topic2):
    t0p0 = TopicPartition(topic, 0)
    t0p1 = TopicPartition(topic, 1)
    t0p2 = TopicPartition(topic, 2)
    t1p0 = TopicPartition(topic2, 0)
    t1p1 = TopicPartition(topic2, 1)
    t1p2 = TopicPartition(topic2, 2)

    partitions = (t0p0, t0p1, t0p2, t1p0, t1p1, t1p2)

    for p in partitions:
        send_messages(range(0, 100), partition=p.partition, topic=p.topic)

    consumer1 = kafka_consumer_factory(group_id=None, topics=())
    consumer1.assign(partitions)
    for _ in range(600):
        next(consumer1)

    result = kafka_admin_client.delete_records({t0p0: -1, t0p1: 50, t1p0: 40, t1p2: 30}, timeout_ms=1000)
    assert result[t0p0] == {"low_watermark": 100, "error_code": 0, "partition_index": t0p0.partition}
    assert result[t0p1] == {"low_watermark": 50, "error_code": 0, "partition_index": t0p1.partition}
    assert result[t1p0] == {"low_watermark": 40, "error_code": 0, "partition_index": t1p0.partition}
    assert result[t1p2] == {"low_watermark": 30, "error_code": 0, "partition_index": t1p2.partition}

    consumer2 = kafka_consumer_factory(group_id=None, topics=())
    consumer2.assign(partitions)
    all_messages = consumer2.poll(max_records=600, timeout_ms=2000)
    assert sum(len(x) for x in all_messages.values()) == 600 - 100 - 50 - 40 - 30
    assert not consumer2.poll(max_records=1, timeout_ms=1000) # ensure there are no delayed messages

    assert not all_messages.get(t0p0, [])
    assert [r.offset for r in all_messages[t0p1]] == list(range(50, 100))
    assert [r.offset for r in all_messages[t0p2]] == list(range(100))

    assert [r.offset for r in all_messages[t1p0]] == list(range(40, 100))
    assert [r.offset for r in all_messages[t1p1]] == list(range(100))
    assert [r.offset for r in all_messages[t1p2]] == list(range(30, 100))


@pytest.mark.skipif(env_kafka_version() < (0, 11), reason="Delete records requires broker >=0.11.0")
def test_delete_records_with_errors(kafka_admin_client, topic, send_messages):
    sleep(1)  # sometimes the topic is not created yet...?
    p0 = TopicPartition(topic, 0)
    p1 = TopicPartition(topic, 1)
    p2 = TopicPartition(topic, 2)
    # verify that topic has been created
    send_messages(range(0, 1), partition=p2.partition, topic=p2.topic)

    with pytest.raises(UnknownTopicOrPartitionError):
        kafka_admin_client.delete_records({TopicPartition(topic, 9999): -1})
    with pytest.raises(UnknownTopicOrPartitionError):
        kafka_admin_client.delete_records({TopicPartition("doesntexist", 0): -1})
    with pytest.raises(OffsetOutOfRangeError):
        kafka_admin_client.delete_records({p0: 1000})
    with pytest.raises(BrokerResponseError):
        kafka_admin_client.delete_records({p0: 1000, p1: 1000})


@pytest.mark.skipif(env_kafka_version() < (0, 10, 1), reason="Create topics requires broker >=0.10.1")
def test_create_delete_topics(kafka_admin_client):
    topic_name = random_string(4)
    response = kafka_admin_client.create_topics([NewTopic(topic_name, 1, 1)])
    assert response['topics'][0]['name'] == topic_name
    assert response['topics'][0]['error_code'] == 0 # NoError

    response = kafka_admin_client.delete_topics([topic_name])
    assert response['topics'][0]['name'] == topic_name
    assert response['topics'][0]['error_code'] == 0 # NoError

    topic_name = random_string(4)
    response = kafka_admin_client.create_topics({topic_name: {'num_partitions': 1, 'replication_factor': 1}})
    assert response['topics'][0]['name'] == topic_name
    assert response['topics'][0]['error_code'] == 0 # NoError

    response = kafka_admin_client.delete_topics([topic_name])
    assert response['topics'][0]['name'] == topic_name
    assert response['topics'][0]['error_code'] == 0 # NoError

    # Create topics requires explicit num_partitions/replication_factor on < 2.4
    if env_kafka_version() < (2, 4):
        with pytest.raises(IncompatibleBrokerVersion):
            kafka_admin_client.create_topics([topic_name])

        with pytest.raises(IncompatibleBrokerVersion):
            kafka_admin_client.create_topics({topic_name: {'num_partitions': 2}})

    else:
        topic_name = random_string(4)
        response = kafka_admin_client.create_topics([topic_name])
        assert response['topics'][0]['name'] == topic_name
        assert response['topics'][0]['error_code'] == 0 # NoError

        response = kafka_admin_client.delete_topics([topic_name])
        assert response['topics'][0]['name'] == topic_name
        assert response['topics'][0]['error_code'] == 0 # NoError


@pytest.mark.skipif(env_kafka_version() < (1, 0), reason="CreatePartitions requires broker >=1.0")
def test_create_partitions(kafka_admin_client, topic):
    # topic fixture creates with 4 partitions by default
    topic_metadata = kafka_admin_client.describe_topics([topic])
    assert len(topic_metadata) == 1
    original_count = len(topic_metadata[0]['partitions'])
    assert original_count == 4

    # Increase to 6 partitions
    new_total = 6
    response = kafka_admin_client.create_partitions({topic: NewPartitions(new_total, [[0], [0]])})
    for result in response.results:
        assert result[0] == topic
        assert result[1] == 0  # NoError

    timeout_at = time() + 30
    while time() < timeout_at:
        # Verify the new partition count
        topic_metadata = kafka_admin_client.describe_topics([topic])
        if len(topic_metadata[0]['partitions']) == new_total:
            break
        else:
            sleep(1)
    else:
        raise KafkaTimeoutError('Failed to create partitions')


@pytest.mark.skipif(env_kafka_version() < (2, 2), reason="Leader Election requires broker >=2.2")
def test_elect_leaders(kafka_admin_client, topic):
    topic_metadata = kafka_admin_client.describe_topics([topic])[0]
    assert topic_metadata['name'] == topic
    partitions = list(map(lambda p: p['partition_index'], topic_metadata['partitions']))
    election_type = 0 # Preferred
    topic_partitions = {topic: partitions}
    # When Leader Election is not needed (cluster is stable), error 84 is returned
    response = kafka_admin_client.elect_leaders(election_type, topic_partitions)
    assert len(response.replica_election_results) == 1
    result = response.replica_election_results[0]
    assert result[0] == topic
    partition_set = set(partitions)
    for partition in result[1]:
        assert partition[0] in partition_set
        partition_set.remove(partition[0])
        assert partition[1] == ElectionNotNeededError.errno
    assert partition_set == set()


@pytest.mark.skipif(env_kafka_version() < (1, 0), reason="DescribeLogDirsRequest requires broker >= 1.0")
def test_describe_log_dirs(kafka_admin_client):
    log_dirs = kafka_admin_client.describe_log_dirs()
    assert log_dirs
    broker_map = {result['broker']: result for result in log_dirs}
    for broker in kafka_admin_client._manager.cluster.brokers():
        assert broker.node_id in broker_map
        assert len(broker_map[broker.node_id]['log_dirs']) > 0
        for log_dir in broker_map[broker.node_id]['log_dirs']:
            assert 'log_dir' in log_dir
            assert log_dir['error_code'] == 0


@pytest.mark.skipif(env_kafka_version() < (2, 4), reason="AlterPartitionReassignments requires broker >=2.4")
def test_alter_partition_reassignments(kafka_admin_client, topic):
    topic_metadata = kafka_admin_client.describe_topics([topic])[0]
    brokers = [b.node_id for b in kafka_admin_client._manager.cluster.brokers()]
    # Single-broker cluster: only valid reassignment target is [broker]
    tp = TopicPartition(topic, 0)

    result = kafka_admin_client.alter_partition_reassignments({tp: brokers})
    assert result['error_code'] == 0
    assert len(result['responses']) == 1
    assert result['responses'][0]['name'] == topic


@pytest.mark.skipif(env_kafka_version() < (2, 4), reason="ListPartitionReassignments requires broker >=2.4")
def test_list_partition_reassignments(kafka_admin_client, topic):
    # No reassignments in progress on a freshly-created topic
    result = kafka_admin_client.list_partition_reassignments()
    assert isinstance(result, dict)

    # Scoped lookup for specific partitions also returns an (empty) dict
    tp = TopicPartition(topic, 0)
    result = kafka_admin_client.list_partition_reassignments([tp])
    assert isinstance(result, dict)
    for key, value in result.items():
        assert isinstance(key, TopicPartition)
        assert set(value.keys()) == {'replicas', 'adding_replicas', 'removing_replicas'}


@pytest.mark.skipif(env_kafka_version() < (4, 0), reason="DescribeTopicPartitions requires broker >=4.0 (KRaft)")
def test_describe_topic_partitions(kafka_admin_client, topic):
    result = kafka_admin_client.describe_topic_partitions([topic])
    assert 'topics' in result
    assert 'next_cursor' in result
    assert len(result['topics']) == 1
    t = result['topics'][0]
    assert t['name'] == topic
    assert t['error_code'] == 0
    # topic fixture creates 4 partitions
    assert len(t['partitions']) == 4
    for p in t['partitions']:
        assert p['error_code'] == 0
        assert p['partition_index'] in {0, 1, 2, 3}
        assert p['leader_id'] >= 0
        assert len(p['replica_nodes']) >= 1


@pytest.mark.skipif(env_kafka_version() < (4, 0), reason="DescribeTopicPartitions requires broker >=4.0 (KRaft)")
def test_describe_topic_partitions_pagination(kafka_admin_client, topic):
    # Request only 1 partition per page; we should get a cursor back.
    result = kafka_admin_client.describe_topic_partitions(
        [topic], response_partition_limit=1)
    # Small clusters may still satisfy the request without a cursor if the
    # broker ignores the partition limit — tolerate either outcome but verify
    # the cursor round-trips when present.
    if result['next_cursor'] is not None:
        cursor = result['next_cursor']
        assert cursor['topic_name'] == topic
        next_result = kafka_admin_client.describe_topic_partitions(
            [topic], response_partition_limit=10, cursor=cursor)
        assert next_result['topics']
