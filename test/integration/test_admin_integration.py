from kafka.structs import TopicPartition
import pytest

from logging import info
from test.testutil import env_kafka_version, random_string
from threading import Event, Thread
from time import time, sleep

from kafka.admin import (
    ACLFilter, ACLOperation, ACLPermissionType, ResourcePattern, ResourceType, ACL, ConfigResource, ConfigResourceType)
from kafka.errors import (
        BrokerResponseError, KafkaError, NoError, CoordinatorNotAvailableError, NonEmptyGroupError,
        GroupIdNotFoundError, OffsetOutOfRangeError, UnknownTopicOrPartitionError)


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
    broker_id = kafka_admin_client._client.cluster._brokers[0].nodeId
    configs = kafka_admin_client.describe_configs([ConfigResource(ConfigResourceType.BROKER, broker_id)])

    assert len(configs) == 1
    assert configs[0].resources[0][2] == ConfigResourceType.BROKER
    assert configs[0].resources[0][3] == str(broker_id)
    assert len(configs[0].resources[0][4]) > 1


@pytest.mark.xfail(condition=True,
                   reason="https://github.com/dpkp/kafka-python/issues/1929",
                   raises=AssertionError)
@pytest.mark.skipif(env_kafka_version() < (0, 11), reason="Describe config features require broker >=0.11")
def test_describe_configs_topic_resource_returns_configs(topic, kafka_admin_client):
    """Tests that describe config returns configs for topic
    """
    configs = kafka_admin_client.describe_configs([ConfigResource(ConfigResourceType.TOPIC, topic)])

    assert len(configs) == 1
    assert configs[0].resources[0][2] == ConfigResourceType.TOPIC
    assert configs[0].resources[0][3] == topic
    assert len(configs[0].resources[0][4]) > 1


@pytest.mark.skipif(env_kafka_version() < (0, 11), reason="Describe config features require broker >=0.11")
def test_describe_configs_mixed_resources_returns_configs(topic, kafka_admin_client):
    """Tests that describe config returns configs for mixed resource types (topic + broker)
    """
    broker_id = kafka_admin_client._client.cluster._brokers[0].nodeId
    configs = kafka_admin_client.describe_configs([
        ConfigResource(ConfigResourceType.TOPIC, topic),
        ConfigResource(ConfigResourceType.BROKER, broker_id)])

    assert len(configs) == 2

    for config in configs:
        assert (config.resources[0][2] == ConfigResourceType.TOPIC
                and config.resources[0][3] == topic) or \
               (config.resources[0][2] == ConfigResourceType.BROKER
                and config.resources[0][3] == str(broker_id))
        assert len(config.resources[0][4]) > 1


@pytest.mark.skipif(env_kafka_version() < (0, 11), reason="Describe config features require broker >=0.11")
def test_describe_configs_invalid_broker_id_raises(kafka_admin_client):
    """Tests that describe config raises exception on non-integer broker id
    """
    broker_id = "str"

    with pytest.raises(ValueError):
        kafka_admin_client.describe_configs([ConfigResource(ConfigResourceType.BROKER, broker_id)])


@pytest.mark.skipif(env_kafka_version() < (0, 11), reason='Describe consumer group requires broker >=0.11')
def test_describe_consumer_group_does_not_exist(kafka_admin_client):
    """Tests that the describe consumer group call fails if the group coordinator is not available
    """
    with pytest.raises(CoordinatorNotAvailableError):
        kafka_admin_client.describe_consumer_groups(['test'])


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
        for consumer_group in output:
            assert(consumer_group.group in group_id_list)
            if consumer_group.group == group_id_list[0]:
                assert(len(consumer_group.members) == 2)
            else:
                assert(len(consumer_group.members) == 1)
            for member in consumer_group.members:
                    assert(member.member_metadata.subscription[0] == topic)
                    assert(member.member_assignment.assignment[0][0] == topic)
            consumer_groups.add(consumer_group.group)
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
def test_delete_consumergroups(kafka_admin_client, kafka_consumer_factory, send_messages):
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

    consumergroups = {group_id for group_id, _ in kafka_admin_client.list_consumer_groups()}
    assert group1 in consumergroups
    assert group2 in consumergroups
    assert group3 in consumergroups

    delete_results = {
        group_id: error
        for group_id, error in kafka_admin_client.delete_consumer_groups([group1, group2])
    }
    assert delete_results[group1] == NoError
    assert delete_results[group2] == NoError
    assert group3 not in delete_results

    consumergroups = {group_id for group_id, _ in kafka_admin_client.list_consumer_groups()}
    assert group1 not in consumergroups
    assert group2 not in consumergroups
    assert group3 in consumergroups


@pytest.mark.skipif(env_kafka_version() < (1, 1), reason="Delete consumer groups requires broker >=1.1")
def test_delete_consumergroups_with_errors(kafka_admin_client, kafka_consumer_factory, send_messages):
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

    consumergroups = {group_id for group_id, _ in kafka_admin_client.list_consumer_groups()}
    assert group1 in consumergroups
    assert group2 in consumergroups
    assert group3 not in consumergroups

    delete_results = {
        group_id: error
        for group_id, error in kafka_admin_client.delete_consumer_groups([group1, group2, group3])
    }

    assert delete_results[group1] == NoError
    assert delete_results[group2] == NonEmptyGroupError
    assert delete_results[group3] == GroupIdNotFoundError

    consumergroups = {group_id for group_id, _ in kafka_admin_client.list_consumer_groups()}
    assert group1 not in consumergroups
    assert group2 in consumergroups
    assert group3 not in consumergroups

@pytest.fixture(name="topic2")
def _topic2(kafka_broker, request):
    """Same as `topic` fixture, but a different name if you need to topics."""
    topic_name = '%s_%s' % (request.node.name, random_string(10))
    kafka_broker.create_topics([topic_name])
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



