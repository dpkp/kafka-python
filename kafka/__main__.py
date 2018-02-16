from __future__ import absolute_import, print_function

import argparse
import operator
from pprint import pprint
import sys
import time

from kafka import KafkaConsumer, KafkaProducer
from kafka.client_async import KafkaClient
from kafka.coordinator.protocol import ConsumerProtocolMemberMetadata, ConsumerProtocolMemberAssignment
from kafka.errors import for_code as error_for_code
from kafka.protocol.admin import ListGroupsRequest, DescribeGroupsRequest
from kafka.protocol.commit import GroupCoordinatorRequest

def print_padded(data):
    fmt = []
    for i in range(len(data[0])):
        pad = max([len(str(operator.getitem(row, i))) for row in data])
        fmt.append('{%d:<%d}' % (i, pad))
    fmt_str = '   '.join(fmt)
    for row in data:
        print(fmt_str.format(*row))


def check_future(future):
    if future.failed():
        print(future.exception)
        sys.exit(getattr(future.exception, 'errno', 128))
    elif getattr(future.value, 'error_code', 0) != 0:
        print(error_for_code(future.value.error_code))
        sys.exit(future.value.error_code)


parser = argparse.ArgumentParser()
parser.add_argument('--server', required=True, action='append')
parser.add_argument('--list-topics', action='store_true')
parser.add_argument('--list-groups', action='store_true')
parser.add_argument('--describe-group')

args = parser.parse_args()

cli = KafkaClient(bootstrap_servers=args.server)

if args.list_topics:
    cli.cluster.need_all_topic_metadata = True
    metadata = cli.poll(future=cli.cluster.request_update())[0]

    topics = [topic for _, topic, _, _ in metadata.topics]
    for topic in sorted(topics):
        print(topic)


elif args.list_groups:
    req = ListGroupsRequest[0]()

    # start connections to all brokers
    [cli.ready(broker.nodeId) for broker in cli.cluster.brokers()]

    results = [('GROUP', 'TYPE', 'NODE', 'HOST')]
    for broker in cli.cluster.brokers():
        while not cli.ready(broker.nodeId):
            time.sleep(0.1)
        future = cli.send(broker.nodeId, req)
        cli.poll(future=future)
        check_future(future)
        for group_name, group_type in future.value.groups:
            results.append((group_name, group_type, broker.nodeId, broker.host))
    print_padded(results[0:1] + sorted(results[1:]))


elif args.describe_group:
    req = GroupCoordinatorRequest[0](args.describe_group)
    node_id = cli.least_loaded_node()
    future = cli.send(node_id, req)
    cli.poll(future=future)
    check_future(future)

    req = DescribeGroupsRequest[0]([args.describe_group])
    coordinator_id = future.value.coordinator_id
    while not cli.ready(coordinator_id):
        time.sleep(0.1)
    future = cli.send(coordinator_id, req)
    cli.poll(future=future)
    check_future(future)

    for error_code, group, state, protocol_type, protocol, members in future.value.groups:
        results = [('GROUP', 'STATE', 'TYPE', 'PROTOCOL', 'MEMBERS', 'ERRORCODE')]
        results.append((group, state, protocol_type, protocol, len(members), error_code))
        print_padded(results)
        print()
        results = [('MEMBER', 'CLIENT', 'HOST', 'VERSION', 'SUBSCRIPTION', 'ASSIGNMENT')]
        for member_id, client_id, client_host, metadata_bytes, assignment_bytes in sorted(members):
            metadata = ConsumerProtocolMemberMetadata.decode(metadata_bytes)
            assignment = ConsumerProtocolMemberAssignment.decode(assignment_bytes)
            assigned = {}
            for topic, partitions in assignment.assignment:
                assigned[topic] = partitions

            # Print a new row per client - subscribed topic
            for topic in metadata.subscription:
                results.append((member_id, client_id, client_host, metadata.version,
                                topic, assigned.get(topic, [])))
        print_padded(results[0:1] + sorted(results[1:]))
