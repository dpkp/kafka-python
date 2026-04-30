import pytest

from .mock_broker import MockBroker
from kafka.cluster import ClusterMetadata
from kafka.net.compat import KafkaNetClient
from kafka.net.manager import KafkaConnectionManager
from kafka.net.selector import NetworkSelector
from kafka.protocol.metadata import MetadataResponse


@pytest.fixture
def metrics():
    from kafka.metrics import Metrics

    metrics = Metrics()
    try:
        yield metrics
    finally:
        metrics.close()


@pytest.fixture
def broker(request):
    """
    To set broker version, use indirect parametrize:

    @pytest.mark.parametrize("broker", [(2, 3)], indirect=True)
    """
    broker_version = getattr(request, 'param', (4, 2))
    return MockBroker(broker_version=broker_version)


@pytest.fixture
def multi_broker(broker):
    Broker = MetadataResponse.MetadataResponseBroker
    broker.set_metadata(brokers=[
        Broker(node_id=0, host=broker.host, port=broker.port, rack=None),
        Broker(node_id=1, host=broker.host, port=broker.port, rack=None),
    ])
    return broker


@pytest.fixture
def client(net, manager, broker):
    cli = KafkaNetClient(net=net, manager=manager)
    try:
        yield cli
    finally:
        cli.close()


@pytest.fixture
def net():
    return NetworkSelector()


@pytest.fixture
def manager(net, broker):
    manager = KafkaConnectionManager(
        net,
        bootstrap_servers='%s:%d' % (broker.host, broker.port),
        api_version=broker.broker_version,
        request_timeout_ms=5000,
    )
    broker.attach(manager)
    try:
        yield manager
    finally:
        manager.close()


@pytest.fixture
def cluster():
    return ClusterMetadata()
