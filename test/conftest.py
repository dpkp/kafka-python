import faulthandler

import pytest

from .mock_broker import MockBroker
from kafka.cluster import ClusterMetadata
from kafka.net.compat import KafkaNetClient
from kafka.net.manager import KafkaConnectionManager
from kafka.net.backend.selector import NetworkSelector
from kafka.protocol.metadata import MetadataResponse


# Independent watchdog at slightly under pytest-timeout's deadline (see pytest.ini)
faulthandler.enable()
faulthandler.dump_traceback_later(280, exit=False, repeat=False)


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
    backend = NetworkSelector()
    try:
        yield backend
    finally:
        backend.close()


@pytest.fixture(params=['selector', 'asyncio'])
def both_net(request):
    """A *started* backend, parametrized over selector + asyncio, for curated
    both-backends end-to-end tests.

    Unlike the default ``net`` fixture (an unstarted NetworkSelector that tests
    drive inline), this runs the loop on its own IO thread -- required for the
    asyncio backend, whose ``run()`` has no inline no-IO-thread fallback. Pair
    it with a MockCluster (stateful): a started loop runs background heartbeat /
    metadata-refresh coroutines that fire unscripted requests, which MockCluster
    answers and a scripted MockBroker would not.

    The manager does not own a passed-in instance, so it neither starts nor
    closes it -- this fixture owns the lifecycle and closes it on teardown.
    """
    from kafka.net.backend import resolve_backend
    backend = resolve_backend(request.param, {})
    backend.start()
    try:
        yield backend
    finally:
        backend.close()


@pytest.fixture
def manager(net, broker):
    broker.attach(net)
    manager = KafkaConnectionManager(
        net,
        bootstrap_servers='%s:%d' % (broker.host, broker.port),
        api_version=broker.broker_version,
        request_timeout_ms=5000,
    )
    try:
        yield manager
    finally:
        manager.close()


@pytest.fixture
def cluster():
    return ClusterMetadata()
