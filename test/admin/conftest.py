import pytest

from kafka.admin import KafkaAdminClient


@pytest.fixture
def admin(broker):
    admin = KafkaAdminClient(
        kafka_client=broker.client_factory(),
        bootstrap_servers='%s:%d' % (broker.host, broker.port),
        request_timeout_ms=5000,
    )
    try:
        yield admin
    finally:
        admin.close()
