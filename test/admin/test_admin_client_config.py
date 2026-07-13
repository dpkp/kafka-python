import pytest

from kafka.admin import KafkaAdminClient
from kafka.errors import KafkaConfigurationError


def test_default_api_timeout_smaller_than_request_timeout_raises():
    # Validation runs before bootstrap/network, so no broker is needed.
    with pytest.raises(KafkaConfigurationError):
        KafkaAdminClient(bootstrap_servers='localhost:9092',
                         request_timeout_ms=70000, default_api_timeout_ms=60000)
