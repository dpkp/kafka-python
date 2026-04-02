import logging
import os
import socket
import ssl

import py
import pytest

from test.integration.fixtures import KafkaFixture
from test.testutil import env_kafka_version

log = logging.getLogger(__name__)


@pytest.fixture(scope='module')
def ssl_kafka(request, tmp_path_factory):
    tmp = tmp_path_factory.mktemp('ssl-kafka')
    # Use py.path for compatibility with fixtures.py
    tmp = py.path.local(str(tmp)) # pylint: disable=no-member
    broker = KafkaFixture.instance(0, tmp_dir=tmp, transport='SSL')
    broker.start()
    yield broker
    broker.close()


@pytest.mark.skipif(env_kafka_version() < (0, 9), reason="SSL support requires broker >=0.9")
class TestSSLConnection:
    def test_ssl_handshake(self, ssl_kafka):
        """Verify raw SSL handshake works with the broker."""
        ctx = ssl.create_default_context()
        ctx.load_verify_locations(os.path.join(ssl_kafka.ssl_dir, 'ca-cert'))
        ctx.check_hostname = False
        sock = socket.create_connection(('localhost', ssl_kafka.port))
        ssock = ctx.wrap_socket(sock, server_hostname='localhost')
        assert ssock.version() is not None
        ssock.close()

    def test_admin_client_ssl(self, ssl_kafka):
        """Test KafkaAdminClient over SSL."""
        from kafka.admin import KafkaAdminClient, NewTopic
        import time

        admin = KafkaAdminClient(
            bootstrap_servers='localhost:%d' % ssl_kafka.port,
            security_protocol='SSL',
            ssl_cafile=os.path.join(ssl_kafka.ssl_dir, 'ca-cert'),
            ssl_check_hostname=False,
        )
        topic = 'test-ssl-%d' % int(time.time())
        admin.create_topics([NewTopic(topic, 1, 1)])
        admin.delete_topics([topic])
        admin.close()
