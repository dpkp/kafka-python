import ssl

from unittest.mock import MagicMock

from kafka.net.backend import NetTransport, NetProtocol
from kafka.net.backend.transport import KafkaTCPTransport

from kafka.net.ssl import KafkaSSLTransport


class TestKafkaSSLTransport:
    """Regression tests for https://github.com/dpkp/kafka-python/issues/3113:
    TLS SNI (server_hostname) must be sent regardless of ssl_check_hostname so
    that SNI-routed clusters (nginx/Istio/Strimzi ingress) remain reachable when
    hostname verification is disabled.
    """

    def test_sni_sent_when_check_hostname_true(self, net):
        ctx = MagicMock()
        ctx.check_hostname = True
        KafkaSSLTransport(net, ctx, host='broker.example.com')
        _, kwargs = ctx.wrap_bio.call_args
        assert kwargs['server_hostname'] == 'broker.example.com'

    def test_sni_sent_when_check_hostname_false(self, net):
        # The bug: SNI used to be suppressed when verification was disabled.
        ctx = MagicMock()
        ctx.check_hostname = False
        KafkaSSLTransport(net, ctx, host='broker.example.com')
        _, kwargs = ctx.wrap_bio.call_args
        assert kwargs['server_hostname'] == 'broker.example.com'

    def test_sni_strips_trailing_dot(self, net):
        # A trailing dot is a valid FQDN but illegal in the SNI extension.
        ctx = MagicMock()
        ctx.check_hostname = False
        KafkaSSLTransport(net, ctx, host='broker.example.com.')
        _, kwargs = ctx.wrap_bio.call_args
        assert kwargs['server_hostname'] == 'broker.example.com'

    def test_sni_none_when_host_missing(self, net):
        ctx = MagicMock()
        KafkaSSLTransport(net, ctx, host=None)
        _, kwargs = ctx.wrap_bio.call_args
        assert kwargs['server_hostname'] is None

    def test_provided_ssl_context_is_used(self, net):
        ctx = MagicMock()
        t = KafkaSSLTransport(net, ctx, host='broker.example.com')
        assert t._ssl_context is ctx


class TestBuildSSLContext:
    def test_returns_provided_context(self):
        ctx = MagicMock()
        config = dict(ssl_context=ctx)
        assert KafkaSSLTransport.build_ssl_context(config) is ctx

    def test_check_hostname_propagates_to_context(self):
        config = dict(ssl_check_hostname=False)
        ctx = KafkaSSLTransport.build_ssl_context(config)
        assert ctx.check_hostname is False

    def test_check_hostname_true_requires_verification(self):
        config = dict(ssl_check_hostname=True)
        ctx = KafkaSSLTransport.build_ssl_context(config)
        assert ctx.check_hostname is True

    def test_empty_config_builds_default_context(self):
        # Missing keys fall back to DEFAULT_CONFIG: a real TLS-client context
        # with hostname checking on. This is the default-build path.
        ctx = KafkaSSLTransport.build_ssl_context({})
        assert isinstance(ctx, ssl.SSLContext)
        assert ctx.check_hostname is True
