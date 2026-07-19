from unittest.mock import patch

import pytest

from kafka.net.proxy import KafkaTCPProxy

from kafka.net.http_connect import HttpConnectProxyProtocol
from kafka.net.socks5 import Socks5ProxyProtocol


class TestKafkaTCPProxyRegistry:
    def test_socks5(self, net):
        assert 'socks5' in KafkaTCPProxy._registry
        factory = KafkaTCPProxy(net, 'socks5://foo.bar')
        assert isinstance(factory, Socks5ProxyProtocol)

    def test_socks5h(self, net):
        assert 'socks5h' in KafkaTCPProxy._registry
        factory = KafkaTCPProxy(net, 'socks5h://foo.bar')
        assert isinstance(factory, Socks5ProxyProtocol)

    def test_http(self, net):
        assert 'http' in KafkaTCPProxy._registry
        factory = KafkaTCPProxy(net, 'http://proxy:8080')
        assert isinstance(factory, HttpConnectProxyProtocol)

    def test_unknown_scheme_raises(self, net):
        with pytest.raises(ValueError, match='Unsupported proxy url scheme'):
            KafkaTCPProxy(net, 'ftp://proxy:8080')

    def test_no_scheme_raises(self, net):
        with pytest.raises(ValueError, match='scheme'):
            KafkaTCPProxy(net, 'no-scheme')

    def test_empty_string_raises(self, net):
        with pytest.raises(ValueError, match='scheme'):
            KafkaTCPProxy(net, '')

    def test_kafka_net_import_registers_socks5(self):
        # Importing kafka.net must register Socks5ProxyProtocol. Regression guard
        # against accidentally dropping the import from kafka/net/__init__.py.
        import kafka.net  # noqa: F401
        assert KafkaTCPProxy._registry['socks5'] is Socks5ProxyProtocol
        assert KafkaTCPProxy._registry['socks5h'] is Socks5ProxyProtocol

    def test_subclass_auto_registers(self, net):
        class _TestProxy(KafkaTCPProxy):
            SCHEMES = ('test-autoregister',)
            def __init__(self, net, proxy_url):
                self.proxy_url = proxy_url
        try:
            assert KafkaTCPProxy._registry['test-autoregister'] is _TestProxy
            proxy = KafkaTCPProxy(net, 'test-autoregister://x')
            assert isinstance(proxy, _TestProxy)
            assert proxy.proxy_url == 'test-autoregister://x'  # pylint: disable=no-member
        finally:
            KafkaTCPProxy._registry.pop('test-autoregister', None)

    def test_duplicate_scheme_last_wins(self):
        prior = KafkaTCPProxy._registry.get('test-dup')
        class _First(KafkaTCPProxy):
            SCHEMES = ('test-dup',)
        class _Second(KafkaTCPProxy):
            SCHEMES = ('test-dup',)
        try:
            assert KafkaTCPProxy._registry['test-dup'] is _Second
        finally:
            if prior is None:
                KafkaTCPProxy._registry.pop('test-dup', None)
            else:
                KafkaTCPProxy._registry['test-dup'] = prior
