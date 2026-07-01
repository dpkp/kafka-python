"""Conformance tests for the NetBackend contract (kafka/net/backend.py).

NetworkSelector is the reference implementation; these pin that it satisfies
the NetBackend Protocol structurally and that the shared lifecycle helper
``on_io_thread()`` behaves correctly. Step 4's AsyncioBackend will be held to
the same isinstance/method-presence checks.
"""
import asyncio
import threading

import pytest

from kafka.net.backend import (
    NetBackend, Transport, resolve_backend, register_backend, _BACKENDS,
)
from kafka.net.selector import NetworkSelector
from kafka.net.transport import KafkaTCPTransport


# The full contract surface, kept here so a missing/renamed method fails loudly.
CONTRACT_METHODS = (
    'start', 'stop', 'close', 'on_io_thread',
    'call_soon', 'call_soon_threadsafe', 'call_soon_with_future',
    'call_at', 'call_later', 'cancel',
    'sleep', 'create_connection',
    'run', 'create_future', 'wakeup',
)

# Removed from the contract by the connection-seam revision (selector-private).
NON_CONTRACT_METHODS = ('wait_read', 'wait_write', 'unregister_event', 'poll')


class TestNetBackendContract:
    def test_networkselector_satisfies_protocol(self):
        assert isinstance(NetworkSelector(), NetBackend)

    def test_plain_object_is_not_netbackend(self):
        assert not isinstance(object(), NetBackend)

    def test_partial_impl_is_not_netbackend(self):
        class Partial:
            def start(self):
                pass
            # missing everything else
        assert not isinstance(Partial(), NetBackend)

    def test_all_contract_methods_present_and_callable(self):
        net = NetworkSelector()
        for name in CONTRACT_METHODS:
            assert callable(getattr(net, name)), name

    def test_readiness_primitives_and_poll_excluded(self):
        # wait_read/wait_write/unregister_event (selector-private, replaced by
        # the connection seam) and legacy poll() are intentionally NOT in the
        # contract, though they still exist on NetworkSelector.
        net = NetworkSelector()
        for name in NON_CONTRACT_METHODS:
            assert name not in CONTRACT_METHODS
            assert hasattr(net, name), name  # still present on the selector impl


class TestTransportContract:
    def test_kafkatcptransport_satisfies_transport(self):
        # Method-presence check against the Transport protocol (no socket needed).
        for name in ('write', 'close', 'abort', 'is_closing',
                     'pause_reading', 'resume_reading', 'host_port'):
            assert callable(getattr(KafkaTCPTransport, name)), name

    def test_plain_object_is_not_transport(self):
        assert not isinstance(object(), Transport)


class TestOnIoThread:
    def test_false_before_start(self):
        net = NetworkSelector()
        assert net.on_io_thread() is False

    def test_false_from_other_thread_true_on_loop(self):
        net = NetworkSelector()
        net.start()
        try:
            async def where():
                return net.on_io_thread()
            # Runs on the IO thread -> True; the calling test thread -> False.
            assert net.run(where) is True
            assert net.on_io_thread() is False
            assert threading.current_thread() is not net._io_thread
        finally:
            net.close()


@pytest.fixture
def clean_registry():
    """Snapshot/restore the backend registry so register_backend() in a test
    doesn't leak into others."""
    saved = dict(_BACKENDS)
    try:
        yield
    finally:
        _BACKENDS.clear()
        _BACKENDS.update(saved)


class TestResolveBackend:
    def test_none_defaults_to_networkselector(self):
        b = resolve_backend(None, {})
        assert isinstance(b, NetworkSelector)
        assert isinstance(b, NetBackend)

    def test_explicit_instance_used_as_is(self):
        sel = NetworkSelector()
        assert resolve_backend(sel, {}) is sel

    def test_name_selector_resolves(self):
        assert isinstance(resolve_backend('selector', {}), NetworkSelector)

    def test_unknown_name_raises(self):
        with pytest.raises(ValueError, match='Unknown net backend'):
            resolve_backend('bogus', {})

    def test_asyncio_name_unregistered_raises(self):
        # In Phase-1/Step-3 the asyncio backend is not registered yet; an
        # explicit request for it is a hard error (an auto-detect is not).
        with pytest.raises(ValueError, match='Unknown net backend'):
            resolve_backend('asyncio', {})

    def test_non_backend_instance_raises(self):
        with pytest.raises(TypeError):
            resolve_backend(object(), {})

    def test_config_passed_through_to_default(self):
        b = resolve_backend(None, {'client_id': 'resolver-test'})
        assert b.config['client_id'] == 'resolver-test'

    def test_name_resolution_passes_config(self, clean_registry):
        seen = {}

        def factory(**config):
            seen.update(config)
            return NetworkSelector(**config)

        register_backend('dummy', factory)
        resolve_backend('dummy', {'client_id': 'via-name'})
        assert seen.get('client_id') == 'via-name'

    def test_autodetect_uses_registered_backend_in_loop(self, clean_registry):
        sentinel = NetworkSelector()
        register_backend('asyncio', lambda **cfg: sentinel)

        async def main():
            return resolve_backend(None, {})

        assert asyncio.run(main()) is sentinel

    def test_autodetect_falls_back_when_unregistered_in_loop(self, clean_registry):
        _BACKENDS.pop('asyncio', None)  # ensure not registered

        async def main():
            return resolve_backend(None, {})

        assert isinstance(asyncio.run(main()), NetworkSelector)

    def test_no_running_loop_defaults_to_selector(self):
        assert isinstance(resolve_backend(None, {}), NetworkSelector)


class TestClientNetConfig:
    def test_default_config_has_net_none(self):
        from kafka.producer.kafka import KafkaProducer
        from kafka.consumer.group import KafkaConsumer
        from kafka.admin.client import KafkaAdminClient
        for cls in (KafkaProducer, KafkaConsumer, KafkaAdminClient):
            assert cls.DEFAULT_CONFIG['net'] is None, cls.__name__

    def test_kafkanetclient_resolves_net(self):
        from kafka.net.compat import KafkaNetClient
        c = KafkaNetClient(bootstrap_servers='localhost:9092')
        assert isinstance(c._net, NetworkSelector)
        c._net.close()

    def test_kafkanetclient_honors_explicit_instance(self):
        from kafka.net.compat import KafkaNetClient
        sel = NetworkSelector()
        c = KafkaNetClient(net=sel, bootstrap_servers='localhost:9092')
        assert c._net is sel
        sel.close()
