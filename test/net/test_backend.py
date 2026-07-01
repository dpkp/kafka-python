"""Conformance tests for the NetBackend contract (kafka/net/backend.py).

NetworkSelector is the reference implementation; these pin that it satisfies
the NetBackend Protocol structurally and that the shared lifecycle helper
``on_io_thread()`` behaves correctly. Step 4's AsyncioBackend will be held to
the same isinstance/method-presence checks.
"""
import threading

from kafka.net.backend import NetBackend
from kafka.net.selector import NetworkSelector


# The full contract surface, kept here so a missing/renamed method fails loudly.
CONTRACT_METHODS = (
    'start', 'stop', 'close', 'on_io_thread',
    'call_soon', 'call_soon_threadsafe', 'call_soon_with_future',
    'call_at', 'call_later', 'cancel',
    'sleep', 'wait_read', 'wait_write',
    'run', 'create_future', 'wakeup', 'unregister_event',
)


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

    def test_poll_is_not_part_of_contract(self):
        # poll() exists on NetworkSelector (legacy) but is intentionally not
        # in the NetBackend surface; asyncio has no equivalent.
        assert 'poll' not in CONTRACT_METHODS


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
