"""Concurrency test for KafkaAdminClient IO thread.

Verifies that multiple caller threads can safely invoke admin methods
concurrently while a dedicated IO thread owns the event loop. Exercises
the thread-safety foundation in KafkaConnectionManager (start/stop,
cross-thread run via Event, call_soon_threadsafe).
"""
import threading

import pytest

from kafka.admin import KafkaAdminClient


@pytest.fixture
def admin(broker):
    admin = KafkaAdminClient(
        kafka_client=broker.client_factory(),
        bootstrap_servers='%s:%d' % (broker.host, broker.port),
        request_timeout_ms=5000,
        max_in_flight_requests_per_connection=32,
    )
    try:
        yield admin
    finally:
        admin.close()


def test_concurrent_describe_cluster(admin):
    """Many threads calling describe_cluster at once all succeed with
    consistent results and no deadlock."""
    N = 16
    iterations = 4
    errors = []
    results = []
    results_lock = threading.Lock()
    barrier = threading.Barrier(N)

    def worker():
        try:
            barrier.wait(timeout=5)
            for _ in range(iterations):
                r = admin.describe_cluster()
                with results_lock:
                    results.append(r)
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(N)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=15)
        assert not t.is_alive(), 'worker deadlocked'

    assert not errors, errors
    assert len(results) == N * iterations
    for r in results:
        assert r['cluster_id'] == 'mock-cluster'


def test_close_unblocks_pending_callers(broker):
    """If close() is called while a caller is blocked on run(), the
    caller receives a KafkaConnectionError rather than hanging."""
    admin = KafkaAdminClient(
        kafka_client=broker.client_factory(),
        bootstrap_servers='%s:%d' % (broker.host, broker.port),
        request_timeout_ms=5000,
    )
    manager = admin._manager

    blocked = threading.Event()
    released = threading.Event()

    async def blocker():
        blocked.set()
        # Wait forever — only unblocks via manager.stop()
        await manager._net.sleep(3600)

    result = {}

    def caller():
        try:
            manager.run(blocker)
        except BaseException as exc:
            result['exception'] = exc
        finally:
            released.set()

    t = threading.Thread(target=caller, daemon=True)
    t.start()
    assert blocked.wait(timeout=5), 'blocker never scheduled'

    admin.close()

    assert released.wait(timeout=5), 'caller did not unblock on close'
    assert 'exception' in result
    # stop() fails pending waiters with KafkaConnectionError('Manager stopped')
    from kafka.errors import KafkaConnectionError
    assert isinstance(result['exception'], KafkaConnectionError)
