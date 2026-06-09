from unittest.mock import MagicMock

import pytest

from kafka.partitioner import DefaultPartitioner, StickyPartitioner, murmur2


def _cluster(topic, all_partitions, available=None):
    """Build a mock ClusterMetadata that returns the given partition
    sets for ``topic`` (matches the interface DefaultPartitioner /
    StickyPartitioner actually call: ``topics()``,
    ``partitions_for_topic()``, ``available_partitions_for_topic()``)."""
    if available is None:
        available = all_partitions
    cluster = MagicMock()
    cluster.topics.return_value = {topic}
    cluster.partitions_for_topic.return_value = set(all_partitions)
    cluster.available_partitions_for_topic.return_value = set(available)
    return cluster


def test_default_partitioner():
    partitioner = DefaultPartitioner()
    all_partitions = list(range(100))
    cluster = _cluster('t', all_partitions)
    # partitioner should return the same partition for the same key
    p1 = partitioner.partition('t', 'foo', b'foo', 'msg', b'msg', cluster)
    p2 = partitioner.partition('t', 'foo', b'foo', 'msg', b'msg', cluster)
    assert p1 == p2
    assert p1 in all_partitions

    # when key is None, choose one of available partitions
    cluster_single = _cluster('t', all_partitions, available=[123])
    cluster_single.partitions_for_topic.return_value = set(all_partitions) | {123}
    assert partitioner.partition('t', None, None, 'msg', b'msg', cluster_single) == 123

    # with fallback to all_partitions when available is empty
    cluster_no_avail = _cluster('t', all_partitions, available=[])
    assert partitioner.partition('t', None, None, 'msg', b'msg', cluster_no_avail) in all_partitions


def test_default_partitioner_unknown_topic():
    partitioner = DefaultPartitioner()
    cluster = _cluster('t', list(range(10)))
    with pytest.raises(ValueError, match='not found'):
        partitioner.partition('other', 'k', b'k', 'msg', b'msg', cluster)


@pytest.mark.parametrize("bytes_payload,partition_number", [
    (b'', 681), (b'a', 524), (b'ab', 434), (b'abc', 107), (b'123456789', 566),
    (b'\x00 ', 742)
])
def test_murmur2_java_compatibility(bytes_payload, partition_number):
    partitioner = DefaultPartitioner()
    all_partitions = list(range(1000))
    cluster = _cluster('t', all_partitions)
    # compare with output from Kafka's org.apache.kafka.clients.producer.Partitioner
    assert partitioner.partition('t', 'foo', bytes_payload, 'msg', b'msg', cluster) == partition_number


def test_murmur2_not_ascii():
    # Verify no regression of murmur2() bug encoding py2 bytes that don't ascii encode
    murmur2(b'\xa4')
    murmur2(b'\x81' * 1000)


# KIP-480 sticky partitioner


class TestStickyPartitioner:
    def test_keyed_records_hash_like_default(self):
        sticky = StickyPartitioner()
        default = DefaultPartitioner()
        all_partitions = list(range(100))
        cluster = _cluster('t', all_partitions)
        assert (sticky.partition('t', 'foo', b'foo', 'msg', b'msg', cluster)
                == default.partition('t', 'foo', b'foo', 'msg', b'msg', cluster))
        assert (sticky.partition('t', 'bar', b'bar', 'msg', b'msg', cluster)
                == default.partition('t', 'bar', b'bar', 'msg', b'msg', cluster))

    def test_unknown_topic_raises(self):
        sticky = StickyPartitioner()
        cluster = _cluster('t', [0, 1])
        with pytest.raises(ValueError, match='not found'):
            sticky.partition('other', None, None, 'msg', b'msg', cluster)

    def test_null_key_sticks_until_on_new_batch(self):
        """A stream of null-key records pins to the chosen partition;
        ``on_new_batch`` rotates immediately (no "absorb first event"
        hack - KafkaProducer.send invokes the hook only on the
        abort-for-new-batch retry path, matching Java)."""
        sticky = StickyPartitioner()
        all_partitions = list(range(10))
        cluster = _cluster('t', all_partitions)
        p1 = sticky.partition('t', None, None, 'msg', b'msg', cluster)
        for _ in range(50):
            assert sticky.partition('t', None, None, 'msg', b'msg', cluster) == p1

        # on_new_batch rotates immediately.
        sticky.on_new_batch('t', cluster, p1)
        p2 = sticky.partition('t', None, None, 'msg', b'msg', cluster)
        assert p2 != p1, 'on_new_batch should rotate'
        for _ in range(50):
            assert sticky.partition('t', None, None, 'msg', b'msg', cluster) == p2

    def test_on_new_batch_rotation_avoids_prev_partition(self):
        """Regression test for the prior bug where on_new_batch passed
        available=None to _pick_sticky_locked and the rotation could
        randomly land back on the same partition. Now that on_new_batch
        passes the cluster through, the available-aware avoid logic
        kicks in and rotation is guaranteed."""
        sticky = StickyPartitioner()
        all_partitions = list(range(10))
        cluster = _cluster('t', all_partitions)
        p1 = sticky.partition('t', None, None, 'msg', b'msg', cluster)
        # Many rotations should never pick p1 again when many partitions
        # are available and p1 is the avoid target.
        for _ in range(200):
            sticky._sticky['t'] = p1  # restore state between iterations
            sticky.on_new_batch('t', cluster, p1)
            assert sticky._sticky['t'] != p1, (
                'rotation must avoid prev when other available partitions exist')

    def test_per_topic_state_independent(self):
        """Stickiness is per-topic; rotating one topic doesn't affect another."""
        sticky = StickyPartitioner()
        all_partitions = list(range(10))
        cluster_a = _cluster('a', all_partitions)
        cluster_b = _cluster('b', all_partitions)
        # Combined cluster mock answers for both topics.
        cluster = MagicMock()
        cluster.topics.return_value = {'a', 'b'}
        cluster.partitions_for_topic.side_effect = (
            lambda t: set(all_partitions))
        cluster.available_partitions_for_topic.side_effect = (
            lambda t: set(all_partitions))
        p_a = sticky.partition('a', None, None, 'msg', b'msg', cluster)
        p_b = sticky.partition('b', None, None, 'msg', b'msg', cluster)
        # Rotate 'a' once.
        sticky.on_new_batch('a', cluster, p_a)
        # 'b' is untouched.
        assert sticky.partition('b', None, None, 'msg', b'msg', cluster) == p_b

    def test_unavailable_sticky_partition_repicks(self):
        """If the stuck partition's leader becomes unavailable, the next
        partition() call repicks from the available set."""
        sticky = StickyPartitioner()
        all_partitions = list(range(10))
        cluster = _cluster('t', all_partitions, available=all_partitions)
        p1 = sticky.partition('t', None, None, 'msg', b'msg', cluster)
        # Now only partitions != p1 are available.
        cluster.available_partitions_for_topic.return_value = (
            set(all_partitions) - {p1})
        p2 = sticky.partition('t', None, None, 'msg', b'msg', cluster)
        assert p2 != p1
        assert p2 in (set(all_partitions) - {p1})

    def test_single_partition_topic_cannot_rotate(self):
        """on_new_batch on a single-partition topic just keeps the same
        partition - there's nothing else to rotate to."""
        sticky = StickyPartitioner()
        cluster = _cluster('t', [0])
        assert sticky.partition('t', None, None, 'msg', b'msg', cluster) == 0
        sticky.on_new_batch('t', cluster, 0)
        assert sticky.partition('t', None, None, 'msg', b'msg', cluster) == 0

    def test_on_new_batch_ignores_stale_prev_partition(self):
        """If a key-routed send or another caller already rotated the
        sticky between when we picked and when on_new_batch fires, the
        hook is a no-op (don't override their choice)."""
        sticky = StickyPartitioner()
        all_partitions = list(range(10))
        cluster = _cluster('t', all_partitions)
        p1 = sticky.partition('t', None, None, 'msg', b'msg', cluster)
        # Simulate someone else rotating away.
        sticky._sticky['t'] = (p1 + 1) % len(all_partitions)
        current = sticky._sticky['t']
        sticky.on_new_batch('t', cluster, prev_partition=p1)
        assert sticky._sticky['t'] == current, 'should not overwrite live sticky'

    def test_no_available_picks_without_avoid(self):
        """When ``available_partitions_for_topic`` returns empty, Java
        picks random % partitions.size() without enforcing ``!= avoid``
        - ensure we mirror that and don't filter the avoided partition
        out of an already-degraded fallback set."""
        sticky = StickyPartitioner()
        cluster = _cluster('t', [0, 1, 2], available=[])
        sticky._sticky['t'] = 1  # pre-seed sticky
        observed = set()
        for _ in range(200):
            partition = sticky._pick_sticky_locked('t', cluster, avoid=1)
            observed.add(partition)
        assert observed == {0, 1, 2}, (
            'avoid should not filter when available is empty; got %s' % observed)

    def test_single_available_partition_repeats_even_if_avoided(self):
        """Single-element ``available`` must always return that one,
        even if it equals ``avoid`` (Java's nextPartition does the same)."""
        sticky = StickyPartitioner()
        cluster = _cluster('t', [0, 1, 2], available=[1])
        for _ in range(50):
            partition = sticky._pick_sticky_locked('t', cluster, avoid=1)
            assert partition == 1

    def test_pick_sticky_unknown_topic_returns_none(self):
        """_pick_sticky_locked is the defensive layer for on_new_batch's
        no-pre-validation call; a topic that disappeared from cluster
        metadata returns None (caller no-ops)."""
        sticky = StickyPartitioner()
        cluster = MagicMock()
        cluster.partitions_for_topic.return_value = None
        assert sticky._pick_sticky_locked('gone', cluster) is None

    def test_thread_safety_under_contention(self):
        """Concurrent ``partition`` + ``on_new_batch`` calls from many
        threads should never raise (no torn read-modify-write) and the
        final ``_sticky[topic]`` value must be one of the valid
        partitions."""
        import threading
        sticky = StickyPartitioner()
        topic = 't'
        partitions = list(range(20))
        cluster = _cluster(topic, partitions)
        errors = []
        stop = threading.Event()

        def hammer():
            try:
                while not stop.is_set():
                    p = sticky.partition(topic, None, None, 'msg', b'msg', cluster)
                    sticky.on_new_batch(topic, cluster, p)
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=hammer) for _ in range(8)]
        for t in threads:
            t.start()
        # Let them race for a moment.
        import time as _time
        _time.sleep(0.2)
        stop.set()
        for t in threads:
            t.join(timeout=2)
        assert not errors, 'concurrent ops raised: %r' % errors
        assert sticky._sticky[topic] in partitions
