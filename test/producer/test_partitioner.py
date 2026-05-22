import pytest

from kafka.partitioner import DefaultPartitioner, StickyPartitioner, murmur2


def test_default_partitioner():
    partitioner = DefaultPartitioner()
    all_partitions = available = list(range(100))
    # partitioner should return the same partition for the same key
    p1 = partitioner(b'foo', all_partitions, available)
    p2 = partitioner(b'foo', all_partitions, available)
    assert p1 == p2
    assert p1 in all_partitions

    # when key is None, choose one of available partitions
    assert partitioner(None, all_partitions, [123]) == 123

    # with fallback to all_partitions
    assert partitioner(None, all_partitions, []) in all_partitions


@pytest.mark.parametrize("bytes_payload,partition_number", [
    (b'', 681), (b'a', 524), (b'ab', 434), (b'abc', 107), (b'123456789', 566),
    (b'\x00 ', 742)
])
def test_murmur2_java_compatibility(bytes_payload, partition_number):
    partitioner = DefaultPartitioner()
    all_partitions = available = list(range(1000))
    # compare with output from Kafka's org.apache.kafka.clients.producer.Partitioner
    assert partitioner(bytes_payload, all_partitions, available) == partition_number


def test_murmur2_not_ascii():
    # Verify no regression of murmur2() bug encoding py2 bytes that don't ascii encode
    murmur2(b'\xa4')
    murmur2(b'\x81' * 1000)


# KIP-480 sticky partitioner


class TestStickyPartitioner:
    def test_keyed_records_hash_like_default(self):
        sticky = StickyPartitioner()
        default = DefaultPartitioner()
        all_partitions = available = list(range(100))
        assert (sticky.partition('t', b'foo', all_partitions, available)
                == default(b'foo', all_partitions, available))
        assert (sticky.partition('t', b'bar', all_partitions, available)
                == default(b'bar', all_partitions, available))

    def test_null_key_sticks_until_second_on_new_batch(self):
        """The *first* on_new_batch event is absorbed (it's the first
        batch being opened on the newly-picked sticky - exactly what we
        want). Rotation only happens on the *second* event, which
        signals that the previous batch filled up. Without this, the
        partitioner would rotate on every record whose partition had
        no existing batch."""
        sticky = StickyPartitioner()
        all_partitions = available = list(range(10))
        p1 = sticky.partition('t', None, all_partitions, available)
        for _ in range(50):
            assert sticky.partition('t', None, all_partitions, available) == p1

        # First on_new_batch: opens the first batch on p1 - no rotation.
        sticky.on_new_batch('t', all_partitions, p1)
        assert sticky.partition('t', None, all_partitions, available) == p1

        # Second on_new_batch: previous batch filled - rotate.
        sticky.on_new_batch('t', all_partitions, p1)
        p2 = sticky.partition('t', None, all_partitions, available)
        assert p2 != p1, 'second on_new_batch should rotate'
        for _ in range(50):
            assert sticky.partition('t', None, all_partitions, available) == p2

    def test_per_topic_state_independent(self):
        """Stickiness is per-topic; rotating one topic doesn't affect another."""
        sticky = StickyPartitioner()
        all_partitions = available = list(range(10))
        p_a = sticky.partition('a', None, all_partitions, available)
        p_b = sticky.partition('b', None, all_partitions, available)
        # Two on_new_batch events on 'a' to actually rotate it.
        sticky.on_new_batch('a', all_partitions, p_a)
        sticky.on_new_batch('a', all_partitions, p_a)
        # 'b' is untouched.
        assert sticky.partition('b', None, all_partitions, available) == p_b

    def test_unavailable_sticky_partition_repicks(self):
        """If the stuck partition's leader becomes unavailable, the next
        partition() call repicks from the available set."""
        sticky = StickyPartitioner()
        all_partitions = list(range(10))
        p1 = sticky.partition('t', None, all_partitions, all_partitions)
        # Now only partitions != p1 are available.
        available = [p for p in all_partitions if p != p1]
        p2 = sticky.partition('t', None, all_partitions, available)
        assert p2 != p1
        assert p2 in available

    def test_single_partition_topic_cannot_rotate(self):
        """on_new_batch on a single-partition topic just keeps the same
        partition - there's nothing else to rotate to."""
        sticky = StickyPartitioner()
        all_partitions = available = [0]
        assert sticky.partition('t', None, all_partitions, available) == 0
        sticky.on_new_batch('t', all_partitions, 0)
        assert sticky.partition('t', None, all_partitions, available) == 0

    def test_on_new_batch_ignores_stale_prev_partition(self):
        """If a key-routed send or another caller already rotated the
        sticky between when we picked and when on_new_batch fires, the
        hook is a no-op (don't override their choice)."""
        sticky = StickyPartitioner()
        all_partitions = available = list(range(10))
        p1 = sticky.partition('t', None, all_partitions, available)
        # Simulate someone else rotating away.
        sticky._sticky['t'] = (p1 + 1) % len(all_partitions)
        current = sticky._sticky['t']
        sticky.on_new_batch('t', all_partitions, prev_partition=p1)
        assert sticky._sticky['t'] == current, 'should not overwrite live sticky'

    def test_legacy_callable_interface_still_works(self):
        """A user-supplied custom partitioner written against the old
        callable signature must keep working. We exercise that shim
        via StickyPartitioner itself (it implements both forms)."""
        sticky = StickyPartitioner()
        all_partitions = available = list(range(100))
        # __call__ (no topic) ignores stickiness; just verify it returns
        # a valid partition for both keyed and null-key inputs.
        assert sticky(b'foo', all_partitions, available) in all_partitions
        assert sticky(None, all_partitions, available) in all_partitions
