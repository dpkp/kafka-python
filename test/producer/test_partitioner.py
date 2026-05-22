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

    def test_null_key_sticks_until_on_new_batch(self):
        """A stream of null-key records pins to the chosen partition;
        ``on_new_batch`` rotates immediately (no "absorb first event"
        hack - KafkaProducer.send invokes the hook only on the
        abort-for-new-batch retry path, matching Java)."""
        sticky = StickyPartitioner()
        all_partitions = available = list(range(10))
        p1 = sticky.partition('t', None, all_partitions, available)
        for _ in range(50):
            assert sticky.partition('t', None, all_partitions, available) == p1

        # on_new_batch rotates immediately.
        sticky.on_new_batch('t', all_partitions, p1)
        p2 = sticky.partition('t', None, all_partitions, available)
        assert p2 != p1, 'on_new_batch should rotate'
        for _ in range(50):
            assert sticky.partition('t', None, all_partitions, available) == p2

    def test_per_topic_state_independent(self):
        """Stickiness is per-topic; rotating one topic doesn't affect another."""
        sticky = StickyPartitioner()
        all_partitions = available = list(range(10))
        p_a = sticky.partition('a', None, all_partitions, available)
        p_b = sticky.partition('b', None, all_partitions, available)
        # Rotate 'a' once.
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

    def test_no_available_picks_without_avoid(self):
        """When ``available`` is empty, Java picks random %
        partitions.size() without enforcing ``!= avoid`` — make sure we
        don't try to filter the avoided partition out of an already-stale
        fallback set."""
        sticky = StickyPartitioner()
        sticky.partition('t', None, [0, 1, 2], [0, 1, 2])
        # Force a state where avoid is set and available is empty; verify
        # the fallback can pick any partition (including ``avoid``).
        observed = set()
        for _ in range(200):
            partition = sticky._pick_sticky_locked(
                't', [0, 1, 2], available=[], avoid=1)
            observed.add(partition)
        # All three should appear over many iterations — proves we're not
        # filtering ``avoid`` out of the all-partitions fallback.
        assert observed == {0, 1, 2}, (
            'avoid should not filter when available is empty; got %s' % observed)

    def test_single_available_partition_repeats_even_if_avoided(self):
        """Single-element ``available`` must always return that one,
        even if it equals ``avoid`` (Java's nextPartition does the same)."""
        sticky = StickyPartitioner()
        for _ in range(50):
            partition = sticky._pick_sticky_locked(
                't', [0, 1, 2], available=[1], avoid=1)
            assert partition == 1

    def test_thread_safety_under_contention(self):
        """Concurrent ``partition`` + ``on_new_batch`` calls from many
        threads should never raise (no torn read-modify-write) and the
        final ``_sticky[topic]`` value must be one of the valid
        partitions."""
        import threading
        sticky = StickyPartitioner()
        topic = 't'
        partitions = list(range(20))
        errors = []
        stop = threading.Event()

        def hammer():
            try:
                while not stop.is_set():
                    p = sticky.partition(topic, None, partitions, partitions)
                    sticky.on_new_batch(topic, partitions, p)
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
