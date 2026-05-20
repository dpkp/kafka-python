"""KIP-480 sticky partitioner.

Records with a non-None key are hashed to a partition just like
:class:`~kafka.partitioner.default.DefaultPartitioner`. Records with a
None key go to a *sticky* partition — i.e. the same partition is reused
for every null-key record on a topic until KafkaProducer signals that a
batch has been completed (via :meth:`StickyPartitioner.on_new_batch`),
at which point a different partition is picked.

The goal is to give the RecordAccumulator larger, denser batches for
null-key sends so per-batch overhead (CRC, compression, broker
round-trip) is amortized across more records. Java's benchmark in
KIP-480 reported substantial throughput/latency improvements over the
default-random behavior, though kafka-python is unlikely to see similar
improvements while predominantly CPU-bound on per-record overhead.
"""

import random

from kafka.partitioner.default import murmur2


class StickyPartitioner:
    """Partitioner that sticks null-key records to one partition per
    topic until ``on_new_batch`` rotates it.

    Thread-safety: the underlying ``_sticky`` dict is mutated only by
    individually-atomic Python ops (get / setitem / contains). Two
    concurrent partitioners may pick different sticky partitions; the
    last write wins and both choices are valid, so no lock is needed.
    """

    def __init__(self):
        self._sticky = {}  # topic -> partition_id
        # Java's accumulator distinguishes "first batch created on a
        # partition" (no rotation) from "existing batch filled, new one
        # being created" (rotate). Our accumulator collapses both into
        # ``new_batch_created=True``, so the partitioner absorbs the
        # *first* on_new_batch event per sticky and only rotates on the
        # subsequent one. Without this, we'd rotate on every record
        # whose partition has no existing batch, defeating stickiness.
        self._sticky_seen_batch = set()  # topics whose current sticky has had >=1 batch event

    def partition(self, topic, key, all_partitions, available):
        """Choose a partition for the next record.

        Arguments:
            topic (str): topic to partition on.
            key (bytes or None): partitioning key.
            all_partitions (list[int]): every partition ID for the topic,
                sorted ascending.
            available (list[int]): partitions whose leader is currently
                known (may be empty when metadata is stale).

        Returns:
            int: chosen partition ID.
        """
        if key is not None:
            idx = murmur2(key)
            idx &= 0x7fffffff
            idx %= len(all_partitions)
            return all_partitions[idx]
        # Null key: reuse the sticky partition if still valid.
        partition = self._sticky.get(topic)
        if partition is not None:
            if available:
                if partition in available:
                    return partition
            elif partition in all_partitions:
                return partition
            # Stale (leader unavailable, topic shrunk); fall through to re-pick.
        return self._pick_sticky(topic, all_partitions, available)

    def on_new_batch(self, topic, all_partitions, prev_partition):
        """Hook called by ``KafkaProducer`` when the accumulator just
        opened a new batch for ``topic`` on ``prev_partition``.

        The *first* event per sticky is absorbed silently: it
        corresponds to the first batch ever being created on the
        partition we just picked, which is expected — we want
        subsequent records to keep landing there. The *second* event
        means the previous batch filled up and a new one was opened;
        that's the signal to rotate to a different partition so the
        next records build up a fresh dense batch elsewhere.
        """
        if self._sticky.get(topic) != prev_partition:
            # Someone else (or a key-routed send) already moved us off
            # this partition; don't override their choice.
            return
        if topic not in self._sticky_seen_batch:
            self._sticky_seen_batch.add(topic)
            return
        # Existing batch filled; rotate.
        self._sticky_seen_batch.discard(topic)
        self._pick_sticky(topic, all_partitions, None,
                          avoid=prev_partition)

    def _pick_sticky(self, topic, all_partitions, available, avoid=None):
        pool = available if available else all_partitions
        candidates = [p for p in pool if p != avoid] if avoid is not None else pool
        if not candidates:
            # Single-partition topic, or only the avoid-partition is
            # available — no rotation possible.
            candidates = pool
        partition = random.choice(candidates)
        self._sticky[topic] = partition
        # Reset the seen-batch flag; the new sticky has had no batches yet.
        self._sticky_seen_batch.discard(topic)
        return partition

    # Compatibility shim: legacy code paths that treat partitioners as
    # bare callables (key, all_partitions, available) still work, though
    # they lose the per-topic stickiness.
    def __call__(self, key, all_partitions, available):
        if key is not None:
            idx = murmur2(key)
            idx &= 0x7fffffff
            idx %= len(all_partitions)
            return all_partitions[idx]
        pool = available if available else all_partitions
        return random.choice(pool)
