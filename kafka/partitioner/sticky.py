"""KIP-480 sticky partitioner.

Records with a non-None key are hashed to a partition just like
:class:`~kafka.partitioner.default.DefaultPartitioner`. Records with a
None key go to a *sticky* partition - i.e. the same partition is reused
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
import threading

from kafka.partitioner.default import murmur2


class StickyPartitioner:
    """Partitioner that sticks null-key records to one partition per
    topic until ``on_new_batch`` rotates it.

    Thread-safety: ``_sticky`` mutations are protected by ``_lock`` so
    concurrent ``send()`` callers can't observe a torn read-modify-write.
    """

    def __init__(self):
        self._sticky = {}  # topic -> partition_id
        self._lock = threading.Lock()

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
        with self._lock:
            partition = self._sticky.get(topic)
            if partition is not None:
                if available:
                    if partition in available:
                        return partition
                elif partition in all_partitions:
                    return partition
                # Stale (leader unavailable, topic shrunk); fall through to re-pick.
            return self._pick_sticky_locked(topic, all_partitions, available)

    def on_new_batch(self, topic, all_partitions, prev_partition):
        """Hook called by ``KafkaProducer`` on the abort-for-new-batch
        retry path: rotate the sticky for ``topic`` so the next
        null-key record lands on a different partition.

        Stale events (where another thread already rotated us off
        ``prev_partition``) are no-ops.
        """
        with self._lock:
            if self._sticky.get(topic) != prev_partition:
                # Another caller already rotated us; don't override.
                return
            self._pick_sticky_locked(topic, all_partitions, None,
                                     avoid=prev_partition)

    def _pick_sticky_locked(self, topic, all_partitions, available, avoid=None):
        """Pick a new sticky partition for ``topic``. Must be called with
        ``self._lock`` held."""
        if available:
            if len(available) == 1:
                partition = available[0]
            else:
                # >= 2 available: pick uniformly, avoiding ``avoid`` if set.
                candidates = [p for p in available if p != avoid] if avoid is not None else available
                if not candidates:
                    candidates = available
                partition = random.choice(candidates)
        else:
            # No partitions are currently available - pick from the full
            # set without enforcing ``!= avoid``
            partition = random.choice(all_partitions)
        self._sticky[topic] = partition
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
