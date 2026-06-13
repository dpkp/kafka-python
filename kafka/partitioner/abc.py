from abc import ABC, abstractmethod


class Partitioner(ABC):
    """Base class for pluggable partition selection strategies.

    A :class:`~kafka.KafkaProducer` consults its configured partitioner to
    choose a partition for each record whose ``partition`` was not supplied
    explicitly to :meth:`~kafka.KafkaProducer.send`. Subclass this and
    implement :meth:`partition`, then pass an instance via the
    ``partitioner`` config argument.

    Two built-in implementations are provided: ``DefaultPartitioner``
    (murmur2-hash keyed records, random partition for null keys) and
    ``StickyPartitioner`` (KIP-480; null-key records stick to one partition
    per topic until a new batch is started, then rotate).

    Sticky-style partitioners may additionally implement an optional
    ``on_new_batch(self, topic, cluster, prev_partition)`` hook, which the
    producer calls when a null-key record would have triggered a fresh
    batch, giving the partitioner a chance to rotate off its current sticky
    choice. The hook is looked up with ``getattr``, so it is entirely
    optional.
    """

    @abstractmethod
    def partition(self, topic, key, serialized_key, value, serialized_value, cluster):
        """Choose a partition for a record.

        Arguments:
            topic (str): The topic the record is destined for.
            key: The user-supplied key, before serialization. May be None.
            serialized_key (bytes): The post-serializer key bytes, or None
                when the caller passed ``key=None``.
            value: The user-supplied value, before serialization.
            serialized_value (bytes): The post-serializer value bytes, or
                None when the caller passed ``value=None``.
            cluster (ClusterMetadata): A live cluster snapshot. Use
                ``cluster.partitions_for_topic(topic)`` for all partitions
                and ``cluster.available_partitions_for_topic(topic)`` for
                those whose leader is currently known.

        Returns:
            int: The partition id to assign the record to.

        Raises:
            ValueError: If the topic is not present in cluster metadata.
                Partitioner exceptions surface to the caller via the
                returned :class:`~kafka.producer.future.FutureRecordMetadata`.
        """
        pass

