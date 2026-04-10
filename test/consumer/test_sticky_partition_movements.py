from kafka.structs import TopicPartition

from kafka.coordinator.assignors.sticky.partition_movements import PartitionMovements


def test_empty_movements_are_sticky():
    partition_movements = PartitionMovements()
    assert partition_movements.are_sticky()


def test_sticky_movements():
    partition_movements = PartitionMovements()
    partition_movements.move_partition(TopicPartition('t', 1), 'C1', 'C2')
    partition_movements.move_partition(TopicPartition('t', 1), 'C2', 'C3')
    partition_movements.move_partition(TopicPartition('t', 1), 'C3', 'C1')
    assert partition_movements.are_sticky()


def test_should_detect_non_sticky_assignment():
    partition_movements = PartitionMovements()
    partition_movements.move_partition(TopicPartition('t', 1), 'C1', 'C2')
    partition_movements.move_partition(TopicPartition('t', 2), 'C2', 'C1')
    assert not partition_movements.are_sticky()
