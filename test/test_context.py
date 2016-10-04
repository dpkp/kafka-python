"""
OffsetCommitContext tests.
"""
from . import unittest

from mock import MagicMock, patch

from kafka.context import OffsetCommitContext
from kafka.errors import OffsetOutOfRangeError


class TestOffsetCommitContext(unittest.TestCase):
    """
    OffsetCommitContext tests.
    """

    def setUp(self):
        self.client = MagicMock()
        self.consumer = MagicMock()
        self.topic = "topic"
        self.group = "group"
        self.partition = 0
        self.consumer.topic = self.topic
        self.consumer.group = self.group
        self.consumer.client = self.client
        self.consumer.offsets = {self.partition: 0}
        self.context = OffsetCommitContext(self.consumer)

    def test_noop(self):
        """
        Should revert consumer after context exit with no mark() call.
        """
        with self.context:
            # advance offset
            self.consumer.offsets = {self.partition: 1}

        # offset restored
        self.assertEqual(self.consumer.offsets, {self.partition: 0})
        # and seek called with relative zero delta
        self.assertEqual(self.consumer.seek.call_count, 1)
        self.assertEqual(self.consumer.seek.call_args[0], (0, 1))

    def test_mark(self):
        """
        Should remain at marked location ater context exit.
        """
        with self.context as context:
            context.mark(self.partition, 0)
            # advance offset
            self.consumer.offsets = {self.partition: 1}

        # offset sent to client
        self.assertEqual(self.client.send_offset_commit_request.call_count, 1)

        # offset remains advanced
        self.assertEqual(self.consumer.offsets, {self.partition: 1})

        # and seek called with relative zero delta
        self.assertEqual(self.consumer.seek.call_count, 1)
        self.assertEqual(self.consumer.seek.call_args[0], (0, 1))

    def test_mark_multiple(self):
        """
        Should remain at highest marked location after context exit.
        """
        with self.context as context:
            context.mark(self.partition, 0)
            context.mark(self.partition, 1)
            context.mark(self.partition, 2)
            # advance offset
            self.consumer.offsets = {self.partition: 3}

        # offset sent to client
        self.assertEqual(self.client.send_offset_commit_request.call_count, 1)

        # offset remains advanced
        self.assertEqual(self.consumer.offsets, {self.partition: 3})

        # and seek called with relative zero delta
        self.assertEqual(self.consumer.seek.call_count, 1)
        self.assertEqual(self.consumer.seek.call_args[0], (0, 1))

    def test_rollback(self):
        """
        Should rollback to initial offsets on context exit with exception.
        """
        with self.assertRaises(Exception):
            with self.context as context:
                context.mark(self.partition, 0)
                # advance offset
                self.consumer.offsets = {self.partition: 1}

                raise Exception("Intentional failure")

        # offset rolled back (ignoring mark)
        self.assertEqual(self.consumer.offsets, {self.partition: 0})

        # and seek called with relative zero delta
        self.assertEqual(self.consumer.seek.call_count, 1)
        self.assertEqual(self.consumer.seek.call_args[0], (0, 1))

    def test_out_of_range(self):
        """
        Should reset to beginning of valid offsets on `OffsetOutOfRangeError`
        """
        def _seek(offset, whence):
            # seek must be called with 0, 0 to find the beginning of the range
            self.assertEqual(offset, 0)
            self.assertEqual(whence, 0)
            # set offsets to something different
            self.consumer.offsets = {self.partition: 100}

        with patch.object(self.consumer, "seek", _seek):
            with self.context:
                raise OffsetOutOfRangeError()

        self.assertEqual(self.consumer.offsets, {self.partition: 100})
