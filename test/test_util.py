# -*- coding: utf-8 -*-
import struct

import six
from . import unittest

import kafka.errors
import kafka.util
import kafka.structs


class UtilTest(unittest.TestCase):
    @unittest.skip("Unwritten")
    def test_relative_unpack(self):
        pass

    def test_write_int_string(self):
        self.assertEqual(
            kafka.util.write_int_string(b'some string'),
            b'\x00\x00\x00\x0bsome string'
        )

    def test_write_int_string__unicode(self):
        with self.assertRaises(TypeError) as cm:
            kafka.util.write_int_string(u'unicode')
        #: :type: TypeError
        te = cm.exception
        if six.PY2:
            self.assertIn('unicode', str(te))
        else:
            self.assertIn('str', str(te))
        self.assertIn('to be bytes', str(te))

    def test_write_int_string__empty(self):
        self.assertEqual(
            kafka.util.write_int_string(b''),
            b'\x00\x00\x00\x00'
        )

    def test_write_int_string__null(self):
        self.assertEqual(
            kafka.util.write_int_string(None),
            b'\xff\xff\xff\xff'
        )

    def test_read_int_string(self):
        self.assertEqual(kafka.util.read_int_string(b'\xff\xff\xff\xff', 0), (None, 4))
        self.assertEqual(kafka.util.read_int_string(b'\x00\x00\x00\x00', 0), (b'', 4))
        self.assertEqual(kafka.util.read_int_string(b'\x00\x00\x00\x0bsome string', 0), (b'some string', 15))

    def test_read_int_string__insufficient_data(self):
        with self.assertRaises(kafka.errors.BufferUnderflowError):
            kafka.util.read_int_string(b'\x00\x00\x00\x021', 0)

    def test_write_short_string(self):
        self.assertEqual(
            kafka.util.write_short_string(b'some string'),
            b'\x00\x0bsome string'
        )

    def test_write_short_string__unicode(self):
        with self.assertRaises(TypeError) as cm:
            kafka.util.write_short_string(u'hello')
        #: :type: TypeError
        te = cm.exception
        if six.PY2:
            self.assertIn('unicode', str(te))
        else:
            self.assertIn('str', str(te))
        self.assertIn('to be bytes', str(te))

    def test_write_short_string__empty(self):
        self.assertEqual(
            kafka.util.write_short_string(b''),
            b'\x00\x00'
        )

    def test_write_short_string__null(self):
        self.assertEqual(
            kafka.util.write_short_string(None),
            b'\xff\xff'
        )

    def test_write_short_string__too_long(self):
        with self.assertRaises(struct.error):
            kafka.util.write_short_string(b' ' * 33000)

    def test_read_short_string(self):
        self.assertEqual(kafka.util.read_short_string(b'\xff\xff', 0), (None, 2))
        self.assertEqual(kafka.util.read_short_string(b'\x00\x00', 0), (b'', 2))
        self.assertEqual(kafka.util.read_short_string(b'\x00\x0bsome string', 0), (b'some string', 13))

    def test_read_int_string__insufficient_data2(self):
        with self.assertRaises(kafka.errors.BufferUnderflowError):
            kafka.util.read_int_string('\x00\x021', 0)

    def test_relative_unpack2(self):
        self.assertEqual(
            kafka.util.relative_unpack('>hh', b'\x00\x01\x00\x00\x02', 0),
            ((1, 0), 4)
        )

    def test_relative_unpack3(self):
        with self.assertRaises(kafka.errors.BufferUnderflowError):
            kafka.util.relative_unpack('>hh', '\x00', 0)

    def test_group_by_topic_and_partition(self):
        t = kafka.structs.TopicPartition

        l = [
            t("a", 1),
            t("a", 2),
            t("a", 3),
            t("b", 3),
        ]

        self.assertEqual(kafka.util.group_by_topic_and_partition(l), {
            "a": {
                1: t("a", 1),
                2: t("a", 2),
                3: t("a", 3),
            },
            "b": {
                3: t("b", 3),
            }
        })

        # should not be able to group duplicate topic-partitions
        t1 = t("a", 1)
        with self.assertRaises(AssertionError):
            kafka.util.group_by_topic_and_partition([t1, t1])
