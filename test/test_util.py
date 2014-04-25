import os
import random
import struct
import unittest2
import kafka.util
import kafka.common

class UtilTest(unittest2.TestCase):
    @unittest2.skip("Unwritten")
    def test_relative_unpack(self):
        pass

    def test_write_int_string(self):
        self.assertEqual(
            kafka.util.write_int_string('some string'),
            '\x00\x00\x00\x0bsome string'
        )

    def test_write_int_string__empty(self):
        self.assertEqual(
            kafka.util.write_int_string(''),
            '\x00\x00\x00\x00'
        )

    def test_write_int_string__null(self):
        self.assertEqual(
            kafka.util.write_int_string(None),
            '\xff\xff\xff\xff'
        )

    def test_read_int_string(self):
        self.assertEqual(kafka.util.read_int_string('\xff\xff\xff\xff', 0), (None, 4))
        self.assertEqual(kafka.util.read_int_string('\x00\x00\x00\x00', 0), ('', 4))
        self.assertEqual(kafka.util.read_int_string('\x00\x00\x00\x0bsome string', 0), ('some string', 15))

    def test_read_int_string__insufficient_data(self):
        with self.assertRaises(kafka.common.BufferUnderflowError):
            kafka.util.read_int_string('\x00\x00\x00\x021', 0)

    def test_write_short_string(self):
        self.assertEqual(
            kafka.util.write_short_string('some string'),
            '\x00\x0bsome string'
        )

    def test_write_short_string__empty(self):
        self.assertEqual(
            kafka.util.write_short_string(''),
            '\x00\x00'
        )

    def test_write_short_string__null(self):
        self.assertEqual(
            kafka.util.write_short_string(None),
            '\xff\xff'
        )

    def test_write_short_string__too_long(self):
        with self.assertRaises(struct.error):
            kafka.util.write_short_string(' ' * 33000)

    def test_read_short_string(self):
        self.assertEqual(kafka.util.read_short_string('\xff\xff', 0), (None, 2))
        self.assertEqual(kafka.util.read_short_string('\x00\x00', 0), ('', 2))
        self.assertEqual(kafka.util.read_short_string('\x00\x0bsome string', 0), ('some string', 13))

    def test_read_int_string__insufficient_data(self):
        with self.assertRaises(kafka.common.BufferUnderflowError):
            kafka.util.read_int_string('\x00\x021', 0)

    def test_relative_unpack(self):
        self.assertEqual(
            kafka.util.relative_unpack('>hh', '\x00\x01\x00\x00\x02', 0),
            ((1, 0), 4)
        )

    def test_relative_unpack(self):
        with self.assertRaises(kafka.common.BufferUnderflowError):
            kafka.util.relative_unpack('>hh', '\x00', 0)


    def test_group_by_topic_and_partition(self):
        t = kafka.common.TopicAndPartition

        l = [
            t("a", 1),
            t("a", 1),
            t("a", 2),
            t("a", 3),
            t("b", 3),
        ]

        self.assertEqual(kafka.util.group_by_topic_and_partition(l), {
            "a" : {
                1 : t("a", 1),
                2 : t("a", 2),
                3 : t("a", 3),
            },
            "b" : {
                3 : t("b", 3),
            }
        })
