import os
import random
import struct
import unittest2
import kafka.conn

class ConnTest(unittest2.TestCase):
    def test_collect_hosts__happy_path(self):
        hosts = "localhost:1234,localhost"
        results = kafka.conn.collect_hosts(hosts)

        self.assertEqual(set(results), set([
            ('localhost', 1234),
            ('localhost', 9092),
        ]))

    def test_collect_hosts__string_list(self):
        hosts = [
            'localhost:1234',
            'localhost',
        ]

        results = kafka.conn.collect_hosts(hosts)

        self.assertEqual(set(results), set([
            ('localhost', 1234),
            ('localhost', 9092),
        ]))

    def test_collect_hosts__with_spaces(self):
        hosts = "localhost:1234, localhost"
        results = kafka.conn.collect_hosts(hosts)

        self.assertEqual(set(results), set([
            ('localhost', 1234),
            ('localhost', 9092),
        ]))

    @unittest2.skip("Not Implemented")
    def test_send(self):
        pass

    @unittest2.skip("Not Implemented")
    def test_send__reconnects_on_dirty_conn(self):
        pass

    @unittest2.skip("Not Implemented")
    def test_send__failure_sets_dirty_connection(self):
        pass

    @unittest2.skip("Not Implemented")
    def test_recv(self):
        pass

    @unittest2.skip("Not Implemented")
    def test_recv__reconnects_on_dirty_conn(self):
        pass

    @unittest2.skip("Not Implemented")
    def test_recv__failure_sets_dirty_connection(self):
        pass

    @unittest2.skip("Not Implemented")
    def test_recv__doesnt_consume_extra_data_in_stream(self):
        pass

    @unittest2.skip("Not Implemented")
    def test_close__object_is_reusable(self):
        pass
