import glob
import logging
import os
import select
import shlex
import shutil
import socket
import subprocess
import sys
import tempfile
from threading import Thread, Event
import time
import unittest
from urlparse import urlparse

from kafka import *
from kafka.common import *

def get_open_port():
    sock = socket.socket()
    sock.bind(('',0))
    port = sock.getsockname()[1]
    sock.close()
    return port

def build_kafka_classpath():
    baseDir = "./kafka-src"
    jars = []
    jars += glob.glob(os.path.join(baseDir, "project/boot/scala-2.8.0/lib/*.jar"))
    jars += glob.glob(os.path.join(baseDir, "core/target/scala_2.8.0/*.jar"))
    jars += glob.glob(os.path.join(baseDir, "core/lib/*.jar"))
    jars += glob.glob(os.path.join(baseDir, "core/lib_managed/scala_2.8.0/compile/*.jar"))
    jars += glob.glob(os.path.join(baseDir, "core/target/scala-2.8.0/kafka_2.8.0-*.jar"))
    jars += glob.glob(os.path.join(baseDir, "/Users/mumrah/.ivy2/cache/org.slf4j/slf4j-api/jars/slf4j-api-1.6.4.jar"))
    cp = ":".join(["."] + [os.path.abspath(jar) for jar in jars])
    cp += ":" + os.path.abspath(os.path.join(baseDir, "conf/log4j.properties"))
    return cp

class KafkaFixture(Thread):
    def __init__(self, host, port, broker_id, zk_chroot=None):
        Thread.__init__(self)
        self.broker_id = broker_id
        self.zk_chroot = zk_chroot
        self.port = port
        self.capture = ""
        self.shouldDie = Event()
        self.tmpDir = tempfile.mkdtemp()
        print("tmp dir: %s" % self.tmpDir)

    def run(self):
        # Create the log directory
        logDir = os.path.join(self.tmpDir, 'logs')
        os.mkdir(logDir)
        stdout = open(os.path.join(logDir, 'stdout'), 'w')

        # Create the config file
        if self.zk_chroot is None:
            self.zk_chroot= "kafka-python_%s" % self.tmpDir.replace("/", "_")
        logConfig = "test/resources/log4j.properties"
        configFile = os.path.join(self.tmpDir, 'server.properties')
        f = open('test/resources/server.properties', 'r')
        props = f.read()
        f = open(configFile, 'w')
        f.write(props % {'broker.id': self.broker_id,
                         'kafka.port': self.port,
                         'kafka.tmp.dir': logDir,
                         'kafka.partitions': 2,
                         'zk.chroot': self.zk_chroot})
        f.close()

        cp = build_kafka_classpath()
    
        # Create the Zookeeper chroot
        args = shlex.split("java -cp %s org.apache.zookeeper.ZooKeeperMain create /%s kafka-python" % (cp, self.zk_chroot))
        proc = subprocess.Popen(args)
        ret = proc.wait()
        if ret != 0:
            sys.exit(1)


        # Start Kafka
        args = shlex.split("java -Xmx256M -server -Dlog4j.configuration=%s -cp %s kafka.Kafka %s" % (logConfig, cp, configFile))
        proc = subprocess.Popen(args, bufsize=1, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env={"JMX_PORT":"%d" % get_open_port()})

        killed = False
        while True:
            (rlist, wlist, xlist) = select.select([proc.stdout], [], [], 1)
            if proc.stdout in rlist:
                read = proc.stdout.readline()
                stdout.write(read)
                stdout.flush()
                self.capture += read

            if self.shouldDie.is_set():
                proc.terminate()
                killed = True

            if proc.poll() is not None:
                #shutil.rmtree(self.tmpDir)
                if killed:
                    break
                else:
                    raise RuntimeError("Kafka died. Aborting.")

    def wait_for(self, target, timeout=10):
        t1 = time.time()
        while True:
            t2 = time.time()
            if t2-t1 >= timeout:
                return False
            if target in self.capture:
                return True
            time.sleep(0.100)

    def close(self):
        self.shouldDie.set()

class ExternalKafkaFixture(object):
    def __init__(self, host, port):
        print("Using already running Kafka at %s:%d" % (host, port))

    def close(self):
        pass


class TestKafkaClient(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if os.environ.has_key('KAFKA_URI'):
            parse = urlparse(os.environ['KAFKA_URI'])
            (host, port) = (parse.hostname, parse.port)
            cls.server = ExternalKafkaFixture(host, port)
            cls.client = KafkaClient(host, port)
        else:
            port = get_open_port()
            cls.server = KafkaFixture("localhost", port, 0)
            cls.server.start()
            cls.server.wait_for("Kafka server started")
            cls.client = KafkaClient("localhost", port)

    @classmethod
    def tearDownClass(cls):
        cls.client.close()
        cls.server.close()

    #####################
    #   Produce Tests   #
    #####################

    def test_produce_many_simple(self):
        produce = ProduceRequest("test_produce_many_simple", 0, messages=[
            create_message("Test message %d" % i) for i in range(100)
        ]) 

        for resp in self.client.send_produce_request([produce]):
            self.assertEquals(resp.error, 0)
            self.assertEquals(resp.offset, 0)

        (offset, ) = self.client.send_offset_request([OffsetRequest("test_produce_many_simple", 0, -1, 1)])
        self.assertEquals(offset.offsets[0], 100)

        for resp in self.client.send_produce_request([produce]):
            self.assertEquals(resp.error, 0)
            self.assertEquals(resp.offset, 100)

        (offset, ) = self.client.send_offset_request([OffsetRequest("test_produce_many_simple", 0, -1, 1)])
        self.assertEquals(offset.offsets[0], 200)

        for resp in self.client.send_produce_request([produce]):
            self.assertEquals(resp.error, 0)
            self.assertEquals(resp.offset, 200)

        (offset, ) = self.client.send_offset_request([OffsetRequest("test_produce_many_simple", 0, -1, 1)])
        self.assertEquals(offset.offsets[0], 300)

    def test_produce_10k_simple(self):
        produce = ProduceRequest("test_produce_10k_simple", 0, messages=[
            create_message("Test message %d" % i) for i in range(10000)
        ]) 

        for resp in self.client.send_produce_request([produce]):
            self.assertEquals(resp.error, 0)
            self.assertEquals(resp.offset, 0)

        (offset, ) = self.client.send_offset_request([OffsetRequest("test_produce_10k_simple", 0, -1, 1)])
        self.assertEquals(offset.offsets[0], 10000)

    def test_produce_many_gzip(self):
        message1 = create_gzip_message(["Gzipped 1 %d" % i for i in range(100)])
        message2 = create_gzip_message(["Gzipped 2 %d" % i for i in range(100)])

        produce = ProduceRequest("test_produce_many_gzip", 0, messages=[message1, message2])

        for resp in self.client.send_produce_request([produce]):
            self.assertEquals(resp.error, 0)
            self.assertEquals(resp.offset, 0)

        (offset, ) = self.client.send_offset_request([OffsetRequest("test_produce_many_gzip", 0, -1, 1)])
        self.assertEquals(offset.offsets[0], 200)

    def test_produce_many_snappy(self):
        message1 = create_snappy_message(["Snappy 1 %d" % i for i in range(100)])
        message2 = create_snappy_message(["Snappy 2 %d" % i for i in range(100)])

        produce = ProduceRequest("test_produce_many_snappy", 0, messages=[message1, message2])

        for resp in self.client.send_produce_request([produce]):
            self.assertEquals(resp.error, 0)
            self.assertEquals(resp.offset, 0)

        (offset, ) = self.client.send_offset_request([OffsetRequest("test_produce_many_snappy", 0, -1, 1)])
        self.assertEquals(offset.offsets[0], 200)

    def test_produce_mixed(self):
        message1 = create_message("Just a plain message")
        message2 = create_gzip_message(["Gzipped %d" % i for i in range(100)])
        message3 = create_snappy_message(["Snappy %d" % i for i in range(100)])

        produce = ProduceRequest("test_produce_mixed", 0, messages=[message1, message2, message3])

        for resp in self.client.send_produce_request([produce]):
            self.assertEquals(resp.error, 0)
            self.assertEquals(resp.offset, 0)

        (offset, ) = self.client.send_offset_request([OffsetRequest("test_produce_mixed", 0, -1, 1)])
        self.assertEquals(offset.offsets[0], 201)


    def test_produce_100k_gzipped(self):
        req1 = ProduceRequest("test_produce_100k_gzipped", 0, messages=[
            create_gzip_message(["Gzipped batch 1, message %d" % i for i in range(50000)])
        ])

        for resp in self.client.send_produce_request([req1]):
            self.assertEquals(resp.error, 0)
            self.assertEquals(resp.offset, 0)

        (offset, ) = self.client.send_offset_request([OffsetRequest("test_produce_100k_gzipped", 0, -1, 1)])
        self.assertEquals(offset.offsets[0], 50000)

        req2 = ProduceRequest("test_produce_100k_gzipped", 0, messages=[
            create_gzip_message(["Gzipped batch 2, message %d" % i for i in range(50000)])
        ])

        for resp in self.client.send_produce_request([req2]):
            self.assertEquals(resp.error, 0)
            self.assertEquals(resp.offset, 50000)

        (offset, ) = self.client.send_offset_request([OffsetRequest("test_produce_100k_gzipped", 0, -1, 1)])
        self.assertEquals(offset.offsets[0], 100000)

    #####################
    #   Consume Tests   #
    #####################

    def test_consume_none(self):
        fetch = FetchRequest("test_consume_none", 0, 0, 1024)

        fetch_resp = self.client.send_fetch_request([fetch])[0]
        self.assertEquals(fetch_resp.error, 0)
        self.assertEquals(fetch_resp.topic, "test_consume_none")
        self.assertEquals(fetch_resp.partition, 0)
        
        messages = list(fetch_resp.messages)
        self.assertEquals(len(messages), 0)

    def test_produce_consume(self):
        produce = ProduceRequest("test_produce_consume", 0, messages=[
            create_message("Just a test message"),
            create_message("Message with a key", "foo"),
        ])

        for resp in self.client.send_produce_request([produce]):
            self.assertEquals(resp.error, 0)
            self.assertEquals(resp.offset, 0)

        fetch = FetchRequest("test_produce_consume", 0, 0, 1024)

        fetch_resp = self.client.send_fetch_request([fetch])[0]
        self.assertEquals(fetch_resp.error, 0)

        messages = list(fetch_resp.messages)
        self.assertEquals(len(messages), 2)
        self.assertEquals(messages[0].offset, 0)
        self.assertEquals(messages[0].message.value, "Just a test message")
        self.assertEquals(messages[0].message.key, None)
        self.assertEquals(messages[1].offset, 1)
        self.assertEquals(messages[1].message.value, "Message with a key")
        self.assertEquals(messages[1].message.key, "foo")

    def test_produce_consume_many(self):
        produce = ProduceRequest("test_produce_consume_many", 0, messages=[
            create_message("Test message %d" % i) for i in range(100)
        ])

        for resp in self.client.send_produce_request([produce]):
            self.assertEquals(resp.error, 0)
            self.assertEquals(resp.offset, 0)

        # 1024 is not enough for 100 messages...
        fetch1 = FetchRequest("test_produce_consume_many", 0, 0, 1024)

        (fetch_resp1,) = self.client.send_fetch_request([fetch1])
    
        self.assertEquals(fetch_resp1.error, 0)
        self.assertEquals(fetch_resp1.highwaterMark, 100)
        messages = list(fetch_resp1.messages)
        self.assertTrue(len(messages) < 100)

        # 10240 should be enough
        fetch2 = FetchRequest("test_produce_consume_many", 0, 0, 10240)
        (fetch_resp2,) = self.client.send_fetch_request([fetch2])

        self.assertEquals(fetch_resp2.error, 0)
        self.assertEquals(fetch_resp2.highwaterMark, 100)
        messages = list(fetch_resp2.messages)
        self.assertEquals(len(messages), 100)
        for i, message in enumerate(messages):
            self.assertEquals(message.offset, i)
            self.assertEquals(message.message.value, "Test message %d" % i)
            self.assertEquals(message.message.key, None)

    def test_produce_consume_two_partitions(self):
        produce1 = ProduceRequest("test_produce_consume_two_partitions", 0, messages=[
            create_message("Partition 0 %d" % i) for i in range(10)
        ])
        produce2 = ProduceRequest("test_produce_consume_two_partitions", 1, messages=[
            create_message("Partition 1 %d" % i) for i in range(10)
        ])

        for resp in self.client.send_produce_request([produce1, produce2]):
            self.assertEquals(resp.error, 0)
            self.assertEquals(resp.offset, 0)

        fetch1 = FetchRequest("test_produce_consume_two_partitions", 0, 0, 1024)
        fetch2 = FetchRequest("test_produce_consume_two_partitions", 1, 0, 1024)
        fetch_resp1, fetch_resp2 = self.client.send_fetch_request([fetch1, fetch2])
        self.assertEquals(fetch_resp1.error, 0)
        self.assertEquals(fetch_resp1.highwaterMark, 10)
        messages = list(fetch_resp1.messages)
        self.assertEquals(len(messages), 10)
        for i, message in enumerate(messages):
            self.assertEquals(message.offset, i)
            self.assertEquals(message.message.value, "Partition 0 %d" % i)
            self.assertEquals(message.message.key, None)
        self.assertEquals(fetch_resp2.error, 0)
        self.assertEquals(fetch_resp2.highwaterMark, 10)
        messages = list(fetch_resp2.messages)
        self.assertEquals(len(messages), 10)
        for i, message in enumerate(messages):
            self.assertEquals(message.offset, i)
            self.assertEquals(message.message.value, "Partition 1 %d" % i)
            self.assertEquals(message.message.key, None)

    ####################
    #   Offset Tests   #
    ####################

    def test_commit_fetch_offsets(self):
        req = OffsetCommitRequest("test_commit_fetch_offsets", 0, 42, "metadata")
        (resp,) = self.client.send_offset_commit_request("group", [req])
        self.assertEquals(resp.error, 0)

        req = OffsetFetchRequest("test_commit_fetch_offsets", 0)
        (resp,) = self.client.send_offset_fetch_request("group", [req])
        self.assertEquals(resp.error, 0)
        self.assertEquals(resp.offset, 42)
        self.assertEquals(resp.metadata, "") # Metadata isn't stored for now

    # Producer Tests

    def test_simple_producer(self):
        producer = SimpleProducer(self.client, "test_simple_producer")
        producer.send_messages("one", "two")
        producer.send_messages("three")

        fetch1 = FetchRequest("test_simple_producer", 0, 0, 1024)
        fetch2 = FetchRequest("test_simple_producer", 1, 0, 1024)
        fetch_resp1, fetch_resp2 = self.client.send_fetch_request([fetch1, fetch2])
        self.assertEquals(fetch_resp1.error, 0)
        self.assertEquals(fetch_resp1.highwaterMark, 2)
        messages = list(fetch_resp1.messages)
        self.assertEquals(len(messages), 2)
        self.assertEquals(messages[0].message.value, "one")
        self.assertEquals(messages[1].message.value, "two")
        self.assertEquals(fetch_resp2.error, 0)
        self.assertEquals(fetch_resp2.highwaterMark, 1)
        messages = list(fetch_resp2.messages)
        self.assertEquals(len(messages), 1)
        self.assertEquals(messages[0].message.value, "three")

class TestConsumer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Broker 0
        port = get_open_port()
        cls.server1 = KafkaFixture("localhost", port, 0)
        cls.server1.start()
        cls.server1.wait_for("Kafka server started")

        # Broker 1
        zk = cls.server1.zk_chroot
        port = get_open_port()
        cls.server2 = KafkaFixture("localhost", port, 1, zk)
        cls.server2.start()
        cls.server2.wait_for("Kafka server started")

        # Client bootstraps from broker 1
        cls.client = KafkaClient("localhost", port)

    @classmethod
    def tearDownClass(cls):
        cls.client.close()
        cls.server1.close()
        cls.server2.close()

    def test_consumer(self):
        # Produce 100 messages to partition 0
        produce1 = ProduceRequest("test_consumer", 0, messages=[
            create_message("Test message 0 %d" % i) for i in range(100)
        ])

        for resp in self.client.send_produce_request([produce1]):
            self.assertEquals(resp.error, 0)
            self.assertEquals(resp.offset, 0)

        # Produce 100 messages to partition 1
        produce2 = ProduceRequest("test_consumer", 1, messages=[
            create_message("Test message 1 %d" % i) for i in range(100)
        ])

        for resp in self.client.send_produce_request([produce2]):
            self.assertEquals(resp.error, 0)
            self.assertEquals(resp.offset, 0)

        # Start a consumer
        consumer = SimpleConsumer(self.client, "group1", "test_consumer")
        all_messages = []
        for message in consumer:
            all_messages.append(message)

        self.assertEquals(len(all_messages), 200)
        self.assertEquals(len(all_messages), len(set(all_messages))) # make sure there are no dupes

        consumer.seek(-10, 2)
        all_messages = []
        for message in consumer:
            all_messages.append(message)

        self.assertEquals(len(all_messages), 10)

        consumer.seek(-13, 2)
        all_messages = []
        for message in consumer:
            all_messages.append(message)

        self.assertEquals(len(all_messages), 13)

    def test_pending(self):
        # Produce 10 messages to partition 0 and 1

        produce1 = ProduceRequest("test_pending", 0, messages=[
            create_message("Test message 0 %d" % i) for i in range(10)
        ])
        for resp in self.client.send_produce_request([produce1]):
            self.assertEquals(resp.error, 0)
            self.assertEquals(resp.offset, 0)

        produce2 = ProduceRequest("test_pending", 1, messages=[
            create_message("Test message 1 %d" % i) for i in range(10)
        ])
        for resp in self.client.send_produce_request([produce2]):
            self.assertEquals(resp.error, 0)
            self.assertEquals(resp.offset, 0)

        consumer = SimpleConsumer(self.client, "group1", "test_pending")
        self.assertEquals(consumer.pending(), 20)
        self.assertEquals(consumer.pending(partitions=[0]), 10)
        self.assertEquals(consumer.pending(partitions=[1]), 10)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main() 
