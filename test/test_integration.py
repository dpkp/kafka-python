import glob
import logging
import os
import re
import select
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid

from urlparse import urlparse

from kafka import *  # noqa
from kafka.common import *  # noqa
from kafka.codec import has_gzip, has_snappy


def kafka_log4j():
    return os.path.abspath("./test/resources/log4j.properties")


def kafka_classpath():
    # ./kafka-src/bin/kafka-run-class.sh is the authority.
    ivy = os.path.expanduser("~/.ivy2/cache")
    base = os.path.abspath("./kafka-src/")

    jars = ["."]
    jars.append(ivy + "/org.xerial.snappy/snappy-java/bundles/snappy-java-1.0.4.1.jar")
    jars.append(ivy + "/org.scala-lang/scala-library/jars/scala-library-2.8.0.jar")
    jars.append(ivy + "/org.scala-lang/scala-compiler/jars/scala-compiler-2.8.0.jar")
    jars.append(ivy + "/log4j/log4j/jars/log4j-1.2.15.jar")
    jars.append(ivy + "/org.slf4j/slf4j-api/jars/slf4j-api-1.6.4.jar")
    jars.append(ivy + "/org.apache.zookeeper/zookeeper/jars/zookeeper-3.3.4.jar")
    jars.append(ivy + "/net.sf.jopt-simple/jopt-simple/jars/jopt-simple-3.2.jar")
    jars.extend(glob.glob(base + "/core/target/scala-2.8.0/*.jar"))
    jars.extend(glob.glob(base + "/core/lib/*.jar"))
    jars.extend(glob.glob(base + "/perf/target/scala-2.8.0/kafka*.jar"))

    jars = filter(os.path.exists, map(os.path.abspath, jars))
    return ":".join(jars)


def kafka_run_class_args(*args):
    # ./kafka-src/bin/kafka-run-class.sh is the authority.
    result = ["java", "-Xmx512M", "-server"]
    result.append("-Dlog4j.configuration=file:%s" % kafka_log4j())
    result.append("-Dcom.sun.management.jmxremote")
    result.append("-Dcom.sun.management.jmxremote.authenticate=false")
    result.append("-Dcom.sun.management.jmxremote.ssl=false")
    result.append("-cp")
    result.append(kafka_classpath())
    result.extend(args)
    return result


def get_open_port():
    sock = socket.socket()
    sock.bind(("", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def render_template(source_file, target_file, binding):
    with open(source_file, "r") as handle:
        template = handle.read()
    with open(target_file, "w") as handle:
        handle.write(template.format(**binding))


class ExternalServiceFixture(object):
    def __init__(self, host, port):
        print("Using already running service at %s:%d" % (host, port))

    def open(self):
        pass

    def close(self):
        pass


class SubprocessFixture(threading.Thread):
    def __init__(self, args=[]):
        threading.Thread.__init__(self)

        self.args = args
        self.captured_stdout = ""
        self.captured_stderr = ""
        self.stdout_file = None
        self.stderr_file = None
        self.capture_stdout = True
        self.capture_stderr = True
        self.show_stdout = True
        self.show_stderr = True

        self.should_die = threading.Event()

    def configure_stdout(self, file=None, capture=True, show=False):
        self.stdout_file = file
        self.capture_stdout = capture
        self.show_stdout = show

    def configure_stderr(self, file=None, capture=False, show=True):
        self.stderr_file = file
        self.capture_stderr = capture
        self.show_stderr = show

    def run(self):
        stdout_handle = None
        stderr_handle = None
        try:
            if self.stdout_file:
                stdout_handle = open(self.stdout_file, "w")
            if self.stderr_file:
                stderr_handle = open(self.stderr_file, "w")
            self.run_with_handles(stdout_handle, stderr_handle)
        finally:
            if stdout_handle:
                stdout_handle.close()
            if stderr_handle:
                stderr_handle.close()

    def run_with_handles(self, stdout_handle, stderr_handle):
        child = subprocess.Popen(
            self.args,
            bufsize=1,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        alive = True

        while True:
            (rds, wds, xds) = select.select([child.stdout, child.stderr], [], [], 1)

            if child.stdout in rds:
                line = child.stdout.readline()
                if stdout_handle:
                    stdout_handle.write(line)
                    stdout_handle.flush()
                if self.capture_stdout:
                    self.captured_stdout += line
                if self.show_stdout:
                    sys.stdout.write(line)
                    sys.stdout.flush()

            if child.stderr in rds:
                line = child.stderr.readline()
                if stderr_handle:
                    stderr_handle.write(line)
                    stderr_handle.flush()
                if self.capture_stderr:
                    self.captured_stderr += line
                if self.show_stderr:
                    sys.stderr.write(line)
                    sys.stderr.flush()

            if self.should_die.is_set():
                child.terminate()
                alive = False

            if child.poll() is not None:
                if not alive:
                    break
                else:
                    raise RuntimeError("Subprocess has died. Aborting.")

    def wait_for(self, pattern, timeout=10):
        t1 = time.time()
        while True:
            t2 = time.time()
            if t2 - t1 >= timeout:
                raise RuntimeError("Waiting for %r timed out" % pattern)
            if re.search(pattern, self.captured_stdout) is not None:
                return
            if re.search(pattern, self.captured_stderr) is not None:
                return
            time.sleep(0.1)

    def start(self):
        threading.Thread.start(self)

    def stop(self):
        self.should_die.set()
        self.join()


class ZookeeperFixture(object):
    @staticmethod
    def instance():
        if "ZOOKEEPER_URI" in os.environ:
            parse = urlparse(os.environ["ZOOKEEPER_URI"])
            (host, port) = (parse.hostname, parse.port)
            fixture = ExternalServiceFixture(host, port)
        else:
            (host, port) = ("127.0.0.1", get_open_port())
            fixture = ZookeeperFixture(host, port)
            fixture.open()
        return fixture

    def __init__(self, host, port):
        self.host = host
        self.port = port

        self.tmp_dir = None
        self.child = None

    def open(self):
        self.tmp_dir = tempfile.mkdtemp()
        print("*** Running local Zookeeper instance...")
        print("  host    = %s" % self.host)
        print("  port    = %s" % self.port)
        print("  tmp_dir = %s" % self.tmp_dir)

        # Generate configs
        properties = os.path.join(self.tmp_dir, "zookeeper.properties")
        render_template("./test/resources/zookeeper.properties", properties, vars(self))

        # Configure Zookeeper child process
        self.child = SubprocessFixture(kafka_run_class_args(
            "org.apache.zookeeper.server.quorum.QuorumPeerMain",
            properties
        ))
        self.child.configure_stdout(os.path.join(self.tmp_dir, "stdout.txt"))
        self.child.configure_stderr(os.path.join(self.tmp_dir, "stderr.txt"))

        # Party!
        print("*** Starting Zookeeper...")
        self.child.start()
        self.child.wait_for(r"Snapshotting")
        print("*** Done!")

    def close(self):
        print("*** Stopping Zookeeper...")
        self.child.stop()
        self.child = None
        print("*** Done!")
        shutil.rmtree(self.tmp_dir)


class KafkaFixture(object):
    @staticmethod
    def instance(broker_id, zk_host, zk_port, zk_chroot=None):
        if zk_chroot is None:
            zk_chroot = "kafka-python_" + str(uuid.uuid4()).replace("-", "_")
        if "KAFKA_URI" in os.environ:
            parse = urlparse(os.environ["KAFKA_URI"])
            (host, port) = (parse.hostname, parse.port)
            fixture = ExternalServiceFixture(host, port)
        else:
            (host, port) = ("localhost", get_open_port())
            fixture = KafkaFixture(host, port, broker_id, zk_host, zk_port, zk_chroot)
            fixture.open()
        return fixture

    def __init__(self, host, port, broker_id, zk_host, zk_port, zk_chroot):
        self.host = host
        self.port = port

        self.broker_id = broker_id

        self.zk_host = zk_host
        self.zk_port = zk_port
        self.zk_chroot = zk_chroot

        self.tmp_dir = None
        self.child = None

    def open(self):
        self.tmp_dir = tempfile.mkdtemp()
        print("*** Running local Kafka instance")
        print("  host      = %s" % self.host)
        print("  port      = %s" % self.port)
        print("  broker_id = %s" % self.broker_id)
        print("  zk_host   = %s" % self.zk_host)
        print("  zk_port   = %s" % self.zk_port)
        print("  zk_chroot = %s" % self.zk_chroot)
        print("  tmp_dir   = %s" % self.tmp_dir)

        # Create directories
        os.mkdir(os.path.join(self.tmp_dir, "logs"))
        os.mkdir(os.path.join(self.tmp_dir, "data"))

        # Generate configs
        properties = os.path.join(self.tmp_dir, "kafka.properties")
        render_template("./test/resources/kafka.properties", properties, vars(self))

        # Configure Kafka child process
        self.child = SubprocessFixture(kafka_run_class_args(
            "kafka.Kafka", properties
        ))
        self.child.configure_stdout(os.path.join(self.tmp_dir, "stdout.txt"))
        self.child.configure_stderr(os.path.join(self.tmp_dir, "stderr.txt"))

        # Party!
        print("*** Creating Zookeeper chroot node...")
        proc = subprocess.Popen(kafka_run_class_args(
            "org.apache.zookeeper.ZooKeeperMain",
            "-server", "%s:%d" % (self.zk_host, self.zk_port),
            "create", "/%s" % self.zk_chroot, "kafka-python"
        ))
        if proc.wait() != 0:
            print("*** Failed to create Zookeeper chroot node")
            raise RuntimeError("Failed to create Zookeeper chroot node")
        print("*** Done!")

        print("*** Starting Kafka...")
        self.child.start()
        self.child.wait_for(r"\[Kafka Server \d+\], started")
        print("*** Done!")

    def close(self):
        print("*** Stopping Kafka...")
        self.child.stop()
        self.child = None
        print("*** Done!")
        shutil.rmtree(self.tmp_dir)


class TestKafkaClient(unittest.TestCase):
    @classmethod
    def setUpClass(cls):  # noqa
        cls.zk = ZookeeperFixture.instance()
        cls.server = KafkaFixture.instance(0, cls.zk.host, cls.zk.port)
        cls.client = KafkaClient(cls.server.host, cls.server.port)

    @classmethod
    def tearDownClass(cls):  # noqa
        cls.client.close()
        cls.server.close()
        cls.zk.close()

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
        if not has_gzip():
            return
        message1 = create_gzip_message(["Gzipped 1 %d" % i for i in range(100)])
        message2 = create_gzip_message(["Gzipped 2 %d" % i for i in range(100)])

        produce = ProduceRequest("test_produce_many_gzip", 0, messages=[message1, message2])

        for resp in self.client.send_produce_request([produce]):
            self.assertEquals(resp.error, 0)
            self.assertEquals(resp.offset, 0)

        (offset, ) = self.client.send_offset_request([OffsetRequest("test_produce_many_gzip", 0, -1, 1)])
        self.assertEquals(offset.offsets[0], 200)

    def test_produce_many_snappy(self):
        if not has_snappy():
            return
        message1 = create_snappy_message(["Snappy 1 %d" % i for i in range(100)])
        message2 = create_snappy_message(["Snappy 2 %d" % i for i in range(100)])

        produce = ProduceRequest("test_produce_many_snappy", 0, messages=[message1, message2])

        for resp in self.client.send_produce_request([produce]):
            self.assertEquals(resp.error, 0)
            self.assertEquals(resp.offset, 0)

        (offset, ) = self.client.send_offset_request([OffsetRequest("test_produce_many_snappy", 0, -1, 1)])
        self.assertEquals(offset.offsets[0], 200)

    def test_produce_mixed(self):
        if not has_gzip() or not has_snappy():
            return
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
        self.assertEquals(resp.metadata, "")  # Metadata isn't stored for now

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


class TestKafkaConsumer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):  # noqa
        cls.zk = ZookeeperFixture.instance()
        cls.server1 = KafkaFixture.instance(0, cls.zk.host, cls.zk.port)
        cls.server2 = KafkaFixture.instance(1, cls.zk.host, cls.zk.port)
        cls.client = KafkaClient(cls.server2.host, cls.server2.port)

    @classmethod
    def tearDownClass(cls):  # noqa
        cls.client.close()
        cls.server1.close()
        cls.server2.close()
        cls.zk.close()

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
        # Make sure there are no duplicates
        self.assertEquals(len(all_messages), len(set(all_messages)))

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

        consumer.stop()

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
        consumer.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
