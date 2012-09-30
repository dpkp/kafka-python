import glob
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

from kafka import KafkaClient, ProduceRequest, FetchRequest

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
    jars += glob.glob(os.path.join(baseDir, "perf/target/scala_2.8.0/kafka*.jar"))
    jars += glob.glob(os.path.join(baseDir, "core/lib_managed/scala_2.8.0/compile/*.jar"))
    return ":".join(["."] + [os.path.abspath(jar) for jar in jars])

class KafkaFixture(Thread):
    def __init__(self, port):
        Thread.__init__(self)
        self.port = port
        self.capture = ""
        self.shouldDie = Event()
        self.tmpDir = tempfile.mkdtemp()
        print("tmp dir: %s" % self.tmpDir)

    def run(self):
        # Create the log directory
        logDir = os.path.join(self.tmpDir, 'logs')
        os.mkdir(logDir)

        # Create the config file
        logConfig = "test/resources/log4j.properties"
        configFile = os.path.join(self.tmpDir, 'server.properties')
        f = open('test/resources/server.properties', 'r')
        props = f.read()
        f = open(configFile, 'w')
        f.write(props % {'kafka.port': self.port, 'kafka.tmp.dir': logDir, 'kafka.partitions': 2})
        f.close()

        # Start Kafka
        args = shlex.split("java -Xmx256M -server -Dlog4j.configuration=%s -cp %s kafka.Kafka %s" % (logConfig, build_kafka_classpath(), configFile))
        proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env={"JMX_PORT":"%d" % get_open_port()})

        killed = False
        while True:
            (rlist, wlist, xlist) = select.select([proc.stdout], [], [], 1)
            if proc.stdout in rlist:
                read = proc.stdout.readline()
                sys.stdout.write(read)
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


class IntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        port = get_open_port()
        cls.server = KafkaFixture(port)
        cls.server.start()
        cls.server.wait_for("Kafka server started")
        cls.kafka = KafkaClient("localhost", port)

    @classmethod
    def tearDownClass(cls):
        cls.kafka.close()
        cls.server.shouldDie.set()

    def test_produce(self):
        # Produce a message, check that the log got created
        req = ProduceRequest("test-produce", 0, [KafkaClient.create_message("testing")])
        self.kafka.send_message_set(req)
        self.assertTrue(self.server.wait_for("Created log for 'test-produce'-0"))

        # Same thing, different partition
        req = ProduceRequest("test-produce", 1, [KafkaClient.create_message("testing")])
        self.kafka.send_message_set(req)
        self.assertTrue(self.server.wait_for("Created log for 'test-produce'-1"))

    def test_produce_consume(self):
        # Send two messages and consume them
        message1 = KafkaClient.create_message("testing 1")
        message2 = KafkaClient.create_message("testing 2")
        req = ProduceRequest("test-produce-consume", 0, [message1, message2])
        self.kafka.send_message_set(req)
        self.assertTrue(self.server.wait_for("Created log for 'test-produce-consume'-0"))
        self.assertTrue(self.server.wait_for("Flushing log 'test-produce-consume-0'"))
        req = FetchRequest("test-produce-consume", 0, 0, 1024)
        (messages, req) = self.kafka.get_message_set(req)
        self.assertEquals(len(messages), 2)
        self.assertEquals(messages[0], message1)
        self.assertEquals(messages[1], message2)

        # Do the same, but for a different partition
        message3 = KafkaClient.create_message("testing 3")
        message4 = KafkaClient.create_message("testing 4")
        req = ProduceRequest("test-produce-consume", 1, [message3, message4])
        self.kafka.send_message_set(req)
        self.assertTrue(self.server.wait_for("Created log for 'test-produce-consume'-1"))
        self.assertTrue(self.server.wait_for("Flushing log 'test-produce-consume-1'"))
        req = FetchRequest("test-produce-consume", 1, 0, 1024)
        (messages, req) = self.kafka.get_message_set(req)
        self.assertEquals(len(messages), 2)
        self.assertEquals(messages[0], message3)
        self.assertEquals(messages[1], message4)

    def test_check_offset(self):
        # Produce/consume a message, check that the next offset looks correct
        message1 = KafkaClient.create_message("testing 1")
        req = ProduceRequest("test-check-offset", 0, [message1])
        self.kafka.send_message_set(req)
        self.assertTrue(self.server.wait_for("Created log for 'test-check-offset'-0"))
        self.assertTrue(self.server.wait_for("Flushing log 'test-check-offset-0'"))
        req = FetchRequest("test-check-offset", 0, 0, 1024)
        (messages, nextReq) = self.kafka.get_message_set(req)
        self.assertEquals(len(messages), 1)
        self.assertEquals(messages[0], message1)
        self.assertEquals(nextReq.offset, len(KafkaClient.encode_message(message1)))

        # Produce another message, consume with the last offset
        message2 = KafkaClient.create_message("test 2")
        req = ProduceRequest("test-check-offset", 0, [message2])
        self.kafka.send_message_set(req)
        self.assertTrue(self.server.wait_for("Flushing log 'test-check-offset-0'"))

        # Verify
        (messages, nextReq) = self.kafka.get_message_set(nextReq)
        self.assertEquals(len(messages), 1)
        self.assertEquals(messages[0], message2)
        self.assertEquals(nextReq.offset, len(KafkaClient.encode_message(message1)) + len(KafkaClient.encode_message(message2)))


if __name__ == "__main__":
    unittest.main() 
