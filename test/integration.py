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

class KafkaFixture(Thread):
    def __init__(self, port):
        Thread.__init__(self)
        self.port = port
        self.capture = ""
        self.shouldDie = Event()
        self.tmpDir = tempfile.mkdtemp()

    def run(self):
        # Create the log directory
        logDir = os.path.join(self.tmpDir, 'logs')
        os.mkdir(logDir)

        # Create the config file
        configFile = os.path.join(self.tmpDir, 'server.properties')
        f = open('test/resources/server.properties', 'r')
        props = f.read()
        f = open(configFile, 'w')
        f.write(props % {'kafka.port': self.port, 'kafka.tmp.dir': logDir, 'kafka.partitions': 2})
        f.close()

        # Start Kafka
        args = shlex.split("./kafka-src/bin/kafka-server-start.sh %s" % configFile)
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
                shutil.rmtree(self.tmpDir)
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
            time.sleep(1)


class IntegrationTest(unittest.TestCase):
    def setUp(self):
        port = get_open_port()
        self.server = KafkaFixture(port)
        self.server.start()
        self.server.wait_for("Kafka server started")
        self.kafka = KafkaClient("localhost", port)

    def test_produce(self):
        req = ProduceRequest("my-topic", 0, [KafkaClient.create_message("testing")])
        self.kafka.send_message_set(req)
        self.assertTrue(self.server.wait_for("Created log for 'my-topic'-0"))

        req = ProduceRequest("my-topic", 1, [KafkaClient.create_message("testing")])
        self.kafka.send_message_set(req)
        self.assertTrue(self.server.wait_for("Created log for 'my-topic'-1"))

    def tearDown(self):
        self.kafka.close()
        self.server.shouldDie.set()


if __name__ == "__main__":
    unittest.main() 
