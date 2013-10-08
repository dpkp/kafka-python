import glob
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
import uuid

from urlparse import urlparse


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
KAFKA_ROOT = os.path.join(PROJECT_ROOT, "kafka-src")
IVY_ROOT = os.path.expanduser("~/.ivy2/cache")
SCALA_VERSION = '2.8.0'

if "PROJECT_ROOT" in os.environ:
    PROJECT_ROOT = os.environ["PROJECT_ROOT"]
if "KAFKA_ROOT" in os.environ:
    KAFKA_ROOT = os.environ["KAFKA_ROOT"]
if "IVY_ROOT" in os.environ:
    IVY_ROOT = os.environ["IVY_ROOT"]
if "SCALA_VERSION" in os.environ:
    SCALA_VERSION = os.environ["SCALA_VERSION"]


def test_resource(file):
    return os.path.join(PROJECT_ROOT, "test", "resources", file)


def test_classpath():
    # ./kafka-src/bin/kafka-run-class.sh is the authority.
    jars = ["."]
    # assume all dependencies have been packaged into one jar with sbt-assembly's task "assembly-package-dependency"
    jars.extend(glob.glob(KAFKA_ROOT + "/core/target/scala-%s/*.jar" % SCALA_VERSION))

    jars = filter(os.path.exists, map(os.path.abspath, jars))
    return ":".join(jars)


def kafka_run_class_args(*args):
    # ./kafka-src/bin/kafka-run-class.sh is the authority.
    result = ["java", "-Xmx512M", "-server"]
    result.append("-Dlog4j.configuration=file:%s" % test_resource("log4j.properties"))
    result.append("-Dcom.sun.management.jmxremote")
    result.append("-Dcom.sun.management.jmxremote.authenticate=false")
    result.append("-Dcom.sun.management.jmxremote.ssl=false")
    result.append("-cp")
    result.append(test_classpath())
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


class ExternalService(object):
    def __init__(self, host, port):
        print("Using already running service at %s:%d" % (host, port))

    def open(self):
        pass

    def close(self):
        pass


class SpawnedService(threading.Thread):
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
            fixture = ExternalService(host, port)
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
        template = test_resource("zookeeper.properties")
        properties = os.path.join(self.tmp_dir, "zookeeper.properties")
        render_template(template, properties, vars(self))

        # Configure Zookeeper child process
        self.child = SpawnedService(kafka_run_class_args(
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
    def instance(broker_id, zk_host, zk_port, zk_chroot=None, replicas=1, partitions=2):
        if zk_chroot is None:
            zk_chroot = "kafka-python_" + str(uuid.uuid4()).replace("-", "_")
        if "KAFKA_URI" in os.environ:
            parse = urlparse(os.environ["KAFKA_URI"])
            (host, port) = (parse.hostname, parse.port)
            fixture = ExternalService(host, port)
        else:
            (host, port) = ("127.0.0.1", get_open_port())
            fixture = KafkaFixture(host, port, broker_id, zk_host, zk_port, zk_chroot, replicas, partitions)
            fixture.open()
        return fixture

    def __init__(self, host, port, broker_id, zk_host, zk_port, zk_chroot, replicas=1, partitions=2):
        self.host = host
        self.port = port

        self.broker_id = broker_id

        self.zk_host = zk_host
        self.zk_port = zk_port
        self.zk_chroot = zk_chroot

        self.replicas   = replicas
        self.partitions = partitions

        self.tmp_dir = None
        self.child = None

    def open(self):
        self.tmp_dir = tempfile.mkdtemp()
        print("*** Running local Kafka instance")
        print("  host       = %s" % self.host)
        print("  port       = %s" % self.port)
        print("  broker_id  = %s" % self.broker_id)
        print("  zk_host    = %s" % self.zk_host)
        print("  zk_port    = %s" % self.zk_port)
        print("  zk_chroot  = %s" % self.zk_chroot)
        print("  replicas   = %s" % self.replicas)
        print("  partitions = %s" % self.partitions)
        print("  tmp_dir    = %s" % self.tmp_dir)

        # Create directories
        os.mkdir(os.path.join(self.tmp_dir, "logs"))
        os.mkdir(os.path.join(self.tmp_dir, "data"))

        # Generate configs
        template = test_resource("kafka.properties")
        properties = os.path.join(self.tmp_dir, "kafka.properties")
        render_template(template, properties, vars(self))

        # Configure Kafka child process
        self.child = SpawnedService(kafka_run_class_args(
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
        self.child.wait_for(r"\[Kafka Server %d\], Started" % self.broker_id)
        print("*** Done!")

    def close(self):
        print("*** Stopping Kafka...")
        self.child.stop()
        self.child = None
        print("*** Done!")
        shutil.rmtree(self.tmp_dir)
