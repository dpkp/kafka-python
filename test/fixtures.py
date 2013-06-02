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
        properties = os.path.join(self.tmp_dir, "zookeeper.properties")
        render_template("./test/resources/zookeeper.properties", properties, vars(self))

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
    def instance(broker_id, zk_host, zk_port, zk_chroot=None):
        if zk_chroot is None:
            zk_chroot = "kafka-python_" + str(uuid.uuid4()).replace("-", "_")
        if "KAFKA_URI" in os.environ:
            parse = urlparse(os.environ["KAFKA_URI"])
            (host, port) = (parse.hostname, parse.port)
            fixture = ExternalService(host, port)
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
        self.child.wait_for(r"\[Kafka Server \d+\], started")
        print("*** Done!")

    def close(self):
        print("*** Stopping Kafka...")
        self.child.stop()
        self.child = None
        print("*** Done!")
        shutil.rmtree(self.tmp_dir)
