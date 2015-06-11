import logging
import os
import os.path
import shutil
import subprocess
import tempfile
import time
from six.moves import urllib
import uuid

from six.moves.urllib.parse import urlparse  # pylint: disable-msg=E0611
from test.service import ExternalService, SpawnedService
from test.testutil import get_open_port


log = logging.getLogger(__name__)


class Fixture(object):
    kafka_version = os.environ.get('KAFKA_VERSION', '0.8.0')
    scala_version = os.environ.get("SCALA_VERSION", '2.8.0')
    project_root = os.environ.get('PROJECT_ROOT', os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    kafka_root = os.environ.get("KAFKA_ROOT", os.path.join(project_root, 'servers', kafka_version, "kafka-bin"))
    ivy_root = os.environ.get('IVY_ROOT', os.path.expanduser("~/.ivy2/cache"))

    @classmethod
    def download_official_distribution(cls,
                                       kafka_version=None,
                                       scala_version=None,
                                       output_dir=None):
        if not kafka_version:
            kafka_version = cls.kafka_version
        if not scala_version:
            scala_version = cls.scala_version
        if not output_dir:
            output_dir = os.path.join(cls.project_root, 'servers', 'dist')

        distfile = 'kafka_%s-%s' % (scala_version, kafka_version,)
        url_base = 'https://archive.apache.org/dist/kafka/%s/' % (kafka_version,)
        output_file = os.path.join(output_dir, distfile + '.tgz')

        if os.path.isfile(output_file):
            log.info("Found file already on disk: %s", output_file)
            return output_file

        # New tarballs are .tgz, older ones are sometimes .tar.gz
        try:
            url = url_base + distfile + '.tgz'
            log.info("Attempting to download %s", url)
            response = urllib.request.urlopen(url)
        except urllib.error.HTTPError:
            log.exception("HTTP Error")
            url = url_base + distfile + '.tar.gz'
            log.info("Attempting to download %s", url)
            response = urllib.request.urlopen(url)

        log.info("Saving distribution file to %s", output_file)
        with open(output_file, 'w') as output_file_fd:
            output_file_fd.write(response.read())

        return output_file

    @classmethod
    def test_resource(cls, filename):
        return os.path.join(cls.project_root, "servers", cls.kafka_version, "resources", filename)

    @classmethod
    def kafka_run_class_args(cls, *args):
        result = [os.path.join(cls.kafka_root, 'bin', 'kafka-run-class.sh')]
        result.extend(args)
        return result

    @classmethod
    def kafka_run_class_env(cls):
        env = os.environ.copy()
        env['KAFKA_LOG4J_OPTS'] = "-Dlog4j.configuration=file:%s" % cls.test_resource("log4j.properties")
        return env

    @classmethod
    def render_template(cls, source_file, target_file, binding):
        with open(source_file, "r") as handle:
            template = handle.read()
        with open(target_file, "w") as handle:
            handle.write(template.format(**binding))


class ZookeeperFixture(Fixture):
    @classmethod
    def instance(cls):
        if "ZOOKEEPER_URI" in os.environ:
            parse = urlparse(os.environ["ZOOKEEPER_URI"])
            (host, port) = (parse.hostname, parse.port)
            fixture = ExternalService(host, port)
        else:
            (host, port) = ("127.0.0.1", get_open_port())
            fixture = cls(host, port)

        fixture.open()
        return fixture

    def __init__(self, host, port):
        self.host = host
        self.port = port

        self.tmp_dir = None
        self.child = None

    def out(self, message):
        log.info("*** Zookeeper [%s:%d]: %s", self.host, self.port, message)

    def open(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.out("Running local instance...")
        log.info("  host    = %s", self.host)
        log.info("  port    = %s", self.port)
        log.info("  tmp_dir = %s", self.tmp_dir)

        # Generate configs
        template = self.test_resource("zookeeper.properties")
        properties = os.path.join(self.tmp_dir, "zookeeper.properties")
        self.render_template(template, properties, vars(self))

        # Configure Zookeeper child process
        args = self.kafka_run_class_args("org.apache.zookeeper.server.quorum.QuorumPeerMain", properties)
        env = self.kafka_run_class_env()

        # Party!
        self.out("Starting...")
        timeout = 5
        max_timeout = 30
        backoff = 1
        while True:
            self.child = SpawnedService(args, env)
            self.child.start()
            timeout = min(timeout, max_timeout)
            if self.child.wait_for(r"binding to port", timeout=timeout):
                break
            self.child.stop()
            timeout *= 2
            time.sleep(backoff)
        self.out("Done!")

    def close(self):
        self.out("Stopping...")
        self.child.stop()
        self.child = None
        self.out("Done!")
        shutil.rmtree(self.tmp_dir)


class KafkaFixture(Fixture):
    @classmethod
    def instance(cls, broker_id, zk_host, zk_port, zk_chroot=None, replicas=1, partitions=2):
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

        self.replicas = replicas
        self.partitions = partitions

        self.tmp_dir = None
        self.child = None
        self.running = False

    def out(self, message):
        log.info("*** Kafka [%s:%d]: %s", self.host, self.port, message)

    def open(self):
        if self.running:
            self.out("Instance already running")
            return

        self.tmp_dir = tempfile.mkdtemp()
        self.out("Running local instance...")
        log.info("  host       = %s", self.host)
        log.info("  port       = %s", self.port)
        log.info("  broker_id  = %s", self.broker_id)
        log.info("  zk_host    = %s", self.zk_host)
        log.info("  zk_port    = %s", self.zk_port)
        log.info("  zk_chroot  = %s", self.zk_chroot)
        log.info("  replicas   = %s", self.replicas)
        log.info("  partitions = %s", self.partitions)
        log.info("  tmp_dir    = %s", self.tmp_dir)

        # Create directories
        os.mkdir(os.path.join(self.tmp_dir, "logs"))
        os.mkdir(os.path.join(self.tmp_dir, "data"))

        # Generate configs
        template = self.test_resource("kafka.properties")
        properties = os.path.join(self.tmp_dir, "kafka.properties")
        self.render_template(template, properties, vars(self))

        # Party!
        self.out("Creating Zookeeper chroot node...")
        args = self.kafka_run_class_args("org.apache.zookeeper.ZooKeeperMain",
                                         "-server", "%s:%d" % (self.zk_host, self.zk_port),
                                         "create",
                                         "/%s" % self.zk_chroot,
                                         "kafka-python")
        env = self.kafka_run_class_env()
        proc = subprocess.Popen(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        if proc.wait() != 0:
            self.out("Failed to create Zookeeper chroot node")
            self.out(proc.stdout.read())
            self.out(proc.stderr.read())
            raise RuntimeError("Failed to create Zookeeper chroot node")
        self.out("Done!")

        self.out("Starting...")

        # Configure Kafka child process
        args = self.kafka_run_class_args("kafka.Kafka", properties)
        env = self.kafka_run_class_env()

        timeout = 5
        max_timeout = 30
        backoff = 1
        while True:
            self.child = SpawnedService(args, env)
            self.child.start()
            timeout = min(timeout, max_timeout)
            if self.child.wait_for(r"\[Kafka Server %d\], Started" %
                                   self.broker_id, timeout=timeout):
                break
            self.child.stop()
            timeout *= 2
            time.sleep(backoff)
        self.out("Done!")
        self.running = True

    def close(self):
        if not self.running:
            self.out("Instance already stopped")
            return

        self.out("Stopping...")
        self.child.stop()
        self.child = None
        self.out("Done!")
        shutil.rmtree(self.tmp_dir)
        self.running = False
