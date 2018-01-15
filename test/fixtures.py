from __future__ import absolute_import

import atexit
import logging
import os
import os.path
import shutil
import subprocess
import tempfile
import time
import uuid

from six.moves import urllib
from six.moves.urllib.parse import urlparse  # pylint: disable=E0611,F0401

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

    def kafka_run_class_env(self):
        env = os.environ.copy()
        env['KAFKA_LOG4J_OPTS'] = "-Dlog4j.configuration=file:%s" % self.test_resource("log4j.properties")
        return env

    @classmethod
    def render_template(cls, source_file, target_file, binding):
        log.info('Rendering %s from template %s', target_file, source_file)
        with open(source_file, "r") as handle:
            template = handle.read()
            assert len(template) > 0, 'Empty template %s' % source_file
        with open(target_file, "w") as handle:
            handle.write(template.format(**binding))
            handle.flush()
            os.fsync(handle)

        # fsync directory for durability
        # https://blog.gocept.com/2013/07/15/reliable-file-updates-with-python/
        dirfd = os.open(os.path.dirname(target_file), os.O_DIRECTORY)
        os.fsync(dirfd)
        os.close(dirfd)


class ZookeeperFixture(Fixture):
    @classmethod
    def instance(cls):
        if "ZOOKEEPER_URI" in os.environ:
            parse = urlparse(os.environ["ZOOKEEPER_URI"])
            (host, port) = (parse.hostname, parse.port)
            fixture = ExternalService(host, port)
        else:
            (host, port) = ("127.0.0.1", None)
            fixture = cls(host, port)

        fixture.open()
        return fixture

    def __init__(self, host, port):
        self.host = host
        self.port = port

        self.tmp_dir = None
        self.child = None

    def kafka_run_class_env(self):
        env = super(ZookeeperFixture, self).kafka_run_class_env()
        env['LOG_DIR'] = os.path.join(self.tmp_dir, 'logs')
        return env

    def out(self, message):
        log.info("*** Zookeeper [%s:%s]: %s", self.host, self.port or '(auto)', message)

    def open(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.out("Running local instance...")
        log.info("  host    = %s", self.host)
        log.info("  port    = %s", self.port or '(auto)')
        log.info("  tmp_dir = %s", self.tmp_dir)

        # Configure Zookeeper child process
        template = self.test_resource("zookeeper.properties")
        properties = os.path.join(self.tmp_dir, "zookeeper.properties")
        args = self.kafka_run_class_args("org.apache.zookeeper.server.quorum.QuorumPeerMain", properties)
        env = self.kafka_run_class_env()

        # Party!
        timeout = 5
        max_timeout = 30
        backoff = 1
        end_at = time.time() + max_timeout
        tries = 1
        auto_port = (self.port is None)
        while time.time() < end_at:
            if auto_port:
                self.port = get_open_port()
            self.out('Attempting to start on port %d (try #%d)' % (self.port, tries))
            self.render_template(template, properties, vars(self))
            self.child = SpawnedService(args, env)
            self.child.start()
            timeout = min(timeout, max(end_at - time.time(), 0))
            if self.child.wait_for(r"binding to port", timeout=timeout):
                break
            self.child.dump_logs()
            self.child.stop()
            timeout *= 2
            time.sleep(backoff)
            tries += 1
        else:
            raise Exception('Failed to start Zookeeper before max_timeout')
        self.out("Done!")
        atexit.register(self.close)

    def close(self):
        if self.child is None:
            return
        self.out("Stopping...")
        self.child.stop()
        self.child = None
        self.out("Done!")
        shutil.rmtree(self.tmp_dir)

    def __del__(self):
        self.close()


class KafkaFixture(Fixture):
    @classmethod
    def instance(cls, broker_id, zk_host, zk_port, zk_chroot=None,
                 host=None, port=None,
                 transport='PLAINTEXT', replicas=1, partitions=2):
        if zk_chroot is None:
            zk_chroot = "kafka-python_" + str(uuid.uuid4()).replace("-", "_")
        if "KAFKA_URI" in os.environ:
            parse = urlparse(os.environ["KAFKA_URI"])
            (host, port) = (parse.hostname, parse.port)
            fixture = ExternalService(host, port)
        else:
            if host is None:
                host = "localhost"
            fixture = KafkaFixture(host, port, broker_id,
                                   zk_host, zk_port, zk_chroot,
                                   transport=transport,
                                   replicas=replicas, partitions=partitions)
            fixture.open()
        return fixture

    def __init__(self, host, port, broker_id, zk_host, zk_port, zk_chroot,
                 replicas=1, partitions=2, transport='PLAINTEXT'):
        self.host = host
        self.port = port

        self.broker_id = broker_id
        self.transport = transport.upper()
        self.ssl_dir = self.test_resource('ssl')

        self.zk_host = zk_host
        self.zk_port = zk_port
        self.zk_chroot = zk_chroot

        self.replicas = replicas
        self.partitions = partitions

        self.tmp_dir = None
        self.child = None
        self.running = False

    def kafka_run_class_env(self):
        env = super(KafkaFixture, self).kafka_run_class_env()
        env['LOG_DIR'] = os.path.join(self.tmp_dir, 'logs')
        return env

    def out(self, message):
        log.info("*** Kafka [%s:%s]: %s", self.host, self.port or '(auto)', message)

    def open(self):
        if self.running:
            self.out("Instance already running")
            return

        self.tmp_dir = tempfile.mkdtemp()
        self.out("Running local instance...")
        log.info("  host       = %s", self.host)
        log.info("  port       = %s", self.port or '(auto)')
        log.info("  transport  = %s", self.transport)
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

        self.out("Creating Zookeeper chroot node...")
        args = self.kafka_run_class_args("org.apache.zookeeper.ZooKeeperMain",
                                         "-server", "%s:%d" % (self.zk_host, self.zk_port),
                                         "create",
                                         "/%s" % self.zk_chroot,
                                         "kafka-python")
        env = self.kafka_run_class_env()
        proc = subprocess.Popen(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # Fix bugs where integration tests hang in docker container by sending
        # blank line to stdin
        proc.communicate('')

        if proc.wait() != 0:
            self.out("Failed to create Zookeeper chroot node")
            self.out(proc.stdout.read())
            self.out(proc.stderr.read())
            raise RuntimeError("Failed to create Zookeeper chroot node")
        self.out("Done!")

        # Configure Kafka child process
        properties = os.path.join(self.tmp_dir, "kafka.properties")
        template = self.test_resource("kafka.properties")
        args = self.kafka_run_class_args("kafka.Kafka", properties)
        env = self.kafka_run_class_env()

        timeout = 5
        max_timeout = 30
        backoff = 1
        end_at = time.time() + max_timeout
        tries = 1
        auto_port = (self.port is None)
        while time.time() < end_at:
            # We have had problems with port conflicts on travis
            # so we will try a different port on each retry
            # unless the fixture was passed a specific port
            if auto_port:
                self.port = get_open_port()
            self.out('Attempting to start on port %d (try #%d)' % (self.port, tries))
            self.render_template(template, properties, vars(self))
            self.child = SpawnedService(args, env)
            self.child.start()
            timeout = min(timeout, max(end_at - time.time(), 0))
            if self.child.wait_for(r"\[Kafka Server %d\], Started" %
                                   self.broker_id, timeout=timeout):
                break
            self.child.dump_logs()
            self.child.stop()
            timeout *= 2
            time.sleep(backoff)
            tries += 1
        else:
            raise Exception('Failed to start KafkaInstance before max_timeout')
        self.out("Done!")
        self.running = True
        atexit.register(self.close)

    def __del__(self):
        self.close()

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
