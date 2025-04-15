from __future__ import absolute_import, division

import atexit
import base64
import logging
import os
import os.path
import socket
import subprocess
import time
import uuid

import py
from kafka.vendor.six.moves import range
from kafka.vendor.six.moves.urllib.parse import urlparse  # pylint: disable=E0611,F0401

from kafka import errors, KafkaAdminClient, KafkaClient, KafkaConsumer, KafkaProducer
from kafka.errors import InvalidReplicationFactorError, KafkaTimeoutError
from kafka.protocol.admin import CreateTopicsRequest
from kafka.protocol.metadata import MetadataRequest
from test.testutil import env_kafka_version, random_string
from test.service import ExternalService, SpawnedService

log = logging.getLogger(__name__)


def get_open_port():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def gen_ssl_resources(directory):
    os.system("""
    cd {0}
    echo Generating SSL resources in {0}

    # Step 1
    keytool -keystore kafka.server.keystore.jks -alias localhost -validity 1 \
      -genkey -storepass foobar -keypass foobar \
      -dname "CN=localhost, OU=kafka-python, O=kafka-python, L=SF, ST=CA, C=US" \
      -ext SAN=dns:localhost

    # Step 2
    openssl genrsa -out ca-key 2048
    openssl req -new -x509 -key ca-key -out ca-cert -days 1 \
      -subj "/C=US/ST=CA/O=MyOrg, Inc./CN=mydomain.com"
    keytool -keystore kafka.server.truststore.jks -alias CARoot -import \
      -file ca-cert -storepass foobar -noprompt

    # Step 3
    keytool -keystore kafka.server.keystore.jks -alias localhost -certreq \
      -file cert-file -storepass foobar
    openssl x509 -req -CA ca-cert -CAkey ca-key -in cert-file -out cert-signed \
      -days 1 -CAcreateserial -passin pass:foobar
    keytool -keystore kafka.server.keystore.jks -alias CARoot -import \
      -file ca-cert -storepass foobar -noprompt
    keytool -keystore kafka.server.keystore.jks -alias localhost -import \
      -file cert-signed -storepass foobar -noprompt
    """.format(directory))


class Fixture(object):
    kafka_version = os.environ.get('KAFKA_VERSION', '0.11.0.2')
    scala_version = os.environ.get("SCALA_VERSION", '2.8.0')
    project_root = os.environ.get('PROJECT_ROOT',
                                  os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
    kafka_root = os.environ.get("KAFKA_ROOT",
                                os.path.join(project_root, 'servers', kafka_version, "kafka-bin"))

    def __init__(self):
        self.child = None
        if not os.path.isdir(self.kafka_root):
            raise FileNotFoundError(self.kafka_root)

    @classmethod
    def test_resource(cls, filename):
        path = os.path.join(cls.project_root, "servers", cls.kafka_version, "resources", filename)
        if os.path.isfile(path):
            return path
        return os.path.join(cls.project_root, "servers", "resources", "default", filename)

    @classmethod
    def run_script(cls, script, *args):
        result = [os.path.join(cls.kafka_root, 'bin', script)]
        result.extend([str(arg) for arg in args])
        return result

    @classmethod
    def kafka_run_class_args(cls, *args):
        result = [os.path.join(cls.kafka_root, 'bin', 'kafka-run-class.sh')]
        result.extend([str(arg) for arg in args])
        return result

    def kafka_run_class_env(self):
        env = os.environ.copy()
        env['KAFKA_LOG4J_OPTS'] = "-Dlog4j.configuration=file:%s" % \
                                  (self.test_resource("log4j.properties"),)
        return env

    @classmethod
    def render_template(cls, source_file, target_file, binding):
        log.info('Rendering %s from template %s', target_file.strpath, source_file)
        with open(source_file, "r") as handle:
            template = handle.read()
            assert len(template) > 0, 'Empty template %s' % (source_file,)
        with open(target_file.strpath, "w") as handle:
            handle.write(template.format(**binding))
            handle.flush()
            os.fsync(handle)

        # fsync directory for durability
        # https://blog.gocept.com/2013/07/15/reliable-file-updates-with-python/
        dirfd = os.open(os.path.dirname(target_file.strpath), os.O_DIRECTORY)
        os.fsync(dirfd)
        os.close(dirfd)
        log.debug("Template string:")
        for line in template.splitlines():
            log.debug('  ' + line.strip())
        log.debug("Rendered template:")
        with open(target_file.strpath, 'r') as o:
            for line in o:
                log.debug('  ' + line.strip())
        log.debug("binding:")
        for key, value in binding.items():
            log.debug("  {key}={value}".format(key=key, value=value))

    def dump_logs(self):
        self.child.dump_logs()


class ZookeeperFixture(Fixture):
    @classmethod
    def instance(cls, host=None, port=None, external=False):
        if host is None:
            host = "127.0.0.1"
        fixture = cls(host, port, external=external)
        fixture.open()
        return fixture

    def __init__(self, host, port, external=False, tmp_dir=None):
        super(ZookeeperFixture, self).__init__()
        self.host = host
        self.port = port
        self.running = external
        self.tmp_dir = tmp_dir

    def kafka_run_class_env(self):
        env = super(ZookeeperFixture, self).kafka_run_class_env()
        env['LOG_DIR'] = self.tmp_dir.join('logs').strpath
        return env

    def out(self, message):
        if len(log.handlers) > 0:
            log.info("*** Zookeeper [%s:%s]: %s", self.host, self.port or '(auto)', message)

    def open(self):
        if self.running:
            return
        if self.tmp_dir is None:
            self.tmp_dir = py.path.local.mkdtemp() #pylint: disable=no-member
        self.tmp_dir.ensure(dir=True)

        self.out("Running local instance...")
        log.info("  host    = %s", self.host)
        log.info("  port    = %s", self.port or '(auto)')
        log.info("  tmp_dir = %s", self.tmp_dir.strpath)

        # Configure Zookeeper child process
        template = self.test_resource("zookeeper.properties")
        properties = self.tmp_dir.join("zookeeper.properties")
        # Consider replacing w/ run_script('zookeper-server-start.sh', ...)
        args = self.kafka_run_class_args("org.apache.zookeeper.server.quorum.QuorumPeerMain",
                                         properties.strpath)
        env = self.kafka_run_class_env()

        # Party!
        timeout = 5
        max_timeout = 120
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
            backoff += 1
        else:
            raise RuntimeError('Failed to start Zookeeper before max_timeout')
        self.out("Done!")
        atexit.register(self.close)

    def close(self):
        if self.child is None:
            return
        self.out("Stopping...")
        self.child.stop()
        self.child = None
        self.out("Done!")
        self.tmp_dir.remove()

    def __del__(self):
        self.close()


class KafkaFixture(Fixture):
    broker_user = 'alice'
    broker_password = 'alice-secret'

    @classmethod
    def instance(cls, broker_id, zookeeper=None, zk_chroot=None,
                 host="localhost", port=None, external=False,
                 transport='PLAINTEXT', replicas=1, partitions=4,
                 sasl_mechanism=None, auto_create_topic=True, tmp_dir=None):

        # Kafka requries zookeeper prior to 4.0 release
        if env_kafka_version() < (4, 0):
            if zookeeper is None:
                if "ZOOKEEPER_URI" in os.environ:
                    parse = urlparse(os.environ["ZOOKEEPER_URI"])
                    (host, port) = (parse.hostname, parse.port)
                    zookeeper = ZookeeperFixture.instance(host=host, port=port, external=True)
                elif not external:
                    zookeeper = ZookeeperFixture.instance()
            if zk_chroot is None:
                zk_chroot = "kafka-python_" + str(uuid.uuid4()).replace("-", "_")

        fixture = KafkaFixture(host, port, broker_id,
                               zookeeper=zookeeper, zk_chroot=zk_chroot,
                               external=external,
                               transport=transport,
                               replicas=replicas, partitions=partitions,
                               sasl_mechanism=sasl_mechanism,
                               auto_create_topic=auto_create_topic,
                               tmp_dir=tmp_dir)

        fixture.open()
        return fixture

    def __init__(self, host, port, broker_id, zookeeper=None, zk_chroot=None,
                 replicas=1, partitions=2, transport='PLAINTEXT',
                 sasl_mechanism=None, auto_create_topic=True,
                 tmp_dir=None, external=False):
        super(KafkaFixture, self).__init__()

        self.host = host
        self.controller_bootstrap_host = host
        if port is None:
            self.auto_port = True
            self.port = get_open_port()
        else:
            self.auto_port = False
            self.port = port
        self.controller_port = get_open_port()

        self.cluster_id = self._gen_cluster_id()
        self.broker_id = broker_id
        self.auto_create_topic = auto_create_topic
        self.transport = transport.upper()
        if sasl_mechanism is not None:
            self.sasl_mechanism = sasl_mechanism.upper()
        else:
            self.sasl_mechanism = None
        self.ssl_dir = self.test_resource('ssl')

        # TODO: checking for port connection would be better than scanning logs
        # until then, we need the pattern to work across all supported broker versions
        # The logging format changed slightly in 1.0.0
        if env_kafka_version() < (4, 0):
            self.start_pattern = r"\[Kafka ?Server (id=)?%d\],? started" % (broker_id,)
            # Need to wait until the broker has fetched user configs from zookeeper in case we use scram as sasl mechanism
            self.scram_pattern = r"Removing Produce quota for user %s" % (self.broker_user)
        else:
            self.start_pattern = r"\[KafkaRaftServer nodeId=%d\] Kafka Server started" % (broker_id,)
            self.scram_pattern = r"Replayed UserScramCredentialRecord creating new entry for %s" % (self.broker_user,)

        self.zookeeper = zookeeper
        self.zk_chroot = zk_chroot
        # Add the attributes below for the template binding
        self.zk_host = self.zookeeper.host if self.zookeeper else None
        self.zk_port = self.zookeeper.port if self.zookeeper else None

        self.replicas = replicas
        self.partitions = partitions

        self.tmp_dir = tmp_dir
        self.external = external

        if self.external:
            self.child = ExternalService(self.host, self.port)
            (self._client,) = self.get_clients(1, client_id='_internal_client')
            self.running = True
        else:
            self._client = None
            self.running = False

        self.sasl_config = ''
        self.jaas_config = ''

    def _gen_cluster_id(self):
        return base64.b64encode(uuid.uuid4().bytes).decode('utf-8').rstrip('=')

    def _sasl_config(self):
        if not self.sasl_enabled:
            return ''

        sasl_config = (
            'sasl.enabled.mechanisms={mechanism}\n'
            'sasl.mechanism.inter.broker.protocol={mechanism}\n'
        )
        return sasl_config.format(mechanism=self.sasl_mechanism)

    def _jaas_config(self):
        if not self.sasl_enabled:
            return ''

        elif self.sasl_mechanism == 'PLAIN':
            jaas_config = (
                'org.apache.kafka.common.security.plain.PlainLoginModule required'
                ' username="{user}" password="{password}" user_{user}="{password}";\n'
            )
        elif self.sasl_mechanism in ("SCRAM-SHA-256", "SCRAM-SHA-512"):
            jaas_config = (
                'org.apache.kafka.common.security.scram.ScramLoginModule required'
                ' username="{user}" password="{password}";\n'
            )
        else:
            raise ValueError("SASL mechanism {} currently not supported".format(self.sasl_mechanism))
        return jaas_config.format(user=self.broker_user, password=self.broker_password)

    def _add_scram_user(self):
        self.out("Adding SCRAM credentials for user {} to zookeeper.".format(self.broker_user))
        args = self.run_script('kafka-configs.sh',
                               '--zookeeper',
                               '%s:%d/%s' % (self.zookeeper.host,
                                          self.zookeeper.port,
                                          self.zk_chroot),
                               '--alter',
                               '--entity-type', 'users',
                               '--entity-name', self.broker_user,
                               '--add-config',
                               '{}=[password={}]'.format(self.sasl_mechanism, self.broker_password))
        env = self.kafka_run_class_env()
        proc = subprocess.Popen(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        stdout, stderr = proc.communicate()

        if proc.returncode != 0:
            self.out("Failed to save credentials to zookeeper!")
            self.out(stdout)
            self.out(stderr)
            raise RuntimeError("Failed to save credentials to zookeeper!")
        self.out("User created.")

    @property
    def sasl_enabled(self):
        return self.sasl_mechanism is not None

    def bootstrap_server(self):
        return '%s:%d' % (self.host, self.port)

    def kafka_run_class_env(self):
        env = super(KafkaFixture, self).kafka_run_class_env()
        env['LOG_DIR'] = self.tmp_dir.join('logs').strpath
        return env

    def out(self, message):
        if len(log.handlers) > 0:
            log.info("*** Kafka [%s:%s]: %s", self.host, self.port or '(auto)', message)

    def _create_zk_chroot(self):
        self.out("Creating Zookeeper chroot node...")
        args = self.run_script('zookeeper-shell.sh',
                               '%s:%d' % (self.zookeeper.host,
                                          self.zookeeper.port),
                               'create',
                               '/%s' % (self.zk_chroot,),
                               'kafka-python')
        env = self.kafka_run_class_env()
        proc = subprocess.Popen(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        stdout, stderr = proc.communicate()

        if proc.returncode != 0:
            self.out("Failed to create Zookeeper chroot node")
            self.out(stdout)
            self.out(stderr)
            raise RuntimeError("Failed to create Zookeeper chroot node")
        self.out("Kafka chroot created in Zookeeper!")

    def start(self):
        if self.running:
            return True
        # Configure Kafka child process
        properties = self.tmp_dir.join("kafka.properties")
        jaas_conf = self.tmp_dir.join("kafka_server_jaas.conf")
        properties_template = self.test_resource("kafka.properties")
        jaas_conf_template = self.test_resource("kafka_server_jaas.conf")

        # Consider replacing w/ run_script('kafka-server-start.sh', ...)
        args = self.kafka_run_class_args("kafka.Kafka", properties.strpath)
        env = self.kafka_run_class_env()
        if self.sasl_enabled:
            opts = env.get('KAFKA_OPTS', '').strip()
            opts += ' -Djava.security.auth.login.config={}'.format(jaas_conf.strpath)
            env['KAFKA_OPTS'] = opts
            self.render_template(jaas_conf_template, jaas_conf, vars(self))

        timeout = 5
        max_timeout = 120
        backoff = 1
        end_at = time.time() + max_timeout
        tries = 1
        while time.time() < end_at:
            # We have had problems with port conflicts on travis
            # so we will try a different port on each retry
            # unless the fixture was passed a specific port
            if self.auto_port:
                self.port = get_open_port()
            self.out('Attempting to start on port %d (try #%d)' % (self.port, tries))
            self.render_template(properties_template, properties, vars(self))

            self.child = SpawnedService(args, env)
            self.child.start()
            timeout = min(timeout, max(end_at - time.time(), 0))
            if self._broker_ready(timeout) and self._scram_user_present(timeout):
                break

            self.child.dump_logs()
            self.child.stop()

            timeout *= 2
            time.sleep(backoff)
            tries += 1
            backoff += 1
        else:
            raise RuntimeError('Failed to start KafkaInstance before max_timeout')

        (self._client,) = self.get_clients(1, client_id='_internal_client')

        self.out("Done!")
        self.running = True

    def _broker_ready(self, timeout):
        return self.child.wait_for(self.start_pattern, timeout=timeout)

    def _scram_user_present(self, timeout):
        # no need to wait for scram user if scram is not used
        if not self.sasl_enabled or not self.sasl_mechanism.startswith('SCRAM-SHA-'):
            return True
        return self.child.wait_for(self.scram_pattern, timeout=timeout)

    def open(self):
        if self.running:
            self.out("Instance already running")
            return

        # Create directories
        if self.tmp_dir is None:
            self.tmp_dir = py.path.local.mkdtemp() #pylint: disable=no-member
        self.tmp_dir.ensure(dir=True)
        self.tmp_dir.ensure('logs', dir=True)
        self.tmp_dir.ensure('data', dir=True)
        properties = self.tmp_dir.join('kafka.properties')
        properties_template = self.test_resource('kafka.properties')
        self.render_template(properties_template, properties, vars(self))

        self.out("Running local instance...")
        log.info("  host            = %s", self.host)
        log.info("  port            = %s", self.port or '(auto)')
        log.info("  transport       = %s", self.transport)
        log.info("  sasl_mechanism  = %s", self.sasl_mechanism)
        log.info("  broker_id       = %s", self.broker_id)
        log.info("  zk_host         = %s", self.zk_host)
        log.info("  zk_port         = %s", self.zk_port)
        log.info("  zk_chroot       = %s", self.zk_chroot)
        log.info("  replicas        = %s", self.replicas)
        log.info("  partitions      = %s", self.partitions)
        log.info("  tmp_dir         = %s", self.tmp_dir.strpath)

        if self.zookeeper:
            if self.zk_chroot:
                self._create_zk_chroot()
            # add user to zookeeper for the first server
            if self.sasl_enabled and self.sasl_mechanism.startswith("SCRAM-SHA") and self.broker_id == 0:
                self._add_scram_user()

        else:
            # running in KRaft mode
            self._format_log_dirs()

        self.sasl_config = self._sasl_config()
        self.jaas_config = self._jaas_config()
        self.start()

        atexit.register(self.close)

    def __del__(self):
        self.close()

    def stop(self):
        if self.external:
            return
        if not self.running:
            self.out("Instance already stopped")
            return

        self.out("Stopping...")
        self.child.stop()
        self.child = None
        self.running = False
        self.out("Stopped!")

    def close(self):
        self.stop()
        if self.tmp_dir is not None:
            self.tmp_dir.remove()
            self.tmp_dir = None
        self.out("Done!")

    def dump_logs(self):
        super(KafkaFixture, self).dump_logs()
        self.zookeeper.dump_logs()

    def _format_log_dirs(self):
        self.out("Formatting log dirs for kraft bootstrapping")
        args = self.run_script('kafka-storage.sh', 'format', '--standalone', '-t', self.cluster_id, '-c', self.tmp_dir.join("kafka.properties"))
        if self.sasl_enabled and self.sasl_mechanism.startswith("SCRAM-SHA"):
            args.extend(['--add-scram', '{}=[name={},password={}]'.format(self.sasl_mechanism, self.broker_user, self.broker_password)])
        env = self.kafka_run_class_env()
        proc = subprocess.Popen(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        if proc.returncode != 0:
            self.out("Failed to format log dirs for kraft bootstrap!")
            self.out(stdout)
            self.out(stderr)
            raise RuntimeError("Failed to format log dirs!")
        return True

    def _send_request(self, request, timeout=None):
        def _failure(error):
            raise error
        retries = 10
        while True:
            node_id = self._client.least_loaded_node()
            for connect_retry in range(40):
                self._client.maybe_connect(node_id)
                if self._client.connected(node_id):
                    break
                self._client.poll(timeout_ms=100)
            else:
                raise RuntimeError('Could not connect to broker with node id %s' % (node_id,))

            try:
                future = self._client.send(node_id, request)
                future.error_on_callbacks = True
                future.add_errback(_failure)
                self._client.poll(future=future, timeout_ms=timeout)
                if not future.is_done:
                    raise KafkaTimeoutError()
                return future.value
            except Exception as exc:
                time.sleep(1)
                retries -= 1
                if retries == 0:
                    raise exc
                else:
                    pass # retry

    def _create_topic(self, topic_name, num_partitions=None, replication_factor=None, timeout_ms=10000):
        if num_partitions is None:
            num_partitions = self.partitions
        if replication_factor is None:
            replication_factor = self.replicas

        # Try different methods to create a topic, from the fastest to the slowest
        if self.auto_create_topic and num_partitions == self.partitions and replication_factor == self.replicas:
            self._create_topic_via_metadata(topic_name, timeout_ms)
        elif env_kafka_version() >= (0, 10, 1, 0) and env_kafka_version() < (4, 0):
            try:
                # 4.0 brokers dropped support for CreateTopicsRequest v0 (TODO: pick from api_versions)
                self._create_topic_via_admin_api(topic_name, num_partitions, replication_factor, timeout_ms)
            except InvalidReplicationFactorError:
                # wait and try again
                # on travis the brokers sometimes take a while to find themselves
                time.sleep(0.5)
                self._create_topic_via_admin_api(topic_name, num_partitions, replication_factor, timeout_ms)
        else:
            self._create_topic_via_cli(topic_name, num_partitions, replication_factor)

    def _create_topic_via_metadata(self, topic_name, timeout_ms=10000):
        timeout_at = time.time() + timeout_ms / 1000
        while time.time() < timeout_at:
            response = self._send_request(MetadataRequest[0]([topic_name]), timeout_ms)
            if response.topics[0][0] == 0:
                return
            log.warning("Unable to create topic via MetadataRequest: err %d", response.topics[0][0])
            time.sleep(1)
        else:
            raise RuntimeError('Unable to create topic via MetadataRequest')

    def _create_topic_via_admin_api(self, topic_name, num_partitions, replication_factor, timeout_ms=10000):
        request = CreateTopicsRequest[0]([(topic_name, num_partitions,
                                           replication_factor, [], [])], timeout_ms)
        response = self._send_request(request, timeout=timeout_ms)
        for topic_result in response.topic_errors:
            error_code = topic_result[1]
            if error_code != 0:
                raise errors.for_code(error_code)

    def _create_topic_via_cli(self, topic_name, num_partitions, replication_factor):
        args = self.run_script('kafka-topics.sh',
                               '--create',
                               '--topic', topic_name,
                               '--partitions', self.partitions \
                                   if num_partitions is None else num_partitions,
                               '--replication-factor', self.replicas \
                                   if replication_factor is None \
                                   else replication_factor,
                               *self._cli_connect_args())
        if env_kafka_version() >= (0, 10):
            args.append('--if-not-exists')
        env = self.kafka_run_class_env()
        proc = subprocess.Popen(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        if proc.returncode != 0:
            if 'kafka.common.TopicExistsException' not in stdout:
                self.out("Failed to create topic %s" % (topic_name,))
                self.out(stdout)
                self.out(stderr)
                raise RuntimeError("Failed to create topic %s" % (topic_name,))

    def _cli_connect_args(self):
        if env_kafka_version() < (3, 0, 0):
            return ['--zookeeper', '%s:%s/%s' % (self.zookeeper.host, self.zookeeper.port, self.zk_chroot)]
        else:
            args = ['--bootstrap-server', '%s:%s' % (self.host, self.port)]
            if self.sasl_enabled:
                command_conf = self.tmp_dir.join("sasl_command.conf")
                self.render_template(self.test_resource("sasl_command.conf"), command_conf, vars(self))
                args.append('--command-config')
                args.append(command_conf.strpath)
            return args

    def get_topic_names(self):
        cmd = self.run_script('kafka-topics.sh', '--list', *self._cli_connect_args())
        env = self.kafka_run_class_env()
        env.pop('KAFKA_LOG4J_OPTS')
        proc = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        if proc.returncode != 0:
            self.out("Failed to list topics!")
            self.out(stdout)
            self.out(stderr)
            raise RuntimeError("Failed to list topics!")
        return stdout.decode().splitlines(False)

    def create_topics(self, topic_names, num_partitions=None, replication_factor=None):
        for topic_name in topic_names:
            self._create_topic(topic_name, num_partitions, replication_factor)

    def _enrich_client_params(self, params, **defaults):
        params = params.copy()
        for key, value in defaults.items():
            params.setdefault(key, value)
        params.setdefault('bootstrap_servers', self.bootstrap_server())
        if self.sasl_enabled:
            params.setdefault('sasl_mechanism', self.sasl_mechanism)
            params.setdefault('security_protocol', self.transport)
            if self.sasl_mechanism in ('PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512'):
                params.setdefault('sasl_plain_username', self.broker_user)
                params.setdefault('sasl_plain_password', self.broker_password)
        return params

    @staticmethod
    def _create_many_clients(cnt, cls, *args, **params):
        client_id = params['client_id']
        for _ in range(cnt):
            params['client_id'] = '%s_%s' % (client_id, random_string(4))
            yield cls(*args, **params)

    def get_clients(self, cnt=1, **params):
        params = self._enrich_client_params(params, client_id='client')
        for client in self._create_many_clients(cnt, KafkaClient, **params):
            yield client

    def get_admin_clients(self, cnt, **params):
        params = self._enrich_client_params(params, client_id='admin_client')
        for client in self._create_many_clients(cnt, KafkaAdminClient, **params):
            yield client

    def get_consumers(self, cnt, topics, **params):
        params = self._enrich_client_params(
            params, client_id='consumer', heartbeat_interval_ms=500, auto_offset_reset='earliest'
        )
        for client in self._create_many_clients(cnt, KafkaConsumer, *topics, **params):
            yield client

    def get_producers(self, cnt, **params):
        params = self._enrich_client_params(params, client_id='producer')
        for client in self._create_many_clients(cnt, KafkaProducer, **params):
            yield client


def get_api_versions():
    logging.basicConfig(level=logging.ERROR)
    zk = ZookeeperFixture.instance()
    k = KafkaFixture.instance(0, zk)

    from kafka import KafkaClient
    client = KafkaClient(bootstrap_servers='localhost:{}'.format(k.port))
    client.check_version()

    from pprint import pprint

    pprint(client.get_api_versions())

    client.close()
    k.close()
    zk.close()


def run_brokers():
    logging.basicConfig(level=logging.ERROR)
    k = KafkaFixture.instance(0)
    zk = k.zookeeper

    print("Kafka", k.kafka_version, "running on port:", k.port)
    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        print("Bye!")
        k.close()
        if zk:
            zk.close()


if __name__ == '__main__':
    import sys
    if len(sys.argv) < 2:
        print("Commands: get_api_versions")
        exit(0)
    cmd = sys.argv[1]
    if cmd == 'get_api_versions':
        get_api_versions()
    elif cmd == 'kafka':
        run_brokers()
    else:
        print("Unknown cmd: %s", cmd)
        exit(1)
