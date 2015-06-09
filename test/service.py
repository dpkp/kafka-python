import logging
import re
import select
import subprocess
import threading
import time

__all__ = [
    'ExternalService',
    'SpawnedService',

]


log = logging.getLogger(__name__)


class ExternalService(object):
    def __init__(self, host, port):
        log.info("Using already running service at %s:%d", host, port)
        self.host = host
        self.port = port

    def open(self):
        pass

    def close(self):
        pass


class SpawnedService(threading.Thread):
    def __init__(self, args=None, env=None):
        threading.Thread.__init__(self)

        if args is None:
            raise TypeError("args parameter is required")
        self.args = args
        self.env = env
        self.captured_stdout = []
        self.captured_stderr = []

        self.should_die = threading.Event()
        self.child = None
        self.alive = False

    def run(self):
        self.run_with_handles()

    def _spawn(self):
        if self.alive: return
        if self.child and self.child.poll() is None: return

        self.child = subprocess.Popen(
            self.args,
            env=self.env,
            bufsize=1,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        self.alive = True

    def _despawn(self):
        if self.child.poll() is None:
            self.child.terminate()
        self.alive = False
        for _ in range(50):
            if self.child.poll() is not None:
                self.child = None
                break
            time.sleep(0.1)
        else:
            self.child.kill()

    def run_with_handles(self):
        self._spawn()
        while True:
            (rds, _, _) = select.select([self.child.stdout, self.child.stderr], [], [], 1)

            if self.child.stdout in rds:
                line = self.child.stdout.readline()
                self.captured_stdout.append(line.decode('utf-8'))

            if self.child.stderr in rds:
                line = self.child.stderr.readline()
                self.captured_stderr.append(line.decode('utf-8'))

            if self.child.poll() is not None:
                self.dump_logs()
                self._spawn()

            if self.should_die.is_set():
                self._despawn()
                break

    def dump_logs(self):
        log.critical('stderr')
        for line in self.captured_stderr:
            log.critical(line.rstrip())

        log.critical('stdout')
        for line in self.captured_stdout:
            log.critical(line.rstrip())

    def wait_for(self, pattern, timeout=30):
        t1 = time.time()
        while True:
            t2 = time.time()
            if t2 - t1 >= timeout:
                try:
                    self.child.kill()
                except:
                    log.exception("Received exception when killing child process")
                self.dump_logs()

                log.error("Waiting for %r timed out after %d seconds", pattern, timeout)
                return False

            if re.search(pattern, '\n'.join(self.captured_stdout), re.IGNORECASE) is not None:
                log.info("Found pattern %r in %d seconds via stdout", pattern, (t2 - t1))
                return True
            if re.search(pattern, '\n'.join(self.captured_stderr), re.IGNORECASE) is not None:
                log.info("Found pattern %r in %d seconds via stderr", pattern, (t2 - t1))
                return True
            time.sleep(0.1)

    def start(self):
        threading.Thread.start(self)

    def stop(self):
        self.should_die.set()
        self.join()

