from __future__ import absolute_import

import logging
import os
import re
import select
import subprocess
import sys
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
        super(SpawnedService, self).__init__()

        if args is None:
            raise TypeError("args parameter is required")
        self.args = args
        self.env = env
        self.captured_stdout = []
        self.captured_stderr = []

        self.should_die = threading.Event()
        self.child = None
        self.alive = False
        self.daemon = True
        log.info("Created service for command:")
        log.info(" "+' '.join(self.args))
        log.debug("With environment:")
        for key, value in self.env.items():
            log.debug("  {key}={value}".format(key=key, value=value))

    def _spawn(self):
        if self.alive: return
        if self.child and self.child.poll() is None: return

        self.child = subprocess.Popen(
            self.args,
            preexec_fn=os.setsid, # to avoid propagating signals
            env=self.env,
            bufsize=1,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        self.alive = self.child.poll() is None

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

    def run(self):
        self._spawn()
        while True:
            try:
                (rds, _, _) = select.select([self.child.stdout, self.child.stderr], [], [], 1)
            except select.error as ex:
                if ex.args[0] == 4:
                    continue
                else:
                    raise

            if self.child.stdout in rds:
                line = self.child.stdout.readline().decode('utf-8').rstrip()
                if line:
                    self.captured_stdout.append(line)

            if self.child.stderr in rds:
                line = self.child.stderr.readline().decode('utf-8').rstrip()
                if line:
                    self.captured_stderr.append(line)

            if self.child.poll() is not None:
                self.dump_logs()
                break

            if self.should_die.is_set():
                self._despawn()
                break

    def dump_logs(self):
        sys.stderr.write('\n'.join(self.captured_stderr))
        sys.stdout.write('\n'.join(self.captured_stdout))

    def wait_for(self, pattern, timeout=30):
        start = time.time()
        while True:
            if not self.is_alive():
                raise RuntimeError("Child thread died already.")

            elapsed = time.time() - start
            if elapsed >= timeout:
                log.error("Waiting for %r timed out after %d seconds", pattern, timeout)
                return False

            if re.search(pattern, '\n'.join(self.captured_stdout), re.IGNORECASE) is not None:
                log.info("Found pattern %r in %d seconds via stdout", pattern, elapsed)
                return True
            if re.search(pattern, '\n'.join(self.captured_stderr), re.IGNORECASE) is not None:
                log.info("Found pattern %r in %d seconds via stderr", pattern, elapsed)
                return True
            time.sleep(0.1)

    def stop(self):
        self.should_die.set()
        self.join()
