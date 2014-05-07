import logging
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

class ExternalService(object):
    def __init__(self, host, port):
        print("Using already running service at %s:%d" % (host, port))
        self.host = host
        self.port = port

    def open(self):
        pass

    def close(self):
        pass


class SpawnedService(threading.Thread):
    def __init__(self, args=[]):
        threading.Thread.__init__(self)

        self.args = args
        self.captured_stdout = []
        self.captured_stderr = []

        self.should_die = threading.Event()

    def run(self):
        self.run_with_handles()

    def run_with_handles(self):
        self.child = subprocess.Popen(
            self.args,
            bufsize=1,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        alive = True

        while True:
            (rds, wds, xds) = select.select([self.child.stdout, self.child.stderr], [], [], 1)

            if self.child.stdout in rds:
                line = self.child.stdout.readline()
                self.captured_stdout.append(line)

            if self.child.stderr in rds:
                line = self.child.stderr.readline()
                self.captured_stderr.append(line)

            if self.should_die.is_set():
                self.child.terminate()
                alive = False

            poll_results = self.child.poll()
            if poll_results is not None:
                if not alive:
                    break
                else:
                    self.dump_logs()
                    raise RuntimeError("Subprocess has died. Aborting. (args=%s)" % ' '.join(str(x) for x in self.args))

    def dump_logs(self):
        logging.critical('stderr')
        for line in self.captured_stderr:
            logging.critical(line.rstrip())

        logging.critical('stdout')
        for line in self.captured_stdout:
            logging.critical(line.rstrip())

    def wait_for(self, pattern, timeout=10):
        t1 = time.time()
        while True:
            t2 = time.time()
            if t2 - t1 >= timeout:
                try:
                    self.child.kill()
                except:
                    logging.exception("Received exception when killing child process")
                self.dump_logs()

                raise RuntimeError("Waiting for %r timed out" % pattern)

            if re.search(pattern, '\n'.join(self.captured_stdout), re.IGNORECASE) is not None:
                return
            if re.search(pattern, '\n'.join(self.captured_stderr), re.IGNORECASE) is not None:
                return
            time.sleep(0.1)

    def start(self):
        threading.Thread.start(self)

    def stop(self):
        self.should_die.set()
        self.join()

