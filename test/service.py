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

    def configure_stderr(self, file=None, capture=False, show=False):
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

            if re.search(pattern, self.captured_stdout, re.IGNORECASE) is not None:
                return
            if re.search(pattern, self.captured_stderr, re.IGNORECASE) is not None:
                return
            time.sleep(0.1)

    def start(self):
        threading.Thread.start(self)

    def stop(self):
        self.should_die.set()
        self.join()

