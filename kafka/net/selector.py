import collections
import copy
import inspect
import logging
import heapq
import selectors
import socket
import threading
import time

from kafka.future import Future


log = logging.getLogger(__name__)

def log_trace(msg, *args, **kwargs):
    log.log(5, msg, *args, **kwargs)


MAX_TIMEOUT = 2147483


def yield_callback(callback):
    yield callback()


def _initialize_coro(maybe_coro):
    if inspect.isgenerator(maybe_coro) or inspect.iscoroutine(maybe_coro):
        return maybe_coro
    elif inspect.isgeneratorfunction(maybe_coro) or inspect.iscoroutinefunction(maybe_coro):
        # Defer calling until the Task actually runs to avoid
        # "coroutine was never awaited" warnings if the Task is discarded
        return maybe_coro
    elif inspect.isfunction(maybe_coro) or inspect.ismethod(maybe_coro):
        return yield_callback(maybe_coro)
    else:
        raise TypeError('Generator or coroutine not found: %s' % type(maybe_coro))


class KernelEvent:
    def __init__(self, method, *args):
        self.method = method
        self.args = args

    def __await__(self):
        return (yield self)


class Task:
    def __init__(self, coro):
        self._stack = (_initialize_coro(coro), None)
        self._res = None
        self._exc = None
        self.scheduled_at = None

    def __call__(self, arg=None):
        ret, exc = (None, arg) if isinstance(arg, Exception) else (arg, None)
        while True:
            coro = self._stack[0]
            if callable(coro) and not inspect.isgenerator(coro) and not inspect.iscoroutine(coro):
                coro = coro()
                self._stack = (coro, self._stack[1])
            try:
                if exc:
                    ret = coro.throw(exc)
                else:
                    ret = coro.send(ret)

                if isinstance(ret, (KernelEvent, Future)):
                    # handle in event loop
                    return ret

                elif inspect.isgenerator(ret) or inspect.iscoroutine(ret) or inspect.isfunction(ret):
                    self.push_stack(ret)
                    ret = None

            except StopIteration as final:
                self._stack = self._stack[1]
                if not self._stack:
                    # we're done, back to event loop
                    self._res = final.value
                    raise
                else:
                    #ret = final.value
                    exc = None

            except BaseException as e:
                self._stack = self._stack[1]
                if not self._stack:
                    self._exc = e
                    raise
                else:
                    ret = None
                    exc = e
            else:
                exc = None

    def push_stack(self, coro):
        self._stack = (_initialize_coro(coro), self._stack)

    @property
    def is_done(self):
        return self._stack is None

    @property
    def result(self):
        if not self.is_done:
            raise RuntimeError('Task not complete!')
        return self._res

    @property
    def exception(self):
        if not self.is_done:
            raise RuntimeError('Task not complete!')
        return self._exc


class NetworkSelector:
    DEFAULT_CONFIG = {
        'selector': selectors.DefaultSelector,
        # Warn (or, in debug mode, raise) when a single ready-task step takes
        # longer than this many seconds. A coroutine that hits this threshold
        # is blocking the event loop -- common cause is a tight sync loop
        # over a synchronously-raising await (see cluster._refresh_loop hang
        # where RuntimeError from a closed manager was caught and retried).
        # Mirrors asyncio's loop.slow_callback_duration. Set to 0 to disable.
        'slow_task_threshold_secs': 0.1,
        # When True, raise RuntimeError on slow tasks instead of just warning.
        # Useful in tests so livelocks fail loudly.
        'raise_on_slow_task': False,
    }

    def __init__(self, **configs):
        self.config = copy.copy(self.DEFAULT_CONFIG)
        for key in self.config:
            if key in configs:
                self.config[key] = configs[key]

        # Used by poll() as both a mutex (cross-thread concurrent-entry guard)
        # and the in-loop flag. acquire(blocking=False) doubles as the
        # "is anyone in poll() right now?" check. Held only across poll()'s
        # body; never held by anything else.
        # _poll_owner tracks which thread holds the lock so we can produce
        # an accurate diagnostic (recursive vs concurrent) on contention.
        self._poll_lock = threading.Lock()
        self._poll_owner = None
        self._closed = False
        self._stop = False
        self._selector = self.config['selector']()
        self._scheduled = [] # managed by heapq
        self._ready = collections.deque()
        self._current = None
        self._wakeup_r, self._wakeup_w = socket.socketpair()
        self._wakeup_r.setblocking(False)
        self._wakeup_w.setblocking(False)
        self._selector.register(self._wakeup_r, selectors.EVENT_READ, (None, None))

    def __str__(self):
        return '<NetworkSelector ready=%d scheduled=%d waiting=%d>' % (len(self._ready), len(self._scheduled), len(self._selector.get_map()))

    def run(self):
        while self._scheduled or self._ready:
            self._poll_once()

    def run_forever(self):
        """Run the event loop until stop() is called. Intended to be driven by
        a dedicated IO thread. Wake-ups from other threads must go through
        call_soon_threadsafe() so the select() loop returns promptly."""
        self._stop = False
        while not self._stop:
            self._poll_once()

    def stop(self):
        """Signal run_forever() to exit. Safe to call from any thread."""
        self._stop = True
        self.wakeup()

    def run_until_done(self, task_or_future):
        if not isinstance(task_or_future, (Future, Task)):
            task_or_future = Task(task_or_future)
        if isinstance(task_or_future, Task):
            self.call_soon(task_or_future)
        while not task_or_future.is_done:
            self._poll_once()
        return task_or_future

    def call_at(self, when, task):
        if not isinstance(task, Task):
            task = Task(task)
        task.scheduled_at = when
        heapq.heappush(self._scheduled, (when, task))
        return task

    def call_later(self, delay, task):
        if not isinstance(task, Task):
            task = Task(task)
        self.call_at(time.monotonic() + delay, task)
        return task

    def call_soon(self, task):
        if not isinstance(task, Task):
            task = Task(task)
        self._ready.append(task)
        return task

    def unschedule(self, task):
        if task.scheduled_at is not None:
            self._scheduled.remove((task.scheduled_at, task))
            task.scheduled_at = None

    def reschedule(self, when, task):
        self.unschedule(task)
        self.call_at(when, task)
        return task

    def sleep(self, delay):
        return KernelEvent('_sleep', delay)

    def _sleep(self, delay):
        self.call_later(delay, self._current)
        self._current = None

    def wait_write(self, fileobj):
        return KernelEvent('_wait_write', fileobj)

    def _wait_write(self, fileobj):
        self.register_event(fileobj, selectors.EVENT_WRITE, self._current)
        self._current.push_stack(lambda: self.unregister_event(fileobj, selectors.EVENT_WRITE))
        self._current = None

    def wait_read(self, fileobj):
        return KernelEvent('_wait_read', fileobj)

    def _wait_read(self, fileobj):
        self.register_event(fileobj, selectors.EVENT_READ, self._current)
        self._current.push_stack(lambda: self.unregister_event(fileobj, selectors.EVENT_READ))
        self._current = None

    def _schedule_tasks(self):
        while self._scheduled and self._scheduled[0][0] <= time.monotonic():
            _, task = heapq.heappop(self._scheduled)
            task.scheduled_at = None
            self._ready.append(task)

    def _next_scheduled_timeout(self, now):
        try:
            return self._scheduled[0][0] - now
        except IndexError:
            return None

    # Note: Windows select works w/ sockets only
    def register_event(self, fileobj, event, task):
        log_trace('net.register_event: %s, %s, %s', fileobj, event, task)
        if not isinstance(task, Task):
            task = Task(task)
        try:
            key = self._selector.get_key(fileobj)
            reader, writer = key.data
            if event == selectors.EVENT_READ and reader:
                raise RuntimeError("EVENT_READ already registered for fileobj %s by %s (new: %s)" % (fileobj, reader, task))
            if event == selectors.EVENT_WRITE and writer:
                raise RuntimeError("EVENT_WRITE already registered for fileobj %s by %s (new: %s)" % (fileobj, writer, task))
            self._selector.modify(fileobj, key.events | event, (task, writer) if event == selectors.EVENT_READ else (reader, task))
        except KeyError:
            self._selector.register(fileobj, event, (task, None) if event == selectors.EVENT_READ else (None, task))

    def unregister_event(self, fileobj, event):
        log_trace('net.unregister_event: %s, %s', fileobj, event)
        try:
            key = self._selector.get_key(fileobj)
            reader, writer = key.data
            events = key.events & ~event
            if not events:
                self._selector.unregister(fileobj)
            else:
                self._selector.modify(fileobj, events, (None, writer) if event == selectors.EVENT_READ else (reader, None))
        except KeyError:
            pass

    def add_reader(self, fileobj, task):
        self.register_event(fileobj, selectors.EVENT_READ, task)

    def remove_reader(self, fileobj):
        self.unregister_event(fileobj, selectors.EVENT_READ)

    def add_writer(self, fileobj, task):
        self.register_event(fileobj, selectors.EVENT_WRITE, task)

    def remove_writer(self, fileobj):
        self.unregister_event(fileobj, selectors.EVENT_WRITE)

    def poll(self, timeout_ms=None, future=None):
        if not self._poll_lock.acquire(blocking=False):
            # Lock contended. Distinguish recursive (this thread is already
            # in poll, e.g. via a task callback) from concurrent (a different
            # thread is in poll). Same-thread reentry of a non-RLock fails
            # the same way as cross-thread contention.
            if self._poll_owner is threading.current_thread():
                raise RuntimeError('Recursive access to net.poll!')
            raise RuntimeError('Concurrent access to net.poll!')
        self._poll_owner = threading.current_thread()
        try:
            log_trace('poll: enter')
            start_at = time.monotonic()
            inner_timeout = timeout_ms / 1000 if timeout_ms is not None else None
            if future is not None and future.is_done:
                inner_timeout = 0
            while True:
                self._poll_once(inner_timeout)
                if future is None or future.is_done:
                    break
                elif timeout_ms is not None:
                    inner_timeout = (timeout_ms / 1000) - (time.monotonic() - start_at)
                    if inner_timeout <= 0:
                        break
        finally:
            self._poll_owner = None
            self._poll_lock.release()
            log_trace('poll: exit')

    def _poll_once(self, timeout=None):
        log_trace('_poll_once: enter')
        if self._ready:
            timeout = 0
        else:
            scheduled_timeout = self._next_scheduled_timeout(time.monotonic())
            if scheduled_timeout is not None:
                timeout = min(timeout, scheduled_timeout) if timeout is not None else scheduled_timeout
        if timeout is not None:
            if timeout > MAX_TIMEOUT:
                timeout = MAX_TIMEOUT
            elif timeout < 0:
                timeout = 0
        elif not self._selector.get_map():
            timeout = 0

        ready_events = self._selector.select(timeout)
        log_trace('_poll_once: %d ready_events', len(ready_events))
        self._process_events(ready_events)
        self._schedule_tasks()

        threshold = self.config['slow_task_threshold_secs']
        n = len(self._ready)
        for i in range(n):
            self._current = self._ready.popleft()
            step_start = time.monotonic() if threshold else None
            try:
                log_trace('Calling task %s', self._current)
                event = self._current()

            except StopIteration:
                pass

            except BaseException as e:
                log.exception(e)

            else:
                if isinstance(event, KernelEvent):
                    log_trace('kernel event %s', event.method)
                    getattr(self, event.method)(*event.args)
                elif isinstance(event, Future):
                    event.add_both(lambda _, task=self._current: self.call_soon(task))
                else:
                    raise RuntimeError('Unhandled event type: %s' % event)

            if threshold:
                elapsed = time.monotonic() - step_start
                if elapsed > threshold:
                    msg = (
                        'Task %r ran for %.3fs (>%.3fs threshold). It is '
                        'blocking the event loop -- likely a tight sync loop '
                        'inside a coroutine. Other pollers will time out.'
                        % (self._current, elapsed, threshold))
                    if self.config['raise_on_slow_task']:
                        self._current = None
                        raise RuntimeError(msg)
                    log.warning(msg)

        self._current = None
        log_trace('_poll_once: exit')

    def wakeup(self):
        try:
            self._wakeup_w.send(b'\x00')
        except (BlockingIOError, OSError):
            pass

    def call_soon_threadsafe(self, callback):
        self.call_soon(callback)
        self.wakeup()

    def _rebuild_wakeup_socketpair(self):
        for s in (self._wakeup_r, self._wakeup_w):
            try:
                self._selector.unregister(s)
            except Exception:
                pass
            try:
                s.close()
            except Exception:
                pass
        self._wakeup_r, self._wakeup_w = socket.socketpair()
        self._wakeup_r.setblocking(False)
        self._wakeup_w.setblocking(False)
        self._selector.register(self._wakeup_r, selectors.EVENT_READ, (None, None))

    def close(self):
        self._closed = True
        for s in (self._wakeup_r, self._wakeup_w):
            try:
                self._selector.unregister(s)
            except Exception:
                pass
            try:
                s.close()
            except Exception:
                pass
        self._selector.close()

    def _process_events(self, event_list):
        for key, events in event_list:
            reader, writer = key.data
            fileobj = key.fileobj

            # Drain wakeup socketpair
            if fileobj is self._wakeup_r:
                try:
                    data = self._wakeup_r.recv(1024)
                    if not data:
                        log.warning('Wakeup socket returned empty. Rebuilding.')
                        self._rebuild_wakeup_socketpair()
                except BlockingIOError:
                    pass
                except Exception as e:
                    log.warning('Error reading wakeup socket: %s. Rebuilding.', e)
                    self._rebuild_wakeup_socketpair()
                continue

            if events & selectors.EVENT_WRITE:
                if writer is not None:
                    self._ready.append(writer)
                else:
                    log.warning("Selector got WRITE event without writer...")

            if events & selectors.EVENT_READ:
                if reader is not None:
                    self._ready.append(reader)
                else:
                    log.warning("Selector got READ event without reader...")
