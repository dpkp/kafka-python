import collections
import copy
import inspect
import logging
import heapq
import selectors
import socket
import threading
import time

import kafka.errors as Errors
from kafka.future import Future
from kafka.version import __version__


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
        'client_id': 'kafka-python-' + __version__,
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
        # Strong refs to every Task that hasn't completed yet. Without this,
        # a Task suspended on an externally-unreachable awaitable (e.g. a
        # Future created and awaited inside the Task's own coroutine) forms
        # an orphan cycle and is subject to gc collection. Keeping every
        # pending Task rooted on the selector itself prevents the cycle from
        # ever being garbage-eligible. Tasks are removed when they raise
        # StopIteration (normal completion) or BaseException (raised) inside
        # _poll_once. This mirrors asyncio's loop._tasks weakset.
        self._pending_tasks = set()
        self._current = None
        self._wakeup_r, self._wakeup_w = socket.socketpair()
        self._wakeup_r.setblocking(False)
        self._wakeup_w.setblocking(False)
        self._selector.register(self._wakeup_r, selectors.EVENT_READ, (None, None))
        self._io_thread = None
        self._pending_waiters = {}  # event -> state dict, for pending run() waiters
        self._pending_waiters_lock = threading.Lock()

    def __str__(self):
        return '<NetworkSelector ready=%d scheduled=%d waiting=%d>' % (len(self._ready), len(self._scheduled), len(self._selector.get_map()))

    def run_forever(self):
        """Run the event loop until stop() is called. Intended to be driven by
        a dedicated IO thread. Wake-ups from other threads must go through
        call_soon_threadsafe() so the select() loop returns promptly."""
        self._stop = False
        log.info('IO loop starting (client_id=%s)', self.config['client_id'])
        try:
            while not self._stop:
                self._poll_once()
            self.drain()
        except BaseException as exc:
            log.exception('IO loop crashed (client_id=%s)', self.config['client_id'])
            self._fail_pending_waiters(exc)
            raise
        else:
            log.info('IO loop exited cleanly (client_id=%s, stop=%s)',
                     self.config['client_id'], self._stop)

    def start(self):
        """Spawn a daemon IO thread that owns the event loop. Idempotent."""
        if self._io_thread is not None:
            return
        t = threading.Thread(target=self.run_forever,
                             name='kafka-io-%s' % self.config['client_id'],
                             daemon=True)
        self._io_thread = t
        t.start()

    def stop(self, timeout_ms=None):
        """Signal run_forever() to exit. Safe to call from any thread."""
        if self._stop or self._io_thread is None:
            return
        self._stop = True
        self.wakeup()
        self._io_thread.join(timeout_ms / 1000 if timeout_ms is not None else None)
        self._io_thread = None
        self._fail_pending_waiters(Errors.KafkaConnectionError('Manager stopped'))

    def _fail_pending_waiters(self, exc):
        with self._pending_waiters_lock:
            waiters = list(self._pending_waiters.items())
            self._pending_waiters.clear()
        for event, state in waiters:
            state['exception'] = exc
            event.set()

    def run(self, coro, *args):
        """Schedules coro on the event loop, blocks until complete, returns value or raises.

        If an IO thread is running (via start()), the caller thread blocks on
        a cross-thread Event while the coroutine runs on the IO thread. Safe
        to call concurrently from multiple caller threads.

        If no IO thread is running, falls back to driving the loop on the
        caller thread (legacy behavior).
        """
        if self._io_thread is None:
            future = self.call_soon_with_future(coro, *args)
            self.poll(future=future)
            if future.exception is not None:
                raise future.exception
            return future.value
        elif threading.current_thread() is self._io_thread:
          raise RuntimeError(
              "Cannot block on net.run() from the IO thread itself. "
              "This typically happens when a synchronous rebalance listener "
              "(or another IO-thread callback) calls a blocking consumer/admin API. "
              "Use AsyncConsumerRebalanceListener and await the async variant, "
              "or move the blocking work to a worker thread.")

        event = threading.Event()
        state = {'value': None, 'exception': None}
        async def waiter():
            try:
                state['value'] = await self._invoke(coro, *args)
            except BaseException as exc:
                state['exception'] = exc
            finally:
                with self._pending_waiters_lock:
                    self._pending_waiters.pop(event, None)
                event.set()
        with self._pending_waiters_lock:
            self._pending_waiters[event] = state
        self.call_soon_threadsafe(waiter)
        event.wait()
        if state['exception'] is not None:
            raise state['exception']
        return state['value']

    def drain(self, scheduled=False):
        while self._ready or (scheduled and self._scheduled):
            self._poll_once()

    def call_at(self, when, task):
        if not isinstance(task, Task):
            task = Task(task)
        task.scheduled_at = when
        heapq.heappush(self._scheduled, (when, task))
        self._pending_tasks.add(task)
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
        self._pending_tasks.add(task)
        return task

    def call_soon_threadsafe(self, callback):
        task = self.call_soon(callback)
        self.wakeup()
        return task

    def call_soon_with_future(self, coro, *args):
        if hasattr(coro, '__await__'):
            assert not args, 'initiated coroutine does not accept args'
        future = Future()
        async def wrapper():
            try:
                future.success(await self._invoke(coro, *args))
            except BaseException as exc:
                future.failure(exc)
        self.call_soon_threadsafe(wrapper)
        return future

    async def _invoke(self, coro, *args):
        """Invoke coro/awaitable/function and fully resolve the result.

        If the result is itself a Future (e.g. send() returning an unresolved
        Future), it is awaited so callers receive the resolved value.
        """
        if inspect.iscoroutinefunction(coro):
            result = await coro(*args)
        elif hasattr(coro, '__await__'):
            result = await coro
        else:
            result = coro(*args)
        if inspect.iscoroutine(result) or hasattr(result, '__await__'):
            result = await result
        while isinstance(result, Future):
            result = await result
        return result

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

    def wait_write(self, fileobj):
        return KernelEvent('_wait_write', fileobj)

    def _wait_write(self, fileobj):
        self.register_event(fileobj, selectors.EVENT_WRITE, self._current)
        self._current.push_stack(lambda: self.unregister_event(fileobj, selectors.EVENT_WRITE))

    def wait_read(self, fileobj):
        return KernelEvent('_wait_read', fileobj)

    def _wait_read(self, fileobj):
        self.register_event(fileobj, selectors.EVENT_READ, self._current)
        self._current.push_stack(lambda: self.unregister_event(fileobj, selectors.EVENT_READ))

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
        log_trace('poll: exit')

    def _poll_once(self, timeout=None):
        log_trace('_poll_once: enter')
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
                    # Task ran to completion. Drop the strong ref so the Task
                    # (and its coroutine, frames, locals) is now collectable.
                    self._pending_tasks.discard(self._current)

                except BaseException as e:
                    log.exception(e)
                    # Same as StopIteration -- task is done either way.
                    self._pending_tasks.discard(self._current)

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

        finally:
            self._poll_owner = None
            self._poll_lock.release()
            log_trace('_poll_once: exit')

    def wakeup(self):
        try:
            self._wakeup_w.send(b'\x00')
        except (BlockingIOError, OSError):
            pass

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
        if self._closed:
            return
        self._closed = True
        if self._io_thread is not None:
            self.stop()
        self.drain()
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
