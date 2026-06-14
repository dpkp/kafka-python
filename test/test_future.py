import pytest

from kafka.future import Future


class TestFutureAwait:
    def test_await_resolved_success(self):
        f = Future()
        f.success(42)
        coro = f.__await__()
        with pytest.raises(StopIteration) as exc_info:
            next(coro)
        assert exc_info.value.value == 42

    def test_await_resolved_failure(self):
        f = Future()
        f.failure(ValueError('bad'))
        coro = f.__await__()
        with pytest.raises(ValueError, match='bad'):
            next(coro)

    def test_await_pending_yields_self(self):
        f = Future()
        coro = f.__await__()
        yielded = next(coro)
        assert yielded is f

    def test_callbacks_still_work(self):
        f = Future()
        results = []
        f.add_callback(lambda v: results.append(('cb', v)))
        f.add_errback(lambda e: results.append(('eb', e)))
        f.success(42)
        assert results == [('cb', 42)]

    def test_errbacks_still_work(self):
        f = Future()
        results = []
        f.add_errback(lambda e: results.append(('eb', str(e))))
        f.failure(ValueError('bad'))
        assert results == [('eb', 'bad')]

    def test_chain(self):
        f1 = Future()
        f2 = Future()
        f1.chain(f2)
        f1.success('chained')
        assert f2.succeeded()
        assert f2.value == 'chained'

    def test_chain_failure(self):
        f1 = Future()
        f2 = Future()
        f1.chain(f2)
        f1.failure(ValueError('err'))
        assert f2.failed()
        assert isinstance(f2.exception, ValueError)


def _raise(exc):
    raise exc


class TestFutureErrorOnCallbacks:
    """error_on_callbacks is now a per-instance option (see #2366).

    An explicit value passed to ``Future(error_on_callbacks=...)`` takes
    precedence over the class-level default. (Note: the test suite sets the
    class-level default to True via ``test/__init__.py``, so these tests pass
    explicit values to assert override behavior independent of that default.)
    """

    def test_none_inherits_class_default(self):
        assert Future().error_on_callbacks is Future._default_error_on_callbacks

    def test_explicit_false_overrides_class_default(self):
        f = Future(error_on_callbacks=False)
        assert f.error_on_callbacks is False
        f.add_callback(lambda v: _raise(ValueError('boom')))
        f.success(1)  # suppressed despite class default True
        assert f.succeeded()

    def test_callback_exception_raised_when_enabled(self):
        f = Future(error_on_callbacks=True)
        f.add_callback(lambda v: _raise(ValueError('boom')))
        with pytest.raises(ValueError, match='boom'):
            f.success(1)

    def test_errback_exception_raised_when_enabled(self):
        f = Future(error_on_callbacks=True)
        f.add_errback(lambda e: _raise(RuntimeError('boom')))
        with pytest.raises(RuntimeError, match='boom'):
            f.failure(ValueError('orig'))

    def test_already_done_callback_raises_when_enabled(self):
        f = Future(error_on_callbacks=True)
        f.success(1)
        with pytest.raises(ValueError, match='boom'):
            f.add_callback(lambda v: _raise(ValueError('boom')))
