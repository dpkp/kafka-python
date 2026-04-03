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
