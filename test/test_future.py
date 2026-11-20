from kafka.future import Future


class TestFutureCallbacks:
    def test_callbacks(self):
        f = Future()
        results = []
        f.add_callback(lambda v: results.append(('cb', v)))
        f.add_errback(lambda e: results.append(('eb', e)))
        f.success(42)
        assert results == [('cb', 42)]

    def test_errbacks(self):
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


    def test_remove_cb_eb(self):
        f = Future()
        results = []
        keep_cb = lambda v: results.append(('keep', v))
        drop_cb = lambda v: results.append(('drop_cb', v))
        drop_eb = lambda e: results.append(('drop_eb', e))
        f.add_callback(keep_cb)
        f._add_cb_eb(drop_cb, drop_eb)
        f._remove_cb_eb(drop_cb, drop_eb)
        # Missing callables are ignored without error.
        f._remove_cb_eb(drop_cb, drop_eb)
        assert f._callbacks == [keep_cb]
        assert f._errbacks == []
        f.success('ok')
        assert results == [('keep', 'ok')]
        # Calling after completion is a safe no-op.
        f._remove_cb_eb(keep_cb, drop_eb)


class TestFutureNotAwaitable:
    """The plain Future is a thread-safe handoff primitive, not loop-awaitable.
    __await__ lives on the backend future (e.g. SelectorFuture from
    create_future()), not the base, so awaiting a handoff Future fails loudly.
    The backend futures' await behavior is covered by test_selector.py and the
    NetBackendFuture conformance suite."""

    def test_no_dunder_await(self):
        assert not hasattr(Future(), '__await__')
