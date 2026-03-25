import time

import portal
import pytest


class TestFutures:
    def test_result(self):
        future = portal.Future()
        assert not future.wait(timeout=0)
        assert not future.done()
        future.set_result(42)
        assert future.wait(timeout=0)
        assert future.done()
        assert future.result() == 42

    def test_error(self):
        future = portal.Future()
        future.set_error(ValueError('foo'))
        assert future.wait(timeout=0)
        assert future.done()
        with pytest.raises(ValueError) as e:
            future.result()
        assert e.value.args[0] == 'foo'

    def test_callback(self):
        called = [False]

        def fn(future):
            called[0] = True

        future = portal.Future()
        future.add_callback(fn)
        assert not future.wait(timeout=0)
        assert not future.done()
        assert called[0] is False
        future.set_result(42)
        assert future.wait(timeout=0)
        assert future.done()
        assert future.result() == 42
        assert called[0] is True

    def test_callback_chain(self):
        inner = portal.Future()
        outer = portal.Future()

        def callback(future):
            outer.set_result(future.result() + 1)

        inner.add_callback(callback)

        assert not inner.done()
        assert not outer.done()
        inner.set_result(42)
        assert inner.done()
        assert inner.result() == 42
        assert outer.done()
        assert outer.result() == 43

    def test_wait_completed(self):
        futures = [portal.Future() for _ in range(5)]

        def fn():
            time.sleep(0.05)
            futures[0].set_result(12)
            time.sleep(0.05)
            futures[1].set_result(42)

        worker = portal.Thread(fn)
        worker.start()
        completed = portal.futures.wait(futures, timeout=None, amount=2)
        assert len(completed) == 2
        assert all(x.done() for x in completed)
        assert completed[0].result() == 12
        assert completed[1].result() == 42
        worker.join()

    def test_wait_not_completed(self):
        futures = [portal.Future() for _ in range(5)]

        def fn():
            time.sleep(0.05)
            futures[0].set_result(12)
            time.sleep(0.05)
            futures[1].set_result(42)

        worker = portal.Thread(fn)
        worker.start()
        completed = portal.futures.wait(futures, timeout=0.2, amount=3)
        assert completed is False
        worker.join()
