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

    def test_waitfn(self):
        result = [None]

        def waitfn(future, timeout):
            assert timeout == 0
            if result[0]:
                if not future.done():
                    future.set_result(result[0])

        future = portal.Future(waitfn)
        assert not future.wait(timeout=0)
        assert not future.done()
        result[0] = 42
        assert future.wait(timeout=0)
        assert future.done()
        assert future.result() == 42

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
