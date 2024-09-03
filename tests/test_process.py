import time

import pytest
import zerofun


class TestProcess:

  def test_exitcode(self):
    worker = zerofun.Process(lambda: None, start=True)
    worker.join()
    assert worker.exitcode == 0
    worker = zerofun.Process(lambda: 42, start=True)
    worker.join()
    assert worker.exitcode == 42

  def test_error(self):
    def fn():
      raise KeyError('foo')
    worker = zerofun.Process(fn, start=True)
    worker.join()
    assert not worker.running
    assert worker.exitcode == 1

  def test_error_with_children(self):
    def hang():
      while True:
        time.sleep(0.1)
    def fn():
      zerofun.Process(hang, start=True)
      zerofun.Thread(hang, start=True)
      time.sleep(0.1)
      raise KeyError('foo')
    worker = zerofun.Process(fn, start=True)
    worker.join()
    assert not worker.running
    assert worker.exitcode == 1

  def test_kill(self):
    def fn():
      while True:
        time.sleep(0.1)
    worker = zerofun.Process(fn, start=True)
    worker.kill()
    assert not worker.running
    assert worker.exitcode == -15

  @pytest.mark.parametrize('repeat', range(5))
  def test_kill_with_subproc(self, repeat):
    ready = zerofun.context.mp.Semaphore(0)
    def outer(ready):
      zerofun.Process(inner, ready, start=True)
      ready.release()
      while True:
        time.sleep(0.1)
    def inner(ready):
      ready.release()
      while True:
        time.sleep(0.1)
    worker = zerofun.Process(outer, ready, start=True)
    ready.acquire()
    ready.acquire()
    worker.kill()
    assert not worker.running
    assert worker.exitcode == -15

  @pytest.mark.parametrize('repeat', range(5))
  def test_kill_with_subthread(self, repeat):
    ready = zerofun.context.mp.Event()
    def outer(ready):
      zerofun.Thread(inner, ready, start=True)
      while True:
        time.sleep(0.1)
    def inner(ready):
      ready.set()
      while True:
        time.sleep(0.1)
    worker = zerofun.Process(outer, ready, start=True)
    ready.wait()
    worker.kill()
    assert not worker.running
    assert worker.exitcode == -15

  def test_initfn(self):
    def init():
      zerofun.foo = 42
    zerofun.initfn(init)
    ready = zerofun.context.mp.Event()
    assert zerofun.foo == 42
    def outer(ready):
      assert zerofun.foo == 42
      zerofun.Process(inner, ready, start=True).join()
    def inner(ready):
      assert zerofun.foo == 42
      ready.set()
    zerofun.Process(outer, ready, start=True).join()
    ready.wait()
    assert ready.is_set()
    zerofun.context.initfns.clear()
