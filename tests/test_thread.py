import threading
import time

import pytest
import portal


class TestThread:

  def test_exitcode(self):
    worker = portal.Thread(lambda: None, start=True)
    worker.join()
    assert worker.exitcode == 0
    worker = portal.Thread(lambda: 42, start=True)
    worker.join()
    assert worker.exitcode == 42

  def test_kill(self):
    def fn():
      while True:
        time.sleep(0.1)
    worker = portal.Thread(fn, start=True)
    worker.kill()
    assert not worker.running
    assert worker.exitcode == 2

  def test_error(self):
    def container():
      def fn():
        raise KeyError('foo')
      portal.Thread(fn, start=True).join()
    worker = portal.Process(container, start=True)
    worker.join()
    assert not worker.running
    assert worker.exitcode == 1

  def test_error_with_children(self):
    def container():
      def hang():
        while True:
          time.sleep(0.1)
      children = []
      def fn():
        children.append(portal.Process(hang, start=True))
        children.append(portal.Thread(hang, start=True))
        time.sleep(0.1)
        raise KeyError('foo')
      portal.Thread(fn, start=True).join()
    worker = portal.Process(container, start=True)
    worker.join()
    assert not worker.running
    assert worker.exitcode == 1

  @pytest.mark.parametrize('repeat', range(5))
  def test_kill_with_subthread(self, repeat):
    barrier = threading.Barrier(2)
    child = [None]
    def outer():
      child[0] = portal.Thread(inner)
      child[0].start()
      while True:
        time.sleep(0.1)
    def inner():
      barrier.wait()
      while True:
        time.sleep(0.1)
    worker = portal.Thread(outer, start=True)
    barrier.wait()
    worker.kill()
    time.sleep(0.1)
    assert not worker.running
    assert not child[0].running
    assert worker.exitcode == 2
    assert child[0].exitcode == 2

  @pytest.mark.parametrize('repeat', range(5))
  def test_kill_with_subproc(self, repeat):
    ready = portal.context.mp.Event()
    proc = [None]
    def outer():
      proc[0] = portal.Process(inner, ready, start=True)
      while True:
        time.sleep(0.1)
    def inner(ready):
      ready.set()
      while True:
        time.sleep(0.1)
    worker = portal.Thread(outer, start=True)
    ready.wait()
    worker.kill()
    assert not worker.running
    assert not proc[0].running
    assert worker.exitcode == 2
    assert proc[0].exitcode == -15
