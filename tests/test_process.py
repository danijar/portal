import os
import time

import pytest
import portal


class TestProcess:

  def test_exitcode(self):
    worker = portal.Process(lambda: None, start=True)
    worker.join()
    assert worker.exitcode == 0
    worker = portal.Process(lambda: 42, start=True)
    worker.join()
    assert worker.exitcode == 42

  def test_error(self):

    def fn():
      raise KeyError('foo')

    worker = portal.Process(fn, start=True)
    worker.join()
    assert not worker.running
    assert worker.exitcode == 1

  def test_error_with_children(self):

    def hang():
      while True:
        time.sleep(0.1)

    def fn():
      portal.Process(hang, start=True)
      portal.Thread(hang, start=True)
      time.sleep(0.1)
      raise KeyError('foo')

    worker = portal.Process(fn, start=True)
    worker.join()
    assert not worker.running
    assert worker.exitcode == 1

  @pytest.mark.parametrize('repeat', range(5))
  def test_kill_basic(self, repeat):

    def fn():
      while True:
        time.sleep(0.1)

    worker = portal.Process(fn, start=True)
    worker.kill()
    assert not worker.running
    assert abs(worker.exitcode) >= 1

  @pytest.mark.parametrize('repeat', range(5))
  def test_kill_with_subproc(self, repeat):
    ready = portal.context.mp.Barrier(3)
    queue = portal.context.mp.Queue()

    def outer(ready, queue):
      portal.Process(inner, ready, queue, start=True)
      queue.put(os.getpid())
      queue.close()
      queue.join_thread()
      ready.wait()
      while True:
        time.sleep(0.1)

    def inner(ready, queue):
      queue.put(os.getpid())
      queue.close()
      queue.join_thread()
      ready.wait()
      while True:
        time.sleep(0.1)

    worker = portal.Process(outer, ready, queue, start=True)
    ready.wait()
    worker.kill()
    assert not worker.running
    assert abs(worker.exitcode) >= 1
    assert not portal.proc_alive(queue.get())
    assert not portal.proc_alive(queue.get())

  @pytest.mark.parametrize('repeat', range(5))
  def test_kill_with_subthread(self, repeat):
    ready = portal.context.mp.Event()
    def outer(ready):
      portal.Thread(inner, ready, start=True)
      while True:
        time.sleep(0.1)
    def inner(ready):
      ready.set()
      while True:
        time.sleep(0.1)
    worker = portal.Process(outer, ready, start=True)
    ready.wait()
    worker.kill()
    assert not worker.running
    assert abs(worker.exitcode) >= 1

  def test_initfn(self):

    def init():
      portal.foo = 42

    portal.initfn(init)
    ready = portal.context.mp.Event()
    assert portal.foo == 42

    def outer(ready):
      assert portal.foo == 42
      portal.Process(inner, ready, start=True).join()

    def inner(ready):
      assert portal.foo == 42
      ready.set()

    portal.Process(outer, ready, start=True).join()
    ready.wait()
    assert ready.is_set()
    portal.context.initfns.clear()
