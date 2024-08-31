import time

import pytest
import zerofun


class TestThread:

  def test_exitcode(self):
    worker = zerofun.Thread(lambda: None, start=True)
    worker.join()
    assert worker.exitcode == 0
    worker = zerofun.Thread(lambda: 42, start=True)
    worker.join()
    assert worker.exitcode == 42

  def test_kill(self):
    def fn():
      while True:
        time.sleep(0.1)
    worker = zerofun.Thread(fn, start=True)
    worker.kill()
    assert not worker.running
    assert worker.exitcode == 2

  def test_error(self):
    def container():
      def fn():
        raise KeyError('foo')
      zerofun.Thread(fn, start=True).join()
    worker = zerofun.Process(container, start=True)
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
        children.append(zerofun.Process(hang, start=True))
        children.append(zerofun.Thread(hang, start=True))
        time.sleep(0.1)
        raise KeyError('foo')
      zerofun.Thread(fn, start=True).join()
    worker = zerofun.Process(container, start=True)
    worker.join()
    assert not worker.running
    assert worker.exitcode == 1

  @pytest.mark.parametrize('repeat', range(5))
  def test_kill_with_subthread(self, repeat):
    flag = [False]
    def outer():
      zerofun.Thread(inner, start=True)
      while True:
        time.sleep(0.1)
    def inner():
      try:
        while True:
          time.sleep(0.1)
      except SystemExit:
        flag[0] = True
        raise
    worker = zerofun.Thread(outer, start=True)
    worker.kill()
    assert not worker.running
    assert worker.exitcode == 2
    assert flag[0] is True

  @pytest.mark.parametrize('repeat', range(5))
  def test_kill_with_subproc(self, repeat):
    ready = zerofun.context().mp.Event()
    proc = [None]
    def outer():
      proc[0] = zerofun.Process(inner, ready, start=True)
      while True:
        time.sleep(0.1)
    def inner(ready):
      ready.set()
      while True:
        time.sleep(0.1)
    worker = zerofun.Thread(outer, start=True)
    ready.wait()
    worker.kill()
    assert not worker.running
    assert not proc[0].running
    assert worker.exitcode == 2
    assert proc[0].exitcode == -15
