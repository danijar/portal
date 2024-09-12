import pathlib
import time

import pytest
import portal


class TestErrfile:

  def test_file(self, tmpdir):
    errfile = pathlib.Path(tmpdir) / 'error'

    def fn():
      portal.setup(errfile=errfile, interval=0.1)
      try:
        raise ValueError
      except Exception as e:
        portal.context.error(e, 'worker')

    portal.Process(fn, start=True).join()
    content = errfile.read_text()
    assert "Error in 'worker' (ValueError):" == content.split('\n')[0]
    assert 'Traceback (most recent call last)' in content
    assert 'line' in content
    assert 'in fn' in content

  @pytest.mark.parametrize('repeat', range(5))
  def test_sibling_procs(self, tmpdir, repeat):
    errfile = pathlib.Path(tmpdir) / 'error'
    barrier = portal.context.mp.Barrier(3)

    def fn1(barrier, errfile):
      portal.setup(errfile=errfile, interval=0.1)
      barrier.wait()
      raise ValueError('reason')

    def fn2(barrier, errfile):
      portal.setup(errfile=errfile, interval=0.1)
      barrier.wait()
      while True:
        time.sleep(0.1)

    worker1 = portal.Process(fn1, barrier, errfile, start=True)
    worker2 = portal.Process(fn2, barrier, errfile, start=True)
    barrier.wait()
    worker1.join()
    worker2.join()
    content = errfile.read_text()
    first_line = content.split('\n')[0]
    assert "Error in 'fn1' (ValueError: reason):" == first_line
    assert not worker1.running
    assert not worker2.running
    # The first worker may shut itself down or be shut down based on its own
    # error file watcher, based on how the threads context-switch.
    assert worker1.exitcode in (1, 2)
    assert worker2.exitcode == 2

  @pytest.mark.parametrize('repeat', range(3))
  def test_nested_procs(self, tmpdir, repeat):
    errfile = pathlib.Path(tmpdir) / 'error'
    ready = portal.context.mp.Semaphore(0)

    def hang():
      while True:
        time.sleep(0.1)

    def outer(ready, errfile):
      portal.setup(errfile=errfile, interval=0.1)
      portal.Process(inner, ready, errfile, name='inner', start=True)
      portal.Thread(hang, start=True)
      portal.Process(hang, start=True)
      ready.release()
      hang()

    def inner(ready, errfile):
      portal.setup(errfile=errfile, interval=0.1)
      portal.Thread(hang, start=True)
      portal.Process(hang, start=True)
      ready.release()
      raise ValueError('reason')

    worker = portal.Process(outer, ready, errfile, name='outer', start=True)
    ready.acquire()
    ready.acquire()
    worker.join()
    content = errfile.read_text()
    assert "Error in 'inner' (ValueError: reason):" == content.split('\n')[0]
    assert not worker.running
