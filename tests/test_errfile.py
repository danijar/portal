import os
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
    ready = portal.context.mp.Barrier(7)
    queue = portal.context.mp.Queue()

    def outer(ready, queue, errfile):
      portal.setup(errfile=errfile, interval=0.1)
      portal.Process(inner, ready, queue, name='inner', start=True)
      portal.Thread(hang_thread, ready, start=True)
      portal.Process(hang_process, ready, queue, start=True)
      queue.put(os.getpid())
      queue.close()
      queue.join_thread()
      ready.wait()  # 1
      while True:
        time.sleep(0.1)

    def inner(ready, queue):
      assert portal.context.errfile
      portal.Thread(hang_thread, ready, start=True)
      portal.Process(hang_process, ready, queue, start=True)
      queue.put(os.getpid())
      queue.close()
      queue.join_thread()
      ready.wait()  # 2
      raise ValueError('reason')

    def hang_thread(ready):
      ready.wait()  # 3, 4
      while True:
        time.sleep(0.1)

    def hang_process(ready, queue):
      assert portal.context.errfile
      queue.put(os.getpid())
      queue.close()
      queue.join_thread()
      ready.wait()  # 5, 6
      while True:
        time.sleep(0.1)

    worker = portal.Process(
        outer, ready, queue, errfile, name='outer', start=True)
    ready.wait()  # 7
    worker.join()
    content = errfile.read_text()
    assert "Error in 'inner' (ValueError: reason):" == content.split('\n')[0]
    assert not worker.running
    pids = [queue.get() for _ in range(4)]
    time.sleep(2.0)  # On some systems this can take a while.
    assert not portal.proc_alive(pids[0])
    assert not portal.proc_alive(pids[1])
    assert not portal.proc_alive(pids[2])
    assert not portal.proc_alive(pids[3])
