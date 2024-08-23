import pathlib
import time

import pytest
import zerofun


class TestErrfile:

  def test_file(self, tmpdir):
    errfile = pathlib.Path(tmpdir) / 'error'
    zerofun.setup(errfile)
    try:
      raise ValueError
    except Exception as e:
      zerofun.error(e, 'worker')
    content = errfile.read_text()
    assert "Error in 'worker' (ValueError):" == content.split('\n')[0]
    assert 'Traceback (most recent call last)' in content
    assert 'line' in content
    assert 'in test_file' in content
    zerofun.context().close()

  @pytest.mark.parametrize('repeat', range(3))
  def test_sibling_procs(self, tmpdir, repeat):
    errfile = pathlib.Path(tmpdir) / 'error'
    ready = zerofun.context().mp.Semaphore(0)
    def fn1(ready, errfile):
      zerofun.setup(errfile, check_interval=0.1)
      ready.release()
      raise ValueError('payload')
    def fn2(ready, errfile):
      zerofun.setup(errfile, check_interval=0.1)
      ready.release()
      while True:
        time.sleep(0.1)
    worker1 = zerofun.Process(fn1, ready, errfile, name='worker1', start=True)
    worker2 = zerofun.Process(fn2, ready, errfile, name='worker2', start=True)
    ready.acquire()
    ready.acquire()
    worker1.join(3)
    worker2.join(3)
    content = errfile.read_text()
    assert "Error in 'worker1' (ValueError: payload):" == content.split('\n')[0]
    assert not worker1.running
    assert not worker2.running
    assert worker1.exitcode == 1
    assert worker2.exitcode == 2
    zerofun.context().close()

  @pytest.mark.parametrize('repeat', range(3))
  def test_nested_procs(self, tmpdir, repeat):
    errfile = pathlib.Path(tmpdir) / 'error'
    ready = zerofun.context().mp.Semaphore(0)
    def hang():
      while True:
        time.sleep(0.1)
    def outer(ready, errfile):
      zerofun.setup(errfile, check_interval=0.1)
      zerofun.Process(inner, ready, errfile, name='inner', start=True)
      zerofun.Thread(hang, start=True)
      zerofun.Process(hang, start=True)
      ready.release()
      hang()
    def inner(ready, errfile):
      zerofun.setup(errfile, check_interval=0.1)
      zerofun.Thread(hang, start=True)
      zerofun.Process(hang, start=True)
      ready.release()
      raise ValueError('payload')
    worker = zerofun.Process(outer, ready, errfile, name='outer', start=True)
    ready.acquire()
    ready.acquire()
    worker.join(3)
    content = errfile.read_text()
    assert "Error in 'inner' (ValueError: payload):" == content.split('\n')[0]
    assert not worker.running
    zerofun.context().close()
