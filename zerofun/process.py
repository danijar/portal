import time

import cloudpickle
import psutil

from . import contextlib
from . import utils


class Process:

  def __init__(self, fn, *args, name=None, start=False, context=None):
    fn = cloudpickle.dumps(fn)
    name = name or getattr(fn, '__name__', 'process')
    context = context or contextlib.context()
    self.process = context.mp.Process(
        target=self._wrapper, name=name, args=(context, name, fn, args))
    self.psutil = None
    contextlib.child(self)
    self.started = False
    start and self.start()

  @property
  def name(self):
    return self.process.name

  @property
  def pid(self):
    return self.process.pid

  @property
  def running(self):
    try:
      alive = self.psutil.status() != psutil.STATUS_ZOMBIE
      return self.psutil.is_running() and alive
    except psutil.NoSuchProcess:
      return False

  @property
  def exitcode(self):
    if not self.started or self.running:
      return None
    elif self.process.exitcode is None:
      return 2
    else:
      return self.process.exitcode

  def start(self):
    assert not self.started
    self.started = True
    self.process.start()
    assert self.pid is not None
    self.psutil = psutil.Process(self.pid)
    return self

  def join(self, timeout=None):
    if self.running:
      self.process.join(timeout)
    return self

  def kill(self, timeout=3):
    start = time.time()
    self.process.terminate()
    utils.kill_proc(self.pid, timeout)
    self.process.join(max(0, timeout - (time.time() - start)))
    return self

  def __repr__(self):
    attrs = ('name', 'pid', 'running', 'exitcode')
    attrs = [f'{k}={getattr(self, k)}' for k in attrs]
    return 'Process(' + ', '.join(attrs) + ')'

  @staticmethod
  def _wrapper(context, name, fn, args):
    exitcode = 0
    try:
      context.start()
      contextlib.CONTEXT = context
      fn = cloudpickle.loads(fn)
      exitcode = fn(*args)
      exitcode = exitcode if isinstance(exitcode, int) else 0
    except (SystemExit, KeyboardInterrupt):
      exitcode = 2
    except Exception as e:
      contextlib.context().error(e, name)
      exitcode = 1
    finally:
      contextlib.context().shutdown(exitcode)
