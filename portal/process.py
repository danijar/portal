import atexit
import errno
import traceback

import cloudpickle
import psutil

from . import contextlib
from . import utils


class Process:

  """
  This process implementation extends the standard Python process as follows:

  1. It provides a kill() method that not only terminates the process itself
  but also all its nested child processes. This prevents subprocesses from
  lingering in the background after the Python program has ended.

  2. The process terminates its nested child processes when it encounters an
  error. This prevents lingering subprocesses on error.

  3. When the parent process encounters an error, its subprocess will be killed
  via `atexit`, preventing hangs.

  4. It inherits the context object of its parent process, which provides
  cloudpickled initializer functions for each nested child process and error
  file watching for global shutdown on error.

  5. Standard Python subprocesses do not reliably return the running() state of
  the process. This class makes it more reliable.
  """

  def __init__(self, fn, *args, name=None, start=False):
    name = name or getattr(fn, '__name__', 'process')
    fn = cloudpickle.dumps(fn)
    options = contextlib.context.options()
    self.ready = contextlib.context.mp.Barrier(2)
    self.process = contextlib.context.mp.Process(
        target=self._wrapper, name=name,
        args=(options, self.ready, name, fn, args))
    self.started = False
    self.killed = False
    self.thepid = None
    atexit.register(self.kill)
    contextlib.context.add_worker(self)
    start and self.start()

  @property
  def name(self):
    return self.process.name

  @property
  def pid(self):
    return self.thepid

  @property
  def running(self):
    if not self.started:
      return False
    if not self.process.is_alive():
      return False
    return utils.proc_alive(self.pid)

  @property
  def exitcode(self):
    exitcode = self.process.exitcode
    if self.killed and exitcode is None:
      return -9
    return exitcode

  def start(self):
    assert not self.started
    self.started = True
    self.process.start()
    self.ready.wait()
    self.thepid = self.process.pid
    assert self.thepid is not None
    return self

  def join(self, timeout=None):
    assert self.started
    self.process.join(timeout)
    return self

  def kill(self, timeout=1):
    assert self.started
    try:
      children = list(psutil.Process(self.pid).children(recursive=True))
    except psutil.NoSuchProcess:
      children = []
    try:
      self.process.terminate()
      self.process.join(timeout)
      self.process.kill()
      utils.kill_proc(children, timeout)
    except OSError as e:
      if e.errno != errno.ESRCH:
        contextlib.context.error(e, self.name)
        contextlib.context.shutdown(exitcode=1)
    self.killed = True
    return self

  def __repr__(self):
    attrs = ('name', 'pid', 'running', 'exitcode')
    attrs = [f'{k}={getattr(self, k)}' for k in attrs]
    return 'Process(' + ', '.join(attrs) + ')'

  @staticmethod
  def _wrapper(options, ready, name, fn, args):
    exitcode = 0
    try:
      ready.wait()
      contextlib.setup(**options)
      fn = cloudpickle.loads(fn)
      exitcode = fn(*args)
      exitcode = exitcode if isinstance(exitcode, int) else 0
    except (SystemExit, KeyboardInterrupt) as e:
      compact = traceback.format_tb(e.__traceback__)
      compact = '\n'.join([line.split('\n', 1)[0] for line in compact])
      print(f"Killed process '{name}' at:\n{compact}")
      exitcode = 2
    except Exception as e:
      contextlib.context.error(e, name)
      exitcode = 1
    finally:
      contextlib.context.shutdown(exitcode)
