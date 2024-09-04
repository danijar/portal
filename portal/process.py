import atexit
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

  3. When the parent process encounters an error, the subprocess will be killed
  via `atexit`, preventing hangs.

  4. It inherits the context object of its parent process, which provides
  cloudpickled initializer functions for each nested child process and error
  file watching for global shutdown on error.
  """

  def __init__(self, fn, *args, name=None, start=False):
    name = name or getattr(fn, '__name__', 'process')
    fn = cloudpickle.dumps(fn)
    context = contextlib.context
    options = context.options()
    self.process = context.mp.Process(
        target=self._wrapper, name=name, args=(options, name, fn, args))
    context.add_child(self)
    self.started = False
    atexit.register(self.kill)
    start and self.start()

  @property
  def name(self):
    return self.process.name

  @property
  def pid(self):
    return self.process.pid

  @property
  def running(self):
    if not self.started:
      return False
    return self.process.is_alive()

  @property
  def exitcode(self):
    if not self.started or self.running:
      return None
    else:
      return self.process.exitcode

  def start(self):
    assert not self.started
    self.started = True
    self.process.start()
    assert self.pid is not None
    return self

  def join(self, timeout=None):
    assert self.started
    if self.running:
      self.process.join(timeout)
    return self

  def kill(self, timeout=1):
    try:
      proc = psutil.Process(self.pid)
      tree = [proc] + list(proc.children(recursive=True))
    except psutil.NoSuchProcess:
      tree = []
    self.process.terminate()
    self.process.join(timeout / 2)
    self.process.kill()
    utils.kill_procs(tree, timeout / 2)
    return self

  def __repr__(self):
    attrs = ('name', 'pid', 'running', 'exitcode')
    attrs = [f'{k}={getattr(self, k)}' for k in attrs]
    return 'Process(' + ', '.join(attrs) + ')'

  @staticmethod
  def _wrapper(options, name, fn, args):
    exitcode = 0
    try:
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
