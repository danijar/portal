import threading
import time
import traceback

from . import contextlib
from . import utils


class Thread:

  """
  This thread implementation extends the standard Python thread as follows:

  1. It provide a kill() method that raises a SystemExit error inside the
  thread. This allows killing the thread from the outside.

  2. It keeps track of its subthreads and subprocesses and kills them on error
  to prevent hangs.

  3. The thread is marked as daemon, so that when the parent process dies the
  thread is killed, preventing hangs.
  """

  def __init__(self, fn, *args, name=None, start=False):
    self.fn = fn
    self.excode = None
    name = name or getattr(fn, '__name__', 'thread')
    self.thread = threading.Thread(
        target=self._wrapper, args=args, name=name, daemon=True)
    contextlib.context.add_child(self)
    self.started = False
    start and self.start()

  @property
  def name(self):
    return self.thread.name

  @property
  def ident(self):
    return self.thread.ident

  @property
  def running(self):
    if not self.started:
      return False
    return self.thread.is_alive()

  @property
  def exitcode(self):
    return self.excode

  def start(self):
    assert not self.started
    self.started = True
    self.thread.start()
    return self

  def join(self, timeout=None):
    if self.running:
      self.thread.join(timeout)
    return self

  def kill(self, timeout=3):
    utils.kill_threads(self.thread, timeout)
    return self

  def __repr__(self):
    attrs = ('name', 'ident', 'running', 'exitcode')
    attrs = [f'{k}={getattr(self, k)}' for k in attrs]
    return 'Thread(' + ', '.join(attrs) + ')'

  def _wrapper(self, *args):
    try:
      exitcode = self.fn(*args)
      exitcode = exitcode if isinstance(exitcode, int) else 0
      self.excode = exitcode
    except (SystemExit, KeyboardInterrupt) as e:
      compact = traceback.format_tb(e.__traceback__)
      compact = '\n'.join([line.split('\n', 1)[0] for line in compact])
      print(f"Killed thread '{self.name}' at:\n{compact}")
      [x.kill(0.1) for x in contextlib.context.get_children()]
      self.excode = 2
    except Exception as e:
      [x.kill(0.1) for x in contextlib.context.get_children()]
      contextlib.context.error(e, self.name)
      contextlib.context.shutdown(1)
      self.excode = 1
