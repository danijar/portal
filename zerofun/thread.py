import threading
import time

from . import utils


class Thread:

  def __init__(self, fn, *args, name=None, start=False):
    self.fn = fn
    self.excode = None
    name = name or getattr(fn, '__name__', 'thread')
    self.thread = threading.Thread(
        target=self._wrapper, args=args, name=name, daemon=True)
    utils.child(self)
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
    return self.thread.is_alive()

  @property
  def exitcode(self):
    return self.excode

  def start(self):
    assert not self.started
    self.started = True
    self.thread.start()

  def join(self, timeout=None):
    if self.running:
      self.thread.join(timeout)

  def kill(self, timeout=3):
    start = time.time()
    children = utils.children(self.ident)
    [x.kill(max(0.1, timeout - (time.time() - start))) for x in children]
    utils.kill_thread(self.thread, max(0.1, timeout - (time.time() - start)))

  def __repr__(self):
    attrs = ('name', 'ident', 'running', 'exitcode')
    attrs = [f'{k}={getattr(self, k)}' for k in attrs]
    return 'Thread(' + ', '.join(attrs) + ')'

  def _wrapper(self, *args):
    try:
      exitcode = self.fn(*args)
      exitcode = exitcode if isinstance(exitcode, int) else 0
      self.excode = exitcode
    except (SystemExit, KeyboardInterrupt):
      self.excode = 2
    except Exception as e:
      [x.kill(0.1) for x in utils.children(self.ident)]
      utils.error(e, self.name)
      self.excode = 1
