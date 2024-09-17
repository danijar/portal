import multiprocessing as mp
import os
import pathlib
import sys
import threading
import traceback
import warnings

import cloudpickle
import psutil

from . import utils


class Context:

  def __init__(self):
    self.initfns = []
    self.resolver = None
    self.errfile = None
    self.interval = 20
    self.clientkw = {}
    self.serverkw = {}
    self.printlock = threading.Lock()
    self.done = threading.Event()
    self.watcher = None

  @property
  def mp(self):
    return mp.get_context('spawn')

  def options(self):
    return {
        'resolver': self.resolver and cloudpickle.dumps(self.resolver),
        'errfile': self.errfile,
        'interval': self.interval,
        'initfns': self.initfns,
        'clientkw': self.clientkw,
        'serverkw': self.serverkw,
    }

  def setup(
      self,
      resolver=None,
      errfile=None,
      interval=None,
      initfns=None,
      clientkw=None,
      serverkw=None,
      host=None,
      ipv6=None,
  ):

    if resolver:
      if isinstance(resolver, bytes):
        resolver = cloudpickle.loads(resolver)
      assert callable(resolver)
      self.resolver = resolver

    if errfile:
      if isinstance(errfile, str):
        errfile = pathlib.Path(errfile)
      assert hasattr(errfile, 'exists') and hasattr(errfile, 'write_text')
      self.errfile = errfile
      self._check_errfile()
      self._install_excepthook()

    if interval:
      assert isinstance(interval, (int, float))
      self.interval = interval

    if initfns:
      self.initfns = []
      for fn in initfns:
        self.initfn(fn, call_now=True)

    if clientkw is not None:
      assert isinstance(clientkw, dict)
      self.clientkw = clientkw

    if serverkw is not None:
      assert isinstance(serverkw, dict)
      self.serverkw = serverkw

    if host is not None:
      self.serverkw['host'] = host

    if ipv6 is not None:
      self.clientkw['ipv6'] = ipv6
      self.serverkw['ipv6'] = ipv6

    if self.errfile and not self.watcher:
      self.watcher = threading.Thread(target=self._watcher, daemon=True)
      self.watcher.start()

  def initfn(self, fn, call_now=True):
    if isinstance(fn, bytes):
      pkl, fn = fn, cloudpickle.loads(fn)
    else:
      pkl, fn = cloudpickle.dumps(fn), fn
    self.initfns.append(pkl)
    call_now and fn()

  def print(self, name, *args, color=None):
    assert args
    if color:
      style = utils.style(color=color)
      reset = utils.style(reset=True)
    else:
      style, reset = '', ''
    with self.printlock:
      print(style + f'[{name}]' + reset, *args)

  def error(self, e, name=None):
    typ, tb = type(e), e.__traceback__
    summary = list(traceback.format_exception_only(typ, e))[0].strip('\n')
    long = ''.join(traceback.format_exception(typ, e, tb)).strip('\n')
    message = f"Error in '{name}' ({summary}):\n{long}"
    with self.printlock:
      style = utils.style(color='red')
      reset = utils.style(reset=True)
      print(style + '\n---\n' + message + reset, file=sys.stderr)
    if self.errfile:
      self.errfile.write_text(message)
      print(f'Wrote errorfile: {self.errfile}', file=sys.stderr)

  def shutdown(self, exitcode):
    children = list(psutil.Process(os.getpid()).children(recursive=True))
    utils.kill_proc(children, timeout=1)
    # TODO
    # if exitcode == 0:
    #   for child in self.children(threading.main_thread()):
    #     child.kill()
    #   os._exit(0)
    # else:
    os._exit(exitcode)

  def close(self):
    self.done.set()
    if self.watcher:
      self.watcher.join()

  def add_worker(self, worker):
    assert hasattr(worker, 'kill')
    current = threading.current_thread()
    if current == threading.main_thread():
      return
    if hasattr(worker, 'thread'):
      assert current != worker.thread
    try:
      current.children.append(worker)
    except AttributeError:
      warnings.warn(
          'Using Portal from plain Python threads is discouraged because ' +
          'they can cause hangs during shutdown.')

  def children(self, thread):
    current = thread or threading.current_thread()
    if current == threading.main_thread():
      return []
    return current.children

  def _watcher(self):
    while not self.done.wait(self.interval):
      self._check_errfile()

  def _install_excepthook(self):
    existing = sys.excepthook
    def patched(typ, val, tb):
      if self.errfile:
        message = ''.join(traceback.format_exception(typ, val, tb)).strip('\n')
        self.errfile.write_text(message)
        print(f'Wrote errorfile: {self.errfile}', file=sys.stderr)
      return existing(typ, val, tb)
    sys.excepthook = patched

  def _check_errfile(self):
    if self.errfile and self.errfile.exists():
      message = f'Shutting down due to error file: {self.errfile}'
      print(message, file=sys.stderr)
      self.shutdown(exitcode=2)


context = Context()


def initfn(fn, call_now=True):
  context.initfn(fn, call_now)


def setup(**kwargs):
  context.setup(**kwargs)


def reset():
  global context
  context.close()
  context = Context()
