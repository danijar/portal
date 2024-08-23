import collections
import ctypes
import multiprocessing as mp
import os
import pathlib
import socket
import threading
import time
import traceback

import cloudpickle
import elements
import msgpack
import numpy as np
import psutil


CONTEXT = None

CHILDREN = collections.defaultdict(list)

def setup(errfile=None, check_interval=20):
  global CONTEXT
  CONTEXT = Context(errfile, check_interval)

def context():
  global CONTEXT
  if not CONTEXT:
    CONTEXT = Context(None)
  return CONTEXT

def error(e, name=None):
  context().error(e, name)

def shutdown(exitcode):
  context().shutdown(exitcode)

def initfn(fn):
  context().initfn(fn)

def child(worker):
  global CHILDREN
  if hasattr(worker, 'thread'):
    assert worker.thread.ident != threading.get_ident()
  CHILDREN[threading.get_ident()].append(worker)

def children(ident):
  global CHILDREN
  return CHILDREN[ident]


class Context:

  def __init__(self, errfile=None, check_interval=20):
    if errfile and isinstance(errfile, str):
      errfile = pathlib.Path(errfile)
    self.mp = mp.get_context()
    self.errfile = errfile
    self.check_interval = check_interval
    self.printlock = self.mp.Lock()
    self.initfns = []
    self.watcher = None
    self.started = False
    self.start()

  def __getstate__(self):
    return (self.errfile, self.check_interval, self.printlock, self.initfns)

  def __setstate__(self, x):
    self.errfile, self.check_interval, self.printlock, self.initfns = x
    self.mp = mp.get_context()
    self.started = False

  def initfn(self, initfn):
    self.initfns.append(cloudpickle.dumps(initfn))
    initfn()

  def start(self):
    if self.started:
      return
    self.started = True
    initfns = [cloudpickle.loads(x) for x in self.initfns]
    [x() for x in initfns]
    if self.errfile:
      self.watcher = threading.Thread(target=self._watcher, daemon=True)
      self.watcher.start()

  def error(self, e, name=None):
    typ, tb = type(e), e.__traceback__
    summary = list(traceback.format_exception_only(typ, e))[0].strip('\n')
    long = ''.join(traceback.format_exception(typ, e, tb)).strip('\n')
    message = f"Error in '{name}' ({summary}):\n{long}"
    with self.printlock:
      elements.print(message, color='red')
    if self.errfile:
      with self.errfile.open('wb') as f:
        f.write(message.encode('utf-8'))

  def shutdown(self, exitcode):
    kill_proc(psutil.Process().children(recursive=True))
    os._exit(exitcode)

  def close(self):
    if self.watcher:
      kill_thread(self.watcher)

  def _watcher(self):
    while True:
      time.sleep(self.check_interval)
      if self.errfile and self.errfile.exists():
        print('Detected error file thus shutting down:')
        print(self.errfile.read_text())
        self.shutdown(2)


def run(workers, duration=None):
  [None if x.started else x.start() for x in workers]
  start = time.time()
  while True:
    time.sleep(0.5)
    if duration and time.time() - start >= duration:
      print(f'Shutting down workers after {duration} seconds.')
      [x.kill() for x in workers]
      return
    if all(x.exitcode == 0 for x in workers):
      print('All workers terminated successfully.')
      return
    errored = [x for x in workers if x.exitcode not in (None, 0)]
    if errored:
      time.sleep(0.1)  # Wait for workers to print their error messages.
      print(f'Shutting down workers due to crash in {errored[0].name}.')
      [x.kill() for x in workers]
      raise RuntimeError


def kill_thread(threads, timeout=3):
  threads = threads if isinstance(threads, (list, tuple)) else [threads]
  threads = [x for x in threads if x is not threading.main_thread()]
  for thread in threads:
    if thread.native_id is None:
      # Wait because threa may currently be starting.
      time.sleep(0.2)
    matches = [k for k, v in threading._active.items() if v is thread]
    if not matches:
      continue
    ident = matches[0]
    result = ctypes.pythonapi.PyThreadState_SetAsyncExc(
        ctypes.c_long(ident), ctypes.py_object(SystemExit))
    if result > 1:
      ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(ident), None)
  start = time.time()
  [x.join(max(0.1, timeout - (time.time() - start))) for x in threads]
  for thread in threads:
    if thread.is_alive():
      print('Killed thread is still alive.')


def kill_proc(procs, timeout=3):
  def eachproc(fn, procs):
    result = []
    for proc in list(procs):
      try:
        result.append(fn(proc))
      except psutil.NoSuchProcess:
        pass
    return result
  # Collect all sub processes.
  procs = procs if isinstance(procs, (list, tuple)) else [procs]
  procs = eachproc(
      lambda p: psutil.Process(p) if isinstance(p, int) else p, procs)
  eachproc(lambda p: procs.extend(p.children(recursive=True)), procs)
  procs = list(set(procs))
  # Send SIGINT to attempt graceful shutdown.
  eachproc(lambda p: p.terminate(), procs)
  time.sleep(1)
  _, procs = psutil.wait_procs(procs, timeout)
  # Send SIGTERM to remaining processes to force exit.
  eachproc(lambda p: p.kill(), procs)
  # Should never happen but print warning if any survived.
  eachproc(lambda p: (
      print('Killed subprocess is still alive.')
      if p.status() != psutil.STATUS_ZOMBIE else None), procs)


def free_port():
  rng = np.random.default_rng()
  while True:
    port = int(rng.integers(2000, 7000))
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
      if s.connect_ex(('', port)):
        return port


def pack(data):
  leaves, structure = elements.tree.flatten(data)
  dtypes, shapes, buffers = [], [], []
  for value in leaves:
    value = np.asarray(value)
    if value.dtype == object:
      raise TypeError(data)
    assert value.data.c_contiguous, (
        "Array is not contiguous in memory. Use np.asarray(arr, order='C') " +
        "before passing the data into pack().")
    dtypes.append(value.dtype.str)
    shapes.append(value.shape)
    buffers.append(value.data)
  meta = (structure, dtypes, shapes)
  payload = [msgpack.packb(meta), *buffers]
  return payload


def unpack(payload):
  meta, *buffers = payload
  structure, dtypes, shapes = msgpack.unpackb(meta)
  leaves = [
      np.frombuffer(b, d).reshape(s)
      for i, (d, s, b) in enumerate(zip(dtypes, shapes, buffers))]
  data = elements.tree.unflatten(leaves, structure)
  return data
