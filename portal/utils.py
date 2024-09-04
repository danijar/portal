import ctypes
import socket
import sys
import threading
import time

import numpy as np
import psutil


def run(workers, duration=None):
  [None if x.started else x.start() for x in workers]
  start = time.time()
  while True:
    time.sleep(0.1)
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
      name = errored[0].name
      code = errored[0].exitcode
      print(f"Shutting down workers due to crash in '{name}' ({code}).")
      [x.kill() for x in workers]
      raise RuntimeError(f"'{name}' crashed with exit code {code}")


def kill_threads(threads, timeout=3):
  threads = threads if isinstance(threads, (list, tuple)) else [threads]
  threads = [x for x in threads if x is not threading.main_thread()]
  for thread in threads:
    if thread.native_id is None:
      # Wait because thread may currently be starting.
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


def kill_procs(procs, timeout=3):
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
  _, procs = psutil.wait_procs(procs, timeout)
  # Send SIGTERM to remaining processes to force exit.
  eachproc(lambda p: p.kill(), procs)
  # Should never happen but print warning if any survived.
  eachproc(lambda p: (
      print('Killed subprocess is still alive.')
      if p.status() != psutil.STATUS_ZOMBIE else None), procs)


def free_port():
  # Return a port that is currently free. This function is not thread or
  # process safe, because there is no way to guarantee that the port will still
  # be free at the time it will be used.
  rng = np.random.default_rng()
  while True:
    port = int(rng.integers(2000, 7000))
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
      if s.connect_ex(('', port)):
        return port


def style(color=None, background=None, bold=None, underline=None, reset=None):
  if not sys.stdout.isatty():
    return ''
  escseq = lambda parts: '\033[' + ';'.join(parts) + 'm'
  colors = dict(
      black=0, red=1, green=2, yellow=3, blue=4, magenta=5, cyan=6, white=7)
  parts = []
  if reset:
    parts.append(escseq('0'))
  if color or bold or underline:
    args = ['3' + (str(colors[color]) if color else '9')]
    bold and args.append('1')
    underline and args.append('4')
    parts.append(escseq(args))
  if background:
    parts.append(escseq('4' + str(colors[background])))
  return ''.join(parts)
