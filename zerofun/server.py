import concurrent.futures
import queue

from . import packlib
from . import poollib
from . import server_socket
from . import thread


class Server:

  def __init__(self, port, name='Server', workers=1, errors=True, **kwargs):
    self.socket = server_socket.ServerSocket(port, name, **kwargs)
    self.loop = thread.Thread(self._loop)
    self.methods = {}
    self.jobs = set()
    self.pool = poollib.ThreadPool(workers, 'pool_default')
    self.pools = [self.pool]
    self.errors = errors
    self.running = False

  def bind(self, name, fn, workers=0):
    assert not self.running
    if workers:
      pool = poollib.ThreadPool(workers, f'pool_{name}')
      self.pools.append(pool)
    else:
      pool = self.pool
    self.methods[name] = (fn, pool)

  def start(self, block=True):
    assert not self.running
    self.running = True
    self.loop.start()
    if block:
      self.loop.join(timeout=None)

  def close(self, timeout=None):
    assert self.running
    self.running = False
    self.loop.join(timeout)
    self.loop.kill()
    [x.close() for x in self.pools]

  def stats(self):
    return {}  # TODO

  def __enter__(self):
    return self

  def __exit__(self, *e):
    self.close()

  def _loop(self):
    while self.running or self.jobs:
      while True:  # Loop syntax used to break on error.
        if not self.running:  # Do not accept further requests.
          break
        try:
          addr, data = self.socket.recv(timeout=0.0001)
        except queue.Empty:
          break
        if len(data) < 8:
          self._error(addr, bytes(8), 1, 'Message too short')
          break
        reqnum, data = bytes(data[:8]), data[8:]
        try:
          strlen = int.from_bytes(data[:8], 'little', signed=False)
          name = bytes(data[8: 8 + strlen]).decode('utf-8')
          data = packlib.unpack(data[8 + strlen:])
        except Exception:
          self._error(addr, reqnum, 2, 'Could not decode message')
          break
        if name not in self.methods:
          self._error(addr, reqnum, 3, f'Unknown method {name}')
          break
        fn, pool = self.methods[name]
        job = pool.submit(fn, *data)
        job.addr = addr
        job.reqnum = reqnum
        self.jobs.add(job)
        break  # We do not actually want to loop.
      completed, self.jobs = concurrent.futures.wait(
          self.jobs, 0.0001, concurrent.futures.FIRST_COMPLETED)
      for job in completed:
        try:
          data = job.result()
          data = packlib.pack(data)
          status = int(0).to_bytes(8, 'little', signed=False)
          self.socket.send(job.addr, job.reqnum, status, *data)
        except Exception as e:
          self._error(job.addr, job.reqnum, 4, f'Error in server method: {e}')

  def _error(self, addr, reqnum, status, message):
    status = status.to_bytes(8, 'little', signed=False)
    data = message.encode('utf-8')
    self.socket.send(addr, reqnum, status, data)
    if self.errors:
      raise RuntimeError(message)
