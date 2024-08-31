import collections
import concurrent.futures
import queue
import time

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
    self.postfn_pool = poollib.ThreadPool(1, 'pool_postfn')
    self.postfn_iqueue = collections.deque()
    self.postfn_oqueue = collections.deque()
    self.sendrate = [0, time.time()]
    self.recvrate = [0, time.time()]

  def bind(self, name, workfn, postfn=None, workers=0):
    assert not self.running
    assert name not in self.methods, name
    if workers:
      pool = poollib.ThreadPool(workers, f'pool_{name}')
      self.pools.append(pool)
    else:
      pool = self.pool
    self.methods[name] = (workfn, postfn, pool)

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
    self.socket.close()

  def stats(self):
    now = time.time()
    stats = {
        'numrecv': self.sendrate[0],
        'numsend': self.recvrate[0],
        'sendrate': self.sendrate[0] / (now - self.sendrate[1]),
        'recvrate': self.recvrate[0] / (now - self.recvrate[1]),
        'jobs': len(self.jobs),
    }
    if any(postfn for _, postfn, _ in self.methods.values()):
      stats.update({
          'post_iqueue': len(self.postfn_iqueue),
          'post_oqueue': len(self.postfn_oqueue),
      })
    self.sendrate = [0, now]
    self.recvrate = [0, now]
    return stats

  def __enter__(self):
    self.start(block=False)
    return self

  def __exit__(self, *e):
    self.close()

  def _loop(self):
    while self.running or self.jobs:
      while True:  # Loop syntax used to break on error.
        if not self.running:  # Do not accept further requests.
          break
        try:
          # TODO: Tune the timeout.
          addr, data = self.socket.recv(timeout=0.0001)
        except TimeoutError:
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
        workfn, postfn, pool = self.methods[name]
        job = pool.submit(workfn, *data)
        job.postfn = postfn
        job.addr = addr
        job.reqnum = reqnum
        self.jobs.add(job)
        if postfn:
          # TODO: Somehow block if postfn is lagging behind?
          self.postfn_iqueue.append(job)
        self.recvrate[0] += 1
        break  # We do not actually want to loop.
      # TODO: Tune the timeout.
      completed, self.jobs = concurrent.futures.wait(
          self.jobs, 0.0001, concurrent.futures.FIRST_COMPLETED)
      for job in completed:
        try:
          data = job.result()
          if job.postfn:
            data, info = data
          data = packlib.pack(data)
          status = int(0).to_bytes(8, 'little', signed=False)
          self.socket.send(job.addr, job.reqnum, status, *data)
          self.sendrate[0] += 1
        except Exception as e:
          self._error(job.addr, job.reqnum, 4, f'Error in server method: {e}')
      if completed:
        while self.postfn_iqueue and self.postfn_iqueue[0].done():
          job = self.postfn_iqueue.popleft()
          data, info = job.result()
          self.postfn_oqueue.append(self.postfn_pool.submit(job.postfn, info))
      while self.postfn_oqueue and self.postfn_oqueue[0].done():
        self.postfn_oqueue.popleft().result()  # Check if there was an error.

  def _error(self, addr, reqnum, status, message):
    status = status.to_bytes(8, 'little', signed=False)
    data = message.encode('utf-8')
    self.socket.send(addr, reqnum, status, data)
    if self.errors:
      raise RuntimeError(message)
