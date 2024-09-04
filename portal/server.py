import collections
import concurrent.futures
import threading
import time

from . import packlib
from . import poollib
from . import server_socket
from . import thread


class Server:

  def __init__(self, port, name='Server', workers=1, errors=True, **kwargs):
    self.socket = server_socket.ServerSocket(port, name, **kwargs)
    self.loop = thread.Thread(self._loop, name=f'{name}Loop')
    self.methods = {}
    self.jobs = set()
    self.workers = workers
    self.errors = errors
    self.running = False
    self.pool = poollib.ThreadPool(workers, 'default_pool')
    self.postfn_pool = poollib.ThreadPool(1, 'postfn')
    self.postfn_inp = collections.deque()
    self.postfn_out = collections.deque()
    self.metrics = dict(send=0, recv=0, time=time.time())
    self.pools = [self.pool, self.postfn_pool]

  def bind(self, name, workfn, postfn=None, workers=0):
    assert not self.running
    assert name not in self.methods, name
    if workers:
      pool = poollib.ThreadPool(workers, '{name}_pool')
      self.pools.append(pool)
    else:
      pool = self.pool
    active = threading.Semaphore((workers or self.workers) + 1)
    def workfn2(*args):
      active.acquire()
      return workfn(*args)
    self.methods[name] = (workfn2, postfn, pool, active)

  def start(self, block=True):
    assert not self.running
    self.running = True
    self.loop.start()
    if block:
      self.loop.join(timeout=None)

  def close(self, timeout=None):
    assert self.running
    self.socket.shutdown()
    self.running = False
    self.loop.join(timeout)
    self.loop.kill()
    [x.close() for x in self.pools]
    self.socket.close()

  def stats(self):
    now = time.time()
    mets = self.metrics
    self.metrics = dict(send=0, recv=0, time=now)
    dur = now - mets['time']
    stats = {
        'numsend': mets['send'],
        'numrecv': mets['recv'],
        'sendrate': mets['send'] / dur,
        'recvrate': mets['recv'] / dur,
        'jobs': len(self.jobs),
    }
    if any(postfn for _, postfn, _, _ in self.methods.values()):
      stats.update({
          'post_iqueue': len(self.postfn_inp),
          'post_oqueue': len(self.postfn_out),
      })
    return stats

  def __enter__(self):
    self.start(block=False)
    return self

  def __exit__(self, *e):
    self.close()

  def _loop(self):
    while self.running or self.jobs or self.postfn_out:
      while True:  # Loop syntax used to break on error.
        if not self.running:  # Do not accept further requests.
          break
        try:
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
        self.metrics['recv'] += 1
        workfn, postfn, pool, active = self.methods[name]
        job = pool.submit(workfn, *data)
        job.active = active
        job.postfn = postfn
        job.addr = addr
        job.reqnum = reqnum
        self.jobs.add(job)
        if postfn:
          self.postfn_inp.append(job)
        break  # We do not actually want to loop.
      completed, self.jobs = concurrent.futures.wait(
          self.jobs, 0.0001, concurrent.futures.FIRST_COMPLETED)
      for job in completed:
        try:
          data = job.result()
          if job.postfn:
            data, info = data
          else:
            job.active.release()
          data = packlib.pack(data)
          status = int(0).to_bytes(8, 'little', signed=False)
          self.socket.send(job.addr, job.reqnum, status, *data)
          self.metrics['send'] += 1
        except Exception as e:
          self._error(job.addr, job.reqnum, 4, f'Error in server method: {e}')
      if completed:
        while self.postfn_inp and self.postfn_inp[0].done():
          job = self.postfn_inp.popleft()
          data, info = job.result()
          postjob = self.postfn_pool.submit(job.postfn, info)
          postjob.active = job.active
          self.postfn_out.append(postjob)
      while self.postfn_out and self.postfn_out[0].done():
        postjob = self.postfn_out.popleft()
        postjob.active.release()
        postjob.result()  # Check if there was an error.

  def _error(self, addr, reqnum, status, message):
    status = status.to_bytes(8, 'little', signed=False)
    data = message.encode('utf-8')
    self.socket.send(addr, reqnum, status, data)
    if self.errors:
      raise RuntimeError(message)
