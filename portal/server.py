import collections
import concurrent.futures
import time
import types

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
    requests = collections.deque()
    available = (workers or self.workers) + 1
    self.methods[name] = types.SimpleNamespace(
        workfn=workfn, postfn=postfn, pool=pool,
        requests=requests, available=available)

  def start(self, block=True):
    assert not self.running
    self.running = True
    self.loop.start()
    if block:
      self.loop.join(timeout=None)

  def close(self, timeout=None, internal=False):
    assert self.running
    self.socket.shutdown()
    self.running = False
    if not internal:
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
        'requests': sum(len(m.requests) for m in self.methods.values()),
        'jobs': len(self.jobs),
    }
    if any(method.postfn for method in self.methods.values()):
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
    methods = list(self.methods.values())
    pending = 0
    while self.running or pending:
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
        method = self.methods[name]
        method.requests.append((addr, reqnum, data))
        pending += 1
        break  # We do not actually want to loop.

      for method in methods:
        if method.requests and method.available:
          method.available -= 1
          addr, reqnum, data = method.requests.popleft()
          job = method.pool.submit(method.workfn, *data)
          job.method = method
          job.addr = addr
          job.reqnum = reqnum
          self.jobs.add(job)
          if method.postfn:
            self.postfn_inp.append(job)

      completed, self.jobs = concurrent.futures.wait(
          self.jobs, 0.0001, concurrent.futures.FIRST_COMPLETED)
      for job in completed:
        try:
          data = job.result()
          if job.method.postfn:
            data, _ = data
          data = packlib.pack(data)
          status = int(0).to_bytes(8, 'little', signed=False)
          self.socket.send(job.addr, job.reqnum, status, *data)
          self.metrics['send'] += 1
        except Exception as e:
          self._error(job.addr, job.reqnum, 4, f'Error in server method: {e}')
        finally:
          if not job.method.postfn:
            job.method.available += 1
            pending -= 1

      if completed:
        # Call postfns in the order the requests were received.
        while self.postfn_inp and self.postfn_inp[0].done():
          job = self.postfn_inp.popleft()
          _, info = job.result()
          postjob = self.postfn_pool.submit(job.method.postfn, info)
          postjob.method = job.method
          self.postfn_out.append(postjob)

      while self.postfn_out and self.postfn_out[0].done():
        postjob = self.postfn_out.popleft()
        postjob.result()  # Check if there was an error.
        postjob.method.available += 1
        pending -= 1

  def _error(self, addr, reqnum, status, message):
    status = status.to_bytes(8, 'little', signed=False)
    data = message.encode('utf-8')
    self.socket.send(addr, reqnum, status, data)
    if self.errors:
      # Wait until the error is delivered to the client and then raise.
      self.close(internal=True)
      raise RuntimeError(message)
    else:
      print(f'Error in server method: {message}')
