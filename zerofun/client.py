import collections
import functools
import itertools
import threading
import time
import weakref

from . import client_socket
from . import packlib


class Client:

  def __init__(
      self, host, port, name='Client', maxinflight=16, **kwargs):
    assert 1 <= maxinflight, maxinflight
    self.socket = client_socket.ClientSocket(
        host, port, f'{name}Socket', start=False, **kwargs)
    self.socket.callbacks_recv.append(self._recv)
    self.socket.callbacks_disc.append(self._disc)
    self.socket.start()
    self.maxinflight = maxinflight
    self.reqnum = iter(itertools.count(0))
    self.futures = {}
    self.errors = collections.deque()
    self.sendrate = [0, time.time()]
    self.recvrate = [0, time.time()]
    self.waitmean = [0, 0]
    self.cond = threading.Condition()
    self.lock = threading.Lock()

  @property
  def connected(self):
    return self.socket.connected

  def __getattr__(self, name):
    if name.startswith('_'):
      raise AttributeError(name)
    try:
      return functools.partial(self.call, name)
    except AttributeError:
      raise ValueError(name)

  def stats(self):
    now = time.time()
    stats = {
        'inflight': self._numinflight(),
        'numsend': self.sendrate[0],
        'numrecv': self.recvrate[0],
        'sendrate': self.sendrate[0] / (now - self.sendrate[1]),
        'recvrate': self.recvrate[0] / (now - self.recvrate[1]),
        'waitmean': self.waitmean[0] and (self.waitmean[1] / self.waitmean[0]),
    }
    self.sendrate = [0, now]
    self.recvrate = [0, now]
    self.waitmean = [0, 0]
    return stats

  def connect(self, timeout=None):
    return self.socket.connect(timeout)

  def call(self, method, *data):
    reqnum = next(self.reqnum).to_bytes(8, 'little', signed=False)
    start = time.time()
    while self._numinflight() >= self.maxinflight:
      with self.cond: self.cond.wait(timeout=0.2)
      try:
        self.socket.require_connection(timeout=0)
      except TimeoutError:
        pass
    with self.lock:
      self.waitmean[1] += time.time() - start
      self.waitmean[0] += 1
      self.sendrate[0] += 1
    if self.errors:  # Raise errors of dropped futures.
      raise self.errors.popleft()
    name = method.encode('utf-8')
    strlen = len(name).to_bytes(8, 'little', signed=False)
    self.socket.send(reqnum, strlen, name, *packlib.pack(data))
    future = Future()
    self.futures[reqnum] = future
    return future

  def close(self, timeout=None):
    self.socket.close(timeout)

  def _numinflight(self):
    return len([x for x in self.futures.values() if not x.don])

  def _recv(self, data):
    assert len(data) >= 16, 'Unexpectedly short response'
    reqnum = bytes(data[:8])
    status = int.from_bytes(data[8:16], 'little', signed=False)
    future = self.futures.pop(reqnum, None)
    if not future and not self.socket.options.autoconn:
      # TODO: Why do we sometimes receive delayed respones from the server
      # after client socket has called disconnect callback already?
      self.socket.recv()
      return
    assert future, (
        f'Unexpected request number: {reqnum}',
        sorted(self.futures.keys()))
    if status == 0:
      data = packlib.unpack(data[16:])
      future.set_result(data)
    else:
      message = bytes(data[16:]).decode('utf-8')
      self._seterr(future, RuntimeError(message))
    with self.cond:
      self.cond.notify_all()
    self.socket.recv()

  def _disc(self):
    if not self.socket.options.autoconn:
      for future in self.futures.values():
        assert not future.done()
        self._seterr(future, client_socket.Disconnected)
      self.futures.clear()

  def _seterr(self, future, e):
    raised = [False]
    future.raised = raised
    future.set_error(e)
    weakref.finalize(future, lambda: (
        None if raised[0] else self.errors.append(e)))


class Future:

  def __init__(self):
    self.raised = [False]
    self.con = threading.Condition()
    self.don = False
    self.res = None
    self.err = None

  def __repr__(self):
    if not self.done:
      return 'Future(done=False)'
    elif self.err:
      return f"Future(done=True, error='{self.err}', raised={self.raised[0]})"
    else:
      return 'Future(done=True)'

  def wait(self, timeout=None):
    if self.don:
      return self.don
    with self.con: return self.con.wait(timeout)

  def done(self):
    return self.don

  def result(self, timeout=None):
    if not self.wait(timeout):
      raise TimeoutError
    assert self.don
    if self.err is None:
      return self.res
    if not self.raised[0]:
      self.raised[0] = True
      raise self.err

  def set_result(self, result):
    assert not self.don
    self.don = True
    self.res = result
    with self.con: self.con.notify_all()

  def set_error(self, e):
    assert not self.don
    self.don = True
    self.err = e
    with self.con: self.con.notify_all()
