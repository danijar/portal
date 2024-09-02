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
      self, host, port, name='Client', maxinflight=16, connect=True, **kwargs):
    assert 1 <= maxinflight, maxinflight

    self.socket = client_socket.ClientSocket(
        host, port, name, connect=False, **kwargs)

    self.socket.callbacks_recv.append(self._recv)
    self.socket.callbacks_disc.append(self._disc)
    # self.socket.callbacks_conn.append()

    connect and self.socket.connect()

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
    if self._numinflight() >= self.maxinflight:
      with self.cond:
        self.cond.wait_for(lambda: self._numinflight() < self.maxinflight)

    with self.lock:
      self.waitmean[1] += time.time() - start
      self.waitmean[0] += 1
      self.sendrate[0] += 1

    if self.errors:  # Raise errors of dropped futures.
      raise RuntimeError(self.errors.popleft())

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
    assert future, (
        f'Unexpected request number: {reqnum}',
        sorted(self.futures.keys()))
    if status == 0:
      data = packlib.unpack(data[16:])
      future.set_result(data)
    else:
      message = bytes(data[16:]).decode('utf-8')
      self._seterr(future, message)
    with self.cond:
      self.cond.notify_all()
    self.socket.recv()

  def _disc(self):
    # TODO: Add a test for this.
    for future in self.futures.values():
      if not future.done():
        self._seterr(future, 'Connection lost')

  def _seterr(self, future, message):
    raised = [False]
    future.raised = raised
    future.set_error(message)
    weakref.finalize(future, lambda: (
        None if raised[0] else self.errors.append(message)))


class Future:

  def __init__(self):
    self.raised = [False]
    self.con = threading.Condition()
    self.don = False
    self.res = None
    self.msg = None

  def wait(self, timeout=None):
    if self.don:
      return self.don
    with self.con:
      return self.con.wait(timeout)

  def done(self):
    return self.don

  def result(self, timeout=None):
    if not self.wait(timeout):
      raise TimeoutError
    assert self.don
    if self.msg is None:
      return self.res
    if not self.raised[0]:
      raise RuntimeError(self.msg)

  def set_result(self, result):
    assert not self.don
    self.don = True
    self.res = result
    with self.con:
      self.con.notify_all()

  def set_error(self, message):
    assert not self.don
    self.don = True
    self.msg = message
    with self.con:
      self.con.notify_all()
