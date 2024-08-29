import functools
import itertools
import time
import weakref

from . import client_socket
from . import packlib


class Client:

  def __init__(
      self, host, port, name='Client', maxinflight=16, connect=True, **kwargs):
    assert 1 <= maxinflight, maxinflight
    self.socket = client_socket.ClientSocket(
        host, port, name, connect, **kwargs)
    self.maxinflight = maxinflight
    self.reqnum = iter(itertools.count(0))
    self.futures = {}
    self.errors = []
    self.sendrate = [0, time.time()]
    self.recvrate = [0, time.time()]
    self.waitmean = [0, 0]

  @property
  def connected(self):
    return self.socket.connected

  def __getattr__(self, name):
    if name.startswith('__'):
      raise AttributeError(name)
    try:
      return functools.partial(self.call, name)
    except AttributeError:
      raise ValueError(name)

  def stats(self):
    now = time.time()
    stats = {
        'inflight': self._numinflight(),
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
      self._step(timeout=None)
    self.waitmean[1] += time.time() - start
    self.waitmean[0] += 1
    if self.errors:  # Raise errors of dropped futures.
      raise RuntimeError(self.errors[0])
    name = method.encode('utf-8')
    strlen = len(name).to_bytes(8, 'little', signed=False)
    self.socket.send(reqnum, strlen, name, *packlib.pack(data))
    self.sendrate[0] += 1
    future = Future(self._step)
    self.futures[reqnum] = future
    return future

  def close(self):
    return self.socket.close()

  def _numinflight(self):
    return len([x for x in self.futures.values() if not x.don])

  def _step(self, timeout=None):
    data = self.socket.recv(timeout)  # May raise queue.Empty.
    assert len(data) >= 16, 'Unexpectedly short response'
    reqnum = bytes(data[:8])
    status = int.from_bytes(data[8:16], 'little', signed=False)

    print('->', reqnum, status)

    future = self.futures.pop(reqnum, None)
    assert future, (
        f'Unexpected request number: {reqnum}', sorted(self.futures.keys()))
    if status == 0:
      data = packlib.unpack(data[16:])
      future.set_result(data)
    else:
      message = bytes(data[16:]).decode('utf-8')
      future.set_error(message)
      weakref.finalize(future, lambda: self.errors.append(message))


class Future:

  def __init__(self, step):
    self.step = step
    self.don = False
    self.res = None
    self.msg = None

  def wait(self, timeout=None):
    if timeout is None:
      while not self.don:
        self.step(timeout=None)
      assert self.don
      return True
    start = time.time()
    while True:
      remaining = timeout - (time.time() - start)
      self.step(timeout=max(0, remaining))
      if self.don:
        return True
      if remaining <= 0:
        break
    return False

  def done(self):
    if not self.don:
      self.wait(timeout=0)
    return self.don

  def result(self):
    if not self.don:
      self.wait(timeout=None)
    assert self.don
    if self.msg is not None:
      raise RuntimeError(self.msg)
    return self.res

  def set_result(self, result):
    assert not self.don
    self.don = True
    self.res = result

  def set_error(self, message):
    assert not self.don
    self.don = True
    self.msg = message
