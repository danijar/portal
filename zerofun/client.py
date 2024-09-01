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
        host, port, name, connect, **kwargs)
    self.maxinflight = maxinflight
    self.reqnum = iter(itertools.count(0))
    self.futures = {}
    self.errors = collections.deque()
    self.sendrate = [0, time.time()]
    self.recvrate = [0, time.time()]
    self.waitmean = [0, 0]
    self.lock = threading.Lock()

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
      self._wait(lambda: self._numinflight() < self.maxinflight)
    with self.lock:
      self.waitmean[1] += time.time() - start
      self.waitmean[0] += 1
      self.sendrate[0] += 1
    if self.errors:  # Raise errors of dropped futures.
      raise RuntimeError(self.errors.popleft())
    name = method.encode('utf-8')
    strlen = len(name).to_bytes(8, 'little', signed=False)
    self.socket.send(reqnum, strlen, name, *packlib.pack(data))
    future = Future(self._wait)
    # TODO: Set futures to error state when socket disconnects.
    self.futures[reqnum] = future
    return future

  def close(self, timeout=None):
    self.socket.close(timeout)

  def _numinflight(self):
    return len([x for x in self.futures.values() if not x.don])

  def _wait(self, until, timeout=None):
    # TODO: This waiting function isn't ideal when there are multiple threads
    # waiting at the same time. The problem is that other threads may render
    # the until() condition true for us but we won't notice until 0.1 seconds
    # later if there are no other incoming messages.
    inner = 0.1 if timeout is None else min(timeout, 0.1)
    if timeout not in (0, None):
      start = time.time()
    while not until():
      try:
        data = self.socket.recv(inner)
        self._process(data)
      except TimeoutError:
        if timeout == 0:
          raise
        if timeout is None:
          continue
        if time.time() - start >= timeout:
          raise

  def _process(self, data):
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
      raised = [False]
      future.set_error(message)
      future.raised = raised
      weakref.finalize(future, lambda: (
          None if raised[0] else self.errors.append(message)))


class Future:

  def __init__(self, waitfn):
    self.waitfn = waitfn
    self.raised = None
    self.don = False
    self.res = None
    self.msg = None

  def wait(self, timeout=None):
    if self.don:
      return True
    try:
      self.waitfn(lambda: self.don, timeout)
    except TimeoutError:
      pass
    return self.don

  def done(self):
    return self.don

  def result(self, timeout=None):
    if not self.wait(timeout):
      raise TimeoutError
    assert self.don
    if self.msg is not None:
      self.raised[0] = True
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
