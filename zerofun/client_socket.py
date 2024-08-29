import collections
import dataclasses
import queue
import selectors
import socket
import sys
import threading
import time

from . import buffers
from . import contextlib
from . import thread
from . import utils


class Disconnected(Exception):
  pass


@dataclasses.dataclass
class Options:

  ipv6: bool = False
  reconnect: bool = True
  max_msg_size: int = 4 * 1024 ** 3
  max_recv_queue: int = 128
  max_send_queue: int = 128
  keepalive_after: float = 10
  keepalive_every: float = 10
  keepalive_fails: int = 10
  logging: bool = True


class ClientSocket:

  def __init__(self, host, port=None, name='Client', connect=True, **kwargs):
    assert '://' not in host, host
    if port is None:
      host, port = host.rsplit(':', 1)
      port = int(port)
    self.host = host
    self.port = port
    self.name = name
    self.options = Options(**kwargs)
    self.sel = selectors.DefaultSelector()
    self.sock, self.addr = self._create()
    self.isconnected = threading.Event()
    self.sending = collections.deque()
    self.received = queue.Queue()
    self.running = True
    self.thread = thread.Thread(self._loop, start=True)
    connect and self.connect()

  @property
  def connected(self):
    return self.isconnected.is_set()

  def connect(self, timeout=None):
    assert timeout is None or 0 < timeout
    self._log(f'Connecting to {self.addr}')
    start = time.time()
    self.sock.settimeout(1)
    once = True
    while True:
      try:
        # TODO: Instead of relying on keep-alive, we can also resolve the
        # address again periodically and reconnect if the destination has
        # changed or disconnect if resolving raises an exception.
        addr = contextlib.context().resolver(self.addr)
        self.sock.connect(addr)
        self.sock.settimeout(0)
        self.isconnected.set()
        self._log('Connection established')
        return True
      except ConnectionError:
        time.sleep(1)
      except TimeoutError:
        pass
      if timeout and time.time() - start >= timeout:
        return False
      if once:
        self._log('Still trying to connect...')
        once = False

  def send(self, *data):
    assert self.running
    self._require_connection()
    while len(self.sending) > self.options.max_send_queue:
      raise RuntimeError('Too many outgoing messages enqueued')
    maxsize = self.options.max_msg_size
    self.sending.append(buffers.SendBuffer(*data, maxsize=maxsize))

  def recv(self, timeout=None):
    assert self.running
    self._require_connection()
    if timeout == 0:
      return self.received.get(block=False)
    if timeout and timeout <= 1:
      return self.received.get(block=True, timeout=timeout)
    start = time.time()
    while True:
      try:
        return self.received.get(block=True, timeout=0.1)
      except queue.Empty:
        self._require_connection()
        if timeout and time.time() - start >= timeout:
          raise

  def close(self):
    self.running = False
    self.thread.join()
    self.sock.close()
    self.sel.close()

  def _require_connection(self):
    if self.isconnected.is_set():
      return
    self.sending.clear()
    if self.options.reconnect:
      self.connect(timeout=None)
    else:
      raise Disconnected

  def _loop(self):
    recvbuf = buffers.RecvBuffer(maxsize=self.options.max_msg_size)
    while self.running or (self.sending and self.isconnected.is_set()):
      self.isconnected.wait()
      try:
        ready = tuple(self.sel.select(timeout=0.2))
        if not ready:
          continue
        mask = ready[0][1]
        if mask & selectors.EVENT_READ:
          size = recvbuf.recv(self.sock)
          if not size:
            raise OSError('Received zero bytes')
          if recvbuf.done():
            if self.received.qsize() > self.options.max_recv_queue:
              raise RuntimeError('Too many incoming messages enqueued')
            self.received.put(recvbuf.result())
            recvbuf = buffers.RecvBuffer(maxsize=self.options.max_msg_size)
        if mask & selectors.EVENT_WRITE and self.sending:
          first = self.sending[0]
          try:
            first.send(self.sock)
            if first.done():
              self.sending.popleft()
          except (TimeoutError, BlockingIOError):
            pass
      except OSError as e:
        self._log(f'Connection to server lost ({e})')
        self.isconnected.clear()
        self.sel.unregister(self.sock)
        self.sock.close()
        self.sock, self.addr = self._create()
        continue

  def _create(self):
    if self.options.ipv6:
      sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
      addr = (self.host, self.port, 0, 0)
    else:
      sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      addr = (self.host, self.port)

    after = self.options.keepalive_after
    every = self.options.keepalive_every
    fails = self.options.keepalive_fails
    if sys.platform == 'linux':
      sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
      sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, after)
      sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, every)
      sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, fails)
      sock.setsockopt(
          socket.IPPROTO_TCP, socket.TCP_USER_TIMEOUT,
          1000 * (after + every * fails))
    if sys.platform == 'darwin':
      sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
      sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPALIVE, every)
    if sys.platform == 'win32':
      sock.ioctl(socket.SIO_KEEPALIVE_VALS, (1, after * 1000, every * 1000))

    self.sel.register(sock, selectors.EVENT_READ | selectors.EVENT_WRITE, None)
    return sock, addr

  def _log(self, *args):
    if self.options.logging:
      style = utils.style(color='yellow', bold=True)
      reset = utils.style(reset=True)
      print(style + f'[{self.name}]', *args, reset)
