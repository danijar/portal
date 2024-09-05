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


class Disconnected(Exception):
  pass


@dataclasses.dataclass
class Options:

  ipv6: bool = False
  autoconn: bool = True
  max_msg_size: int = 4 * 1024 ** 3
  max_recv_queue: int = 128
  max_send_queue: int = 128
  keepalive_after: float = 10
  keepalive_every: float = 10
  keepalive_fails: int = 10
  logging: bool = True
  logging_color: str = 'yellow'


class ClientSocket:

  def __init__(self, host, port=None, name='Client', start=True, **kwargs):
    assert (port or ':' in host) and '://' not in host, host
    if port is None:
      host, port = host.rsplit(':', 1)
      port = int(port)
      assert host, host
    self.addr = (host, port)
    self.name = name
    self.options = Options(**{**contextlib.context.clientkw, **kwargs})

    self.callbacks_recv = []
    self.callbacks_conn = []
    self.callbacks_disc = []

    self.isconn = threading.Event()
    self.wantconn = threading.Event()
    self.sendq = collections.deque()
    self.recvq = queue.Queue()

    self.running = True
    self.thread = thread.Thread(self._loop, name=f'{name}Loop')
    start and self.thread.start()

  def start(self):
    self.thread.start()

  @property
  def connected(self):
    return self.isconn.is_set()

  def connect(self, timeout=None):
    assert not self.connected
    if not self.options.autoconn:
      self.wantconn.set()
    return self.isconn.wait(timeout)

  def send(self, *data, timeout=None):
    assert self.running
    if len(self.sendq) > self.options.max_send_queue:
      raise RuntimeError('Too many outgoing messages enqueued')
    self.require_connection(timeout)
    maxsize = self.options.max_msg_size
    self.sendq.append(buffers.SendBuffer(*data, maxsize=maxsize))

  def recv(self, timeout=None):
    assert self.running
    try:
      if timeout is not None and timeout <= 0.2:
        return self.recvq.get(block=(timeout != 0), timeout=timeout)
      start = time.time()
      while True:
        try:
          return self.recvq.get(timeout=min(timeout, 0.2) if timeout else 0.2)
        except queue.Empty:
          timeout = timeout and max(0, timeout - (time.time() - start))
          self.require_connection(timeout)
          if timeout == 0:
            raise
    except queue.Empty:
      raise TimeoutError

  def close(self, timeout=None):
    self.running = False
    self.thread.join(timeout)
    self.thread.kill()

  def require_connection(self, timeout):
    if self.connected:
      return
    if not self.options.autoconn:
      raise Disconnected
    if timeout == 0 or not self.isconn.wait(timeout):
      raise TimeoutError

  def _loop(self):
    recvbuf = buffers.RecvBuffer(maxsize=self.options.max_msg_size)
    sel = selectors.DefaultSelector()
    sock = None
    isconn = False  # Local mirror of self.isconn without the lock.

    while self.running or (self.sendq and isconn):

      if not isconn:
        if not self.options.autoconn and not self.wantconn.wait(timeout=0.2):
          continue
        sock = self._connect()
        if not sock:
          break
        sel.register(sock, selectors.EVENT_READ | selectors.EVENT_WRITE)
        self.isconn.set()
        isconn = True
        if not self.options.autoconn:
          self.wantconn.clear()
        [x() for x in self.callbacks_conn]

      try:

        ready = sel.select(timeout=0.2)
        if not ready:
          continue
        _, mask = ready[0]

        if mask & selectors.EVENT_READ:
          try:
            recvbuf.recv(sock)
            if recvbuf.done():
              if self.recvq.qsize() > self.options.max_recv_queue:
                raise RuntimeError('Too many incoming messages enqueued')
              msg = recvbuf.result()
              self.recvq.put(msg)
              [x(msg) for x in self.callbacks_recv]
              recvbuf = buffers.RecvBuffer(maxsize=self.options.max_msg_size)
          except BlockingIOError:
            pass

        if self.sendq and mask & selectors.EVENT_WRITE:
          try:
            self.sendq[0].send(sock)
            if self.sendq[0].done():
              self.sendq.popleft()
          except BlockingIOError:
            pass

      except OSError as e:
        detail = f'{type(e).__name__}'
        detail = f'{detail}: {e}' if str(e) else detail
        self._log(f'Connection to server lost ({detail})')
        self.isconn.clear()
        isconn = False
        sel.unregister(sock)
        sock.close()
        # Clear message queue on disconnect. There is no meaningful concept of
        # sucessful delivery of a message at this level. For example, the
        # server could receive the message but then go down immediately after,
        # without doing anything meaningful with the message. Resending can be
        # done based on response messages at a higher level.
        self.sendq.clear()
        recvbuf = buffers.RecvBuffer(maxsize=self.options.max_msg_size)
        [x() for x in self.callbacks_disc]
        continue

    if sock:
      sock.close()

  def _connect(self):
    host, port = self.addr
    self._log(f'Connecting to {host}:{port}')
    once = True
    while self.running:
      sock, addr = self._create()
      error = None
      try:
        # We need to resolve the address regularly.
        if contextlib.context.resolver:
          addr = contextlib.context.resolver(addr)
        sock.settimeout(10)
        sock.connect(addr)
        sock.settimeout(0)
        self._log('Connection established')
        return sock
      except TimeoutError as e:
        error = e
      except ConnectionError as e:
        error = e
      except socket.gaierror as e:
        error = e
      time.sleep(0.1)
      if once:
        self._log(f'Still trying to connect... ({error})')
        once = False
      sock.close()
    return None

  def _create(self):
    if self.options.ipv6:
      sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
      addr = (*self.addr, 0, 0)
    else:
      sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      addr = self.addr
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
    return sock, addr

  def _log(self, *args):
    if not self.options.logging:
      return
    contextlib.context.print(
        self.name, *args, color=self.options.logging_color)
