import collections
import dataclasses
import queue
import selectors
import socket
import sys
import threading

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
  resend: bool = False
  max_msg_size: int = 4 * 1024 ** 3
  max_recv_queue: int = 128
  max_send_queue: int = 128
  keepalive_after: float = 10
  keepalive_every: float = 10
  keepalive_fails: int = 10
  logging: bool = True


class ClientSocket:

  def __init__(self, host, port=None, name='Client', connect=True, **kwargs):
    assert (port or ':' in host) and '://' not in host, host
    if port is None:
      host, port = host.rsplit(':', 1)
      port = int(port)
    self.addr = (host, port)
    self.name = name
    self.options = Options(**kwargs)

    self.callbacks_recv = []
    self.callbacks_conn = []
    self.callbacks_disc = []

    self.isconn = threading.Event()
    self.wantconn = threading.Event()
    self.sendq = collections.deque()
    self.recvq = queue.Queue()

    self.running = True
    self.thread = thread.Thread(self._loop, start=True)
    connect and self.connect()

  @property
  def connected(self):
    return self.isconn.is_set()

  def connect(self, timeout=None):
    self.wantconn.set()
    return self.isconn.wait(timeout)

  def send(self, *data, timeout=None):
    assert self.running
    self._require_connection(timeout)
    if len(self.sendq) > self.options.max_send_queue:
      raise RuntimeError('Too many outgoing messages enqueued')
    maxsize = self.options.max_msg_size
    self.sendq.append(buffers.SendBuffer(*data, maxsize=maxsize))

  def recv(self, timeout=None):
    assert self.running
    timeout = utils.Timeout(timeout)  # TODO: Inline and delete class.
    try:
      if timeout.over:
        return self.recvq.get(block=False)
      while True:
        try:
          return self.recvq.get(timeout=min(timeout.number, 0.2))
        except queue.Empty:
          self._require_connection(timeout.left)
          if timeout.over:
            raise
    except queue.Empty:
      raise TimeoutError

  def close(self, timeout=None):
    self.running = False
    self.thread.join(timeout)
    self.thread.kill()

  def _require_connection(self, timeout):
    if self.connected:
      return
    if self.options.reconnect:
      if timeout == 0 or not self.connect(timeout):
        raise TimeoutError
    else:
      raise Disconnected

  def _loop(self):
    sel = selectors.DefaultSelector()
    recvbuf = buffers.RecvBuffer(maxsize=self.options.max_msg_size)

    while self.running or (self.sendq and self.isconn.is_set()):

      if not self.isconn.is_set():
        if not self.wantconn.wait(timeout=0.2):
          continue
        sock = self._connect()
        if not sock:
          break
        sel.register(sock, selectors.EVENT_READ | selectors.EVENT_WRITE)
        self.isconn.set()
        self.wantconn.clear()
        [x() for x in self.callbacks_conn]

      try:
        ready = tuple(sel.select(timeout=0.2))
        if not ready:
          continue
        mask = ready[0][1]
        if mask & selectors.EVENT_READ:
          size = recvbuf.recv(sock)
          if not size:
            raise ConnectionResetError
          if recvbuf.done():
            if self.recvq.qsize() > self.options.max_recv_queue:
              raise RuntimeError('Too many incoming messages enqueued')
            msg = recvbuf.result()
            self.recvq.put(msg)
            [x(msg) for x in self.callbacks_recv]
            recvbuf = buffers.RecvBuffer(maxsize=self.options.max_msg_size)
        if mask & selectors.EVENT_WRITE and self.sendq:
          first = self.sendq[0]
          try:
            first.send(sock)
            if first.done():
              self.sendq.popleft()
          except (TimeoutError, BlockingIOError):
            pass

      except OSError as e:
        detail = f'{type(e).__name__}'
        detail = f'{detail}: {e}' if str(e) else detail
        self._log(f'Connection to server lost ({detail})')
        self.isconn.clear()
        sel.unregister(sock)
        sock.close()
        if self.options.resend:
          if self.sendq:
            self.sendq[0].reset()
        else:
          self.sendq.clear()
        [x() for x in self.callbacks_disc]
        continue

    sock.close()
    sel.close()

  def _connect(self):
    host, port = self.addr
    self._log(f'Connecting to {host}:{port}')
    once = True
    while self.running:
      sock, addr = self._create()
      try:
        # We need to resolve the address regularly.
        addr = contextlib.context().resolver(addr)
        sock.settimeout(10)
        sock.connect(addr)
        sock.settimeout(0)
        self._log('Connection established')
        return sock
      except ConnectionError:
        pass
      except TimeoutError:
        pass
      if once:
        self._log('Still trying to connect...')
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
    if self.options.logging:
      style = utils.style(color='yellow', bold=True)
      reset = utils.style(reset=True)
      print(style + f'[{self.name}]', *args, reset)
