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
    self.isconnected = threading.Event()
    self.shouldconn = threading.Event()
    self.sending = collections.deque()
    self.received = queue.Queue()
    self.running = True
    self.thread = thread.Thread(self._loop, start=True)
    connect and self.connect()

  @property
  def connected(self):
    return self.isconnected.is_set()

  def connect(self, timeout=None):
    self.shouldconn.set()
    return self.isconnected.wait(timeout)

  def send(self, *data, timeout=None):
    assert self.running
    self._require_connection(timeout)
    if len(self.sending) > self.options.max_send_queue:
      raise RuntimeError('Too many outgoing messages enqueued')
    maxsize = self.options.max_msg_size
    self.sending.append(buffers.SendBuffer(*data, maxsize=maxsize))

  def recv(self, timeout=None):
    assert self.running
    timeout = utils.Timeout(timeout)  # TODO: Inline and delete class.
    try:
      if timeout.over:
        return self.received.get(block=False)
      while True:
        try:
          return self.received.get(timeout=min(timeout.number, 0.2))
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

    while self.running or (self.sending and self.isconnected.is_set()):

      if not self.isconnected.is_set():
        if not self.shouldconn.wait(timeout=0.2):
          continue
        sock = self._connect()
        if not sock:
          break
        sel.register(sock, selectors.EVENT_READ | selectors.EVENT_WRITE)
        self.isconnected.set()
        self.shouldconn.clear()

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
            if self.received.qsize() > self.options.max_recv_queue:
              raise RuntimeError('Too many incoming messages enqueued')
            self.received.put(recvbuf.result())
            recvbuf = buffers.RecvBuffer(maxsize=self.options.max_msg_size)
        if mask & selectors.EVENT_WRITE and self.sending:
          first = self.sending[0]
          try:
            first.send(sock)
            if first.done():
              self.sending.popleft()
          except (TimeoutError, BlockingIOError):
            pass

      except OSError as e:
        detail = f'{type(e).__name__}'
        detail = f'{detail}: {e}' if str(e) else detail
        self._log(f'Connection to server lost ({detail})')
        self.isconnected.clear()
        sel.unregister(sock)
        sock.close()
        # Discard remaining messages because it's not clear whether they should
        # be delivered to another server once a new connection is established.
        # Discarding the queue is equivalent to the situation that all messages
        # have been delivered and the receiving end terminated right after
        # without being able to do anything with the received messages.
        self.sending.clear()
        continue

    sock.close()
    sel.close()

  def _connect(self):
    sock, addr = self._create()
    self._log(f'Connecting to {addr[0]}:{addr[1]}')
    # We need to resolve the address regularly.
    sock.settimeout(10)
    once = True
    while self.running:
      try:
        addr = contextlib.context().resolver(addr)
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
    return None

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
    return sock, addr

  def _log(self, *args):
    if self.options.logging:
      style = utils.style(color='yellow', bold=True)
      reset = utils.style(reset=True)
      print(style + f'[{self.name}]', *args, reset)
