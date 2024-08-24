import collections
import dataclasses
import queue
import selectors
import socket
import sys
import threading
import time

from . import buffers
from . import thread
from . import utils


class DisconnectedError(Exception):
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
  keepalive_maxfails: int = 10
  debug: bool = True  # TODO


class ClientSocket:

  def __init__(self, host, port, connect=True, **kwargs):
    self.options = Options(**kwargs)
    if self.options.ipv6:
      self.sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
      self.addr = (host, port, 0, 0)
    else:
      self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      self.addr = (host, port)
    self.sel = selectors.DefaultSelector()
    self.sel.register(self.sock, selectors.EVENT_READ, data=None)
    self._setup(self.sock, self.options)
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
    self._log(f'Connecting to {self.addr}')
    self.sock.settimeout(timeout)
    try:
      addr = utils.context().resolver(self.addr)
      self.sock.connect(addr)
      self.sock.settimeout(0)
      self.isconnected.set()
      return True
    except TimeoutError:
      return False

  def send(self, *data):
    self._require_connection()
    while len(self.sending) > self.options.max_send_queue:
      raise RuntimeError('Too many outgoing messages enqueued')
    maxsize = self.options.max_msg_size
    self.sending.append(buffers.SendBuffer(*data, maxsize=maxsize))

  def recv(self, timeout=None):
    self._require_connection()
    if timeout == 0:
      return self.received.get(block=False)
    if timeout and timeout <= 1:
      return self.received.get(block=True, timeout=timeout)
    start = time.time()
    while True:
      try:
        return self.received.get(block=True, timeout=1)
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
    if not self.connected:
      self.sending.clear()
      self.received.clear()
      raise DisconnectedError

  def _loop(self):
    recvbuf = buffers.RecvBuffer(self.options.max_msg_size)
    while self.running:
      self.isconnected.wait()
      try:
        if tuple(self.sel.select(timeout=1)):
          size = recvbuf.recv(self.sock)
          if not size:
            raise OSError('received zero bytes')
          if recvbuf.done():
            if len(self.received) > self.options.max_recv_queue:
              raise RuntimeError('Too many incoming messages enqueued')
            self.received.append(recvbuf.result())
            recvbuf = buffers.RecvBuffer()
        if self.sending:
          first = self.sending[0]
          try:
            first.send(self.sock)
            if first.done():
              self.sending.popleft()
          except TimeoutError:
            pass
      except OSError as e:
        self._log(f'Connection to server lost ({e})')
        self.isconnected.clear()
        continue

  def _setup(self, sock, options):

    return  # TODO

    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    tcp = socket.IPPROTO_TCP
    if sys.platform == 'darwin':
      TCP_KEEPALIVE = 0x10
      sock.setsockopt(tcp, TCP_KEEPALIVE, options.keepalive_every)
    else:
      sock.setsockopt(tcp, socket.TCP_KEEPIDLE, options.keepalive_after)
      sock.setsockopt(tcp, socket.TCP_KEEPINTVL, options.keepalive_every)
      sock.setsockopt(tcp, socket.TCP_KEEPCNT, options.keepalive_maxfails)

  def _log(self, *args):
    if self.options.debug:
      import elements
      elements.print('[Client]', *args, color='yellow', bold=True)
