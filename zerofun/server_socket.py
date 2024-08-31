import collections
import dataclasses
import queue
import selectors
import socket
import threading

from . import buffers
from . import thread
from . import utils


class Connection:

  def __init__(self, sock, addr):
    self.sock = sock
    self.addr = addr
    self.recvbuf = None

  def fileno(self):
    return self.sock.fileno()


@dataclasses.dataclass
class Options:

  ipv6: bool = False
  max_msg_size: int = 4 * 1024 ** 3
  max_recv_queue: int = 4096
  max_send_queue: int = 4096
  logging: bool = True


class ServerSocket:

  def __init__(self, port, name='Server', **kwargs):
    if isinstance(port, str):
      port = int(port.rsplit(':', 1)[1])
    self.name = name
    self.options = Options(**kwargs)
    if self.options.ipv6:
      self.sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
      self.addr = ('', port, 0, 0)
    else:
      self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      self.addr = ('', port)
    self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self.sock.bind(self.addr)
    self.sock.setblocking(False)
    self.sock.listen()
    self.sel = selectors.DefaultSelector()
    self.sel.register(self.sock, selectors.EVENT_READ, data=None)
    self._log(f'Listening at {self.addr[0]}:{self.addr[1]}')
    self.conns = {}
    self.running = True
    self.received = queue.Queue()  # [(addr, buffer)]
    self.sending = collections.deque()  # [(addr, SendBuffer)]
    self.event = threading.Event()
    self.thread = thread.Thread(self._loop, start=True)

  @property
  def connections(self):
    return tuple(self.conns.keys())

  def recv(self, timeout=None):
    assert self.running
    try:
      return self.received.get(block=(timeout != 0), timeout=timeout)
    except queue.Empty:
      raise TimeoutError

  def send(self, addr, *data):
    assert self.running
    if len(self.sending) > self.options.max_send_queue:
      raise RuntimeError('Too many outgoing messages enqueued')
    maxsize = self.options.max_msg_size
    self.sending.append((addr, buffers.SendBuffer(*data, maxsize=maxsize)))

  def close(self, timeout=None):
    self.running = False
    self.thread.join(timeout)
    [conn.sock.close() for conn in self.conns.values()]
    self.sock.close()
    self.sel.close()

  def _loop(self):
    while self.running:
      writeable = set()
      for key, mask in self.sel.select(timeout=0.2):
        if key.data is None:
          assert mask & selectors.EVENT_READ
          self._accept(key.fileobj)
        elif mask & selectors.EVENT_READ:
          self._recv(key.data)
        elif mask & selectors.EVENT_WRITE:
          writeable.add(key.data.addr)
      remaining = []
      for _ in range(len(self.sending)):
        addr, sendbuf = self.sending.popleft()
        if addr not in writeable:
          remaining.append((addr, sendbuf))
          continue
        # Prevent later buffers from being written before the first buffer for
        # each address is fully written. This would cause mingled data.
        writeable.remove(addr)
        conn = self.conns.get(addr, None)
        if not conn:
          self._log('Dropping messages to disconnected client')
          continue
        try:
          sendbuf.send(conn.sock)
          if not sendbuf.done():
            remaining.append((addr, sendbuf))
        except ConnectionResetError:
          self._disconnect(conn)
        except BlockingIOError:
          remaining.append((addr, sendbuf))

      self.sending.extendleft(reversed(remaining))

  def _accept(self, sock):
    sock, addr = sock.accept()
    self._log(f'Accepted connection from {addr[0]}:{addr[1]}')
    sock.setblocking(False)
    conn = Connection(sock, addr)
    self.sel.register(sock, selectors.EVENT_READ | selectors.EVENT_WRITE, conn)
    self.conns[addr] = conn

  def _recv(self, conn):
    if not conn.recvbuf:
      conn.recvbuf = buffers.RecvBuffer(maxsize=self.options.max_msg_size)
    try:
      conn.recvbuf.recv(conn.sock)
    except ConnectionResetError:
      self._disconnect(conn)
      return
    if conn.recvbuf.done():
      if self.received.qsize() > self.options.max_recv_queue:
        raise RuntimeError('Too many incoming messages enqueued')
      self.received.put((conn.addr, conn.recvbuf.result()))
      conn.recvbuf = None

  def _disconnect(self, conn):
    self._log(f'Closed connection to {conn.addr[0]}:{conn.addr[1]}')
    self.sel.unregister(conn.sock)
    del self.conns[conn.addr]
    conn.sock.close()

  def _log(self, *args, **kwargs):
    if self.options.logging:
      style = utils.style(color='blue', bold=True)
      reset = utils.style(reset=True)
      print(style + f'[{self.name}]', *args, reset)
