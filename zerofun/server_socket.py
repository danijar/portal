import collections
import dataclasses
import queue
import selectors
import socket
import threading

from . import buffers
from . import thread


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
  debug: bool = True  # TODO


class ServerSocket:

  def __init__(self, port, **kwargs):
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
    self._log(f'Listening on port {port}')
    self.conns = {}
    self.running = True
    self.event = threading.Event()
    self.thread = thread.Thread(self._loop, start=True)
    self.received = queue.Queue()  # [(addr, buffer)]
    self.sending = collections.deque()  # [(addr, SendBuffer)]

  @property
  def connections(self):
    return tuple(self.conns.keys())

  def recv(self, timeout=None):
    assert self.running
    return self.received.get(block=(timeout != 0), timeout=timeout)

  def send(self, addr, *data):
    assert self.running
    if len(self.sending) > self.options.max_send_queue:
      raise RuntimeError('Too many outgoing messages enqueued')
    maxsize = self.options.max_msg_size
    self.sending.append((addr, buffers.SendBuffer(*data, maxsize=maxsize)))

  def close(self):
    self.running = False
    self.thread.join()
    [conn.sock.close() for conn in self.conns.values()]
    self.sock.close()
    self.sel.close()

  def _loop(self):
    while self.running:
      for reader, _ in self.sel.select(timeout=0.01):
        if reader.data is None:
          self._accept(reader.fileobj)
        else:
          self._recv(reader.data)
      remaining = []
      for _ in range(len(self.sending)):
        addr, sendbuf = self.sending.popleft()
        conn = self.conns.get(addr, None)
        if not conn:
          self._log('Dropping messages to disconnected client')
          continue
        sendbuf.send(conn.sock)
        if not sendbuf.done():
          remaining.append((addr, sendbuf))
      self.sending.extendleft(reversed(remaining))

  def _accept(self, sock):
    sock, addr = sock.accept()
    self._log(f'Accepted connection from {addr}')
    sock.setblocking(False)
    conn = Connection(sock, addr)
    self.sel.register(sock, selectors.EVENT_READ, conn)
    self.conns[addr] = conn

  def _recv(self, conn):
    if not conn.recvbuf:
      conn.recvbuf = buffers.RecvBuffer(maxsize=self.options.max_msg_size)
    size = conn.recvbuf.recv(conn.sock)
    if conn.recvbuf.done():
      if self.received.qsize() > self.options.max_recv_queue:
        raise RuntimeError('Too many incoming messages enqueued')
      self.received.put((conn.addr, conn.recvbuf.result()))
      conn.recvbuf = None
    if size == 0:
      self._log(f'Closing connection to {conn.addr} (received zero bytes)')
      self.sel.unregister(conn.sock)
      del self.conns[conn.addr]
      conn.sock.close()

  def _log(self, *args, **kwargs):
    if self.options.debug:
      import elements
      elements.print('[Server]', *args, color='blue', bold=True)
