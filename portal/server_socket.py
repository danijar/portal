import collections
import dataclasses
import os
import queue
import selectors
import socket

from . import buffers
from . import contextlib
from . import thread


class Connection:

  def __init__(self, sock, addr):
    self.sock = sock
    self.addr = addr
    self.recvbuf = None
    self.handshake = b''
    self.sendbufs = collections.deque()

  def fileno(self):
    return self.sock.fileno()


@dataclasses.dataclass
class Options:

  ipv6: bool = False
  host: str = ''
  max_msg_size: int = 4 * 1024 ** 3
  max_recv_queue: int = 4096
  max_send_queue: int = 4096
  logging: bool = True
  logging_color: str = 'blue'
  handshake: str = 'portal_handshake'


class ServerSocket:

  def __init__(self, port, name='Server', **kwargs):
    if isinstance(port, str):
      assert '://' not in port, port
      port = int(port.rsplit(':', 1)[-1])
    self.name = name
    self.options = Options(**{**contextlib.context.serverkw, **kwargs})
    self.handshake = self.options.handshake.encode('utf-8')
    if self.options.ipv6:
      self.sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
      self.sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
      self.addr = (self.options.host or '::', port, 0, 0)
    else:
      self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      self.addr = (self.options.host or '0.0.0.0', port)
    self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)  # TODO
    self._log(f'Binding to {self.addr[0]}:{self.addr[1]}')
    self.sock.bind(self.addr)
    self.sock.setblocking(False)
    self.sock.listen(8192)
    self.get_signal, self.set_signal = os.pipe()
    self.sel = selectors.DefaultSelector()
    self.sel.register(self.get_signal, selectors.EVENT_READ, data='signal')
    self.sel.register(self.sock, selectors.EVENT_READ, data=None)
    self._log(f'Listening at {self.addr[0]}:{self.addr[1]}')
    self.conns = {}
    self.recvq = queue.Queue()  # [(addr, bytes)]
    self.reading = True
    self.running = True
    self.error = None
    self.thread = thread.Thread(self._loop, name=f'{name}Loop', start=True)

  @property
  def connections(self):
    return tuple(self.conns.keys())

  def recv(self, timeout=None):
    if self.error:
      raise self.error
    assert self.running
    try:
      return self.recvq.get(block=(timeout != 0), timeout=timeout)
    except queue.Empty:
      raise TimeoutError

  def send(self, addr, *data):
    if self.error:
      raise self.error
    assert self.running
    if self._numsending() > self.options.max_send_queue:
      raise RuntimeError('Too many outgoing messages enqueued')
    maxsize = self.options.max_msg_size
    try:
      self.conns[addr].sendbufs.append(
          buffers.SendBuffer(*data, maxsize=maxsize))
      os.write(self.set_signal, bytes(1))
    except KeyError:
      self._log('Dropping message to disconnected client')

  def shutdown(self):
    self.reading = False

  def close(self, timeout=None):
    self.running = False
    self.thread.join(timeout)
    [conn.sock.close() for conn in self.conns.values()]
    self.sock.close()
    self.sel.close()
    os.close(self.get_signal)
    os.close(self.set_signal)

  def _loop(self):
    writing = False
    try:
      while self.running or self._numsending():
        for key, mask in self.sel.select(timeout=0.2):
          if key.data == 'signal':
            writing = True
            os.read(self.get_signal, 1)
          elif key.data is None and self.reading:
            assert mask & selectors.EVENT_READ
            self._accept(key.fileobj)
          elif mask & selectors.EVENT_READ and self.reading:
            self._recv(key.data)
        if not writing:
          continue
        pending = [conn for conn in self.conns.values() if conn.sendbufs]
        for conn in pending:
          try:
            conn.sendbufs[0].send(conn.sock)
            if conn.sendbufs[0].done():
              conn.sendbufs.popleft()
              if not any(conn.sendbufs for conn in pending):
                writing = False
          except BlockingIOError:
            pass
          except ConnectionResetError:
            # The client is gone but we may have buffered messages left
            # to read, so we keep the socket open until recv() fails.
            pass
    except Exception as e:
      self.error = e

  def _accept(self, sock):
    sock, addr = sock.accept()
    self._log(f'Accepted connection from {addr[0]}:{addr[1]}')
    sock.setblocking(False)
    conn = Connection(sock, addr)
    self.sel.register(sock, selectors.EVENT_READ, data=conn)
    self.conns[addr] = conn

  def _recv(self, conn):
    if not conn.recvbuf:
      conn.recvbuf = buffers.RecvBuffer(maxsize=self.options.max_msg_size)
    try:
      if len(conn.handshake) < len(self.handshake):
        self._handshake(conn)
        return
      else:
        conn.recvbuf.recv(conn.sock)
    except OSError as e:
      # For example:
      # - ConnectionResetError
      # - TimeoutError: [Errno 110] Connection timed out
      self._disconnect(conn, e)
      return
    if not conn.recvbuf.done():
      return
    if self.recvq.qsize() > self.options.max_recv_queue:
      raise RuntimeError('Too many incoming messages enqueued')
    self.recvq.put((conn.addr, conn.recvbuf.result()))
    conn.recvbuf = None

  def _disconnect(self, conn, e):
    detail = f'{type(e).__name__}'
    detail = f'{detail}: {e}' if str(e) else detail
    self._log(f'Closed connection to {conn.addr[0]}:{conn.addr[1]} ({detail})')
    conn = self.conns.pop(conn.addr)
    if conn.sendbufs:
      count = len(conn.sendbufs)
      conn.sendbufs.clear()
      self._log(f'Dropping {count} messages to disconnected client')
    self.sel.unregister(conn.sock)
    conn.sock.close()

  def _numsending(self):
    return sum(len(x.sendbufs) for x in self.conns.values())

  def _handshake(self, conn):
    assert len(conn.handshake) < len(self.handshake)
    conn.handshake += conn.sock.recv(len(self.handshake) - len(conn.handshake))
    if conn.handshake != self.handshake[:len(conn.handshake)]:
      msg = f"Expected handshake '{self.handshake}' got '{conn.handshake}'"
      self._disconnect(conn, ValueError(msg))

  def _log(self, *args, **kwargs):
    if not self.options.logging:
      return
    contextlib.context.print(
        self.name, *args, color=self.options.logging_color)
