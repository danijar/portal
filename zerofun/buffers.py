import collections
import os
import weakref

import numpy as np


class SendBuffer:

  def __init__(self, *buffers, maxsize=None):
    for buffer in buffers:
      assert isinstance(buffer, (bytes, bytearray, memoryview)), type(buffer)
      assert not isinstance(buffer, memoryview) or buffer.c_contiguous
    buffers = tuple(
         x.cast('c') if isinstance(x, memoryview) else x for x in buffers)
    length = sum(len(x) for x in buffers)
    assert all(len(x) for x in buffers)
    assert 1 <= length, length
    assert not maxsize or length <= length, (length, maxsize)
    lenbuf = length.to_bytes(4, 'little', signed=False)
    self.buffers = [lenbuf, *buffers]
    self.remaining = collections.deque(self.buffers)
    self.pos = 0

  def __repr__(self):
    lens = [len(x) for x in self.buffers]
    left = [len(x) for x in self.remaining]
    return f'SendBuffer(pos={self.pos}, lengths={lens} remaining={left})'

  def reset(self):
    self.remaining = collections.deque(self.buffers)
    self.pos = 0

  def send(self, sock):
    first, *others = self.remaining
    assert self.pos < len(first)
    # The writev() call blocks but seems to be slightly faster than sendmsg().
    size = os.writev(sock.fileno(), [memoryview(first)[self.pos:], *others])
    # size = sock.sendmsg(
    #     [memoryview(first)[self.pos:], *others], (), socket.MSG_DONTWAIT)
    if size == 0:
      raise ConnectionResetError
    assert 0 <= size, size
    self.pos += max(0, size)
    while self.remaining and self.pos >= len(self.remaining[0]):
      self.pos -= len(self.remaining.popleft())
    return size

  def done(self):
    return not self.remaining


class RecvBuffer:

  def __init__(self, maxsize):
    self.maxsize = maxsize
    self.lenbuf = bytearray(4)
    self.buffer = None
    self.pos = 0

  def __repr__(self):
    length = self.buffer and len(self.buffer)
    return f'RecvBuffer(pos={self.pos}, length={length})'

  def recv(self, sock):
    if self.buffer is None:
      size = sock.recv_into(memoryview(self.lenbuf)[self.pos:])
      self.pos += max(0, size)
      if self.pos == 4:
        length = int.from_bytes(self.lenbuf, 'little', signed=False)
        assert 1 <= length <= self.maxsize, (1, length, self.maxsize)
        # We use Numpy to allocate uninitialized memory because Python's
        # `bytearray(length)` zero initializes which is slow. This also means
        # the buffer cannot be pickled accidentally unless explicitly converted
        # to a `bytes()` object, which is a nice bonus for preventing
        # performance bugs in user code.
        arr = np.empty(length, np.uint8)
        self.buffer = memoryview(arr.data)
        weakref.finalize(self.buffer, lambda arr=arr: arr)
        self.pos = 0
    else:
      size = sock.recv_into(self.buffer[self.pos:])
      self.pos += max(0, size)
      assert 0 <= self.pos <= len(self.buffer), (0, self.pos, len(self.buffer))
    if size == 0:
      raise ConnectionResetError
    return size

  def done(self):
    return self.buffer and self.pos == len(self.buffer)

  def result(self):
    return self.buffer
