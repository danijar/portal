import os


class SendBuffer:

  def __init__(self, *buffers, maxsize=None):
    self.length = sum(len(x) for x in buffers)
    assert not maxsize or self.length <= self.length, (self.length, maxsize)
    length = self.length.to_bytes(4, 'little', signed=False)
    self.buffers = [length, *buffers]
    self.pos = 0

  def send(self, sock):
    size = os.writev(sock.fileno(), self.buffers)
    self.pos += size
    while self.buffers and self.pos >= len(self.buffers[0]):
      self.pos -= len(self.buffers.pop(0))
    return size

  def done(self):
    return self.pos == self.length


class RecvBuffer:

  def __init__(self, maxsize):
    self.maxsize = maxsize
    self.length = bytearray(4)
    self.buffer = None
    self.pos = 0

  def recv(self, sock):
    if self.buffer is None:
      size = sock.recv_into(self.length[self.pos:])
      self.pos += size
      if self.pos == 4:
        length = int.from_bytes(self.length, 'little', signed=False)

        # assert 0 < length <= self.maxsize, (length, self.maxsize)
        assert length <= self.maxsize, (length, self.maxsize)  # TODO

        self.buffer = bytearray(length)
        self.pos = 0
    else:
      size = sock.recv_into(self.buffer[self.pos:])
      self.pos += size
    return size

  def done(self):
    return self.buffer and self.pos == len(self.buffer)

  def result(self):
    return self.buffer
