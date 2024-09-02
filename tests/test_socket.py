import sys
import time

import pytest
import zerofun


class TestSocket:

  def test_basic(self):
    port = zerofun.free_port()
    server = zerofun.ServerSocket(port)
    client = zerofun.ClientSocket('localhost', port, connect=True)
    assert client.connected
    client.send(b'foo')
    addr, data = server.recv()
    assert addr[0] == '127.0.0.1'
    assert data == b'foo'
    server.send(addr, b'bar')
    assert client.recv() == b'bar'
    server.close()
    client.close()

  def test_multi_buffer(self):
    port = zerofun.free_port()
    server = zerofun.ServerSocket(port)
    client = zerofun.ClientSocket('localhost', port, connect=True)
    client.send(b'foo', b'bar', b'baz')
    addr, data = server.recv()
    assert data == b'foobarbaz'
    server.send(addr, b'ab', b'c')
    assert client.recv() == b'abc'
    server.close()
    client.close()

  def test_multiple_send(self):
    port = zerofun.free_port()
    server = zerofun.ServerSocket(port)
    client = zerofun.ClientSocket('localhost', port, connect=True)
    client.send(b'foo')
    client.send(b'ba', b'r')
    client.send(b'baz')
    assert server.recv()[1] == b'foo'
    assert server.recv()[1] == b'bar'
    assert server.recv()[1] == b'baz'
    assert len(server.connections) == 1
    addr = server.connections[0]
    server.send(addr, b'baz')
    server.send(addr, b'ba', b'r')
    server.send(addr, b'foo')
    assert client.recv() == b'baz'
    assert client.recv() == b'bar'
    assert client.recv() == b'foo'
    server.close()
    client.close()

  @pytest.mark.parametrize('repeat', range(3))
  def test_disconnect_server(self, repeat):
    port = zerofun.free_port()
    server = zerofun.ServerSocket(port)
    client = zerofun.ClientSocket(
        'localhost', port, connect=True, reconnect=False)
    server.close()
    with pytest.raises(zerofun.Disconnected):
      client.recv()
    server = zerofun.ServerSocket(port)
    with pytest.raises(zerofun.Disconnected):
      client.recv()
    client.connect()
    time.sleep(0.2)
    server.send(server.connections[0], b'foo')
    assert client.recv() == b'foo'
    server.close()
    time.sleep(0.2)
    with pytest.raises(zerofun.Disconnected):
      client.send(b'bar')

  @pytest.mark.parametrize('repeat', range(3))
  def test_disconnect_client(self, repeat):
    port = zerofun.free_port()
    server = zerofun.ServerSocket(port)
    client = zerofun.ClientSocket('localhost', port, connect=True)
    client.send(b'foo')
    assert server.recv()[1] == b'foo'
    assert len(server.connections) == 1
    client.close()
    time.sleep(0.2)
    assert len(server.connections) == 0
    client = zerofun.ClientSocket('localhost', port, connect=True)
    time.sleep(0.2)
    assert len(server.connections) == 1
    server.close()
    client.close()

  @pytest.mark.skipif(sys.platform == 'darwin', reason='firewall popups')
  @pytest.mark.parametrize('repeat', range(3))
  def test_server_dies(self, repeat):
    port = zerofun.free_port()
    q = zerofun.context.mp.Queue()

    def server_fn(port, q):
      # Receive exactly one message and then exit wihout close().
      server = zerofun.ServerSocket(port)
      q.put(bytes(server.recv()[1]))

    def client_fn(port, q):
      client = zerofun.ClientSocket(
          'localhost', port,
          connect=True, reconnect=False,
          keepalive_after=1,
          keepalive_every=1,
          keepalive_fails=1)
      try:
        while True:
          client.send(b'method')
          time.sleep(0.1)
      except zerofun.Disconnected:
        q.put(b'bye')
        client.connect(timeout=None)
        client.send(b'hi')
      client.close()

    server = zerofun.Process(server_fn, port, q, start=True)
    client = zerofun.Process(client_fn, port, q, start=True)
    assert q.get() == b'method'
    server.join()
    assert q.get() == b'bye'
    server = zerofun.Process(server_fn, port, q, start=True)
    server.join()
    client.join()
    assert q.get() == b'hi'

  @pytest.mark.parametrize('repeat', range(3))
  def test_twoway(self, repeat, size=1024 ** 2, prefetch=8):

    def server(port):
      server = zerofun.ServerSocket(port)
      expected = bytearray(size)
      while True:
        addr, data = server.recv()
        if data == b'exit':
          server.send(addr, b'exit')
          break
        server.send(addr, data)
        assert len(data) == size
        assert data == expected
      server.close()

    def client(port):
      data = bytearray(size)
      client = zerofun.ClientSocket('localhost', port)
      for _ in range(prefetch):
        client.send(data)
      for _ in range(100):
        client.send(data)
        result = client.recv()
        assert len(result) == size
      client.send(b'exit')
      while client.recv() != b'exit':
        pass
      client.close()

    port = zerofun.free_port()
    zerofun.run([
        zerofun.Process(server, port),
        zerofun.Process(client, port),
    ])

  @pytest.mark.parametrize('repeat', range(3))
  def test_shutdown(self, repeat):

   def server(port):
     server = zerofun.ServerSocket(port)
     addr, data = server.recv()
     assert data == b'foo'
     large_result = bytes(1024 ** 2)
     server.send(addr, large_result)
     server.close()

   def client(port):
     client = zerofun.ClientSocket('localhost', port)
     client.send(b'foo')
     assert client.recv() == bytes(1024 ** 2)
     client.close()

   port = zerofun.free_port()
   zerofun.run([
       zerofun.Process(server, port),
       zerofun.Process(client, port),
   ])
