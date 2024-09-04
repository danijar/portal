import time

import pytest
import portal


class TestSocket:

  def test_basic(self):
    port = portal.free_port()
    server = portal.ServerSocket(port)
    client = portal.ClientSocket('localhost', port)
    client.connect()
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
    port = portal.free_port()
    server = portal.ServerSocket(port)
    client = portal.ClientSocket('localhost', port)
    client.send(b'foo', b'bar', b'baz')
    addr, data = server.recv()
    assert data == b'foobarbaz'
    server.send(addr, b'ab', b'c')
    assert client.recv() == b'abc'
    server.close()
    client.close()

  def test_multiple_send(self):
    port = portal.free_port()
    server = portal.ServerSocket(port)
    client = portal.ClientSocket('localhost', port)
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
    port = portal.free_port()
    server = portal.ServerSocket(port)
    client = portal.ClientSocket('localhost', port, autoconn=False)
    client.connect()
    server.close()
    with pytest.raises(portal.Disconnected):
      client.recv()
    server = portal.ServerSocket(port)
    with pytest.raises(portal.Disconnected):
      client.recv()
    client.connect()
    time.sleep(0.2)
    server.send(server.connections[0], b'foo')
    assert client.recv() == b'foo'
    server.close()
    time.sleep(0.2)
    with pytest.raises(portal.Disconnected):
      client.send(b'bar')

  @pytest.mark.parametrize('repeat', range(5))
  def test_disconnect_client(self, repeat):
    port = portal.free_port()
    server = portal.ServerSocket(port)
    client = portal.ClientSocket('localhost', port)
    client.send(b'foo')
    assert server.recv()[1] == b'foo'
    assert len(server.connections) == 1
    client.close()
    time.sleep(0.2)
    assert len(server.connections) == 0
    client = portal.ClientSocket('localhost', port)
    time.sleep(0.2)
    assert len(server.connections) == 1
    server.close()
    client.close()

  @pytest.mark.parametrize('repeat', range(3))
  def test_server_dies(self, repeat):
    port = portal.free_port()
    q = portal.context.mp.Queue()

    def server_fn(port, q):
      # Receive exactly one message and then exit wihout close().
      server = portal.ServerSocket(port)
      x = bytes(server.recv()[1])
      q.put(x)
      q.close()
      q.join_thread()

    def client_fn(port, q):
      client = portal.ClientSocket(
          'localhost', port,
          autoconn=False,
          keepalive_after=1,
          keepalive_every=1,
          keepalive_fails=1)
      client.connect()
      try:
        assert client.connected
        while True:
          client.send(b'method')
          time.sleep(0.1)
      except portal.Disconnected:
        q.put(b'bye')
        client.connect(timeout=None)
        client.send(b'hi')
      q.close()
      q.join_thread()
      client.close()

    server = portal.Process(server_fn, port, q, start=True)
    client = portal.Process(client_fn, port, q, start=True)
    assert q.get() == b'method'
    server.join()
    assert q.get() == b'bye'
    server = portal.Process(server_fn, port, q, start=True)
    server.join()
    client.join()
    assert q.get() == b'hi'

  @pytest.mark.parametrize('repeat', range(3))
  def test_twoway(self, repeat, size=1024 ** 2, prefetch=8):

    def server(port):
      server = portal.ServerSocket(port)
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
      client = portal.ClientSocket('localhost', port)
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

    port = portal.free_port()
    portal.run([
        portal.Process(server, port),
        portal.Process(client, port),
    ])

  @pytest.mark.parametrize('repeat', range(3))
  def test_shutdown(self, repeat):

   def server(port):
     server = portal.ServerSocket(port)
     addr, data = server.recv()
     assert data == b'foo'
     large_result = bytes(1024 ** 2)
     server.send(addr, large_result)
     server.close()

   def client(port):
     client = portal.ClientSocket('localhost', port)
     client.send(b'foo')
     assert client.recv() == bytes(1024 ** 2)
     client.close()

   port = portal.free_port()
   portal.run([
       portal.Process(server, port),
       portal.Process(client, port),
   ])
