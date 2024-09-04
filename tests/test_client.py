import threading
import time

import pytest
import zerofun


SERVERS = [
    zerofun.Server,
    zerofun.BatchServer,
]


class TestClient:

  def test_none_result(self):
    port = zerofun.free_port()
    server = zerofun.Server(port)
    def fn():
      pass
    server.bind('fn', fn)
    server.start(block=False)
    client = zerofun.Client('localhost', port)
    assert client.fn().result() is None
    client.close()
    server.close()

  def test_manual_connect(self):
    port = zerofun.free_port()
    client = zerofun.Client('localhost', port, autoconn=False)
    assert not client.connected
    server = zerofun.Server(port)
    server.bind('fn', lambda x: x)
    server.start(block=False)
    client.connect()
    assert client.connected
    assert client.fn(12).result() == 12
    client.close()
    server.close()

  @pytest.mark.parametrize('repeat', range(10))
  def test_manual_reconnect(self, repeat):
    port = zerofun.free_port()
    server = zerofun.Server(port)
    server.bind('fn', lambda x: x)
    server.start(block=False)
    client = zerofun.Client('localhost', port, autoconn=False)
    client.connect()
    assert client.fn(1).result() == 1
    server.close()
    with pytest.raises(zerofun.Disconnected):
      client.fn(2).result()
    server = zerofun.Server(port)
    server.bind('fn', lambda x: x)
    server.start(block=False)
    client.connect()
    assert client.fn(3).result() == 3
    client.close()
    server.close()

  def test_connect_before_server(self):
    port = zerofun.free_port()
    results = []

    def client():
      client = zerofun.Client('localhost', port)
      results.append(client.fn(12).result())
      client.close()

    thread = zerofun.Thread(client, start=True)
    time.sleep(0.2)
    server = zerofun.Server(port)
    server.bind('fn', lambda x: x)
    server.start(block=False)
    thread.join()
    server.close()
    assert results[0] == 12

  def test_future_order(self):
    port = zerofun.free_port()
    server = zerofun.Server(port)
    server.bind('fn', lambda x: x)
    server.start(block=False)
    client = zerofun.Client('localhost', port)
    future1 = client.fn(1)
    future2 = client.fn(2)
    future3 = client.fn(3)
    assert future2.result() == 2
    assert future1.result() == 1
    assert future3.result() == 3
    server.close()
    client.close()

  def test_future_timeout(self):
    port = zerofun.free_port()
    server = zerofun.Server(port)
    def fn(x):
      time.sleep(0.1)
      return x
    server.bind('fn', fn)
    server.start(block=False)
    client = zerofun.Client('localhost', port)
    future = client.fn(42)
    with pytest.raises(TimeoutError):
      future.result(timeout=0)
    with pytest.raises(TimeoutError):
      future.result(timeout=0.01)
    with pytest.raises(TimeoutError):
      future.result(timeout=0)
    assert future.result(timeout=0.2) == 42
    client.close()
    server.close()

  def test_maxinflight(self):
    port = zerofun.free_port()
    server = zerofun.Server(port)
    parallel = [0]
    lock = threading.Lock()

    def fn(data):
      with lock:
        parallel[0] += 1
        assert parallel[0] <= 2
      time.sleep(0.1)
      with lock:
        parallel[0] -= 1
      return data
    server.bind('fn', fn, workers=4)
    server.start(block=False)

    client = zerofun.Client('localhost', port, maxinflight=2)
    futures = [client.fn(i) for i in range(16)]
    results = [x.result() for x in futures]
    assert results == list(range(16))
    client.close()
    server.close()

  @pytest.mark.parametrize('repeat', range(5))
  def test_future_cleanup(self, repeat):
    port = zerofun.free_port()
    server = zerofun.Server(port)
    server.bind('fn', lambda x: x)
    server.start(block=False)
    client = zerofun.Client('localhost', port)
    client.fn(1)
    client.fn(2)
    # Wait for the server to respond to the first two requests, so that all
    # futures are inside the client by the time we block on the third future.
    time.sleep(0.1)
    future3 = client.fn(3)
    assert len(client.futures) == 1
    assert future3.result() == 3
    del future3
    assert len(client.futures) == 0
    client.close()
    server.close()

  @pytest.mark.parametrize('repeat', range(3))
  def test_future_cleanup_errors(self, repeat):
    port = zerofun.free_port()
    server = zerofun.Server(port, errors=False)
    def fn(x):
      if x == 2:
        raise ValueError(x)
      return x
    server.bind('fn', fn)
    server.start(block=False)
    client = zerofun.Client('localhost', port, maxinflight=1)
    client.fn(1)
    client.fn(2)
    time.sleep(0.2)
    with pytest.raises(RuntimeError):
      client.fn(3)
    assert client.fn(3).result() == 3
    client.close()
    server.close()

  @pytest.mark.parametrize('repeat', range(5))
  def test_client_threadsafe(self, repeat, users=16):
    port = zerofun.free_port()
    server = zerofun.Server(port)
    server.bind('fn', lambda x: x, workers=4)
    server.start(block=False)
    client = zerofun.Client('localhost', port, maxinflight=8)
    barrier = threading.Barrier(users)

    def user():
      barrier.wait()
      for x in range(4):
        assert client.fn(x).result() == x

    zerofun.run([zerofun.Thread(user) for _ in range(users)])
    server.close()
    client.close()

  @pytest.mark.parametrize('repeat', range(5))
  @pytest.mark.parametrize('Server', SERVERS)
  def test_maxinflight_disconnect(self, repeat, Server):
    port = zerofun.free_port()
    a = threading.Barrier(3)
    b = threading.Barrier(2)
    c = threading.Barrier(2)

    def server():
      def fn(x):
        if x in (1, 2):
          a.wait()
        time.sleep(0.1)
        return x
      server = Server(port)
      server.bind('fn', fn, workers=2)
      server.start(block=False)
      # Close server after receiving two requests but before responding to any
      # of them, to ensure the client is waiting for maxinflight during the
      # disconnect.
      a.wait()
      server.close()
      b.wait()
      server = Server(port)
      server.bind('fn', fn)
      server.start(block=False)
      c.wait()
      server.close()

    def client():
      client = zerofun.Client(
          'localhost', port, maxinflight=2, autoconn=False)
      client.connect()
      start = time.time()
      future1 = client.fn(1)
      time.sleep(0.1)
      future2 = client.fn(2)
      assert time.time() - start < 0.2
      try:
        client.fn(3)
        client.fn(3)
        client.fn(3)
        assert False
      except zerofun.Disconnected:
        assert True
      assert future1.result() == 1
      assert future2.result() == 2
      b.wait()
      client.connect()
      assert client.fn(4).result() == 4
      c.wait()
      client.close()

    zerofun.run([
      zerofun.Thread(server),
      zerofun.Thread(client),
    ])

  @pytest.mark.parametrize('repeat', range(10))
  @pytest.mark.parametrize('Server', SERVERS)
  def test_server_drops_autoconn(self, repeat, Server):  # TODO: Sometimes hangs
    port = zerofun.free_port()
    a = threading.Barrier(2)
    b = threading.Barrier(2)

    def server():
      server = Server(port)
      server.bind('fn', lambda x: x)
      server.start(block=False)
      a.wait()
      server.close()
      stats = server.stats()
      assert stats['numrecv'] < 3
      assert stats['numsend'] == stats['numrecv']
      server = Server(port)
      server.bind('fn', lambda x: x)
      server.start(block=False)
      print('111')
      b.wait()
      print('222')
      server.close()

    def client():
      client = zerofun.Client(
          'localhost', port, maxinflight=1, autoconn=True, resend=True)
      assert client.fn(1).result() == 1
      a.wait()
      assert client.fn(2).result() == 2
      time.sleep(0.1)
      assert client.fn(3).result() == 3
      print('AAA')
      b.wait()
      print('BBB')
      client.close()

    zerofun.run([
        zerofun.Thread(server),
        zerofun.Thread(client),
    ])

  @pytest.mark.parametrize('repeat', range(3))
  @pytest.mark.parametrize('Server', SERVERS)
  def test_server_drops_manual(self, repeat, Server):
    port = zerofun.free_port()
    a = threading.Barrier(2)
    b = threading.Barrier(2)

    def server():
      server = Server(port)
      server.bind('fn', lambda x: x)
      server.start(block=False)
      a.wait()
      server.close()
      stats = server.stats()
      assert stats['numrecv'] == 1
      assert stats['numsend'] == stats['numrecv']
      server = Server(port)
      server.bind('fn', lambda x: x)
      server.start(block=False)
      b.wait()
      server.close()

    def client():
      client = zerofun.Client(
          'localhost', port, maxinflight=1, autoconn=False)
      client.connect()
      assert client.fn(1).result() == 1
      a.wait()
      time.sleep(0.1)
      with pytest.raises(zerofun.Disconnected):
        client.fn(3).result()
      client.connect()
      assert client.fn(3).result() == 3
      b.wait()
      client.close()

    zerofun.run([
        zerofun.Thread(server),
        zerofun.Thread(client),
    ])
