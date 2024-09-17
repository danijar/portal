import threading
import time

import pytest
import portal


SERVERS = [
    portal.Server,
    portal.BatchServer,
]


class TestClient:

  def test_none_result(self):
    port = portal.free_port()
    server = portal.Server(port)
    def fn():
      pass
    server.bind('fn', fn)
    server.start(block=False)
    client = portal.Client(port)
    assert client.fn().result() is None
    client.close()
    server.close()

  def test_manual_connect(self):
    port = portal.free_port()
    client = portal.Client(port, autoconn=False)
    assert not client.connected
    server = portal.Server(port)
    server.bind('fn', lambda x: x)
    server.start(block=False)
    client.connect()
    assert client.connected
    assert client.fn(12).result() == 12
    client.close()
    server.close()

  @pytest.mark.parametrize('repeat', range(10))
  def test_manual_reconnect(self, repeat):
    port = portal.free_port()
    server = portal.Server(port)
    server.bind('fn', lambda x: x)
    server.start(block=False)
    client = portal.Client(port, autoconn=False)
    client.connect()
    assert client.fn(1).result() == 1
    server.close()

    assert len(client.futures) == 0
    assert len(client.errors) == 0
    try:
      future = client.fn(2)
      try:
        future.result()
        assert False
      except portal.Disconnected:
        assert True
    except portal.Disconnected:
      time.sleep(1)
      future = None
    assert len(client.futures) == 0
    assert len(client.errors) == 0

    server = portal.Server(port)
    server.bind('fn', lambda x: x)
    server.start(block=False)
    client.connect()
    assert client.fn(3).result() == 3
    client.close()
    server.close()

  def test_connect_before_server(self):
    port = portal.free_port()
    results = []

    def client():
      client = portal.Client(port)
      results.append(client.fn(12).result())
      client.close()

    thread = portal.Thread(client, start=True)
    time.sleep(0.2)
    server = portal.Server(port)
    server.bind('fn', lambda x: x)
    server.start(block=False)
    thread.join()
    server.close()
    assert results[0] == 12

  def test_future_order(self):
    port = portal.free_port()
    server = portal.Server(port)
    server.bind('fn', lambda x: x)
    server.start(block=False)
    client = portal.Client(port)
    future1 = client.fn(1)
    future2 = client.fn(2)
    future3 = client.fn(3)
    assert future2.result() == 2
    assert future1.result() == 1
    assert future3.result() == 3
    server.close()
    client.close()

  def test_future_timeout(self):
    port = portal.free_port()
    server = portal.Server(port)
    def fn(x):
      time.sleep(0.1)
      return x
    server.bind('fn', fn)
    server.start(block=False)
    client = portal.Client(port)
    future = client.fn(42)
    with pytest.raises(TimeoutError):
      future.result(timeout=0)
    with pytest.raises(TimeoutError):
      future.result(timeout=0.01)
    with pytest.raises(TimeoutError):
      future.result(timeout=0)
    assert future.result(timeout=1) == 42
    client.close()
    server.close()

  def test_maxinflight(self):
    port = portal.free_port()
    server = portal.Server(port)
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

    client = portal.Client(port, maxinflight=2)
    futures = [client.fn(i) for i in range(16)]
    results = [x.result() for x in futures]
    assert results == list(range(16))
    client.close()
    server.close()

  @pytest.mark.parametrize('repeat', range(5))
  def test_future_cleanup(self, repeat):
    port = portal.free_port()
    server = portal.Server(port)
    server.bind('fn', lambda x: x)
    server.start(block=False)
    client = portal.Client(port, maxinflight=1)
    client.fn(1)
    client.fn(2)
    future3 = client.fn(3)
    assert future3.result() == 3
    del future3
    assert len(client.futures) == 0
    client.close()
    server.close()

  @pytest.mark.parametrize('repeat', range(3))
  def test_future_cleanup_errors(self, repeat):
    port = portal.free_port()
    server = portal.Server(port, errors=False)
    def fn(x):
      if x == 2:
        raise ValueError(x)
      return x
    server.bind('fn', fn)
    server.start(block=False)
    client = portal.Client(port, maxinflight=1)
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
    port = portal.free_port()
    server = portal.Server(port)
    server.bind('fn', lambda x: x, workers=4)
    server.start(block=False)
    client = portal.Client(port, maxinflight=8)
    barrier = threading.Barrier(users)

    def user():
      barrier.wait()
      for x in range(4):
        assert client.fn(x).result() == x

    portal.run([portal.Thread(user) for _ in range(users)])
    server.close()
    client.close()

  @pytest.mark.parametrize('repeat', range(5))
  @pytest.mark.parametrize('Server', SERVERS)
  def test_maxinflight_disconnect(self, repeat, Server):
    port = portal.free_port()
    a = threading.Barrier(2)
    b = threading.Barrier(2)

    def server():
      def fn(x):
        if x == 1:
          a.wait()
        time.sleep(0.1)
        return x
      server = Server(port)
      server.bind('fn', fn, workers=2)
      server.start(block=False)
      a.wait()
      server.close()
      server = Server(port)
      server.bind('fn', fn)
      server.start(block=False)
      b.wait()
      server.close()

    def client():
      client = portal.Client(port, maxinflight=2)
      futures = [client.fn(x) for x in range(5)]
      results = [x.result() for x in futures]
      assert results == list(range(5))
      b.wait()
      client.close()

    portal.run([
      portal.Thread(server),
      portal.Thread(client),
    ])

  @pytest.mark.parametrize('repeat', range(10))
  @pytest.mark.parametrize('Server', SERVERS)
  def test_server_drops_autoconn(self, repeat, Server):
    port = portal.free_port()
    a = threading.Barrier(2)
    b = threading.Barrier(2)
    c = threading.Barrier(2)

    def server():
      server = Server(port)
      server.bind('fn', lambda x: x)
      server.start(block=False)
      a.wait()
      server.close()
      stats = server.stats()
      assert stats['numrecv'] == 1
      assert stats['numsend'] == stats['numrecv']
      b.wait()
      server = Server(port)
      server.bind('fn', lambda x: x)
      server.start(block=False)
      c.wait()
      server.close()

    def client():
      client = portal.Client(port, maxinflight=1, autoconn=True)
      assert client.fn(1).result() == 1
      a.wait()
      b.wait()
      assert client.fn(2).result() == 2
      time.sleep(0.1)
      assert client.fn(3).result() == 3
      c.wait()
      client.close()

    portal.run([
        portal.Thread(server),
        portal.Thread(client),
    ])

  @pytest.mark.parametrize('repeat', range(3))
  @pytest.mark.parametrize('Server', SERVERS)
  def test_server_drops_manual(self, repeat, Server):
    port = portal.free_port()
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
      client = portal.Client(port, maxinflight=1, autoconn=False)
      client.connect()
      assert client.fn(1).result() == 1
      a.wait()
      time.sleep(0.1)
      with pytest.raises(portal.Disconnected):
        client.fn(3).result()
      client.connect()
      assert client.fn(3).result() == 3
      b.wait()
      client.close()

    portal.run([
        portal.Thread(server),
        portal.Thread(client),
    ])

  @pytest.mark.parametrize('ipv6', (False, True))
  @pytest.mark.parametrize('fmt,typ', (
      ('{port}', int),
      ('{port}', str),
      (':{port}', str),
      ('localhost:{port}', str),
      ('{localhost}:{port}', str),
  ))
  def test_address_formats(self, fmt, typ, ipv6):
    port = portal.free_port()
    server = portal.Server(port, ipv6=ipv6)
    server.bind('fn', lambda x: x)
    server.start(block=False)
    localhost = '::1' if ipv6 else '127.0.0.1'
    addr = typ(fmt.format(port=port, localhost=localhost))
    client = portal.Client(addr, ipv6=ipv6)
    assert client.fn(42).result() == 42
    client.close()
    server.close()

  def test_resolver(self):
    portnum = portal.free_port()

    def client(portnum):
      def resolver(host, portstr):
        assert portstr == 'name'
        return host, portnum
      portal.setup(resolver=resolver)
      client = portal.Client('localhost:name')
      assert client.fn(42).result() == 42

    server = portal.Server(portnum)
    server.bind('fn', lambda x: x)
    server.start(block=False)
    portal.Process(client, portnum, start=True).join()
    server.close()
