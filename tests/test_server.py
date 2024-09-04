import os
import threading
import time

import numpy as np
import pytest
import portal


SERVERS = [
    portal.Server,
    portal.BatchServer,
]


class TestServer:

  @pytest.mark.parametrize('Server', SERVERS)
  def test_basic(self, Server):
    port = portal.free_port()
    server = Server(port)
    def fn(x):
      assert x == 42
      return 2 * x
    server.bind('fn', fn)
    server.start(block=False)
    client = portal.Client('localhost', port)
    future = client.call('fn', 42)
    assert future.result() == 84
    client.close()
    server.close()

  @pytest.mark.parametrize('Server', SERVERS)
  def test_server_scope(self, Server):
    def fn(data):
      assert data == {'foo': np.array(1)}
      return {'foo': 2 * data['foo']}
    port = portal.free_port()
    server = Server(port)
    server.bind('fn', fn)
    with server:
      client = portal.Client('localhost', port)
      future = client.fn({'foo': np.array(1)})
      result = future.result()
      assert result['foo'] == 2
      client.close()

  @pytest.mark.parametrize('Server', SERVERS)
  def test_multiple_clients(self, Server):
    port = portal.free_port()
    server = Server(port)
    server.bind('fn', lambda data: data)
    server.start(block=False)
    clients = [portal.Client('localhost', port) for _ in range(10)]
    futures = [client.fn(i) for i, client in enumerate(clients)]
    results = [future.result() for future in futures]
    assert results == list(range(10))
    [x.close() for x in clients]
    server.close()

  @pytest.mark.parametrize('Server', SERVERS)
  def test_multiple_methods(self, Server):
    port = portal.free_port()
    server = Server(port)
    server.bind('add', lambda x, y: x + y)
    server.bind('sub', lambda x, y: x - y)
    with server:
      client = portal.Client('localhost', port)
      assert client.add(3, 5).result() == 8
      assert client.sub(3, 5).result() == -2
      client.close()

  @pytest.mark.parametrize('Server', SERVERS)
  def test_unknown_method(self, Server):
    port = portal.free_port()
    server = Server(port, errors=False)
    server.bind('foo', lambda x: x)
    server.start(block=False)
    client = portal.Client('localhost', port)
    future = client.bar(42)
    try:
      future.result()
      assert False
    except RuntimeError as e:
      assert e.args == ('Unknown method bar',)
      assert True
    client.close()
    server.close()

  @pytest.mark.parametrize('Server', SERVERS)
  def test_server_errors(self, Server):
    port = portal.free_port()

    def server(port):
      server = Server(port, errors=True)
      def fn(x):
        if x == 2:
          raise ValueError(x)
        return x
      server.bind('fn', fn)
      server.start(block=True)

    # For the BatchServer, there are resource leak warnings on crash.
    os.environ['PYTHONWARNINGS'] = 'ignore'
    server = portal.Process(server, port, start=True)
    del os.environ['PYTHONWARNINGS']

    client = portal.Client('localhost', port)
    assert client.fn(1).result() == 1
    assert server.running
    with pytest.raises(RuntimeError):
      client.fn(2).result()

    client.close()
    server.join()
    assert server.exitcode not in (0, None)

  @pytest.mark.parametrize('repeat', range(3))
  @pytest.mark.parametrize('Server', SERVERS)
  def test_postfn_ordered(self, repeat, Server):
    completed = []
    logged = []

    def workfn(x):
      if x in (0, 2, 6):
        time.sleep(0.1)
      completed.append(x)
      return x, x

    def postfn(x):
      logged.append(x)

    port = portal.free_port()
    server = Server(port, workers=4)
    server.bind('fn', workfn, postfn)
    server.start(block=False)
    client = portal.Client('localhost', port)
    futures = [client.fn(x) for x in range(10)]
    results = [x.result() for x in futures]
    server.close()
    client.close()
    assert results == list(range(10))
    assert completed != list(range(10))
    assert logged == list(range(10))

  @pytest.mark.parametrize('repeat', range(5))
  @pytest.mark.parametrize('Server', SERVERS)
  @pytest.mark.parametrize('workers', (1, 4))
  def test_postfn_no_backlog(self, repeat, Server, workers):
    port = portal.free_port()
    lock = threading.Lock()
    work_calls = [0]
    done_calls = [0]
    def workfn(x):
      with lock:
        work_calls[0] += 1
        print(work_calls[0], done_calls[0])
        assert work_calls[0] <= done_calls[0] + workers + 1
      return x, x
    def postfn(x):
      with lock:
        done_calls[0] += 1
      time.sleep(0.01)
    server = Server(port, workers=workers)
    server.bind('fn', workfn, postfn)
    server.start(block=False)
    client = portal.Client('localhost', port)
    futures = [client.fn(i) for i in range(20)]
    [future.result() for future in futures]
    client.close()
    server.close()

  @pytest.mark.parametrize('repeat', range(3))
  @pytest.mark.parametrize('Server', SERVERS)
  def test_shared_pool(self, repeat, Server):
    def slow(x):
      time.sleep(0.2)
      return x
    def fast(x):
      return x
    port = portal.free_port()
    server = Server(port, workers=1)
    server.bind('slow', slow)
    server.bind('fast', fast)
    server.start(block=False)
    client = portal.Client('localhost', port)
    slow_future = client.slow(0)
    fast_future = client.fast(0)
    assert not slow_future.done()
    # The slow request is processed first, so this will wait until both
    # requests are done.
    fast_future.result()
    assert slow_future.wait(0.01)
    server.close()
    client.close()

  @pytest.mark.parametrize('repeat', range(3))
  @pytest.mark.parametrize('Server', SERVERS)
  def test_separate_pools(self, repeat, Server):
    def slow(x):
      time.sleep(0.1)
      return x
    def fast(x):
      return x
    port = portal.free_port()
    server = Server(port)
    server.bind('slow', slow, workers=1)
    server.bind('fast', fast, workers=1)
    server.start(block=False)
    client = portal.Client('localhost', port)
    slow_future = client.slow(0)
    fast_future = client.fast(0)
    # Both requests are processed in parallel, so the fast request returns
    # before the slow request is done.
    fast_future.result()
    assert not slow_future.wait(0.01)
    server.close()
    client.close()

  @pytest.mark.parametrize('Server', SERVERS)
  @pytest.mark.parametrize('workers', (1, 10))
  def test_proxy(self, Server, workers):
    inner_port = portal.free_port()
    outer_port = portal.free_port()

    server = Server(inner_port, 'InnerServer')
    server.bind('fn', lambda x: 2 * x)
    server.start(block=False)

    kwargs = dict(name='ProxyClient', maxinflight=4)
    proxy_client = portal.Client('localhost', inner_port, **kwargs)
    proxy_server = Server(outer_port, 'ProxyServer', workers=workers)
    proxy_server.bind('fn2', lambda x: proxy_client.fn(x).result())
    proxy_server.start(block=False)

    client = portal.Client('localhost', outer_port, 'OuterClient')
    futures = [client.fn2(x) for x in range(20)]
    results = [future.result() for future in futures]
    assert results == list(range(0, 40, 2))

    proxy_server.close()
    proxy_client.close()
    server.close()
    client.close()

  @pytest.mark.parametrize('Server', SERVERS)
  def test_sharray(self, Server):
    done = portal.context.mp.Event()

    def server(port, done):
      server = Server(port)
      def fn(data):
        assert data.array.shape == (3, 2)
        assert data.array.dtype == np.float32
        return data
      server.bind('fn', fn)
      server.start(block=False)
      done.wait()
      server.close()

    def client(port):
      data = portal.SharedArray((3, 2), np.float32)
      data.array[:] = np.arange(6, dtype=np.float32).reshape(3, 2)
      client = portal.Client('localhost', port)
      result = client.call('fn', data).result()
      assert result.name == data.name
      assert result, data
      client.close()

    port = portal.free_port()
    client = portal.Process(client, port, start=True)
    server = portal.Process(server, port, done, start=True)
    client.join()
    done.set()
    server.join()

  @pytest.mark.parametrize('repeat', range(3))
  @pytest.mark.parametrize('Server', SERVERS)
  def test_client_drops(self, repeat, Server):
    barrier = threading.Barrier(2)

    def fn(x):
      if x == 1:
        barrier.wait()
        time.sleep(0.2)
      return x

    port = portal.free_port()
    server = Server(port)
    server.bind('fn', fn)
    server.start(block=False)

    client = portal.Client('localhost', port)
    client.fn(1)
    barrier.wait()
    client.close()
    stats = server.stats()
    assert stats['numrecv'] == 1
    assert stats['numsend'] == 0

    client = portal.Client('localhost', port)
    assert client.fn(2).result() == 2
    client.close()
    server.close()
