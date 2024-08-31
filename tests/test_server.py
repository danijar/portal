import threading
import time

import numpy as np
import pytest
import zerofun


# TODO: Test client disconnecting while server is computing.

SERVERS = [
    zerofun.Server,
]


class TestServer:

  @pytest.mark.parametrize('Server', SERVERS)
  def test_basic(self, Server):
    port = zerofun.free_port()
    server = Server(port)
    def fn(x):
      assert x == 42
      return 2 * x
    server.bind('fn', fn)
    server.start(block=False)
    client = zerofun.Client('localhost', port)
    future = client.call('fn', 42)
    assert future.result() == 84
    client.close()
    server.close()

  @pytest.mark.parametrize('Server', SERVERS)
  def test_none_result(self, Server):
    port = zerofun.free_port()
    server = Server(port)
    def fn():
      pass
    server.bind('fn', fn)
    server.start(block=False)
    client = zerofun.Client('localhost', port)
    assert client.fn().result() is None
    client.close()
    server.close()

  @pytest.mark.parametrize('Server', SERVERS)
  def test_manual_connect(self, Server):
    port = zerofun.free_port()
    client = zerofun.Client('localhost', port, connect=False, reconnect=False)
    result = client.connect(timeout=0.01)
    assert result is False
    assert not client.connected
    server = Server(port)
    server.bind('fn', lambda x: x)
    server.start(block=False)
    client.connect()
    assert client.connected
    assert client.fn(12).result() == 12
    client.close()
    server.close()

  @pytest.mark.parametrize('Server', SERVERS)
  def test_manual_reconnect(self, Server):
    port = zerofun.free_port()
    server = Server(port)
    server.bind('fn', lambda x: x)
    server.start(block=False)
    client = zerofun.Client('localhost', port, reconnect=False)
    assert client.fn(1).result() == 1
    server.close()
    with pytest.raises(zerofun.Disconnected):
      client.fn(2).result()
    server = Server(port)
    server.bind('fn', lambda x: x)
    server.start(block=False)
    client.connect()
    assert client.fn(3).result() == 3
    client.close()
    server.close()

  @pytest.mark.parametrize('Server', SERVERS)
  def test_server_scope(self, Server):
    def fn(data):
      assert data == {'foo': np.array(1)}
      return {'foo': 2 * data['foo']}
    port = zerofun.free_port()
    server = Server(port)
    server.bind('fn', fn)
    with server:
      client = zerofun.Client('localhost', port)
      future = client.fn({'foo': np.array(1)})
      result = future.result()
      assert result['foo'] == 2
      client.close()

  @pytest.mark.parametrize('Server', SERVERS)
  def test_multiple_clients(self, Server):
    port = zerofun.free_port()
    server = Server(port)
    server.bind('fn', lambda data: data)
    with server:
      clients = [zerofun.Client('localhost', port) for _ in range(10)]
      futures = [client.fn(i) for i, client in enumerate(clients)]
      results = [future.result() for future in futures]
      assert results == list(range(10))
    [x.close() for x in clients]

  @pytest.mark.parametrize('Server', SERVERS)
  def test_multiple_methods(self, Server):
    port = zerofun.free_port()
    server = Server(port)
    server.bind('add', lambda x, y: x + y)
    server.bind('sub', lambda x, y: x - y)
    with server:
      client = zerofun.Client('localhost', port)
      assert client.add(3, 5).result() == 8
      assert client.sub(3, 5).result() == -2

  @pytest.mark.parametrize('Server', SERVERS)
  def test_connect_before_server(self, Server):
    port = zerofun.free_port()
    results = []

    def client():
      client = zerofun.Client('localhost', port)
      results.append(client.fn(12).result())
      client.close()

    thread = zerofun.Thread(client, start=True)
    time.sleep(0.2)
    server = Server(port)
    server.bind('fn', lambda x: x)
    server.start(block=False)
    thread.join()
    server.close()
    assert results[0] == 12

  @pytest.mark.parametrize('Server', SERVERS)
  def test_future_order(self, Server):
    port = zerofun.free_port()
    server = Server(port)
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

  @pytest.mark.parametrize('Server', SERVERS)
  def test_future_timeout(self, Server):
    port = zerofun.free_port()
    server = Server(port)
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

  @pytest.mark.parametrize('Server', SERVERS)
  def test_maxinflight(self, Server):
    port = zerofun.free_port()
    server = Server(port)
    parallel = [0]
    lock = threading.Lock()

    def fn(data):
      with lock:
        parallel[0] += 1
        assert parallel[0] <= 2
      time.sleep(0.2)
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

  @pytest.mark.parametrize('Server', SERVERS)
  def test_server_errors(self, Server):
    port = zerofun.free_port()

    def server(port):
      server = Server(port, errors=True)
      def fn(x):
        if x == 2:
          raise ValueError(x)
        return x
      server.bind('fn', fn)
      server.start(block=True)

    server = zerofun.Process(server, port, start=True)
    client = zerofun.Client('localhost', port)
    assert client.fn(1).result() == 1
    assert server.running
    with pytest.raises(RuntimeError):
      client.fn(2).result()

    client.close()
    server.join()
    assert server.exitcode not in (0, None)

  @pytest.mark.parametrize('Server', SERVERS)
  def test_future_errors(self, Server):
    port = zerofun.free_port()
    server = Server(port, errors=False)
    def fn(x):
      if x == 2:
        raise ValueError(x)
      return x
    server.bind('fn', fn)
    server.start(block=False)
    client = zerofun.Client('localhost', port)
    future1 = client.fn(1)
    future2 = client.fn(2)
    future3 = client.fn(3)
    assert future3.result() == 3
    with pytest.raises(RuntimeError):
      future2.result()
    assert future1.result() == 1
    client.close()
    server.close()

  @pytest.mark.parametrize('Server', SERVERS)
  def test_future_cleanup(self, Server):
    port = zerofun.free_port()
    server = Server(port)
    server.bind('fn', lambda x: x)
    server.start(block=False)
    client = zerofun.Client('localhost', port)
    client.fn(1)
    client.fn(2)
    future3 = client.fn(3)
    assert len(client.futures) == 3
    assert future3.result() == 3
    del future3
    assert not client.futures
    client.close()
    server.close()

  @pytest.mark.parametrize('repeat', range(3))
  @pytest.mark.parametrize('Server', SERVERS)
  def test_future_cleanup_errors(self, repeat, Server):
    port = zerofun.free_port()
    server = Server(port, errors=False)
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

    port = zerofun.free_port()
    server = Server(port, workers=4)
    server.bind('fn', workfn, postfn)
    server.start(block=False)
    client = zerofun.Client('localhost', port)
    futures = [client.fn(x) for x in range(10)]
    results = [x.result() for x in futures]
    server.close()
    client.close()
    assert results == list(range(10))
    assert completed != list(range(10))
    assert logged == list(range(10))

  @pytest.mark.parametrize('Server', SERVERS)
  @pytest.mark.parametrize('workers', (1, 4))
  def test_postfn_no_backlog(self, Server, workers):
    port = zerofun.free_port()
    lock = threading.Lock()
    work_calls = [0]
    done_calls = [0]
    def workfn(x):
      with lock:
        work_calls[0] += 1
        print(work_calls[0], done_calls[0])
        assert work_calls[0] <= done_calls[0] + workers
      return x, x
    def postfn(x):
      with lock:
        done_calls[0] += 1
      time.sleep(0.01)
    server = Server(port, workers=workers)
    server.bind('fn', workfn, postfn)
    server.start(block=False)
    client = zerofun.Client('localhost', port)
    futures = [client.fn(i) for i in range(20)]
    [future.result() for future in futures]

  @pytest.mark.parametrize('repeat', range(3))
  @pytest.mark.parametrize('Server', SERVERS)
  def test_shared_pool(self, repeat, Server):
    def slow(x):
      time.sleep(0.1)
      return x
    def fast(x):
      time.sleep(0.01)
      return x
    port = zerofun.free_port()
    server = Server(port, workers=1)
    server.bind('slow', slow)
    server.bind('fast', fast)
    server.start(block=False)
    client = zerofun.Client('localhost', port)
    slow_future = client.slow(0)
    fast_future = client.fast(0)
    assert not slow_future.done()
    # The slow request is processed first, so this will wait until both
    # requests are done.
    fast_future.result()
    assert slow_future.done()
    server.close()
    client.close()

  @pytest.mark.parametrize('repeat', range(3))
  @pytest.mark.parametrize('Server', SERVERS)
  def test_separate_pools(self, repeat, Server):
    def slow(x):
      time.sleep(0.1)
      return x
    def fast(x):
      time.sleep(0.01)
      return x
    port = zerofun.free_port()
    server = Server(port)
    server.bind('slow', slow, workers=1)
    server.bind('fast', fast, workers=1)
    server.start(block=False)
    client = zerofun.Client('localhost', port)
    slow_future = client.slow(0)
    fast_future = client.fast(0)
    # Both requests are processed in parallel, so the fast request returns
    # before the slow request is done.
    fast_future.result()
    assert not slow_future.done()
    server.close()
    client.close()

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

  @pytest.mark.parametrize('Server', SERVERS)
  @pytest.mark.parametrize('workers', (1, 10))
  def test_proxy(self, Server, workers):
    inner_port = zerofun.free_port()
    outer_port = zerofun.free_port()

    server = Server(inner_port, 'InnerServer')
    server.bind('fn', lambda x: 2 * x)
    server.start(block=False)

    kwargs = dict(name='ProxyClient', maxinflight=4)
    proxy_client = zerofun.Client('localhost', inner_port, **kwargs)
    proxy_server = Server(outer_port, 'ProxyServer', workers=workers)
    proxy_server.bind('fn2', lambda x: proxy_client.fn(x).result())
    proxy_server.start(block=False)

    client = zerofun.Client('localhost', outer_port, 'OuterClient')
    futures = [client.fn2(x) for x in range(20)]
    results = [future.result() for future in futures]
    assert results == list(range(0, 40, 2))

    proxy_server.close()
    proxy_client.close()
    server.close()
    client.close()

  @pytest.mark.parametrize('Server', SERVERS)
  def test_sharray(self, Server):

    def server(port):
      server = Server(port)
      def fn(data):
        assert data.array.shape == (3, 2)
        assert data.array.dtype == np.float32
        return data
      server.bind('fn', fn)
      server.start(block=True)

    def client(port):
      data = zerofun.SharedArray((3, 2), np.float32)
      data.array[:] = np.arange(6, dtype=np.float32).reshape(3, 2)
      client = zerofun.Client('localhost', port)
      result = client.call('fn', data).result()
      assert result.name == data.name
      assert result, data
      client.close()
      data.close()

    port = zerofun.free_port()
    client = zerofun.Process(client, port, start=True)
    server = zerofun.Process(server, port, start=True)
    client.join()
    server.kill()

  @pytest.mark.parametrize('Server', SERVERS)
  def test_client_drops(self, Server):
    barrier = threading.Barrier(2)

    def fn(x):
      if x == 1:
        barrier.wait()
        time.sleep(0.5)
      return x

    port = zerofun.free_port()
    server = Server(port)
    server.bind('fn', fn)
    server.start(block=False)

    client = zerofun.Client('localhost', port)
    client.fn(1)
    barrier.wait()
    client.close()
    stats = server.stats()
    assert stats['numrecv'] == 1
    assert stats['numsend'] == 0

    client = zerofun.Client('localhost', port)
    assert client.fn(2).result() == 2
    server.close()
    client.close()

  # TODO

  # @pytest.mark.parametrize('Server', SERVERS)
  # @pytest.mark.parametrize('batch', (1, 2, 4))
  # def test_batching_single(self, Server, addr, batch):
  #   addr = addr.format(port=zerofun.get_free_port())
  #   calls = [0]
  #   def function(data):
  #     assert set(data.keys()) == {'foo'}
  #     assert data['foo'].shape == (batch, 1)
  #     calls[0] += 1
  #     return data
  #   server = Server(addr)
  #   server.bind('function', function, batch=batch)
  #   with server:
  #     client = zerofun.Client(addr, pings=0, maxage=1)
  #     client.connect(retry=False, timeout=1)
  #     futures = [
  #         client.function({'foo': np.asarray([i])}) for i in range(batch)]
  #     results = [future.result()['foo'][0] for future in futures]
  #     assert calls[0] == 1
  #     assert results == list(range(batch))

  # @pytest.mark.parametrize('Server', SERVERS)
  # @pytest.mark.parametrize('batch', (1, 2, 4))
  # def test_batching_multiple(self, Server, addr, batch):
  #   addr = addr.format(port=zerofun.get_free_port())
  #   def function(data):
  #     return data
  #   server = Server(addr)
  #   server.bind('function', function, batch=batch)
  #   with server:
  #     clients = []
  #     for _ in range(3):
  #       client = zerofun.Client(addr, pings=0, maxage=1)
  #       client.connect(retry=False, timeout=1)
  #       clients.append(client)
  #     futures = ([], [], [])
  #     refs = ([], [], [])
  #     for n in range(batch):
  #       for i, client in enumerate(clients):
  #         futures[i].append(client.function({'foo': [i * n]}))
  #         refs[i].append(i * n)
  #     assert refs[0] == [x.result()['foo'][0] for x in futures[0]]
  #     assert refs[1] == [x.result()['foo'][0] for x in futures[1]]
  #     assert refs[2] == [x.result()['foo'][0] for x in futures[2]]

  # @pytest.mark.parametrize('Server', SERVERS)
  # @pytest.mark.parametrize('workers', (2, 3, 10))
  # def test_proxy_batched(self, Server, inner_addr, outer_addr, workers):
  #   inner_addr = inner_addr.format(port=zerofun.get_free_port())
  #   outer_addr = outer_addr.format(port=zerofun.get_free_port())
  #   proxy_client = zerofun.Client(inner_addr)
  #   proxy_server = Server(outer_addr)
  #   proxy_server.bind(
  #       'function', lambda x: proxy_client.function(x).result(),
  #       batch=2, workers=workers)
  #   server = Server(inner_addr)
  #   server.bind(
  #       'function', lambda data: {'foo': 2 * data['foo']}, workers=workers)
  #   with server:
  #     proxy_client.connect(retry=False, timeout=1)
  #     with proxy_server:
  #       client = zerofun.Client(outer_addr, pings=0, maxage=1)
  #       client.connect(retry=False, timeout=1)
  #       futures = [client.function({'foo': 13}) for _ in range(10)]
  #       results = [future.result()['foo'] for future in futures]
  #       print(results)
  #       assert all(result == 26 for result in results)

  # @pytest.mark.parametrize('Server', SERVERS)
  # @pytest.mark.parametrize('data', (
  #     {'a': np.zeros((3, 2), np.float32), 'b': np.ones((1,), np.uint8)},
  #     {'a': 12, 'b': [np.ones((1,), np.uint8), 13]},
  #     {'a': 12, 'b': ['c', [1, 2, 3]]},
  #     [],
  #     {},
  #     12,
  #     [[{}, []]],
  # ))
  # def test_tree_data(self, Server, addr, data):
  #   data = elements.tree.map(np.asarray, data)
  #   print(data)
  #   def tree_equal(tree1, tree2):
  #     try:
  #       comps = elements.tree.map(lambda x, y: np.all(x == y), tree1, tree2)
  #       comps, _ = elements.tree.flatten(comps)
  #       return all(comps)
  #     except TypeError:
  #       return False
  #   addr = addr.format(port=zerofun.get_free_port())
  #   client = zerofun.Client(addr, pings=0, maxage=1)
  #   server = Server(addr)
  #   def workfn(indata):
  #     assert tree_equal(indata, data)
  #     return indata
  #   server.bind('function', workfn)
  #   with server:
  #     client.connect(retry=False, timeout=1)
  #     outdata = client.function(data).result()
  #     assert tree_equal(outdata, data)

  # @pytest.mark.parametrize('Server', SERVERS)
  # @pytest.mark.parametrize('data', (
  #     {'a': np.zeros((3, 2), np.float32), 'b': np.ones((1,), np.uint8)},
  #     {'a': 12, 'b': [np.ones((1,), np.uint8), 13]},
  #     {'a': 12, 'b': ['c', [1, 2, 3]]},
  #     [],
  #     {},
  #     12,
  #     [[{}, []]],
  # ))
  # def test_tree_data_batched(self, Server, addr, data):
  #   data = elements.tree.map(np.asarray, data)
  #   print(data)
  #   def tree_equal(tree1, tree2):
  #     try:
  #       comps = elements.tree.map(lambda x, y: np.all(x == y), tree1, tree2)
  #       comps, _ = elements.tree.flatten(comps)
  #       return all(comps)
  #     except TypeError:
  #       return False
  #   addr = addr.format(port=zerofun.get_free_port())
  #   client = zerofun.Client(addr, pings=0, maxage=1)
  #   server = Server(addr)
  #   def workfn(indata):
  #     return indata
  #   server.bind('function', workfn, batch=4)
  #   with server:
  #     client.connect(retry=False, timeout=1)
  #     futures = [client.function(data) for _ in range(4)]
  #     for future in futures:
  #       assert tree_equal(future.result(), data)

  # @pytest.mark.parametrize('Server', SERVERS)
  # def test_tree_none_result(self, Server, addr):
  #   addr = addr.format(port=zerofun.get_free_port())
  #   client = zerofun.Client(addr, pings=0, maxage=1)
  #   server = Server(addr)
  #   def workfn(indata):
  #     pass  # No return value
  #   server.bind('function', workfn)
  #   with server:
  #     client.connect(retry=False, timeout=1)
  #     result = client.function([]).result()
  #     assert result == []
