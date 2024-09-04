import functools

import numpy as np
import pytest
import portal


BATCH_SERVERS = [
    portal.BatchServer,
    functools.partial(portal.BatchServer, process=False),
    functools.partial(portal.BatchServer, shmem=True),
]


class TestBatching:

  @pytest.mark.parametrize('BatchServer', BATCH_SERVERS)
  def test_single_client(self, BatchServer):
    port = portal.free_port()
    server = portal.BatchServer(port)
    def fn(x):
      assert x.shape == (4,)
      return 2 * x
    server.bind('fn', fn, batch=4)
    server.start(block=False)
    client = portal.Client('localhost', port)
    futures = [client.fn(x) for x in range(8)]
    results = [x.result() for x in futures]
    assert (results == 2 * np.arange(8)).all()
    client.close()
    server.close()

  def test_multiple_clients(self):
    port = portal.free_port()
    server = portal.BatchServer(port)
    def fn(x):
      assert x.shape == (4,)
      return 2 * x
    server.bind('fn', fn, batch=4)
    server.start(block=False)
    clients = [portal.Client('localhost', port) for _ in range(8)]
    futures = [x.fn(i) for i, x in enumerate(clients)]
    results = [x.result() for x in futures]
    assert (results == 2 * np.arange(8)).all()
    [x.close() for x in clients]
    server.close()

  def test_multiple_workers(self):
    port = portal.free_port()
    server = portal.BatchServer(port)
    def fn(x):
      assert x.shape == (4,)
      return 2 * x
    server.bind('fn', fn, workers=4, batch=4)
    server.start(block=False)
    clients = [portal.Client('localhost', port) for _ in range(32)]
    futures = [x.fn(i) for i, x in enumerate(clients)]
    results = [x.result() for x in futures]
    assert (results == 2 * np.arange(32)).all()
    [x.close() for x in clients]
    server.close()

  @pytest.mark.parametrize('workers', (1, 10))
  def test_proxy(self, workers):
    inner_port = portal.free_port()
    outer_port = portal.free_port()

    server = portal.Server(inner_port, 'InnerServer')
    server.bind('fn', lambda x: 2 * x)
    server.start(block=False)

    kwargs = dict(name='ProxyClient', maxinflight=4)
    proxy_client = portal.Client('localhost', inner_port, **kwargs)
    proxy_server = portal.BatchServer(
        outer_port, 'ProxyServer', workers=workers)
    proxy_server.bind(
        'fn2', lambda x: proxy_client.fn(x).result(), batch=2)
    proxy_server.start(block=False)

    client = portal.Client('localhost', outer_port, 'OuterClient')
    futures = [client.fn2(x) for x in range(16)]
    results = [future.result() for future in futures]
    assert (results == 2 * np.arange(16)).all()

    proxy_server.close()
    proxy_client.close()
    server.close()
    client.close()

  @pytest.mark.parametrize('data', (
      {'a': np.zeros((3, 2), np.float32), 'b': np.ones((1,), np.uint8)},
      {'a': np.array(12), 'b': [np.ones((1,), np.uint8)]},
      {'a': np.array(12), 'b': ['c', [np.array(1), np.array(2.0)]]},
      [],
      {},
      np.array(12),
      [[{}, []]],
  ))
  def test_tree(self, data):
    print(f'\n{data}')
    port = portal.free_port()
    server = portal.BatchServer(port)
    server.bind('fn', lambda x: x, batch=4)
    server.start(block=False)
    client = portal.Client('localhost', port)
    futures = [client.fn(data) for _ in range(4)]
    results = [x.result() for x in futures]
    for result in results:
      assert portal.tree_equals(result, data)
    client.close()
    server.close()

  def test_shape_mismatch(self):
    port = portal.free_port()
    server = portal.BatchServer(port, errors=False)
    server.bind('fn', lambda x: x, batch=2)
    server.start(block=False)
    client = portal.Client('localhost', port)
    future1 = client.fn({'a': np.array(12)})
    future2 = client.fn(42)
    with pytest.raises(RuntimeError):
      future2.result()
    future3 = client.fn({'a': np.array(42)})
    assert future1.result() == {'a': np.array(12)}
    assert future3.result() == {'a': np.array(42)}
    client.close()
    server.close()

  @pytest.mark.parametrize('repeat', range(3))
  def test_client_drops(self, repeat):
    port = portal.free_port()
    server = portal.BatchServer(port)
    server.bind('fn', lambda x: 2 * x, batch=4)
    server.start(block=False)
    client = portal.Client('localhost', port, name='Client1', autoconn=False)
    client.connect()
    future1 = client.fn(1)
    future2 = client.fn(2)
    client.close()
    client = portal.Client('localhost', port, name='Client2')
    future3 = client.fn(3)
    future4 = client.fn(4)
    with pytest.raises(portal.Disconnected):
      future1.result()
    with pytest.raises(portal.Disconnected):
      future2.result()
    assert future3.result() == 6
    assert future4.result() == 8
    client.close()
    server.close()

  @pytest.mark.parametrize('repeat', range(3))
  def test_server_drops(self, repeat):
    port = portal.free_port()
    server = portal.BatchServer(port)
    server.bind('fn', lambda x: 2 * x, batch=2)
    server.start(block=False)
    client = portal.Client('localhost', port, autoconn=False)
    client.connect()
    future1 = client.fn(1)
    server.close()

    server = portal.BatchServer(port)
    server.bind('fn', lambda x: 2 * x, batch=2)
    server.start(block=False)

    with pytest.raises(portal.Disconnected):
      future1.result()
    client.connect()
    future2 = client.fn(2)
    future3 = client.fn(3)
    assert future2.result() == 4
    assert future3.result() == 6
    server.close()
    client.close()
