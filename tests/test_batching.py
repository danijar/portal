import functools

import numpy as np
import pytest
import zerofun


BATCH_SERVERS = [
    zerofun.BatchServer,
    functools.partial(zerofun.BatchServer, process=False),
    # functools.partial(zerofun.BatchServer, shmem=True),  # TODO
]


class TestBatching:

  @pytest.mark.parametrize('BatchServer', BATCH_SERVERS)
  def test_single_client(self, BatchServer):
    port = zerofun.free_port()
    server = zerofun.BatchServer(port)
    def fn(x):
      assert x.shape == (4,)
      return 2 * x
    server.bind('fn', fn, batch=4)
    server.start(block=False)
    client = zerofun.Client('localhost', port)
    futures = [client.fn(x) for x in range(8)]
    results = [x.result() for x in futures]
    assert (results == 2 * np.arange(8)).all()
    client.close()
    server.close()

  def test_multiple_clients(self):
    port = zerofun.free_port()
    server = zerofun.BatchServer(port)
    def fn(x):
      assert x.shape == (4,)
      return 2 * x
    server.bind('fn', fn, batch=4)
    server.start(block=False)
    clients = [zerofun.Client('localhost', port) for _ in range(8)]
    futures = [x.fn(i) for i, x in enumerate(clients)]
    results = [x.result() for x in futures]
    assert (results == 2 * np.arange(8)).all()
    [x.close() for x in clients]
    server.close()

  @pytest.mark.parametrize('workers', (1, 10))
  def test_proxy(self, workers):
    inner_port = zerofun.free_port()
    outer_port = zerofun.free_port()

    server = zerofun.Server(inner_port, 'InnerServer')
    server.bind('fn', lambda x: 2 * x)
    server.start(block=False)

    kwargs = dict(name='ProxyClient', maxinflight=4)
    proxy_client = zerofun.Client('localhost', inner_port, **kwargs)
    proxy_server = zerofun.BatchServer(
        outer_port, 'ProxyServer', workers=workers)
    proxy_server.bind(
        'fn2', lambda x: proxy_client.fn(x).result(), batch=2)
    proxy_server.start(block=False)

    client = zerofun.Client('localhost', outer_port, 'OuterClient')
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
    port = zerofun.free_port()
    server = zerofun.BatchServer(port)
    server.bind('fn', lambda x: x, batch=4)
    server.start(block=False)
    client = zerofun.Client('localhost', port)
    futures = [client.fn(data) for _ in range(4)]
    results = [x.result() for x in futures]
    for result in results:
      assert zerofun.tree_equals(result, data)
    client.close()
    server.close()

  # TODO:
  # - multiple workers
  # - some client drops and comes back up
  # - server drops and comes back up
  # - test error types
  # - test done fn
