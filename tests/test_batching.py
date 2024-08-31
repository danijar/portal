import numpy as np
import zerofun


class TestBatching:

  def test_single_client(self):
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
    assert (results == np.arange(0, 16, 2)).all()
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
    assert (results == np.arange(0, 16, 2)).all()
    [x.close() for x in clients]
    server.close()

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

