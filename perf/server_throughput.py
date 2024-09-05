import collections
import time

import portal


def main():

  size = 1024 ** 3 // 4
  prefetch = 8
  twoway = False

  def server(port):
    server = portal.Server(port)
    # server = portal.BatchServer(port)
    # server = portal.BatchServer(port, process=False)
    # server = portal.BatchServer(port, shmem=True)
    def fn(x):
      assert len(x) == size
      return x if twoway else b'ok'
    server.bind('foo', fn)
    server.start(block=True)

  def client(port):
    data = bytearray(size)
    client = portal.Client('localhost', port, maxinflight=prefetch + 1)
    futures = collections.deque()
    for _ in range(prefetch):
      futures.append(client.call('foo', data))
    durations = collections.deque(maxlen=50)
    start = time.perf_counter()
    while True:
      futures.append(client.call('foo', data))
      result = futures.popleft().result()
      if twoway:
        assert len(result) == size
      else:
        assert result == b'ok'
      end = time.perf_counter()
      durations.append(end - start)
      start = end
      avgdur = sum(durations) / len(durations)
      mbps = size / avgdur / (1024 ** 2)
      mbps *= 2 if twoway else 1
      print(mbps)  # 3700 oneway, 3000 twoway

  portal.setup(host='localhost')
  port = portal.free_port()
  portal.run([
      portal.Process(server, port),
      portal.Process(client, port),
  ])


if __name__ == '__main__':
  main()
