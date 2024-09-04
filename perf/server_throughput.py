import collections
import time

import zerofun


def main():

  size = 1024 ** 3 // 4
  prefetch = 8
  twoway = False

  def server(port):
    server = zerofun.Server(port)
    # server = zerofun.BatchServer(port)
    # server = zerofun.BatchServer(port, process=False)
    # server = zerofun.BatchServer(port, shmem=True)
    def fn(x):
      assert len(x) == size
      return x if twoway else b'ok'
    server.bind('foo', fn)
    server.start(block=True)

  def client(port):
    data = bytearray(size)
    client = zerofun.Client('localhost', port, maxinflight=prefetch + 1)
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

  zerofun.setup(hostname='localhost')
  port = zerofun.free_port()
  zerofun.run([
      zerofun.Process(server, port),
      zerofun.Process(client, port),
  ])


if __name__ == '__main__':
  main()
