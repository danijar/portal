import collections
import time

import zerofun


def main():

  size = 1024 ** 3 // 4
  parts = 64
  prefetch = 8

  def server(port):
    server = zerofun.Server(f'tcp://*:{port}')
    server.bind('foo', lambda data: b'ok')
    server.run()

  def client(port):
    data = [bytearray(size // parts) for _ in range(parts)]
    client = zerofun.Client(
        f'tcp://localhost:{port}', connect=True,
        maxinflight=prefetch)

    res = collections.deque()
    for _ in range(prefetch):
      res.append(client.foo(data))

    durations = collections.deque(maxlen=10)
    start = time.time()
    while True:
      res.append(client.foo(data))
      res.popleft().result()

      end = time.time()
      durations.append(end - start)
      start = end
      avgdur = sum(durations) / len(durations)
      mbps = size / avgdur / (1024 ** 2)
      print(mbps)  # ~5000

  port = zerofun.get_free_port()
  zerofun.run([
      zerofun.Process(server, port),
      zerofun.Process(client, port),
  ])



if __name__ == '__main__':
  main()

