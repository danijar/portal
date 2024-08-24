import collections
import time

import zerofun


def main():

  size = 2 * 1024 ** 3
  parts = 100
  prefetch = 8

  def server(port):
    server = zerofun.ServerSocket(port)
    durations = collections.deque(maxlen=10)
    start = time.time()
    while True:
      addr, data = server.recv()
      server.send(addr, b'ok')
      assert len(data) == size // parts * parts
      end = time.time()
      durations.append(end - start)
      start = end
      avgdur = sum(durations) / len(durations)
      mbps = size / avgdur / (1024 ** 2)
      print(mbps)  # ~2000

  def client(port):
    data = [bytearray(size // parts) for _ in range(parts)]
    client = zerofun.ClientSocket('localhost', port)
    for _ in range(prefetch):
      client.send(*data)
    while True:
      client.send(*data)
      client.recv()

  port = zerofun.free_port()
  zerofun.run([
      zerofun.Process(server, port),
      zerofun.Process(client, port),
  ])



if __name__ == '__main__':
  main()
