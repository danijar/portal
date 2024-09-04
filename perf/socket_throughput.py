import collections
import time

import portal


def main():

  size = 1024 ** 3 // 4
  parts = 64
  prefetch = 8
  twoway = False
  assert size % parts == 0

  def server(port):
    server = portal.ServerSocket(port)
    while True:
      addr, data = server.recv()
      if twoway:
        server.send(addr, data)
      else:
        server.send(addr, b'ok')
      assert len(data) == size

  def client(port):
    data = [bytearray(size // parts) for _ in range(parts)]
    client = portal.ClientSocket('localhost', port)
    for _ in range(prefetch):
      client.send(*data)
    durations = collections.deque(maxlen=50)
    start = time.time()
    while True:
      client.send(*data)
      result = client.recv()
      if twoway:
        assert len(result) == size
      else:
        assert result == b'ok'
      end = time.time()
      durations.append(end - start)
      start = end
      avgdur = sum(durations) / len(durations)
      mbps = size / avgdur / (1024 ** 2)
      mbps *= 2 if twoway else 1
      print(mbps)  # 3500 oneway, 2500 twoway

  portal.setup(hostname='localhost')
  port = portal.free_port()
  portal.run([
      portal.Process(server, port),
      portal.Process(client, port),
  ])


if __name__ == '__main__':
  main()
