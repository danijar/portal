import collections
import time

import portal


def main():

  size = 1024 ** 3 // 4
  parts = 64
  prefetch = 16

  def server(port1):
    server = portal.ServerSocket(port1)
    durations = collections.deque(maxlen=50)
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
      print(mbps)  # ~3500

  def proxy(port1, port2):
    server = portal.ServerSocket(port2)
    client = portal.ClientSocket(port1)
    addrs = collections.deque()
    while True:
      try:
        addr, data = server.recv(timeout=0.0001)
        addrs.append(addr)
        client.send(data)
      except TimeoutError:
        pass
      try:
        data = client.recv(timeout=0.0001)
        server.send(addrs.popleft(), data)
      except TimeoutError:
        pass

  def client(port2):
    data = [bytearray(size // parts) for _ in range(parts)]
    client = portal.ClientSocket(port2)
    for _ in range(prefetch):
      client.send(*data)
    while True:
      client.send(*data)
      assert client.recv() == b'ok'

  port1 = portal.free_port()
  port2 = portal.free_port()
  portal.run([
      portal.Process(server, port1),
      portal.Process(proxy, port1, port2),
      portal.Process(client, port2),
  ])


if __name__ == '__main__':
  main()
