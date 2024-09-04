import collections
import time

import portal


def main():

  size = 1024
  parts = 32

  def server(port):
    server = portal.ServerSocket(port)
    while True:
      addr, data = server.recv()
      server.send(addr, b'ok')
      assert len(data) == size // parts * parts

  def client(port):
    data = [bytearray(size // parts) for _ in range(parts)]
    client = portal.ClientSocket('localhost', port)
    durations = collections.deque(maxlen=10)
    while True:
      start = time.perf_counter()
      client.send(*data)
      client.recv()
      end = time.perf_counter()
      durations.append(end - start)
      ping = sum(durations) / len(durations)
      print(1000 * ping)  # <1ms

  portal.setup(hostname='localhost')
  port = portal.free_port()
  portal.run([
      portal.Process(server, port),
      portal.Process(client, port),
  ])



if __name__ == '__main__':
  main()

