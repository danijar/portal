import collections
import time

import zerofun


def main():

  size = 1024
  parts = 32

  def server(port):
    server = zerofun.ServerSocket(port)
    while True:
      addr, data = server.recv()
      server.send(addr, b'ok')
      assert len(data) == size // parts * parts

  def client(port):
    data = [bytearray(size // parts) for _ in range(parts)]
    client = zerofun.ClientSocket('localhost', port)
    durations = collections.deque(maxlen=10)
    while True:
      start = time.perf_counter()
      client.send(*data)
      client.recv()
      end = time.perf_counter()
      durations.append(end - start)
      ping = sum(durations) / len(durations)
      print(1000 * ping)  # <1ms

  port = zerofun.free_port()
  zerofun.run([
      zerofun.Process(server, port),
      zerofun.Process(client, port),
  ])



if __name__ == '__main__':
  main()

