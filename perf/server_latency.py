import collections
import time

import portal


def main():

  size = 1024

  def server(port):
    server = portal.Server(port)
    def fn(x):
      assert len(x) == size
      return b'ok'
    server.bind('foo', fn)
    server.start(block=True)

  def client(port):
    data = bytearray(size)
    client = portal.Client('localhost', port)
    futures = collections.deque()
    durations = collections.deque(maxlen=50)
    while True:
      start = time.perf_counter()
      futures.append(client.call('foo', data))
      result = futures.popleft().result()
      assert result == b'ok'
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
