import collections
import time

import zerofun


def main():

  size = 1024 ** 3 // 4
  prefetch = 0  # 4  # 16

  def server(port):
    server = zerofun.Server(port)
    server.bind('foo', lambda x: x)
    # server.bind('foo', lambda x: ())
    server.start(block=True)

  def client(port):
    data = bytearray(size)
    client = zerofun.Client('localhost', port, maxinflight=prefetch + 1)
    futures = collections.deque()
    for _ in range(prefetch):
      futures.append(client.call('foo', data))
    durations = collections.deque(maxlen=50)
    start = time.time()
    while True:
      futures.append(client.call('foo', data))
      data = futures.popleft().result()
      assert len(data) == size
      end = time.time()
      durations.append(end - start)
      start = end
      avgdur = sum(durations) / len(durations)
      bidirectional = 2
      mbps = bidirectional * size / avgdur / (1024 ** 2)
      print(mbps)  # ~3500

  port = zerofun.free_port()
  zerofun.run([
      zerofun.Process(server, port),
      zerofun.Process(client, port),
  ])


if __name__ == '__main__':
  main()


# import collections
# import time
#
# import zerofun
#
#
# def main():
#
#   size = 1024 ** 3 // 4
#   prefetch = 16
#
#   def server(port):
#     server = zerofun.Server(port)
#     # server.bind('foo', lambda x: x)
#     server.bind('foo', lambda x: ())
#     server.start(block=True)
#
#   def client(port):
#     data = bytearray(size)
#     client = zerofun.Client('localhost', port, maxinflight=prefetch + 1)
#     futures = collections.deque()
#     for _ in range(prefetch):
#       futures.append(client.call('foo', data))
#     durations = collections.deque(maxlen=50)
#     start = time.time()
#     while True:
#       futures.append(client.call('foo', data))
#       futures.popleft().result()
#       # assert len(data) == size
#       end = time.time()
#       durations.append(end - start)
#       start = end
#       avgdur = sum(durations) / len(durations)
#       mbps = size / avgdur / (1024 ** 2)
#       print(mbps)  # ~3500
#
#   port = zerofun.free_port()
#   zerofun.run([
#       zerofun.Process(server, port),
#       zerofun.Process(client, port),
#   ])
#
#
# if __name__ == '__main__':
#   main()
