import collections
import concurrent.futures
import queue
import threading
import time
import traceback

import numpy as np

from . import contextlib
from . import poollib
from . import process
from . import server_socket
from . import thread
from . import utils


Method = collections.namedtuple('Method', (
    'name,workfn,postfn,pool,workers,batch,inqueue,busy'))


class Server:

  def __init__(
      self, port, name='Server', workers=1, errors=True, process=False,
      **kwargs):
    self.socketargs = dict(**kwargs, port=port, name=name)
    self.methods = {}

    self.pool_default = poollib.ThreadPool(workers, 'pool_default')
    self.pool_postfn = poollib.ThreadPool(1, 'pool_postfn')
    self.pools = [self.pool_default, self.pool_postfn]

    # self.result_set = set()
    # self.done_queue = collections.deque()
    # self.done_proms = collections.deque()

    self.process = process
    self.started = False

  def bind(self, name, workfn, postfn=None, workers=0, batch=0):
    assert not self.started
    if workers:
      pool = poollib.ThreadPool(workers, f'pool_{name}')
      self.pools.append(pool)
    else:
      workers = self.workers
      pool = self.pool_default
    inqueue = collections.deque()
    args = (name, workfn, postfn, pool, workers, batch, inqueue, [0])
    self.methods[name] = Method(*args)

  def start(self, block=True):
    assert not self.started
    if self.process:
      self.running = contextlib.context().mp.Event()
      self.running.set()
      requests = contextlib.context().mp.Queue()
      responses = contextlib.context().mp.Queue()
      self.endpoint = process.Process(
          self._endpoint, self.running, requests, responses, process,
          methods, self.socketargs, start=True)
    else:
      self.running = threading.Event()
      self.running.set()
      self.endpoint = thread.Thread(
          self._endpoint, self.running, requests, responses, process,
          methods, self.socketargs, start=True)
    self.dispatcher = thread.Thread(self._dispatcher, requests, responses)
    if block:
      self.error.wait()
      self.close()
      raise RuntimeError

  def check(self):
    assert self.started
    if self.error.is_set():
      raise RuntimeError

  def close(self):
    assert self.started
    self.running.clear()
    self.dispatcher.join(timeout=1)
    self.endpoint.join(timeout=1)
    [x.close() for x in self.pools]

  def stats(self):
    return {}  # TODO

  def __enter__(self):
    return self

  def __exit__(self, *e):
    self.close()

  @classmethod
  def _endpoint(
      cls, running, requests, responses, process, methods, socketargs):
    socket = server_socket.ServerSocket(**socketargs)
    shmem = process  # TODO
    endpoint = Endpoint(socket, methods, requests, responses, shmem)
    while running.is_set():
      endpoint.step()
    socket.close()

    try:
      addr, data = self.socket.recv(timeout=0.001)
    except queue.Empty:
      return
    reqnum, data = data[:8], data[8:]
    strlen, data = int.from_bytes(data[:8], 'little', signed=False), data[8:]
    name, data = data[:strlen].decode('utf-8'), data[strlen:]
    try:
      batch_size = self.batch_sizes[name]
    except KeyError:
      self.send_error(addr, reqnum, 1, f'Unknown method {name}')
      return
    data = utils.unpack(data)

    if not batch_size:
      if self.use_shmem:
        ...
      self.requests.put((name, addr, reqnum, data))
    else:
      entries = utils.pack(data)
      if name not in self.batches:
        if self.use_shmem:
          ...
        else:
          buffers = [bytearray(batch_size * len(x)) for x in entries]
        self.batches[name] = [0, buffers]

      pos, structure, buffers = self.batches[name]

      ...

      for buffer, entry in zip(buffers, entries):
        buffer[pos * len(entry): (pos + 1) * len(entry)] = entry
      pos += 1
      if pos == batch_size:
        del self.batches[method.name]
        self.requests.put((name, addr, reqnum, buffers))

      # else:
      #   try:
      #     batch.append(data)
      #   except ValueError:
      #     message = 'Structure mismatch when batching requests'
      #     self.send_error(addr, reqnum, 2, message)
      #     return
      # if batch.full():
      #   del self.batches[method.name]
      #   self.requests.put((name, addr, reqnum, batch.result()))

    else:
      if self.use_shmem:
        data = self.make_shmem(data)  # TODO
      self.requests.put((name, addr, reqnum, data))

  def maybe_send(self):
    requests.put((name, addr(s), reqnum(s), data))

    (name, addr, reqnum, data) = responses.get(timeout=0)  # block=False?
    method = methods[name]
    if method.batch:
      assert len(addr) == len(reqnum) == method.batch
      for i, (addr, reqnum) in enumerate(zip(addr, reqnum)):
        socket.send(addr, reqnum, self._batch_index(data, i))
    else:
      socket.send(addr, data)

  def send_error(self, addr, reqnum, status, message):
    assert 1 <= status, status
    status = status.to_bytes(8, 'little', signed=False)
    self.socket.send(addr, reqnum, status, message)


  def _dispatcher(self):

    try:
      name, addr, reqnu, data = self.requests.get(timeout=0)
    except queue.Empty:
      ...

    method = self.methods[name]
    method.pool.submit((,,,))

    pools ...

    self.responses.put(...)

    # TODO: should not have knowledge of batching
    ...

#   def _loop(self):
#
#     # socket = sockets.ServerSocket(self.address, self.ipv6)
#     while context.running:
#       now = time.time()
#       result = socket.receive()
#       self._handle_request(socket, result, now)
#       for method in self.methods.values():
#         self._handle_input(method, now)
#       self._handle_results(socket, now)
#       self._handle_dones()
#       time.sleep(0.0001)
#     socket.close()
#
#   def _handle_request(self, socket, result, now):
#     if result is None:
#       return
#     addr, rid, name, payload = result
#     method = self.methods.get(name, None)
#     if not method:
#       socket.send_error(addr, rid, f'Unknown method {name}.')
#       return
#     method.inqueue.append((addr, rid, payload, now))
#     self._handle_input(method, now)
#
#   def _handle_input(self, method, now):
#     if len(method.inqueue) < method.insize:
#       return
#     if method.inprog[0] >= 2 * method.workers:
#       return
#     method.inprog[0] += 1
#     if method.batched:
#       inputs = [method.inqueue.popleft() for _ in range(method.insize)]
#       addr, rid, payload, recvd = zip(*inputs)
#     else:
#       addr, rid, payload, recvd = method.inqueue.popleft()
#     future = method.pool.submit(self._work, method, addr, rid, payload, recvd)
#     future.method = method
#     future.addr = addr
#     future.rid = rid
#     self.result_set.add(future)
#     if method.postfn:
#       self.done_queue.append(future)
#
#   def _handle_results(self, socket, now):
#     completed, self.result_set = concurrent.futures.wait(
#         self.result_set, 0, concurrent.futures.FIRST_COMPLETED)
#     for future in completed:
#       method = future.method
#       try:
#         result = future.result()
#         addr, rid, payload, logs, recvd = result
#         if method.batched:
#           for addr, rid, payload in zip(addr, rid, payload):
#             socket.send_result(addr, rid, payload)
#           for recvd in recvd:
#             self.agg.add(method.name, now - recvd, ('min', 'avg', 'max'))
#         else:
#           socket.send_result(addr, rid, payload)
#           self.agg.add(method.name, now - recvd, ('min', 'avg', 'max'))
#       except Exception as e:
#         print(f'Exception in server {self.name}:')
#         typ, tb = type(e), e.__traceback__
#         full = ''.join(traceback.format_exception(typ, e, tb)).strip('\n')
#         print(full)
#         if method.batched:
#           for addr, rid in zip(future.addr, future.rid):
#             socket.send_error(addr, rid, repr(e))
#         else:
#           socket.send_error(future.addr, future.rid, repr(e))
#         if self.errors:
#           self.exception = e
#       finally:
#         if not method.postfn:
#           method.inprog[0] -= 1
#
#   def _handle_dones(self):
#     while self.done_queue and self.done_queue[0].done():
#       future = self.done_queue.popleft()
#       if future.exception():
#         continue
#       addr, rid, payload, logs, recvd = future.result()
#       future2 = self.pool_postfn.submit(future.method.postfn, logs)
#       future2.method = future.method
#       self.done_proms.append(future2)
#     while self.done_proms and self.done_proms[0].done():
#       future = self.done_proms.popleft()
#       future.result()
#       future.method.inprog[0] -= 1
#
#   def _work(self, method, addr, rid, payload, recvd):
#     if method.batched:
#       data = [sockets.unpack(x) for x in payload]
#       data = elements.tree.map(lambda *xs: np.stack(xs), *data)
#     else:
#       data = sockets.unpack(payload)
#     if method.postfn:
#       result, logs = method.workfn(data)
#     else:
#       result = method.workfn(data)
#       if result is None:
#         result = []
#       logs = None
#     if method.batched:
#       results = [
#           elements.tree.map(lambda x: x[i], result)
#           for i in range(method.insize)]
#       payload = [sockets.pack(x) for x in results]
#     else:
#       payload = sockets.pack(result)
#     return addr, rid, payload, logs, recvd


class Endpoint:

  def __init__(self, socket, requests, responses, batch_sizes, use_shmem):
    self.socket = socket
    self.requests = requests
    self.responses = responses
    self.batch_sizes = batch_sizes
    self.use_shmem = use_shmem
    self.batches = {}

  def step(self):
    self.maybe_recv()
    self.maybe_send()
    time.sleep(0.0001)

  def maybe_recv(self):
    try:
      addr, data = self.socket.recv(timeout=0)
    except queue.Empty:
      return
    reqnum, data = data[:8], data[8:]
    strlen, data = int.from_bytes(data[:8], 'little', signed=False), data[8:]
    name, data = data[:strlen].decode('utf-8'), data[strlen:]
    try:
      batch_size = self.batch_sizes[name]
    except KeyError:
      self.send_error(addr, reqnum, 1, f'Unknown method {name}')
      return
    data = utils.unpack(data)

    if not batch_size:
      if self.use_shmem:
        ...
      self.requests.put((name, addr, reqnum, data))
    else:
      entries = utils.pack(data)
      if name not in self.batches:
        if self.use_shmem:
          ...
        else:
          buffers = [bytearray(batch_size * len(x)) for x in entries]
        self.batches[name] = [0, buffers]

      pos, structure, buffers = self.batches[name]

      ...

      for buffer, entry in zip(buffers, entries):
        buffer[pos * len(entry): (pos + 1) * len(entry)] = entry
      pos += 1
      if pos == batch_size:
        del self.batches[method.name]
        self.requests.put((name, addr, reqnum, buffers))

      # else:
      #   try:
      #     batch.append(data)
      #   except ValueError:
      #     message = 'Structure mismatch when batching requests'
      #     self.send_error(addr, reqnum, 2, message)
      #     return
      # if batch.full():
      #   del self.batches[method.name]
      #   self.requests.put((name, addr, reqnum, batch.result()))

    else:
      if self.use_shmem:
        data = self.make_shmem(data)  # TODO
      self.requests.put((name, addr, reqnum, data))

  def maybe_send(self):
    requests.put((name, addr(s), reqnum(s), data))

    (name, addr, reqnum, data) = responses.get(timeout=0)  # block=False?
    method = methods[name]
    if method.batch:
      assert len(addr) == len(reqnum) == method.batch
      for i, (addr, reqnum) in enumerate(zip(addr, reqnum)):
        socket.send(addr, reqnum, self._batch_index(data, i))
    else:
      socket.send(addr, data)

  def send_error(self, addr, reqnum, status, message):
    assert 1 <= status, status
    status = status.to_bytes(8, 'little', signed=False)
    self.socket.send(addr, reqnum, status, message)

