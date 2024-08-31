import queue

import numpy as np

from . import client
from . import packlib
from . import process
from . import server
from . import server_socket
from . import sharray
from . import thread
from . import utils


class BatchServer:

  def __init__(
      self, port, name='Server', workers=1, errors=True, process=True,
      **kwargs):

    self.errors = errors
    self.kwargs = kwargs
    port2 = utils.free_port()

    self.batargs = (
        self.running, port, inner_port, f'{name}Batcher',
        self.batch_sizes, errors, process)
    self.batcher = Batcher(port, port2, , errors, **kwargs)

    self.server = server.Server(port2, name, workers, errors, **kwargs)

  def bind(self, name, workfn, donefn=None, batch=0, workers=0):
    self.batch_sizes[name] = batch
    self.server.bind(name, workfn, donefn, workers=0)

  def start(self, block=True):
    self.running.set()
    if self.process:
      self.batcher = process.Process(batcher, *self.batargs, **self.kwargs)
    else:
      self.batcher = process.Thread(batcher, *self.batargs, **self.kwargs)
    self.batcher.start()
    self.server.start(block=block)

  def close(self, timeout=None):
    self.running.clear()
    self.server.close(timeout and 0.5 * timeout)
    self.batcher.join(timeout and 0.5 * timeout)
    self.batcher.kill()

  def stats(self):
    return self.server.stats()

  def __enter__(self):
    return self

  def __exit__(self, *e):
    self.close()


def batcher(
    running, outer_port, inner_port, name, batch_sizes, errors, shmem,
    **kwargs):

  outer = server_socket.ServerSocket(outer_port, name, **kwargs)
  inner = client.Client('localhost', inner_port, name, **kwargs)
  batches = {}  # {method: ([addr], [reqnum], structure, [sharray])}
  jobs = []

  def send_error(addr, reqnum, status, message):
    assert 1 <= status, status
    status = status.to_bytes(8, 'little', signed=False)
    outer.send(addr, reqnum, status, message)
    if errors:
      raise RuntimeError(message)

  while running.is_set() or jobs:

    while True:  # Loop syntax used to break on error.
      if not running.is_set():  # Do not accept further requests.
        break
      try:
        addr, data = outer.recv(timeout=0.0001)
      except queue.Empty:
        break
      reqnum, data = bytes(data[:8]), data[8:]
      strlen, data = int.from_bytes(data[:8], 'little', signed=False), data[8:]
      name, data = data[:strlen].decode('utf-8'), data[strlen:]
      if name not in batch_sizes:
        send_error(addr, reqnum, 3, f'Unknown method {name}')
        break
      data = packlib.unpack(data)
      batch_size = batch_sizes[name]
      if not batch_size:
        job = inner.call(name, data)
        job.args = (False, addr, reqnum)
        jobs.append(job)
        break
      leaves, structure = flatten(data)
      if name not in batches:
        if shmem:
          buffers = [
              sharray.SharedArray((batch_size, *leaf.shape), leaf.dtype)
              for leaf in leaves]
        else:
          buffers = [
              np.empty((batch_size, *leaf.shape), leaf.dtype)
              for leaf in leaves]
        batches[name] = ([], [], structure, buffers)
      addrs, reqnums, reference, buffers = batches[name]
      if structure != reference:
        send_error(addr, reqnum, 5, (
            f'Argument structure {structure} does not match previous ' +
            f'requests with structure {reference} for batched server ' +
            f'method {name}.'))
        break
      index = len(addrs)
      addrs.append(addr)
      reqnums.append(reqnum)
      for buffer, leaf in zip(buffers, leaves):
        buffer[index] = leaf
      if len(addrs) == batch_size:
        # TODO: Keep track of the shared memory buffers until the inner server
        # has responded and then close them. Later, we can also add them to a
        # free list to reuse them.
        del batches[name]
        data = unflatten(buffers, reference)
        job = inner.call(name, data)
        job.args = (True, addrs, reqnums)
        jobs.append(job)
      break  # We do not actually want to loop.

    done, pending = [], []
    [done.append(x) if x.done() else pending.append(x) for x in jobs]
    jobs = pending
    for job in done:
      batched, addr, reqnum = job.args
      try:
        result = job.result()
      except RuntimeError as e:
        if batched:
          for i, (addr, reqnum) in enumerate(zip(addr, reqnum)):
            send_error(addr, reqnum, 6, e.args[0])
        else:
          send_error(addr, reqnum, 6, e.args[0])
        continue
      status = int(0).to_bytes(8, 'little', signed=True)
      if batched:
        for i, (addr, reqnum) in enumerate(zip(addr, reqnum)):
          data = packlib.pack(treemap(lambda x: x[i], result))
          outer.send(addr, reqnum, status, *data)
      else:
        outer.send(addr, reqnum, status, *result)

  outer.close()
  inner.close()
