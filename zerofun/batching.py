import threading

import elements  # TODO
import numpy as np
import zerofun

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
      self, port, name='Server', workers=1, errors=True,
      process=True, shmem=False, **kwargs):
    inner_port = utils.free_port()
    self.name = name
    self.server = server.Server(inner_port, name, workers, errors, **kwargs)
    if process:
      self.running = zerofun.context().mp.Event()
    else:
      self.running = threading.Event()
    self.process = process
    self.batsizes = {}
    self.batargs = (
        self.running, port, inner_port, f'{name}Batcher',
        self.batsizes, errors, shmem, kwargs)
    self.started = False

  def bind(self, name, workfn, donefn=None, batch=0, workers=0):
    assert not self.started
    self.batsizes[name] = batch
    self.server.bind(name, workfn, donefn, workers=workers)

  def start(self, block=True):
    assert not self.started
    self.started = True
    self.running.set()
    if self.process:
      self.batcher = process.Process(
          batcher, *self.batargs, name=f'{self.name}Batcher', start=True)
    else:
      self.batcher = thread.Thread(
          batcher, *self.batargs, name=f'{self.name}Batcher', start=True)
    self.server.start(block=block)

  def close(self, timeout=None):
    assert self.started
    self.running.clear()
    self.server.close(timeout and 0.5 * timeout)
    self.batcher.join(timeout and 0.5 * timeout)
    self.batcher.kill()

  def stats(self):
    return self.server.stats()

  def __enter__(self):
    self.start(block=False)
    return self

  def __exit__(self, *e):
    self.close()


def batcher(
    running, outer_port, inner_port, name, batsizes, errors, shmem,
    kwargs):

  outer = server_socket.ServerSocket(outer_port, name, **kwargs)
  inner = client.Client('localhost', inner_port, name, **kwargs, maxinflight=9999999)  # TODO
  batches = {}  # {method: ([addr], [reqnum], structure, [array])}
  jobs = []

  def send_error(addr, reqnum, status, message):
    assert 1 <= status, status
    status = status.to_bytes(8, 'little', signed=False)
    data = message.encode('utf-8')
    outer.send(addr, reqnum, status, data)
    if errors:
      raise RuntimeError(message)

  try:
    while running.is_set() or jobs:

      while True:  # Loop syntax used to break on error.
        if not running.is_set():  # Do not accept further requests.
          break
        try:
          # TODO: Tune timeouts.
          addr, data = outer.recv(timeout=0.0001)
        except TimeoutError:
          break
        reqnum, data = bytes(data[:8]), data[8:]
        strlen, data = int.from_bytes(data[:8], 'little', signed=False), data[8:]
        name, data = bytes(data[:strlen]).decode('utf-8'), data[strlen:]
        if name not in batsizes:
          send_error(addr, reqnum, 3, f'Unknown method {name}')
          break
        data = packlib.unpack(data)
        batch_size = batsizes[name]
        if not batch_size:
          job = inner.call(name, *data)
          job.args = (False, addr, reqnum)
          jobs.append(job)
          break
        leaves, structure = elements.tree.flatten(data)
        leaves = [np.asarray(x) for x in leaves]
        if any(x.dtype == object for x in leaves):
          send_error(addr, reqnum, 5, 'Only array arguments can be batched.')
          break
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
          send_error(addr, reqnum, 6, (
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
          data = elements.tree.unflatten(buffers, reference)
          job = inner.call(name, *data)
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
            data = packlib.pack(elements.tree.map(lambda x: x[i], result))
            outer.send(addr, reqnum, status, *data)
        else:
          data = packlib.pack(result)
          outer.send(addr, reqnum, status, *data)

  finally:
    outer.close()
    inner.close()
