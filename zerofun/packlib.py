import struct

import elements
import msgpack
import numpy as np


def pack(data):
  leaves, structure = elements.tree.flatten(data)
  dtypes, shapes, buffers = [], [], []
  for value in leaves:
    value = np.asarray(value)
    if value.dtype == object:
      raise TypeError(data)
    assert value.data.c_contiguous, (
        "Array is not contiguous in memory. Use np.asarray(arr, order='C') " +
        "before passing the data into pack().")
    dtypes.append(value.dtype.str)
    shapes.append(value.shape)
    buffers.append(value.data.cast('c'))
  meta = msgpack.packb((structure, dtypes, shapes))
  buffers = [meta, *buffers]
  length = len(buffers).to_bytes(8, 'little', signed=False)
  sizes = struct.pack('<' + ('Q' * len(buffers)), *[len(x) for x in buffers])
  buffers = [length, sizes, *buffers]
  return buffers


def unpack(buffer):
  length = int.from_bytes(buffer[:8], 'little', signed=False)
  buffer = buffer[8:]
  sizes = struct.unpack('<' + ('Q' * length), buffer[:8 * length])
  buffer = buffer[8 * length:]
  limits = np.cumsum(sizes)
  buffers = [buffer[i: j] for i, j in zip([0, *limits[:-1]], limits)]
  meta, *buffers = buffers
  structure, dtypes, shapes = msgpack.unpackb(meta)
  leaves = [
      np.frombuffer(b, d).reshape(s)
      for i, (d, s, b) in enumerate(zip(dtypes, shapes, buffers))]
  data = elements.tree.unflatten(leaves, structure)
  return data


def tree_equals(xs, ys):
  assert type(xs) == type(ys)
  if isinstance(xs, (list, tuple)):
    assert len(xs) == len(ys)
    return all(tree_equals(x, y) for x, y in zip(xs, ys))
  elif isinstance(xs, dict):
    assert xs.keys() == ys.keys()
    return all(tree_equals(xs[k], ys[k]) for k in xs.keys())
  elif isinstance(xs, np.ndarray):
    assert xs.shape == ys.shape
    assert xs.dtype == ys.dtype
    return (xs == ys).all()
  else:
    return xs == ys
