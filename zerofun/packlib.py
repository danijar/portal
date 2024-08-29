import struct

import elements
import msgpack
import numpy as np


def pack(data):
  leaves, treedef = elements.tree.flatten(data)
  specs, buffers = [], []
  for value in leaves:
    if isinstance(value, str):
      specs.append(['utf8'])
      buffers.append(value.encode('utf-8'))
    elif isinstance(value, (bytes, bytearray, memoryview)):
      if isinstance(value, memoryview):
        value = value.cast('c')
        assert value.c_contiguous
      specs.append(['bytes'])
      buffers.append(value)
    elif isinstance(value, (np.ndarray, int, float)):
      value = np.asarray(value)
      if value.dtype == object:
        raise TypeError(data)
      assert value.data.c_contiguous, (
          "Array is not contiguous in memory. Use np.asarray(arr, order='C') " +
          "before passing the data into pack().")
      specs.append(['array', value.shape, value.dtype.str])
      buffers.append(value.data.cast('c'))
    else:
      raise NotImplementedError(type(value))
  buffers = [msgpack.packb(treedef), msgpack.packb(specs), *buffers]
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
  treedef, specs, *buffers = buffers
  treedef = msgpack.unpackb(treedef)
  specs = msgpack.unpackb(specs)
  leaves = []
  for spec, buffer in zip(specs, buffers):
    if spec[0] == 'utf8':
      leaves.append(bytes(buffer).decode('utf-8'))
    elif spec[0] == 'bytes':
      leaves.append(buffer)
    elif spec[0] == 'array':
      shape, dtype = spec[1:]
      leaves.append(np.frombuffer(buffer, dtype).reshape(shape))
    else:
      raise NotImplementedError(spec)
  data = elements.tree.unflatten(leaves, treedef)
  return data


def tree_equals(xs, ys):
  assert type(xs) == type(ys), (type(xs), type(ys))
  if isinstance(xs, (list, tuple)):
    assert len(xs) == len(ys)
    return all(tree_equals(x, y) for x, y in zip(xs, ys))
  elif isinstance(xs, dict):
    assert xs.keys() == ys.keys()
    return all(tree_equals(xs[k], ys[k]) for k in xs.keys())
  elif isinstance(xs, np.ndarray):
    assert xs.shape == ys.shape, (xs.shape, ys.shape)
    assert xs.dtype == ys.dtype, (xs.dtype, ys.dtype)
    return (xs == ys).all()
  else:
    return xs == ys
