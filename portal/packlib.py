import struct

import msgpack
import numpy as np

from . import sharray


def pack(data):
  leaves, treedef = tree_flatten(data)
  specs, buffers = [], []
  for value in leaves:
    if value is None:
      specs.append(['none'])
      buffers.append(b'\x00')
    elif isinstance(value, str):
      specs.append(['utf8'])
      buffers.append(value.encode('utf-8'))
    elif isinstance(value, (bytes, bytearray, memoryview)):
      if isinstance(value, memoryview):
        value = value.cast('c')
        assert value.c_contiguous
      specs.append(['bytes'])
      buffers.append(value)
    elif isinstance(value, (np.ndarray, np.generic, int, float)):
      value = np.asarray(value)
      if value.dtype == object:
        raise TypeError(data)
      assert value.data.c_contiguous, (
          "Array is not contiguous in memory. Use " +
          "np.asarray(arr, order='C') before passing the data into pack().")
      specs.append(['array', value.shape, value.dtype.str])
      buffers.append(value.data.cast('c'))
    elif isinstance(value, sharray.SharedArray):
      specs.append(['sharray', *value.__getstate__()])
      buffers.append(b'\x00')
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
    if spec[0] == 'none':
      assert buffer == b'\x00'
      leaves.append(None)
    elif spec[0] == 'utf8':
      leaves.append(bytes(buffer).decode('utf-8'))
    elif spec[0] == 'bytes':
      leaves.append(buffer)
    elif spec[0] == 'array':
      shape, dtype = spec[1:]
      leaves.append(np.frombuffer(buffer, dtype).reshape(shape))
    elif spec[0] == 'sharray':
      assert buffer == b'\x00'
      leaves.append(sharray.SharedArray(*spec[1:]))
    else:
      raise NotImplementedError(spec)
  data = tree_unflatten(leaves, treedef)
  return data


def tree_map(fn, *trees, isleaf=None):
  assert trees, 'Provide one or more nested Python structures'
  kw = dict(isleaf=isleaf)
  first = trees[0]
  try:
    assert all(isinstance(x, type(first)) for x in trees)
    if isleaf and isleaf(trees[0]):
      return fn(*trees)
    if isinstance(first, list):
      assert all(len(x) == len(first) for x in trees)
      return [tree_map(
          fn, *[t[i] for t in trees], **kw) for i in range(len(first))]
    if isinstance(first, tuple):
      assert all(len(x) == len(first) for x in trees)
      return tuple([tree_map(
          fn, *[t[i] for t in trees], **kw) for i in range(len(first))])
    if isinstance(first, dict):
      assert all(set(x.keys()) == set(first.keys()) for x in trees)
      return {k: tree_map(fn, *[t[k] for t in trees], **kw) for k in first}
    if hasattr(first, 'keys') and hasattr(first, 'get'):
      assert all(set(x.keys()) == set(first.keys()) for x in trees)
      return type(first)(
          {k: tree_map(fn, *[t[k] for t in trees], **kw) for k in first})
  except AssertionError:
    raise TypeError('Tree structure differs between arguments')
  return fn(*trees)


def tree_flatten(tree, isleaf=None):
  leaves = []
  tree_map(lambda x: leaves.append(x), tree, isleaf=isleaf)
  structure = tree_map(lambda x: None, tree, isleaf=isleaf)
  return tuple(leaves), structure


def tree_unflatten(leaves, structure):
  leaves = iter(tuple(leaves))
  return tree_map(lambda x: next(leaves), structure)


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
  elif isinstance(xs, sharray.SharedArray):
    assert xs.name == ys.name, (xs.name, ys.name)
    assert xs.array.shape == ys.array.shape, (xs.array.shape, ys.array.shape)
    assert xs.array.dtype == ys.array.dtype, (xs.array.dtype, ys.array.dtype)
    return (xs == ys).all()
  else:
    return xs == ys
