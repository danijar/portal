import math
import weakref
from multiprocessing import shared_memory

import numpy as np


class SharedArray:

  def __init__(self, shape, dtype, name=None):
    if name:
      self.shm = shared_memory.SharedMemory(name=name)
    else:
      size = math.prod(shape) * np.dtype(dtype).itemsize
      self.shm = shared_memory.SharedMemory(create=True, size=size)
    self.arr = np.ndarray(shape, dtype, self.shm.buf)
    # This unlinks the shared memory buffer, but it will survive until the last
    # process closes up their file pointer. This could cause a problem if a
    # SharedArray is serialize and the Python object goes out of scope before
    # trying to deserialize it (in the same or a different process).
    weakref.finalize(self.arr, self.close)

  @property
  def name(self):
    return self.shm.name

  @property
  def array(self):
    return self.arr

  def result(self):
    return self.arr

  def close(self):
    self.arr = None
    try:
      self.shm.unlink()
    except FileNotFoundError:
      pass

  def __getstate__(self):
    return (self.arr.shape, self.arr.dtype.str, self.shm.name)

  def __setstate__(self, args):
    self.__init__(*args)
