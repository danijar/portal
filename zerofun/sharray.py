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
    weakref.finalize(self.arr, self.shm.close)

  @property
  def array(self):
    return self.arr

  def result(self):
    weakref.finalize(self.arr, self.close)
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
