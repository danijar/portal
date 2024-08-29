import pytest
import zerofun
import numpy as np


class TestPack:

  @pytest.mark.parametrize('value', [
      np.asarray(42),
      bytes(7),
      {'foo': np.zeros((2, 4), np.float64), 'bar': np.asarray(1)},
      {'foo': [np.asarray(1), np.asarray(2)]},
      'hello world',
  ])
  def test_pack(self, value):
    buffers = zerofun.pack(value)
    buffer = b''.join(buffers)
    restored = zerofun.unpack(buffer)
    assert zerofun.tree_equals(value, restored)

  def test_sharray(self):
    content = np.arange(6).reshape(3, 2)
    value = zerofun.SharedArray((3, 2), np.float32)
    value.arr[:] = content
    buffers = zerofun.pack({'foo': value})
    buffer = b''.join(buffers)
    restored = zerofun.unpack(buffer)
    assert restored['foo'].arr.shape == (3, 2)
    assert (restored['foo'].arr == content).all()
    assert restored['foo'].shm.name == value.shm.name
