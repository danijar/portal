import pytest
import zerofun
import numpy as np


class TestPack:

  @pytest.mark.parametrize('value', [
      np.asarray(42),
      {'foo': np.zeros((2, 4), np.float64), 'bar': np.asarray(1)},
      {'foo': [np.asarray(1), np.asarray(2)]},
  ])
  def test_pack(self, value):
    buffers = zerofun.pack(value)
    buffer = b''.join(buffers)
    restored = zerofun.unpack(buffer)
    assert zerofun.tree_equals(value, restored)
