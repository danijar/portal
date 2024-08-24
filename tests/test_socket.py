import time

import zerofun


class TestSocket:

  def test_basic(self):
    port = zerofun.free_port()

    server = zerofun.ServerSocket(port)

    client = zerofun.ClientSocket('localhost', port, connect=True)
    assert client.connected

    client.send(b'foo')
    addr, data = server.recv()
    assert data == b'foo'

    # while not (result := server.recv()):
    #   time.sleep(0.1)
    # assert result == b'foo'
