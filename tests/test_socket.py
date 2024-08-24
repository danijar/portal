import zerofun


class TestSocket:

  def test_basic(self):
    port = zerofun.free_port()
    server = zerofun.ServerSocket(port)
    client = zerofun.ClientSocket('localhost', port, connect=True)
    assert client.connected
    client.send(b'foo')
    addr, data = server.recv()
    assert addr[0] == '127.0.0.1'
    assert data == b'foo'
    server.send(addr, b'bar')
    assert client.recv() == b'bar'
    server.close()
    client.close()
