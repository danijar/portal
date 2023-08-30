[![PyPI](https://img.shields.io/pypi/v/zerofun.svg)](https://pypi.python.org/pypi/zerofun/#history)

# 🙅 Zerofun

Remote function calls for array data using [ZMQ](https://zeromq.org/).

## Overview

Zerofun provides a `Server` that you can bind functions to and a `Client` that
can call the messages and receive their results. The function inputs and
results are both flat **dicts of Numpy arrays**. The data is sent efficiently
without serialization to maximize throughput.

## Installation

```sh
pip install zerofun
```

## Example

This example runs the server and client in the same Python program using
subprocesses, but they could also be separate Python scripts running on
different machines.

```python
def server():
  import zerofun
  server = zerofun.Server('tcp://*:2222')
  server.bind('add', lambda data: {'result': data['foo'] + data['bar']})
  server.bind('msg', lambda data: print('Message from client:', data['msg']))
  server.run()

def client():
  import zerofun
  client = zerofun.Client('tcp://localhost:2222')
  client.connect()
  future = client.add({'foo': 1, 'bar': 1})
  result = future.result()
  print(result)  # {'result': 2}
  client.msg({'msg': 'Hello World'})

if __name__ == '__main__':
  import zerofun
  server_proc = zerofun.Process(server, start=True)
  client_proc = zerofun.Process(client, start=True)
  client_proc.join()
  server_proc.terminate()
```

## Features

Several productivity and performance features are available:

- **Request batching:** The server can batch requests together so that the user
  function receives a dict of stacked arrays and the function result will be
  split and sent back to the corresponding clients.
- **Multithreading:** Servers can use a thread pool to process multiple
  requests in parallel. Optionally, each function can also request its own
  thread pool to allow functions to block (e.g. for rate limiting) without
  blocking other functions.
- **Async clients:** Clients can send multiple overlapping requests and wait
  on the results when needed using `Future` objects. The maximum number of
  inflight requests can be limited to avoid requests building up when the
  server is slower than the client.
- **Error handling:** Exceptions raised in server functions are reported to the
  client and raised in `future.result()` or, if the user did not store the
  future object, on the next request. Worker exception can also be reraised in
  the server application using `server.check()`.
- **Heartbeating:** Clients can send ping requests when they have not received
  a result from the server for a while, allowing to wait for results that take
  a long time to compute without assuming connection loss.
- **Concurrency:** `Thread` and `Process` implementations with exception
  forwarding that can be forcefully terminated by the parent, which Python
  threads do not natively support. Stoppable threads and processes are also
  available for coorperative shutdown.
- **GIL load reduction:** The `ProcServer` behaves just like the normal
  `Server` but uses a background process to batch requests and fan out results,
  substantially reducing GIL load for the server workers in the main process.

## Questions

Please open a [GitHub issue](https://github.com/danijar/zerofun/issues) for
each question. Over time, we will add common questions to the README.
