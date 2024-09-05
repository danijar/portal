[![PyPI](https://img.shields.io/pypi/v/portal.svg)](https://pypi.python.org/pypi/portal/#history)

# 🌀 Portal

Fast and reliable distributed systems in Python.

## Features

- 📡 **Communication:** Portal lets you bind functions to a `Server` and call
  them from one or more `Client`s. Wait on results via `Future` objects.
  Clients can automatically restore broken connections.
- 🚀 **Performance:** Optimized for throughput and latency. Array data is
  zero-copy serialized and deserialized for throughput near the hardware limit.
- 🤸 **Flexibility:** Function inputs and outputs can be nested dicts and lists
  of numbers, strings, bytes, None values, and Numpy arrays. Bytes allow
  applications to chose their own serialization, such as `pickle`.
- 🚨 **Error handlings:** Provides `Process` and `Thread` objects that can
  reliably be killed by the parent. Unhandled exceptions in threads stop
  the program. Error files can be used to stop distributed systems.
- 📦 **Request batching:** Use `BatchServer` to collect multiple incoming
  requests and process them at once, for example for AI inference servers.
  Batching and dispatching happens in a separate process to free the GIL.
- ✅ **Correctness:** Covered by over 100 unit tests for common usage and edge
  cases and used for large scale distributed AI systems.

## Installation

```sh
pip install portal
```

## Example

This example runs the server and client in the same Python program using
subprocesses, but they could also be separate Python scripts running on
different machines.

```python
def server():
  import portal
  server = portal.Server(2222)
  server.bind('add', lambda x, y: x + y)
  server.bind('greet', lambda msg: print('Message from client:', msg))
  server.start()

def client():
  import portal
  client = portal.Client('localhost', 2222)
  future = client.add(12, 42)
  result = future.result()
  print(result)  # 54
  client.greet('Hello World')

if __name__ == '__main__':
  import portal
  server_proc = portal.Process(server, start=True)
  client_proc = portal.Process(client, start=True)
  client_proc.join()
  server_proc.kill()
  print('Done')
```

## Questions

Please open a separate [GitHub issue](https://github.com/danijar/portal/issues)
for each question.
