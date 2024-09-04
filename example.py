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
