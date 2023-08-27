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
