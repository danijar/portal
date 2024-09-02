import zerofun

# Do not expose server sockets externally to avoid popups on Mac.
zerofun.setup(hostname='localhost')
