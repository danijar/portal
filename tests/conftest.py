import portal

# Do not expose server sockets externally to avoid popups on Mac.
portal.setup(host='localhost')
