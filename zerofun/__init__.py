__version__ = '2.4.0'

print("""NOTE: Zerofun has been rewritten without ZMQ using raw Python sockets
and is now called Portal. You can keep using Zerofun but it will receive no
further updates. To suppress the message, pin the version to `zerofun==2.3.1`.
To upgrade, run `pip install portal` and visit github.com/danijar/portal. The
API is nearly the same but allows for more flexible function arguments and
offers higher performance and reliability.""")

import multiprocessing as mp
try:
  mp.set_start_method('spawn')
except RuntimeError:
  pass

from .client import Client
from .thread import Thread, StoppableThread
from .process import Process, StoppableProcess
from .utils import run
from .utils import port_free
from .utils import get_free_port
from .utils import warn_remote_error
from .utils import kill_proc
from .utils import kill_subprocs
from .utils import proc_alive
from .server import Server
from .proc_server import ProcServer
from .sockets import NotAliveError, RemoteError, ProtocolError
