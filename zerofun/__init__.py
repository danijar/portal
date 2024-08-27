__version__ = '3.0.0'

import multiprocessing as mp
try:
  mp.set_start_method('spawn')
except RuntimeError:
  pass

from .contextlib import setup
from .contextlib import context

from .thread import Thread
from .process import Process

from .server_socket import ServerSocket
from .client_socket import ClientSocket, Disconnected

from .client import Client
from .server import Server
from .proc_server import ProcServer

from .utils import run
from .utils import kill_proc
from .utils import kill_thread
from .utils import free_port
from .utils import pack
from .utils import unpack
