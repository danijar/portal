__version__ = '3.1.4'

import multiprocessing as mp
try:
  mp.set_start_method('spawn')
except RuntimeError:
  pass

from .contextlib import context
from .contextlib import initfn
from .contextlib import reset
from .contextlib import setup

from .thread import Thread
from .process import Process

from .server_socket import ServerSocket
from .client_socket import ClientSocket
from .client_socket import Disconnected

from .client import Client
from .server import Server
from .batching import BatchServer

from .packlib import pack
from .packlib import unpack
from .packlib import tree_equals

from .sharray import SharedArray

from .utils import free_port
from .utils import kill_procs
from .utils import kill_threads
from .utils import run
