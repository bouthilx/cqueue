__descr__ = 'Message Queue Primitives'
__version__ = '0.9.0'
__license__ = 'BSD-3-Clause'
__author__ = u'Pierre Delaunay'
__author_short__ = u'Delaunay'
__author_email__ = 'pierre@delaunay.io'
__copyright__ = u'2017-2019 Pierre Delaunay'
__url__ = 'https://github.com/Delaunay/cqueue'


from msgqueue.backends.queue import QueueServer
from msgqueue.backends.queue import MessageQueue
from msgqueue.backends.queue import QueueMonitor

from msgqueue.backends import new_monitor, new_client, new_server
from msgqueue.future import Future
