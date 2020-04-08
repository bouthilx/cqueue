from datetime import datetime
from collections import defaultdict
import bson
import time
import json
from multiprocessing import RLock
import zipfile


from msgqueue.uri import parse_uri
from msgqueue.backends.queue import QueueMonitor, Agent, Message


def cached(f):
    def wrapper(self, *args):
        with self.lock:
            # start = time.time()
            r = f(self, *args)
            # print(f'Function {f} took {time.time() - start}')
            return r
    return wrapper


def bson_encode(x):
    r = bson.decode(x.read())['data']
    return r


class ZipQueueMonitor(QueueMonitor):
    def __init__(self, uri):
        uri = parse_uri(uri)
        self.lock = RLock()
        self.zip = zipfile.ZipFile(uri.get('path', uri.get('address', None)))
        self._cache = {}
        self.format = 'json'
        # find the format
        self.queues(self.namespaces()[0])
        self.index = defaultdict(list)
        self.should_build_index = defaultdict(lambda: True)

    def archive(self, namespace, archive_name, namespace_out=None, format=None):
        """Archive a namespace into a zipfile and delete the namespace from the database"""
        raise RuntimeError('Already achieved')

    def clear(self, namespace, name):
        """Clear the queue by removing all messages"""
        raise RuntimeError('Archives are read-only')

    def reset_queue(self, namespace, name):
        """Hard reset the queue, putting all unactioned messages into an unread state"""
        raise RuntimeError('Archives are read-only')

    def requeue_lost_messages(self, namespace):
        raise RuntimeError('Archives are read-only')

    @cached
    def namespaces(self):
        namespaces = set()
        for name in self.zip.namelist():
            try:
                n, _ = name.split('/', maxsplit=2)
                namespaces.add(n)
            except ValueError:
                pass

        return list(namespaces)

    @cached
    def queues(self, namespace):
        queues = set()

        for name in self.zip.namelist():
            if name.startswith(namespace):
                try:
                    _, queue = name.split('/', maxsplit=2)

                    if queue.endswith('.json'):
                        self.format = 'json'
                        queues.add(queue[:-5])

                    elif queue.endswith('.bson'):
                        self.format = 'bson'
                        queues.add(queue[:-5])

                except ValueError:
                    pass

        queues.discard('logs')
        queues.discard('system')
        return list(queues)

    @property
    def loader(self):
        if self.format == 'json':
            return json.load
        else:
            return bson_encode

    @cached
    def agents(self, namespace):
        with self.lock:
            with self.zip.open(f'{namespace}/system.{self.format}', 'r') as queue:
                return list(Agent(**m) for m in self.loader(queue))

    @cached
    def messages(self, namespace, name, limit=100):
        with self.lock:
            with self.zip.open(f'{namespace}/{name}.{self.format}', 'r') as queue:
                m = list(self.build_index(namespace, name, Message(**m)) for m in self.loader(queue))
                self.should_build_index[(namespace, name)] = False
                return m

    def build_index(self, namespace, name, message):
        # indexing cause issues with Dash
        if self.should_build_index[(namespace, name)]:
            if message.read:
                self.index[(namespace, name, 'read')].append(message)
            else:
                self.index[(namespace, name, 'underead')].append(message)

            if message.actioned:
                self.index[(namespace, name, 'actioned')].append(message)
            else:
                self.index[(namespace, name, 'unactioned')].append(message)

            self.index[(namespace, name, message.mtype)].append(message)
        return message

    @cached
    def unread_messages(self, namespace, name):
        if len(self.index[(namespace, name, 'unread')]) > 0:
            return self.index[(namespace, name, 'unread')]

        messages = self.messages(namespace, name)

        unread = []
        for m in messages:
            if not m.read:
                unread.append(m)

        return unread

    @cached
    def unactioned_messages(self, namespace, name):
        if len(self.index[(namespace, name, 'unactioned')]) > 0:
            return self.index[(namespace, name, 'unactioned')]

        messages = self.messages(namespace, name)

        unread = []
        for m in messages:
            if not m.actioned:
                unread.append(m)

        return unread

    @cached
    def read_messages(self, namespace, name):
        if len(self.index[(namespace, name, 'read')]) > 0:
            return self.index[(namespace, name, 'read')]

        messages = self.messages(namespace, name)

        unread = []
        for m in messages:
            if m.read:
                unread.append(m)

        return unread

    @cached
    def actioned_messages(self, namespace, name):
        if len(self.index[(namespace, name, 'actioned')]) > 0:
            return self.index[(namespace, name, 'actioned')]

        messages = self.messages(namespace, name)

        unread = []
        for m in messages:
            if m.actioned:
                unread.append(m)

        return unread

    @cached
    def unread_count(self, namespace, name):
        return len(self.unread_messages(namespace, name))

    @cached
    def unactioned_count(self, namespace, name):
        return len(self.unactioned_messages(namespace, name))

    @cached
    def actioned_count(self, namespace, name):
        return len(self.actioned_messages(namespace, name))

    @cached
    def read_count(self, namespace, name):
        return len(self.read_messages(namespace, name))

    @cached
    def dead_agents(self, namespace, timeout_s=60):
        """Return a list of unresponsive agent"""
        lost = []

        with self.lock:
            for agent in self.agents(namespace):
                if agent.message and datetime.utcnow().timestamp() - agent.heartbeat > timeout_s:
                    lost.append(agent)

        return lost

    @cached
    def lost_messages(self, namespace, timeout_s=60):
        """Return the list of messages that were assigned to worker that died"""
        lost = []

        with self.lock:
            for agent in self.dead_agents(namespace, timeout_s):
                lost.append((agent.message, agent.queue))
        return lost

    @cached
    def failed_messages(self, namespace, queue):
        """Return the list of messages that failed because of an exception was raised"""
        failed = []
        for m in self.messages(namespace, queue):
            if m.error:
                failed.append(m)
        return failed

    @cached
    def log(self, namespace, agent, ltype: int = 0):
        """Return the log of an agent"""
        if isinstance(agent, Agent):
            agent = agent.uid

        with self.lock:
            with self.zip.open(f'{namespace}/logs/{agent}_{ltype}.txt', 'r') as log:
                return log.read().decode('utf-8')

    @cached
    def reply(self, namespace, name, uid):
        """Return the reply of a message"""
        with self.lock:
            for m in self.messages(namespace,  name):
                if m.replying_to == uid:
                    return m


def new_monitor(*args, **kwargs):
    return ZipQueueMonitor(*args, **kwargs)


if __name__ == '__main__':
    from msgqueue.backends import new_monitor
    monitor = ZipQueueMonitor('zip:/home/setepenre/work/olympus/data.zip')

    for m in monitor.messages('classification', 'OLYWORK'):
        print(m)

    for n in monitor.namespaces():
        print(n)

    for n in monitor.queues('classification'):
        print(n)

   #  monitor.stop()
