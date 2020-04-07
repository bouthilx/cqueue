import json
from threading import RLock
import zipfile
from datetime import datetime

from msgqueue.uri import parse_uri
from msgqueue.backends.cockroach.util import _parse, _parse_agent
from msgqueue.backends.queue import QueueMonitor, Agent, Message


class ZipQueueMonitor(QueueMonitor):
    def __init__(self, uri):
        uri = parse_uri(uri)
        self.lock = RLock()
        self.zip = zipfile.ZipFile(uri.get('path', uri.get('address', None)))

    def archive(self, namespace, archive_name):
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

    def namespaces(self):
        namespaces = set()
        for name in self.zip.namelist():
            n, _ = name.split('/', maxsplit=2)
            namespaces.add(n)

        return list(namespaces)

    def queues(self, namespace):
        queues = set()

        for name in self.zip.namelist():
            if name.startswith(namespace):
                _, queue, _ = name.split('/', maxsplit=3)
                queues.add(queue)

        return list(queues)

    def agents(self, namespace):
        with self.lock:
            with self.zip.open(f'{namespace}/system.json', 'r') as queue:
                return (Agent(**m) for m in json.load(fp=queue))

    def messages(self, namespace, name, limit=100):
        with self.lock:
            with self.zip.open(f'{namespace}/{name}.json', 'r') as queue:
                return (Message(**m) for m in json.load(fp=queue))

    def unread_messages(self, namespace, name):
        unread = []
        for m in self.messages(namespace, name):
            if not m.read:
                unread.append(m)
        return unread

    def unactioned_messages(self, namespace, name):
        unread = []
        for m in self.messages(namespace, name):
            if not m.actioned:
                unread.append(m)
        return unread

    def read_messages(self, namespace, name):
        unread = []
        for m in self.messages(namespace, name):
            if m.read:
                unread.append(m)
        return unread

    def actioned_messages(self, namespace, name):
        unread = []
        for m in self.messages(namespace, name):
            if m.actioned:
                unread.append(m)
        return unread

    def unread_count(self, namespace, name):
        return len(self.unread_messages(namespace, name))

    def unactioned_count(self, namespace, name):
        return len(self.unactioned_messages(namespace, name))

    def actioned_count(self, namespace, name):
        return len(self.actioned_messages(namespace, name))

    def read_count(self, namespace, name):
        return len(self.read_messages(namespace, name))

    def dead_agents(self, namespace, timeout_s=60):
        """Return a list of unresponsive agent"""
        lost = []

        with self.lock:
            for agent in self.agents(namespace):
                if agent.message and datetime.utcnow().timestamp() - agent.heartbeat > timeout_s:
                    lost.append(agent)

        return lost

    def lost_messages(self, namespace, timeout_s=60):
        """Return the list of messages that were assigned to worker that died"""
        lost = []

        with self.lock:
            for agent in self.dead_agents(namespace, timeout_s):
                lost.append((agent.message, agent.queue))
        return lost

    def failed_messages(self, namespace, queue):
        """Return the list of messages that failed because of an exception was raised"""
        failed = []
        for m in self.messages(namespace, queue):
            if m.error:
                failed.append(m)
        return failed

    def log(self, namespace, agent, ltype: int = 0):
        """Return the log of an agent"""
        if isinstance(agent, Agent):
            agent = agent.uid

        with self.lock:
            with self.zip.open(f'{namespace}/logs/{agent}_{ltype}.txt', 'r') as log:
                return log.read().decode('utf-8')

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
    monitor = new_monitor('zip:/home/setepenre/work/olympus/data.zip')

    for m in monitor.messages('classification', 'OLYWORK'):
        print(m)
