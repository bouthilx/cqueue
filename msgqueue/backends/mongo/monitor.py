import datetime
import pymongo
from threading import RLock

from msgqueue.uri import parse_uri
from msgqueue.backends.queue import QueueMonitor, Agent

from .util import _parse, _parse_agent


class MongoQueueMonitor(QueueMonitor):
    def __init__(self, uri=None, cursor=None):
        # When using this inside a dashbord it is executed in a multi threaded environment
        # You need to lock the cursor to not get some errors
        self.lock = RLock()

        if cursor is None:
            uri = parse_uri(uri)
            self.client = pymongo.MongoClient(host=uri['address'], port=int(uri['port']))
        else:
            self.client = cursor

    def namespaces(self):
        with self.lock:
            return list(n['namespace'] for n in self.client.qsystem.namespaces.find({}))

    def queues(self, namespace):
        with self.lock:
            return list(n['name'] for n in self.client.qsystem.namespaces.find({'namespace': namespace}))

    def reply(self, namespace, name, uid):
        with self.lock:
            msg = self.client[namespace][name].find_one(
                {'replying_to': uid},
            )
            return _parse(msg)

    def messages(self, namespace, name, limit=100):
        with self.lock:
            if isinstance(name, list):
                data = []
                for n in name:
                    data.extend(self.messages(namespace, n, limit))
                return data

            return [
                _parse(msg) for msg in self.client[namespace][name].find({})]

    def unread_messages(self, namespace, name):
        with self.lock:
            return [
                _parse(msg) for msg in self.client[namespace][name].find({'read': False})]

    def unactioned_messages(self, namespace, name):
        with self.lock:
            return [
                _parse(msg) for msg in self.client[namespace][name].find({'actioned': False, 'read': True})]

    def unread_count(self, namespace, name):
        with self.lock:
            return self.client[namespace][name].count({'read': False})

    def unactioned_count(self, namespace, name):
        with self.lock:
            return self.client[namespace][name].count({'actioned': False, 'read': True})

    def read_count(self, namespace, name):
        with self.lock:
            return self.client[namespace][name].count({'read': True})

    def actioned_count(self, namespace, name):
        with self.lock:
            return self.client[namespace][name].count({'actioned': True})

    def agent_count(self, namespace):
        with self.lock:
            return self.client[namespace].system.count()

    def reset_queue(self, namespace, name):
        with self.lock:
            msgs = self.client[namespace][name].find({'actioned': False, 'read':  True})
            rc = self.client[namespace][name].update(
                {'actioned': False},
                {'$set': {
                    'read': False, 'read_time': None}
                }
            )

            items = []
            for msg in msgs:
                items.append(_parse(msg))

            return items

    def unactioned(self, namespace, name):
        with self.lock:
            return [_parse(msg) for msg in self.client[namespace][name].find({'actioned': False})]

    def dump(self, namespace, name):
        rows = self.client[namespace][name].find()
        for row in rows:
            print(_parse(row))

    def agents(self, namespace):
        with self.lock:
            agents = self.client[namespace].system.find()
            return [_parse_agent(agent) for agent in agents]

    def dead_agents(self, namespace, timeout_s=60):
        agents = self.client[namespace].system.find({
            'heartbeat': {
                '$gt': datetime.datetime.utcnow() + datetime.timedelta(timeout_s)
            },
            'alive': {
                '$eq': True
            }
        })

        return [_parse_agent(agent) for agent in agents]

    def lost_messages(self, namespace, timeout_s=60):
        agents = self.client[namespace].system.find({
            'heartbeat': {
                '$gt': datetime.datetime.utcnow() + datetime.timedelta(timeout_s)
            },
            'message': {
                '$ne': None
            }
        })

        msg = []
        for a in agents:
            msg.append((a.queue, a.message))

        return msg

    def requeue_lost_messages(self, namespace, timeout_s=60, max_retry=3):
        lost = self.lost_messages(namespace, timeout_s)
        for queue, message in lost:
            self.client[namespace][queue].update({
                '_id': message.uid,
                'read': True,
                'actioned': False,
                'retry': {'$lt': max_retry}
            }, {
                'read': {'$set': False},
                'read_time': {'$set': None},
                'error': {'$set': None},
                '$inc': {
                    'retry': 1
                }
            })

    def failed_messages(self, namespace, queue):
        failed = self.client[namespace][queue].find({
            'error': {'$ne': None}
        })
        return [_parse(m) for m in failed]

    def requeue_failed_messages(self, namespace, queue, max_retry=3):
        self.client[namespace][queue].update({
            'error': {'$ne': None},
            'actioned': False,
            'read': True,
            'retry': {'$lt': max_retry}
        }, {
            'read': {'$set': False},
            'read_time': {'$set': None},
            'error': {'$set': None},
            '$inc': {
                'retry': 1
            }
        })

    def log(self, namespace, agent, ltype=0):
        from bson import ObjectId
        if isinstance(agent, Agent):
            agent = agent.uid

        lines = self.client[namespace].logs.find({
            'agent': ObjectId(agent),
            'ltype': ltype
        })
        return ''.join([l['line'] for l in lines])


def new_monitor(*args, **kwargs):
    return MongoQueueMonitor(*args, **kwargs)
