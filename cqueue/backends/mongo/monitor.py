import datetime
import pymongo
from threading import RLock

from cqueue.uri import parse_uri
from cqueue.backends.queue import QueueMonitor

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

    def get_namespaces(self):
        with self.lock:
            return [(n['namespace'], n['name']) for n in self.client.qsystem.namespaces.find({})]

    def get_reply(self, namespace, name, uid):
        with self.lock:
            msg = self.client[namespace][name].find_one(
                {'replying_to': uid},
            )
            return _parse(msg)

    def get_all_messages(self, namespace, name, limit=100):
        with self.lock:
            return [
                _parse(msg) for msg in self.client[namespace][name].find({})]

    def get_unread_messages(self, namespace, name):
        with self.lock:
            return [
                _parse(msg) for msg in self.client[namespace][name].find({'read': False})]

    def get_unactioned_messages(self, namespace, name):
        with self.lock:
            return [
                _parse(msg) for msg in self.client[namespace][name].find({'actioned': False, 'read': True})]

    def unread_count(self, namespace, name):
        with self.lock:
            return self.client[namespace][name].count({'read': False})

    def unactioned_count(self, namespace, name):
        with self.lock:
            return self.client[namespace][name].count({'actioned': False})

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

    def get_unactioned(self, namespace, name):
        """See `~mlbaselines.distributed.queue.MessageQueue`"""
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

    def fetch_dead_agent(self, namespace, timeout_s=60):
        agents = self.client[namespace].system.find({
            'heartbeat': {
                '$gt': datetime.datetime.utcnow() + datetime.timedelta(timeout_s)
            },
            'alive': {
                '$eq': True
            }
        })

        return [_parse_agent(agent) for agent in agents]

    def fetch_lost_messages(self, namespace, timeout_s=60):
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

    def requeue_messages(self, namespace, timeout_s):
        lost = self.fetch_lost_messages(namespace, timeout_s)
        for queue, message in lost:
            self.client[namespace][queue].update({
                '_id': message.uid,
                'read': True,
                'actioned': False
            }, {
                'read': {'$set': False},
                'read_time': {'$set': None}
            })

    def get_log(self, namespace, agent, ltype=0):
        from bson import ObjectId

        lines = self.client[namespace].logs.find({
            'agent': ObjectId(agent),
            'ltype': ltype
        })
        return ''.join([l['line'] for l in lines])


def new_monitor(*args, **kwargs):
    return MongoQueueMonitor(*args, **kwargs)
