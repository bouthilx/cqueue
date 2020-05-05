import datetime
import pymongo
from bson.objectid import ObjectId
from threading import RLock

from msgqueue.uri import parse_uri
from msgqueue.backends.queue import QueueMonitor, Agent, to_dict

from .util import _parse, _parse_agent


def mongo_to_dict(a):
    if isinstance(a, ObjectId):
        return str(a)

    return to_dict(a)


class MongoQueueMonitor(QueueMonitor):
    def __init__(self, uri, database, cursor=None):
        # When using this inside a dashbord it is executed in a multi threaded environment
        # You need to lock the cursor to not get some errors
        self.lock = RLock()

        if cursor is None:
            mongodb_uri = uri.replace('mongo', 'mongodb')
            uri = parse_uri(uri)
            if uri.get('username') is not None:
                self.client = pymongo.MongoClient(mongodb_uri)
            else:
                self.client = pymongo.MongoClient(host=uri['address'], port=int(uri['port']))
        else:
            self.client = cursor

        self.database = database
        self.db = self.client[self.database]
        self.last_times = []

    def namespaces(self, queue=None):
        with self.lock:
            if queue is not None:
                return list(self.db[queue].distinct('namespace'))

            return list(set(n['namespace'] for n in self.db.namespaces.find({})))

    def queues(self):
        with self.lock:
            return list(set(n['name'] for n in self.db.namespaces.find()))

    def reply(self, name, uid):
        with self.lock:
            msg = self.db[name].find_one({
                'replying_to': uid
            })
            return _parse(msg)

    @staticmethod
    def add_time_filter(query: dict, name, time):
        if time is not None:
            query[name] = {'$gt': time}
        return query

    def messages(self, name, namespace, limit=100, mtype=None, time=None):
        with self.lock:
            if isinstance(name, list):
                data = []
                for n in name:
                    data.extend(self.messages(namespace, n, limit))
                return data

            query = dict()
            self.add_filter(query, 'namespace', namespace)
            self.add_filter(query, 'mtype', mtype)
            self.add_time_filter(query, 'time', time)

            results = [_parse(msg) for msg in self.db[name].find(query, sort=[
                ('time', pymongo.ASCENDING),
            ])]

            return results

    def clear(self, name, namespace):
        with self.lock:
            if namespace is not None:
                self.db[name].delete_many({'namespace': namespace})
                self.db.qsystem.delete_many({'namespace': namespace})
            else:
                self.db[name].drop()

    def unread_messages(self, name, namespace, mtype=None):
        with self.lock:
            query = {
                'read': False
            }

            self.add_filter(query, 'namespace', namespace)
            self.add_filter(query, 'mtype', mtype)

            return [_parse(msg) for msg in self.db[name].find(query)]

    def unactioned_messages(self, name, namespace, mtype=None):
        with self.lock:
            query = {
                'read': True,
                'actioned': False,
            }

            self.add_filter(query, 'namespace', namespace)
            self.add_filter(query, 'mtype', mtype)
            return [_parse(msg) for msg in self.db[name].find(query)]

    def unactioned(self, name, namespace):
        with self.lock:
            return [_parse(msg) for msg in self.db[name].find({
                'actioned': False,
                'namespace': namespace
            })]

    @staticmethod
    def add_filter(query: dict, name, value):
        if value is not None:
            if isinstance(value, (tuple, list)):
                query[name] = {'$in': tuple(value)}
            else:
                query[name] = value
        return query

    def unread_count(self, name, namespace, mtype=None):
        with self.lock:
            query = {
                'read': False
            }

            self.add_filter(query, 'namespace', namespace)
            self.add_filter(query, 'mtype', mtype)
            return self.db[name].count(query)

    def unactioned_count(self, name, namespace, mtype=None):
        with self.lock:
            query = {
                'read': True,
                'actioned': False,
            }

            self.add_filter(query, 'namespace', namespace)
            self.add_filter(query, 'mtype', mtype)
            return self.db[name].count(query)

    def read_count(self, name, namespace, mtype=None):
        with self.lock:
            query = {
                'read': True
            }

            self.add_filter(query, 'namespace', namespace)
            self.add_filter(query, 'mtype', mtype)
            return self.db[name].count(query)

    def actioned_count(self, name, namespace, mtype=None):
        with self.lock:
            query = {
                'read': True,
                'actioned': True,
            }

            self.add_filter(query, 'namespace', namespace)
            self.add_filter(query, 'mtype', mtype)
            return self.db[name].count(query)

    def agent_count(self):
        with self.lock:
            return self.db.system.count()

    def reset_queue(self, name, namespace):
        with self.lock:
            query = {
                'actioned': False,
                'read':  True,
            }
            self.add_filter(query, 'namespace', namespace)

            rc = self.db[name].update_many(
                query,
                {'$set': {
                    'read': False,
                    'read_time': None
                }}
            )

    def dump(self, name, namespace):
        rows = self.db[name].find({'namespace': namespace})
        for row in rows:
            print(_parse(row))

    def agents(self, namespace):
        with self.lock:
            query = {}
            self.add_filter(query, 'namespace', namespace)

            agents = self.db.system.find(query)
            return [_parse_agent(agent) for agent in agents]

    def dead_agents(self, namespace, timeout_s=60):
        agents = self.db.system.find({
            'namespace': namespace,
            'heartbeat': {
                '$lt': datetime.datetime.utcnow() - datetime.timedelta(seconds=timeout_s)
            },
            'alive': {
                '$eq': True
            }
        })

        return [_parse_agent(agent) for agent in agents]

    def _lost_query(self, namespace, timeout_s=60):
        query = {
            'read': True,
            'actioned': False,
            'heartbeat': {
                '$lt': datetime.datetime.utcnow() - datetime.timedelta(seconds=timeout_s)
            }
        }

        self.add_filter(query, 'namespace', namespace)
        return query

    def lost_messages(self, queue, namespace, timeout_s=60):
        lost = self.db[queue].find(self._lost_query(namespace, timeout_s))
        return [_parse(msg) for msg in lost]

    def requeue_lost_messages(self, queue, namespace, timeout_s=60, max_retry=3):
        query = self._lost_query(namespace, timeout_s)
        query['retry'] = {
            '$lt': max_retry
        }

        self.db[queue].update_many(query, {
            '$set': {
                'read': False,
                'read_time': None,
                'error': None,
            },
            '$inc': {
                'retry': 1
            }
        })

    def _failed_query(self, namespace):
        query = {
            'error': {'$ne': None},
            'actioned': False,
            'read': True,
        }

        self.add_filter(query, 'namespace', namespace)
        return query

    def failed_messages(self, queue, namespace):
        failed = self.db[queue].find(self._failed_query(namespace))
        return [_parse(m) for m in failed]

    def requeue_failed_messages(self, queue, namespace, max_retry=3):
        query = self._failed_query(namespace)
        query['retry'] = {
            '$lt': max_retry
        }

        result = self.db[queue].update_many(query, {
            '$set': {
                'read': False,
                'read_time': None,
                'error': None,
            },
            '$inc': {
                'retry': 1
            }
        })
        return result.modified_count

    def log(self, agent, ltype=0):
        from bson import ObjectId
        if isinstance(agent, Agent):
            agent = agent.uid

        lines = self.db.logs.find({
            'agent': ObjectId(agent),
            'ltype': ltype
        })
        return ''.join([l['line'] for l in lines])

    def _log_types(self, namespace, agent):
        data = self.db.logs.find({
            'namespace': namespace,
            'agent': agent.uid
        })

        return set(r['ltype'] for r in data)

    def archive(self, namespace, archive_name, namespace_out=None, format='json'):
        def remove_db(nm):
            self.client.drop_database(nm)

        self._make_archive(
            namespace,
            archive_name,
            namespace_out,
            format,
            remove_db,
            self._log_types,
            self.lock,
            mongo_to_dict
        )

    def aggregate_monitor(self):
        from .aggregate_monitor import AggregateMonitor
        return AggregateMonitor(uri=None, database=self.database, cursor=self.client)


def new_monitor(*args, **kwargs):
    return MongoQueueMonitor(*args, **kwargs)
