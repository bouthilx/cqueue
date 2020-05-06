import datetime
import pymongo
from threading import RLock

from msgqueue.uri import parse_uri
from msgqueue.backends.queue import QueueMonitor
from .util import _parse, _parse_agent


class AggregateMonitor(QueueMonitor):
    """Monitor the work queue across multiple namespaces"""
    def __init__(self, uri, database, cursor=None):
        # When using this inside a dashboard it is executed in a multi threaded environment
        # You need to lock the cursor to not get some errors
        self.lock = RLock()

        if cursor is None:
            uri = parse_uri(uri)
            self.client = pymongo.MongoClient(host=uri['address'], port=int(uri['port']))
        else:
            self.client = cursor

        self.database = database
        self.db = self.client[self.database]

    @staticmethod
    def add_time_filter(query: dict, name, time):
        if time is not None:
            query[name] = {'$gt': time}
        return query

    @staticmethod
    def add_filter(query: dict, name, value):
        if value is not None:
            if isinstance(value, (tuple, list)):
                query[name] = {'$in': tuple(value)}
            else:
                query[name] = value
        return query

    @staticmethod
    def _grouping(delimiter):
        if delimiter is None:
            return '$namespace'
        else:
            return '$g1'

    @staticmethod
    def _split_namespace(projection, query, group, delimiter=None):
        if delimiter is not None:
            projection['g0'] = {'$arrayElemAt': [{'$split': ['$namespace', delimiter]}, 0]}
            projection['g1'] = {'$arrayElemAt': [{'$split': ['$namespace', delimiter]}, 1]}
            query['g0'] = group
        else:
            query['namespace'] = group

    @staticmethod
    def _messages_projection():
        return {
            '_id': 1,
            'time': 1,
            'read': 1,
            'mtype': 1,
            'actioned': 1,
            'heartbeat': 1,
            'read_time': 1,
            'actioned_time': 1,
            'retry': 1,
            'error': 1,
            'message': 1,
            'replying_to': 1,
        }

    def _message_pipeline(self, query, group, delimiter, projection=None):
        if projection is None:
            projection = self._messages_projection()

        self._split_namespace(projection, query, group, delimiter)
        return [
            {'$project': projection},
            {'$match': query},
        ]

    def count_pipeline(self, field_name, query, group, delimiter=None):
        projection = self._messages_projection()
        projection['runtime'] = {'$subtract': ['$actioned_time', '$read_time']}

        grouping = self._grouping(delimiter)
        pipe = self._message_pipeline(query, group, delimiter, projection)
        pipe.append({
            '$group': {
                '_id': grouping,
                field_name: {
                    '$sum': 1
                },
                'runtime': {
                    '$avg': '$runtime'
                }
            }
        })
        return pipe

    def _agent_pipeline(self, query, group, delimiter=None):
        projection = {
            '_id': 1,
            'time': 1,
            'agent': 1,
            'heartbeat': 1,
            'alive': 1,
            'namespace': 1,
            'message': 1,
            'queue': 1,
        }

        self._split_namespace(projection, query, group, delimiter)
        return [
            {'$project': projection},
            {'$match': query},
        ]

    def agents(self, group, delimiter=None):
        pipe = self._agent_pipeline({}, group, delimiter)
        return self.db.system.aggregate(pipe)

    def agent_count(self, group, delimiter=None):
        pipe = self._agent_pipeline({
            'alive': True,
        }, group, delimiter)

        grouping = self._grouping(delimiter)
        pipe.append({'$group': {
            '_id': grouping,
            'agent': {
                '$sum': 1
            }
        }})

        return self.db.system.aggregate(pipe)

    def lost_count(self, name, group, delimiter=None, mtype=None, timeout_s=120):
        query = {
            'read': True,
            'actioned': False,
            'heartbeat': {
                'lt': datetime.datetime.utcnow() - datetime.timedelta(timeout_s)
            }
        }
        self.add_filter(query, 'mtype', mtype)
        pipe = self.count_pipeline('lost', query, group, delimiter)
        return self.db[name].aggregate(pipe)

    def failed_count(self, name, group, mtype=None, delimiter=None):
        query = {
            'error': {'$ne': None},
            'actioned': False,
            'read': True,
        }
        self.add_filter(query, 'mtype', mtype)
        pipe = self.count_pipeline('failed', query, group, delimiter)
        return self.db[name].aggregate(pipe)

    def unread_count(self, name, group, mtype=None, delimiter=None):
        with self.lock:
            query = {
                'read': False
            }
            self.add_filter(query, 'mtype', mtype)
            pipe = self.count_pipeline('unread', query, group, delimiter)
            return self.db[name].aggregate(pipe)

    def unactioned_count(self, name, group, mtype=None, delimiter=None):
        with self.lock:
            query = {
                'read': True,
                'actioned': False,
            }

            self.add_filter(query, 'mtype', mtype)
            pipe = self.count_pipeline('unactioned', query, group, delimiter)
            print(delimiter, pipe)
            return self.db[name].aggregate(pipe)

    def read_count(self, name, group, mtype=None, delimiter=None):
        with self.lock:
            query = {
                'read': True
            }

            self.add_filter(query, 'mtype', mtype)
            pipe = self.count_pipeline('read', query, group, delimiter)
            return self.db[name].aggregate(pipe)

    def actioned_count(self, name, group, mtype=None, delimiter=None):
        with self.lock:
            query = {
                'read': True,
                'actioned': True,
            }

            self.add_filter(query, 'mtype', mtype)
            pipe = self.count_pipeline('actioned', query, group, delimiter)
            return self.db[name].aggregate(pipe)

    def messages(self, queue, group, mtype=None, limit=None, time=None, delimiter=None):
        with self.lock:
            query = dict()
            self.add_filter(query, 'mtype', mtype)
            self.add_time_filter(query, 'time', time)

            pipe = self._message_pipeline(query, group, delimiter)
            return [_parse(m) for m in self.db[queue].aggregate(pipe)]
