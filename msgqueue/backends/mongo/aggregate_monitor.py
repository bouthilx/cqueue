import datetime
import pymongo
from threading import RLock

from msgqueue.uri import parse_uri
from .util import _parse, _parse_agent


class AggregateMonitor:
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
    def add_filter(query: dict, name, value):
        if value is not None:
            if isinstance(value, (tuple, list)):
                query[name] = {'$in': tuple(value)}
            else:
                query[name] = value
        return query

    def group_by_substring(self, name, query, field_name, group, delimiter=None):
        projection = {
            'read': 1,
            'actioned': 1,
            'heartbeat': 1,
            'error': 1,
            'runtime': {'$subtract': ['$actioned_time', '$read_time']}
        }

        # We cant use regex because we want to extract the matching group
        # mongodb regex only matches b
        if delimiter is None:
            projection['group']   = {'$substr': ['$namespace', 0, len(group)]}
            projection['section'] = {'$substr': ['$namespace', len(group), -1]}
            query['group'] = group
            grouping = '$section'
        else:
            projection['groups'] = {'$split': ['$namespace', delimiter]}
            projection['g0'] = {'$arrayElemAt': [{'$split': ['$namespace', delimiter]}, 0]}
            projection['g1'] = {'$arrayElemAt': [{'$split': ['$namespace', delimiter]}, 1]}
            query['g0'] = group
            grouping = '$g1'

        return self.db[name].aggregate([
            {'$project': projection},
            {'$match': query},
            {'$group': {
                '_id': f'{grouping}',
                field_name: {
                    '$sum': 1
                },
                'runtime': {
                    '$avg': '$runtime'
                }
            }}
        ])

    def group_by_namespace(self, name, query, field_name, length=None):
        return self.db[name].aggregate([
            {'$match': query},
            {'$group': {
                '_id': '$namespace',
                field_name: {
                    '$sum': 1
                }
            }}
        ])

    def agent_count(self):
        counts = self.db.system.aggregate([
            {'$match': {
                'alive': True
            }},
            {'$group': {
                '_id': '$namespace',
                'agent': {
                    '$sum': 1
                }
            }}
        ])
        return counts

    def lost_count(self, name, mtype=None, timeout_s=60, groupby=group_by_namespace):
        query = {
            'read': True,
            'actioned': False,
            'heartbeat': {
                'lt': datetime.datetime.utcnow() - datetime.timedelta(timeout_s)
            }
        }
        self.add_filter(query, 'mtype', mtype)
        return groupby(self, name, query, 'lost')

    def failed_count(self, name, mtype=None, groupby=group_by_namespace):
        query = {
            'error': {'$ne': None},
            'actioned': False,
            'read': True,
        }
        self.add_filter(query, 'mtype', mtype)
        return groupby(self, name, query, 'failed')

    def unread_count(self, name, mtype=None, groupby=group_by_namespace):
        with self.lock:
            query = {
                'read': False
            }
            self.add_filter(query, 'mtype', mtype)
            return groupby(self, name, query, 'unread')

    def unactioned_count(self, name, mtype=None, groupby=group_by_namespace):
        with self.lock:
            query = {
                'read': True,
                'actioned': False,
            }

            self.add_filter(query, 'mtype', mtype)
            return groupby(self, name, query, 'unactioned')

    def read_count(self, name, mtype=None, groupby=group_by_namespace):
        with self.lock:
            query = {
                'read': True
            }

            self.add_filter(query, 'mtype', mtype)
            return groupby(self, name, query, 'read')

    def actioned_count(self, name, mtype=None, groupby=group_by_namespace):
        with self.lock:
            query = {
                'read': True,
                'actioned': True,
            }

            self.add_filter(query, 'mtype', mtype)
            return groupby(self, name, query, 'actioned')

    def messages(self, queue, group, delimiter=None):
        projection = {
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
            'g0': {'$arrayElemAt': [{'$split': ['$namespace', delimiter]}, 0]},
            'g1': {'$arrayElemAt': [{'$split': ['$namespace', delimiter]}, 1]},
        }

        query = {
            'g0': group
        }

        return [_parse(m) for m in self.db[queue].aggregate([
            {'$project': projection},
            {'$match': query}
        ])]
