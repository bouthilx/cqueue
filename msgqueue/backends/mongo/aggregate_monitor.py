import datetime
import pymongo
from bson.objectid import ObjectId
from threading import RLock

from msgqueue.uri import parse_uri
from msgqueue.backends.queue import Agent, to_dict

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

    def group_by_namespace(self, name, query, field_name):
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

    def lost_count(self, name, mtype=None, timeout_s=60):
        query = {
            'read': True,
            'actioned': False,
            'heartbeat': {
                'lt': datetime.datetime.utcnow() - datetime.timedelta(timeout_s)
            }
        }
        self.add_filter(query, 'mtype', mtype)
        return self.group_by_namespace(name, query, 'lost')

    def failed_count(self, name, mtype=None):
        query = {
            'error': {'$ne': None},
            'actioned': False,
            'read': True,
        }
        self.add_filter(query, 'mtype', mtype)
        return self.group_by_namespace(name, query, 'failed')

    def unread_count(self, name, mtype=None):
        with self.lock:
            query = {
                'read': False
            }
            self.add_filter(query, 'mtype', mtype)
            return self.group_by_namespace(name, query, 'unread')

    def unactioned_count(self, name, mtype=None):
        with self.lock:
            query = {
                'read': True,
                'actioned': False,
            }

            self.add_filter(query, 'mtype', mtype)
            return self.group_by_namespace(name, query, 'unactioned')

    def read_count(self, name, mtype=None):
        with self.lock:
            query = {
                'read': True
            }

            self.add_filter(query, 'mtype', mtype)
            return self.group_by_namespace(name, query, 'read')

    def actioned_count(self, name, mtype=None):
        with self.lock:
            query = {
                'read': True,
                'actioned': True,
            }

            self.add_filter(query, 'mtype', mtype)
            return self.group_by_namespace(name, query, 'actioned')
