import datetime
import os
import pymongo
import shutil
import signal
import subprocess
import time
import traceback
import threading
from threading import RLock

from multiprocessing import Process, Manager
from cqueue.logs import info, error
from cqueue.uri import parse_uri
from cqueue.backends.queue import Message, MessageQueue, Agent, QueueMonitor, QueuePacemaker

_base = os.path.dirname(os.path.realpath(__file__))


class MongoDB:
    def __init__(self, address, port, location, clean_on_exit=True):
        self.location = location
        self.data_path = f'{self.location}/db'
        self.pid_file = f'{self.location}/pid'

        os.makedirs(self.data_path, exist_ok=True)

        self.address = address
        self.port = port
        self.location = location
        self.bin = 'mongod'

        if self.bin is None:
            raise RuntimeError('Your OS is not supported')

        if not os.path.exists(self.bin):
            info('Using system binary')
            self.bin = 'mongod'

        self.arguments = [
            '--dbpath', self.data_path,
            '--wiredTigerCacheSizeGB', '1',
            '--port', str(port),
            '--bind_ip', address,
            '--pidfilepath', self.pid_file
        ]

        self.manager: Manager = Manager()
        self.properties = self.manager.dict()
        self.properties['running'] = False
        self.clean_on_exit = clean_on_exit
        self._process: Process = None
        self.cmd = None

    def _start(self, properties):
        kwargs = dict(
            args=' '.join([self.bin] + self.arguments),
            stdout=subprocess.PIPE,
            bufsize=1,
            stderr=subprocess.STDOUT
        )
        self.cmd = kwargs['args']

        with subprocess.Popen(**kwargs, shell=True) as proc:
            try:
                properties['running'] = True
                properties['pid'] = proc.pid

                while properties['running']:
                    if proc.poll() is None:
                        line = proc.stdout.readline().decode('utf-8')
                        if line:
                            self.parse(properties, line)
                    else:
                        properties['running'] = False
                        properties['exit'] = proc.returncode

            except Exception:
                error(traceback.format_exc())

    def start(self, wait=True):
        try:
            self._process = Process(target=self._start, args=(self.properties,))
            self._process.start()

            # wait for all the properties to be populated
            if wait:
                while self.properties.get('ready') is None:
                    time.sleep(0.01)

            self.properties['db_pid'] = int(open(self.pid_file, 'r').read())
            self._setup()

        except Exception as e:
            error(traceback.format_exc(e))

    def _setup(self, client='track_client'):
        pass

    def new_queue(self, namespace, name, client='default_user', clients=None):
        client = pymongo.MongoClient(
            host=self.address,
            port=self.port)

        queues = client[namespace]
        queue = queues[name]
        queue.create_index([
            ('time', pymongo.DESCENDING),
            ('mtype', pymongo.DESCENDING),
            ('read', pymongo.DESCENDING),
            ('actioned', pymongo.DESCENDING),
            ('replied_id', pymongo.DESCENDING)
        ])

        client.qsystem.namespaces.insert_one({
            'namespace': namespace,
            'name': name
        })

    def stop(self):
        self.properties['running'] = False
        self._process.terminate()

        try:
            os.kill(self.properties['db_pid'], signal.SIGTERM)
        except ProcessLookupError:
            pass

        if self.clean_on_exit:
            shutil.rmtree(self.location)

    def wait(self):
        while self._process.is_alive():
            time.sleep(0.01)

    def __enter__(self):
        self.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        if exc_type is not None:
            raise exc_type

    def parse(self, properties, line):
        line = line.strip()

        if line.endswith(f'waiting for connections on port {self.port}'):
            properties['ready'] = True


def start_message_queue(location, addrs, join=None, clean_on_exit=True):
    cockroach = MongoDB(location, addrs, join, clean_on_exit, schema=None)
    return cockroach


class MongoQueuePacemaker(QueuePacemaker):
    def __init__(self, agent, namespace, wait_time, capture):
        self.client = agent.client
        super(MongoQueuePacemaker, self).__init__(agent, namespace, wait_time, capture)

    def register_agent(self, agent_name):
        self.agent_id = self.client[self.namespace].system.insert_one({
            'time': datetime.datetime.utcnow(),
            'agent': agent_name,
            'heartbeat': datetime.datetime.utcnow(),
            'alive': True,
            'message': None,
            'queue': None
        }).inserted_id
        return self.agent_id

    def update_heartbeat(self):
        self.client[self.namespace].system.update_one(
            {'_id': self.agent_id},
            {'$set': {
                'heartbeat': datetime.datetime.utcnow()}
            })

    def register_message(self, name, message):
        if message is None:
            return None

        self.client[self.namespace].system.update_one({'_id': self.agent_id}, {
            '$set': {
                'message': message.uid,
                'queue': name
            }
        })
        return message

    def unregister_message(self, uid=None):
        self.client[self.namespace].system.update_one({
            '_id': self.agent_id,
            'message': uid}, {
            '$set': {
                'message': None,
                'queue': None}})

    def unregister_agent(self):
        self.client[self.namespace].system.update_one({'_id': self.agent_id}, {
            '$set': {'alive': False}
        })

    def insert_log_line(self, line, ltype=0):
        if self.agent_id is None:
            return

        self.client[self.namespace].logs.insert_one({
            'agent': self.agent_id,
            'ltype': ltype,
            'line': line
        })


def _parse_agent(agent):
    agent['uid'] = agent['_id']
    agent.pop('_id')
    return Agent(**agent)


def _parse(result):
    if result is None:
        return None

    return Message(
        result['_id'],
        result['time'],
        result['mtype'],
        result['read'],
        result['read_time'],
        result['actioned'],
        result['actioned_time'],
        result['replying_to'],
        result['message'],
    )


class MongoClient(MessageQueue):
    """Simple cockroach db queue client

    Parameters
    ----------
    uri: str
        mongodb://192.168.0.10:8123
    """

    def __init__(self, uri, namespace, name='worker'):
        uri = parse_uri(uri)
        self.name = name
        self.namespace = namespace
        self.client = pymongo.MongoClient(host=uri['address'], port=int(uri['port']))
        self.heartbeat_monitor = None

        self.capture = True
        self.timeout = 60

    def pacemaker(self, namespace, wait_time, capture):
        return MongoQueuePacemaker(self, namespace, wait_time, capture)

    def enqueue(self, name, message, mtype=0, replying_to=None):
        """See `~mlbaselines.distributed.queue.MessageQueue`"""
        return self.client[self.namespace][name].insert_one({
            'time': datetime.datetime.utcnow(),
            'mtype': mtype,
            'read': False,
            'read_time': None,
            'actioned': False,
            'actioned_time': None,
            'replying_to': replying_to,
            'message': message,
        }).inserted_id

    def dequeue(self, name):
        """See `~mlbaselines.distributed.queue.MessageQueue`"""
        msg = self.client[self.namespace][name].find_one_and_update(
            {'read': False},
            {'$set': {
                'read': True, 'read_time': datetime.datetime.utcnow()}
            },
            return_document=pymongo.ReturnDocument.AFTER
        )
        return self.heartbeat_monitor.register_message(name, _parse(msg))

    def mark_actioned(self, name, message: Message = None, uid: int = None):
        """See `~mlbaselines.distributed.queue.MessageQueue`"""
        if isinstance(message, Message):
            uid = message.uid

        self.client[self.namespace][name].find_one_and_update(
            {'_id': uid},
            {'$set': {
                'actioned': True,
                'actioned_time': datetime.datetime.utcnow()}
            }
        )
        self.heartbeat_monitor.unregister_message(uid)
        return message

    def get_reply(self, name, uid):
        return MongoQueueMonitor(
            cursor=self.client).get_reply(self.namespace, name, uid)


def start_mongod():
    from argparse import ArgumentParser
    import os

    parser = ArgumentParser()
    parser.add_argument('--address', type=str, default='localhost')
    parser.add_argument('--port', type=int, default=8123)
    parser.add_argument('--loc', type=str, default=os.getcwd())
    args = parser.parse_args()

    print(args.port)
    server = MongoDB(args.address, args.port, args.loc, False)

    server.start()


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
                '$gt': datetime.datetime.utcnow() + datetime.timedelta(timeout_s, granularity='seconds')
            },
            'alive': {
                '$eq': True
            }
        })

        return [_parse_agent(agent) for agent in agents]

    def fetch_lost_messages(self, namespace, timeout_s=60, reset_messages=False):
        agents = self.client[namespace].system.find({
            'heartbeat': {
                '$gt': datetime.datetime.utcnow() + datetime.timedelta(timeout_s)
            },
            'message': {
                '$ne': None
            }
        })

        if reset_messages:
            for a in agents:
                self.client[namespace][a.queue].update({
                    '_id': a.message,
                    'read': True,
                    'actioned': False
                }, {
                    'read': {'$set': False},
                    'read_time': {'$set': None}
                })

        msg = []
        for a in agents:
            msg.append(a.message, a.queue)

        return msg

    def get_log(self, namespace, agent, ltype=0):
        from bson import ObjectId

        lines = self.client[namespace].logs.find({
            'agent': ObjectId(agent),
            'ltype': ltype
        })
        print(namespace, agent)
        return ''.join([l['line'] for l in lines])
