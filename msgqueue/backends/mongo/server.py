import os
import pymongo
import shutil
import signal
import subprocess
import time
import traceback

from multiprocessing import Process, Manager

from msgqueue.backends.queue import QueueServer
from msgqueue.logs import info, error, debug
from msgqueue.uri import parse_uri

_base = os.path.dirname(os.path.realpath(__file__))


class MongoStartError(Exception):
    pass


def new_queue(db, namespace, name):
    queue = db[name]

    # Pop index
    index = [
        ('namespace', pymongo.DESCENDING),
        ('read', pymongo.DESCENDING),
        ('mtype', pymongo.DESCENDING),
    ]

    queue.create_index(index)

    # Other
    index = [
        ('time', pymongo.ASCENDING),
        ('replied_id', pymongo.DESCENDING),
        ('actioned', pymongo.DESCENDING),
    ]

    for i in index:
        queue.create_index([i])

    db.namespaces.insert_one({
        'namespace': namespace,
        'name': name
    })


class MongoDB(QueueServer):
    def __init__(self, uri, database, location, clean_on_exit=True):
        options = parse_uri(uri)
        address = options['address']
        port = options['port']

        self.location = location
        self.data_path = f'{self.location}/db'
        self.pid_file = f'{self.location}/pid'

        os.makedirs(self.data_path, exist_ok=True)

        if os.path.exists(self.pid_file):
            raise RuntimeError('MongoDB is already alive')

        self.address = address
        self.port = int(port)
        self.location = location
        self.bin = 'mongod'
        self.loglines = []

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

        self.database = database
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

        debug(self.cmd)
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
                raise

    def start(self, wait=True):
        try:
            self._process = Process(target=self._start, args=(self.properties,))
            self._process.start()

            # wait for all the properties to be populated
            wait_time = 0
            if wait:
                while self._process.is_alive() and self.properties.get('ready') is None and wait_time < 5:
                    time.sleep(0.01)
                    wait_time += 0.01

                if not self._process.is_alive():
                    raise MongoStartError('MongoDB died')

                if not self.properties.get('ready'):
                    raise MongoStartError('MongoDB could not start')

            self.properties['db_pid'] = int(open(self.pid_file, 'r').read())
            self._setup()
            return True

        except MongoStartError:
            raise

        except Exception as e:
            error(traceback.format_exc(e))
            return False

    def _setup(self, client='track_client'):
        pass

    def new_queue(self, db, namespace, name):
        client = pymongo.MongoClient(
            host=self.address,
            port=self.port)

        new_queue(client[db], namespace, name)

    def stop(self):
        self.properties['running'] = False
        self._process.terminate()

        # kill PID
        try:
            pid = self.properties.get('db_pid', None)
            if pid is None:
                pid = int(open(self.pid_file, 'r').read())

            os.kill(pid, signal.SIGINT)
            time.sleep(10)

            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                info('died gracefully')

        except ProcessLookupError:
            pass

        # ---
        try:
            os.remove(self.pid_file)
        except FileNotFoundError:
            pass
        
        if self.clean_on_exit:
            try:
                shutil.rmtree(self.location)
            except FileNotFoundError:
                pass

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
        debug(line[40:-1])
        line = line.strip()
        # save the init logs for debugging
        if '[initandlisten]' in line:
            self.loglines.append(line)

        if line.endswith(f'waiting for connections on port {self.port}'):
            properties['ready'] = True

        if 'shutting down with code:0' in line:
            pass

        elif 'shutting down' in line:
            print('\n'.join(self.loglines))
            raise RuntimeError(f'Closing because: `{line}`')


def new_server(uri, database, location, join=None, clean_on_exit=True):
    mongo = MongoDB(uri, database, location, clean_on_exit)
    return mongo


def start_server_main():
    from argparse import ArgumentParser
    import os

    parser = ArgumentParser()
    parser.add_argument('--address', type=str, default='localhost')
    parser.add_argument('--port', type=int, default=8123)
    parser.add_argument('--loc', type=str, default=os.getcwd())
    args = parser.parse_args()

    server = new_server(f'mongo://{args.address}:{args.port}', args.loc, None, False)
    server.start()
