import os
import pymongo
import shutil
import signal
import subprocess
import time
import traceback

from multiprocessing import Process, Manager
from cqueue.logs import info, error


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


def new_server(location, address, port, join=None, clean_on_exit=True):
    cockroach = MongoDB(location, f'{address}:{port}', join, clean_on_exit, schema=None)
    return cockroach


def start_server_main():
    from argparse import ArgumentParser
    import os

    parser = ArgumentParser()
    parser.add_argument('--address', type=str, default='localhost')
    parser.add_argument('--port', type=int, default=8123)
    parser.add_argument('--loc', type=str, default=os.getcwd())
    args = parser.parse_args()

    server = new_server(args.loc, args.address, args.port, None, False)
    server.start()
