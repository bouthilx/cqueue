import os
import shutil
import signal
import subprocess
import time
import traceback
import psycopg2

from multiprocessing import Process, Manager

from cqueue.uri import parse_uri
from cqueue.logs import debug, info, error, warning


VERSION = '19.1.1'

COCKROACH_HASH = {
    'posix': '051b9f3afd3478b62e3fce0d140df6f091b4a1e4ef84f05c3f1c3588db2495fa',
    'macos': 'ec1fe3dfb55c67b74c3f04c15d495a55966b930bb378b416a5af5f877fb093de'
}

_base = os.path.dirname(os.path.realpath(__file__))

COCKROACH_BIN = {
    'posix': f'{_base}/bin/cockroach'
}


def track_schema(clients):
    permissions = []
    for client in clients:
        permissions.append(f'CREATE USER IF NOT EXISTS {client};')
        permissions.append(f'GRANT ALL ON DATABASE track TO {client};')
    permissions = '\n'.join(permissions)

    return f"""
    CREATE DATABASE IF NOT EXISTS track;
    SET DATABASE = track;
    {permissions}
    CREATE TABLE IF NOT EXISTS track.projects (
        uid             BYTES PRIMARY KEY,
        name            STRING,
        description     STRING,
        metadata        JSONB,
        trial_groups    BYTES[],
        trials          BYTES[]
    );
    CREATE TABLE IF NOT EXISTS track.trial_groups (
        uid         BYTES PRIMARY KEY,
        name        STRING,
        description STRING,
        metadata    JSONB,
        trials      BYTES[],
        project_id  BYTES
    );
    CREATE TABLE IF NOT EXISTS track.trials (
        uid         BYTES,
        hash        BYTES,
        revision    SMALLINT,
        name        STRING,
        description STRING,
        tags        JSONB,
        version     BYTES,
        group_id    BYTES,
        project_id  BYTES,
        parameters  JSONB,
        metadata    JSONB,
        metrics     JSONB,
        chronos     JSONB,
        status      JSONB,
        errors      JSONB,
        PRIMARY KEY (hash, revision)
    );""".encode('utf8')


def system_schema(clients):
    user = []
    for client in clients:
        user.append(f'CREATE USER IF NOT EXISTS {client};')
    user = '\n'.join(user)

    permissions = []
    for client in clients:
        permissions.append(f'GRANT ALL ON DATABASE archive TO {client};')
        permissions.append(f'GRANT ALL ON TABLE archive.messages TO {client};')
        permissions.append(f'GRANT ALL ON DATABASE qsystem TO {client};')
        permissions.append(f'GRANT ALL ON TABLE qsystem.namespaces TO {client};')
    permissions = '\n'.join(permissions)

    return f"""
    {user}
    CREATE DATABASE IF NOT EXISTS qsystem;
    CREATE TABLE IF NOT EXISTS qsystem.namespaces (
        namespace       character(64),
        name            character(64)
    );

    CREATE DATABASE IF NOT EXISTS archive;
    SET DATABASE = archive;
    CREATE TABLE IF NOT EXISTS archive.messages (
        namespace       character(64),
        name            character(64),
        uid             INTEGER,
        time            TIMESTAMP   DEFAULT current_timestamp(),
        mtype           INT,
        read            BOOLEAN,
        read_time       TIMESTAMP,
        actioned        BOOLEAN,
        actioned_time   TIMESTAMP,
        replying_to     INTEGER,
        message         JSONB
    );

    CREATE INDEX IF NOT EXISTS archive_messages_index
    ON archive.messages  (
        namespace   DESC,
        name        DESC,
        read        DESC, 
        time        DESC,
        actioned    DESC,
        mtype       ASC,
        replying_to DESC
    );
    {permissions}
    """


def message_queue_schema(clients, namespace, name):
    """Create a message queue table

    uid          : message uid to update messages
    time         : timestamp when the message was created
    read         : was the message read
    read_time    : when was the message read
    actioned     : was the message actioned
    actioned_time: when was the message actioned
    message      : the message
    """
    assert name != 'system', 'system name is reserved'
    assert name != 'logs', 'logs name is reserved'

    permissions = []
    for client in clients:
        permissions.append(f'GRANT ALL ON DATABASE {namespace} TO {client};')
        permissions.append(f'GRANT ALL ON TABLE {namespace}.{name} TO {client};')
        permissions.append(f'GRANT ALL ON TABLE {namespace}.system TO {client};')
        permissions.append(f'GRANT ALL ON TABLE {namespace}.logs TO {client};')
    permissions = '\n'.join(permissions)

    return f"""
    CREATE DATABASE IF NOT EXISTS {namespace};
    SET DATABASE = {namespace};
    CREATE TABLE IF NOT EXISTS {namespace}.{name} (
        uid             SERIAL      PRIMARY KEY,
        time            TIMESTAMP   DEFAULT current_timestamp(),
        mtype           INT,
        read            BOOLEAN,
        read_time       TIMESTAMP,
        actioned        BOOLEAN,
        actioned_time   TIMESTAMP,
        replying_to     INTEGER,
        message         JSONB
    );
    CREATE TABLE IF NOT EXISTS {namespace}.system (
        uid             SERIAL      PRIMARY KEY,
        time            TIMESTAMP   DEFAULT current_timestamp(),
        agent           JSONB,
        heartbeat       TIMESTAMP   DEFAULT current_timestamp(),
        alive           BOOLEAN     DEFAULT true,
        message         INTEGER,
        queue           character(64)
    );
    CREATE TABLE IF NOT EXISTS {namespace}.logs (
        uid             SERIAL      PRIMARY KEY,
        agent           INTEGER,
        ltype           INT,
        line            JSONB
    );

    CREATE INDEX IF NOT EXISTS messages_index
    ON {namespace}.{name} (
        read        DESC, 
        time        DESC,
        actioned    DESC,
        mtype       ASC,
        replying_to DESC
    );

    CREATE INDEX IF NOT EXISTS system_index
    ON {namespace}.system (
        uid         ASC,
        message     ASC,
        queue       ASC
    );

    CREATE INDEX IF NOT EXISTS logs_index
    ON {namespace}.logs (
        uid         ASC,
        ltype       ASC,
        agent       ASC
    );
    {permissions}
    INSERT INTO qsystem.namespaces(namespace, name) VALUES ('{namespace}', '{name}');
    """


def new_queue(client, username, namespace, queue):
    client.execute(message_queue_schema([username], namespace, queue))


class CockRoachDB:
    """ cockroach db is a highly resilient database that allow us to remove
    the Master in a traditional distributed setup.

    This spawn a cockroach node that will store its data in `location`
    """

    def __init__(self, uri, location, join=None, clean_on_exit=True):
        self.location = location

        logs = f'{location}/logs'
        temp = f'{location}/tmp'
        external = f'{location}/extern'
        store = location

        os.makedirs(logs, exist_ok=True)
        os.makedirs(temp, exist_ok=True)
        os.makedirs(external, exist_ok=True)

        self.uri = parse_uri(uri)
        self.location = location
        self.bin = COCKROACH_BIN.get(os.name)

        if self.bin is None:
            raise RuntimeError('Your OS is not supported')

        if not os.path.exists(self.bin):
            info('Using system binary')
            self.bin = 'cockroach'
        else:
            hash = COCKROACH_HASH.get(os.name)

        # db_uri = uri.replace('cockroach', 'postgresql')
        self.addrs = f'{self.uri["address"]}:{self.uri["port"]}'
        self.arguments = [
            'start', '--insecure',
            f'--listen-addr={self.addrs}',
            f'--external-io-dir={external}',
            f'--store={store}',
            f'--temp-dir={temp}',
            f'--log-dir={logs}',
            f'--pid-file={location}/cockroach_pid'
        ]

        if join is not None:
            self.arguments.append(f'--join={join}')

        self.manager: Manager = Manager()
        self.properties = self.manager.dict()
        self.properties['running'] = False
        self.clean_on_exit = clean_on_exit
        self._process: Process = None
        self.cmd = None
        self.client = None
        self.cursor = None

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

    def start(self, wait=True):
        try:
            self._process = Process(target=self._start, args=(self.properties,))
            self._process.start()

            # wait for all the properties to be populated
            if wait:
                while self.properties.get('nodeID') is None and self._process.is_alive():
                    time.sleep(0.01)

            self.properties['db_pid'] = int(open(f'{self.location}/cockroach_pid', 'r').read())
        except Exception as e:
            error(traceback.format_exc(e))

        self._setup()

    def _setup(self):
        username = self.uri.get('username', 'root')
        password = self.uri.get('password', 'none')

        # create the first user and system tables
        self._run_local_query(system_schema([username]))

        # -- Login if our new user
        self.client = psycopg2.connect(
            user='root',
            password=password,
            # sslmode='require',
            # sslrootcert='certs/ca.crt',
            # sslkey='certs/client.maxroach.key',
            # sslcert='certs/client.maxroach.crt',
            port=self.uri['port'],
            host=self.uri['address'])

        self.client.set_session(autocommit=True)
        self.cursor = self.client.cursor()

    def _run_local_query(self, statement):
        if isinstance(statement, str):
            statement = statement.encode('utf8')

        out = subprocess.check_output(
            f'{self.bin} sql --insecure --host={self.addrs}', input=statement, shell=True)
        debug(out.decode('utf8').strip())

    def new_queue(self, namespace, name, client='root'):
        """Create a new queue

        Parameters
        ----------
        name: str
            create a new queue named `name`

        client: str
            client name to use

        clients: str
            create permission for all the clients
        """
        new_queue(self.cursor, client, namespace, name)

    def stop(self):
        self.properties['running'] = False
        self._process.terminate()

        os.kill(self.properties['db_pid'], signal.SIGTERM)

        if self.clean_on_exit:
            shutil.rmtree(self.location)

    def wait(self):
        while True:
            time.sleep(0.01)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        if exc_type is not None:
            raise exc_type

    def parse(self, properties, line):
        debug(line[:-1])

        if line[0] == '*':
            return
        try:
            a, b = line.split(':', maxsplit=1)
            properties[a.strip()] = b.strip()

        except Exception as e:
            print(e, line, end='\n')
            print(traceback.format_exc())
            raise RuntimeError(f'{line} (cmd: {self.cmd})')

    # properties that are populated once the server has started
    @property
    def node_id(self):
        return self.properties.get('nodeID')

    @property
    def status(self):
        return self.properties.get('status')

    @property
    def sql(self):
        return self.properties.get('sql')

    @property
    def client_flags(self):
        return self.properties.get('client flags')

    @property
    def webui(self):
        return self.properties.get('webui')

    @property
    def build(self):
        return self.properties.get('build')


def new_server(uri, location, join=None, clean_on_exit=True):
    cockroach = CockRoachDB(uri, location, join, clean_on_exit)
    return cockroach


def start_server_main():
    from argparse import ArgumentParser
    import os

    parser = ArgumentParser()
    parser.add_argument('--address', type=str, default='localhost')
    parser.add_argument('--port', type=int, default=8123)
    parser.add_argument('--loc', type=str, default=os.getcwd())
    args = parser.parse_args()

    server = new_server(f'cockroach://{args.address}:{args.port}', args.loc, None, False)
    server.start()
