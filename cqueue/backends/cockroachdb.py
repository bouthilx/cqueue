import os
import json
import psycopg2
import shutil
import signal
import subprocess
import time
import traceback
import threading

from multiprocessing import Process, Manager
from cqueue.logs import debug, info, error
from cqueue.uri import parse_uri
from cqueue.backends.queue import Message, MessageQueue, QueueMonitor, Agent


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

    user = []
    for client in clients:
        user.append(f'CREATE USER IF NOT EXISTS {client};')
    user = '\n'.join(user)

    permissions = []
    for client in clients:
        permissions.append(f'GRANT ALL ON DATABASE {namespace} TO {client};')
        permissions.append(f'GRANT ALL ON TABLE {namespace}.{name} TO {client};')
        permissions.append(f'GRANT ALL ON TABLE {namespace}.system TO {client};')
        permissions.append(f'GRANT ALL ON DATABASE archive TO {client};')
        permissions.append(f'GRANT ALL ON TABLE archive.messages TO {client};')
        permissions.append(f'GRANT ALL ON DATABASE qsystem TO {client};')
        permissions.append(f'GRANT ALL ON TABLE qsystem.namespaces TO {client};')
    permissions = '\n'.join(permissions)

    return f"""
    {user}
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
        alive           BOOLEAN     DEFAULT true
    );

    CREATE DATABASE IF NOT EXISTS qsystem;
    CREATE TABLE IF NOT EXISTS qsystem.namespaces (
        namespace       character(64),
        name            character(64)
    );

    CREATE INDEX IF NOT EXISTS messages_index
    ON {namespace}.{name}  (
        read        DESC, 
        time        DESC,
        actioned    DESC,
        mtype       ASC,
        replying_to DESC
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


class CockRoachDB:
    """ cockroach db is a highly resilient database that allow us to remove
    the Master in a traditional distributed setup.

    This spawn a cockroach node that will store its data in `location`
    """

    def __init__(self, location, addrs, join=None, clean_on_exit=True, schema=None):
        self.location = location

        logs = f'{location}/logs'
        temp = f'{location}/tmp'
        external = f'{location}/extern'
        store = location

        os.makedirs(logs, exist_ok=True)
        os.makedirs(temp, exist_ok=True)
        os.makedirs(external, exist_ok=True)

        self.location = location
        self.addrs = addrs
        self.bin = COCKROACH_BIN.get(os.name)

        if self.bin is None:
            raise RuntimeError('Your OS is not supported')

        if not os.path.exists(self.bin):
            info('Using system binary')
            self.bin = 'cockroach'
        else:
            hash = COCKROACH_HASH.get(os.name)

        self.arguments = [
            'start', '--insecure',
            f'--listen-addr={addrs}',
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
        self.schema = schema

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
            # self._setup()
        except Exception as e:
            error(traceback.format_exc(e))

    def _setup(self, client='track_client'):
        if self.schema is not None:
            if callable(self.schema):
                self.schema = self.schema(client)

            out = subprocess.check_output(
                f'{self.bin} sql --insecure --host={self.addrs}', input=self.schema, shell=True)
            debug(out.decode('utf8').strip())

    def new_queue(self, namespace, name, client='default_user', clients=None):
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
        if clients is None:
            clients = []

        clients.append(client)
        statement = message_queue_schema(clients, namespace, name)

        if isinstance(statement, str):
            statement = statement.encode('utf8')

        out = subprocess.check_output(
            f'{self.bin} sql --insecure --host={self.addrs}', input=statement, shell=True)
        debug(out.decode('utf8').strip())

        statement = f"""INSERT INTO qsystem.namespaces(namespace, name) VALUES ('{namespace}', '{name}');""".encode('utf8')
        out = subprocess.check_output(
            f'{self.bin} sql --insecure --host={self.addrs}', input=statement, shell=True)
        debug(out.decode('utf8').strip())

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


def start_message_queue(name, location, addrs, join=None, clean_on_exit=True):
    cockroach = CockRoachDB(location, addrs, join, clean_on_exit, schema=None)
    return cockroach


class AgentMonitor(threading.Thread):
    def __init__(self, agent, namespace, wait_time=60):
        threading.Thread.__init__(self)
        self.stopped = threading.Event()
        self.wait_time = wait_time
        self.agent = agent
        self.cursor = agent.cursor
        self.namespace = namespace

    def stop(self):
        """Stop monitoring."""
        self.stopped.set()
        self.join()

    def run(self):
        """Run the trial monitoring every given interval."""
        while not self.stopped.wait(self.wait_time):
            self.update_heartbeat()

    def update_heartbeat(self):
        self.cursor.execute(f"""
        UPDATE 
            {self.namespace}.system
        SET 
            heartbeat = current_timestamp()
        WHERE
            uid = %s
        """, (self.agent.agent_id,))


def _parse(result):
    if result is None:
        return None

    return Message(
        result[0],
        result[1],
        result[2],
        result[3],
        result[4],
        result[5],
        result[6],
        result[7],
        result[8],
    )


def _parse_agent(result):
    if result is None:
        return None

    return Agent(
        result[0],
        result[1],
        result[2],
        result[3],
        result[4]
    )


class CKMQClient(MessageQueue):
    """Simple cockroach db queue client

    Parameters
    ----------
    uri: str
        cockroach://192.168.0.10:8123
    """

    def __init__(self, uri, namespace, name=None):
        uri = parse_uri(uri)

        self.con = psycopg2.connect(
            user=uri.get('username', 'default_user'),
            password=uri.get('password', 'mq_password'),
            # sslmode='require',
            # sslrootcert='certs/ca.crt',
            # sslkey='certs/client.maxroach.key',
            # sslcert='certs/client.maxroach.crt',
            port=uri['port'],
            host=uri['address']
        )
        self.con.set_session(autocommit=True)
        self.cursor = self.con.cursor()
        self.name = name
        self.agent_id = None
        self.namespace = namespace
        self.heartbeat_monitor = None

    def __enter__(self):
        self.agent_id = self._register_agent(self.name)
        self.heartbeat_monitor = AgentMonitor(self, self.namespace, wait_time=60)
        self.heartbeat_monitor.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.heartbeat_monitor.stop()
        self._remove()

    def _register_agent(self, agent_name):
        self.cursor.execute(f"""
        INSERT INTO
            {self.namespace}.system (agent)
        VALUES
            (%s)
        RETURNING uid
        """, (json.dumps(agent_name),))

        if self.cursor.rowcount <= 0:
            return None

        return self.cursor.fetchone()[0]

    def _update_heartbeat(self):
        self.cursor.execute(f"""
        UPDATE {self.namespace}.qsystem SET
            heartbeat = current_timestamp()
        WHERE
            uid = %s
        """, (self.agent_id,))

    def _remove(self):
        self.cursor.execute(f"""
        UPDATE {self.namespace}.system SET
            alive = false
        WHERE uid = %s""", (self.agent_id,))

    def enqueue(self, name, message, mtype=0, replying_to=None):
        """See `~mlbaselines.distributed.queue.MessageQueue`"""
        self.cursor.execute(f"""
        INSERT INTO  
            {self.namespace}.{name} (mtype, read, actioned, message, replying_to)
        VALUES
            (%s, %s, %s, %s, %s)
        RETURNING uid
        """, (mtype, False, False, json.dumps(message), replying_to))

        if self.cursor.rowcount <= 0:
            return None

        return self.cursor.fetchone()[0]

    def dequeue(self, name):
        """See `~mlbaselines.distributed.queue.MessageQueue`"""

        self.cursor.execute(f"""
        UPDATE {self.namespace}.{name} SET 
            (read, read_time) = (true, current_timestamp())
        WHERE 
            read = false
        ORDER BY
            time
        LIMIT 1
        RETURNING *
        """)

        if self.cursor.rowcount <= 0:
            return None

        return _parse(self.cursor.fetchone())

    def mark_actioned(self, name, message: Message = None, uid: int = None):
        """See `~mlbaselines.distributed.queue.MessageQueue`"""

        if isinstance(message, Message):
            uid = message.uid

        self.cursor.execute(f"""
        UPDATE {self.namespace}.{name} SET 
            (actioned, actioned_time) = (true, current_timestamp())
        WHERE 
            uid = %s
        RETURNING *
        """, (uid,))

        if self.cursor.rowcount <= 0:
            return None

        return _parse(self.cursor.fetchone())

    def get_reply(self, name, uid):
        return CKQueueMonitor(
            cursor=self.cursor).get_reply(self.namespace, name, uid)


class CKQueueMonitor(QueueMonitor):
    def __init__(self, uri=None, cursor=None):
        if cursor is None:
            uri = parse_uri(uri)
            self.con = psycopg2.connect(
                user=uri.get('username', 'default_user'),
                password=uri.get('password', 'mq_password'),
                # sslmode='require',
                # sslrootcert='certs/ca.crt',
                # sslkey='certs/client.maxroach.key',
                # sslcert='certs/client.maxroach.crt',
                port=uri['port'],
                host=uri['address']
            )
            self.con.set_session(autocommit=True)
            self.cursor = self.con.cursor()
        else:
            self.cursor = cursor

    def _fetch_all(self):
        rows = self.cursor.fetchall()
        records = []
        for row in rows:
            records.append(_parse(row))

        return records

    def dump(self, namespace, name):
        self.cursor.execute(f'SELECT *  FROM {namespace}.{name}')

        rows = self.cursor.fetchall()
        for row in rows:
            print(_parse(row))

    def get_namespaces(self):
        self.cursor.execute(f"""
        SELECT
            *
        FROM qsystem.namespaces;
        """)

        if self.cursor.rowcount <= 0:
            return []

        return [(n[0], n[1]) for n in self.cursor.fetchall()]

    def archive_namespace(self, namespace):
        # TODO create partition per namespace
        self.cursor.execute(f"""
        SELECT 
          tablename as table 
        FROM 
          pg_tables  
        WHERE schemaname = '{namespace}'
        """)

        names = [n for n in self.cursor.fetchall()]
        for table in names:
            if table == 'system':
                continue

            self.cursor.execute(f"""
            INSERT INTO archive.messages
                SELECT
                    {namespace},
                    {table},
                    *
                FROM {namespace}.{table};
            """)

        self.cursor.execute(f"""
        DROP DATABASE IF EXISTS {namespace}
        """)

    def get_all_messages(self, namespace, name, limit=100):
        self.cursor.execute(f"""
        SELECT 
            * 
        FROM 
            {namespace}.{name}
        LIMIT {limit}
        """)

        if self.cursor.rowcount <= 0:
            return []

        return self._fetch_all()

    def get_unread_messages(self, namespace, name):
        self.cursor.execute(f"""
        SELECT 
            * 
        FROM 
            {namespace}.{name}
        WHERE 
            read = false
        """)

        if self.cursor.rowcount <= 0:
            return []

        return self._fetch_all()

    def get_unactioned_messages(self, namespace, name):
        self.cursor.execute(f"""
        SELECT 
            * 
        FROM 
            {namespace}.{name}
        WHERE 
            read = true        AND
            actioned = false
        """)

        if self.cursor.rowcount <= 0:
            return []

        return self._fetch_all()

    def unread_count(self, namespace, name):
        self.cursor.execute(f"""
        SELECT 
            COUNT(*)
        FROM 
            {namespace}.{name}
        WHERE 
            read = false
        """)

        if self.cursor.rowcount <= 0:
            return None

        return self.cursor.fetchone()[0]

    def unactioned_count(self, namespace, name):
        self.cursor.execute(f"""
        SELECT 
            COUNT(*)
        FROM 
            {namespace}.{name}
        WHERE 
            actioned = false
        """)

        if self.cursor.rowcount <= 0:
            return None

        return self.cursor.fetchone()[0]

    def read_count(self, namespace, name):
        self.cursor.execute(f"""
        SELECT 
            COUNT(*)
        FROM 
            {namespace}.{name}
        WHERE 
            read = true
        """)

        if self.cursor.rowcount <= 0:
            return None

        return self.cursor.fetchone()[0]

    def actioned_count(self, namespace, name):
        self.cursor.execute(f"""
        SELECT 
            COUNT(*)
        FROM 
            {namespace}.{name}
        WHERE 
            actioned = true
        """)

        if self.cursor.rowcount <= 0:
            return None

        return self.cursor.fetchone()[0]

    def reset_queue(self, namespace, name):
        """Resume queue from previous statement

        Returns
        --------
        returns all the restored messages

        """
        self.cursor.execute(f"""
        UPDATE {namespace}.{name}
            SET 
                (read, read_time) = (false, null)
            WHERE 
                actioned = false
        RETURNING *
        """)

        if self.cursor.rowcount <= 0:
            return []

        rows = self.cursor.fetchall()
        records = []
        for row in rows:
            records.append(_parse(row))

        return records

    def get_reply(self, namespace, name, uid):
        """Get the replied message from the queue"""
        self.cursor.execute(f"""
        SELECT 
            * 
        FROM 
            {namespace}.{name}
        WHERE 
            replying_to = %s
        """, (uid,))

        if self.cursor.rowcount <= 0:
            return None

        return _parse(self.cursor.fetchone())

    def agents(self, namespace):
        self.cursor.execute(f"""
        SELECT 
            *
        FROM
            {namespace}.system
        """)

        if self.cursor.rowcount <= 0:
            return []

        return [_parse_agent(a) for a in self.cursor.fetchall()]
