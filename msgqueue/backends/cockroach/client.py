import json
import psycopg2
from threading import RLock

from msgqueue.uri import parse_uri
from msgqueue.backends.queue import Message, MessageQueue, QueuePacemaker
from .server import new_queue
from .util import _parse


class CKPacemaker(QueuePacemaker):
    def __init__(self, agent, wait_time, capture):
        self.client = agent.cursor
        self.lock = agent.lock
        self.database = agent.database
        super(CKPacemaker, self).__init__(agent, wait_time, capture)

    def register_agent(self, agent_name):
        with self.lock:
            self.client.execute(f"""
            INSERT INTO
                {self.database}.system (agent)
            VALUES
                (%s)
            RETURNING uid
            """, (json.dumps(agent_name),))

            self.agent_id = self.client.fetchone()[0]
            return self.agent_id

    def update_heartbeat(self):
        with self.lock:
            self.client.execute(f"""
            UPDATE 
                {self.database}.{self.name}
            SET 
                heartbeat = current_timestamp()
            WHERE
                uid = %s
            """, (self.message.uid,))

    def register_message(self, name, message):
        if message is None:
            return None

        self.name = name
        self.message = message
        self.update_heartbeat()

        return message

    def unregister_message(self, uid):
        pass

    def unregister_agent(self):
        with self.lock:
            self.client.execute(f"""
            UPDATE {self.database}.system SET
                alive = false
            WHERE 
                uid = %s
            """, (self.agent_id,))

    def insert_log_line(self, line, ltype=0):
        if self.agent_id is None or self.client is None:
            return

        with self.lock:
            self.client.execute(f"""
            INSERT INTO {self.database}.logs (agent, ltype, line)
            VALUES
                (%s, %s, %s)
            """, (self.agent_id, ltype, json.dumps(line)))


class CKMQClient(MessageQueue):
    """Simple cockroach db queue client

    Parameters
    ----------
    uri: str
        cockroach://192.168.0.10:8123
    """

    def __init__(self, uri, database, name='worker', log_capture=True, timeout=60):
        uri = parse_uri(uri)
        self.username = 'root'  # uri.get('username', 'default_user')
        self.password = uri.get('password', 'mq_password')
        self.con = psycopg2.connect(
            user=self.username,
            password=self.password,
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
        self.heartbeat_monitor = None
        self.lock = RLock()
        self.database = database

        self.capture = log_capture
        self.timeout = timeout

    def join(self):
        self.con.close()
        return self.heartbeat_monitor.join()

    def pacemaker(self, wait_time, capture):
        return CKPacemaker(self, wait_time, capture)

    def enqueue(self, queue, namespace, message, mtype=0, replying_to=None):
        """See `~mlbaselines.distributed.queue.MessageQueue`"""
        with self.lock:
            # need to be root to create database :/
            if not self._queue_exist(queue):
                new_queue(self.cursor, self.database, queue, namespace, 'root')

            print('enqueue')

            self.cursor.execute(f"""
            INSERT INTO  
                {self.database}.{queue} (mtype, read, actioned, message, replying_to, namespace, retry)
            VALUES
                (%s, %s, %s, %s, %s, %s, 0)
            RETURNING uid
            """, (mtype, False, False, json.dumps(message), replying_to, namespace))

            return self.cursor.fetchone()[0]

    def dequeue(self, queue, namespace, mtype=None):
        """See `~mlbaselines.distributed.queue.MessageQueue`"""
        query = ['read = false']
        args = list()

        if isinstance(mtype, (list, tuple)):
            query.append('mtype in %s')
            args.append(tuple(mtype))

        elif isinstance(mtype, int):
            query.append('mtype = %s')
            args.append(mtype)

        if namespace is not None:
            query.append('namespace = %s')
            args.append(namespace)

        query = ' AND\n'.join(query)
        with self.lock:
            try:
                select = f"""
                UPDATE {self.database}.{queue} SET
                    (read, read_time) = (true, current_timestamp())
                WHERE
                    {query}
                ORDER BY
                    time ASC
                LIMIT 1
                RETURNING *
                """
                self.cursor.execute(select, tuple(args))
                return self._register_message(queue, _parse(self.cursor.fetchone()))

            except psycopg2.errors.UndefinedTable:
                return None

    def mark_actioned(self, name, uid: Message):
        """See `~mlbaselines.distributed.queue.MessageQueue`"""
        if isinstance(uid, Message):
            uid = uid.uid

        with self.lock:
            self.cursor.execute(f"""
            UPDATE {self.database}.{name} SET 
                (actioned, actioned_time) = (true, current_timestamp())
            WHERE 
                uid = %s
            """, (uid,))

            self._unregister_message(uid)
            return uid

    def mark_error(self, name, uid, error):
        if isinstance(uid, Message):
            uid = uid.uid

        with self.lock:
            self.cursor.execute(f"""
            UPDATE {self.database}.{name} SET 
                error = %s
            WHERE 
                uid = %s
            """, (json.dumps(error), uid))

            self._unregister_message(uid)
            return uid

    def monitor(self):
        from .monitor import CKQueueMonitor
        return CKQueueMonitor(self.database, cursor=self.cursor, lock=self.lock)


def new_client(*args, **kwargs):
    return CKMQClient(*args, **kwargs)
