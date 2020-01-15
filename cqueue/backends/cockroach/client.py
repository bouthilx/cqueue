import json
import psycopg2
from threading import RLock

from cqueue.uri import parse_uri
from cqueue.backends.queue import Message, MessageQueue, QueuePacemaker
from .server import new_queue
from .util import _parse


class CKPacemaker(QueuePacemaker):
    def __init__(self, agent, namespace, wait_time, capture):
        self.client = agent.cursor
        self.lock = agent.lock
        super(CKPacemaker, self).__init__(agent, namespace, wait_time, capture)

    def register_agent(self, agent_name):
        with self.lock:
            self.client.execute(f"""
            INSERT INTO
                {self.namespace}.system (agent)
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
                {self.namespace}.system
            SET 
                heartbeat = current_timestamp()
            WHERE
                uid = %s
            """, (self.agent_id,))

    def register_message(self, name, message):
        if message is None:
            return None

        with self.lock:
            self.client.execute(f"""
            UPDATE {self.namespace}.system SET
                message = %s,
                queue = %s
            WHERE 
                uid = %s
            """, (message.uid, name, self.agent_id))

            return message

    def unregister_message(self, uid):
        with self.lock:
            self.client.execute(f"""
            UPDATE {self.namespace}.system SET
                message = NULL
            WHERE 
                uid = %s AND
                message = %s
            """, (self.agent_id, uid))

    def unregister_agent(self):
        with self.lock:
            self.client.execute(f"""
            UPDATE {self.namespace}.system SET
                alive = false
            WHERE 
                uid = %s
            """, (self.agent_id,))

    def insert_log_line(self, line, ltype=0):
        if self.agent_id is None:
            return

        with self.lock:
            self.client.execute(f"""
            INSERT INTO {self.namespace}.logs (agent, ltype, line)
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

    def __init__(self, uri, namespace, name='worker', log_capture=True, timeout=60):
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
        self.namespace = namespace
        self.heartbeat_monitor = None
        self.lock = RLock()

        self.capture = log_capture
        self.timeout = timeout

    def join(self):
        self.con.close()
        return self.heartbeat_monitor.join()

    def pacemaker(self, namespace, wait_time, capture):
        return CKPacemaker(self, namespace, wait_time, capture)

    def enqueue(self, name, message, mtype=0, replying_to=None):
        """See `~mlbaselines.distributed.queue.MessageQueue`"""
        with self.lock:
            # need to be root to create database :/
            if not self._queue_exist(name):
                new_queue(self.cursor, 'root', self.namespace, name)

            self.cursor.execute(f"""
            INSERT INTO  
                {self.namespace}.{name} (mtype, read, actioned, message, replying_to)
            VALUES
                (%s, %s, %s, %s, %s)
            RETURNING uid
            """, (mtype, False, False, json.dumps(message), replying_to))

            return self.cursor.fetchone()[0]

    def dequeue(self, name, mtype=None):
        """See `~mlbaselines.distributed.queue.MessageQueue`"""
        query = ['read = false']
        args = list()

        if isinstance(mtype, (list, tuple)):
            query.append('mtype in %s')
            args.append(tuple(mtype))

        elif isinstance(mtype, int):
            query.append('mtype = %s')
            args.append(mtype)

        query = ' AND\n'.join(query)
        with self.lock:
            try:
                self.cursor.execute(f"""
                UPDATE {self.namespace}.{name} SET 
                    (read, read_time) = (true, current_timestamp())
                WHERE 
                    {query}
                ORDER BY
                    time
                LIMIT 1
                RETURNING *
                """, tuple(args))

                return self._register_message(name, _parse(self.cursor.fetchone()))
            except psycopg2.errors.UndefinedTable:
                return None

    def mark_actioned(self, name, uid: Message):
        """See `~mlbaselines.distributed.queue.MessageQueue`"""
        if isinstance(uid, Message):
            uid = uid.uid

        with self.lock:
            self.cursor.execute(f"""
            UPDATE {self.namespace}.{name} SET 
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
            UPDATE {self.namespace}.{name} SET 
                error = %s
            WHERE 
                uid = %s
            """, (uid, json.dumps(error)))

            self._unregister_message(uid)
            return uid

    def reply(self, name, uid):
        if isinstance(uid, Message):
            uid = uid.uid

        return self.monitor().reply(self.namespace, name, uid)

    def monitor(self):
        from .monitor import CKQueueMonitor
        return CKQueueMonitor(cursor=self.cursor, lock=self.lock)


def new_client(*args, **kwargs):
    return CKMQClient(*args, **kwargs)
