import json
import psycopg2
from threading import RLock

from cqueue.uri import parse_uri
from cqueue.backends.queue import Message, MessageQueue, QueuePacemaker
from .util import _parse


class CKPacemaker(QueuePacemaker):
    def __init__(self, agent, namespace, wait_time, capture):
        self.cursor = agent.cursor
        self.lock = agent.lock
        super(CKPacemaker, self).__init__(agent, namespace, wait_time, capture)

    def register_agent(self, agent_name):
        with self.lock:
            self.cursor.execute(f"""
            INSERT INTO
                {self.namespace}.system (agent)
            VALUES
                (%s)
            RETURNING uid
            """, (json.dumps(agent_name),))

            self.agent_id = self.cursor.fetchone()[0]
            return self.agent_id

    def update_heartbeat(self):
        with self.lock:
            self.cursor.execute(f"""
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
            self.cursor.execute(f"""
            UPDATE {self.namespace}.system SET
                message = %s,
                queue = %s
            WHERE 
                uid = %s
            """, (message.uid, name, self.agent_id))

            return message

    def unregister_message(self, uid):
        with self.lock:
            self.cursor.execute(f"""
            UPDATE {self.namespace}.system SET
                message = NULL
            WHERE 
                uid = %s AND
                message = %s
            """, (self.agent_id, uid))

    def unregister_agent(self):
        with self.lock:
            self.cursor.execute(f"""
            UPDATE {self.namespace}.system SET
                alive = false
            WHERE 
                uid = %s
            """, (self.agent_id,))

    def insert_log_line(self, line, ltype=0):
        if self.agent_id is None:
            return

        with self.lock:
            self.cursor.execute(f"""
            INSERT INTO {self.namespace}.logs (agent, ltype, line)
            VALUES
                (%s, %s, %s)
            """, (self.agent_id, ltype, line))


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
        self.lock = RLock()

        self.capture = True
        self.timeout = 60

    def pacemaker(self, namespace, wait_time, capture):
        return CKPacemaker(self, namespace, wait_time, capture)

    def enqueue(self, name, message, mtype=0, replying_to=None):
        """See `~mlbaselines.distributed.queue.MessageQueue`"""
        with self.lock:
            self.cursor.execute(f"""
            INSERT INTO  
                {self.namespace}.{name} (mtype, read, actioned, message, replying_to)
            VALUES
                (%s, %s, %s, %s, %s)
            RETURNING uid
            """, (mtype, False, False, json.dumps(message), replying_to))

            return self.cursor.fetchone()[0]

    def dequeue(self, name):
        """See `~mlbaselines.distributed.queue.MessageQueue`"""
        with self.lock:
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

            return self.heartbeat_monitor.register_message(name, _parse(self.cursor.fetchone()))

    def mark_actioned(self, name, message: Message = None, uid: int = None):
        """See `~mlbaselines.distributed.queue.MessageQueue`"""
        if isinstance(message, Message):
            uid = message.uid

        with self.lock:
            self.cursor.execute(f"""
            UPDATE {self.namespace}.{name} SET 
                (actioned, actioned_time) = (true, current_timestamp())
            WHERE 
                uid = %s
            """, (uid,))

            self.heartbeat_monitor.unregister_message(uid)
            return message

    def get_reply(self, name, uid):
        return self.monitor().get_reply(self.namespace, name, uid)

    def monitor(self):
        from .monitor import CKQueueMonitor
        return CKQueueMonitor(cursor=self.cursor, lock=self.lock)


def new_client(*args, **kwargs):
    return CKMQClient(*args, **kwargs)
