import psycopg2
from threading import RLock

from msgqueue.uri import parse_uri
from msgqueue.backends.queue import QueueMonitor, Agent, Message

from .util import _parse, _parse_agent


class CKQueueMonitor(QueueMonitor):
    def __init__(self, uri=None, cursor=None, lock=None):
        # When using this inside a dashbord it is executed in a multi threaded environment
        # You need to lock the cursor to not get some errors

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
            self.lock = RLock()
        else:
            self.cursor = cursor
            self.lock = lock

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

    def namespaces(self):
        with self.lock:
            self.cursor.execute(f"""
            SELECT
                namespace
            FROM qsystem.namespaces;
            """)

            return [n[0] for n in self.cursor.fetchall()]

    def queues(self, namespace):
        with self.lock:
            self.cursor.execute(f"""
            SELECT
                name
            FROM qsystem.namespaces
            WHERE
                namespace = %s
            ;
            """, (namespace,))

            return [n[0] for n in self.cursor.fetchall()]

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

    def messages(self, namespace, name, limit=100):
        with self.lock:
            self.cursor.execute(f"""
            SELECT 
                * 
            FROM 
                {namespace}.{name}
            LIMIT {limit}
            """)

            return self._fetch_all()

    def unread_messages(self, namespace, name):
        with self.lock:
            self.cursor.execute(f"""
            SELECT 
                * 
            FROM 
                {namespace}.{name}
            WHERE 
                read = false
            """)

            return self._fetch_all()

    def unactioned_messages(self, namespace, name):
        with self.lock:
            self.cursor.execute(f"""
            SELECT 
                * 
            FROM 
                {namespace}.{name}
            WHERE 
                read = true        AND
                actioned = false
            """)

            return self._fetch_all()

    def unread_count(self, namespace, name):
        with self.lock:
            self.cursor.execute(f"""
            SELECT 
                COUNT(*)
            FROM 
                {namespace}.{name}
            WHERE 
                read = false
            """)

            return self.cursor.fetchone()[0]

    def unactioned_count(self, namespace, name):
        with self.lock:
            self.cursor.execute(f"""
            SELECT 
                COUNT(*)
            FROM 
                {namespace}.{name}
            WHERE 
                read = true AND
                actioned = false
            """)

            return self.cursor.fetchone()[0]

    def read_count(self, namespace, name):
        with self.lock:
            self.cursor.execute(f"""
            SELECT 
                COUNT(*)
            FROM 
                {namespace}.{name}
            WHERE 
                read = true
            """)

            return self.cursor.fetchone()[0]

    def actioned_count(self, namespace, name):
        with self.lock:
            self.cursor.execute(f"""
            SELECT 
                COUNT(*)
            FROM 
                {namespace}.{name}
            WHERE 
                actioned = true
            """)

            return self.cursor.fetchone()[0]

    def reset_queue(self, namespace, name):
        with self.lock:
            self.cursor.execute(f"""
            UPDATE {namespace}.{name}
                SET 
                    (read, read_time) = (false, null)
                WHERE 
                    actioned = false
            RETURNING *
            """)

            rows = self.cursor.fetchall()
            records = []
            for row in rows:
                records.append(_parse(row))

            return records

    def reply(self, namespace, name, uid):
        if isinstance(uid, Message):
            uid = uid.uid

        with self.lock:
            self.cursor.execute(f"""
            SELECT 
                * 
            FROM 
                {namespace}.{name}
            WHERE 
                replying_to = %s
            """, (uid,))

            return _parse(self.cursor.fetchone())

    def agents(self, namespace):
        with self.lock:
            self.cursor.execute(f"""
            SELECT 
                *
            FROM
                {namespace}.system
            """)

            return [_parse_agent(a) for a in self.cursor.fetchall()]

    def dead_agents(self, namespace, timeout_s=60):
        with self.lock:
            self.cursor.execute(f"""
            SELECT
                *
            FROM
                {namespace}.system
            WHERE
                alive = true                                        AND
                current_timestamp() - heartbeat >  {timeout_s} * interval '1 second'
            """)

            return [_parse_agent(agent) for agent in self.cursor.fetchall()]

    def lost_messages(self, namespace, timeout_s=60):
        with self.lock:
            self.cursor.execute(f"""
            SELECT
                *
            FROM
                {namespace}.system
            WHERE
                message != NULL                                     AND
                current_timestamp() - heartbeat >  {timeout_s} * interval '1 second'
            """)

            agents = [_parse_agent(agent) for agent in self.cursor.fetchall()]

        msg = []
        for a in agents:
            msg.append((a.message, a.queue))

        return msg

    def requeue_lost_messages(self, namespace, timeout_s=60, max_retry=3):
        lost = self.lost_messages(namespace, timeout_s)

        with self.lock:
            for queue, message in lost:
                self.cursor.execute(f"""
                UPDATE {namespace}.{queue}
                SET 
                    (read, read_time, error, retry) = (false, null, null, retry + 1)
                WHERE
                    uid      = %s    AND
                    read     = true  AND
                    actioned = false AND
                    retry    < %s
                """, (message.uid, max_retry))

    def failed_messages(self, namespace, queue):
        with self.lock:
            self.cursor.execute(f"""
            SELECT *
            FROM {namespace}.{queue}
            WHERE
                error   != null
            """)

    def requeue_failed_messages(self, namespace, queue, max_retry=3):
        with self.lock:
            self.cursor.execute(f"""
            UPDATE {namespace}.{queue}
            SET 
                (read, read_time, error, retry) = (false, null, null, retry + 1)
            WHERE
                read     = true  AND
                actioned = false AND
                retry    < %s    AND
                error   != null
            """, (max_retry,))

    def log(self, namespace, agent, ltype=0):
        if isinstance(agent, Agent):
            agent = agent.uid

        with self.lock:
            self.cursor.execute(f"""
            SELECT 
                line
            FROM 
                {namespace}.logs
            WHERE
                agent = %s AND
                ltype = %s
            """, (agent, ltype))

            return ''.join([r[0] for r in self.cursor.fetchall()])


def new_monitor(*args, **kwargs):
    return CKQueueMonitor(*args, **kwargs)
