import psycopg2
from threading import RLock

from msgqueue.uri import parse_uri
from msgqueue.backends.queue import QueueMonitor, Agent, Message, to_dict

from .util import _parse, _parse_agent


class CKQueueMonitor(QueueMonitor):
    def __init__(self, database, uri=None, cursor=None, lock=None):
        # When using this inside a dashbord it is executed in a multi threaded environment
        # You need to lock the cursor to not get some errors
        self.database = database

        if cursor is None:
            uri = parse_uri(uri)
            self.con = psycopg2.connect(
                user=uri.get('username', 'root'),
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

    def queues(self):
        with self.lock:
            self.cursor.execute(f"""
            SELECT
                name
            FROM {self.database}.qsystem;
            """)

            return list(set(n[0] for n in self.cursor.fetchall()))

    def _log_types(self, agent):
        self.cursor.execute(f"""
            SELECT 
                ltype 
            FROM 
                {self.database}.logs
            WHERE
                agent = {agent.uid}
            """)
        return set(r[0] for r in self.cursor.fetchall())

    def archive(self, namespace, archive_name, namespace_out=None, format='json'):
        """Archive a namespace into a zipfile and delete the namespace from the database"""

        def remove_db(nm):
            self.cursor.execute(f"""
            DROP DATABASE IF EXISTS {nm}
            """)

        self._make_archive(
            namespace,
            archive_name,
            namespace_out,
            format,
            remove_db,
            self._log_types,
            self.lock
        )

    def clear(self, name, namespace):
        with self.lock:
            if namespace is not None:
                query = f"""DELETE FROM {self.database}.{name} WHERE namespace = namespace"""
                self.cursor.execute(f"DELETE FROM {self.database}.qsystem WHERE namespace = %s", (namespace,))
            else:
                query = f"""DELETE FROM {self.database}.{name}"""

            self.cursor.execute(query)

    def _constraint(self, name, value):
        constraint = None
        args = None

        if value is not None:
            if isinstance(value, (list, tuple)):
                constraint = f'{name} in %s'
                args = (tuple(value))
            else:
                constraint = f'{name} = %s'
                args = (value)

        return constraint, args

    def new_filters(self, namespace, mtype):
        constraint = []
        args = []

        a, b = self._constraint('namespace', namespace)
        if a is not None:
            constraint.append(a)
            args.append(b)

        a, b = self._constraint('mtype', mtype)
        if a is not None:
            constraint.append(a)
            args.append(b)

        if len(constraint) == 0:
            # there is a where so we need a condition here
            return '1 = 1', tuple()

        a = ' AND '.join(constraint)
        b = tuple(args)
        return a, b

    def messages(self, name, namespace, limit=None, mtype=None):
        with self.lock:
            if isinstance(name, list):
                data = []
                for n in name:
                    data.extend(self.messages(namespace, n, limit))
                return data

            constraints, args = self.new_filters(namespace, mtype)
            query = f"""
            SELECT 
                *
            FROM 
                {self.database}.{name}
            WHERE
                {constraints}
            """

            if limit is not None:
                query = f'{query} LIMIT {limit}'

            self.cursor.execute(query, args)
            return self._fetch_all()

    def unread_messages(self, name, namespace, mtype=None):
        with self.lock:
            constraints, args = self.new_filters(namespace, mtype)
            self.cursor.execute(f"""
            SELECT 
                * 
            FROM 
                {self.database}.{name}
            WHERE 
                {constraints}
                AND read = false 
            """, args)

            return self._fetch_all()

    def unactioned_messages(self, name, namespace, mtype=None):
        with self.lock:
            constraints, args = self.new_filters(namespace, mtype)
            self.cursor.execute(f"""
            SELECT 
                * 
            FROM 
                {self.database}.{name}
            WHERE 
                {constraints}
                AND read = true        
                AND actioned = false
            """, args)

            return self._fetch_all()

    def unread_count(self, name, namespace, mtype=None):
        with self.lock:
            constraints, args = self.new_filters(namespace, mtype)
            self.cursor.execute(f"""
            SELECT 
                COUNT(*)
            FROM 
                {self.database}.{name}
            WHERE 
                {constraints}
                AND read = false
            """, args)

            return self.cursor.fetchone()[0]

    def unactioned_count(self, name, namespace, mtype=None):
        with self.lock:
            constraints, args = self.new_filters(namespace, mtype)
            self.cursor.execute(f"""
            SELECT 
                COUNT(*)
            FROM 
                {self.database}.{name}
            WHERE 
                {constraints}
                AND read = true
                AND actioned = false
            """, args)

            return self.cursor.fetchone()[0]

    def read_count(self, name, namespace, mtype=None):
        with self.lock:
            constraints, args = self.new_filters(namespace, mtype)
            query = f"""
            SELECT 
                COUNT(*)
            FROM 
                {self.database}.{name}
            WHERE 
                {constraints}
                AND read = true
            """
            self.cursor.execute(query, args)
            return self.cursor.fetchone()[0]

    def actioned_count(self, name, namespace, mtype=None):
        with self.lock:
            constraints, args = self.new_filters(namespace, mtype)
            self.cursor.execute(f"""
            SELECT 
                COUNT(*)
            FROM 
                {self.database}.{name}
            WHERE 
                {constraints}
                AND actioned = true
            """, args)

            return self.cursor.fetchone()[0]

    def reset_queue(self, name, namespace):
        with self.lock:
            constraints, args = self.new_filters(namespace, None)
            self.cursor.execute(f"""
            UPDATE {self.database}.{name}
                SET 
                    (read, read_time) = (false, null)
                WHERE 
                    {constraints}
                    AND actioned = false
            RETURNING *
            """, args)

            rows = self.cursor.fetchall()
            records = []
            for row in rows:
                records.append(_parse(row))

            return records

    def reply(self, name, uid):
        if isinstance(uid, Message):
            uid = uid.uid

        with self.lock:
            self.cursor.execute(f"""
            SELECT 
                * 
            FROM 
                {self.database}.{name}
            WHERE 
                replying_to = %s AND
            """, (uid,))

            return _parse(self.cursor.fetchone())

    def agents(self, namespace):
        with self.lock:
            constraint = '1 = 1'
            args = tuple()
            if namespace is not None:
                constraint = 'namespace = %s'
                args = (namespace,)

            self.cursor.execute(f"""
            SELECT 
                *
            FROM
                {self.database}.system
            WHERE
                {constraint}
            """, args)

            return [_parse_agent(a) for a in self.cursor.fetchall()]

    def lost_messages(self, queue, namespace, timeout_s=60, max_retry=3):
        with self.lock:
            self.cursor.execute(f"""
            SELECT
                *
            FROM
                {self.database}.{queue}
            WHERE
                namespace = %s      AND
                retry < {max_retry} AND
                current_timestamp() - heartbeat >  {timeout_s} * interval '1 second'
            """, (namespace,))

            agents = [_parse_agent(agent) for agent in self.cursor.fetchall()]

        msg = []
        for a in agents:
            msg.append((a.message, a.queue))

        return msg

    def requeue_lost_messages(self, queue, namespace, timeout_s=60, max_retry=3):
        with self.lock:
            constraint = ''
            args = tuple()

            if namespace is not None:
                constraint = 'AND namespace = %s'
                args = (namespace,)

            self.cursor.execute(f"""
               UPDATE {self.database}.{queue}
               SET 
                   (read, read_time, error, retry) = (false, null, null, retry + 1)
               WHERE
                   read     = true  AND
                   actioned = false AND
                   retry    < %s    AND
                   current_timestamp() - heartbeat >  {timeout_s} * interval '1 second'         
                   {constraint}
               RETURNING uid
               """, (max_retry,) + args)

            return len([i for i in self.cursor.fetchall()])

    def failed_messages(self, queue, namespace):
        with self.lock:
            constraint = ''
            args = tuple()

            if namespace is not None:
                constraint = 'AND namespace = %s'
                args = (namespace,)

            self.cursor.execute(f"""
            SELECT *
            FROM {self.database}.{queue}
            WHERE
                read     = true  AND
                actioned = false AND
                error    IS NOT NULL
                {constraint}
            """, args)

            return self._fetch_all()

    def requeue_failed_messages(self, queue, namespace, max_retry=3):
        with self.lock:
            constraint = ''
            args = tuple()

            if namespace is not None:
                constraint = 'AND namespace = %s'
                args = (namespace,)

            query = f"""
            UPDATE {self.database}.{queue}
            SET 
                (read, read_time, error, retry) = (false, null, null, retry + 1)
            WHERE
                read     = true  AND
                actioned = false AND
                retry    < %s    AND
                error    IS NOT NULL
                {constraint}
            RETURNING uid
            """

            self.cursor.execute(query, (max_retry,) + args)
            return len([i for i in self.cursor.fetchall()])

    def log(self, agent, ltype=0):
        if isinstance(agent, Agent):
            agent = agent.uid

        with self.lock:
            self.cursor.execute(f"""
            SELECT 
                line
            FROM 
                {self.database}.logs
            WHERE
                agent = %s       AND
                ltype = %s
            """, (agent, ltype))

            return ''.join([r[0] for r in self.cursor.fetchall()])


def new_monitor(*args, **kwargs):
    return CKQueueMonitor(*args, **kwargs)
