import psycopg2
from threading import RLock

from msgqueue.uri import parse_uri
from msgqueue.backends.queue import QueueMonitor, Agent, Message, to_dict

from .util import _parse, _parse_agent


class CKQueueMonitor(QueueMonitor):
    def __init__(self, uri=None, cursor=None, lock=None):
        # When using this inside a dashbord it is executed in a multi threaded environment
        # You need to lock the cursor to not get some errors

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

    def _log_types(self, namespace, agent):
        self.cursor.execute(f"""
            SELECT 
                ltype 
            FROM 
                {namespace}.logs
            WHERE
                agent = {agent.uid}
            """)
        return set(r[0] for r in self.cursor.fetchall())

    def archive(self, namespace, archive_name):
        """Archive a namespace into a zipfile and delete the namespace from the database"""
        import zipfile
        import json

        class _Wrapper:
            def __init__(self, buffer):
                self.buffer = buffer

            def write(self, data):
                self.buffer.write(data.encode('utf-8'))

        with self.lock:
            with zipfile.ZipFile(archive_name, 'w') as archive:
                queues = self.queues(namespace)

                for queue in set(queues):
                    with archive.open(f'{namespace}/{queue}.json', 'w') as queue_archive:
                        messages = self.messages(namespace, queue)
                        json.dump(messages, fp=_Wrapper(queue_archive), default=to_dict)

                with archive.open(f'{namespace}/system.json', 'w') as system_archive:
                    agents = self.agents(namespace)
                    json.dump(agents, fp=_Wrapper(system_archive), default=to_dict)

                for agent in agents:
                    for type in self._log_types(namespace, agent):
                        with archive.open(f'{namespace}/logs/{agent.uid}_{type}.txt', 'w') as logs_archive:
                            log = self.log(namespace, agent, type)
                            _Wrapper(logs_archive).write(log)

            self.cursor.execute(f"""
            DROP DATABASE IF EXISTS {namespace}
            """)

        print('Archiving is done')
        return None

        # # TODO create partition per namespace
        # self.cursor.execute(f"""
        # SELECT
        #   tablename as table
        # FROM
        #   pg_tables
        # WHERE schemaname = '{namespace}'
        # """)
        #
        # names = [n for n in self.cursor.fetchall()]
        # for table in names:
        #     if table == 'system':
        #         continue
        #
        #     self.cursor.execute(f"""
        #     INSERT INTO archive.messages
        #         SELECT
        #             {namespace},
        #             {table},
        #             *
        #         FROM {namespace}.{table};
        #     """)
        #
        # self.cursor.execute(f"""
        # DROP DATABASE IF EXISTS {namespace}
        # """)

    def clear(self, namespace, name):
        with self.lock:
            query = f"""DELETE FROM {namespace}.{name}"""
            self.cursor.execute(query)
            self.cursor.execute("DELETE FROM qsystem.namespaces WHERE namespace = %s", (namespace,))

    def messages(self, namespace, name, limit=None):
        with self.lock:
            if isinstance(name, list):
                data = []
                for n in name:
                    data.extend(self.messages(namespace, n, limit))
                return data

            query = f"""
            SELECT 
                * 
            FROM 
                {namespace}.{name}
            """
            if limit is not None:
                query = f'{query} LIMIT {limit}'

            self.cursor.execute(query)
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
