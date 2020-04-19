from contextlib import closing
import socket
import time

import os
import pytest
import shutil
from itertools import product

from msgqueue.logs import set_verbose_level
set_verbose_level(10)

from msgqueue.backends import known_backends, new_server
from msgqueue.backends import new_client, new_monitor_process


backends = known_backends()
formats = ['json', 'bson']

WORK_ITEM = 1
RESULT_ITEM = 2

DATABASE = 'TESTDB'
NAMESPACE = 'TESTNAME'
QUEUE = 'TESTQUEUE'


def get_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


class Environment:
    def __init__(self, backend):
        port = get_free_port()
        # local db do not have user/pass
        self.uri = f'{backend}://localhost:{port}'
        self.server = None
        self.client = None
        self.namespace = NAMESPACE

    def __enter__(self):
        self.server = new_server(uri=self.uri, database=DATABASE)
        try:
            self.server.start(wait=True)
        except Exception as e:
            self.server.stop()
            shutil.rmtree('/tmp/queue/', ignore_errors=True)
            raise e

        self.client = new_client(self.uri, DATABASE, 'client-test')
        self.monitor = self.client.monitor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.server.stop()
        time.sleep(1)
        shutil.rmtree('/tmp/queue/', ignore_errors=True)


@pytest.mark.parametrize('backend', backends)
def test_pop_empty_client(backend):
    with Environment(backend) as env:
        client = env.client
        msg = client.pop(QUEUE, NAMESPACE)
        assert msg is None


@pytest.mark.parametrize('backend', backends)
def test_pop_client(backend):
    with Environment(backend) as env:
        client = env.client
        nothing_item = {'json': 'nothing'}
        work_item = {'json': 'work'}
        result_item = {'json': 'result'}

        client.push(QUEUE, NAMESPACE, work_item, WORK_ITEM)
        client.push(QUEUE, NAMESPACE, nothing_item, 0)
        client.push(QUEUE, NAMESPACE, result_item, RESULT_ITEM)

        with client:
            # name = env.monitor.namespaces()[0]
            queue = env.monitor.queues()[0]

            names = (queue, NAMESPACE)
            # assert names == (QUEUE, env.namespace)

            msg = client.pop(QUEUE, NAMESPACE)
            assert msg is not None

            print('log to capture', msg)

            assert env.monitor.read_count(*names) == 1
            assert env.monitor.unactioned_count(*names) == 1

            client.mark_actioned(QUEUE, msg)

            assert env.monitor.unactioned_count(*names) == 0

            print(msg.message, work_item)
            assert msg.message == work_item, 'work_item was inserted first it needs to be first out'
            assert env.monitor.unread_count(*names) == 2

        time.sleep(1)
        # for coverage sake check the queries work
        agents = env.monitor.agents(None)
        print('Agents:', agents)

        # print('Dead Agents:', env.monitor.dead_agents(env.namespace))
        print(env.monitor.log(agents[0]), end='')
        print(env.monitor.lost_messages(QUEUE, NAMESPACE))
        print(env.monitor.requeue_lost_messages(QUEUE, NAMESPACE))
        print(env.monitor.failed_messages(QUEUE, NAMESPACE))
        print(env.monitor.requeue_failed_messages(QUEUE, NAMESPACE))
        print(env.monitor.messages(QUEUE, NAMESPACE))
        print(env.monitor.unread_messages(QUEUE, NAMESPACE))
        print(env.monitor.unactioned_messages(QUEUE, NAMESPACE))


@pytest.mark.parametrize('backend,format', product(backends, formats))
def test_mtype_pop_client(backend, format):
    with Environment(backend) as env:
        client = env.client
        nothing_item = {'json': 'nothing'}
        work_item = {'json': 'work'}
        result_item = {'json': 'result'}

        client.push(QUEUE, NAMESPACE, work_item, WORK_ITEM)
        client.push(QUEUE, NAMESPACE, nothing_item, 0)
        client.push(QUEUE, NAMESPACE, result_item, RESULT_ITEM)

        msg = client.pop(QUEUE, NAMESPACE, RESULT_ITEM)
        client.mark_actioned(QUEUE, msg)
        assert msg.message == result_item, 'result_item is the only message with the correct type'
        env.monitor.archive(
            env.namespace,
            f'test_{backend}.zip',
            namespace_out='new_namespace',
            format=format)

    monitor = new_monitor_process(f'zip:test_{backend}.zip', DATABASE)
    assert len(monitor.messages(QUEUE, 'new_namespace')) == 3
    os.remove(f'test_{backend}.zip')
    monitor.stop()


@pytest.mark.parametrize('backend,format', product(backends, formats))
def test_union_pop_client(backend, format):
    with Environment(backend) as env:
        client = env.client
        nothing_item = {'json': 'nothing'}
        work_item = {'json': 'work'}
        result_item = {'json': 'result'}

        client.push(QUEUE, NAMESPACE, nothing_item, 0)
        client.push(QUEUE, NAMESPACE, work_item, WORK_ITEM)
        client.push(QUEUE, NAMESPACE, result_item, RESULT_ITEM)

        msg = client.pop(QUEUE, NAMESPACE, (WORK_ITEM, RESULT_ITEM))
        client.mark_actioned(QUEUE, msg)
        assert msg.message == work_item, 'work_item was inserted first it needs to be first out'

        env.monitor.archive(
            env.namespace,
            f'test_{backend}.zip',
            namespace_out='new_namespace',
            format=format)

    monitor = new_monitor_process(f'zip:test_{backend}.zip', DATABASE)
    assert len(monitor.messages(QUEUE, 'new_namespace')) == 3
    os.remove(f'test_{backend}.zip')
    monitor.stop()


@pytest.mark.skipif(True, reason='need a db with users')
def test_user_pass():
    uri = f'mongo://user:pass@127.0.0.1:27017'
    client = new_client(uri, 'test')
    client.push(QUEUE, NAMESPACE, 'test')
    _ = client.pop(QUEUE, NAMESPACE)


if __name__ == '__main__':
    test_pop_client('mongo')
