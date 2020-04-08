from contextlib import closing
import socket
import time

import os
import pytest
import shutil
from itertools import product

from msgqueue.logs import set_verbose_level
from msgqueue.backends import known_backends, new_server
from msgqueue.backends import new_client, new_monitor_process

set_verbose_level(10)
backends = known_backends()
formats = ['json', 'bson']

WORK_ITEM = 1
RESULT_ITEM = 2


def get_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


class Environment:
    def __init__(self, backend):
        port = get_free_port()
        self.uri = f'{backend}://root:pass123@localhost:{port}'
        self.server = None
        self.client = None
        self.namespace = 'testing_namespace'

    def __enter__(self):
        self.server = new_server(uri=self.uri)
        self.server.start(wait=True)
        self.client = new_client(self.uri, self.namespace, 'client-test')
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
        msg = client.pop('testing_queue')
        assert msg is None


@pytest.mark.parametrize('backend', backends)
def test_pop_client(backend):
    with Environment(backend) as env:
        client = env.client
        nothing_item = {'json': 'nothing'}
        work_item = {'json': 'work'}
        result_item = {'json': 'result'}

        client.push('testing_queue', work_item, WORK_ITEM)
        client.push('testing_queue', nothing_item, 0)
        client.push('testing_queue', result_item, RESULT_ITEM)

        with client:
            name = env.monitor.namespaces()[0]
            queue = env.monitor.queues(name)[0]
            names = (name, queue)
            assert names == (env.namespace, 'testing_queue')

            msg = client.pop('testing_queue')

            print('log to capture')

            assert env.monitor.read_count(*names) == 1
            assert env.monitor.unactioned_count(*names) == 1

            client.mark_actioned('testing_queue', msg)

            assert env.monitor.unactioned_count(*names) == 0

            assert msg.message == work_item, 'work_item was inserted first it needs to be first out'
            assert env.monitor.unread_count(*names) == 2

        time.sleep(1)
        # for coverage sake check the queries work
        agents = env.monitor.agents(env.namespace)
        print(agents)

        print(env.monitor.dead_agents(env.namespace))
        print(env.monitor.log(env.namespace, agents[0]), end='')
        print(env.monitor.lost_messages(env.namespace))
        print(env.monitor.requeue_lost_messages(env.namespace))
        print(env.monitor.failed_messages(*names))
        print(env.monitor.requeue_failed_messages(*names))
        print(env.monitor.messages(*names))
        print(env.monitor.unread_messages(*names))
        print(env.monitor.unactioned_messages(*names))


@pytest.mark.parametrize('backend,format', product(backends, formats))
def test_mtype_pop_client(backend, format):
    with Environment(backend) as env:
        client = env.client
        nothing_item = {'json': 'nothing'}
        work_item = {'json': 'work'}
        result_item = {'json': 'result'}

        client.push('testing_queue', work_item, WORK_ITEM)
        client.push('testing_queue', nothing_item, 0)
        client.push('testing_queue', result_item, RESULT_ITEM)

        msg = client.pop('testing_queue', RESULT_ITEM)
        client.mark_actioned('testing_queue', msg)
        assert msg.message == result_item, 'result_item is the only message with the correct type'
        env.monitor.archive(
            env.namespace,
            f'test_{backend}.zip',
            namespace_out='new_namespace',
            format=format)

    monitor = new_monitor_process(f'zip:test_{backend}.zip')
    assert len(monitor.messages('new_namespace', 'testing_queue')) == 3
    os.remove(f'test_{backend}.zip')
    monitor.stop()


@pytest.mark.parametrize('backend,format', product(backends, formats))
def test_union_pop_client(backend, format):
    with Environment(backend) as env:
        client = env.client
        nothing_item = {'json': 'nothing'}
        work_item = {'json': 'work'}
        result_item = {'json': 'result'}

        client.push('testing_queue', nothing_item, 0)
        client.push('testing_queue', work_item, WORK_ITEM)
        client.push('testing_queue', result_item, RESULT_ITEM)

        msg = client.pop('testing_queue', (WORK_ITEM, RESULT_ITEM))
        client.mark_actioned('testing_queue', msg)
        assert msg.message == work_item, 'work_item was inserted first it needs to be first out'

        env.monitor.archive(
            env.namespace,
            f'test_{backend}.zip',
            namespace_out='new_namespace',
            format=format)

    monitor = new_monitor_process(f'zip:test_{backend}.zip')
    assert len(monitor.messages('new_namespace', 'testing_queue')) == 3
    os.remove(f'test_{backend}.zip')
    monitor.stop()


if __name__ == '__main__':
    import traceback
    for b in reversed(backends):
        try:
            print(b)
            test_pop_client(b)
            print('--')
        except:
            print(traceback.format_exc())
