from contextlib import closing
import socket
import time

import pytest
import shutil

from cqueue.logs import set_verbose_level
from cqueue.backends import known_backends, new_server
from cqueue.backends import new_client

set_verbose_level(10)
backends = known_backends()

WORK_ITEM = 1
RESULT_ITEM = 2


def get_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


class TestEnvironment:
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
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.server.stop()
        time.sleep(1)
        shutil.rmtree('/tmp/queue/', ignore_errors=True)


@pytest.mark.parametrize('backend', backends)
def test_pop_empty_client(backend):
    with TestEnvironment(backend) as env:
        client = env.client
        msg = client.pop('testing_queue')
        assert msg is None


@pytest.mark.parametrize('backend', backends)
def test_pop_client(backend):
    with TestEnvironment(backend) as env:
        client = env.client
        nothing_item = {'json': 'nothing'}
        work_item = {'json': 'work'}
        result_item = {'json': 'result'}

        client.push('testing_queue', work_item, WORK_ITEM)
        client.push('testing_queue', nothing_item, 0)
        client.push('testing_queue', result_item, RESULT_ITEM)

        msg = client.pop('testing_queue')
        client.mark_actioned('testing_queue', msg)
        assert msg.message == work_item, 'work_item was inserted first it needs to be first out'


@pytest.mark.parametrize('backend', backends)
def test_mtype_pop_client(backend):
    with TestEnvironment(backend) as env:
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


@pytest.mark.parametrize('backend', backends)
def test_union_pop_client(backend):
    with TestEnvironment(backend) as env:
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


if __name__ == '__main__':
    import traceback
    for b in reversed(backends):
        try:
            print(b)
            test_pop_empty_client(b)
            test_pop_client(b)
            test_mtype_pop_client(b)
            test_union_pop_client(b)
            print('--')
        except:
            print(traceback.format_exc())
