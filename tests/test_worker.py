import pytest

from cqueue.logs import set_verbose_level
from cqueue.backends import known_backends
from cqueue.worker import BaseWorker, WORK_ITEM, SHUTDOWN, WORKER_JOIN, WORKER_LEFT

from tests.test_client import TestEnvironment

set_verbose_level(10)
backends = known_backends()


class TestWorker(BaseWorker):
    def __init__(self, uri, namespace):
        super(TestWorker, self).__init__(
            uri, namespace, worker_id='worker-test',
            work_queue='test_work', result_queue='test_result')

        self.new_handler(WORK_ITEM, self.do_work)

    def do_work(self, message, context):
        self.push_result(message.message['v'] + 1)
        pass


@pytest.mark.parametrize('backend', backends)
def test_base_worker(backend):
    with TestEnvironment(backend) as env:
        client = env.client

        # with client:
        client.push('test_work', message={'v': 1}, mtype=WORK_ITEM)
        client.push('test_work', message={'v': 2}, mtype=WORK_ITEM)
        client.push('test_work', message={}, mtype=SHUTDOWN)

        with client:
            # process 2 message and quit
            worker = TestWorker(env.uri, env.namespace)
            worker.run()

            assert client.pop('test_result').mtype == WORKER_JOIN   # worker join
            assert client.pop('test_result').message == 2           # result
            assert client.pop('test_result').message == 3           # result
            assert client.pop('test_result').mtype == WORKER_LEFT   # worker left


if __name__ == '__main__':
    import traceback
    for b in ['cockroach']:
        try:
            print(b)
            test_base_worker(b)
            print('--')
        except:
            print(traceback.format_exc())
            raise
