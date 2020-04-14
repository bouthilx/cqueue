import pytest

from msgqueue.logs import set_verbose_level
from msgqueue.backends import known_backends
from msgqueue.worker import BaseWorker, WORK_ITEM, SHUTDOWN, WORKER_JOIN, WORKER_LEFT

from tests.test_client import Environment

set_verbose_level(10)
backends = known_backends()


DATABASE = 'TESTDB'
NAMESPACE = 'TESTNAME'
WORK_QUEUE = 'TESTWORK'
RESULT_QUEUE = 'TESTQUEUE'


class TestWorker(BaseWorker):
    def __init__(self, uri, dbname):
        super(TestWorker, self).__init__(
            uri, DATABASE, NAMESPACE, worker_id='worker-test',
            work_queue=WORK_QUEUE, result_queue=RESULT_QUEUE)

        self.new_handler(WORK_ITEM, self.do_work)

    def do_work(self, message, context):
        self.push_result(message.message['v'] + 1)
        pass


@pytest.mark.parametrize('backend', backends)
def test_base_worker(backend):
    with Environment(backend) as env:
        client = env.client

        # with client:
        client.push(WORK_QUEUE, NAMESPACE, message={'v': 1}, mtype=WORK_ITEM)
        client.push(WORK_QUEUE, NAMESPACE, message={'v': 2}, mtype=WORK_ITEM)
        client.push(WORK_QUEUE, NAMESPACE, message={}, mtype=SHUTDOWN)

        with client:
            # process 2 message and quit
            worker = TestWorker(env.uri, env.namespace)
            worker.run()

            messages = []
            m = client.pop(RESULT_QUEUE, NAMESPACE)
            while m is not None:
                messages.append(m)
                m = client.pop(RESULT_QUEUE, NAMESPACE)

            print(messages)

            assert messages[0].mtype == WORKER_JOIN   # worker join
            assert messages[1].message == 2           # result
            assert messages[2].message == 3           # result
            assert messages[3].mtype == WORKER_LEFT   # worker left


@pytest.mark.parametrize('backend', backends)
def test_base_worker_requeue(backend):
    with Environment(backend) as env:
        client = env.client

        # with client:
        # This message is going to fail & should be requeued 3 times
        client.push(WORK_QUEUE, NAMESPACE, message={'c': 1}, mtype=WORK_ITEM)
        # Shut down is executed only after all the requeues
        client.push(WORK_QUEUE, NAMESPACE, message={}, mtype=SHUTDOWN)

        with client:
            # process 2 message and quit
            worker = TestWorker(env.uri, env.namespace)
            worker.run()

            messages = []
            m = client.pop(RESULT_QUEUE, NAMESPACE)
            while m is not None:
                messages.append(m)
                m = client.pop(RESULT_QUEUE, NAMESPACE)

            assert len(messages) == 2
            assert messages[0].mtype == WORKER_JOIN  # worker join
            assert messages[1].mtype == WORKER_LEFT  # worker left

            failed_messages = client.monitor().failed_messages(WORK_QUEUE, NAMESPACE)
            assert failed_messages[0].mtype == WORK_ITEM  # worker left
            assert failed_messages[0].retry == 3  # worker left


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
