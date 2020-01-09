import traceback
import time

from cqueue.logs import error, info, warning
from cqueue.backends import make_message_broker, make_message_client
from cqueue.backends.queue import MessageQueue, Message

WORK_QUEUE = 'work'
RESULT_QUEUE = 'result'

WORKER_JOIN = 1     # Worker joined work group
WORK_ITEM   = 2     # Worker received work item
RESULT_ITEM = 3     # Worker pushing results
SHUTDOWN    = 4     # Worker should shutdown
WORKER_LEFT = 5     # Worker left work group


def unregistered_workitem(worker, message, context):
    warning(f'{message} has no registered handlers')
    return None


def shutdown_worker(worker, message, context):
    worker.running = False
    return None


def ignore(worker, message, context):
    pass


class Worker:
    def __init__(self, queue_uri, worker_id, work_queue, result_queue=None):
        self.uri = queue_uri
        self.client: MessageQueue = make_message_client(queue_uri)
        self.running = False
        self.work_id = worker_id
        self.broker = None
        self.work_queue = work_queue
        self.result_queue = result_queue
        self.context = {}
        self.dispatcher = {
            SHUTDOWN: shutdown_worker
        }

    def new_handler(self, message_type, handler):
        self.dispatcher[message_type] = handler

    def pop_workitem(self):
        return self.client.pop(self.work_queue)

    def push_result(self, result, mtype=RESULT_ITEM):
        return self.client.push(self.result_queue, message=result, mtype=mtype)

    def run(self):
        info('starting worker')

        self.running = True
        self.client.name = f'worker-{self.work_id}'
        self.client.push(self.result_queue, message={'worker': '1'}, mtype=WORKER_JOIN)
        last_message = None

        with self.client:
            while self.running:
                try:
                    workitem = self.pop_workitem()

                    # wait for more work to come through
                    if workitem is None:
                        time.sleep(0.01)
                        continue

                    handler = self.dispatcher.get(workitem.mtype, unregistered_workitem)
                    result = handler(self, workitem, self.context)

                    if self.result_queue is not None and result is not None:
                        self.push_result(result)

                    self.client.mark_actioned(self.work_queue, workitem)
                    last_message = workitem

                except Exception:
                    error(traceback.format_exc())

            self.client.push(self.result_queue, message={'worker': '1'}, mtype=WORKER_LEFT)


if __name__ == '__main__':
    from multiprocessing import Process
    uri = 'cockroach://0.0.0.0:8123'

    # start the message broker
    broker = make_message_broker(uri)
    broker.start()

    # create new queues
    broker.new_queue(WORK_QUEUE)
    broker.new_queue(RESULT_QUEUE)

    # queue work to be done
    with make_message_client(uri) as task_master:
        for i in range(0, 100):
            task_master.push(WORK_QUEUE, message={'value': i}, mtype=WORK_ITEM)

    # Worker Setup
    def increment(worker, message, context):
        return message.message['value'] + 1

    # Create the worker
    def new_worker(id):
        worker = Worker(uri, id, WORK_QUEUE, RESULT_QUEUE)
        worker.new_handler(WORK_ITEM, increment)
        worker.new_handler(WORKER_JOIN, ignore)
        worker.run()

    workers = []
    for w in range(0, 10):
        p = Process(target=new_worker, args=(w,))
        workers.append(p)
        p.start()

    # Aggregate the result
    # --------------------
    def read_results(worker, message, context):
        context['count'] = context.get('count', 0) + 1
        print(message.message, end=' ')

        if context['count'] >= 100:
            # Close all workers
            for _ in range(0, 10):
                worker.push_result({}, mtype=SHUTDOWN)

    def worker_join(worker, message, context):
        context['worker'] = context.get('worker', 0) + 1
        print('Workers:', context['worker'])

    def worker_left(worker, message, context):
        context['worker'] = context.get('worker', 0) - 1

        print('Workers:', context['worker'])
        if context['worker'] == 0:
            worker.running = False

    task_master = Worker(uri, 10, RESULT_QUEUE, WORK_QUEUE)
    task_master.new_handler(WORKER_JOIN, worker_join)
    task_master.new_handler(RESULT_ITEM, read_results)
    task_master.new_handler(WORKER_LEFT, worker_left)
    task_master.run()

    broker.stop()
