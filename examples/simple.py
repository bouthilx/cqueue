from cqueue.backends import make_message_broker, make_message_client
from cqueue.worker import BaseWorker
from cqueue.worker import WORKER_JOIN, WORK_ITEM, WORK_QUEUE, \
    RESULT_ITEM, RESULT_QUEUE, WORKER_LEFT, SHUTDOWN


class WorkerReceiver(BaseWorker):
    def __init__(self, uri, id):
        super(WorkerReceiver, self).__init__(uri, id, WORK_QUEUE, RESULT_QUEUE)
        self.new_handler(WORK_ITEM, self.increment)
        self.new_handler(WORKER_JOIN, self.ignore_message)

    def increment(self, message, context):
        return message.message['value'] + 1


class ResultReceiver(BaseWorker):
    def __init__(self, uri):
        super(ResultReceiver, self).__init__(uri, -1, RESULT_QUEUE, WORK_QUEUE)
        self.new_handler(WORKER_JOIN, self.worker_join)
        self.new_handler(RESULT_ITEM, self.read_results)
        self.new_handler(WORKER_LEFT, self.worker_left)

    def read_results(self, message, context):
        context['count'] = context.get('count', 0) + 1
        print(message.message, end=' ')

        if context['count'] >= 100:
            # Close all workers
            for _ in range(0, context['worker']):
                self.push_result({}, mtype=SHUTDOWN)

    def worker_join(self, message, context):
        context['worker'] = context.get('worker', 0) + 1
        print('worker joined (total: {})'.format(context['worker']))

    def worker_left(self, message, context):
        context['worker'] = context.get('worker', 0) - 1

        print('worker left (total: {})'.format(context['worker']))
        if context['worker'] == 0:
            self.running = False


if __name__ == '__main__':
    from multiprocessing import Process
    uri = 'mongodb://0.0.0.0:8123'

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

    workers = []
    for w in range(0, 10):
        p = Process(target=lambda u, w: WorkerReceiver(u, w).run(), args=(uri, w,))
        workers.append(p)
        p.start()

    receiver = ResultReceiver(uri)
    receiver.run()

    # Stop Message Broker / Server
    broker.stop()
