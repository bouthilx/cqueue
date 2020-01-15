from msgqueue import new_server, new_client
from msgqueue.worker import BaseWorker
from msgqueue.worker import WORKER_JOIN, WORK_ITEM, RESULT_ITEM, WORKER_LEFT, SHUTDOWN

result = 'result'
work = 'work'


class WorkerReceiver(BaseWorker):
    def __init__(self, uri, namespace, id):
        super(WorkerReceiver, self).__init__(uri, namespace, id, work, result)
        self.new_handler(WORK_ITEM, self.increment)
        self.new_handler(WORKER_JOIN, self.ignore_message)

    def increment(self, message, context):
        return message.message['value'] + 1

    @staticmethod
    def sync_worker(*args, **kwargs):
        """Start the worker in the main process"""
        return WorkerReceiver(*args, **kwargs).run()

    @staticmethod
    def async_worker(*args, **kwargs):
        """Start a worker in a new process"""
        from multiprocessing import Process
        p = Process(target=WorkerReceiver.sync_worker, args=args, kwargs=kwargs)
        p.start()
        return p


class ResultReceiver(BaseWorker):
    def __init__(self, uri, namespace):
        super(ResultReceiver, self).__init__(uri, namespace, 'receiver', result, work)
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
    uri = 'mongo://0.0.0.0:8123'

    # start the message broker
    broker = new_server(uri)
    broker.start()

    # create new queues
    namespace = 'example'

    broker.new_queue(namespace, work)
    broker.new_queue(namespace, result)

    # queue work to be done
    with new_client(uri, namespace) as task_master:
        for i in range(0, 100):
            task_master.push(work, message={'value': i}, mtype=WORK_ITEM)

    workers = [
        WorkerReceiver.async_worker(uri, namespace, w) for w in range(0, 10)
    ]

    receiver = ResultReceiver(uri, namespace)
    receiver.run()

    # Stop Message Broker / Server
    broker.stop()
