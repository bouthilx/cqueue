import traceback
import time
from typing import Dict

from cqueue.logs import error, info, warning
from cqueue.backends import make_message_client
from cqueue.backends.queue import MessageQueue, Message

WORK_QUEUE = 'work'
RESULT_QUEUE = 'result'


WORKER_JOIN = 1     # Worker joined work group
WORK_ITEM   = 2     # Worker received work item
RESULT_ITEM = 3     # Worker pushing results
SHUTDOWN    = 4     # Worker should shutdown
WORKER_LEFT = 5     # Worker left work group


class BaseWorker:
    def __init__(self, queue_uri, namespace, worker_id, work_queue, result_queue=None):
        self.uri = queue_uri
        self.namespace = namespace
        self.client: MessageQueue = make_message_client(queue_uri, namespace)
        self.running = False
        self.work_id = worker_id
        self.broker = None
        self.work_queue = work_queue
        self.result_queue = result_queue
        self.context = {}
        self.client.name = f'worker-{self.work_id}'
        self.dispatcher = {
            SHUTDOWN: self.shutdown_worker
        }

    def unregistered_workitem(self, message: Message, context: Dict):
        warning(f'{self.client.name} {message} has no registered handlers')
        return None

    def shutdown_worker(self, message: Message, context: Dict):
        info('shutting down')
        self.running = False
        return None

    def ignore_message(self, message: Message, context: Dict):
        pass

    def new_handler(self, message_type, handler):
        self.dispatcher[message_type] = handler

    def pop_workitem(self):
        return self.client.pop(self.work_queue)

    def push_result(self, result, mtype=RESULT_ITEM, replying_to=None):
        return self.client.push(self.result_queue, message=result, mtype=mtype, replying_to=replying_to)

    def run(self, stop_when_empty=False):
        info('starting worker')

        self.running = True
        self.client.push(self.result_queue, {}, mtype=WORKER_JOIN)

        with self.client:
            while self.running:
                # This code should not throw
                workitem = self.pop_workitem()

                # wait for more work to come through
                if workitem is None:
                    if stop_when_empty:
                        return
                    else:
                        time.sleep(0.01)
                    continue

                handler = self.dispatcher.get(workitem.mtype, self.unregistered_workitem)

                # Error handling for User code
                try:
                    result = handler(workitem, self.context)

                    if self.result_queue is not None and result is not None:
                        self.push_result(result, replying_to=workitem.uid)

                    self.client.mark_actioned(self.work_queue, workitem)
                except Exception:
                    error(traceback.format_exc())

            # --
            self.client.push(self.result_queue, {}, mtype=WORKER_LEFT)
