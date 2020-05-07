import traceback
import time
from dataclasses import dataclass
from typing import Dict

from msgqueue.logs import error, info, warning
from msgqueue.backends import new_client
from msgqueue.backends.queue import MessageQueue, Message, ActionRecord, RecordQueue

WORK_QUEUE = 'work'
RESULT_QUEUE = 'result'

WORKER_JOIN = 1     # Worker joined work group
WORK_ITEM   = 2     # Worker received work item
RESULT_ITEM = 3     # Worker pushing results
SHUTDOWN    = 4     # Worker should shutdown
WORKER_LEFT = 5     # Worker left work group


class BaseWorker:
    """

    Parameters
    ----------
    namespaced: bool
        if true prevent worker form picking up work from other queues

    timeout: int
        time without message after which the worker will shutdown by itself

    max_retry: int
        Number of time a message with an error will be retried before being dropped

    result_queue: str
        Name of the queue where the result are placed
    """
    def __init__(self, queue_uri, database, namespace, worker_id, work_queue, result_queue=None):
        self.uri = queue_uri
        self.namespace = namespace
        self.client: MessageQueue = new_client(queue_uri, database)
        self.running = False
        self.work_id = worker_id
        self.broker = None
        self.work_queue = work_queue
        self.result_queue = result_queue
        self.context = {}
        self.client.name = f'worker-{self.work_id}'
        self.namespaced = True
        self.timeout = 5 * 60
        self.max_retry = 3
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
        workitem = None
        wait_time = 0
        namespace = None
        if self.namespaced:
            namespace = self.namespace

        while workitem is None:
            workitem = self.client.pop(self.work_queue, namespace)

            if workitem is None:
                time.sleep(0.01)
                wait_time += 0.01

            if wait_time > self.timeout:
                self.shutdown_worker(None, None)
                break

        return workitem

    def requeue(self, queue=None):
        if queue is None:
            queue = self.work_queue

        namespace = None
        if self.namespaced:
            namespace = self.namespace

        requeued = self.client.monitor().requeue_failed_messages(
            queue, namespace, max_retry=self.max_retry)

        info(f'Requeued {requeued} failed messages in {queue}')

        self.client.monitor().requeue_lost_messages(
            queue, namespace, timeout_s=self.timeout, max_retry=self.max_retry)

    def push_result(self, result, mtype=RESULT_ITEM, replying_to=None):
        uid = None
        namespace = self.namespace

        if replying_to:
            uid = replying_to.uid
            namespace = replying_to.namespace

        self.requeue(self.result_queue)
        return self.client.push(
            self.result_queue,
            namespace,
            result,
            mtype=mtype,
            replying_to=uid)

    def run(self):
        info('starting worker')

        self.running = True
        self.client.push(self.result_queue, self.namespace, {}, mtype=WORKER_JOIN)

        with self.client:
            while self.running:
                # Check if messages were lost
                self.requeue()

                # This code should not throw
                workitem = self.pop_workitem()

                if workitem is None:
                    continue

                handler = self.dispatcher.get(workitem.mtype, self.unregistered_workitem)

                namespace = self.client.heartbeat_monitor.message.namespace
                self.context['namespace'] = namespace
                self.context['client'] = self.client

                # Error handling for User code
                try:
                    result = handler(workitem, self.context)

                    if isinstance(result, ActionRecord):
                        ops = RecordQueue(history=result)
                        ops.mark_actioned(self.work_queue, workitem)
                        ops.execute(self.client)
                        continue

                    if self.result_queue is not None and result is not None:
                        self.push_result(result, replying_to=workitem)

                    self.client.mark_actioned(self.work_queue, workitem)

                except KeyboardInterrupt:
                    info('Task interrupted')
                    self.client.mark_error(self.work_queue, workitem, 'interrupted by KeyboardInterrupt')
                    self.client.push(self.result_queue, self.namespace, {}, mtype=WORKER_LEFT)
                    raise
                except Exception:
                    error_str = traceback.format_exc()
                    error(error_str)
                    self.client.mark_error(self.work_queue, workitem, error_str)

            # --
            self.client.push(self.result_queue, self.namespace, {}, mtype=WORKER_LEFT)
