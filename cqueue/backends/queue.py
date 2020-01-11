from dataclasses import dataclass
from datetime import datetime
import threading


@dataclass
class Agent:
    uid: int
    time: datetime
    agent: str
    heartbeat: datetime
    alive: bool
    message: int


@dataclass
class Message:
    uid: int
    time: datetime
    mtype: int
    read: bool
    read_time: datetime
    actioned: bool
    actioned_time: datetime
    replying_to: int
    message: str

    def __repr__(self):
        return f"""Message({self.uid}, {self.time}, {self.mtype}, {self.read}, """ +\
            f"""{self.read_time}, {self.actioned}, {self.actioned_time}, {self.message})"""


class QueuePacemaker(threading.Thread):
    def __init__(self, agent, namespace, wait_time=60):
        threading.Thread.__init__(self)
        self.namespace = namespace
        self.stopped = threading.Event()
        self.wait_time = wait_time
        self.agent = agent
        self.agent_id = None

    def register_agent(self, agent_name):
        raise NotImplementedError()

    def run(self):
        """Run the trial monitoring every given interval."""
        while not self.stopped.wait(self.wait_time):
            self.update_heartbeat()

    def update_heartbeat(self):
        raise NotImplementedError()

    def register_message(self, message):
        pass

    def stop(self):
        """Stop monitoring."""
        self.stopped.set()
        self.join()

    def unregister_agent(self):
        raise NotImplementedError()


class MessageQueue:
    def pacemaker(self, namespace, wait_time):
        raise NotImplementedError()

    def __enter__(self):
        self.heartbeat_monitor = self.pacemaker(self.namespace, wait_time=60)
        self.heartbeat_monitor.register_agent(self.name)
        self.heartbeat_monitor.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.heartbeat_monitor.stop()
        self.heartbeat_monitor.unregister_agent()

    def enqueue(self, name, message, mtype=0, replying_to=None):
        """Insert a new message inside the queue

        Parameters
        ----------
        name: str
            Message queue namespace

        message: str
            message to insert

        mtype: int
            message type

        replying_to: int
            message id this message replies to
        """
        raise NotImplementedError()

    def dequeue(self, name):
        """Remove oldest message from the queue i.e mark is as read

        Parameters
        ----------
        name: str
            Queue namespace to pop message from

        """
        raise NotImplementedError()

    def mark_actioned(self, name, message: Message = None, uid: int = None):
        """Mark a message as actioned

        Parameters
        ----------
        name: str
            Message queue namespace

        message: Optional[str]
            message object to update

        uid: Optional[int]
            uid of the message to update

        """
        raise NotImplementedError()

    def push(self, name, message, mtype=0, replying_to=None):
        return self.enqueue(name, message, mtype, replying_to)

    def pop(self, name):
        return self.dequeue(name)

    def get_reply(self, name):
        raise NotImplementedError()


class QueueMonitor:
    def get_namespaces(self):
        raise NotImplementedError()

    def get_all_messages(self, namespace, name, limit=100):
        raise NotImplementedError()

    def get_unread_messages(self, namespace, name):
        raise NotImplementedError()

    def get_unactioned_messages(self, namespace, name):
        raise NotImplementedError()

    def unread_count(self, namespace, name):
        raise NotImplementedError()

    def unactioned_count(self, namespace, name):
        raise NotImplementedError()

    def actioned_count(self, namespace, name):
        raise NotImplementedError()

    def read_count(self, namespace, name):
        raise NotImplementedError()

    def reset_queue(self, namespace, name):
        raise NotImplementedError()

    def agents(self, namespace):
        raise NotImplementedError()


