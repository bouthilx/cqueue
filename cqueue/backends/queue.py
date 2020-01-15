from dataclasses import dataclass
from datetime import datetime
import threading
from typing import Union, List


@dataclass
class Agent:
    uid: int             # Unique ID of the agent
    time: datetime       # Time the agent was created
    agent: str           # Name of the Agent (Names are not unique)
    heartbeat: datetime  # Last time we had a proof of life
    alive: bool          # Is the Agent Alive
    # Necessary to detect messages that are stuck
    message: int         # Message the Agent is processing
    queue: str           # Message queue the message belong to


@dataclass
class Message:
    uid: int                        # Unique ID of the message
    time: datetime                  # Time that message was created
    mtype: int                      # type of message
    read: bool                      # Was that message read
    read_time: datetime             # Time when that message was read
    actioned: bool                  # Was that message processed
    actioned_time: datetime         # Time when that message was done being processed
    replying_to: int                # Message ID this message relies to
    message: str                    # User data

    def __repr__(self):
        return f"""Message({self.uid}, {self.time}, {self.mtype}, {self.read}, """ +\
            f"""{self.read_time}, {self.actioned}, {self.actioned_time}, {self.message})"""


class _Buffer:
    def __init__(self, file, pacemaker, ltype=0):
        self.file = file
        self.pacemaker = pacemaker
        self.ltype = ltype

    def flush(self):
        if self.file is not None:
            self.file.flush()

    def write(self, data):
        import traceback
        try:
            self.pacemaker.insert_log_line(data, ltype=self.ltype)
        except Exception as e:
            print(f'`{data}`', file=self.file)
            print(traceback.format_exc(), file=self.file)

        if self.file is not None:
            self.file.write(data)


class QueuePacemaker(threading.Thread):
    def __init__(self, agent, namespace, wait_time, capture):
        threading.Thread.__init__(self)
        self.namespace = namespace
        self.stopped = threading.Event()
        self.wait_time = wait_time
        self.agent = agent
        self.agent_id = None
        self.capture = capture
        if capture:
            self.capture_output()

    def capture_output(self):
        # capture std output
        import sys
        sys.stdout = _Buffer(sys.stdout, self, ltype=0)
        sys.stderr = _Buffer(sys.stderr, self, ltype=0)

        # capture logging output
        import logging

        stream = _Buffer(None, self, ltype=0)

        root = logging.getLogger()
        root.propagate = False
        ch = logging.StreamHandler(stream)
        formatter = logging.Formatter(
            '%(relativeCreated)8d [%(levelname)8s] %(name)s [%(process)d] %(pathname)s:%(lineno)d %(message)s')
        ch.setFormatter(formatter)
        root.addHandler(ch)

    def register_agent(self, agent_name):
        raise NotImplementedError()

    def run(self):
        """Run the trial monitoring every given interval."""
        while not self.stopped.wait(self.wait_time):
            self.update_heartbeat()

    def update_heartbeat(self):
        raise NotImplementedError()

    def register_message(self, name, message):
        raise NotImplementedError()

    def unregister_message(self, uid):
        raise NotImplementedError()

    def stop(self):
        """Stop monitoring."""
        self.stopped.set()
        self.join()

        if self.capture:
            import sys
            sys.stdout = sys.stdout.file
            sys.stderr = sys.stderr.file

    def unregister_agent(self):
        raise NotImplementedError()

    def insert_log_line(self, line, ltype=0):
        raise NotImplementedError()


class MessageQueue:
    def pacemaker(self, namespace, wait_time, capture):
        raise NotImplementedError()

    def __enter__(self):
        self.heartbeat_monitor = self.pacemaker(self.namespace, self.timeout, self.capture)
        self.heartbeat_monitor.register_agent(self.name)
        self.heartbeat_monitor.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.heartbeat_monitor.stop()
        self.heartbeat_monitor.unregister_agent()
        self.heartbeat_monitor.client = None

    def join(self):
        return self.heartbeat_monitor.join()

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

    def dequeue(self, name, mtype: Union[int, List[int]] = None):
        """Remove oldest message from the queue i.e mark is as read

        Parameters
        ----------
        name: str
            Queue namespace to pop message from

        mtype: Union[int, List[int]
            type of message to look for (default: none)
        """
        raise NotImplementedError()

    def mark_actioned(self, name, message: Union[Message, int]):
        """Mark a message as actioned

        Parameters
        ----------
        name: str
            Message queue namespace

        message: Union[Message, int]
            message object to update or uid of the message

        """
        raise NotImplementedError()

    def push(self, name, message, mtype=0, replying_to=None):
        return self.enqueue(name, message, mtype, replying_to)

    def pop(self, name, mtype: Union[int, List[int]] = None):
        return self.dequeue(name, mtype)

    def get_reply(self, name, message: Union[int, List[int]]):
        raise NotImplementedError()

    def _register_message(self, name, msg):
        if self.heartbeat_monitor:
            return self.heartbeat_monitor.register_message(name, msg)

        return msg

    def _unregister_message(self, uid):
        if self.heartbeat_monitor:
            return self.heartbeat_monitor.unregister_message(uid)


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

    def fetch_dead_agents(self, namespace, timeout_s=60):
        raise NotImplementedError()

    def fetch_lost_messages(self, namespace, timeout_s=60):
        raise NotImplementedError()

    def requeue_messages(self, namespace):
        raise NotImplementedError()

    def get_log(self, namespace, agent: Union[Agent, int], ltype: int = 0):
        raise NotImplementedError()

