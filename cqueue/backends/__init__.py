from cqueue.logs import info
from cqueue.uri import parse_uri


def make_cockroach_server(uri, *args, **kwargs):
    from cqueue.backends.cockroachdb import CockRoachDB
    data = parse_uri(uri)

    return CockRoachDB(
        location='/tmp/cockroach/queue',
        addrs=f'{data.get("address")}:{data.get("port")}'
    )


def make_cockroach_client(uri, *args, **kwargs):
    from cqueue.backends.cockroachdb import CKMQClient
    return CKMQClient(uri, *args, **kwargs)


def make_python_client(uri, *args, **kwargs):
    from cqueue.backends.pyqueue import PythonQueue
    return PythonQueue()


def make_python_broker(uri, *args, **kwargs):
    from cqueue.backends.pyqueue import PythonBroker
    return PythonBroker()


def make_mongo_client(uri, *args, **kwargs):
    from cqueue.backends.mongodb import MongoClient
    return MongoClient(uri, *args, **kwargs)


def make_mongo_broker(uri, *args, **kwargs):
    from cqueue.backends.mongodb import MongoDB
    options = parse_uri(uri)
    return MongoDB(options['address'], port=int(options['port']), location='/tmp/mongo')


def make_cockroach_monitor(uri, *args, **kwargs):
    from cqueue.backends.cockroachdb import CKQueueMonitor
    return CKQueueMonitor(uri, *args, **kwargs)


def make_mongo_monitor(uri, *args, **kwargs):
    from cqueue.backends.mongodb import MongoQueueMonitor
    return MongoQueueMonitor(uri, *args, **kwargs)


client_factory = {
    'cockroach': make_cockroach_client,
    'python': make_python_client,
    'mongodb': make_mongo_client,
    'mongo': make_mongo_client,
}

broker_factory = {
    'cockroach': make_cockroach_server,
    'python': make_python_broker,
    'mongodb': make_mongo_broker,
    'mongo': make_mongo_broker
}

monitor_factory = {
    'cockroach': make_cockroach_monitor,
    'mongodb': make_mongo_monitor,
    'mongo': make_mongo_monitor
}


def make_message_broker(uri, *args, **kwargs):
    options = parse_uri(uri)
    return broker_factory.get(options.get('scheme'))(uri, *args, **kwargs)


def make_message_client(uri, *args, **kwargs):
    options = parse_uri(uri)
    return client_factory.get(options.get('scheme'))(uri, *args, **kwargs)


def make_message_monitor(uri, *args, **kwargs):
    options = parse_uri(uri)
    return monitor_factory.get(options.get('scheme'))(uri, *args, **kwargs)


def get_main_script():
    import inspect
    stack = inspect.stack()
    return stack[-1].filename


def ssh_launch(node, cmd, environ):
    cmd = f'ssh -q {node} nohup {cmd} > {node}.out 2> {node}.err < /dev/null &'
    info(cmd)
    return True
