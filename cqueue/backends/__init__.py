import os
import sys

from olympus.utils import parse_uri, info
from olympus.utils.network import get_network_interface, get_ip_address, get_free_port


def make_cockroach_server(uri, *args, **kwargs):
    from olympus.distributed.cockroachdb import CockRoachDB
    data = parse_uri(uri)

    return CockRoachDB(
        location='/tmp/cockroach/queue',
        addrs=f'{data.get("address")}:{data.get("port")}'
    )


def make_cockroach_client(uri, *args, **kwargs):
    from olympus.distributed.cockroachdb import CKMQClient
    return CKMQClient(uri, *args, **kwargs)


def make_python_client(uri, *args, **kwargs):
    from olympus.distributed.pyqueue import PythonQueue
    return PythonQueue()


def make_python_broker(uri, *args, **kwargs):
    from olympus.distributed.pyqueue import PythonBroker
    return PythonBroker()


def make_mongo_client(uri, *args, **kwargs):
    from olympus.distributed.mongo import MongoClient
    return MongoClient(uri)


def make_mongo_broker(uri, *args, **kwargs):
    from olympus.distributed.mongo import MongoDB
    options = parse_uri(uri)
    return MongoDB(options['address'], port=int(options['port']), location='/tmp/mongo')


client_factory = {
    'cockroach': make_cockroach_client,
    'python': make_python_client,
    'mongodb': make_mongo_client,
}

broker_factory = {
    'cockroach': make_cockroach_server,
    'python': make_python_broker,
    'mongodb': make_mongo_broker
}


def make_message_broker(uri, *args, **kwargs):
    options = parse_uri(uri)
    return broker_factory.get(options.get('scheme'))(uri, *args, **kwargs)


def make_message_client(uri, *args, **kwargs):
    options = parse_uri(uri)
    return client_factory.get(options.get('scheme'))(uri, *args, **kwargs)


def get_main_script():
    import inspect
    stack = inspect.stack()
    return stack[-1].filename


def ssh_launch(node, cmd, environ):
    cmd = f'ssh -q {node} nohup {cmd} > {node}.out 2> {node}.err < /dev/null &'
    info(cmd)
    return True
