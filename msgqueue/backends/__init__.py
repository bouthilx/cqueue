from glob import glob
import os
from multiprocessing import RLock

from msgqueue.logs import info
from msgqueue.uri import parse_uri
from msgqueue.backends.queue import QueueMonitor, QueueServer, MessageQueue


def make_delayed_import_error(error):
    def raise_import_error():
        raise error
    return raise_import_error()


def fetch_factories(base_module, base_file_name, function_name):
    factories = {}
    module_path = os.path.dirname(os.path.abspath(base_file_name))

    for module_path in glob(os.path.join(module_path, '[A-Za-z]*')):
        module_file = module_path.split(os.sep)[-1]

        if module_file == base_file_name:
            continue

        module_name = module_file.split(".py")[0]

        print(module_name)
        try:
            module = __import__(".".join([base_module, module_name]), fromlist=[''])

            if hasattr(module, function_name):
                builders = getattr(module, function_name)

                if not isinstance(builders, dict):
                    builders = {module_name: builders}

                for key, builder in builders.items():
                    factories[key] = builder

        except ImportError as e:
            factories[module_name] = make_delayed_import_error(e)

    return factories


client_factory = fetch_factories('msgqueue.backends', __file__, 'new_client')
broker_factory = fetch_factories('msgqueue.backends', __file__, 'new_server')
monitor_factory = fetch_factories('msgqueue.backends', __file__, 'new_monitor')
main_factory = fetch_factories('msgqueue.backends', __file__, 'start_server_main')


def known_backends():
    return list(client_factory.keys())


def _maybe(dictionary, key):
    fun = dictionary.get(key)

    if fun is None:
        raise KeyError(f'`{key}` backend was not found; pick among the known backends: {list(dictionary.keys())}')

    return fun


def new_server(uri, location='/tmp/queue/', clean_on_exit=True, join=None) -> QueueServer:
    options = parse_uri(uri)
    return _maybe(broker_factory, options.get('scheme'))(uri, location, join, clean_on_exit)


def new_client(uri, namespace, name='worker', log_capture=True, timeout=60) -> MessageQueue:
    options = parse_uri(uri)
    return _maybe(client_factory, options.get('scheme'))(uri, namespace, name, log_capture, timeout)


def new_monitor(uri, *args, **kwargs) -> QueueMonitor:
    options = parse_uri(uri)
    return _maybe(monitor_factory, options.get('scheme'))(uri, *args, **kwargs)


def main(name):
    return main_factory[name]()


def get_main_script():
    import inspect
    stack = inspect.stack()
    return stack[-1].filename


def ssh_launch(node, cmd, environ):
    cmd = f'ssh -q {node} nohup {cmd} > {node}.out 2> {node}.err < /dev/null &'
    info(cmd)
    return True


def _worker(remote, parent_remote, factory, *args, **kwargs):
    print('New Monitor process')
    obj = factory(*args, **kwargs)
    print(parent_remote)
    parent_remote.close()

    while True:
        cmd, args, kwargs = remote.recv()

        if cmd == 'stop':
            break

        result = getattr(obj, cmd)(*args, **kwargs)
        remote.send(result)

    remote.close()


class RemoteMonitor:
    """Spawn the monitor in a child process"""
    def __init__(self, *args, **kwargs):
        from multiprocessing import Process, Pipe
        self.remote, work_remote = Pipe()
        self.lock = RLock()

        worker = Process(
            target=_worker,
            args=(work_remote, self.remote, new_monitor, *args),
            kwargs=kwargs)

        worker.start()
        work_remote.close()
        self.fun_name = None

    def new_rpc(self, function):
        def send_rpc(*args, **kwargs):
            with self.lock:
                self.remote.send((function, args, kwargs))
                try:
                    return self.remote.recv()
                except EOFError:
                    if function != 'stop':
                        raise

        print(f'new rpc for {function}')
        return send_rpc

    def __getattr__(self, item):
        return RemoteMonitor.new_rpc(self, item)


def new_monitor_process(*args, **kwargs):
    mon = RemoteMonitor(*args, **kwargs)
    return mon
