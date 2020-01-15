from glob import glob
import os

from cqueue.logs import info
from cqueue.uri import parse_uri


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


client_factory = fetch_factories('cqueue.backends', __file__, 'new_client')
broker_factory = fetch_factories('cqueue.backends', __file__, 'new_server')
monitor_factory = fetch_factories('cqueue.backends', __file__, 'new_monitor')
main_factory = fetch_factories('cqueue.backends', __file__, 'start_server_main')


def known_backends():
    return list(client_factory.keys())


def new_server(uri, location='/tmp/queue/', clean_on_exit=True, join=None):
    options = parse_uri(uri)
    return broker_factory.get(options.get('scheme'))(uri, location, join, clean_on_exit)


def new_client(uri, namespace, name='worker', log_capture=True, timeout=60):
    options = parse_uri(uri)
    return client_factory.get(options.get('scheme'))(uri, namespace, name, log_capture, timeout)


def new_monitor(uri, *args, **kwargs):
    options = parse_uri(uri)
    return monitor_factory.get(options.get('scheme'))(uri, *args, **kwargs)


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
