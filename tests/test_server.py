import pytest
import time

from msgqueue.logs import set_verbose_level
from msgqueue.backends import known_backends, new_server


set_verbose_level(10)
backends = known_backends()


@pytest.mark.parametrize('backend', backends)
def test_server_start_stop(backend):
    """Make sure the server is able to start and stop without hanging"""
    print(backend)
    server = new_server(uri=f'{backend}://test_user:pass123@localhost:8123')
    server.start(wait=True)

    server.new_queue('namespace', 'qeueu')
    time.sleep(1)
    server.stop()


if __name__ == '__main__':
    for b in backends:
        test_server_start_stop(b)
