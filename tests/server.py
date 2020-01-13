import pytest
import time

from cqueue.backends import known_backends, new_server

backends = known_backends()


@pytest.mark.parametrize('backend', backends)
def test_server_start_stop(backend):
    """Make sure the server is able to start and stop without hanging"""
    server = new_server(uri=f'{backend}://0.0.0.0:8123')
    server.start(wait=True)

    time.sleep(1)

    server.stop()

