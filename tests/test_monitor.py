import pytest

from msgqueue.logs import set_verbose_level
from msgqueue.backends import known_backends, new_monitor

from tests.test_client import TestEnvironment

set_verbose_level(10)
backends = known_backends()

