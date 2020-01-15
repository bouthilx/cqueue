import pytest

from cqueue.logs import set_verbose_level
from cqueue.backends import known_backends, new_monitor

from tests.test_client import TestEnvironment

set_verbose_level(10)
backends = known_backends()

