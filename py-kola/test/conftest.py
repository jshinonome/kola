import os
import subprocess
from time import sleep

import pytest

from kola import Q

test_port = 1801
qConn = Q("localhost", test_port)


@pytest.fixture(scope="session", autouse=True)
def start_q_process(request):
    os.system("lsof -i:{} | tail -1 | awk '{{print $2}}' | xargs kill -9".format(test_port))
    proc = subprocess.Popen(["q", "-p", str(test_port)])
    while os.system("lsof -i:1801") > 0:
        sleep(0.1)
    qConn.connect()
    request.addfinalizer(proc.kill)


@pytest.fixture
def q(start_q_process):
    return qConn
