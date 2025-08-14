import os
import subprocess
from time import sleep

import pytest

from kola import J, Q

j6_test_port = 1801
j9_test_port = 1802

qConn = Q("localhost", j6_test_port)
jConn = J("localhost", j9_test_port)


@pytest.fixture(scope="session")
def start_q_process(request):
    os.system(
        "lsof -i:{} | tail -1 | awk '{{print $2}}' | xargs kill -9".format(j6_test_port)
    )
    proc = subprocess.Popen(["q", "-p", str(j6_test_port)])
    while os.system("lsof -i:1801") > 0:
        sleep(0.1)
    qConn.connect()
    request.addfinalizer(proc.kill)


@pytest.fixture
def q(start_q_process):
    return qConn


@pytest.fixture(scope="session")
def start_j_process(request):
    os.system(
        "lsof -i:{} | tail -1 | awk '{{print $2}}' | xargs kill -9".format(j9_test_port)
    )
    proc = subprocess.Popen(["j", "-p", str(j9_test_port)])
    # need to reset terminal to clear the j process output
    os.system("reset")
    while os.system("lsof -i:1802") > 0:
        sleep(0.1)
    jConn.connect()

    def cleanup():
        proc.kill()
        os.system("reset")

    request.addfinalizer(cleanup)


@pytest.fixture
def j(start_j_process):
    return jConn
