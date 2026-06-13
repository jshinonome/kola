import os
import shutil
import subprocess
from time import sleep

import pytest

from kola import Q

q_test_port = 1801

qConn = Q("localhost", q_test_port)


@pytest.fixture(scope="session")
def start_q_process(request):
    if shutil.which("q") is None:
        pytest.skip("q binary not found on PATH — install kdb+/q to run these tests")

    os.system(
        "lsof -i:{} | tail -1 | awk '{{print $2}}' | xargs kill -9".format(q_test_port)
    )
    proc = subprocess.Popen(["q", "-p", str(q_test_port)])

    timeout = 10  # seconds
    elapsed = 0.0
    while os.system("lsof -i:{}".format(q_test_port)) > 0:
        if proc.poll() is not None:
            pytest.fail(
                "q process exited immediately with code {} — check your kdb+ license".format(
                    proc.returncode
                )
            )
        sleep(0.1)
        elapsed += 0.1
        if elapsed >= timeout:
            proc.kill()
            pytest.fail(
                "q process failed to start on port {} within {}s".format(
                    q_test_port, timeout
                )
            )

    qConn.connect()
    request.addfinalizer(proc.kill)


@pytest.fixture
def q(start_q_process):
    return qConn
