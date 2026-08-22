import contextlib
import os
import shutil
import subprocess
import time
from pathlib import Path

import pytest

from kola import KolaError, KolaIOError, Q

startup_timeout = 10
q_external = os.environ.get("KOLA_TEST_Q_EXTERNAL") == "1"
q_connection = None


def _load_q_address():
    host = os.environ.get("KOLA_TEST_Q_HOST", "127.0.0.1").strip()
    if not host:
        raise pytest.UsageError("KOLA_TEST_Q_HOST must not be empty")

    raw_port = os.environ.get("KOLA_TEST_Q_PORT", "1801")
    try:
        port = int(raw_port)
    except ValueError as error:
        raise pytest.UsageError(
            f"KOLA_TEST_Q_PORT must be an integer, got {raw_port!r}"
        ) from error
    if not 1 <= port <= 65535:
        raise pytest.UsageError(
            f"KOLA_TEST_Q_PORT must be between 1 and 65535, got {port}"
        )
    return host, port


q_test_host, q_test_port = _load_q_address()


def _fail_if_q_exited(proc):
    return_code = proc.poll()
    if return_code is not None:
        pytest.fail(
            f"q process exited during startup with code {return_code}. Check that the "
            f"KDB-X/kdb+ license is valid and unexpired and that port {q_test_port} is "
            "available."
        )


def _connect_to_q(proc=None):
    deadline = time.monotonic() + startup_timeout
    last_error = None
    while True:
        if proc is not None:
            _fail_if_q_exited(proc)

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break

        connection = Q(
            q_test_host,
            q_test_port,
            timeout=startup_timeout,
        )
        try:
            connection.connect()
            return connection
        except (KolaError, KolaIOError) as error:
            last_error = error
            with contextlib.suppress(KolaError, KolaIOError):
                connection.disconnect()
            time.sleep(min(0.1, remaining))

    if proc is not None:
        _fail_if_q_exited(proc)
        subject = "q process"
    else:
        subject = "external q server"
    pytest.fail(
        f"{subject} failed the q IPC handshake at {q_test_host}:{q_test_port} "
        f"within {startup_timeout}s. Last error: {last_error}"
    )


def _stop_process(proc):
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=2)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=2)


@pytest.fixture(scope="session")
def start_q_process():
    global q_connection

    proc = None
    try:
        if not q_external:
            q_path = shutil.which("q")
            if q_path is None:
                pytest.skip("q binary not found on PATH; install kdb+/q to run these tests")
            init_script = Path(__file__).resolve().parents[2] / "testing/kdb/init.q"
            allowed_environment = {
                "DYLD_LIBRARY_PATH",
                "HOME",
                "KOLA_Q_ROWS",
                "KX_UPLOAD_TELEMETRY",
                "LD_LIBRARY_PATH",
                "LOCALAPPDATA",
                "LOGNAME",
                "PATH",
                "QLIC",
                "QHOME",
                "QINIT",
                "SystemRoot",
                "TEMP",
                "TMP",
                "TMPDIR",
                "USER",
                "USERPROFILE",
                "WINDIR",
            }
            q_environment = {
                name: value
                for name, value in os.environ.items()
                if name in allowed_environment
            }
            proc = subprocess.Popen(
                [q_path, str(init_script), "-p", f"127.0.0.1:{q_test_port}"],
                env=q_environment,
            )

        q_connection = _connect_to_q(proc)
        yield
    finally:
        if q_connection is not None:
            with contextlib.suppress(KolaError, KolaIOError):
                q_connection.disconnect()
        if proc is not None:
            _stop_process(proc)


@pytest.fixture
def q(start_q_process):
    assert q_connection is not None
    return q_connection
