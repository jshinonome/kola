import contextlib
import logging
import os
import socket
import time

with contextlib.suppress(ImportError):  # Module not available when building docs
    from kola.kola import QConnector, QKolaIOError

logger = logging.getLogger("kola")


class Q(object):
    def __init__(
        self,
        host: str,
        port: int,
        user="",
        passwd="",
        enable_tls=False,
        auto_reconnect=False,
    ):
        if not user:
            try:
                user = os.getlogin()
            except Exception:
                user = "unknown"
        if (not host) or host == socket.gethostname():
            host = "127.0.0.1"
        self.host = host
        self.port = port
        self.user = user
        self.auto_reconnect = auto_reconnect
        self.q = QConnector(host, port, user, passwd, enable_tls)

    def connect(self):
        self.q.connect()

    def disconnect(self):
        self.q.shutdown()

    def sync(self, expr: str, *args):
        if self.auto_reconnect:
            n = 0
            # return 10 times, exponential backoff
            while n < 10:
                try:
                    return self.q.sync(expr, *args)
                except QKolaIOError as e:
                    logging.info(
                        "Failed to query - %s, retrying in %s seconds", e, 2**n
                    )
                    time.sleep(2**n)
                    n += 1
        else:
            return self.q.sync(expr, *args)

    def asyn(self, expr: str, *args):
        return self.q.asyn(expr, *args)
