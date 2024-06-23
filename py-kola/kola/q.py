import contextlib
import os
import socket

with contextlib.suppress(ImportError):  # Module not available when building docs
    from kola.kola import QConnector


class Q(object):
    def __init__(
        self,
        host: str,
        port: int,
        user="",
        passwd="",
        enable_tls=False,
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
        self.q = QConnector(host, port, user, passwd, enable_tls)

    def connect(self):
        self.q.connect()

    def disconnect(self):
        self.q.shutdown()

    def sync(self, expr: str, *args):
        return self.q.sync(expr, *args)

    def asyn(self, expr: str, *args):
        return self.q.asyn(expr, *args)
