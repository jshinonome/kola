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
        retries=0,
        timeout=0,
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
        self.retries = retries
        self.q = QConnector(host, port, user, passwd, enable_tls, timeout)

    def connect(self):
        self.q.connect()

    def disconnect(self):
        self.q.shutdown()

    def sync(self, expr: str, *args):
        if self.retries <= 0:
            return self.q.sync(expr, *args)
        else:
            n = 0
            # exponential backoff
            while n < self.retries:
                try:
                    return self.q.sync(expr, *args)
                except QKolaIOError as e:
                    logging.info(
                        "Failed to sync - '%s', retrying in %s seconds", e, 2**n
                    )
                    time.sleep(2**n)
                    n += 1
                    if n == self.retries:
                        raise (e)

    def asyn(self, expr: str, *args):
        if self.retries <= 0:
            return self.q.asyn(expr, *args)
        else:
            n = 0
            # exponential backoff
            while n < self.retries:
                try:
                    return self.q.asyn(expr, *args)
                except QKolaIOError as e:
                    logging.info(
                        "Failed to async - '%s', retrying in %s seconds", e, 2**n
                    )
                    time.sleep(2**n)
                    n += 1
                    if n == self.retries:
                        raise (e)

    def receive(self):
        if self.retries <= 0:
            return self.q.receive()
        else:
            n = 0
            # exponential backoff
            while n < self.retries:
                try:
                    return self.q.receive()
                except QKolaIOError as e:
                    logging.info(
                        "Failed to receive - '%s', retrying in %s seconds", e, 2**n
                    )
                    self.connect()
                    time.sleep(2**n)
                    n += 1
                    if n == self.retries:
                        raise (e)
