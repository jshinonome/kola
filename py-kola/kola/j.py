import contextlib
import logging
import os
import socket
import time
from typing import Any

with contextlib.suppress(ImportError):  # Module not available when building docs
    from kola.kola import KolaConnector, KolaIOError

logger = logging.getLogger("kola")


# J use IPC protocol version 9, which is not compatible with kdb+
class J(object):
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
        self.j = KolaConnector(host, port, user, passwd, enable_tls, timeout, 9)

    def connect(self):
        self.j.connect()

    def disconnect(self):
        self.j.shutdown()

    def sync(self, expr: str, *args) -> Any:
        if self.retries <= 0:
            return self.j.sync(expr, *args)
        else:
            n = 0
            # exponential backoff
            while n < self.retries:
                try:
                    return self.j.sync(expr, *args)
                except KolaIOError as e:
                    logging.info(
                        "Failed to sync - '%s', retrying in %s seconds", e, 2**n
                    )
                    time.sleep(2**n)
                    n += 1
                    if n == self.retries:
                        raise (e)

    def asyn(self, expr: str, *args) -> None:
        if self.retries <= 0:
            return self.j.asyn(expr, *args)
        else:
            n = 0
            # exponential backoff
            while n < self.retries:
                try:
                    return self.j.asyn(expr, *args)
                except KolaIOError as e:
                    logging.info(
                        "Failed to async - '%s', retrying in %s seconds", e, 2**n
                    )
                    time.sleep(2**n)
                    n += 1
                    if n == self.retries:
                        raise (e)

    def receive(self) -> Any:
        if self.retries <= 0:
            return self.j.receive()
        else:
            n = 0
            # exponential backoff
            while n < self.retries:
                try:
                    return self.j.receive()
                except KolaIOError as e:
                    logging.info(
                        "Failed to receive - '%s', retrying in %s seconds", e, 2**n
                    )
                    self.connect()
                    time.sleep(2**n)
                    n += 1
                    if n == self.retries:
                        raise (e)
