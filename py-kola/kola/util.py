import contextlib
from typing import Any, Literal

import polars as pl

with contextlib.suppress(ImportError):  # Module not available when building docs
    from kola.kola import deserialize, generate_ipc_msg, read_binary_table


def read_binary(filepath: str) -> Any:
    return read_binary_table(filepath)


def serialize_as_ipc_bytes6(
    msg_type: Literal["async", "sync", "response"],
    enable_compression: bool,
    any: object,
) -> bytes:
    if msg_type not in ["async", "sync", "response"]:
        raise Exception("Expect async|sync|response msg type, but got %s", msg_type)
    return generate_ipc_msg(
        ["async", "sync", "response"].index(msg_type),
        enable_compression,
        any,
    )


def deserialize_bytes6(buf: bytes) -> Any:
    return deserialize(buf)


__all__ = [read_binary, serialize_as_ipc_bytes6, deserialize_bytes6]
