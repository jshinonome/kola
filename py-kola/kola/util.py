import contextlib

import polars as pl

with contextlib.suppress(ImportError):  # Module not available when building docs
    from kola.kola import read_binary_table


def read_binary(filepath: str) -> pl.DataFrame:
    return read_binary_table(filepath)


__all__ = [read_binary]
