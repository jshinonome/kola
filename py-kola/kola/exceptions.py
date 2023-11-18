import contextlib

with contextlib.suppress(ImportError):  # Module not available when building docs
    from kola.kola import QKolaError

__all__ = [QKolaError]
