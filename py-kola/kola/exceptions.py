import contextlib

with contextlib.suppress(ImportError):  # Module not available when building docs
    from kola.kola import QKolaAuthError, QKolaError, QKolaIOError

__all__ = [QKolaError, QKolaIOError, QKolaAuthError]
