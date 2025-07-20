import contextlib

with contextlib.suppress(ImportError):  # Module not available when building docs
    from kola.kola import KolaAuthError, KolaError, KolaIOError

__all__ = [KolaError, KolaIOError, KolaAuthError]
