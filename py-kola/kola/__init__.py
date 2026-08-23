import kola.type as QType
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    class KolaQOperator:
        PLUS: ClassVar["KolaQOperator"]

        def __init__(self, name: str) -> None: ...

        @property
        def name(self) -> str: ...

    class KolaQLambda:
        def __init__(self, source: str, context: str = "") -> None: ...

        @property
        def source(self) -> str: ...

        @property
        def context(self) -> str: ...
else:
    from kola.kola import KolaQLambda, KolaQOperator

from kola.exceptions import KolaAuthError, KolaError, KolaIOError
from kola.q import Q
from kola.util import read_binary6, serialize_as_ipc_bytes6

__all__ = [
    serialize_as_ipc_bytes6,
    Q,
    KolaAuthError,
    KolaQLambda,
    KolaQOperator,
    KolaError,
    KolaIOError,
    QType,
    read_binary6,
]
