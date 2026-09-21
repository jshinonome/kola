import pytest

from kola import Operator, serialize_as_ipc_bytes6


def payload(value):
    return serialize_as_ipc_bytes6("sync", False, value)[8:]


@pytest.mark.parametrize(
    "name,code",
    [("+:", 1), ("avg", 23), ("sum", 25), ("hopen", 44), ("::", 255)],
)
def test_operator_serialization(name, code):
    operator = Operator(name)
    assert operator.value == name
    assert payload(operator) == bytes([101, code])


@pytest.mark.parametrize("name", ["", "+", "unknown"])
def test_invalid_operator(name):
    with pytest.raises(ValueError):
        Operator(name)


def test_operator_requires_name():
    with pytest.raises(ValueError):
        Operator(25)


def test_operator_in_containers():
    assert payload([Operator.SUM, None, Operator.PROJECTION_NULL]) == bytes(
        [0, 0, 3, 0, 0, 0, 101, 25, 101, 0, 101, 255]
    )
    assert payload({"f": Operator.SUM}) == bytes(
        [99, 11, 0, 1, 0, 0, 0, 102, 0, 0, 0, 1, 0, 0, 0, 101, 25]
    )


def test_symbols_and_null_unchanged():
    assert payload("sum") == b"\xf5sum\0"
    assert payload(None) == bytes([101, 0])


def test_all_operator_codes():
    assert Operator.PLUS.value == "+:"
    assert Operator.SUM.value == "sum"
    assert len(Operator) == 45
    for operator, code in zip(Operator, [*range(1, 45), 255], strict=True):
        assert payload(operator) == bytes([101, code])
