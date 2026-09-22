import json
from pathlib import Path

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


@pytest.mark.parametrize("name", ["", "unknown", "not_an_operator"])
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
    assert len(Operator) == 82
    for operator, code in zip(list(Operator)[:45], [*range(1, 45), 255], strict=True):
        assert payload(operator) == bytes([101, code])


def test_k102_codes():
    names = [
        ":",
        "+",
        "-",
        "*",
        "%",
        "&",
        "|",
        "^",
        "=",
        "<",
        ">",
        "$",
        ",",
        "#",
        "_",
        "~",
        "!",
        "?",
        "@",
        ".",
        "0:",
        "1:",
        "2:",
        "in",
        "within",
        "like",
        "bin",
        "ss",
        "insert",
        "wsum",
        "wavg",
        "div",
        "xexp",
        "setenv",
        "binr",
        "cov",
        "cor",
    ]
    for code, name in enumerate(names):
        assert payload(Operator(name)) == bytes([102, code])
    assert payload(Operator.BINARY_PLUS) == bytes([102, 1])
    assert payload(Operator.IN) == bytes([102, 23])
    assert payload(Operator.WAVG) == bytes([102, 30])
    assert payload(Operator.DIV) == bytes([102, 31])


def test_k101_k102_distinct():
    assert payload(
        [Operator.PLUS, Operator.BINARY_PLUS, None, Operator.ASSIGN]
    ) == bytes([0, 0, 4, 0, 0, 0, 101, 1, 102, 1, 101, 0, 102, 0])
    assert payload("+") == b"\xf5+\0"


@pytest.mark.parametrize(
    "name,code",
    [
        ("flip", 1),
        ("neg", 2),
        ("first", 3),
        ("reciprocal", 4),
        ("ltime", 4),
        ("where", 5),
        ("reverse", 6),
        ("null", 7),
        ("group", 8),
        ("hclose", 10),
        ("string", 11),
        ("count", 13),
        ("floor", 14),
        ("not", 15),
        ("hdel", 15),
        ("key", 16),
        ("inv", 16),
        ("distinct", 17),
        ("type", 18),
        ("value", 19),
        ("get", 19),
        ("read0", 20),
        ("read1", 21),
    ],
)
def test_unary_aliases(name, code):
    assert payload(Operator[name.upper()]) == bytes([101, code])


_REFERENCE_KEYWORDS = json.loads(
    Path(__file__).with_name("q_keyword_types.json").read_text()
)["keywords"]


@pytest.mark.parametrize(
    "entry",
    [entry for entry in _REFERENCE_KEYWORDS if entry.get("wire_type") in (101, 102)],
    ids=lambda entry: entry["name"],
)
def test_reference_keyword_encoding(entry):
    # AND/OR retain their original unary glyph meanings in our enum.
    member = {"and": "BINARY_AND", "or": "BINARY_OR"}.get(
        entry["name"], entry["name"].upper()
    )
    assert payload(Operator[member]) == bytes([entry["wire_type"], entry["code"]])
