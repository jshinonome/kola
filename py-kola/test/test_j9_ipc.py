import logging
import math
from datetime import date, datetime, time, timedelta, timezone

import pytest

from kola import J, KolaError, KolaIOError

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "query,expect",
    [
        ("0b", False),
        ("1b", True),
        # byte
        ("0x00", 0),
        ("0xFF", 255),
        # short
        ("9h", 9),
        # int
        ("9i", 9),
        # long
        ("9", 9),
        # real
        # ("0ne", math.isnan),
        ("-0we", math.isinf),
        ("9e", 9),
        ("0we", math.isinf),
        # float
        # ("0n", math.isnan),
        ("-0w", math.isinf),
        ("9.0", 9),
        ("0w", math.isinf),
        # char
        ('" "', " "),
        ('"J"', "J"),
        ('"JS"', "JS"),
        # symbol
        ("`", ""),
        ("`q", "q"),
        ("`kdb", "kdb"),
        # timestamp
        ("2023.11.11D00:00:00", datetime(2023, 11, 11, 0, 0, tzinfo=timezone.utc)),
        (
            "2023.11.11D10:02:00.979147390",
            datetime(2023, 11, 11, 10, 2, 0, 979147, tzinfo=timezone.utc),
        ),
        # date
        ("2022.05.30", date(2022, 5, 30)),
        # timespan
        ("0D00:00:00", timedelta(seconds=0)),
        ("0D12:34:56.123456789", timedelta(seconds=45296, microseconds=123456)),
        # time
        ("00:00:00.000", time(0, 0)),
        ("12:34:56.789", time(12, 34, 56, 789000)),
        # datetime
        (
            "2023.11.11T00:00:00.000",
            datetime(2023, 11, 11, tzinfo=timezone.utc),
        ),
        (
            "2023.11.11T12:34:56.789",
            datetime(2023, 11, 11, 12, 34, 56, 789000, tzinfo=timezone.utc),
        ),
    ],
)
def test_read_atom(j, query, expect):
    actual = j.sync(query)
    if callable(expect):
        assert expect(actual)
    else:
        assert actual == expect


def test_error(j):
    with pytest.raises(KolaError, match="Unsupported"):
        j.sync("1+`a")


def test_auto_connect(j):
    j.disconnect()
    j.sync("now`")
    j.connect()


def test_asyn(j):
    j.asyn(".kola.x:18")
    assert 18 == j.sync(".kola.x")


def test_io_error():
    j = J("DUMMY", 1800)
    with pytest.raises(
        KolaIOError,
        match="failed to lookup address information: Name or service not known",
    ):
        j.sync("1+`a")
