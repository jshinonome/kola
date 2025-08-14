import logging
import math
from datetime import date, datetime, time, timedelta, timezone

import polars as pl
import pytest

from kola import KolaError, KolaIOError, Q

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "query,expect",
    [
        ("0b", False),
        ("1b", True),
        # guid
        ("0Ng", "00000000-0000-0000-0000-000000000000"),
        (
            '"G"$"28293c64-d8d8-cb14-cdde-ceef9ee93847"',
            "28293c64-d8d8-cb14-cdde-ceef9ee93847",
        ),
        # byte
        ("0x00", 0),
        ("0xFF", 255),
        # short
        ("0Nh", -32768),
        ("-0Wh", -32767),
        ("9h", 9),
        ("0Wh", 32767),
        # int
        ("0Ni", -2147483648),
        ("-0Wi", -2147483647),
        ("9i", 9),
        ("0Wi", 2147483647),
        # long
        ("0N", -9223372036854775808),
        ("-0W", -9223372036854775807),
        ("9", 9),
        ("0W", 9223372036854775807),
        # real
        ("0ne", math.isnan),
        ("-0we", math.isinf),
        ("9e", 9),
        ("0we", math.isinf),
        # float
        ("0n", math.isnan),
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
        ("0Np", datetime(1970, 1, 1, 0, 0, tzinfo=timezone.utc)),
        ("2023.11.11D0", datetime(2023, 11, 11, 0, 0, tzinfo=timezone.utc)),
        (
            "2023.11.11D10:02:00.979147390",
            datetime(2023, 11, 11, 10, 2, 0, 979147, tzinfo=timezone.utc),
        ),
        # date
        ("0Nd", date(1, 1, 1)),
        ("2022.05.30", date(2022, 5, 30)),
        # timespan
        ("0D00", timedelta(seconds=0)),
        ("0D12:34:56.123456789", timedelta(seconds=45296, microseconds=123456)),
        # minute
        ("00:00:00", time(0, 0)),
        ("12:34:56", time(12, 34, 56)),
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
def test_read_atom(q, query, expect):
    actual = q.sync(query)
    if callable(expect):
        assert expect(actual)
    else:
        assert actual == expect


@pytest.mark.parametrize(
    "query,expect",
    [
        # bool
        ("10b", pl.Series("boolean", [True, False])),
        ("(,)0b", pl.Series("boolean", [False])),
        # guid
        (
            "(,)0Ng",
            pl.Series(
                "guid", [bytes.fromhex("00000000000000000000000000000000")], pl.Binary
            ),
        ),
        (
            '0Ng,"G"$"5ae7962d-49f2-404d-5aec-f7c8abbae288"',
            pl.Series(
                "guid",
                [
                    bytes.fromhex("00000000000000000000000000000000"),
                    bytes.fromhex("5ae7962d49f2404d5aecf7c8abbae288"),
                ],
                pl.Binary,
            ),
        ),
        # byte
        ("0x00FF", pl.Series("byte", [0, 255], pl.UInt8)),
        # short
        ("0N -0W 9 0Wh", pl.Series("short", [None, -32767, 9, 32767], pl.Int16)),
        # int
        (
            "0N -0W 9 0Wi",
            pl.Series("int", [None, None, 9, None], pl.Int32),
        ),
        # long
        (
            "0N -0W 9 0W",
            pl.Series("long", [None, None, 9, None], pl.Int64),
        ),
        # real
        (
            "0n -0w 9 0we",
            pl.Series("real", [None, float("-inf"), 9.0, float("inf")], pl.Float32),
        ),
        # float
        (
            "0n -0w 9 0w",
            pl.Series("float", [None, float("-inf"), 9.0, float("inf")], pl.Float64),
        ),
        # string
        ('("";"string")', pl.Series("string", ["", "string"])),
        # symbol
        ("``q`kdb", pl.Series("symbol", ["", "q", "kdb"])),
        # timestamp
        (
            "0N 2021.06.03D0 2021.06.03D12:34:56.123456789p",
            pl.Series(
                "timestamp",
                [None, 1622678400000000000, 1622723696123456789],
                pl.Datetime("ns"),
            ),
        ),
        # date
        ("0N 2022.05.30d", pl.Series("date", [None, 19142], pl.Date)),
        # timespan
        (
            "0N 0D00 0D12:34:56.123456789n",
            pl.Series("timespan", [None, 0, 45296123456789], pl.Duration("ns")),
        ),
        # minute
        ("0N 00:00 12:34u", pl.Series("second", [None, 0, 45240000000000], pl.Time)),
        # second
        (
            "0N 00:00:00 12:34:56v",
            pl.Series("second", [None, 0, 45296000000000], pl.Time),
        ),
        # time
        (
            "0n 00:00:00.000 12:34:56.789t",
            pl.Series("time", [None, 0, 45296789000000], pl.Time),
        ),
        # datetime
        (
            "0n 2022.06.03T00:00:00.000 2022.06.03T12:34:56.789z",
            pl.Series(
                "datetime",
                [None, 1654214400000000000, 1654259696789000000],
                pl.Datetime("ns"),
            ),
        ),
        # nested list
        (
            "(1 2;();3 4)",
            pl.Series("long", [[1, 2], [], [3, 4]]),
        ),
        # empty list
        (
            "()",
            pl.Series("null", []),
        ),
    ],
)
def test_read_list(q, query, expect):
    actual = q.sync(query)
    assert (actual == expect).all()


def test_error(q):
    with pytest.raises(KolaError, match="type"):
        q.sync("1+`a")
    with pytest.raises(KolaError, match='"Not supported empty dictionary"'):
        q.sync('"()!()"', {})


def test_auto_connect(q):
    q.disconnect()
    q.sync(".z.p")
    q.connect()


def test_io_error():
    q = Q("DUMMY", 1800)
    with pytest.raises(
        KolaIOError,
        match="failed to lookup address information: Name or service not known",
    ):
        q.sync("1+`a")


def test_asyn(q):
    q.asyn(".kola.x:18")
    assert 18 == q.sync(".kola.x")


@pytest.mark.parametrize(
    "query,expect",
    [
        (
            "([]sym:`a`b`c;prices:3 3#til 9)",
            pl.DataFrame(
                [
                    pl.Series("sym", ["a", "b", "c"], pl.Categorical),
                    pl.Series(
                        "prices", [[0, 1, 2], [3, 4, 5], [6, 7, 8]], pl.List(int)
                    ),
                ]
            ),
        ),
        (
            'enlist `float`long`char`string!(9.0;9;"c";`string)',
            pl.DataFrame(
                [
                    pl.Series("float", [9.0], pl.Float64),
                    pl.Series("long", [9], pl.Int64),
                    pl.Series("char", ["c"], pl.Utf8),
                    pl.Series("string", ["string"], pl.Categorical),
                ]
            ),
        ),
        (
            'enlist `float`long`char`string!(0n;0N;" ";"")',
            pl.DataFrame(
                [
                    pl.Series("float", [None], pl.Float64),
                    pl.Series("long", [None], pl.Int64),
                    pl.Series("char", [" "], pl.Utf8),
                    pl.Series("string", [""]),
                ]
            ),
        ),
        (
            "enlist `sym`timestamp`bool!(`sym;2021.06.03D;1b)",
            pl.DataFrame(
                [
                    pl.Series("sym", ["sym"], pl.Categorical),
                    pl.Series("timestamp", [1622678400000000000], pl.Datetime("ns")),
                    pl.Series("bool", [True], pl.Boolean),
                ]
            ),
        ),
        (
            "enlist `sym`timestamp`bool!(`;0Np;0b)",
            pl.DataFrame(
                [
                    pl.Series("sym", [""], pl.Categorical),
                    pl.Series("timestamp", [None], pl.Datetime("ns")),
                    pl.Series("bool", [False], pl.Boolean),
                ]
            ),
        ),
        (
            "0#enlist `sym`timestamp`bool!(`sym;2022.06.05D;1b)",
            pl.DataFrame(
                [
                    pl.Series("sym", [], pl.Categorical),
                    pl.Series("timestamp", [], pl.Datetime("ns")),
                    pl.Series("bool", [], pl.Boolean),
                ]
            ),
        ),
    ],
)
def test_read_table(q, query, expect: pl.DataFrame):
    actual = q.sync(query)
    assert str(actual) == str(expect)


@pytest.mark.parametrize(
    "k_atom,py_atom",
    [
        ("0b", False),
        ("1b", True),
        # long
        ("0N", -9223372036854775808),
        ("-0W", -9223372036854775807),
        ("9", 9),
        ("0W", 9223372036854775807),
        # float
        ("0n", math.nan),
        ("-0w", -math.inf),
        ("9.0", 9.0),
        ("0w", math.inf),
        # char
        ('enlist " "', b" "),
        ('"JS"', b"JS"),
        # symbol
        ("`", ""),
        ("`q", "q"),
        ("`kdb", "kdb"),
        # timestamp
        ("1970.01.01D0", datetime(1970, 1, 1, 0, 0, tzinfo=timezone.utc)),
        ("2023.11.11D0", datetime(2023, 11, 11, 0, 0, tzinfo=timezone.utc)),
        (
            "2023.11.11D10:02:00.979147",
            datetime(2023, 11, 11, 10, 2, 0, 979147, tzinfo=timezone.utc),
        ),
        # date
        ("0001.01.01", date(1, 1, 1)),
        ("2022.05.30", date(2022, 5, 30)),
        # timespan
        ("0D00", timedelta(seconds=0)),
        ("0D12:34:56.123456", timedelta(seconds=45296, microseconds=123456)),
        # time
        ("00:00:00.000", time(0, 0)),
        ("12:34:56.789", time(12, 34, 56, 789000)),
    ],
)
def test_write_atom(q, k_atom, py_atom):
    query = "{{x~{}}}".format(k_atom)
    assert q.sync(query, py_atom)


@pytest.mark.parametrize(
    "k_list,py_list",
    [
        # bool
        ("10b", pl.Series("", [True, False], pl.Boolean)),
        ("(,)0b", pl.Series("", [False], pl.Boolean)),
        # byte
        ("0x00FF", pl.Series("", [0, 255], pl.UInt8)),
        # short
        ("0N -0W 9 0Wh", pl.Series("", [None, -32767, 9, 32767], pl.Int16)),
        # int
        ("0N -0W 9 0Wi", pl.Series("", [None, -2147483647, 9, 2147483647], pl.Int32)),
        # long
        (
            "0N -0W 9 0W",
            pl.Series(
                "", [None, -9223372036854775807, 9, 9223372036854775807], pl.Int64
            ),
        ),
        # real
        (
            "0n -0w 9 0We",
            pl.Series("", [math.nan, -math.inf, 9.0, math.inf], pl.Float32),
        ),
        # float
        (
            "0n -0w 9 0W",
            pl.Series("", [math.nan, -math.inf, 9.0, math.inf], pl.Float64),
        ),
        # string
        ('("";"string")', pl.Series("", ["", "string"], pl.Utf8)),
        # symbol
        ("``q`kdb", pl.Series("", ["", "q", "kdb"], pl.Categorical)),
        # timestamp
        (
            "0N 2021.06.03D0 2021.06.03D12:34:56.123456789p",
            pl.Series(
                "timestamp",
                [None, 1622678400000000000, 1622723696123456789],
                pl.Datetime("ns"),
            ),
        ),
        # date
        ("0N 2022.05.30d", pl.Series("date", [None, 19142], pl.Date)),
        # timespan
        (
            "0N 0D00 0D12:34:56.123456789n",
            pl.Series("timespan", [None, 0, 45296123456789], pl.Duration("ns")),
        ),
        # time
        (
            "0n 00:00:00.000 12:34:56.789t",
            pl.Series("time", [None, 0, 45296789000000], pl.Time),
        ),
    ],
)
def test_write_list(q, k_list, py_list):
    query = "{{x~{}}}".format(k_list)
    assert q.sync(query, py_list)


@pytest.mark.parametrize(
    "q_table,df",
    [
        # (
        #     "([]sym:`a`b`c;prices:3 3#til 9)",
        #     pl.DataFrame(
        #         [
        #             pl.Series("sym", ["a", "b", "c"], pl.Categorical),
        #             pl.Series("prices", [[0, 1, 2], [3, 4, 5], [6, 7, 8]], pl.List(int)),
        #         ]
        #     ),
        # ),
        # (
        #     '([]sym:enlist each "abc";prices:3 3#til 9)',
        #     pl.DataFrame(
        #         [
        #             pl.Series("sym", ["a", "b", "c"]),
        #             pl.Series("prices", [[0, 1, 2], [3, 4, 5], [6, 7, 8]], pl.List(int)),
        #         ]
        #     ),
        # ),
        (
            'enlist `float`long`char`string!(9.0;9;(,)"c";"string")',
            pl.DataFrame(
                [
                    pl.Series("float", [9.0], pl.Float64),
                    pl.Series("long", [9], pl.Int64),
                    pl.Series("char", ["c"], pl.Utf8),
                    pl.Series("string", ["string"]),
                ]
            ),
        ),
        (
            'enlist `float`long`char`string!(0n;0N;(,)" ";"")',
            pl.DataFrame(
                [
                    pl.Series("float", [math.nan], pl.Float64),
                    pl.Series("long", [None], pl.Int64),
                    pl.Series("char", [" "], pl.Utf8),
                    pl.Series("string", [""]),
                ]
            ),
        ),
        (
            "enlist `sym`timestamp`bool!(`sym;2021.06.03D;1b)",
            pl.DataFrame(
                [
                    pl.Series("sym", ["sym"], pl.Categorical),
                    pl.Series("timestamp", [1622678400000000000], pl.Datetime("ns")),
                    pl.Series("bool", [True], pl.Boolean),
                ]
            ),
        ),
        (
            "enlist `sym`timestamp`bool!(`;0Np;0b)",
            pl.DataFrame(
                [
                    pl.Series("sym", [""], pl.Categorical),
                    pl.Series("timestamp", [None], pl.Datetime("ns")),
                    pl.Series("bool", [False], pl.Boolean),
                ]
            ),
        ),
        (
            'flip `sym`timestamp`bool!"SPB"$\\:()',
            pl.DataFrame(
                [
                    pl.Series("sym", [], pl.Categorical),
                    pl.Series("timestamp", [], pl.Datetime("ns")),
                    pl.Series("bool", [], pl.Boolean),
                ]
            ),
        ),
    ],
)
def test_write_table(q, q_table, df: pl.DataFrame):
    query = "{{x~{}}}".format(q_table)
    assert q.sync(query, df)


@pytest.mark.parametrize(
    "k_dict,py_dict",
    [
        (
            "`startTime`endTime!09:00:00.000 11:30:00.000",
            {"startTime": time(9), "endTime": time(11, 30)},
        ),
        (
            "`sym`date!(`7203.T`2226.T;2023.11.18)",
            {
                "sym": pl.Series("", ["7203.T", "2226.T"], pl.Categorical),
                "date": date(2023, 11, 18),
            },
        ),
    ],
)
def test_write_dict(q, k_dict, py_dict):
    query = "{{x~{}}}".format(k_dict)
    assert q.sync(query, py_dict)
