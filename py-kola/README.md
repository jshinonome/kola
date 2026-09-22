# kola

[![PyPI](https://img.shields.io/pypi/v/kola)](https://pypi.org/project/kola/)
[![Python](https://img.shields.io/pypi/pyversions/kola)](https://pypi.org/project/kola/)

A Python [Polars](https://pola-rs.github.io/polars/) interface to kdb+/q, powered by Rust.

## Installation

```bash
pip install kola
```

**Requirements**: Python ≥ 3.10, Polars ≥ 1.31.0, PyArrow ≥ 20.0.0

## Quick Start

### Create a Connection

```python
import polars as pl
import kola

# basic connection
conn = kola.Q('localhost', 1800)

# with authentication
conn = kola.Q('localhost', 1800, user='user', passwd='password')

# with TLS and retry
conn = kola.Q('localhost', 1800, enable_tls=True, retries=3, timeout=30)
```

**Parameters**:

| Parameter    | Type   | Default | Description                                        |
| ------------ | ------ | ------- | -------------------------------------------------- |
| `host`       | `str`  |         | Hostname of the q process                          |
| `port`       | `int`  |         | Port of the q process                              |
| `user`       | `str`  | `""`    | Username (defaults to OS login user)               |
| `passwd`     | `str`  | `""`    | Password                                           |
| `enable_tls` | `bool` | `False` | Enable TLS encryption                              |
| `retries`    | `int`  | `0`     | Number of retries with exponential backoff          |
| `timeout`    | `int`  | `0`     | Connection timeout in seconds (0 = no timeout)     |

### Connect / Disconnect

```python
# explicitly connect (auto-connects on first query)
conn.connect()

# disconnect (auto-disconnects on IO error)
conn.disconnect()
```

### String Query

```python
conn.sync("select from trade where date=last date")
```

### Functional Query

Supports Python [basic data types](#basic-data-type), `pl.Series`, `pl.DataFrame`, and `dict` (with string keys).

```python
from datetime import date, time

conn.sync(
    ".gw.query",
    "table",
    {
        "date": date(2023, 11, 21),
        "syms": pl.Series("", ["sym0", "sym1"], kola.QType.Symbol),
        "startTime": time(9),
        "endTime": time(11, 30),
    },
)
```

### Send DataFrame

```python
# Polars DataFrame
conn.sync("upsert", "table", pl_df)

# Pandas DataFrame (cast to Polars first)
conn.sync("upsert", "table", pl.DataFrame(pd_df))
```

### Async Query

```python
conn.asyn("upsert", "table", pl_df)
```

### Subscribe

```python
from kola import QType

conn.sync(".u.sub", pl.Series("", ["table1", "table2"], QType.Symbol), "")

# with symbol filter
conn.sync(
    ".u.sub",
    pl.Series("", ["table1", "table2"], QType.Symbol),
    pl.Series("", ["sym1", "sym2"], QType.Symbol),
)

while True:
    # returns ("upd", "table", pl.DataFrame)
    upd = conn.receive()
    print(upd)
```

### Generate IPC Bytes

Serialize data as kdb+ IPC bytes without a connection.

```python
from kola import serialize_as_ipc_bytes6

df = pl.DataFrame(
    {
        "sym": pl.Series("sym", ["a", "b", "c"], pl.Categorical),
        "price": [1, 2, 3],
    }
)

# without compression
buffer = serialize_as_ipc_bytes6("sync", False, ["upd", "table", df])

# with compression
buffer = serialize_as_ipc_bytes6("sync", True, ["upd", "table", df])
```

**`msg_type`**: `"async"` | `"sync"` | `"response"`

Use the `Operator` enum to serialize K101 unary primitives and K102 operators:

```python
from kola import Operator, serialize_as_ipc_bytes6

buffer = serialize_as_ipc_bytes6("sync", False, Operator.SUM)
assert buffer[8:] == bytes([101, 25])
```

Operators also work as query arguments and inside lists and dictionaries.
Members include `Operator.SUM`, `Operator.AVG`, `Operator.PLUS` (unary `+:`),
and `Operator.PROJECTION_NULL` (`::`). The `.value` attribute holds the q name;
`Operator("sum")` also resolves to `Operator.SUM`. Unknown names raise `ValueError`.
Ordinary strings serialize as symbols; `None` serializes as generic null.
Received K101 and K102 operators are returned as `Operator` enum members.

K102 members include `Operator.BINARY_PLUS` (`+`), `Operator.IN`, `Operator.WAVG`,
and `Operator.DIV`. For example:

```python
buffer = serialize_as_ipc_bytes6("sync", False, Operator.BINARY_PLUS)
assert buffer[8:] == bytes([102, 1])
```

Named unary aliases include `Operator.FLIP`, `Operator.NEG`, `Operator.FIRST`,
`Operator.COUNT`, `Operator.TYPE`, and `Operator.VALUE`. They retain the existing
symbolic values: `Operator.FLIP is Operator.PLUS`, and both encode as `[101, 1]`.
The mappings follow jkdb's K101/K102 lists; named unary aliases were checked
against q 3.6. This does not claim exhaustive coverage across q versions.

The 177 keywords on the [KX reference card](https://code.kx.com/q/ref/) were
checked against q 3.6 on 2026-09-22. The per-keyword runtime type and serialized
type are recorded in [test/q_keyword_types.json](test/q_keyword_types.json).
Run `python scripts/check_q_keywords.py` from the repository root to repeat the
check with your installed q version. The script resolves and serializes values
without applying the functions.

Of those keywords, 58 serialize directly as K101/K102 values. The others include
lambdas, projections, compositions, derived functions, the `csv` character,
and seven syntax keywords that cannot be resolved as standalone values.
`Operator.LSQ` and `Operator.MMU` alias K102 `!` and `$`. Use `BINARY_AND` and
`BINARY_OR` for the q keywords `and` and `or`; the original `AND` and `OR` enum
members retain their unary `&:` and `|:` meanings.

In q 3.6, `hopen`, `var`, `dev`, `cov`, and `cor` have primitive runtime types
but serialize as compatibility lambdas. Their current enum encodings use the
direct jkdb primitive codes and do not reproduce those lambda encodings.
`<>`, `<=`, and `>=` serialize as K105 compositions and are not yet supported.

### Read Binary Table

Read a kdb+ binary table (splayed/flat file) directly into a Polars DataFrame.

```python
from kola import read_binary6

df = read_binary6("/path/to/binary/table")
```

## Error Handling

```python
from kola import KolaError, KolaIOError, KolaAuthError

try:
    conn.sync("select from trade")
except KolaAuthError:
    print("Authentication failed")
except KolaIOError:
    print("Connection error")
except KolaError:
    print("General kola error")
```

## QType

`kola.QType` provides Polars dtype aliases for q types, useful when constructing `pl.Series` for functional queries.

| QType       | Polars dtype          |
| ----------- | --------------------- |
| `Boolean`   | `pl.Boolean`          |
| `Guid`      | `pl.Array(pl.Binary, 16)` |
| `Byte`      | `pl.UInt8`            |
| `Short`     | `pl.Int16`            |
| `Int`       | `pl.Int32`            |
| `Long`      | `pl.Int64`            |
| `Real`      | `pl.Float32`          |
| `Float`     | `pl.Float64`          |
| `Char`      | `pl.UInt8`            |
| `String`    | `pl.Utf8`             |
| `Symbol`    | `pl.Categorical`      |
| `Timestamp` | `pl.Datetime("ns")`   |
| `Date`      | `pl.Date`             |
| `Datetime`  | `pl.Datetime("ms")`   |
| `Timespan`  | `pl.Duration("ns")`   |
| `Time`      | `pl.Time`             |

## Data Type Mapping

### Deserialization (q → Python)

#### Atom

| q type      | n   | size | Python type  | Note                        |
| ----------- | --- | ---- | ------------ | --------------------------- |
| `boolean`   | 1   | 1    | `bool`       |                             |
| `guid`      | 2   | 16   | `str`        |                             |
| `byte`      | 4   | 1    | `int`        |                             |
| `short`     | 5   | 2    | `int`        |                             |
| `int`       | 6   | 4    | `int`        |                             |
| `long`      | 7   | 8    | `int`        |                             |
| `real`      | 8   | 4    | `float`      |                             |
| `float`     | 9   | 8    | `float`      |                             |
| `char`      | 10  | 1    | `str`        |                             |
| `string`    | 10  | 1    | `str`        |                             |
| `symbol`    | 11  | \*   | `str`        |                             |
| `timestamp` | 12  | 8    | `datetime`   |                             |
| `month`     | 13  | 4    | `-`          |                             |
| `date`      | 14  | 4    | `date`       | 0001.01.01 - 9999.12.31     |
| `datetime`  | 15  | 8    | `datetime`   |                             |
| `timespan`  | 16  | 8    | `timedelta`  |                             |
| `minute`    | 17  | 4    | `time`       | 00:00 - 23:59               |
| `second`    | 18  | 4    | `time`       | 00:00:00 - 23:59:59         |
| `time`      | 19  | 4    | `time`       | 00:00:00.000 - 23:59:59.999 |

#### List / Table

| q type           | n   | size | Polars dtype               |
| ---------------- | --- | ---- | -------------------------- |
| `boolean list`   | 1   | 1    | `pl.Boolean`               |
| `guid list`      | 2   | 16   | `pl.Array(pl.Binary, 16)`  |
| `byte list`      | 4   | 1    | `pl.UInt8`                 |
| `short list`     | 5   | 2    | `pl.Int16`                 |
| `int list`       | 6   | 4    | `pl.Int32`                 |
| `long list`      | 7   | 8    | `pl.Int64`                 |
| `real list`      | 8   | 4    | `pl.Float32`               |
| `float list`     | 9   | 8    | `pl.Float64`               |
| `char list`      | 10  | 1    | `pl.Utf8`                  |
| `string list`    | 10  | 1    | `pl.Utf8`                  |
| `symbol list`    | 11  | \*   | `pl.Categorical`           |
| `timestamp list` | 12  | 8    | `pl.Datetime("ns")`        |
| `month list`     | 13  | 4    | `-`                        |
| `date list`      | 14  | 4    | `pl.Date`                  |
| `datetime list`  | 15  | 8    | `pl.Datetime("ms")`        |
| `timespan list`  | 16  | 8    | `pl.Duration("ns")`        |
| `minute list`    | 17  | 4    | `pl.Time`                  |
| `second list`    | 18  | 4    | `pl.Time`                  |
| `time list`      | 19  | 4    | `pl.Time`                  |
| `table`          | 98  | \*   | `pl.DataFrame`             |
| `dictionary`     | 99  | \*   | `-`                        |
| `keyed table`    | 99  | \*   | `pl.DataFrame`             |

> Guid is deserialized as a 16-byte fixed binary array. Use `.hex()` to convert to string if needed:
>
> ```python
> df.with_columns(pl.col("uuid").apply(lambda u: u.hex()))
> ```

> `real`/`float` `0n` is mapped to Polars `null`, not `NaN`.

> `short`/`int`/`long` null and infinity values (`0Nh/i/j`, `0Wh/i/j`, `-0Wh/i/j`) are mapped to `null`.

### Serialization (Python → q)

#### Basic Data Type

| Python type  | q type      | Note                        |
| ------------ | ----------- | --------------------------- |
| `bool`       | `boolean`   |                             |
| `int`        | `long`      |                             |
| `float`      | `float`     |                             |
| `str`        | `symbol`    |                             |
| `bytes`      | `string`    |                             |
| `datetime`   | `timestamp` |                             |
| `date`       | `date`      | 0001.01.01 - 9999.12.31     |
| `timedelta`  | `timespan`  |                             |
| `time`       | `time`      | 00:00:00.000 - 23:59:59.999 |

#### Series, DataFrame, and Dictionary

| Polars dtype               | q type    |
| -------------------------- | --------- |
| `dict`                     | dict      |
| `pl.Boolean`               | boolean   |
| `pl.Array(pl.Binary, 16)`  | guid      |
| `pl.UInt8`                 | byte      |
| `pl.Int16`                 | short     |
| `pl.Int32`                 | int       |
| `pl.Int64`                 | long      |
| `pl.Float32`               | real      |
| `pl.Float64`               | float     |
| `pl.Utf8`                  | char      |
| `pl.Categorical`           | symbol    |
| `pl.Datetime`              | timestamp |
| `pl.Date`                  | date      |
| `pl.Duration`              | timespan  |
| `pl.Time`                  | time      |
| `pl.DataFrame`             | table     |

> Dictionary serialization requires `str` keys.

## Polars Documentation

- [User Guide](https://docs.pola.rs/)
- [API Reference](https://docs.pola.rs/api/python/stable/reference/index.html)
