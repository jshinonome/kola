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

### Operators and Lambdas

Pass q primitives and arbitrary lambdas as first-class arguments:

```python
from kola import KolaQLambda, KolaQOperator

conn.sync("{[op;a;b] .[op;(a;b)]}", KolaQOperator.PLUS, 1, 2)
conn.sync("{[op;a;b] .[op;(a;b)]}", KolaQLambda("{x+y}"), 1, 2)

# A non-root q context can be supplied explicitly.
scoped = KolaQLambda("{x+y}", "analytics")
```

`KolaQOperator(name)` accepts supported q primitive names such as `"+"`; it does not expose wire opcodes. `KolaQLambda(source, context="")` preserves its source text, requires a brace-delimited UTF-8 body (optionally prefixed with `k)`), rejects NUL bytes in both fields, and rejects context values beginning with `"."`. The context `"analytics"` represents q namespace `.analytics` because the wire context omits the leading dot. Lambda source is executable q code: construct it only from trusted input.

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
| `primitive` | 101-103 | 1   | `KolaQOperator` | supported unary/binary/ternary primitive |
| `lambda`    | 100 | \*   | `KolaQLambda` | source and q context |

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
| `KolaQOperator` | primitive | supported primitive name |
| `KolaQLambda` | lambda | source and q context |

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
