# kola

a Python [Polars](https://pola-rs.github.io/polars/) Interface to `j*` and `q`

## Basic Data Type Map

### j\*

#### Deserialization

##### Atom

| j type      | n   | size | python type | note                        |
| ----------- | --- | ---- | ----------- | --------------------------- |
| `boolean`   | 1   | 1    | `bool`      |                             |
| `u8`        | 4   | 1    | `int`       |                             |
| `i16`       | 5   | 2    | `int`       |                             |
| `i32`       | 6   | 4    | `int`       |                             |
| `i64`       | 7   | 8    | `int`       |                             |
| `f32`       | 8   | 4    | `float`     |                             |
| `f64`       | 9   | 8    | `float`     |                             |
| `string`    | 10  | 1    | `str`       |                             |
| `symbol`    | 11  | \*   | `str`       |                             |
| `timestamp` | 12  | 8    | `datetime`  |                             |
| `date`      | 14  | 4    | `date`      | 0001.01.01 - 9999.12.31     |
| `datetime`  | 15  | 8    | `datetime`  |                             |
| `duration`  | 16  | 8    | `timedelta` |                             |
| `time`      | 19  | 4    | `time`      | 00:00:00.000 - 23:59:59.999 |

##### Composite Data Type

| k type       | n    | size | python type    |
| ------------ | ---- | ---- | -------------- |
| `series`     | 1-15 | -    | `pl.Series`    |
| `list`       | 90   | -    | `Tuple`        |
| `dictionary` | 91   | \*   | `dict`         |
| `dataframe`  | 92   | \*   | `pl.DataFrame` |

#### Serialization

##### Basic Data Type

| python type | j type     | note                        |
| ----------- | ---------- | --------------------------- |
| `bool`      | `boolean`  |                             |
| `int`       | `i64`      |                             |
| `float`     | `f64`      |                             |
| `str`       | `symbol`   |                             |
| `bytes`     | `string`   |                             |
| `date`      | `date`     | 0001.01.01 - 9999.12.31     |
| `datetime`  | `datetime` |                             |
| `timedelta` | `duration` |                             |
| `time`      | `time`     | 00:00:00.000 - 23:59:59.999 |

##### Dictionary, Series and DataFrame

| python type    | j type    |
| -------------- | --------- |
| `dict`         | dict      |
| `pl.Series`    | series    |
| `pl.DataFrame` | dataframe |

> for dictionary, requires `string` as keys.

### q

#### Deserialization

##### Atom

| k type      | n   | size | python type | note                        |
| ----------- | --- | ---- | ----------- | --------------------------- |
| `boolean`   | 1   | 1    | `bool`      |                             |
| `guid`      | 2   | 16   | `str`       |                             |
| `byte`      | 4   | 1    | `int`       |                             |
| `short`     | 5   | 2    | `int`       |                             |
| `int`       | 6   | 4    | `int`       |                             |
| `long`      | 7   | 8    | `int`       |                             |
| `real`      | 8   | 4    | `float`     |                             |
| `float`     | 9   | 8    | `float`     |                             |
| `char`      | 10  | 1    | `str`       |                             |
| `string`    | 10  | 1    | `str`       |                             |
| `symbol`    | 11  | \*   | `str`       |                             |
| `timestamp` | 12  | 8    | `datetime`  |                             |
| `month`     | 13  | 4    | `-`         |                             |
| `date`      | 14  | 4    | `date`      | 0001.01.01 - 9999.12.31     |
| `datetime`  | 15  | 8    | `datetime`  |                             |
| `timespan`  | 16  | 8    | `timedelta` |                             |
| `minute`    | 17  | 4    | `time`      | 00:00 - 23:59               |
| `second`    | 18  | 4    | `time`      | 00:00:00 - 23:59:59         |
| `time`      | 19  | 4    | `time`      | 00:00:00.000 - 23:59:59.999 |

##### Composite Data Type

| k type           | n   | size | python type              |
| ---------------- | --- | ---- | ------------------------ |
| `boolean list`   | 1   | 1    | `pl.Boolean`             |
| `guid list`      | 2   | 16   | `pl.List(pl.Binary(16))` |
| `byte list`      | 4   | 1    | `pl.Uint8`               |
| `short list`     | 5   | 2    | `pl.Int16`               |
| `int list`       | 6   | 4    | `pl.Int32`               |
| `long list`      | 7   | 8    | `pl.Int64`               |
| `real list`      | 8   | 4    | `pl.Float32`             |
| `float list`     | 9   | 8    | `pl.Float64`             |
| `char list`      | 10  | 1    | `pl.Utf8`                |
| `string list`    | 10  | 1    | `pl.Utf8`                |
| `symbol list`    | 11  | \*   | `pl.Categorical`         |
| `timestamp list` | 12  | 8    | `pl.Datetime`            |
| `month list`     | 13  | 4    | `-`                      |
| `date list`      | 14  | 4    | `pl.Date`                |
| `datetime list`  | 15  | 8    | `pl.Datetime`            |
| `timespan list`  | 16  | 8    | `pl.Duration`            |
| `minute list`    | 17  | 4    | `pl.Time`                |
| `second list`    | 18  | 4    | `pl.Time`                |
| `time list`      | 19  | 4    | `pl.Time`                |
| `table`          | 98  | \*   | `pl.DataFrame`           |
| `dictionary`     | 99  | \*   | `-`                      |
| `keyed table`    | 99  | \*   | `pl.DataFrame`           |

> performance is impacted by converting guid to string, deserialize the uuid to 16 fixed binary list, use .hex() to convert binary to string if required

> real/float 0n is mapped to Polars null not NaN

> short/int/long 0Nh/i/j, 0Wh/i/j and -0Wh/i/j are mapped to null

```
df.with_columns([
    (pl.col("uuid").apply(lambda u: u.hex()))
    ])
```

#### Serialization

##### Basic Data Type

| python type | k type      | note                        |
| ----------- | ----------- | --------------------------- |
| `bool`      | `boolean`   |                             |
| `int`       | `long`      |                             |
| `float`     | `float`     |                             |
| `str`       | `symbol`    |                             |
| `bytes`     | `string`    |                             |
| `datetime`  | `timestamp` |                             |
| `date`      | `date`      | 0001.01.01 - 9999.12.31     |
| `datetime`  | `datetime`  |                             |
| `timedelta` | `timespan`  |                             |
| `time`      | `time`      | 00:00:00.000 - 23:59:59.999 |

##### Dictionary, Series and DataFrame

| python type              | k type    |
| ------------------------ | --------- |
| `dict`                   | dict      |
| `pl.Boolean`             | boolean   |
| `pl.List(pl.Binary(16))` | guid      |
| `pl.Uint8`               | byte      |
| `pl.Int16`               | short     |
| `pl.Int32`               | int       |
| `pl.Int64`               | long      |
| `pl.Float32`             | real      |
| `pl.Float64`             | float     |
| `pl.Utf8`                | char      |
| `pl.Categorical`         | symbol    |
| `pl.Datetime`            | timestamp |
| `pl.Date`                | date      |
| `pl.Datetime`            | datetime  |
| `pl.Duration`            | timespan  |
| `pl.Time`                | time      |
| `pl.DataFrame`           | table     |

> Limited Support for dictionary as arguments, requires `string` as keys.

## Quick Start

### Create a Connection

```python
import polars as pl
import kola
# Connect to j*, J and Q both work with j*
conn = kola.J('localhost', 1800)

# Connect to q
conn = kola.Q('localhost', 1800)

# with retries for IO Errors, 1s, 2s, 4s ...
conn = kola.J('localhost', 1800, retries=3)

# with read timeout error, 2s, "Resource temporarily unavailable"
conn = kola.J('localhost', 1800, retries=3, timeout=2)
```

### Connect(Optional)

Automatically connect when querying q process

```python
conn.connect()
```

### Disconnect

Automatically disconnect if any IO error

```python
conn.disconnect()
```

### String Query

```python
conn.sync("select from trade where date=last date")
```

### Functional Query

For functional query, `kola` supports Python [Basic Data Type](#basic-data-type), `pl.Series`, `pl.DataFrame` and Python Dictionary with string keys and Python [Basic Data Type](#basic-data-type) and `pl.Series` values.

```python
from datetime import date, time

conn.sync(
    ".gw.query",
    "table",
    {
        "date": date(2023, 11, 21),
        "syms": pl.Series("", ["sym0", "sym1"], pl.Categorical),
        # 09:00
        "startTime": time(9),
        # 11:30
        "endTime": time(11, 30),
    },
)
```

### Send DataFrame

```python
# pl_df is a Polars DataFrame
conn.sync("upsert", "table", pl_df)
```

```python
# pd_df is a Pandas DataFrame, use pl.DateFrame to cast Pandas DataFrame
conn.sync("upsert", "table", pl.DataFrame(pd_df))
```

### Async Query

```python
# pl_df is a Polars DataFrame
conn.asyn("upsert", "table", pl_df)
```

### Subscribe

```python
from kola import QType

conn.sync(".u.sub", pl.Series("", ["table1", "table2"], QType.Symbol), "")

# specify symbol filter
conn.sync(
    ".u.sub",
    pl.Series("", ["table1", "table2"], QType.Symbol),
    pl.Series("", ["sym1", "sym2"], QType.Symbol),
)

while true:
    # ("upd", "table", pl.Dataframe)
    upd = conn.receive()
    print(upd)
```

### Generate IPC for q

```python
import polars as pl
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

## Polars Documentations

Refer to

- [User Guide](https://pola-rs.github.io/polars/user-guide/)
- [API Reference](https://pola-rs.github.io/polars/py-polars/html/reference/index.html)
