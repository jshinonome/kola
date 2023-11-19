# kola

a Python [Polars](https://pola-rs.github.io/polars/) Interface to kdb+/q

## Basic Data Type Map

### Deserialization

#### Atom

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

#### Composite Data Type

| k type        | n   | size | python type              |
| ------------- | --- | ---- | ------------------------ |
| `boolean`     | 1   | 1    | `pl.Boolean`             |
| `guid`        | 2   | 16   | `pl.List(pl.Binary(16))` |
| `byte`        | 4   | 1    | `pl.Uint8`               |
| `short`       | 5   | 2    | `pl.Int16`               |
| `int`         | 6   | 4    | `pl.Int32`               |
| `long`        | 7   | 8    | `pl.Int64`               |
| `real`        | 8   | 4    | `pl.Float32`             |
| `float`       | 9   | 8    | `pl.Float64`             |
| `char`        | 10  | 1    | `pl.Utf8`                |
| `string`      | 10  | 1    | `pl.Utf8`                |
| `symbol`      | 11  | \*   | `pl.Categorical`         |
| `timestamp`   | 12  | 8    | `pl.Datetime`            |
| `month`       | 13  | 4    | `-`                      |
| `date`        | 14  | 4    | `pl.Date`                |
| `datetime`    | 15  | 8    | `pl.Datetime`            |
| `timespan`    | 16  | 8    | `pl.Duration`            |
| `minute`      | 17  | 4    | `pl.Time`                |
| `second`      | 18  | 4    | `pl.Time`                |
| `time`        | 19  | 4    | `pl.Time`                |
| `table`       | 98  | \*   | `pl.DataFrame`           |
| `dictionary`  | 99  | \*   | `-`                      |
| `keyed` table | 99  | \*   | `pl.DataFrame`           |

> performance is impacted by converting guid to string, deserialize the uuid to 16 fixed binary list, use .hex() to convert binary to string if required

> real/float 0n is mapped to polars null not NaN

```
df.with_columns([
    (pl.col("uuid").apply(lambda u: u.hex()))
    ])
```

### Serialization

#### Atom

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

#### Composite Data Type

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

> Limited Support for dictionary as arguments, python `string` as keys and python `Atom` and `pl.Series` as values

### Quick Start

```python
import polars as pl
from datetime import datetime
from kola import Q
q = Q('localhost', 1800)
q.connect()
q.sync(".z.D")
```
