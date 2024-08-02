import polars as pl

Boolean = pl.Boolean
Guid = pl.Array(pl.Binary, 16)
Byte = pl.UInt8
Short = pl.Int16
Int = pl.Int32
Long = pl.Int64
Real = pl.Float32
Float = pl.Float64
Char = pl.UInt8
String = pl.Utf8
Symbol = pl.Categorical
Timestamp = pl.Datetime("ns")
Date = pl.Date
Datetime = pl.Datetime("ms")
Timespan = pl.Duration("ns")
Time = pl.Time


__all__ = [
    Boolean,
    Guid,
    Byte,
    Short,
    Int,
    Long,
    Real,
    Float,
    Char,
    String,
    Symbol,
    Timestamp,
    Date,
    Datetime,
    Timespan,
    Time,
]
