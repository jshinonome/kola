use chrono::{DateTime, Duration, NaiveDate, NaiveTime, Timelike, Utc};
use indexmap::IndexMap;
use polars::{
    datatypes::DataType as PolarsDataType,
    prelude::{AnyValue, DataFrame, LargeListArray, TimeUnit},
    series::Series,
};
use uuid::Uuid;

use crate::errors::KolaError;

pub const K_TYPE_SIZE: [usize; 20] = [0, 1, 16, 0, 1, 2, 4, 8, 4, 8, 1, 0, 8, 4, 4, 8, 8, 4, 4, 4];
pub const MIN_Q_TIMESTAMP_UNIX_NANOS: i64 = i64::MIN + 946_684_800_000_000_000 + 2;
pub(crate) const MAX_VALUE_DEPTH: usize = 64;

#[repr(u8)]
pub enum MsgType {
    Async = 0,
    Sync = 1,
    Response = 2,
}

pub(crate) const Q_UNARY_PRIMITIVES: [&str; 42] = [
    "", "+:", "-:", "*:", "%:", "&:", "|:", "^:", "=:", "<:", ">:", "$:", ",:", "#:", "_:", "~:",
    "!:", "?:", "@:", ".:", "0::", "1::", "2::", "avg", "last", "sum", "prd", "min", "max", "exit",
    "getenv", "abs", "sqrt", "log", "exp", "sin", "asin", "cos", "acos", "tan", "atan", "enlist",
];
pub(crate) const Q_BINARY_PRIMITIVES: [&str; 34] = [
    ":", "+", "-", "*", "%", "&", "|", "^", "=", "<", ">", "$", ",", "#", "_", "~", "!", "?", "@",
    ".", "0:", "1:", "2:", "in", "within", "like", "bin", "ss", "insert", "wsum", "wavg", "div",
    "xexp", "setenv",
];
pub(crate) const Q_TERNARY_PRIMITIVES: [&str; 3] = ["'", "/", "\\"];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QOperator {
    name: &'static str,
    k_type: u8,
    opcode: u8,
}

impl QOperator {
    pub const PLUS: Self = Self {
        name: "+",
        k_type: 102,
        opcode: 1,
    };

    pub fn new(name: &str) -> Result<Self, KolaError> {
        if name.as_bytes().contains(&0) {
            return Err(KolaError::NotAbleToSerializeErr(
                "q primitive operator names cannot contain NUL bytes".to_string(),
            ));
        }
        for (k_type, primitives) in [
            (101, Q_UNARY_PRIMITIVES.as_slice()),
            (102, Q_BINARY_PRIMITIVES.as_slice()),
            (103, Q_TERNARY_PRIMITIVES.as_slice()),
        ] {
            if let Some(opcode) = primitives.iter().position(|primitive| *primitive == name) {
                if !name.is_empty() {
                    return Ok(Self {
                        name: primitives[opcode],
                        k_type,
                        opcode: opcode as u8,
                    });
                }
            }
        }
        Err(KolaError::NotAbleToSerializeErr(format!(
            "unsupported q primitive operator name {name:?}"
        )))
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub(crate) fn from_wire(k_type: u8, opcode: u8) -> Option<Self> {
        let primitives = match k_type {
            101 => Q_UNARY_PRIMITIVES.as_slice(),
            102 => Q_BINARY_PRIMITIVES.as_slice(),
            103 => Q_TERNARY_PRIMITIVES.as_slice(),
            _ => return None,
        };
        let name = *primitives.get(opcode as usize)?;
        if name.is_empty() {
            return None;
        }
        Some(Self {
            name,
            k_type,
            opcode,
        })
    }

    pub(crate) fn wire_value(&self) -> (u8, u8) {
        (self.k_type, self.opcode)
    }
}

impl TryFrom<&str> for QOperator {
    type Error = KolaError;

    fn try_from(name: &str) -> Result<Self, Self::Error> {
        Self::new(name)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QLambda {
    source: String,
    context: String,
}

impl QLambda {
    pub fn new(source: impl Into<String>) -> Result<Self, KolaError> {
        Self::with_context(source, "")
    }

    pub fn with_context(
        source: impl Into<String>,
        context: impl Into<String>,
    ) -> Result<Self, KolaError> {
        let source = source.into();
        let context = context.into();
        if source.as_bytes().contains(&0) {
            return Err(KolaError::NotAbleToSerializeErr(
                "q lambda source cannot contain NUL bytes".to_string(),
            ));
        }
        if context.as_bytes().contains(&0) {
            return Err(KolaError::NotAbleToSerializeErr(
                "q lambda context cannot contain NUL bytes".to_string(),
            ));
        }
        if !context.is_empty() && context.starts_with('.') {
            return Err(KolaError::NotAbleToSerializeErr(
                "q lambda context omits the leading dot".to_string(),
            ));
        }
        let trimmed = source.trim();
        let lambda_source = trimmed.strip_prefix("k)").unwrap_or(trimmed);
        if !lambda_source.starts_with('{') || !lambda_source.ends_with('}') {
            return Err(KolaError::NotAbleToSerializeErr(
                "q lambda source must be brace-delimited".to_string(),
            ));
        }
        Ok(Self { source, context })
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn context(&self) -> &str {
        &self.context
    }

    pub fn into_parts(self) -> (String, String) {
        (self.source, self.context)
    }
}

#[derive(Debug, PartialEq)]
pub enum K {
    Boolean(bool),
    Guid(Uuid),
    U8(u8),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Char(u8),
    CharVector(Vec<u8>),
    Symbol(String),
    String(String),
    DateTime(DateTime<Utc>),   // datetime, timestamp
    Date(NaiveDate),           // date
    Time(NaiveTime),           // time, minute, second
    Duration(Duration),        // timespan
    MixedList(Vec<K>),         // mixed list
    Series(Series),            // list, dictionaries
    DataFrame(DataFrame),      // table and keyed table
    Dict(IndexMap<String, K>), // dict, symbols -> atom or list
    Operator(QOperator),
    Lambda(QLambda),
    Null,
}

impl K {
    pub fn j6_len(&self) -> Result<usize, KolaError> {
        self.j6_len_with_depth(0)
    }

    pub(crate) fn j6_len_with_depth(&self, depth: usize) -> Result<usize, KolaError> {
        if depth > MAX_VALUE_DEPTH {
            return Err(KolaError::NotAbleToSerializeErr(format!(
                "q value nesting exceeds {MAX_VALUE_DEPTH} levels"
            )));
        }

        // k type + value
        match self {
            K::Boolean(_) => Ok(2),
            K::Guid(_) => Ok(17),
            K::U8(_) => Ok(2),
            K::I16(_) => Ok(3),
            K::I32(_) => Ok(5),
            K::I64(_) => Ok(9),
            K::F32(_) => Ok(5),
            K::F64(_) => Ok(9),
            K::Char(_) => Ok(2),
            K::CharVector(k) => {
                checked_q_count(k.len())?;
                k.len().checked_add(6).ok_or(KolaError::OverLengthErr())
            }
            K::Symbol(k) => {
                validate_q_symbol(k)?;
                k.len().checked_add(2).ok_or(KolaError::OverLengthErr())
            }
            K::String(k) => {
                checked_q_count(k.len())?;
                k.len().checked_add(6).ok_or(KolaError::OverLengthErr())
            }
            K::DateTime(k) => {
                let unix_nanoseconds = k.timestamp_nanos_opt().ok_or_else(|| {
                    KolaError::NotAbleToSerializeErr(
                        "timestamp is outside q's representable nanosecond range".to_string(),
                    )
                })?;
                unix_nanoseconds
                    .checked_sub(946_684_800_000_000_000)
                    .filter(|value| *value > i64::MIN + 1 && *value < i64::MAX)
                    .ok_or_else(|| {
                        KolaError::NotAbleToSerializeErr(
                            "timestamp is outside q's representable nanosecond range".to_string(),
                        )
                    })?;
                Ok(9)
            }
            K::Date(_) => Ok(5),
            K::Time(k) => {
                if k.nanosecond() % 1_000_000 != 0 {
                    return Err(KolaError::NotAbleToSerializeErr(
                        "q time only supports millisecond precision".to_string(),
                    ));
                }
                Ok(5)
            }
            K::Duration(k) => {
                k.num_nanoseconds().ok_or_else(|| {
                    KolaError::NotAbleToSerializeErr(
                        "duration is outside q's representable nanosecond range".to_string(),
                    )
                })?;
                Ok(9)
            }
            K::MixedList(values) => {
                checked_q_count(values.len())?;
                values.iter().try_fold(6usize, |length, value| {
                    length
                        .checked_add(value.j6_len_with_depth(depth + 1)?)
                        .ok_or(KolaError::OverLengthErr())
                })
            }
            K::Series(series) => get_series_len(series),
            K::DataFrame(df) => {
                checked_q_count(df.width())?;
                // 98 0 99 + symbol list(6) + values(6)
                let mut length: usize = 15;
                for column in df.columns() {
                    validate_q_symbol(column.name())?;
                    length = length
                        .checked_add(column.name().len())
                        .and_then(|length| length.checked_add(1))
                        .ok_or(KolaError::OverLengthErr())?;
                    length = length
                        .checked_add(get_series_len(column.as_materialized_series())?)
                        .ok_or(KolaError::OverLengthErr())?;
                }
                Ok(length)
            }
            K::Operator(_) => Ok(2),
            K::Lambda(lambda) => {
                checked_q_count(lambda.source().len())?;
                lambda
                    .source()
                    .len()
                    .checked_add(lambda.context().len())
                    .and_then(|length| length.checked_add(8))
                    .ok_or(KolaError::OverLengthErr())
            }
            K::Null => Ok(2),
            K::Dict(dict) => {
                if dict.is_empty() {
                    return Err(KolaError::Err("Not supported empty dictionary".to_string()));
                }
                checked_q_count(dict.len())?;
                let mut length = 13usize;
                for (key, value) in dict {
                    validate_q_symbol(key)?;
                    length = length
                        .checked_add(key.len())
                        .and_then(|length| length.checked_add(1))
                        .ok_or(KolaError::OverLengthErr())?;
                    length = length
                        .checked_add(value.j6_len_with_depth(depth + 1)?)
                        .ok_or(KolaError::OverLengthErr())?;
                }
                Ok(length)
            }
        }
    }

    pub fn from_any_value(a: AnyValue) -> K {
        Self::try_from_any_value(a).unwrap_or(K::Null)
    }

    pub fn try_from_any_value(a: AnyValue) -> Result<K, KolaError> {
        let out_of_range = |kind: &str| {
            KolaError::NotAbleToSerializeErr(format!(
                "Polars {kind} value is outside q's representable range"
            ))
        };
        Ok(match a {
            AnyValue::Null => K::Null,
            AnyValue::Boolean(b) => K::Boolean(b),
            AnyValue::String(s) => K::String(s.to_owned()),
            AnyValue::StringOwned(s) => K::String(s.to_string()),
            AnyValue::UInt8(v) => K::U8(v),
            AnyValue::Int16(v) => K::I16(v),
            AnyValue::Int32(v) => K::I32(v),
            AnyValue::Int64(v) => K::I64(v),
            AnyValue::Float32(v) => K::F32(v),
            AnyValue::Float64(v) => K::F64(v),
            AnyValue::Binary(value) => K::Guid(Uuid::from_bytes(
                <[u8; 16]>::try_from(value).map_err(|_| {
                    KolaError::NotAbleToSerializeErr(
                        "Polars binary value must be exactly 16 bytes to encode as a GUID"
                            .to_string(),
                    )
                })?,
            )),
            AnyValue::BinaryOwned(value) => K::Guid(Uuid::from_bytes(
                <[u8; 16]>::try_from(value.as_slice()).map_err(|_| {
                    KolaError::NotAbleToSerializeErr(
                        "Polars binary value must be exactly 16 bytes to encode as a GUID"
                            .to_string(),
                    )
                })?,
            )),
            AnyValue::Date(v) => K::Date(
                v.checked_add(719_163)
                    .and_then(NaiveDate::from_num_days_from_ce_opt)
                    .ok_or_else(|| out_of_range("date"))?,
            ),
            AnyValue::Datetime(v, unit, _) => {
                let multiplier = match unit {
                    TimeUnit::Nanoseconds => 1,
                    TimeUnit::Microseconds => 1_000,
                    TimeUnit::Milliseconds => 1_000_000,
                };
                let nanoseconds = v
                    .checked_mul(multiplier)
                    .ok_or_else(|| out_of_range("datetime"))?;
                K::DateTime(DateTime::from_timestamp_nanos(nanoseconds))
            }
            AnyValue::Duration(v, unit) => {
                let multiplier = match unit {
                    TimeUnit::Nanoseconds => 1,
                    TimeUnit::Microseconds => 1_000,
                    TimeUnit::Milliseconds => 1_000_000,
                };
                let nanoseconds = v
                    .checked_mul(multiplier)
                    .ok_or_else(|| out_of_range("duration"))?;
                K::Duration(Duration::nanoseconds(nanoseconds))
            }
            AnyValue::Time(v) => {
                let nanoseconds = u64::try_from(v)
                    .ok()
                    .filter(|value| *value < 86_400_000_000_000)
                    .ok_or_else(|| out_of_range("time"))?;
                K::Time(
                    NaiveTime::from_num_seconds_from_midnight_opt(
                        (nanoseconds / 1_000_000_000) as u32,
                        (nanoseconds % 1_000_000_000) as u32,
                    )
                    .ok_or_else(|| out_of_range("time"))?,
                )
            }
            AnyValue::Categorical(i, g) => {
                let symbol = g.cat_to_str(i).ok_or_else(|| {
                    KolaError::NotAbleToSerializeErr(format!(
                        "Polars categorical index {i} has no symbol"
                    ))
                })?;
                validate_q_symbol(symbol)?;
                K::Symbol(symbol.to_owned())
            }
            AnyValue::List(s) => K::Series(s),
            unsupported => {
                return Err(KolaError::NotAbleToSerializeErr(format!(
                    "unsupported Polars value {unsupported:?}"
                )))
            }
        })
    }

    pub fn get_j_type_code(&self) -> i16 {
        self.try_get_j_type_code().unwrap_or(100)
    }

    pub fn try_get_j_type_code(&self) -> Result<i16, KolaError> {
        Ok(match self {
            K::Series(s) => match s.dtype() {
                PolarsDataType::Null
                | PolarsDataType::String
                | PolarsDataType::List(_)
                | PolarsDataType::Array(_, _) => 0,
                PolarsDataType::Boolean => 1,
                PolarsDataType::Binary => 2,
                PolarsDataType::UInt8 => 4,
                PolarsDataType::Int16 => 5,
                PolarsDataType::Int32 => 6,
                PolarsDataType::Int64 => 7,
                PolarsDataType::Float32 => 8,
                PolarsDataType::Float64 => 9,
                PolarsDataType::Categorical(_, _) => 11,
                PolarsDataType::Datetime(TimeUnit::Milliseconds, _) => 15,
                PolarsDataType::Datetime(_, _) => 12,
                PolarsDataType::Date => 14,
                PolarsDataType::Duration(_) => 16,
                PolarsDataType::Time => 19,
                unsupported => {
                    return Err(KolaError::NotSupportedSeriesTypeErr(unsupported.clone()))
                }
            },
            K::Boolean(_) => -1,
            K::Guid(_) => -2,
            K::U8(_) => -4,
            K::I16(_) => -5,
            K::I32(_) => -6,
            K::I64(_) => -7,
            K::F32(_) => -8,
            K::F64(_) => -9,
            K::Char(_) => -10,
            K::CharVector(_) | K::String(_) => 10,
            K::Symbol(_) => -11,
            K::DateTime(_) => -12,
            K::Date(_) => -14,
            K::Duration(_) => -16,
            K::Time(_) => -19,
            K::MixedList(_) => 0,
            K::DataFrame(_) => 98,
            K::Dict(_) => 99,
            K::Operator(operator) => operator.k_type as i16,
            K::Lambda(_) => 100,
            K::Null => 101,
        })
    }
}

impl TryFrom<K> for Series {
    type Error = KolaError;

    fn try_from(other: K) -> Result<Self, Self::Error> {
        match other {
            K::Series(series) => Ok(series),
            k => Err(KolaError::Err(format!("Not Series - {k:?}"))),
        }
    }
}

impl TryFrom<K> for DataFrame {
    type Error = KolaError;

    fn try_from(other: K) -> Result<Self, Self::Error> {
        match other {
            K::DataFrame(df) => Ok(df),
            k => Err(KolaError::Err(format!("Not DataFrame - {k:?}"))),
        }
    }
}

fn checked_q_count(length: usize) -> Result<i32, KolaError> {
    i32::try_from(length).map_err(|_| KolaError::OverLengthErr())
}

fn checked_series_size(length: usize, value_width: usize) -> Result<usize, KolaError> {
    checked_q_count(length)?;
    length
        .checked_mul(value_width)
        .and_then(|length| length.checked_add(6))
        .ok_or(KolaError::OverLengthErr())
}

fn checked_nested_series_size(
    outer_length: usize,
    values_length: usize,
    value_width: usize,
) -> Result<usize, KolaError> {
    checked_q_count(outer_length)?;
    let values_size = values_length
        .checked_mul(value_width)
        .ok_or(KolaError::OverLengthErr())?;
    let headers_size = outer_length
        .checked_mul(6)
        .ok_or(KolaError::OverLengthErr())?;
    values_size
        .checked_add(headers_size)
        .and_then(|length| length.checked_add(6))
        .ok_or(KolaError::OverLengthErr())
}

fn list_values_length(series: &Series) -> Result<usize, KolaError> {
    series.chunks().iter().try_fold(0usize, |total, array| {
        let array = array
            .as_any()
            .downcast_ref::<LargeListArray>()
            .ok_or_else(|| {
                KolaError::NotAbleToSerializeErr(
                    "expected Arrow LargeList array for Polars List series".to_string(),
                )
            })?;
        let offsets = array.offsets().as_ref();
        let first = *offsets.first().ok_or_else(|| {
            KolaError::NotAbleToSerializeErr("Polars List offsets are empty".to_string())
        })?;
        let last = *offsets.last().ok_or_else(|| {
            KolaError::NotAbleToSerializeErr("Polars List offsets are empty".to_string())
        })?;
        let chunk_length = last.checked_sub(first).ok_or_else(|| {
            KolaError::NotAbleToSerializeErr(
                "Polars List offsets are not monotonically increasing".to_string(),
            )
        })?;
        total
            .checked_add(usize::try_from(chunk_length).map_err(|_| KolaError::OverLengthErr())?)
            .ok_or(KolaError::OverLengthErr())
    })
}

pub(crate) fn validate_q_symbol(value: &str) -> Result<(), KolaError> {
    if value.as_bytes().contains(&0) {
        return Err(KolaError::NotAbleToSerializeErr(
            "q symbols cannot contain NUL bytes".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_q_time_series(series: &Series) -> Result<(), KolaError> {
    let physical = series
        .cast(&PolarsDataType::Int64)
        .map_err(|error| KolaError::NotAbleToSerializeErr(error.to_string()))?;
    let values = physical
        .i64()
        .map_err(|error| KolaError::NotAbleToSerializeErr(error.to_string()))?;
    for nanoseconds in values.iter().flatten() {
        if !(0..86_400_000_000_000).contains(&nanoseconds) {
            return Err(KolaError::NotAbleToSerializeErr(
                "q time values must be within a single day".to_string(),
            ));
        }
        if nanoseconds % 1_000_000 != 0 {
            return Err(KolaError::NotAbleToSerializeErr(
                "q time only supports millisecond precision".to_string(),
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_guid_series(series: &Series) -> Result<(), KolaError> {
    let values = series
        .binary()
        .map_err(|error| KolaError::NotAbleToSerializeErr(error.to_string()))?;
    if values.iter().flatten().any(|value| value.len() != 16) {
        return Err(KolaError::NotAbleToSerializeErr(
            "binary series values must be exactly 16 bytes to encode as GUIDs".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn get_series_len(series: &Series) -> Result<usize, KolaError> {
    let length = series.len();
    let data_type = series.dtype();
    match data_type {
        PolarsDataType::Null if length == 0 => checked_series_size(length, 0),
        PolarsDataType::Null => Err(KolaError::NotAbleToSerializeErr(
            "nonempty Null series cannot be encoded as q values".to_string(),
        )),
        PolarsDataType::Boolean => checked_series_size(length, 1),
        PolarsDataType::Int16 => checked_series_size(length, 2),
        PolarsDataType::Int32 => checked_series_size(length, 4),
        PolarsDataType::Int64 => checked_series_size(length, 8),
        PolarsDataType::UInt8 => checked_series_size(length, 1),
        PolarsDataType::Float32 => checked_series_size(length, 4),
        PolarsDataType::Float64 => checked_series_size(length, 8),
        PolarsDataType::Datetime(unit, _) => {
            if !matches!(unit, TimeUnit::Milliseconds) {
                let multiplier = match unit {
                    TimeUnit::Nanoseconds => 1,
                    TimeUnit::Microseconds => 1_000,
                    TimeUnit::Milliseconds => unreachable!(),
                };
                let physical = series
                    .cast(&PolarsDataType::Int64)
                    .map_err(|error| KolaError::NotAbleToSerializeErr(error.to_string()))?;
                let values = physical
                    .i64()
                    .map_err(|error| KolaError::NotAbleToSerializeErr(error.to_string()))?;
                for value in values.iter().flatten() {
                    value
                        .checked_mul(multiplier)
                        .and_then(|value| value.checked_sub(946_684_800_000_000_000))
                        .filter(|value| *value > i64::MIN + 1 && *value < i64::MAX)
                        .ok_or_else(|| {
                            KolaError::NotAbleToSerializeErr(
                                "timestamp is outside q's representable nanosecond range"
                                    .to_string(),
                            )
                        })?;
                }
            }
            checked_series_size(length, 8)
        }
        PolarsDataType::Date => {
            let physical = series
                .cast(&PolarsDataType::Int32)
                .map_err(|error| KolaError::NotAbleToSerializeErr(error.to_string()))?;
            let values = physical
                .i32()
                .map_err(|error| KolaError::NotAbleToSerializeErr(error.to_string()))?;
            if values
                .iter()
                .flatten()
                .any(|days| days.checked_sub(10_957).is_none())
            {
                return Err(KolaError::NotAbleToSerializeErr(
                    "date is outside q's representable range".to_string(),
                ));
            }
            checked_series_size(length, 4)
        }
        PolarsDataType::Time => {
            validate_q_time_series(series)?;
            checked_series_size(length, 4)
        }
        PolarsDataType::Duration(unit) => {
            let multiplier = match unit {
                TimeUnit::Nanoseconds => 1,
                TimeUnit::Microseconds => 1_000,
                TimeUnit::Milliseconds => 1_000_000,
            };
            let physical = series
                .cast(&PolarsDataType::Int64)
                .map_err(|error| KolaError::NotAbleToSerializeErr(error.to_string()))?;
            let values = physical
                .i64()
                .map_err(|error| KolaError::NotAbleToSerializeErr(error.to_string()))?;
            if values
                .iter()
                .flatten()
                .any(|value| value.checked_mul(multiplier).is_none())
            {
                return Err(KolaError::NotAbleToSerializeErr(
                    "duration is outside q's representable nanosecond range".to_string(),
                ));
            }
            checked_series_size(length, 8)
        }
        PolarsDataType::String => {
            let physical = series.to_physical_repr();
            let values = physical
                .str()
                .map_err(|error| KolaError::NotAbleToSerializeErr(error.to_string()))?;
            let value_bytes = values.iter().flatten().try_fold(0usize, |length, value| {
                checked_q_count(value.len())?;
                length
                    .checked_add(value.len())
                    .ok_or(KolaError::OverLengthErr())
            })?;
            checked_nested_series_size(length, value_bytes, 1)
        }
        PolarsDataType::List(data_type) => {
            if series.null_count() > 0 {
                return Err(KolaError::NotAbleToSerializeErr(
                    "null values in List columns".to_string(),
                ));
            }
            let values_length = list_values_length(series)?;
            let value_width = match data_type.as_ref() {
                PolarsDataType::Boolean | PolarsDataType::UInt8 => 1,
                PolarsDataType::Int16 => 2,
                PolarsDataType::Int32 | PolarsDataType::Float32 => 4,
                PolarsDataType::Int64 | PolarsDataType::Float64 => 8,
                _ => {
                    return Err(KolaError::NotSupportedPolarsNestedListTypeErr(
                        data_type.as_ref().clone(),
                    ))
                }
            };
            checked_nested_series_size(length, values_length, value_width)
        }
        PolarsDataType::Array(data_type, size) => {
            if *size == 0 {
                return Err(KolaError::NotAbleToSerializeErr(
                    "zero-width Array columns cannot be encoded as q lists".to_string(),
                ));
            }
            if series.null_count() > 0 {
                return Err(KolaError::NotAbleToSerializeErr(
                    "null values in Array columns".to_string(),
                ));
            }
            match data_type.as_ref() {
                PolarsDataType::Boolean => {
                    let values_length = length
                        .checked_mul(*size)
                        .ok_or(KolaError::OverLengthErr())?;
                    checked_nested_series_size(length, values_length, 1)
                }
                _ => Err(KolaError::NotSupportedPolarsNestedListTypeErr(
                    data_type.as_ref().clone(),
                )),
            }
        }
        PolarsDataType::Binary => {
            validate_guid_series(series)?;
            checked_series_size(length, 16)
        }
        PolarsDataType::Categorical(_, _) => {
            let categorical = series
                .cat32()
                .map_err(|error| KolaError::NotAbleToSerializeErr(error.to_string()))?;
            checked_q_count(length)?;
            categorical.iter_str().try_fold(6usize, |length, value| {
                let value = value.unwrap_or("");
                validate_q_symbol(value)?;
                length
                    .checked_add(value.len())
                    .and_then(|length| length.checked_add(1))
                    .ok_or(KolaError::OverLengthErr())
            })
        }
        _ => Err(KolaError::NotSupportedSeriesTypeErr(data_type.clone())),
    }
}
