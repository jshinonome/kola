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
            K::CharVector(k) => k.len().checked_add(6).ok_or(KolaError::OverLengthErr()),
            K::Symbol(k) => {
                validate_q_symbol(k)?;
                k.len().checked_add(2).ok_or(KolaError::OverLengthErr())
            }
            K::String(k) => k.len().checked_add(6).ok_or(KolaError::OverLengthErr()),
            K::DateTime(_) => Ok(9),
            K::Date(_) => Ok(5),
            K::Time(k) => {
                if k.nanosecond() % 1_000_000 != 0 {
                    return Err(KolaError::NotAbleToSerializeErr(
                        "q time only supports millisecond precision".to_string(),
                    ));
                }
                Ok(5)
            }
            K::Duration(_) => Ok(9),
            K::MixedList(values) => values.iter().try_fold(6usize, |length, value| {
                length
                    .checked_add(value.j6_len_with_depth(depth + 1)?)
                    .ok_or(KolaError::OverLengthErr())
            }),
            K::Series(series) => get_series_len(series),
            K::DataFrame(df) => {
                // 98 0 99 + symbol list(6) + values(6)
                let mut length: usize = 15;
                for column in df.columns() {
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
            K::Null => Ok(2),
            K::Dict(dict) => {
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
        match a {
            AnyValue::Boolean(b) => K::Boolean(b),
            AnyValue::String(s) => K::String(s.to_owned()),
            AnyValue::UInt8(v) => K::U8(v),
            AnyValue::Int16(v) => K::I16(v),
            AnyValue::Int32(v) => K::I32(v),
            AnyValue::Int64(v) => K::I64(v),
            AnyValue::Float32(v) => K::F32(v),
            AnyValue::Float64(v) => K::F64(v),
            AnyValue::Binary(value) => <[u8; 16]>::try_from(value)
                .map(Uuid::from_bytes)
                .map(K::Guid)
                .unwrap_or(K::Null),
            AnyValue::BinaryOwned(value) => <[u8; 16]>::try_from(value.as_slice())
                .map(Uuid::from_bytes)
                .map(K::Guid)
                .unwrap_or(K::Null),
            AnyValue::Date(v) => K::Date(NaiveDate::from_num_days_from_ce_opt(v + 719163).unwrap()),
            AnyValue::Datetime(v, TimeUnit::Milliseconds, _) => {
                K::DateTime(DateTime::from_timestamp_nanos(v * 1000000))
            }
            AnyValue::Datetime(v, TimeUnit::Nanoseconds, _) => {
                K::DateTime(DateTime::from_timestamp_nanos(v))
            }
            AnyValue::Duration(v, TimeUnit::Nanoseconds) => K::Duration(Duration::nanoseconds(v)),
            AnyValue::Time(v) => K::Time(
                NaiveTime::from_num_seconds_from_midnight_opt(
                    (v / 1000000000) as u32,
                    (v % 1000000000) as u32,
                )
                .unwrap(),
            ),
            AnyValue::Categorical(i, g) => {
                let sym = g.cat_to_str(i).unwrap_or("");
                K::Symbol(sym.to_owned())
            }
            AnyValue::List(s) => K::Series(s),
            AnyValue::StringOwned(s) => K::String(s.to_string()),
            _ => K::Null,
        }
    }

    pub fn get_j_type_code(&self) -> i16 {
        match self {
            K::Series(s) => match s.dtype() {
                PolarsDataType::Boolean => 1,
                PolarsDataType::UInt8 => 2,
                PolarsDataType::Int16 => 3,
                PolarsDataType::Int32 => 4,
                PolarsDataType::Int64 => 5,
                PolarsDataType::Date => 6,
                PolarsDataType::Time => 7,
                PolarsDataType::Datetime(TimeUnit::Milliseconds, _) => 8,
                PolarsDataType::Datetime(TimeUnit::Nanoseconds, _) => 9,
                PolarsDataType::Duration(_) => 10,
                PolarsDataType::Float32 => 11,
                PolarsDataType::Float64 => 12,
                PolarsDataType::String => 13,
                PolarsDataType::Categorical(_, _) => 14,
                _ => 15,
            },
            K::Boolean(_) => -1,
            K::U8(_) => -2,
            K::I16(_) => -3,
            K::I32(_) => -4,
            K::I64(_) => -5,
            K::Date(_) => -6,
            K::Time(_) => -7,
            K::DateTime(_) => -9,
            K::Duration(_) => -10,
            K::F32(_) => -11,
            K::F64(_) => -12,
            K::CharVector(_) => -13,
            K::String(_) => -13,
            K::Symbol(_) => -14,
            K::MixedList(_) => 90,
            K::Dict(_) => 91,
            K::DataFrame(_) => 92,
            K::Null => 0,
            _ => 100,
        }
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

fn checked_series_size(length: usize, value_width: usize) -> Result<usize, KolaError> {
    length
        .checked_mul(value_width)
        .and_then(|length| length.checked_add(6))
        .ok_or(KolaError::OverLengthErr())
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
    if values
        .iter()
        .flatten()
        .any(|nanoseconds| nanoseconds % 1_000_000 != 0)
    {
        return Err(KolaError::NotAbleToSerializeErr(
            "q time only supports millisecond precision".to_string(),
        ));
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
        PolarsDataType::Null => checked_series_size(length, 2),
        PolarsDataType::Boolean => checked_series_size(length, 1),
        PolarsDataType::Int16 => checked_series_size(length, 2),
        PolarsDataType::Int32 => checked_series_size(length, 4),
        PolarsDataType::Int64 => checked_series_size(length, 8),
        PolarsDataType::UInt8 => checked_series_size(length, 1),
        PolarsDataType::UInt16 => checked_series_size(length, 4),
        PolarsDataType::UInt32 => checked_series_size(length, 8),
        PolarsDataType::Float32 => checked_series_size(length, 4),
        PolarsDataType::Float64 => checked_series_size(length, 8),
        // to k datetime
        PolarsDataType::Datetime(_, _) => checked_series_size(length, 8),
        PolarsDataType::Date => checked_series_size(length, 4),
        // to time
        PolarsDataType::Time => {
            validate_q_time_series(series)?;
            checked_series_size(length, 4)
        }
        // to timespan
        PolarsDataType::Duration(_) => checked_series_size(length, 8),
        // to string
        PolarsDataType::String => {
            let physical = series.to_physical_repr();
            let values = physical
                .str()
                .map_err(|error| KolaError::NotAbleToSerializeErr(error.to_string()))?;
            let value_bytes = values.iter().flatten().try_fold(0usize, |length, value| {
                length
                    .checked_add(value.len())
                    .ok_or(KolaError::OverLengthErr())
            })?;
            checked_series_size(length, 6)?
                .checked_add(value_bytes)
                .ok_or(KolaError::OverLengthErr())
        }
        PolarsDataType::List(data_type) => {
            let values_length = series
                .chunks()
                .iter()
                .map(|array| {
                    let array = array.as_any().downcast_ref::<LargeListArray>().unwrap();
                    let offsets = array.offsets().as_ref();
                    (offsets[offsets.len() - 1] - offsets[0]) as usize
                })
                .sum::<usize>();
            match data_type.as_ref() {
                PolarsDataType::Boolean => Ok(values_length + 6 * length + 6),
                PolarsDataType::UInt8 => Ok(values_length + 6 * length + 6),
                PolarsDataType::Int16 => Ok(2 * values_length + 6 * length + 6),
                PolarsDataType::Int32 => Ok(4 * values_length + 6 * length + 6),
                PolarsDataType::Int64 => Ok(8 * values_length + 6 * length + 6),
                PolarsDataType::Float32 => Ok(4 * values_length + 6 * length + 6),
                PolarsDataType::Float64 => Ok(8 * values_length + 6 * length + 6),
                _ => Err(KolaError::NotSupportedSeriesTypeErr(
                    data_type.as_ref().clone(),
                )),
            }
        }
        PolarsDataType::Array(data_type, size) => {
            let length = series.len();
            match data_type.as_ref() {
                PolarsDataType::Boolean => Ok((size + 6) * length + 6),
                PolarsDataType::UInt8 => Ok((size + 6) * length + 6),
                PolarsDataType::Int16 => Ok((2 * size + 6) * length + 6),
                PolarsDataType::Int32 => Ok((4 * size + 6) * length + 6),
                PolarsDataType::Int64 => Ok((8 * size + 6) * length + 6),
                PolarsDataType::Float32 => Ok((4 * size + 6) * length + 6),
                PolarsDataType::Float64 => Ok((8 * size + 6) * length + 6),
                _ => Err(KolaError::NotSupportedSeriesTypeErr(
                    data_type.as_ref().clone(),
                )),
            }
        }
        PolarsDataType::Binary => {
            validate_guid_series(series)?;
            checked_series_size(length, 16)
        }
        // to symbol
        PolarsDataType::Categorical(_, _) => {
            let categorical = series
                .cat32()
                .map_err(|error| KolaError::NotAbleToSerializeErr(error.to_string()))?;
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
