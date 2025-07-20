use std::usize;

use chrono::{DateTime, Duration, NaiveDate, NaiveTime, Utc};
use indexmap::IndexMap;
use polars::{
    datatypes::DataType as PolarsDataType,
    prelude::{AnyValue, DataFrame, LargeListArray, TimeUnit},
    series::Series,
};
use polars_arrow::array::{FixedSizeListArray, ValueSize};
use rayon::iter::ParallelIterator;
use uuid::Uuid;

use crate::errors::KolaError;

pub const K_TYPE_SIZE: [usize; 20] = [0, 1, 16, 0, 1, 2, 4, 8, 4, 8, 1, 0, 8, 4, 4, 8, 8, 4, 4, 4];

#[repr(u8)]
pub enum MsgType {
    Async = 0,
    Sync = 1,
    Response = 2,
}

#[derive(Debug, PartialEq)]
pub enum J {
    Boolean(bool),
    Guid(Uuid),
    U8(u8),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Char(u8),
    Symbol(String),
    String(String),
    DateTime(DateTime<Utc>),   // datetime, timestamp
    Date(NaiveDate),           // date
    Time(NaiveTime),           // time, minute, second
    Duration(Duration),        // timespan
    MixedList(Vec<J>),         // mixed list
    Series(Series),            // list, dictionaries
    DataFrame(DataFrame),      // table and keyed table
    Dict(IndexMap<String, J>), // dict, symbols -> atom or list
    Null,
}

impl J {
    pub fn len(&self) -> Result<usize, KolaError> {
        // k type + value
        match self {
            J::Boolean(_) => Ok(2),
            J::Guid(_) => Ok(17),
            J::U8(_) => Ok(2),
            J::I16(_) => Ok(3),
            J::I32(_) => Ok(5),
            J::I64(_) => Ok(9),
            J::F32(_) => Ok(5),
            J::F64(_) => Ok(9),
            J::Char(_) => Ok(2),
            J::Symbol(k) => Ok(k.len() + 2),
            J::String(k) => Ok(k.len() + 6),
            J::DateTime(_) => Ok(9),
            J::Date(_) => Ok(5),
            J::Time(_) => Ok(5),
            J::Duration(_) => Ok(9),
            J::MixedList(l) => {
                let lens = l
                    .iter()
                    .map(|k| k.len())
                    .collect::<Result<Vec<_>, KolaError>>();
                Ok(lens?.into_iter().sum::<usize>() + 6)
            }
            J::Series(series) => get_series_len(series),
            J::DataFrame(df) => {
                // 98 0 99 + symbol list(6) + values(6)
                let mut length: usize = 15;
                for column in df.get_columns().into_iter() {
                    length += column.name().len() + 1;
                    length += get_series_len(column.as_materialized_series())?
                }
                Ok(length)
            }
            J::Null => Ok(2),
            J::Dict(dict) => {
                let mut length = 13;
                for (k, v) in dict.iter() {
                    length += k.len() + 1;
                    length += v.len()?;
                }
                Ok(length)
            }
        }
    }

    pub fn from_any_value(a: AnyValue) -> J {
        match a {
            AnyValue::Boolean(b) => J::Boolean(b),
            AnyValue::String(s) => J::String(s.to_owned()),
            AnyValue::UInt8(v) => J::U8(v),
            AnyValue::Int16(v) => J::I16(v),
            AnyValue::Int32(v) => J::I32(v),
            AnyValue::Int64(v) => J::I64(v),
            AnyValue::Float32(v) => J::F32(v),
            AnyValue::Float64(v) => J::F64(v),
            AnyValue::Date(v) => J::Date(NaiveDate::from_num_days_from_ce_opt(v + 719163).unwrap()),
            AnyValue::Datetime(v, TimeUnit::Milliseconds, _) => {
                J::DateTime(DateTime::from_timestamp_nanos(v * 1000000))
            }
            AnyValue::Datetime(v, TimeUnit::Nanoseconds, _) => {
                J::DateTime(DateTime::from_timestamp_nanos(v))
            }
            AnyValue::Duration(v, TimeUnit::Nanoseconds) => J::Duration(Duration::nanoseconds(v)),
            AnyValue::Time(v) => J::Time(
                NaiveTime::from_num_seconds_from_midnight_opt(
                    (v / 1000000000) as u32,
                    (v % 1000000000) as u32,
                )
                .unwrap(),
            ),
            AnyValue::Categorical(i, g, _) => {
                let sym = g.get(i);
                J::Symbol(sym.to_owned())
            }
            AnyValue::List(s) => J::Series(s),
            AnyValue::StringOwned(s) => J::String(s.to_string()),
            _ => J::Null,
        }
    }

    pub fn get_j_type_code(&self) -> i16 {
        match self {
            J::Series(s) => match s.dtype() {
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
            J::Boolean(_) => -1,
            J::U8(_) => -2,
            J::I16(_) => -3,
            J::I32(_) => -4,
            J::I64(_) => -5,
            J::Date(_) => -6,
            J::Time(_) => -7,
            J::DateTime(_) => -9,
            J::Duration(_) => -10,
            J::F32(_) => -11,
            J::F64(_) => -12,
            J::String(_) => -13,
            J::Symbol(_) => -14,
            J::MixedList(_) => 90,
            J::Dict(_) => 91,
            J::DataFrame(_) => 92,
            J::Null => 0,
            _ => 100,
        }
    }
}

impl TryFrom<J> for Series {
    type Error = KolaError;

    fn try_from(other: J) -> Result<Self, Self::Error> {
        match other {
            J::Series(series) => Ok(series),
            k => Err(KolaError::Err(format!("Not Series - {:?}", k))),
        }
    }
}

impl TryFrom<J> for DataFrame {
    type Error = KolaError;

    fn try_from(other: J) -> Result<Self, Self::Error> {
        match other {
            J::DataFrame(df) => Ok(df),
            k => Err(KolaError::Err(format!("Not DataFrame - {:?}", k))),
        }
    }
}

pub(crate) fn get_series_len(series: &Series) -> Result<usize, KolaError> {
    let length = series.len();
    let data_type = series.dtype();
    match data_type {
        PolarsDataType::Null => Ok(length * 2 + 6),
        PolarsDataType::Boolean => Ok(length + 6),
        PolarsDataType::Int16 => Ok(length * 2 + 6),
        PolarsDataType::Int32 => Ok(length * 4 + 6),
        PolarsDataType::Int64 => Ok(length * 8 + 6),
        PolarsDataType::UInt8 => Ok(length * 2 + 6),
        PolarsDataType::UInt16 => Ok(length * 4 + 6),
        PolarsDataType::UInt32 => Ok(length * 8 + 6),
        PolarsDataType::Float32 => Ok(length * 4 + 6),
        PolarsDataType::Float64 => Ok(length * 8 + 6),
        // to k datetime
        PolarsDataType::Datetime(_, _) => Ok(length * 8 + 6),
        PolarsDataType::Date => Ok(length * 8 + 6),
        // to time
        // to timespan
        PolarsDataType::Time => Ok(length * 8 + 6),
        // to timespan
        PolarsDataType::Duration(_) => Ok(length * 8 + 6),
        // to string
        PolarsDataType::String => {
            let ptr = series.to_physical_repr();
            let array = ptr.str().unwrap();
            let str_size: usize = array.par_iter().map(|s| s.unwrap_or("").len()).sum();
            Ok(array.get_values_size() * 6 + str_size)
        }
        PolarsDataType::List(data_type) => {
            let array = series.chunks()[0]
                .as_any()
                .downcast_ref::<LargeListArray>()
                .unwrap();
            let length = array.offsets().len();
            let values_length = array.len();
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
            let array = series.chunks()[0]
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .unwrap();
            let length = array.len();
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
            let array = series.binary().unwrap();
            let is_16_fixed_binary = array.into_iter().any(|v| 16 == v.unwrap_or(&[]).len());
            if is_16_fixed_binary {
                Ok(16 * length + 6)
            } else {
                Err(KolaError::Err(format!(
                    "Only support 16 fixed size binary as guid",
                )))
            }
        }
        // to symbol
        PolarsDataType::Categorical(_, _) => {
            let cat = series.categorical().unwrap();
            let mut length: usize = 6;
            for s in cat.iter_str() {
                length += s.unwrap_or("").len() + 1;
            }
            Ok(length)
        }
        _ => Err(KolaError::NotSupportedSeriesTypeErr(data_type.clone())),
    }
}
