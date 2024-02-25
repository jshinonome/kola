use chrono::{DateTime, Duration, NaiveDate, NaiveTime, Utc};
use polars::{
    datatypes::DataType as PolarsDataType,
    prelude::{DataFrame, LargeListArray},
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

#[derive(Debug)]
pub struct Dict {
    keys: Box<Vec<String>>,
    values: Box<Vec<K>>,
}

#[derive(Debug)]
pub enum K {
    Bool(bool),
    Guid(Uuid),
    Byte(u8),
    Short(i16),
    Int(i32),
    Long(i64),
    Real(f32),
    Float(f64),
    Char(u8),
    Symbol(String),
    String(String),
    DateTime(DateTime<Utc>), // datetime, timestamp
    Date(NaiveDate),         // date
    Time(NaiveTime),         // time, minute, second
    Duration(Duration),      // timespan
    Series(Series),          // list, dictionaries
    DataFrame(DataFrame),    // table and keyed table
    Dict(Dict),              // dict, symbols -> atom or list
    None(u8),
}

impl K {
    pub fn len(&self) -> Result<usize, KolaError> {
        // k type + value
        match self {
            K::Bool(_) => Ok(2),
            K::Guid(_) => Ok(17),
            K::Byte(_) => Ok(2),
            K::Short(_) => Ok(3),
            K::Int(_) => Ok(5),
            K::Long(_) => Ok(9),
            K::Real(_) => Ok(5),
            K::Float(_) => Ok(9),
            K::Char(_) => Ok(2),
            K::Symbol(k) => Ok(k.len() + 2),
            K::String(k) => Ok(k.len() + 6),
            K::DateTime(_) => Ok(9),
            K::Date(_) => Ok(5),
            K::Time(_) => Ok(5),
            K::Duration(_) => Ok(9),
            K::Series(series) => get_series_len(series),
            K::DataFrame(df) => {
                // 98 0 99 + symbol list(6) + values(6)
                let mut length: usize = 15;
                for column in df.get_columns().into_iter() {
                    length += column.name().len() + 1;
                    length += get_series_len(column)?
                }
                Ok(length)
            }
            K::None(_) => Ok(2),
            K::Dict(dict) => {
                let mut length = 13;
                dict.keys()
                    .as_ref()
                    .iter()
                    .for_each(|key| length += key.len() + 1);
                dict.values()
                    .as_ref()
                    .iter()
                    .for_each(|k| length += k.len().unwrap());
                Ok(length)
            }
        }
    }
}

impl TryFrom<K> for Series {
    type Error = KolaError;

    fn try_from(other: K) -> Result<Self, Self::Error> {
        match other {
            K::Series(series) => Ok(series),
            k => Err(KolaError::Err(format!("Not Series - {:?}", k))),
        }
    }
}

impl TryFrom<K> for DataFrame {
    type Error = KolaError;

    fn try_from(other: K) -> Result<Self, Self::Error> {
        match other {
            K::DataFrame(df) => Ok(df),
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

impl Dict {
    pub fn with_capacity(capacity: usize) -> Self {
        Dict {
            keys: Box::new(Vec::with_capacity(capacity)),
            values: Box::new(Vec::with_capacity(capacity)),
        }
    }
    pub fn set(&mut self, key: String, value: K) -> Result<(), KolaError> {
        if self.keys.len() < self.keys.capacity() {
            match value {
                K::Bool(_)
                | K::Byte(_)
                | K::Short(_)
                | K::Int(_)
                | K::Long(_)
                | K::Real(_)
                | K::Float(_)
                | K::Char(_)
                | K::Symbol(_)
                | K::String(_)
                | K::DateTime(_)
                | K::Date(_)
                | K::Time(_)
                | K::Duration(_)
                | K::Series(_)
                | K::DataFrame(_)
                | K::None(_) => {
                    self.keys.push(key);
                    self.values.push(value);
                    Ok(())
                }
                // K::Guid(_) => todo!(),
                _ => Err(KolaError::Err(format!(
                    "Not support {:?} as value of dict",
                    value
                ))),
            }
        } else {
            Err(KolaError::Err("Exceed capacity of dict".to_string()))
        }
    }
    pub fn keys(&self) -> &Box<Vec<String>> {
        &self.keys
    }

    pub fn values(&self) -> &Box<Vec<K>> {
        &self.values
    }
}
