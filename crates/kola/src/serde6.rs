use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveTime, Timelike, Utc};
use indexmap::IndexMap;
use polars::chunked_array::ops::ChunkFillNullValue;
use polars::datatypes::{
    CategoricalOrdering, DataType as PolarsDataType, TimeUnit as PolarTimeUnit,
};
use polars::prelude::DataFrame;
use polars::series::{IntoSeries, Series};
use polars_arrow::array::{
    Array, BinaryViewArray, BooleanArray, FixedSizeBinaryArray, FixedSizeListArray, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, ListArray, PrimitiveArray, UInt8Array,
    Utf8ViewArray,
};
use polars_arrow::bitmap::Bitmap;
use polars_arrow::buffer::Buffer;
use polars_arrow::datatypes::{ArrowDataType, Field, TimeUnit};
use polars_arrow::legacy::kernels::set::set_at_nulls;
use polars_arrow::types::NativeType;
use polars_arrow::{array::Utf8Array, offset::OffsetsBuffer};
use rayon::iter::IntoParallelIterator;
use rayon::prelude::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};
use std::cmp::{max, min};
use std::i64;
use std::io::Write;
use uuid::Uuid;
// time difference between chrono and q types
pub const NANOS_DIFF: i64 = 946684800000000000;
const NANOS_PER_DAY: i64 = 86400000000000;
const MS_PER_DAY: f64 = 86400000.0;
pub const DAY_DIFF: i32 = 730120;
const K_TYPE_NAME: [&str; 20] = [
    "",
    "boolean",
    "guid",
    "",
    "byte",
    "short",
    "int",
    "long",
    "real",
    "float",
    "char",
    "symbol",
    "timestamp",
    "",
    "date",
    "datetime",
    "timespan",
    "minute",
    "second",
    "time",
];

use crate::types::get_series_len;
use crate::{
    errors::KolaError,
    types::{J, K_TYPE_SIZE},
};

pub fn deserialize(vec: &[u8], pos: &mut usize, is_column: bool) -> Result<J, KolaError> {
    let k_type = vec[*pos];
    *pos += 1;
    let start_pos = *pos;
    match k_type {
        237..=255 => match k_type {
            255 => {
                *pos += 1;
                Ok(J::Boolean(vec[start_pos] == 1))
            }
            254 => {
                *pos += 16;
                Ok(J::Guid(Uuid::from_bytes(
                    vec[start_pos..start_pos + 16].try_into().unwrap(),
                )))
            }
            252 => {
                *pos += 1;
                Ok(J::U8(vec[start_pos]))
            }
            251 => {
                *pos += 2;
                Ok(J::I16(i16::from_le_bytes(
                    vec[start_pos..start_pos + 2].try_into().unwrap(),
                )))
            }
            250 => {
                *pos += 4;
                Ok(J::I32(i32::from_le_bytes(
                    vec[start_pos..start_pos + 4].try_into().unwrap(),
                )))
            }
            249 => {
                *pos += 8;
                Ok(J::I64(i64::from_le_bytes(
                    vec[start_pos..start_pos + 8].try_into().unwrap(),
                )))
            }
            248 => {
                *pos += 4;
                Ok(J::F32(f32::from_le_bytes(
                    vec[start_pos..start_pos + 4].try_into().unwrap(),
                )))
            }
            247 => {
                *pos += 8;
                Ok(J::F64(f64::from_le_bytes(
                    vec[start_pos..start_pos + 8].try_into().unwrap(),
                )))
            }
            246 => {
                *pos += 1;
                Ok(J::Char(vec[start_pos]))
            }
            245 => {
                let mut eod_pos = *pos;
                while eod_pos <= vec.len() && vec[eod_pos] != 0 {
                    eod_pos += 1;
                }
                *pos = eod_pos + 1;
                Ok(J::Symbol(
                    String::from_utf8(vec[start_pos..eod_pos].to_vec()).unwrap(),
                ))
            }
            // timestamp
            244 => {
                let ns = i64::from_le_bytes(vec[*pos..*pos + 8].try_into().unwrap())
                    .saturating_add(NANOS_DIFF);
                *pos += 8;
                Ok(J::DateTime(create_datetime(ns)))
            }
            // month
            243 => {
                let unit = i32::from_le_bytes(vec[*pos..*pos + 4].try_into().unwrap());
                let year;
                let month;
                if unit >= 0 {
                    year = 2000 + unit / 12;
                    month = 1 + unit % 12;
                } else {
                    year = 2000 + (unit - 11) / 12;
                    month = 12 + (unit - 11) % 12
                }
                *pos += 4;
                Ok(J::Date(
                    NaiveDate::from_ymd_opt(year, month as u32, 1).unwrap(),
                ))
            }
            // date
            242 => {
                let days = i32::from_le_bytes(vec[*pos..*pos + 4].try_into().unwrap())
                    .saturating_add(DAY_DIFF);
                *pos += 4;
                let date = match NaiveDate::from_num_days_from_ce_opt(days) {
                    Some(date) => date,
                    None => {
                        if days > NaiveDate::MAX.num_days_from_ce() {
                            NaiveDate::MAX
                        } else {
                            NaiveDate::MIN
                        }
                    }
                };
                Ok(J::Date(date))
            }
            // datetime
            241 => {
                let unit = f64::from_le_bytes(vec[*pos..*pos + 8].try_into().unwrap());
                let ns = NANOS_DIFF + (unit * NANOS_PER_DAY as f64) as i64;
                *pos += 8;
                Ok(J::DateTime(create_datetime(ns)))
            }
            // timespan
            240 => {
                let ns = i64::from_le_bytes(vec[*pos..*pos + 8].try_into().unwrap());
                *pos += 8;
                Ok(J::Duration(Duration::nanoseconds(ns)))
            }
            // time, second, minute
            239 | 238 | 237 => {
                let unit = i32::from_le_bytes(vec[*pos..*pos + 4].try_into().unwrap());
                if unit < 0 {
                    return Err(KolaError::NotSupportedMinusTimeErr(k_type));
                }
                let unit = unit as u32;
                let mut seconds: u32 = 0;
                let mut nanos: u32 = 0;
                // ms
                if k_type == 237 {
                    seconds = unit / 1000;
                    nanos = 1000000 * (unit % 1000)
                // second
                } else if k_type == 238 {
                    seconds = unit;
                } else if k_type == 239 {
                    seconds = unit * 60;
                }
                *pos += 4;
                Ok(J::Time(
                    NaiveTime::from_num_seconds_from_midnight_opt(seconds, nanos).unwrap_or(
                        NaiveTime::from_num_seconds_from_midnight_opt(
                            23 * 3600 + 59 * 60 + 59,
                            999_999_999,
                        )
                        .unwrap(),
                    ),
                ))
            }
            _ => Err(KolaError::NotSupportedKTypeErr(k_type)),
        },
        // string, list(i16, i32, i64, f32, f64)
        0..=19 => {
            let end_pos = match calculate_array_end_index(vec, *pos, k_type) {
                Ok(end_pos) => end_pos,
                Err(e) => {
                    if !is_column && k_type == 0 {
                        *pos += 1;
                        let length =
                            u32::from_le_bytes(vec[*pos..*pos + 4].try_into().unwrap()) as usize;
                        *pos += 4;
                        let mut res = Vec::with_capacity(length);
                        for _ in 0..length {
                            res.push(deserialize(vec, pos, false)?);
                        }
                        return Ok(J::MixedList(res));
                    } else {
                        return Err(e);
                    }
                }
            };
            let start_pos = *pos;
            *pos = end_pos;
            if k_type == 10 {
                deserialize_series(&vec[start_pos..end_pos], k_type, false)
            } else {
                deserialize_series(&vec[start_pos..end_pos], k_type, true)
            }
        }
        99 => {
            if vec[*pos] == 98 {
                let mut key_df: DataFrame = deserialize(vec, pos, true)?.try_into()?;
                let value_df: DataFrame = deserialize(vec, pos, true)?.try_into()?;
                unsafe { key_df.hstack_mut_unchecked(value_df.get_columns()) };
                Ok(J::DataFrame(key_df))
            } else if vec[*pos] == 11 {
                *pos += 1;
                let end_pos = calculate_array_end_index(vec, *pos, 11)?;
                let keys = deserialize_series(&vec[*pos..end_pos], 11, true)?;
                *pos = end_pos;
                if vec[end_pos] > 19 {
                    return Err(KolaError::Err(format!(
                        "Not support k type {:?} values in dictionary",
                        vec[end_pos]
                    )));
                }
                let values = deserialize(vec, pos, is_column)?;
                let keys = Series::try_from(keys).unwrap();
                match values {
                    J::Series(s) => {
                        let mut dict = IndexMap::with_capacity(keys.len());
                        for (k, v) in keys.categorical().unwrap().iter_str().zip(s.iter()) {
                            dict.insert(k.unwrap().to_string(), J::from_any_value(v));
                        }
                        Ok(J::Dict(dict))
                    }
                    J::MixedList(l) => {
                        let mut dict = IndexMap::with_capacity(keys.len());
                        for (k, v) in keys.categorical().unwrap().iter_str().zip(l.into_iter()) {
                            dict.insert(k.unwrap().to_string(), v);
                        }
                        Ok(J::Dict(dict))
                    }
                    _ => unreachable!(),
                }
            } else {
                Err(KolaError::Err(format!(
                    "Only support symbol keys dictionary or keyed table, got k type {:?}",
                    vec[*pos]
                )))
            }
        }
        98 => {
            *pos += 3;
            let end_pos = calculate_array_end_index(vec, *pos, 11)?;
            let k = deserialize_series(&vec[*pos..end_pos], 11, false)?;
            *pos = end_pos;
            let symbols = if let J::Series(series) = k {
                series
            } else {
                return Err(KolaError::DeserializationErr(format!(
                    "Expecting array, but got {:?}",
                    k
                )));
            };
            let symbols = symbols.str().unwrap();
            *pos += 6;
            let mut k_types = vec![0u8; symbols.len()];
            let mut vectors: Vec<&[u8]> = Vec::with_capacity(symbols.len());
            for i in 0..symbols.len() {
                k_types[i] = vec[*pos];
                *pos += 1;
                let end_pos = calculate_array_end_index(vec, *pos, k_types[i])?;
                vectors.push(&vec[*pos..end_pos]);
                *pos = end_pos;
            }

            let mut columns: Vec<Series> = vectors
                .par_iter()
                .zip(k_types.clone())
                .map(|(v, t)| deserialize_series(v, t, true).unwrap().try_into().unwrap())
                .collect();
            columns.iter_mut().zip(symbols).for_each(|(c, n)| {
                c.rename(n.unwrap_or("").into());
            });
            Ok(J::DataFrame(
                DataFrame::new(columns.into_iter().map(|c| c.into()).collect()).unwrap(),
            ))
        }
        101 => {
            *pos += 1;
            if vec[start_pos] == 0 {
                Ok(J::Null)
            } else {
                Err(KolaError::NotSupportedKOperatorErr(vec[*pos]))
            }
        }
        // q error
        128 => {
            let mut eod_pos = *pos;
            while eod_pos <= vec.len() && vec[eod_pos] != 0 {
                eod_pos += 1;
            }
            *pos = eod_pos;
            Err(KolaError::ServerErr(
                String::from_utf8(vec[start_pos..eod_pos].to_vec()).unwrap(),
            ))
        }
        _ => Err(KolaError::NotSupportedKTypeErr(k_type)),
    }
}

fn create_field(k_type: u8, name: &str) -> Result<Field, KolaError> {
    match k_type {
        1 => Ok(Field::new(name.into(), ArrowDataType::Boolean, false)),
        2 => Ok(Field::new(name.into(), ArrowDataType::Binary, false)),
        4 => Ok(Field::new(name.into(), ArrowDataType::UInt8, false)),
        5 => Ok(Field::new(name.into(), ArrowDataType::Int16, true)),
        6 => Ok(Field::new(name.into(), ArrowDataType::Int32, true)),
        7 => Ok(Field::new(name.into(), ArrowDataType::Int64, true)),
        8 => Ok(Field::new(name.into(), ArrowDataType::Float32, false)),
        9 => Ok(Field::new(name.into(), ArrowDataType::Float64, false)),
        10 => Ok(Field::new(name.into(), ArrowDataType::LargeUtf8, false)),
        11 => Ok(Field::new(name.into(), ArrowDataType::LargeUtf8, false)),
        12 => Ok(Field::new(
            name.into(),
            ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )),
        14 => Ok(Field::new(name.into(), ArrowDataType::Date32, true)),
        15 => Ok(Field::new(
            name.into(),
            ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )),
        16 => Ok(Field::new(
            name.into(),
            ArrowDataType::Time64(TimeUnit::Nanosecond),
            true,
        )),
        17 => Ok(Field::new(
            name.into(),
            ArrowDataType::Time32(TimeUnit::Millisecond),
            true,
        )),
        18 => Ok(Field::new(
            name.into(),
            ArrowDataType::Time32(TimeUnit::Millisecond),
            true,
        )),
        19 => Ok(Field::new(
            name.into(),
            ArrowDataType::Time32(TimeUnit::Millisecond),
            true,
        )),
        _ => Err(KolaError::NotSupportedKListErr(k_type)),
    }
}

fn calculate_array_end_index(vec: &[u8], start_pos: usize, k_type: u8) -> Result<usize, KolaError> {
    let mut pos = start_pos;
    match k_type {
        0 => {
            pos += 1;
            let length = u32::from_le_bytes(vec[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            if length == 0 {
                return Ok(pos);
            }
            let sub_k_type = vec[pos];
            if sub_k_type > 19 {
                return Err(KolaError::NotSupportedKNestedListErr(sub_k_type));
            }
            let k_size = K_TYPE_SIZE[sub_k_type as usize];
            if let 1 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 12 = sub_k_type {
                for _ in 0..length {
                    let current_k_type = vec[pos];
                    if sub_k_type != current_k_type && current_k_type != 0 {
                        return Err(KolaError::NotSupportedKMixedListErr(sub_k_type, vec[pos]));
                    }
                    pos += 2;
                    let sub_length = i32::from_le_bytes(vec[pos..pos + 4].try_into().unwrap());
                    if current_k_type == 0 && sub_length > 0 {
                        return Err(KolaError::NotSupportedKMixedListErr(sub_k_type, vec[pos]));
                    }
                    pos += 4;
                    pos += k_size * sub_length as usize;
                }
                Ok(pos)
            } else if let 11 = sub_k_type {
                for _ in 0..length {
                    pos += 2;
                    let sub_length = i32::from_le_bytes(vec[pos..pos + 4].try_into().unwrap());
                    pos += 4;
                    for _ in 0..sub_length {
                        let mut k = 0;
                        while vec[pos + k] != 0 {
                            k += 1;
                        }
                        pos += k + 1;
                    }
                }
                Ok(pos)
            } else {
                Err(KolaError::NotSupportedKNestedListErr(sub_k_type))
            }
        }
        // symbol list
        11 => {
            pos += 1;
            let length = u32::from_le_bytes(vec[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            let mut i = 0;
            while i < length {
                if vec[pos] == 0 {
                    i += 1;
                }
                pos += 1;
            }
            Ok(pos)
        }
        _ => {
            if k_type > 20 {
                Err(KolaError::NotSupportedKListErr(k_type))
            } else if K_TYPE_SIZE[k_type as usize] > 0 {
                pos += 1;
                let length = u32::from_le_bytes(vec[pos..pos + 4].try_into().unwrap()) as usize;
                let k_size = K_TYPE_SIZE[k_type as usize];
                Ok(pos + 4 + k_size * length)
            } else {
                Err(KolaError::NotSupportedKListErr(k_type))
            }
        }
    }
}

fn deserialize_series(vec: &[u8], k_type: u8, as_column: bool) -> Result<J, KolaError> {
    let mut pos = 1;
    let length = u32::from_le_bytes(vec[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;
    if length == 0 {
        return new_empty_series(k_type);
    }
    let mut series: Series;
    let array_box: Box<dyn Array>;
    let k_size = K_TYPE_SIZE[k_type as usize];
    let array_vec = &vec[pos..];
    let name = K_TYPE_NAME[k_type as usize];
    match k_type {
        0 => deserialize_nested_array(vec),
        1 => {
            array_box =
                BooleanArray::from_slice(array_vec.iter().map(|u| *u == 1).collect::<Vec<_>>())
                    .boxed();
            series = Series::from_arrow(name.into(), array_box).unwrap();
            Ok(J::Series(series))
        }
        2 => {
            array_box = FixedSizeBinaryArray::new(
                ArrowDataType::FixedSizeBinary(16),
                Buffer::from(array_vec.to_vec()),
                None,
            )
            .boxed();
            series = Series::from_arrow(name.into(), array_box).unwrap();
            Ok(J::Series(series))
        }
        4 => {
            array_box = UInt8Array::from_vec(array_vec.to_vec()).boxed();
            series = Series::from_arrow(name.into(), array_box).unwrap();
            Ok(J::Series(series))
        }
        5 => {
            let array_vec = array_vec.to_vec();
            let new_ptr: *const i16 = array_vec.as_ptr().cast();
            let slice = unsafe { core::slice::from_raw_parts(new_ptr, array_vec.len() / k_size) };
            let bitmap = Bitmap::from_iter(slice.into_iter().map(|s| *s != i16::MIN));
            let mut array = Int16Array::from_slice(slice);
            array.set_validity(Some(bitmap));
            series = Series::from_arrow(name.into(), array.boxed()).unwrap();
            Ok(J::Series(series))
        }
        6 => {
            let array_vec = array_vec.to_vec();
            let new_ptr: *const i32 = array_vec.as_ptr().cast();
            let slice = unsafe { core::slice::from_raw_parts(new_ptr, array_vec.len() / k_size) };
            let bitmap = Bitmap::from_iter(
                slice
                    .into_iter()
                    .map(|s| *s > i32::MIN + 1 && *s < i32::MAX),
            );
            let mut array = Int32Array::from_slice(slice);
            array.set_validity(Some(bitmap));
            series = Series::from_arrow(name.into(), array.boxed()).unwrap();
            Ok(J::Series(series))
        }
        7 => {
            let array_vec = array_vec.to_vec();
            let new_ptr: *const i64 = array_vec.as_ptr().cast();
            let slice = unsafe { core::slice::from_raw_parts(new_ptr, array_vec.len() / k_size) };
            let bitmap = Bitmap::from_iter(
                slice
                    .into_iter()
                    .map(|s| *s > i64::MIN + 1 && *s < i64::MAX),
            );
            let mut array = Int64Array::from_slice(slice);
            array.set_validity(Some(bitmap));
            series = Series::from_arrow(name.into(), array.boxed()).unwrap();
            Ok(J::Series(series))
        }
        8 => {
            let array_vec = array_vec.to_vec();
            let new_ptr: *const f32 = array_vec.as_ptr().cast();
            let slice = unsafe { core::slice::from_raw_parts(new_ptr, array_vec.len() / k_size) };
            let bitmap = Bitmap::from_iter(slice.into_iter().map(|s| !f32::is_nan(*s)));
            let mut array = Float32Array::from_slice(slice);
            array.set_validity(Some(bitmap));
            series = Series::from_arrow(name.into(), array.boxed()).unwrap();
            Ok(J::Series(series))
        }
        9 => {
            let array_vec = array_vec.to_vec();
            let new_ptr: *const f64 = array_vec.as_ptr().cast();
            let slice = unsafe { core::slice::from_raw_parts(new_ptr, array_vec.len() / k_size) };
            let bitmap = Bitmap::from_iter(slice.into_iter().map(|s| !f64::is_nan(*s)));
            let mut array = Float64Array::from_slice(slice);
            array.set_validity(Some(bitmap));
            series = Series::from_arrow(name.into(), array.boxed()).unwrap();
            Ok(J::Series(series))
        }
        10 => {
            if as_column {
                let offsets: Vec<i64> = (0..=length as i64).collect();
                array_box = Utf8Array::<i64>::new(
                    ArrowDataType::LargeUtf8,
                    OffsetsBuffer::try_from(offsets).unwrap(),
                    Buffer::from(array_vec.to_vec()),
                    None,
                )
                .boxed();
                series = Series::from_arrow(name.into(), array_box).unwrap();
                Ok(J::Series(series))
            } else {
                Ok(J::String(String::from_utf8_lossy(array_vec).to_string()))
            }
        }
        11 => {
            let mut v8: Vec<u8> = Vec::with_capacity(vec.len() - length);
            let mut offsets: Vec<i64> = vec![0i64; length + 1];
            let mut i = 0;
            let mut start_pos = pos;
            while i < length {
                if vec[pos] == 0 {
                    v8.write(&vec[start_pos..pos]).unwrap();
                    offsets[i + 1] = offsets[i] + (pos - start_pos) as i64;
                    start_pos = pos + 1;
                    i += 1;
                }
                pos += 1;
            }
            array_box = Utf8Array::<i64>::new(
                ArrowDataType::LargeUtf8,
                OffsetsBuffer::try_from(offsets).unwrap(),
                Buffer::from(v8),
                None,
            )
            .boxed();
            series = Series::from_arrow(name.into(), array_box).unwrap();
            if as_column {
                series = series
                    .cast(&PolarsDataType::Categorical(
                        None,
                        CategoricalOrdering::Lexical,
                    ))
                    .unwrap();
            }
            return Ok(J::Series(series));
        }
        12 => {
            let array_vec = array_vec.to_vec();
            let new_ptr: *const i64 = array_vec.as_ptr().cast();
            let slice = unsafe { core::slice::from_raw_parts(new_ptr, array_vec.len() / k_size) };
            let slice = slice
                .into_iter()
                .map(|ns| match *ns {
                    i64::MIN => *ns,
                    _ => ns.saturating_add(NANOS_DIFF),
                })
                .collect::<Vec<_>>();
            let bitmap = Bitmap::from_iter(slice.iter().map(|s| *s != i64::MIN));
            let array = PrimitiveArray::new(
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
                slice.into(),
                Some(bitmap),
            );
            array_box = array.boxed();
            series = Series::from_arrow(name.into(), array_box).unwrap();
            Ok(J::Series(series))
        }
        14 => {
            let array_vec = array_vec.to_vec();
            let new_ptr: *const i32 = array_vec.as_ptr().cast();
            let slice = unsafe { core::slice::from_raw_parts(new_ptr, array_vec.len() / k_size) };
            let bitmap = Bitmap::from_iter(slice.iter().map(|s| *s != i32::MIN));
            let slice = slice
                .into_iter()
                .map(|day| {
                    let day = day.saturating_add(10957);
                    max(min(day, 95026601), -96465658)
                })
                .collect::<Vec<_>>();
            let array = PrimitiveArray::new(ArrowDataType::Date32, slice.into(), Some(bitmap));
            array_box = array.boxed();
            series = Series::from_arrow(name.into(), array_box).unwrap();
            Ok(J::Series(series))
        }
        15 => {
            let array_vec = array_vec.to_vec();
            let new_ptr: *const f64 = array_vec.as_ptr().cast();
            let slice = unsafe { core::slice::from_raw_parts(new_ptr, array_vec.len() / k_size) };
            let slice = slice
                .into_iter()
                .map(|t| {
                    if t.is_nan() {
                        i64::MIN
                    } else if t.is_finite() {
                        (*t * MS_PER_DAY).round() as i64 * 1000000 + NANOS_DIFF
                    } else if t.is_sign_positive() {
                        i64::MAX
                    } else {
                        i64::MIN + 1
                    }
                })
                .collect::<Vec<_>>();
            let bitmap = Bitmap::from_iter(slice.iter().map(|s| *s != i64::MIN));
            let array = PrimitiveArray::new(
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
                slice.into(),
                Some(bitmap),
            );
            array_box = array.boxed();
            series = Series::from_arrow(name.into(), array_box).unwrap();
            Ok(J::Series(series))
        }
        // timespan
        16 => {
            let array_vec = array_vec.to_vec();
            let new_ptr: *const i64 = array_vec.as_ptr().cast();
            let slice = unsafe { core::slice::from_raw_parts(new_ptr, array_vec.len() / k_size) };
            let bitmap = Bitmap::from_iter(slice.iter().map(|s| *s != i64::MIN));
            let array = PrimitiveArray::new(
                ArrowDataType::Duration(TimeUnit::Nanosecond),
                slice.to_vec().into(),
                Some(bitmap),
            );
            array_box = array.boxed();
            series = Series::from_arrow(name.into(), array_box).unwrap();
            Ok(J::Series(series))
        }
        // minutes, seconds, time
        17 | 18 | 19 => {
            let array_vec = array_vec.to_vec();
            let new_ptr: *const i32 = array_vec.as_ptr().cast();
            let slice = unsafe { core::slice::from_raw_parts(new_ptr, array_vec.len() / k_size) };
            let bitmap = Bitmap::from_iter(slice.iter().map(|s| *s != i32::MIN));
            let multiplier = if k_type == 17 {
                60000_000_000
            } else if k_type == 18 {
                1000_000_000
            } else {
                1_000_000
            };

            let slice = slice
                .into_iter()
                .map(|t| {
                    let ns = (*t as i64).saturating_mul(multiplier);
                    return min(NANOS_PER_DAY - 1, max(0, ns));
                })
                .collect::<Vec<_>>();

            let array = PrimitiveArray::new(
                ArrowDataType::Time64(TimeUnit::Nanosecond),
                slice.into(),
                Some(bitmap),
            );
            array_box = array.boxed();
            series = Series::from_arrow(name.into(), array_box).unwrap();
            Ok(J::Series(series))
        }
        _ => Err(KolaError::NotSupportedKListErr(k_type)),
    }
}

fn new_empty_series(k_type: u8) -> Result<J, KolaError> {
    let name = K_TYPE_NAME[k_type as usize];
    let series = match k_type {
        0 => Series::new_empty(name.into(), &PolarsDataType::Null),
        1 => Series::new_empty(name.into(), &PolarsDataType::Boolean),
        2 => Series::new_empty(name.into(), &PolarsDataType::Binary),
        4 | 10 => Series::new_empty(name.into(), &PolarsDataType::String),
        5 => Series::new_empty(name.into(), &PolarsDataType::Int16),
        6 => Series::new_empty(name.into(), &PolarsDataType::Int32),
        7 => Series::new_empty(name.into(), &PolarsDataType::Int64),
        8 => Series::new_empty(name.into(), &PolarsDataType::Float32),
        9 => Series::new_empty(name.into(), &PolarsDataType::Float64),
        11 => Series::new_empty(
            name.into(),
            &PolarsDataType::Categorical(None, CategoricalOrdering::Lexical),
        ),
        12 | 15 => Series::new_empty(
            name.into(),
            &PolarsDataType::Datetime(PolarTimeUnit::Nanoseconds, None),
        ),
        14 => Series::new_empty(name.into(), &PolarsDataType::Date),
        16 => Series::new_empty(
            name.into(),
            &PolarsDataType::Duration(PolarTimeUnit::Nanoseconds),
        ),
        17 | 18 | 19 => Series::new_empty(name.into(), &PolarsDataType::Time),
        _ => return Err(KolaError::NotSupportedKListErr(k_type)),
    };
    Ok(J::Series(series))
}

fn deserialize_nested_array(vec: &[u8]) -> Result<J, KolaError> {
    let mut pos: usize = 1;
    let length = u32::from_le_bytes(vec[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;
    let k_type = vec[pos];
    let k_size = K_TYPE_SIZE[k_type as usize];
    let name = K_TYPE_NAME[k_type as usize];
    let mut offsets: Vec<i64> = vec![0i64; length + 1];
    let mut v8 = Vec::with_capacity(length * k_size);
    // bool, byte, short, int, long, real, float, string
    if let 1 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 12 = k_type {
        for i in 0..length {
            pos += 2;
            let sub_length = i32::from_le_bytes(vec[pos..pos + 4].try_into().unwrap());
            offsets[i + 1] = sub_length as i64 + offsets[i];
            pos += 4;
            v8.write(&vec[pos..pos + k_size * sub_length as usize])
                .unwrap();
            pos += k_size * sub_length as usize;
        }
    } else if let 11 = k_type {
        let mut sub_offsets: Vec<i64> = Vec::new();
        sub_offsets.push(0);
        v8 = Vec::with_capacity(vec.len());
        for i in 0..length {
            pos += 2;
            let sub_length = i32::from_le_bytes(vec[pos..pos + 4].try_into().unwrap());
            offsets[i + 1] = sub_length as i64 + offsets[i];
            pos += 4;
            for _ in 0..sub_length {
                let mut k = 0;
                while vec[pos + k] != 0 {
                    k += 1;
                }
                // exclude last 0x00, as sym ends with 0x00
                v8.write(&vec[pos..pos + k]).unwrap();
                sub_offsets.push(sub_offsets.last().unwrap() + k as i64);
                pos += k + 1;
            }
        }
        let array_box = Utf8Array::<i64>::new(
            ArrowDataType::LargeUtf8,
            OffsetsBuffer::try_from(sub_offsets).unwrap(),
            Buffer::from(v8),
            None,
        )
        .boxed();

        let field = create_field(k_type, "symbol").unwrap();
        let offsets_buf = OffsetsBuffer::<i64>::try_from(offsets).unwrap();
        let list_array = ListArray::<i32>::new(
            ArrowDataType::List(Box::new(field)),
            OffsetsBuffer::<i32>::try_from(&offsets_buf).unwrap(),
            array_box,
            None,
        );
        let series = Series::from_arrow(name.into(), list_array.boxed()).unwrap();
        let series = series
            .cast(&PolarsDataType::List(
                PolarsDataType::Categorical(None, CategoricalOrdering::Lexical).boxed(),
            ))
            .unwrap();
        return Ok(J::Series(series));
    } else {
        return Err(KolaError::NotSupportedKNestedListErr(k_type));
    }
    let offsets_buf = OffsetsBuffer::<i64>::try_from(offsets).unwrap();
    match k_type {
        1 | 4 | 5 | 6 | 7 | 8 | 9 | 12 => {
            let field: Field;
            let list_array: ListArray<i32>;
            let array_box: Box<dyn Array>;
            let k_size = K_TYPE_SIZE[k_type as usize];
            if k_type == 1 {
                array_box =
                    BooleanArray::from_slice(v8.into_iter().map(|u| u == 1).collect::<Vec<_>>())
                        .boxed();
                field = create_field(k_type, "boolean").unwrap();
            } else if k_type == 4 {
                let bytes: Buffer<u8> = v8.to_vec().into();
                array_box = UInt8Array::from_slice(bytes.as_slice()).boxed();
                field = create_field(k_type, "byte").unwrap();
            } else if k_type == 5 {
                let new_ptr: *const i16 = v8.as_ptr().cast();
                let slice = unsafe { core::slice::from_raw_parts(new_ptr, v8.len() / k_size) };
                let bitmap = Bitmap::from_iter(slice.iter().map(|s| *s != i16::MIN));
                let mut array = Int16Array::from_slice(slice);
                array.set_validity(Some(bitmap));
                array_box = array.boxed();
                field = create_field(k_type, "short").unwrap();
            } else if k_type == 6 {
                let new_ptr: *const i32 = v8.as_ptr().cast();
                let slice = unsafe { core::slice::from_raw_parts(new_ptr, v8.len() / k_size) };
                let bitmap = Bitmap::from_iter(slice.iter().map(|s| *s != i32::MIN));
                let mut array = Int32Array::from_slice(slice);
                array.set_validity(Some(bitmap));
                array_box = array.boxed();
                field = create_field(k_type, "int").unwrap();
            } else if k_type == 7 {
                let new_ptr: *const i64 = v8.as_ptr().cast();
                let slice = unsafe { core::slice::from_raw_parts(new_ptr, v8.len() / k_size) };
                let bitmap = Bitmap::from_iter(slice.iter().map(|s| *s != i64::MIN));
                let mut array = Int64Array::from_slice(slice);
                array.set_validity(Some(bitmap));
                array_box = array.boxed();
                field = create_field(k_type, "long").unwrap();
            } else if k_type == 8 {
                let new_ptr: *const f32 = v8.as_ptr().cast();
                let slice = unsafe { core::slice::from_raw_parts(new_ptr, v8.len() / k_size) };
                let bitmap = Bitmap::from_iter(slice.into_iter().map(|s| !f32::is_nan(*s)));
                let mut array = Float32Array::from_slice(slice);
                array.set_validity(Some(bitmap));
                array_box = array.boxed();
                field = create_field(k_type, "real").unwrap();
            } else if k_type == 9 {
                let new_ptr: *const f64 = v8.as_ptr().cast();
                let slice = unsafe { core::slice::from_raw_parts(new_ptr, v8.len() / k_size) };
                let bitmap = Bitmap::from_iter(slice.into_iter().map(|s| !f64::is_nan(*s)));
                let mut array = Float64Array::from_slice(slice);
                array.set_validity(Some(bitmap));
                array_box = array.boxed();
                field = create_field(k_type, "float").unwrap();
            } else if k_type == 12 {
                let new_ptr: *mut i64 = v8.as_mut_ptr().cast();
                let slice = unsafe { core::slice::from_raw_parts(new_ptr, v8.len() / k_size) };
                let slice = slice
                    .into_iter()
                    .map(|ns| match *ns {
                        i64::MIN => *ns,
                        _ => ns.saturating_add(NANOS_DIFF),
                    })
                    .collect::<Vec<_>>();
                let bitmap = Bitmap::from_iter(slice.iter().map(|s| *s != i64::MIN));
                let array = PrimitiveArray::new(
                    ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
                    slice.into(),
                    Some(bitmap),
                );
                array_box = array.boxed();
                field = create_field(k_type, "timestamp").unwrap();
            } else {
                unreachable!()
            }

            list_array = ListArray::<i32>::new(
                ArrowDataType::List(Box::new(field)),
                OffsetsBuffer::<i32>::try_from(&offsets_buf).unwrap(),
                array_box,
                None,
            );

            Ok(J::Series(
                Series::from_arrow(name.into(), list_array.boxed()).unwrap(),
            ))
        }
        10 => {
            let array_box = Utf8Array::<i64>::new(
                ArrowDataType::LargeUtf8,
                OffsetsBuffer::<i64>::try_from(offsets_buf).unwrap(),
                Buffer::from(v8),
                None,
            )
            .boxed();
            Ok(J::Series(
                Series::from_arrow(name.into(), array_box).unwrap(),
            ))
        }
        _ => unreachable!(),
    }
}

fn create_datetime(ns: i64) -> DateTime<Utc> {
    match DateTime::from_timestamp(ns / 1000000000, (ns % 1000000000) as u32) {
        Some(dt) => dt,
        None => {
            if ns > 0 {
                DateTime::from_timestamp(9223372036, 854775804).unwrap()
            } else {
                DateTime::from_timestamp(0, 0).unwrap()
            }
        }
    }
}

pub fn decompress(vec: &[u8], de_vec: &mut [u8], start_pos: usize) {
    let mut d_pos: usize = 0;
    // skip decompressed msg length
    let mut x_pos: usize = 4;
    let mut c_pos: usize = start_pos;
    let mut x = [0usize; 256];
    let mut n: u8 = 0;

    let mut i: u8 = 0;
    while d_pos < de_vec.len() {
        if i == 0 {
            n = vec[c_pos];
            c_pos += 1;
            i = 1;
        }
        let mut r: usize = 0;
        if n & i != 0 {
            let s = x[vec[c_pos] as usize];
            c_pos += 1;
            r = vec[c_pos] as usize;
            c_pos += 1;
            for j in 0..r + 2 {
                de_vec[d_pos + j] = de_vec[s + j]
            }
            d_pos += 2;
        } else {
            de_vec[d_pos] = vec[c_pos];
            d_pos += 1;
            c_pos += 1;
        }

        for i in x_pos..d_pos - 1 {
            x[(de_vec[i] ^ de_vec[i + 1]) as usize] = i
        }

        x_pos = d_pos - 1;

        if n & i != 0 {
            d_pos += r;
            x_pos = d_pos;
        }
        i <<= 1
    }
}

pub fn compress(vec: Vec<u8>) -> Vec<u8> {
    if vec.len() < 2000 {
        return vec;
    } else {
        let mut c_vec = vec![0u8; vec.len() / 2];
        // compressed bytes start position
        let mut c_pos: usize;
        if vec.len() > 4294967295 {
            c_pos = 16;
            c_vec[2] = 2;
            for i in 3..8 {
                c_vec[i + 8] = vec[i]
            }
        } else {
            c_pos = 12;
            c_vec[2] = 1;
            // copy raw vec length
            for i in 4..8 {
                c_vec[i + 4] = vec[i]
            }
        }
        let mut n_pos: usize = c_pos;
        let mut o_pos: usize = 8;
        let mut x = [0usize; 256];

        let mut px: u8 = 0;
        let mut n: u8 = 0;
        let mut p_pos: usize = 0;

        let mut i: u8 = 0;

        while o_pos < vec.len() {
            if i == 0 {
                if c_pos > c_vec.len() - 17 {
                    return vec;
                }
                i = 1;
                c_vec[n_pos] = n;
                n_pos = c_pos;
                c_pos += 1;
                n = 0;
            }
            let mut skip = vec.len() - o_pos < 3;
            let mut x_pos: usize = 0;
            let mut cx: u8 = 0;
            if !skip {
                cx = vec[o_pos] ^ vec[o_pos + 1];
                x_pos = x[cx as usize];
                skip = x_pos == 0 || vec[o_pos] != vec[x_pos];
            }

            if p_pos > 0 {
                x[px as usize] = p_pos;
                p_pos = 0;
            }

            if skip {
                px = cx;
                p_pos = o_pos;
                c_vec[c_pos] = vec[o_pos];
                c_pos += 1;
                o_pos += 1;
            } else {
                x[cx as usize] = o_pos;
                n |= i;
                x_pos += 2;
                o_pos += 2;
                let s = o_pos;
                let max_index = min(o_pos + 255, vec.len());
                while o_pos < max_index && vec[x_pos] == vec[o_pos] {
                    o_pos += 1;
                    x_pos += 1;
                }
                c_vec[c_pos] = cx;
                c_pos += 1;
                c_vec[c_pos] = (o_pos - s) as u8;
                c_pos += 1;
            }

            i <<= 1;
        }
        c_vec[n_pos] = n;
        c_vec[0] = vec[0];
        c_vec[1] = vec[1];
        let c_len = u32::to_le_bytes(c_pos as u32);
        for i in 0..4 {
            c_vec[i + 4] = c_len[i]
        }
        c_vec[3] = (c_pos >> 32) as u8;
        c_vec.resize(c_pos, 0u8);
        return c_vec;
    }
}

pub fn serialize(k: &J) -> Result<Vec<u8>, KolaError> {
    let k_length = k.len()?;
    let mut vec: Vec<u8>;
    match k {
        J::Boolean(k) => {
            vec = Vec::with_capacity(k_length);
            vec.write(&[255, (*k as u8)]).unwrap();
        }
        J::Guid(k) => {
            vec = Vec::with_capacity(k_length);
            vec.write(&[254u8]).unwrap();
            vec.write(k.as_bytes()).unwrap();
        }
        J::U8(k) => {
            vec = Vec::with_capacity(k_length);
            vec.write(&[252, *k]).unwrap();
        }
        J::I16(k) => {
            vec = Vec::with_capacity(k_length);
            vec.write(&[251]).unwrap();
            vec.write(&NativeType::to_le_bytes(k)).unwrap();
        }
        J::I32(k) => {
            vec = Vec::with_capacity(k_length);
            vec.write(&[250]).unwrap();
            vec.write(&NativeType::to_le_bytes(k)).unwrap();
        }
        J::I64(k) => {
            vec = Vec::with_capacity(k_length);
            vec.write(&[249]).unwrap();
            vec.write(&NativeType::to_le_bytes(k)).unwrap();
        }
        J::F32(k) => {
            vec = Vec::with_capacity(k_length);
            vec.write(&[248]).unwrap();
            vec.write(&NativeType::to_le_bytes(k)).unwrap();
        }
        J::F64(k) => {
            vec = Vec::with_capacity(k_length);
            vec.write(&[247]).unwrap();
            vec.write(&NativeType::to_le_bytes(k)).unwrap();
        }
        J::Char(k) => {
            vec = Vec::with_capacity(k_length);
            vec.write(&[246, *k]).unwrap();
        }
        J::Symbol(k) => {
            vec = Vec::with_capacity(k_length);
            vec.write(&[245]).unwrap();
            vec.write(k.as_bytes()).unwrap();
            vec.write(&[0]).unwrap();
        }
        J::String(k) => {
            vec = Vec::with_capacity(k_length);
            vec.write(&[10, 0]).unwrap();
            vec.write(&(k.len() as u32).to_le_bytes()).unwrap();
            vec.write(k.as_bytes()).unwrap();
        }
        // to timestamp
        J::DateTime(k) => {
            vec = Vec::with_capacity(k_length);
            vec.write(&[244]).unwrap();
            let ns = match k.timestamp_nanos_opt() {
                Some(ns) => ns.saturating_sub(NANOS_DIFF),
                _ => i64::MIN,
            };
            vec.write(&ns.to_le_bytes()).unwrap();
        }
        // to date
        J::Date(k) => {
            vec = Vec::with_capacity(k_length);
            vec.write(&[242]).unwrap();
            let days = k.num_days_from_ce().saturating_sub(DAY_DIFF);
            vec.write(&days.to_le_bytes()).unwrap();
        }
        // to time
        J::Time(k) => {
            vec = Vec::with_capacity(k_length);
            vec.write(&[237]).unwrap();
            let milliseconds = k.num_seconds_from_midnight() * 1000 + k.nanosecond() / 1000000;
            vec.write(&(milliseconds as i32).to_le_bytes()).unwrap();
        }
        // to timespan
        J::Duration(k) => {
            vec = Vec::with_capacity(k_length);
            vec.write(&[240]).unwrap();
            let ns = k.num_nanoseconds();
            vec.write(&(ns.unwrap_or(i64::MIN)).to_le_bytes()).unwrap();
        }
        J::MixedList(l) => {
            vec = Vec::with_capacity(k_length);
            vec.write(&[0, 0]).unwrap();
            vec.write(&(l.len() as u32).to_le_bytes()).unwrap();
            for atom in l.iter() {
                vec.write(&serialize(atom)?).unwrap();
            }
        }
        // to list
        J::Series(k) => {
            vec = serialize_series(k, k_length)?;
        }
        // to table
        J::DataFrame(k) => {
            vec = Vec::with_capacity(k_length);
            let column_names = k.get_column_names();
            let column_count = column_names.len() as i32;
            vec.write(&[98, 0, 99, 11, 0]).unwrap();
            vec.write(&column_count.to_le_bytes()).unwrap();
            column_names.into_iter().for_each(|s| {
                vec.write(s.as_bytes()).unwrap();
                vec.write(&[0]).unwrap();
            });
            vec.write(&[0, 0]).unwrap();
            let columns = k.get_columns();
            vec.write(&column_count.to_le_bytes()).unwrap();
            let vectors = columns
                .into_par_iter()
                .map(|s| {
                    serialize_series(
                        s.as_materialized_series(),
                        get_series_len(s.as_materialized_series()).unwrap(),
                    )
                })
                .collect::<Result<Vec<Vec<u8>>, KolaError>>()?;
            vectors.into_iter().for_each(|v| {
                vec.write(&v).unwrap();
            });
        }
        // to (::)
        J::Null => {
            vec = Vec::with_capacity(k_length);
            vec.write(&[101, 0]).unwrap();
        }
        J::Dict(dict) => {
            let keys = dict.keys();
            let length = keys.len() as i32;
            if length == 0 {
                return Err(KolaError::Err("Not supported empty dictionary".to_string()));
            };
            vec = Vec::with_capacity(k_length);
            vec.write(&[99, 11, 0]).unwrap();
            vec.write(&length.to_le_bytes()).unwrap();
            keys.into_iter().for_each(|k| {
                vec.write(k.as_bytes()).unwrap();
                vec.write(&[0]).unwrap();
            });
            vec.write(&[0, 0]).unwrap();
            vec.write(&length.to_le_bytes()).unwrap();
            dict.values().into_iter().for_each(|v| {
                vec.write(&serialize(v).unwrap()).unwrap();
            });
        }
    };
    Ok(vec)
}

fn serialize_series(series: &Series, k_length: usize) -> Result<Vec<u8>, KolaError> {
    let mut vec: Vec<u8> = Vec::with_capacity(k_length);
    let k_length = series.len();
    if k_length > i32::MAX as usize {
        return Err(KolaError::OverLengthErr());
    }
    let k_size: usize;
    match series.dtype() {
        PolarsDataType::Boolean => {
            vec.write(&[1, 0]).unwrap();
            vec.write(&(k_length as i32).to_le_bytes()).unwrap();
            let ptr = series.to_physical_repr();
            let chunks = &ptr.bool().unwrap().chunks();
            chunks.into_iter().for_each(|array| {
                let array = unsafe {
                    array
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap_unchecked()
                        .values()
                };
                array.iter().for_each(|b| {
                    if b {
                        vec.write(&[1u8]).unwrap();
                    } else {
                        vec.write(&[0u8]).unwrap();
                    }
                });
            })
        }
        PolarsDataType::UInt8 => {
            k_size = 1;
            vec.write(&[4, 0]).unwrap();
            vec.write(&(k_length as i32).to_le_bytes()).unwrap();
            let ptr = series.to_physical_repr();
            let chunks = &ptr.u8().unwrap().chunks();
            chunks.into_iter().for_each(|array| {
                let array = unsafe {
                    array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<u8>>()
                        .unwrap_unchecked()
                        .values()
                };
                let v8 = unsafe { core::slice::from_raw_parts(array.as_ptr(), k_length / k_size) };
                vec.write(v8).unwrap();
            })
        }
        PolarsDataType::Int16 => {
            k_size = 2;
            vec.write(&[5, 0]).unwrap();
            vec.write(&(k_length as i32).to_le_bytes()).unwrap();
            let chunks = series.i16().unwrap();
            let chunks = if chunks.null_count() > 0 {
                chunks.fill_null_with_values(i16::MIN).unwrap()
            } else {
                chunks.clone()
            };
            chunks.chunks().into_iter().for_each(|array| {
                let array = unsafe {
                    array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<i16>>()
                        .unwrap_unchecked()
                        .values()
                };
                let v8 = unsafe {
                    core::slice::from_raw_parts(array.as_ptr().cast(), k_length * k_size)
                };
                vec.write(v8).unwrap();
            })
        }
        PolarsDataType::Int32 => {
            k_size = 4;
            vec.write(&[6, 0]).unwrap();
            vec.write(&(k_length as i32).to_le_bytes()).unwrap();
            let chunks = series.i32().unwrap();
            let chunks = if chunks.null_count() > 0 {
                chunks.fill_null_with_values(i32::MIN).unwrap()
            } else {
                chunks.clone()
            };
            chunks.chunks().into_iter().for_each(|array| {
                let array = unsafe {
                    array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<i32>>()
                        .unwrap_unchecked()
                        .values()
                };
                let v8 = unsafe {
                    core::slice::from_raw_parts(array.as_ptr().cast(), k_length * k_size)
                };
                vec.write(v8).unwrap();
            })
        }
        PolarsDataType::Int64 => {
            k_size = 8;
            vec.write(&[7, 0]).unwrap();
            vec.write(&(k_length as i32).to_le_bytes()).unwrap();
            let new_series: Series;
            let ptr = if series.null_count() > 0 {
                new_series = series
                    .i64()
                    .unwrap()
                    .fill_null_with_values(i64::MIN)
                    .unwrap()
                    .into_series();
                new_series.to_physical_repr()
            } else {
                series.to_physical_repr()
            };
            let chunks = &ptr.i64().unwrap().chunks();
            chunks.into_iter().for_each(|array| {
                let array = unsafe {
                    array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<i64>>()
                        .unwrap_unchecked()
                        .values()
                };
                let v8 = unsafe {
                    core::slice::from_raw_parts(array.as_ptr().cast(), k_length * k_size)
                };
                vec.write(v8).unwrap();
            })
        }
        PolarsDataType::Float32 => {
            k_size = 4;
            vec.write(&[8, 0]).unwrap();
            vec.write(&(k_length as i32).to_le_bytes()).unwrap();
            let new_series: Series;
            let ptr = if series.null_count() > 0 {
                new_series = series
                    .f32()
                    .unwrap()
                    .fill_null_with_values(f32::NAN)
                    .unwrap()
                    .into_series();
                new_series.to_physical_repr()
            } else {
                series.to_physical_repr()
            };
            let chunks = &ptr.f32().unwrap().chunks();
            chunks.into_iter().for_each(|array| {
                let array = unsafe {
                    array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<f32>>()
                        .unwrap_unchecked()
                        .values()
                };
                let v8 = unsafe {
                    core::slice::from_raw_parts(array.as_ptr().cast(), k_length * k_size)
                };
                vec.write(v8).unwrap();
            })
        }
        PolarsDataType::Float64 => {
            k_size = 8;
            vec.write(&[9, 0]).unwrap();
            vec.write(&(k_length as i32).to_le_bytes()).unwrap();
            let new_series: Series;
            let ptr = if series.null_count() > 0 {
                new_series = series
                    .f64()
                    .unwrap()
                    .fill_null_with_values(f64::NAN)
                    .unwrap()
                    .into_series();
                new_series.to_physical_repr()
            } else {
                series.to_physical_repr()
            };
            let chunks = &ptr.f64().unwrap().chunks();
            chunks.into_iter().for_each(|array| {
                let array = unsafe {
                    array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<f64>>()
                        .unwrap_unchecked()
                        .values()
                };
                let v8 = unsafe {
                    core::slice::from_raw_parts(array.as_ptr().cast(), k_length * k_size)
                };
                vec.write(v8).unwrap();
            })
        }
        PolarsDataType::String => {
            vec.write(&[0, 0]).unwrap();
            vec.write(&(k_length as i32).to_le_bytes()).unwrap();
            let ptr = series.to_physical_repr();
            let array = ptr.str().unwrap();
            array.chunks().into_iter().for_each(|arr| {
                let arr = &**arr;
                let arr = unsafe { &*(arr as *const dyn Array as *const Utf8ViewArray) };
                arr.into_iter().for_each(|s| {
                    vec.write(&[10, 0]).unwrap();
                    match s {
                        Some(s) => {
                            vec.write(&(s.len() as u32).to_le_bytes()).unwrap();
                            let v8 =
                                unsafe { core::slice::from_raw_parts(s.as_ptr().cast(), s.len()) };
                            vec.write(v8).unwrap();
                        }
                        None => {
                            vec.write(&[0, 0, 0, 0]).unwrap();
                        }
                    }
                });
            });
        }
        PolarsDataType::Date => {
            // max date - 95026601
            k_size = 4;
            vec.write(&[14, 0]).unwrap();
            vec.write(&(k_length as i32).to_le_bytes()).unwrap();
            let chunks = series.cast(&PolarsDataType::Int32).unwrap();
            let chunks = chunks.i32().unwrap();
            let chunks = if chunks.null_count() > 0 {
                chunks.fill_null_with_values(i32::MIN).unwrap()
            } else {
                chunks.clone()
            };
            chunks.chunks().into_iter().for_each(|array| {
                let buffer = unsafe {
                    array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<i32>>()
                        .unwrap_unchecked()
                        .values()
                };
                let array: Vec<i32> = buffer
                    .as_slice()
                    .iter()
                    .map(|d| {
                        if *d == i32::MIN {
                            *d
                        } else {
                            d.saturating_sub(10957)
                        }
                    })
                    .collect();
                let v8 = unsafe {
                    core::slice::from_raw_parts(array.as_ptr().cast(), k_length * k_size)
                };
                vec.write(v8).unwrap();
            })
        }
        PolarsDataType::Datetime(unit, _) => {
            k_size = 8;
            vec.write(&[12, 0]).unwrap();
            vec.write(&(k_length as i32).to_le_bytes()).unwrap();
            let new_series: Series;
            let ptr = if series.null_count() > 0 {
                new_series = series
                    .datetime()
                    .unwrap()
                    .fill_null_with_values(i64::MIN)
                    .unwrap()
                    .into_series();
                new_series.to_physical_repr()
            } else {
                series.to_physical_repr()
            };
            let chunks = &ptr.i64().unwrap().chunks();
            chunks.into_iter().for_each(|array| {
                let buffer = unsafe {
                    array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<i64>>()
                        .unwrap_unchecked()
                        .values()
                };
                let multplier = match unit {
                    PolarTimeUnit::Nanoseconds => 1,
                    PolarTimeUnit::Microseconds => 1000,
                    PolarTimeUnit::Milliseconds => 1000000,
                };
                let array: Vec<i64> = buffer
                    .as_slice()
                    .iter()
                    .map(|d| {
                        if *d == i64::MIN {
                            *d
                        } else {
                            d.saturating_mul(multplier).saturating_sub(NANOS_DIFF)
                        }
                    })
                    .collect();
                let v8 = unsafe {
                    core::slice::from_raw_parts(array.as_ptr().cast(), k_length * k_size)
                };
                vec.write(v8).unwrap();
            })
        }
        PolarsDataType::Duration(_) => {
            k_size = 8;
            vec.write(&[16, 0]).unwrap();
            vec.write(&(k_length as i32).to_le_bytes()).unwrap();
            let new_series: Series;
            let ptr = if series.null_count() > 0 {
                new_series = series
                    .duration()
                    .unwrap()
                    .fill_null_with_values(i64::MIN)
                    .unwrap()
                    .into_series();
                new_series.to_physical_repr()
            } else {
                series.to_physical_repr()
            };
            let chunks = &ptr.i64().unwrap().chunks();
            chunks.into_iter().for_each(|array| {
                let array = unsafe {
                    array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<i64>>()
                        .unwrap_unchecked()
                        .values()
                };
                let v8 = unsafe {
                    core::slice::from_raw_parts(array.as_ptr().cast(), k_length * k_size)
                };
                vec.write(v8).unwrap();
            })
        }
        PolarsDataType::Time => {
            k_size = 4;
            vec.write(&[19, 0]).unwrap();
            vec.write(&(k_length as i32).to_le_bytes()).unwrap();
            let new_series: Series;
            let ptr = if series.null_count() > 0 {
                new_series = series
                    .time()
                    .unwrap()
                    .fill_null_with_values(i64::MIN)
                    .unwrap()
                    .into_series();
                new_series.to_physical_repr()
            } else {
                series.to_physical_repr()
            };
            let chunks = &ptr.i64().unwrap().chunks();
            chunks.into_iter().for_each(|array| {
                let buffer = unsafe {
                    array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<i64>>()
                        .unwrap_unchecked()
                        .values()
                };
                let array: Vec<i32> = buffer
                    .as_slice()
                    .iter()
                    .map(|d| {
                        if *d == i64::MIN {
                            i32::MIN
                        } else {
                            (d / 1000_000) as i32
                        }
                    })
                    .collect();

                let v8 = unsafe {
                    core::slice::from_raw_parts(array.as_ptr().cast(), k_length * k_size)
                };
                vec.write(v8).unwrap();
            })
        }
        PolarsDataType::Array(data_type, size) => {
            vec.write(&[0, 0]).unwrap();
            vec.write(&(k_length as i32).to_le_bytes()).unwrap();
            let array = unsafe {
                series.array().unwrap().chunks()[0]
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .unwrap_unchecked()
            };
            match data_type.as_ref() {
                PolarsDataType::Boolean => {
                    let array = unsafe {
                        array
                            .values()
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .unwrap_unchecked()
                            .values()
                    };
                    let len_vec = (*size as i32).to_le_bytes();
                    for (i, b) in array.iter().enumerate() {
                        if i % size == 0 {
                            vec.write(&[1, 0]).unwrap();
                            vec.write(&len_vec).unwrap();
                        }
                        if b {
                            vec.write(&[1u8]).unwrap();
                        } else {
                            unsafe { vec.set_len(vec.len() + 1) }
                        }
                    }
                }
                PolarsDataType::UInt8 => todo!(),
                PolarsDataType::Int16 => todo!(),
                PolarsDataType::Int32 => todo!(),
                PolarsDataType::Int64 => todo!(),
                PolarsDataType::Float32 => todo!(),
                PolarsDataType::Float64 => todo!(),
                _ => {
                    return Err(KolaError::NotSupportedPolarsNestedListTypeErr(
                        data_type.as_ref().clone(),
                    ))
                }
            }
        }
        PolarsDataType::List(data_type) => {
            vec.write(&[0, 0]).unwrap();
            vec.write(&(k_length as i32).to_le_bytes()).unwrap();
            let list = unsafe {
                series.list().unwrap().chunks()[0]
                    .as_any()
                    .downcast_ref::<ListArray<i64>>()
                    .unwrap_unchecked()
            };
            let offsets = list.offsets().as_ref();
            match data_type.as_ref() {
                PolarsDataType::Boolean => {
                    let list = unsafe {
                        list.values()
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .unwrap_unchecked()
                            .values()
                    };
                    for i in 0..k_length {
                        let start_offset = offsets[i] as usize;
                        let end_offset = offsets[i + 1] as usize;
                        vec.write(&[1, 0]).unwrap();
                        vec.write(&((offsets[i + 1] - offsets[i]) as i32).to_le_bytes())
                            .unwrap();
                        for j in start_offset..end_offset {
                            if list.get_bit(j) {
                                vec.write(&[1u8]).unwrap();
                            } else {
                                unsafe { vec.set_len(vec.len() + 1) }
                            }
                        }
                    }
                }
                PolarsDataType::UInt8 => {
                    let list = unsafe {
                        list.values()
                            .as_any()
                            .downcast_ref::<UInt8Array>()
                            .unwrap_unchecked()
                            .values()
                            .as_ref()
                    };
                    for i in 0..k_length {
                        let start_offset = offsets[i] as usize;
                        let end_offset = offsets[i + 1] as usize;
                        vec.write(&[4, 0]).unwrap();
                        vec.write(&((offsets[i + 1] - offsets[i]) as i32).to_le_bytes())
                            .unwrap();
                        vec.write(&list[start_offset..end_offset]).unwrap();
                    }
                }
                PolarsDataType::Int16 => {
                    let k_type = 5u8;
                    let k_size = K_TYPE_SIZE[k_type as usize];
                    let array = unsafe {
                        list.values()
                            .as_any()
                            .downcast_ref::<Int16Array>()
                            .unwrap_unchecked()
                    };
                    let p_array: PrimitiveArray<i16>;
                    let array = if array.null_count() > 0 {
                        p_array = set_at_nulls(array, i16::MIN);
                        p_array.values()
                    } else {
                        array.values()
                    };
                    let v8: &[u8] = unsafe {
                        core::slice::from_raw_parts(array.as_ptr().cast(), k_length * k_size)
                    };
                    for i in 0..k_length {
                        let start_offset = k_size * offsets[i] as usize;
                        let end_offset = k_size * offsets[i + 1] as usize;
                        vec.write(&[k_type, 0]).unwrap();
                        vec.write(&((offsets[i + 1] - offsets[i]) as i32).to_le_bytes())
                            .unwrap();
                        vec.write(&v8[start_offset..end_offset]).unwrap();
                    }
                }
                PolarsDataType::Int32 => {
                    let k_type = 6u8;
                    let k_size = K_TYPE_SIZE[k_type as usize];
                    let array = unsafe {
                        list.values()
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap_unchecked()
                    };
                    let p_array: PrimitiveArray<i32>;
                    let array = if array.null_count() > 0 {
                        p_array = set_at_nulls(array, i32::MIN);
                        p_array.values()
                    } else {
                        array.values()
                    };
                    let v8: &[u8] = unsafe {
                        core::slice::from_raw_parts(array.as_ptr().cast(), k_length * k_size)
                    };
                    for i in 0..k_length {
                        let start_offset = k_size * offsets[i] as usize;
                        let end_offset = k_size * offsets[i + 1] as usize;
                        vec.write(&[k_type, 0]).unwrap();
                        vec.write(&((offsets[i + 1] - offsets[i]) as i32).to_le_bytes())
                            .unwrap();
                        vec.write(&v8[start_offset..end_offset]).unwrap();
                    }
                }
                PolarsDataType::Int64 => {
                    let k_type = 7u8;
                    let k_size = K_TYPE_SIZE[k_type as usize];
                    let array = unsafe {
                        list.values()
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap_unchecked()
                    };
                    let p_array: PrimitiveArray<i64>;
                    let array = if array.null_count() > 0 {
                        p_array = set_at_nulls(array, i64::MIN);
                        p_array.values()
                    } else {
                        array.values()
                    };
                    let v8: &[u8] = unsafe {
                        core::slice::from_raw_parts(array.as_ptr().cast(), k_length * k_size)
                    };
                    for i in 0..k_length {
                        let start_offset = k_size * offsets[i] as usize;
                        let end_offset = k_size * offsets[i + 1] as usize;
                        vec.write(&[k_type, 0]).unwrap();
                        vec.write(&((offsets[i + 1] - offsets[i]) as i32).to_le_bytes())
                            .unwrap();
                        vec.write(&v8[start_offset..end_offset]).unwrap();
                    }
                }
                PolarsDataType::Float32 => {
                    let k_type = 8u8;
                    let k_size = K_TYPE_SIZE[k_type as usize];
                    let array = unsafe {
                        list.values()
                            .as_any()
                            .downcast_ref::<Float32Array>()
                            .unwrap_unchecked()
                    };
                    let p_array: PrimitiveArray<f32>;
                    let array = if array.null_count() > 0 {
                        p_array = set_at_nulls(array, f32::NAN);
                        p_array.values()
                    } else {
                        array.values()
                    };
                    let v8: &[u8] = unsafe {
                        core::slice::from_raw_parts(array.as_ptr().cast(), k_length * k_size)
                    };
                    for i in 0..k_length {
                        let start_offset = k_size * offsets[i] as usize;
                        let end_offset = k_size * offsets[i + 1] as usize;
                        vec.write(&[k_type, 0]).unwrap();
                        vec.write(&((offsets[i + 1] - offsets[i]) as i32).to_le_bytes())
                            .unwrap();
                        vec.write(&v8[start_offset..end_offset]).unwrap();
                    }
                }
                PolarsDataType::Float64 => {
                    let k_type = 9u8;
                    let k_size = K_TYPE_SIZE[k_type as usize];
                    let array = unsafe {
                        list.values()
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .unwrap_unchecked()
                    };
                    let p_array: PrimitiveArray<f64>;
                    let array = if array.null_count() > 0 {
                        p_array = set_at_nulls(array, f64::NAN);
                        p_array.values()
                    } else {
                        array.values()
                    };
                    let v8: &[u8] = unsafe {
                        core::slice::from_raw_parts(array.as_ptr().cast(), k_length * k_size)
                    };
                    for i in 0..k_length {
                        let start_offset = k_size * offsets[i] as usize;
                        let end_offset = k_size * offsets[i + 1] as usize;
                        vec.write(&[k_type, 0]).unwrap();
                        vec.write(&((offsets[i + 1] - offsets[i]) as i32).to_le_bytes())
                            .unwrap();
                        vec.write(&v8[start_offset..end_offset]).unwrap();
                    }
                }
                _ => {
                    return Err(KolaError::NotSupportedPolarsNestedListTypeErr(
                        data_type.as_ref().clone(),
                    ))
                }
            }
        }
        PolarsDataType::Categorical(_, _) => {
            vec.write(&[11, 0]).unwrap();
            vec.write(&(k_length as i32).to_le_bytes()).unwrap();
            let cat = series.categorical().unwrap();
            cat.iter_str()
                .map(|s| {
                    if let Some(s) = s {
                        [s.as_bytes(), &[0u8]].concat()
                    } else {
                        vec![0u8]
                    }
                })
                .for_each(|v| {
                    vec.write(&v).unwrap();
                });
        }
        PolarsDataType::Binary => {
            vec.write(&[2, 0]).unwrap();
            vec.write(&(k_length as i32).to_le_bytes()).unwrap();
            let array = series.binary().unwrap();
            array.chunks().into_iter().for_each(|arr| {
                let arr = &**arr;
                let arr = unsafe { &*(arr as *const dyn Array as *const BinaryViewArray) };
                arr.into_iter().for_each(|b| match b {
                    Some(b) => {
                        vec.write(b).unwrap();
                    }
                    None => {
                        vec.write(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
                            .unwrap();
                    }
                });
            });
        }
        PolarsDataType::Null if k_length == 0 => {
            vec.write(&[0, 0, 0, 0, 0, 0]).unwrap();
        }
        _ => return Err(KolaError::NotSupportedSeriesTypeErr(series.dtype().clone())),
    }
    Ok(vec)
}

#[cfg(test)]
mod tests {
    use indexmap::IndexMap;
    use polars::{
        datatypes::CategoricalOrdering,
        prelude::{CompatLevel, NamedFrom},
    };
    use polars_arrow::{
        array::{BooleanArray, UInt8Array},
        offset::OffsetsBuffer,
    };

    use crate::serde6::*;

    #[test]
    fn decompress_msg() {
        let vec: Vec<u8> = [
            222, 7, 0, 0, 0, 1, 0, 208, 7, 0, 0, 1, 1, 255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 255,
            0, 255, 0, 255, 0, 197,
        ]
        .to_vec();
        let length = u32::from_le_bytes(vec[0..4].try_into().unwrap());
        let mut de_vec = vec![0; (length - 8) as usize];
        decompress(&vec, &mut de_vec, 4);
        let mut expected_vec = [1u8; 2006].to_vec();
        expected_vec[1] = 0;
        expected_vec[2] = 208;
        expected_vec[3] = 7;
        expected_vec[4] = 0;
        expected_vec[5] = 0;
        assert_eq!(de_vec, expected_vec);
    }

    #[test]
    fn compress_msg() {
        let mut vec = [0u8; 2014].to_vec();
        vec[0] = 1;
        vec[1] = 1;
        vec[4] = 222;
        vec[5] = 7;
        vec[8] = 1;
        vec[10] = 208;
        vec[11] = 7;
        let c_vec = compress(vec);
        let expected_vec: Vec<u8> = [
            1, 1, 1, 0, 36, 0, 0, 0, 222, 7, 0, 0, 192, 1, 0, 208, 7, 0, 0, 0, 255, 0, 255, 63, 0,
            255, 0, 255, 0, 255, 0, 255, 0, 255, 0, 199,
        ]
        .to_vec();
        assert_eq!(c_vec, expected_vec);
    }

    #[test]
    fn deserialize_and_serialize_boolean_list() {
        let vec = [1, 0, 2, 0, 0, 0, 1, 0].to_vec();
        let k = deserialize(&vec, &mut 0, false).unwrap();
        let name = K_TYPE_NAME[vec[0] as usize];
        let expect = Series::from_arrow(
            name.into(),
            BooleanArray::from([Some(true), Some(false)]).boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&J::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_guid_list() {
        let vec = [
            2, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 90, 231, 150, 45, 73,
            242, 64, 77, 90, 236, 247, 200, 171, 186, 226, 136,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, false).unwrap();
        let name = K_TYPE_NAME[vec[0] as usize];
        let binary_array = FixedSizeBinaryArray::new(
            ArrowDataType::FixedSizeBinary(16),
            Buffer::from(
                [
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 90, 231, 150, 45, 73, 242, 64,
                    77, 90, 236, 247, 200, 171, 186, 226, 136,
                ]
                .to_vec(),
            ),
            None,
        );
        let expect = Series::from_arrow(name.into(), binary_array.boxed()).unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&J::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_byte_list() {
        let vec = [4, 0, 2, 0, 0, 0, 0, 1].to_vec();
        let k = deserialize(&vec, &mut 0, false).unwrap();
        let name = K_TYPE_NAME[vec[0] as usize];
        let expect =
            Series::from_arrow(name.into(), UInt8Array::from([Some(0), Some(1)]).boxed()).unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&J::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_short_list() {
        let vec = [5, 0, 4, 0, 0, 0, 0, 128, 1, 128, 0, 0, 255, 127].to_vec();
        let k = deserialize(&vec, &mut 0, false).unwrap();
        let name = K_TYPE_NAME[vec[0] as usize];
        let expect = Series::from_arrow(
            name.into(),
            Int16Array::from([None, Some(i16::MIN + 1), Some(0), Some(i16::MAX)]).boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&J::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_int_list() {
        let vec = [
            6, 0, 4, 0, 0, 0, 0, 0, 0, 128, 1, 0, 0, 128, 0, 0, 0, 0, 255, 255, 255, 127,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, false).unwrap();
        let name = K_TYPE_NAME[vec[0] as usize];
        let expect = Series::from_arrow(
            name.into(),
            Int32Array::from([None, None, Some(0), None]).boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        let vec = [
            6, 0, 4, 0, 0, 0, 0, 0, 0, 128, 0, 0, 0, 128, 0, 0, 0, 0, 0, 0, 0, 128,
        ]
        .to_vec();
        assert_eq!(vec, serialize(&J::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_long_list() {
        let vec = [
            7, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 1, 0, 0, 0, 0, 0, 0, 128, 0, 0, 0, 0, 0, 0,
            0, 0, 255, 255, 255, 255, 255, 255, 255, 127,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, false).unwrap();
        let name = K_TYPE_NAME[vec[0] as usize];
        let expect = Series::from_arrow(
            name.into(),
            Int64Array::from([None, None, Some(0), None]).boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        let vec = [
            7, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 0, 0, 0, 0, 0, 0, 0, 128, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 128,
        ]
        .to_vec();
        assert_eq!(vec, serialize(&J::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_real_list() {
        let vec = [
            8, 0, 4, 0, 0, 0, 0, 0, 192, 127, 0, 0, 128, 255, 0, 0, 0, 0, 0, 0, 128, 127,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, false).unwrap();
        let name = K_TYPE_NAME[vec[0] as usize];
        let expect = Series::from_arrow(
            name.into(),
            Float32Array::from([
                None,
                Some(f32::NEG_INFINITY),
                Some(0.0),
                Some(f32::INFINITY),
            ])
            .boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&J::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_float_list() {
        let vec = [
            9, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 248, 127, 0, 0, 0, 0, 0, 0, 240, 255, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 127,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, false).unwrap();
        let name = K_TYPE_NAME[vec[0] as usize];
        let expect = Series::from_arrow(
            name.into(),
            Float64Array::from([
                None,
                Some(f64::NEG_INFINITY),
                Some(0.0),
                Some(f64::INFINITY),
            ])
            .boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&J::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_symbol_list() {
        let vec = [11, 0, 3, 0, 0, 0, 97, 0, 0, 97, 98, 99, 0].to_vec();
        let k = deserialize(&vec, &mut 0, false).unwrap();
        let name = K_TYPE_NAME[vec[0] as usize];
        let expect = Series::from_arrow(
            name.into(),
            Utf8Array::<i64>::from([Some("a"), Some(""), Some("abc")]).boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        let expect = expect
            .cast(&PolarsDataType::Categorical(
                None,
                CategoricalOrdering::Lexical,
            ))
            .unwrap();
        assert_eq!(
            series.to_arrow(0, CompatLevel::newest()),
            expect.to_arrow(0, CompatLevel::newest())
        );
        assert_eq!(vec, serialize(&J::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_string_list() {
        let vec = [
            0, 0, 3, 0, 0, 0, 10, 0, 1, 0, 0, 0, 97, 10, 0, 2, 0, 0, 0, 97, 98, 10, 0, 3, 0, 0, 0,
            97, 98, 99,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, false).unwrap();
        let name = K_TYPE_NAME[vec[6] as usize];
        let expect = Series::from_arrow(
            name.into(),
            Utf8Array::<i64>::from([Some("a"), Some("ab"), Some("abc")]).boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&J::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_timestamp_list() {
        let vec = [
            12, 0, 3, 0, 0, 0, 21, 45, 32, 237, 183, 167, 114, 10, 0, 0, 0, 0, 0, 0, 0, 128, 0, 0,
            199, 153, 133, 126, 114, 10,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, false).unwrap();
        let name = K_TYPE_NAME[vec[0] as usize];
        let expect = Series::from_arrow(
            name.into(),
            PrimitiveArray::new(
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
                vec![1699533296123456789i64, i64::MIN, 1699488000000000000].into(),
                Some(Bitmap::from([true, false, true])),
            )
            .boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&J::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_date_list() {
        let vec = [
            14, 0, 3, 0, 0, 0, 9, 34, 0, 0, 0, 0, 0, 128, 220, 210, 169, 5,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, false).unwrap();
        let name = K_TYPE_NAME[vec[0] as usize];
        let expect = Series::from_arrow(
            name.into(),
            PrimitiveArray::new(
                ArrowDataType::Date32,
                vec![19670, -96465658, 95026601].into(),
                Some(Bitmap::from([true, false, true])),
            )
            .boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&J::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_datetime_list() {
        let vec = [
            15, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 248, 255, 0, 0, 0, 0, 0, 0, 240, 255, 70, 5, 58,
            27, 195, 4, 193, 64, 0, 0, 0, 0, 0, 0, 240, 127,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, false).unwrap();
        let name = K_TYPE_NAME[vec[0] as usize];
        let expect = Series::from_arrow(
            name.into(),
            PrimitiveArray::new(
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
                vec![i64::MIN, i64::MIN + 1, 1699533296789000000i64, i64::MAX].into(),
                Some(Bitmap::from([false, true, true, true])),
            )
            .boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect)
    }

    #[test]
    fn deserialize_and_serialize_timespan_list() {
        let vec = [
            16, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 21, 45, 89, 83, 50, 41, 0, 0, 1, 0, 0, 0,
            0, 0, 0, 128, 255, 255, 255, 255, 255, 255, 255, 127,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, false).unwrap();
        let name = K_TYPE_NAME[vec[0] as usize];
        let expect = Series::from_arrow(
            name.into(),
            PrimitiveArray::new(
                ArrowDataType::Duration(TimeUnit::Nanosecond),
                vec![i64::MIN, 45296123456789, i64::MIN + 1, i64::MAX].into(),
                Some(Bitmap::from([false, true, true, true])),
            )
            .boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&J::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_minute_list() {
        let vec = [
            17, 0, 4, 0, 0, 0, 0, 0, 0, 128, 242, 2, 0, 0, 1, 0, 0, 128, 255, 255, 255, 127,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, false).unwrap();
        let name = K_TYPE_NAME[vec[0] as usize];
        let expect = Series::from_arrow(
            name.into(),
            PrimitiveArray::new(
                ArrowDataType::Time64(TimeUnit::Nanosecond),
                vec![i64::MIN, 45240000_000000, 0i64, NANOS_PER_DAY - 1].into(),
                Some(Bitmap::from([false, true, true, true])),
            )
            .boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect)
    }

    #[test]
    fn deserialize_second_list() {
        let vec = [
            18, 0, 4, 0, 0, 0, 0, 0, 0, 128, 240, 176, 0, 0, 1, 0, 0, 128, 255, 255, 255, 127,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, false).unwrap();
        let name = K_TYPE_NAME[vec[0] as usize];
        let expect = Series::from_arrow(
            name.into(),
            PrimitiveArray::new(
                ArrowDataType::Time64(TimeUnit::Nanosecond),
                vec![i64::MIN, 45296000_000000, 0i64, NANOS_PER_DAY - 1].into(),
                Some(Bitmap::from([false, true, true, true])),
            )
            .boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect)
    }

    #[test]
    fn deserialize_and_serialize_time_list() {
        let vec = [
            19, 0, 4, 0, 0, 0, 0, 0, 0, 128, 149, 44, 179, 2, 0, 0, 0, 0, 255, 91, 38, 5,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, false).unwrap();
        let name = K_TYPE_NAME[vec[0] as usize];
        let expect = Series::from_arrow(
            name.into(),
            PrimitiveArray::new(
                ArrowDataType::Time64(TimeUnit::Nanosecond),
                vec![i64::MIN, 45296789_000000, 0i64, 86399999_000_000].into(),
                Some(Bitmap::from([false, true, true, true])),
            )
            .boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&J::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_bool_nested_list() {
        let vec = [
            0, 0, 3, 0, 0, 0, 1, 0, 1, 0, 0, 0, 1, 1, 0, 2, 0, 0, 0, 1, 1, 1, 0, 3, 0, 0, 0, 1, 1,
            1,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, false).unwrap();
        let k_type = vec[6];
        let name = K_TYPE_NAME[k_type as usize];
        let offsets = OffsetsBuffer::<i32>::try_from([0, 1, 3, 6].to_vec()).unwrap();
        let array = BooleanArray::from([true; 6].map(|b| Some(b)));
        let field = create_field(k_type, name).unwrap();
        let expect = Series::from_arrow(
            name.into(),
            ListArray::new(
                ArrowDataType::List(Box::new(field)),
                offsets,
                array.boxed(),
                None,
            )
            .boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&J::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_bool_nested_array() {
        let vec = [
            0, 0, 3, 0, 0, 0, 1, 0, 2, 0, 0, 0, 1, 0, 1, 0, 2, 0, 0, 0, 1, 0, 1, 0, 2, 0, 0, 0, 1,
            0,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, false).unwrap();
        let k_type = vec[6];
        let name = K_TYPE_NAME[k_type as usize];
        let array = BooleanArray::from([true, false, true, false, true, false].map(|b| Some(b)));
        let field = create_field(k_type, name).unwrap();
        let expect = Series::from_arrow(
            name.into(),
            FixedSizeListArray::new(
                ArrowDataType::FixedSizeList(Box::new(field), 2),
                array.len() / 2,
                array.boxed(),
                None,
            )
            .boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&J::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_byte_nested_list() {
        let vec = [
            0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 0, 0, 4, 0, 1, 0, 0, 0, 1, 4, 0, 2, 0, 0, 0, 1, 2,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, false).unwrap();
        let k_type = vec[6];
        let name = K_TYPE_NAME[k_type as usize];
        let offsets = OffsetsBuffer::<i32>::try_from([0, 0, 1, 3].to_vec()).unwrap();
        let array = UInt8Array::from_slice(vec![1, 1, 2]);
        let field = create_field(k_type, name).unwrap();
        let expect = Series::from_arrow(
            name.into(),
            ListArray::new(
                ArrowDataType::List(Box::new(field)),
                offsets,
                array.boxed(),
                None,
            )
            .boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&J::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_short_nested_list() {
        let vec = [
            0, 0, 3, 0, 0, 0, 5, 0, 0, 0, 0, 0, 5, 0, 1, 0, 0, 0, 0, 128, 5, 0, 2, 0, 0, 0, 1, 0,
            2, 0,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, false).unwrap();
        let k_type = vec[6];
        let name = K_TYPE_NAME[k_type as usize];
        let offsets = OffsetsBuffer::<i32>::try_from([0, 0, 1, 3].to_vec()).unwrap();
        let array = Int16Array::from([None, Some(1), Some(2)]);
        let field = create_field(k_type, name).unwrap();
        let expect = Series::from_arrow(
            name.into(),
            ListArray::new(
                ArrowDataType::List(Box::new(field)),
                offsets,
                array.boxed(),
                None,
            )
            .boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&J::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_int_nested_list() {
        let vec = [
            0, 0, 3, 0, 0, 0, 6, 0, 0, 0, 0, 0, 6, 0, 1, 0, 0, 0, 0, 0, 0, 128, 6, 0, 2, 0, 0, 0,
            1, 0, 0, 0, 2, 0, 0, 0,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, false).unwrap();
        let k_type = vec[6];
        let name = K_TYPE_NAME[k_type as usize];
        let offsets = OffsetsBuffer::<i32>::try_from([0, 0, 1, 3].to_vec()).unwrap();
        let array = Int32Array::from([None, Some(1), Some(2)]);
        let field = create_field(k_type, name).unwrap();
        let expect = Series::from_arrow(
            name.into(),
            ListArray::new(
                ArrowDataType::List(Box::new(field)),
                offsets,
                array.boxed(),
                None,
            )
            .boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&J::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_long_nested_list() {
        let vec = [
            0, 0, 3, 0, 0, 0, 7, 0, 0, 0, 0, 0, 7, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 7, 0,
            2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, false).unwrap();
        let k_type = vec[6];
        let name = K_TYPE_NAME[k_type as usize];
        let offsets = OffsetsBuffer::<i32>::try_from([0, 0, 1, 3].to_vec()).unwrap();
        let array = Int64Array::from([None, Some(1), Some(2)]);
        let field = create_field(k_type, name).unwrap();
        let expect = Series::from_arrow(
            name.into(),
            ListArray::new(
                ArrowDataType::List(Box::new(field)),
                offsets,
                array.boxed(),
                None,
            )
            .boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&J::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_real_nested_list() {
        let vec = [
            0, 0, 3, 0, 0, 0, 8, 0, 0, 0, 0, 0, 8, 0, 1, 0, 0, 0, 0, 0, 128, 127, 8, 0, 2, 0, 0, 0,
            0, 0, 128, 63, 0, 0, 128, 255,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, false).unwrap();
        let k_type = vec[6];
        let name = K_TYPE_NAME[k_type as usize];
        let offsets = OffsetsBuffer::<i32>::try_from([0, 0, 1, 3].to_vec()).unwrap();
        let array =
            Float32Array::from([Some(f32::INFINITY), Some(1.0f32), Some(f32::NEG_INFINITY)]);
        let field = create_field(k_type, name).unwrap();
        let expect = Series::from_arrow(
            name.into(),
            ListArray::new(
                ArrowDataType::List(Box::new(field)),
                offsets,
                array.boxed(),
                None,
            )
            .boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&J::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_float_nested_list() {
        let vec = [
            0, 0, 3, 0, 0, 0, 9, 0, 0, 0, 0, 0, 9, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 127, 9, 0,
            2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 240, 255,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, false).unwrap();
        let k_type = vec[6];
        let name = K_TYPE_NAME[k_type as usize];
        let offsets = OffsetsBuffer::<i32>::try_from([0, 0, 1, 3].to_vec()).unwrap();
        let array = Float64Array::from([Some(f64::INFINITY), Some(1.0), Some(f64::NEG_INFINITY)]);
        let field = create_field(k_type, name).unwrap();
        let expect = Series::from_arrow(
            name.into(),
            ListArray::new(
                ArrowDataType::List(Box::new(field)),
                offsets,
                array.boxed(),
                None,
            )
            .boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&J::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_timestamp_nested_list() {
        let vec = [
            0, 0, 3, 0, 0, 0, 7, 0, 0, 0, 0, 0, 7, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 7, 0,
            2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, false).unwrap();
        let k_type = vec[6];
        let name = K_TYPE_NAME[k_type as usize];
        let offsets = OffsetsBuffer::<i32>::try_from([0, 0, 1, 3].to_vec()).unwrap();
        let array = Int64Array::from([None, Some(1), Some(2)]);
        let field = create_field(k_type, name).unwrap();
        let expect = Series::from_arrow(
            name.into(),
            ListArray::new(
                ArrowDataType::List(Box::new(field)),
                offsets,
                array.boxed(),
                None,
            )
            .boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&J::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_mixed_list() {
        let vec = [
            0, 0, 3, 0, 0, 0, 245, 117, 112, 100, 0, 245, 116, 0, 98, 0, 99, 11, 0, 1, 0, 0, 0, 97,
            0, 0, 0, 1, 0, 0, 0, 7, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, false).unwrap();
        let expect = J::MixedList(vec![
            J::Symbol("upd".to_owned()),
            J::Symbol("t".to_owned()),
            J::DataFrame(
                DataFrame::new(vec![Series::new("a".into(), [1i64].as_ref()).into()]).unwrap(),
            ),
        ]);
        assert_eq!(k, expect);
        assert_eq!(vec, serialize(&expect).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_table() {
        let vec = [
            98, 0, 99, 11, 0, 2, 0, 0, 0, 97, 0, 98, 0, 0, 0, 2, 0, 0, 0, 7, 0, 1, 0, 0, 0, 1, 0,
            0, 0, 0, 0, 0, 0, 9, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, false).unwrap();
        let df: DataFrame = k.try_into().unwrap();
        let s0 = Series::new("a".into(), [1i64].as_ref());
        let s1 = Series::new("b".into(), [1.0f64].as_ref());
        let expect = DataFrame::new(vec![s0.into(), s1.into()]).unwrap();
        assert_eq!(df, expect);
        assert_eq!(vec, serialize(&J::DataFrame(expect)).unwrap());
    }

    #[test]
    fn deserialize_keyed_table() {
        let vec = [
            99, 98, 0, 99, 11, 0, 1, 0, 0, 0, 97, 0, 0, 0, 1, 0, 0, 0, 9, 0, 1, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 240, 63, 98, 0, 99, 11, 0, 1, 0, 0, 0, 98, 0, 0, 0, 1, 0, 0, 0, 9, 0, 1, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 240, 63,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, false).unwrap();
        let df: DataFrame = k.try_into().unwrap();
        let s0 = Series::new("a".into(), [1i64].as_ref());
        let s1 = Series::new("b".into(), [1.0f64].as_ref());
        let expect = DataFrame::new(vec![s0.into(), s1.into()]).unwrap();
        assert_eq!(df, expect);
    }

    #[test]
    fn serialize_bool() {
        let k = J::Boolean(true);
        assert_eq!(serialize(&k).unwrap(), [255, 1]);
    }

    #[test]
    fn serialize_guid() {
        let k = J::Guid(
            Uuid::from_slice(&[
                88, 13, 140, 135, 229, 87, 13, 177, 58, 25, 203, 58, 68, 214, 35, 177,
            ])
            .unwrap(),
        );
        assert_eq!(
            serialize(&k).unwrap(),
            [254, 88, 13, 140, 135, 229, 87, 13, 177, 58, 25, 203, 58, 68, 214, 35, 177,]
        );
    }

    #[test]
    fn serialize_byte() {
        let k = J::U8(99);
        assert_eq!(serialize(&k).unwrap(), [252, 99]);
    }

    #[test]
    fn serialize_short() {
        let k = J::I16(99);
        assert_eq!(serialize(&k).unwrap(), [251, 99, 0]);
    }

    #[test]
    fn serialize_int() {
        let k = J::I32(99999999);
        assert_eq!(serialize(&k).unwrap(), [250, 255, 224, 245, 5]);
    }

    #[test]
    fn serialize_long() {
        let k = J::I64(9999_9999_9999_9999);
        assert_eq!(
            serialize(&k).unwrap(),
            [249, 255, 255, 192, 111, 242, 134, 35, 0]
        );
    }

    #[test]
    fn serialize_real() {
        let k = J::F32(9.9e10);
        assert_eq!(serialize(&k).unwrap(), [248, 225, 102, 184, 81]);
    }

    #[test]
    fn serialize_float() {
        let k = J::F64(9.9e10);
        assert_eq!(serialize(&k).unwrap(), [247, 0, 0, 0, 30, 220, 12, 55, 66]);
    }

    #[test]
    fn serialize_symbol() {
        let k = J::Symbol("abc".to_string());
        assert_eq!(serialize(&k).unwrap(), [245, 97, 98, 99, 0]);
    }

    #[test]
    fn serialize_string() {
        let k = J::String("abc".to_string());
        assert_eq!(serialize(&k).unwrap(), [10, 0, 3, 0, 0, 0, 97, 98, 99]);
    }

    #[test]
    fn serialize_timestamp() {
        let k = J::DateTime(DateTime::<Utc>::from_timestamp(0, 123456789).unwrap());
        assert_eq!(
            serialize(&k).unwrap(),
            [244, 21, 205, 24, 181, 48, 179, 220, 242]
        );
    }

    #[test]
    fn serialize_date() {
        let k = J::Date(NaiveDate::from_ymd_opt(2023, 11, 15).unwrap());
        assert_eq!(serialize(&k).unwrap(), [242, 15, 34, 0, 0]);
    }

    #[test]
    fn serialize_time() {
        let k = J::Time(NaiveTime::from_hms_milli_opt(0, 17, 24, 70).unwrap());
        assert_eq!(serialize(&k).unwrap(), [237, 102, 238, 15, 0]);
    }

    #[test]
    fn serialize_duration() {
        let k = J::Duration(Duration::nanoseconds(822896123456789));
        assert_eq!(
            serialize(&k).unwrap(),
            [240, 21, 45, 32, 111, 107, 236, 2, 0]
        );
    }

    #[test]
    fn serialize_none() {
        let k = J::Null;
        assert_eq!(serialize(&k).unwrap(), [101, 0]);
    }

    #[test]
    fn deserialize_and_serialize_dict() {
        let vec = [
            99, 11, 0, 2, 0, 0, 0, 97, 0, 98, 0, 0, 0, 2, 0, 0, 0, 249, 1, 0, 0, 0, 0, 0, 0, 0,
            247, 0, 0, 0, 0, 0, 0, 240, 63,
        ]
        .to_vec();
        let mut dict = IndexMap::with_capacity(2);
        dict.insert("a".to_string(), J::I64(1));
        dict.insert("b".to_string(), J::F64(1.0));
        let k = J::Dict(dict);
        assert_eq!(deserialize(&vec, &mut 0, false).unwrap(), k);
        assert_eq!(vec, serialize(&k).unwrap());
    }
}
