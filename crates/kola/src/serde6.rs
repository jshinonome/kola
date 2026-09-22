use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveTime, Timelike, Utc};
use indexmap::IndexMap;
use polars::chunked_array::ops::ChunkFillNullValue;
use polars::datatypes::{DataType as PolarsDataType, TimeUnit as PolarTimeUnit};
use polars::prelude::{Categories, DataFrame};
use polars::series::{IntoSeries, Series};
use polars_arrow::array::{
    Array, BinaryViewArray, BooleanArray, FixedSizeBinaryArray, FixedSizeListArray, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, ListArray, PrimitiveArray, UInt8Array,
    Utf8ViewArray,
};
use polars_arrow::bitmap::Bitmap;
use polars_arrow::datatypes::{ArrowDataType, Field, TimeUnit};
use polars_arrow::legacy::kernels::set::set_at_nulls;
use polars_arrow::types::NativeType;
use polars_arrow::{array::Utf8Array, offset::OffsetsBuffer};
use polars_buffer::Buffer;
use rayon::iter::IntoParallelIterator;
use rayon::prelude::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};
use std::cmp::min;
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
    types::{Operator, K, K_TYPE_SIZE},
};

pub fn deserialize(vec: &[u8], pos: &mut usize, is_column: bool) -> Result<K, KolaError> {
    let k_type = vec[*pos];
    *pos += 1;
    let start_pos = *pos;
    match k_type {
        237..=255 => match k_type {
            255 => {
                *pos += 1;
                Ok(K::Boolean(vec[start_pos] == 1))
            }
            254 => {
                *pos += 16;
                Ok(K::Guid(Uuid::from_bytes(
                    vec[start_pos..start_pos + 16].try_into().unwrap(),
                )))
            }
            252 => {
                *pos += 1;
                Ok(K::U8(vec[start_pos]))
            }
            251 => {
                *pos += 2;
                Ok(K::I16(i16::from_le_bytes(
                    vec[start_pos..start_pos + 2].try_into().unwrap(),
                )))
            }
            250 => {
                *pos += 4;
                Ok(K::I32(i32::from_le_bytes(
                    vec[start_pos..start_pos + 4].try_into().unwrap(),
                )))
            }
            249 => {
                *pos += 8;
                Ok(K::I64(i64::from_le_bytes(
                    vec[start_pos..start_pos + 8].try_into().unwrap(),
                )))
            }
            248 => {
                *pos += 4;
                Ok(K::F32(f32::from_le_bytes(
                    vec[start_pos..start_pos + 4].try_into().unwrap(),
                )))
            }
            247 => {
                *pos += 8;
                Ok(K::F64(f64::from_le_bytes(
                    vec[start_pos..start_pos + 8].try_into().unwrap(),
                )))
            }
            246 => {
                *pos += 1;
                Ok(K::Char(vec[start_pos]))
            }
            245 => {
                let mut eod_pos = *pos;
                while eod_pos < vec.len() && vec[eod_pos] != 0 {
                    eod_pos += 1;
                }
                *pos = eod_pos + 1;
                Ok(K::Symbol(
                    String::from_utf8_lossy(&vec[start_pos..eod_pos]).into_owned(),
                ))
            }
            // timestamp
            244 => {
                let ns = i64::from_le_bytes(vec[*pos..*pos + 8].try_into().unwrap())
                    .saturating_add(NANOS_DIFF);
                *pos += 8;
                Ok(K::DateTime(create_datetime(ns)))
            }
            // month
            243 => {
                let unit = i32::from_le_bytes(vec[*pos..*pos + 4].try_into().unwrap());
                let year = 2000 + unit.div_euclid(12);
                let month = 1 + unit.rem_euclid(12);
                *pos += 4;
                // null and infinity are out of range
                let date = match NaiveDate::from_ymd_opt(year, month as u32, 1) {
                    Some(date) => date,
                    None => {
                        if unit > 0 {
                            NaiveDate::MAX
                        } else {
                            NaiveDate::MIN
                        }
                    }
                };
                Ok(K::Date(date))
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
                Ok(K::Date(date))
            }
            // datetime
            241 => {
                let unit = f64::from_le_bytes(vec[*pos..*pos + 8].try_into().unwrap());
                // same as null timestamp
                let ns = if unit.is_nan() {
                    i64::MIN
                } else {
                    (unit * NANOS_PER_DAY as f64) as i64
                };
                let ns = ns.saturating_add(NANOS_DIFF);
                *pos += 8;
                Ok(K::DateTime(create_datetime(ns)))
            }
            // timespan
            240 => {
                let ns = i64::from_le_bytes(vec[*pos..*pos + 8].try_into().unwrap());
                *pos += 8;
                Ok(K::Duration(Duration::nanoseconds(ns)))
            }
            // time, second, minute
            237..=239 => {
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
                Ok(K::Time(
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
                        return Ok(K::MixedList(res));
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
                key_df = key_df
                    .hstack(value_df.columns())
                    .map_err(|e| KolaError::Err(e.to_string()))?;
                Ok(K::DataFrame(key_df))
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
                    K::Series(s) => {
                        let mut dict = IndexMap::with_capacity(keys.len());
                        for (k, v) in keys.cat32().unwrap().iter_str().zip(s.iter()) {
                            dict.insert(k.unwrap().to_string(), K::from_any_value(v));
                        }
                        Ok(K::Dict(dict))
                    }
                    K::MixedList(l) => {
                        let mut dict = IndexMap::with_capacity(keys.len());
                        for (k, v) in keys.cat32().unwrap().iter_str().zip(l) {
                            dict.insert(k.unwrap().to_string(), v);
                        }
                        Ok(K::Dict(dict))
                    }
                    K::String(s) => {
                        let mut dict = IndexMap::with_capacity(keys.len());
                        for (k, v) in keys.cat32().unwrap().iter_str().zip(s.bytes()) {
                            dict.insert(k.unwrap().to_string(), K::Char(v));
                        }
                        Ok(K::Dict(dict))
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
            let symbols = if let K::Series(series) = k {
                series
            } else {
                return Err(KolaError::DeserializationErr(format!(
                    "Expecting array, but got {k:?}"
                )));
            };
            let symbols = symbols.str().unwrap();
            *pos += 6;
            let mut k_types = vec![0u8; symbols.len()];
            let mut vectors: Vec<&[u8]> = Vec::with_capacity(symbols.len());
            for k_type in k_types.iter_mut().take(symbols.len()) {
                *k_type = vec[*pos];
                *pos += 1;
                let end_pos = calculate_array_end_index(vec, *pos, *k_type)?;
                vectors.push(&vec[*pos..end_pos]);
                *pos = end_pos;
            }

            let mut columns: Vec<Series> = vectors
                .par_iter()
                .zip(k_types.clone())
                .map(|(v, t)| deserialize_series(v, t, true)?.try_into())
                .collect::<Result<_, KolaError>>()?;
            columns.iter_mut().zip(symbols.iter()).for_each(|(c, n)| {
                c.rename(n.unwrap_or("").into());
            });
            Ok(K::DataFrame(
                DataFrame::new_infer_height(columns.into_iter().map(|c| c.into()).collect())
                    .unwrap(),
            ))
        }
        101 | 102 => {
            let code = *vec.get(start_pos).ok_or_else(|| {
                KolaError::DeserializationErr(format!("Missing K{k_type} operator code"))
            })?;
            *pos += 1;
            if k_type == 101 && code == 0 {
                Ok(K::Null)
            } else {
                Ok(K::Operator(Operator::try_from((k_type, code))?))
            }
        }
        // q error
        128 => {
            let mut eod_pos = *pos;
            while eod_pos < vec.len() && vec[eod_pos] != 0 {
                eod_pos += 1;
            }
            *pos = eod_pos;
            Err(KolaError::ServerErr(
                String::from_utf8_lossy(&vec[start_pos..eod_pos]).into_owned(),
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
                    for _ in 0..sub_length {
                        let mut k = 0;
                        while pos + k < vec.len() && vec[pos + k] != 0 {
                            k += 1;
                        }
                        if pos + k >= vec.len() {
                            return Err(KolaError::DeserializationErr(
                                "Unterminated symbol".to_string(),
                            ));
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
                if pos >= vec.len() {
                    return Err(KolaError::DeserializationErr(
                        "Unterminated symbol".to_string(),
                    ));
                }
                if vec[pos] == 0 {
                    i += 1;
                }
                pos += 1;
            }
            Ok(pos)
        }
        _ => {
            if k_type > 19 {
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

fn deserialize_series(vec: &[u8], k_type: u8, as_column: bool) -> Result<K, KolaError> {
    let mut pos = 1;
    let length = u32::from_le_bytes(vec[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;
    if length == 0 {
        return new_empty_series(k_type);
    }
    let mut series: Series;
    let array_box: Box<dyn Array>;
    let array_vec = &vec[pos..];
    let name = K_TYPE_NAME[k_type as usize];
    match k_type {
        0 => deserialize_nested_array(vec),
        1 => {
            array_box =
                BooleanArray::from_slice(array_vec.iter().map(|u| *u == 1).collect::<Vec<_>>())
                    .boxed();
            series = Series::from_arrow(name.into(), array_box).unwrap();
            Ok(K::Series(series))
        }
        2 => {
            array_box = FixedSizeBinaryArray::new(
                ArrowDataType::FixedSizeBinary(16),
                Buffer::from(array_vec.to_vec()),
                None,
            )
            .boxed();
            series = Series::from_arrow(name.into(), array_box).unwrap();
            Ok(K::Series(series))
        }
        4 => {
            array_box = UInt8Array::from_vec(array_vec.to_vec()).boxed();
            series = Series::from_arrow(name.into(), array_box).unwrap();
            Ok(K::Series(series))
        }
        5 => {
            let native_vec: Vec<i16> = to_native_vec(array_vec);
            let slice = native_vec.as_slice();
            let bitmap =
                Bitmap::from_iter(slice.iter().map(|s| *s > i16::MIN + 1 && *s < i16::MAX));
            let mut array = Int16Array::from_vec(native_vec);
            array.set_validity(Some(bitmap));
            series = Series::from_arrow(name.into(), array.boxed()).unwrap();
            Ok(K::Series(series))
        }
        6 => {
            let native_vec: Vec<i32> = to_native_vec(array_vec);
            let slice = native_vec.as_slice();
            let bitmap =
                Bitmap::from_iter(slice.iter().map(|s| *s > i32::MIN + 1 && *s < i32::MAX));
            let mut array = Int32Array::from_vec(native_vec);
            array.set_validity(Some(bitmap));
            series = Series::from_arrow(name.into(), array.boxed()).unwrap();
            Ok(K::Series(series))
        }
        7 => {
            let native_vec: Vec<i64> = to_native_vec(array_vec);
            let slice = native_vec.as_slice();
            let bitmap =
                Bitmap::from_iter(slice.iter().map(|s| *s > i64::MIN + 1 && *s < i64::MAX));
            let mut array = Int64Array::from_vec(native_vec);
            array.set_validity(Some(bitmap));
            series = Series::from_arrow(name.into(), array.boxed()).unwrap();
            Ok(K::Series(series))
        }
        8 => {
            let native_vec: Vec<f32> = to_native_vec(array_vec);
            let slice = native_vec.as_slice();
            let bitmap = Bitmap::from_iter(slice.iter().map(|s| !f32::is_nan(*s)));
            let mut array = Float32Array::from_vec(native_vec);
            array.set_validity(Some(bitmap));
            series = Series::from_arrow(name.into(), array.boxed()).unwrap();
            Ok(K::Series(series))
        }
        9 => {
            let native_vec: Vec<f64> = to_native_vec(array_vec);
            let slice = native_vec.as_slice();
            let bitmap = Bitmap::from_iter(slice.iter().map(|s| !f64::is_nan(*s)));
            let mut array = Float64Array::from_vec(native_vec);
            array.set_validity(Some(bitmap));
            series = Series::from_arrow(name.into(), array.boxed()).unwrap();
            Ok(K::Series(series))
        }
        10 => {
            if as_column {
                let offsets: Vec<i64> = (0..=length as i64).collect();
                array_box = create_lossy_utf8_array(offsets, array_vec.to_vec()).boxed();
                series = Series::from_arrow(name.into(), array_box).unwrap();
                Ok(K::Series(series))
            } else {
                Ok(K::String(String::from_utf8_lossy(array_vec).to_string()))
            }
        }
        11 => {
            let mut v8: Vec<u8> = Vec::with_capacity(vec.len() - length);
            let mut offsets: Vec<i64> = vec![0i64; length + 1];
            let mut i = 0;
            let mut start_pos = pos;
            while i < length {
                if vec[pos] == 0 {
                    let s = String::from_utf8_lossy(&vec[start_pos..pos]);
                    v8.write_all(s.as_bytes()).unwrap();
                    offsets[i + 1] = offsets[i] + s.len() as i64;
                    start_pos = pos + 1;
                    i += 1;
                }
                pos += 1;
            }
            // SAFETY: values are UTF-8 via from_utf8_lossy above
            array_box = unsafe {
                Utf8Array::<i64>::new_unchecked(
                    ArrowDataType::LargeUtf8,
                    OffsetsBuffer::try_from(offsets).unwrap(),
                    Buffer::from(v8),
                    None,
                )
            }
            .boxed();
            series = Series::from_arrow(name.into(), array_box).unwrap();
            if as_column {
                series = series
                    .cast(&PolarsDataType::Categorical(
                        Categories::global(),
                        Categories::global().mapping(),
                    ))
                    .unwrap();
            }
            Ok(K::Series(series))
        }
        12 => {
            let native_vec: Vec<i64> = to_native_vec(array_vec);
            let slice = native_vec.as_slice();
            let slice = slice
                .iter()
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
            Ok(K::Series(series))
        }
        14 => {
            let native_vec: Vec<i32> = to_native_vec(array_vec);
            let slice = native_vec.as_slice();
            let bitmap = Bitmap::from_iter(slice.iter().map(|s| *s != i32::MIN));
            let slice = slice
                .iter()
                .map(|day| {
                    let day = day.saturating_add(10957);
                    day.clamp(-96465658, 95026601)
                })
                .collect::<Vec<_>>();
            let array = PrimitiveArray::new(ArrowDataType::Date32, slice.into(), Some(bitmap));
            array_box = array.boxed();
            series = Series::from_arrow(name.into(), array_box).unwrap();
            Ok(K::Series(series))
        }
        15 => {
            let native_vec: Vec<f64> = to_native_vec(array_vec);
            let slice = native_vec.as_slice();
            let slice = slice
                .iter()
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
            Ok(K::Series(series))
        }
        // timespan
        16 => {
            let native_vec: Vec<i64> = to_native_vec(array_vec);
            let slice = native_vec.as_slice();
            let bitmap = Bitmap::from_iter(slice.iter().map(|s| *s != i64::MIN));
            let array = PrimitiveArray::new(
                ArrowDataType::Duration(TimeUnit::Nanosecond),
                native_vec.into(),
                Some(bitmap),
            );
            array_box = array.boxed();
            series = Series::from_arrow(name.into(), array_box).unwrap();
            Ok(K::Series(series))
        }
        // minutes, seconds, time
        17..=19 => {
            let native_vec: Vec<i32> = to_native_vec(array_vec);
            let slice = native_vec.as_slice();
            let bitmap = Bitmap::from_iter(slice.iter().map(|s| *s != i32::MIN));
            let multiplier = if k_type == 17 {
                60_000_000_000
            } else if k_type == 18 {
                1_000_000_000
            } else {
                1_000_000
            };

            let slice = slice
                .iter()
                .map(|t| {
                    let ns = (*t as i64).saturating_mul(multiplier);
                    ns.clamp(0, NANOS_PER_DAY - 1)
                })
                .collect::<Vec<_>>();

            let array = PrimitiveArray::new(
                ArrowDataType::Time64(TimeUnit::Nanosecond),
                slice.into(),
                Some(bitmap),
            );
            array_box = array.boxed();
            series = Series::from_arrow(name.into(), array_box).unwrap();
            Ok(K::Series(series))
        }
        _ => Err(KolaError::NotSupportedKListErr(k_type)),
    }
}

fn new_empty_series(k_type: u8) -> Result<K, KolaError> {
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
            &PolarsDataType::Categorical(Categories::global(), Categories::global().mapping()),
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
        17..=19 => Series::new_empty(name.into(), &PolarsDataType::Time),
        _ => return Err(KolaError::NotSupportedKListErr(k_type)),
    };
    Ok(K::Series(series))
}

fn deserialize_nested_array(vec: &[u8]) -> Result<K, KolaError> {
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
            v8.write_all(&vec[pos..pos + k_size * sub_length as usize])
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
                let s = String::from_utf8_lossy(&vec[pos..pos + k]);
                v8.write_all(s.as_bytes()).unwrap();
                sub_offsets.push(sub_offsets.last().unwrap() + s.len() as i64);
                pos += k + 1;
            }
        }
        // SAFETY: values are UTF-8 via from_utf8_lossy above
        let array_box = unsafe {
            Utf8Array::<i64>::new_unchecked(
                ArrowDataType::LargeUtf8,
                OffsetsBuffer::try_from(sub_offsets).unwrap(),
                Buffer::from(v8),
                None,
            )
        }
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
                PolarsDataType::Categorical(Categories::global(), Categories::global().mapping())
                    .boxed(),
            ))
            .unwrap();
        return Ok(K::Series(series));
    } else {
        return Err(KolaError::NotSupportedKNestedListErr(k_type));
    }
    let offsets_buf = OffsetsBuffer::<i64>::try_from(offsets).unwrap();
    match k_type {
        1 | 4 | 5 | 6 | 7 | 8 | 9 | 12 => {
            let field: Field;
            let list_array: ListArray<i32>;
            let array_box: Box<dyn Array>;
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
                let native_vec: Vec<i16> = to_native_vec(&v8);
                let slice = native_vec.as_slice();
                let bitmap = Bitmap::from_iter(slice.iter().map(|s| *s != i16::MIN));
                let mut array = Int16Array::from_vec(native_vec);
                array.set_validity(Some(bitmap));
                array_box = array.boxed();
                field = create_field(k_type, "short").unwrap();
            } else if k_type == 6 {
                let native_vec: Vec<i32> = to_native_vec(&v8);
                let slice = native_vec.as_slice();
                let bitmap = Bitmap::from_iter(slice.iter().map(|s| *s != i32::MIN));
                let mut array = Int32Array::from_vec(native_vec);
                array.set_validity(Some(bitmap));
                array_box = array.boxed();
                field = create_field(k_type, "int").unwrap();
            } else if k_type == 7 {
                let native_vec: Vec<i64> = to_native_vec(&v8);
                let slice = native_vec.as_slice();
                let bitmap = Bitmap::from_iter(slice.iter().map(|s| *s != i64::MIN));
                let mut array = Int64Array::from_vec(native_vec);
                array.set_validity(Some(bitmap));
                array_box = array.boxed();
                field = create_field(k_type, "long").unwrap();
            } else if k_type == 8 {
                let native_vec: Vec<f32> = to_native_vec(&v8);
                let slice = native_vec.as_slice();
                let bitmap = Bitmap::from_iter(slice.iter().map(|s| !f32::is_nan(*s)));
                let mut array = Float32Array::from_vec(native_vec);
                array.set_validity(Some(bitmap));
                array_box = array.boxed();
                field = create_field(k_type, "real").unwrap();
            } else if k_type == 9 {
                let native_vec: Vec<f64> = to_native_vec(&v8);
                let slice = native_vec.as_slice();
                let bitmap = Bitmap::from_iter(slice.iter().map(|s| !f64::is_nan(*s)));
                let mut array = Float64Array::from_vec(native_vec);
                array.set_validity(Some(bitmap));
                array_box = array.boxed();
                field = create_field(k_type, "float").unwrap();
            } else if k_type == 12 {
                let native_vec: Vec<i64> = to_native_vec(&v8);
                let slice = native_vec.as_slice();
                let slice = slice
                    .iter()
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

            Ok(K::Series(
                Series::from_arrow(name.into(), list_array.boxed()).unwrap(),
            ))
        }
        10 => {
            let array_box = create_lossy_utf8_array(offsets_buf.to_vec(), v8).boxed();
            Ok(K::Series(
                Series::from_arrow(name.into(), array_box).unwrap(),
            ))
        }
        _ => unreachable!(),
    }
}

// q chars are bytes, replace invalid UTF-8 sequences same as symbols
fn create_lossy_utf8_array(offsets: Vec<i64>, v8: Vec<u8>) -> Utf8Array<i64> {
    let is_valid = offsets
        .windows(2)
        .all(|w| std::str::from_utf8(&v8[w[0] as usize..w[1] as usize]).is_ok());
    let (offsets, v8) = if is_valid {
        (offsets, v8)
    } else {
        let mut lossy_offsets = Vec::with_capacity(offsets.len());
        let mut lossy_v8 = Vec::with_capacity(v8.len());
        lossy_offsets.push(0i64);
        for w in offsets.windows(2) {
            let s = String::from_utf8_lossy(&v8[w[0] as usize..w[1] as usize]);
            lossy_v8.extend_from_slice(s.as_bytes());
            lossy_offsets.push(lossy_v8.len() as i64);
        }
        (lossy_offsets, lossy_v8)
    };
    // SAFETY: every value is checked or replaced above
    unsafe {
        Utf8Array::<i64>::new_unchecked(
            ArrowDataType::LargeUtf8,
            OffsetsBuffer::try_from(offsets).unwrap(),
            Buffer::from(v8),
            None,
        )
    }
}

// copy little endian bytes into an aligned vec
fn to_native_vec<T: NativeType>(v8: &[u8]) -> Vec<T> {
    let length = v8.len() / size_of::<T>();
    let mut vec: Vec<T> = Vec::with_capacity(length);
    // SAFETY: capacity is length * size_of::<T>() bytes and any bit pattern is a valid T
    unsafe {
        core::ptr::copy_nonoverlapping(
            v8.as_ptr(),
            vec.as_mut_ptr().cast::<u8>(),
            length * size_of::<T>(),
        );
        vec.set_len(length);
    }
    vec
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
        vec
    } else {
        let mut c_vec = vec![0u8; vec.len() / 2];
        // compressed bytes start position
        let mut c_pos: usize;
        if vec.len() > 4294967295 {
            c_pos = 16;
            c_vec[2] = 2;
            c_vec[(3 + 8)..(8 + 8)].copy_from_slice(&vec[3..8]);
        } else {
            c_pos = 12;
            c_vec[2] = 1;
            // copy raw vec length
            c_vec[(4 + 4)..(8 + 4)].copy_from_slice(&vec[4..8]);
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
        c_vec[4..(4 + 4)].copy_from_slice(&c_len);
        c_vec[3] = (c_pos >> 32) as u8;
        c_vec.resize(c_pos, 0u8);
        c_vec
    }
}

pub fn serialize(k: &K) -> Result<Vec<u8>, KolaError> {
    // items of a mixed list, table or dict calculate their own length
    let k_length = if let K::MixedList(_) | K::DataFrame(_) | K::Dict(_) = k {
        0
    } else {
        k.j6_len()?
    };
    let mut vec: Vec<u8>;
    match k {
        K::Boolean(k) => {
            vec = Vec::with_capacity(k_length);
            vec.write_all(&[255, (*k as u8)]).unwrap();
        }
        K::Guid(k) => {
            vec = Vec::with_capacity(k_length);
            vec.write_all(&[254u8]).unwrap();
            vec.write_all(k.as_bytes()).unwrap();
        }
        K::U8(k) => {
            vec = Vec::with_capacity(k_length);
            vec.write_all(&[252, *k]).unwrap();
        }
        K::I16(k) => {
            vec = Vec::with_capacity(k_length);
            vec.write_all(&[251]).unwrap();
            vec.write_all(&NativeType::to_le_bytes(k)).unwrap();
        }
        K::I32(k) => {
            vec = Vec::with_capacity(k_length);
            vec.write_all(&[250]).unwrap();
            vec.write_all(&NativeType::to_le_bytes(k)).unwrap();
        }
        K::I64(k) => {
            vec = Vec::with_capacity(k_length);
            vec.write_all(&[249]).unwrap();
            vec.write_all(&NativeType::to_le_bytes(k)).unwrap();
        }
        K::F32(k) => {
            vec = Vec::with_capacity(k_length);
            vec.write_all(&[248]).unwrap();
            vec.write_all(&NativeType::to_le_bytes(k)).unwrap();
        }
        K::F64(k) => {
            vec = Vec::with_capacity(k_length);
            vec.write_all(&[247]).unwrap();
            vec.write_all(&NativeType::to_le_bytes(k)).unwrap();
        }
        K::Char(k) => {
            vec = Vec::with_capacity(k_length);
            vec.write_all(&[246, *k]).unwrap();
        }
        K::Symbol(k) => {
            vec = Vec::with_capacity(k_length);
            vec.write_all(&[245]).unwrap();
            vec.write_all(k.as_bytes()).unwrap();
            vec.write_all(&[0]).unwrap();
        }
        K::String(k) => {
            vec = Vec::with_capacity(k_length);
            vec.write_all(&[10, 0]).unwrap();
            vec.write_all(&(k.len() as u32).to_le_bytes()).unwrap();
            vec.write_all(k.as_bytes()).unwrap();
        }
        // to timestamp
        K::DateTime(k) => {
            vec = Vec::with_capacity(k_length);
            vec.write_all(&[244]).unwrap();
            let ns = match k.timestamp_nanos_opt() {
                Some(ns) => ns.saturating_sub(NANOS_DIFF),
                _ => i64::MIN,
            };
            vec.write_all(&ns.to_le_bytes()).unwrap();
        }
        // to date
        K::Date(k) => {
            vec = Vec::with_capacity(k_length);
            vec.write_all(&[242]).unwrap();
            let days = k.num_days_from_ce().saturating_sub(DAY_DIFF);
            vec.write_all(&days.to_le_bytes()).unwrap();
        }
        // to time
        K::Time(k) => {
            vec = Vec::with_capacity(k_length);
            vec.write_all(&[237]).unwrap();
            let milliseconds = k.num_seconds_from_midnight() * 1000 + k.nanosecond() / 1000000;
            vec.write_all(&(milliseconds as i32).to_le_bytes()).unwrap();
        }
        // to timespan
        K::Duration(k) => {
            vec = Vec::with_capacity(k_length);
            vec.write_all(&[240]).unwrap();
            let ns = k.num_nanoseconds();
            vec.write_all(&(ns.unwrap_or(i64::MIN)).to_le_bytes())
                .unwrap();
        }
        K::MixedList(l) => {
            let vectors = l
                .iter()
                .map(serialize)
                .collect::<Result<Vec<Vec<u8>>, KolaError>>()?;
            let k_length = 6 + vectors.iter().map(|v| v.len()).sum::<usize>();
            vec = Vec::with_capacity(k_length);
            vec.write_all(&[0, 0]).unwrap();
            vec.write_all(&(l.len() as u32).to_le_bytes()).unwrap();
            vectors.into_iter().for_each(|v| {
                vec.write_all(&v).unwrap();
            });
        }
        // to list
        K::Series(k) => {
            vec = serialize_series(k, k_length)?;
        }
        // to table
        K::DataFrame(k) => {
            let vectors = k
                .columns()
                .into_par_iter()
                .map(|s| {
                    let s = s.as_materialized_series();
                    serialize_series(s, get_series_len(s)?)
                })
                .collect::<Result<Vec<Vec<u8>>, KolaError>>()?;
            let column_names = k.get_column_names();
            // 98 0 99 + symbol list(6) + values(6)
            let k_length = 15
                + column_names.iter().map(|s| s.len() + 1).sum::<usize>()
                + vectors.iter().map(|v| v.len()).sum::<usize>();
            vec = Vec::with_capacity(k_length);
            let column_count = column_names.len() as i32;
            vec.write_all(&[98, 0, 99, 11, 0]).unwrap();
            vec.write_all(&column_count.to_le_bytes()).unwrap();
            column_names.into_iter().for_each(|s| {
                vec.write_all(s.as_bytes()).unwrap();
                vec.write_all(&[0]).unwrap();
            });
            vec.write_all(&[0, 0]).unwrap();
            vec.write_all(&column_count.to_le_bytes()).unwrap();
            vectors.into_iter().for_each(|v| {
                vec.write_all(&v).unwrap();
            });
        }
        K::Operator(operator) => {
            vec = vec![operator.k_type(), operator.code()];
        }
        // to (::)
        K::Null => {
            vec = Vec::with_capacity(k_length);
            vec.write_all(&[101, 0]).unwrap();
        }
        K::Dict(dict) => {
            let keys = dict.keys();
            let length = keys.len() as i32;
            if length == 0 {
                return Err(KolaError::Err("Not supported empty dictionary".to_string()));
            };
            let vectors = dict
                .values()
                .map(serialize)
                .collect::<Result<Vec<Vec<u8>>, KolaError>>()?;
            // 99 + symbol list(6) + values(6)
            let k_length = 13
                + dict.keys().map(|k| k.len() + 1).sum::<usize>()
                + vectors.iter().map(|v| v.len()).sum::<usize>();
            vec = Vec::with_capacity(k_length);
            vec.write_all(&[99, 11, 0]).unwrap();
            vec.write_all(&length.to_le_bytes()).unwrap();
            keys.into_iter().for_each(|k| {
                vec.write_all(k.as_bytes()).unwrap();
                vec.write_all(&[0]).unwrap();
            });
            vec.write_all(&[0, 0]).unwrap();
            vec.write_all(&length.to_le_bytes()).unwrap();
            vectors.into_iter().for_each(|v| {
                vec.write_all(&v).unwrap();
            });
        }
    };
    Ok(vec)
}

fn serialize_fixed_size_list<T: NativeType>(
    vec: &mut Vec<u8>,
    array: &FixedSizeListArray,
    k_type: u8,
    size: usize,
    null: T,
) {
    let array = unsafe {
        array
            .values()
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .unwrap_unchecked()
    };
    let array = set_at_nulls(array, null);
    let array = array.values();
    let k_size = K_TYPE_SIZE[k_type as usize];
    let v8: &[u8] =
        unsafe { core::slice::from_raw_parts(array.as_ptr().cast(), array.len() * k_size) };
    let len_vec = (size as i32).to_le_bytes();
    if size == 0 {
        return;
    }
    for sub_v8 in v8.chunks_exact(size * k_size) {
        vec.write_all(&[k_type, 0]).unwrap();
        vec.write_all(&len_vec).unwrap();
        vec.write_all(sub_v8).unwrap();
    }
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
            vec.write_all(&[1, 0]).unwrap();
            vec.write_all(&(k_length as i32).to_le_bytes()).unwrap();
            let ptr = series.to_physical_repr();
            let chunks = &ptr.bool().unwrap().chunks();
            chunks.iter().for_each(|array| {
                let array = unsafe {
                    array
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap_unchecked()
                        .values()
                };
                array.iter().for_each(|b| {
                    if b {
                        vec.write_all(&[1u8]).unwrap();
                    } else {
                        vec.write_all(&[0u8]).unwrap();
                    }
                });
            })
        }
        PolarsDataType::UInt8 => {
            vec.write_all(&[4, 0]).unwrap();
            vec.write_all(&(k_length as i32).to_le_bytes()).unwrap();
            let ptr = series.to_physical_repr();
            let chunks = &ptr.u8().unwrap().chunks();
            chunks.iter().for_each(|array| {
                let array = unsafe {
                    array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<u8>>()
                        .unwrap_unchecked()
                        .values()
                };
                let v8 = unsafe { core::slice::from_raw_parts(array.as_ptr(), array.len()) };
                vec.write_all(v8).unwrap();
            })
        }
        PolarsDataType::Int16 => {
            k_size = 2;
            vec.write_all(&[5, 0]).unwrap();
            vec.write_all(&(k_length as i32).to_le_bytes()).unwrap();
            let chunks = series.i16().unwrap();
            let chunks = if chunks.null_count() > 0 {
                chunks.fill_null_with_values(i16::MIN).unwrap()
            } else {
                chunks.clone()
            };
            chunks.chunks().iter().for_each(|array| {
                let array = unsafe {
                    array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<i16>>()
                        .unwrap_unchecked()
                        .values()
                };
                let v8 = unsafe {
                    core::slice::from_raw_parts(array.as_ptr().cast(), array.len() * k_size)
                };
                vec.write_all(v8).unwrap();
            })
        }
        PolarsDataType::Int32 => {
            k_size = 4;
            vec.write_all(&[6, 0]).unwrap();
            vec.write_all(&(k_length as i32).to_le_bytes()).unwrap();
            let chunks = series.i32().unwrap();
            let chunks = if chunks.null_count() > 0 {
                chunks.fill_null_with_values(i32::MIN).unwrap()
            } else {
                chunks.clone()
            };
            chunks.chunks().iter().for_each(|array| {
                let array = unsafe {
                    array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<i32>>()
                        .unwrap_unchecked()
                        .values()
                };
                let v8 = unsafe {
                    core::slice::from_raw_parts(array.as_ptr().cast(), array.len() * k_size)
                };
                vec.write_all(v8).unwrap();
            })
        }
        PolarsDataType::Int64 => {
            k_size = 8;
            vec.write_all(&[7, 0]).unwrap();
            vec.write_all(&(k_length as i32).to_le_bytes()).unwrap();
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
            chunks.iter().for_each(|array| {
                let array = unsafe {
                    array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<i64>>()
                        .unwrap_unchecked()
                        .values()
                };
                let v8 = unsafe {
                    core::slice::from_raw_parts(array.as_ptr().cast(), array.len() * k_size)
                };
                vec.write_all(v8).unwrap();
            })
        }
        PolarsDataType::Float32 => {
            k_size = 4;
            vec.write_all(&[8, 0]).unwrap();
            vec.write_all(&(k_length as i32).to_le_bytes()).unwrap();
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
            chunks.iter().for_each(|array| {
                let array = unsafe {
                    array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<f32>>()
                        .unwrap_unchecked()
                        .values()
                };
                let v8 = unsafe {
                    core::slice::from_raw_parts(array.as_ptr().cast(), array.len() * k_size)
                };
                vec.write_all(v8).unwrap();
            })
        }
        PolarsDataType::Float64 => {
            k_size = 8;
            vec.write_all(&[9, 0]).unwrap();
            vec.write_all(&(k_length as i32).to_le_bytes()).unwrap();
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
            chunks.iter().for_each(|array| {
                let array = unsafe {
                    array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<f64>>()
                        .unwrap_unchecked()
                        .values()
                };
                let v8 = unsafe {
                    core::slice::from_raw_parts(array.as_ptr().cast(), array.len() * k_size)
                };
                vec.write_all(v8).unwrap();
            })
        }
        PolarsDataType::String => {
            vec.write_all(&[0, 0]).unwrap();
            vec.write_all(&(k_length as i32).to_le_bytes()).unwrap();
            let ptr = series.to_physical_repr();
            let array = ptr.str().unwrap();
            array.chunks().iter().for_each(|arr| {
                let arr = &**arr;
                let arr = unsafe { &*(arr as *const dyn Array as *const Utf8ViewArray) };
                arr.into_iter().for_each(|s| {
                    vec.write_all(&[10, 0]).unwrap();
                    match s {
                        Some(s) => {
                            vec.write_all(&(s.len() as u32).to_le_bytes()).unwrap();
                            let v8 =
                                unsafe { core::slice::from_raw_parts(s.as_ptr().cast(), s.len()) };
                            vec.write_all(v8).unwrap();
                        }
                        None => {
                            vec.write_all(&[0, 0, 0, 0]).unwrap();
                        }
                    }
                });
            });
        }
        PolarsDataType::Date => {
            // max date - 95026601
            k_size = 4;
            vec.write_all(&[14, 0]).unwrap();
            vec.write_all(&(k_length as i32).to_le_bytes()).unwrap();
            let chunks = series.cast(&PolarsDataType::Int32).unwrap();
            let chunks = chunks.i32().unwrap();
            let chunks = if chunks.null_count() > 0 {
                chunks.fill_null_with_values(i32::MIN).unwrap()
            } else {
                chunks.clone()
            };
            chunks.chunks().iter().for_each(|array| {
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
                    core::slice::from_raw_parts(array.as_ptr().cast(), array.len() * k_size)
                };
                vec.write_all(v8).unwrap();
            })
        }
        PolarsDataType::Datetime(unit, _) => {
            k_size = 8;
            let chunks = &series.cast(&PolarsDataType::Int64).unwrap();
            let chunks = chunks.i64().unwrap();
            let chunks = if chunks.null_count() > 0 {
                chunks.fill_null_with_values(i64::MIN).unwrap()
            } else {
                chunks.clone()
            };
            match unit {
                PolarTimeUnit::Milliseconds => {
                    // serialize as kdb datetime (type 15, f64 fractional days since 2000.01.01)
                    vec.write_all(&[15, 0]).unwrap();
                    vec.write_all(&(k_length as i32).to_le_bytes()).unwrap();
                    chunks.chunks().iter().for_each(|array| {
                        let buffer = unsafe {
                            array
                                .as_any()
                                .downcast_ref::<PrimitiveArray<i64>>()
                                .unwrap_unchecked()
                                .values()
                        };
                        let array: Vec<f64> = buffer
                            .as_slice()
                            .iter()
                            .map(|d| {
                                if *d == i64::MIN {
                                    f64::NAN
                                } else {
                                    *d as f64 / MS_PER_DAY - 10957.0
                                }
                            })
                            .collect();
                        let v8 = unsafe {
                            core::slice::from_raw_parts(array.as_ptr().cast(), array.len() * k_size)
                        };
                        vec.write_all(v8).unwrap();
                    })
                }
                _ => {
                    // serialize as kdb timestamp (type 12, i64 nanoseconds)
                    vec.write_all(&[12, 0]).unwrap();
                    vec.write_all(&(k_length as i32).to_le_bytes()).unwrap();
                    let multiplier = match unit {
                        PolarTimeUnit::Nanoseconds => 1,
                        PolarTimeUnit::Microseconds => 1000,
                        PolarTimeUnit::Milliseconds => unreachable!(),
                    };
                    chunks.chunks().iter().for_each(|array| {
                        let buffer = unsafe {
                            array
                                .as_any()
                                .downcast_ref::<PrimitiveArray<i64>>()
                                .unwrap_unchecked()
                                .values()
                        };
                        let array: Vec<i64> = buffer
                            .as_slice()
                            .iter()
                            .map(|d| {
                                if *d == i64::MIN {
                                    *d
                                } else {
                                    d.saturating_mul(multiplier).saturating_sub(NANOS_DIFF)
                                }
                            })
                            .collect();
                        let v8 = unsafe {
                            core::slice::from_raw_parts(array.as_ptr().cast(), array.len() * k_size)
                        };
                        vec.write_all(v8).unwrap();
                    })
                }
            }
        }
        PolarsDataType::Duration(_) => {
            k_size = 8;
            vec.write_all(&[16, 0]).unwrap();
            vec.write_all(&(k_length as i32).to_le_bytes()).unwrap();
            let chunks = &series.cast(&PolarsDataType::Int64).unwrap();
            let chunks = chunks.i64().unwrap();
            let chunks = if chunks.null_count() > 0 {
                chunks.fill_null_with_values(i64::MIN).unwrap()
            } else {
                chunks.clone()
            };
            chunks.chunks().iter().for_each(|array| {
                let array = unsafe {
                    array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<i64>>()
                        .unwrap_unchecked()
                        .values()
                };
                let v8 = unsafe {
                    core::slice::from_raw_parts(array.as_ptr().cast(), array.len() * k_size)
                };
                vec.write_all(v8).unwrap();
            })
        }
        PolarsDataType::Time => {
            k_size = 4;
            vec.write_all(&[19, 0]).unwrap();
            vec.write_all(&(k_length as i32).to_le_bytes()).unwrap();
            let chunks = &series.cast(&PolarsDataType::Int64).unwrap();
            let chunks = chunks.i64().unwrap();
            let chunks = if chunks.null_count() > 0 {
                chunks.fill_null_with_values(i64::MIN).unwrap()
            } else {
                chunks.clone()
            };
            chunks.chunks().iter().for_each(|array| {
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
                            (d / 1_000_000) as i32
                        }
                    })
                    .collect();

                let v8 = unsafe {
                    core::slice::from_raw_parts(array.as_ptr().cast(), array.len() * k_size)
                };
                vec.write_all(v8).unwrap();
            })
        }
        PolarsDataType::Array(data_type, size) => {
            vec.write_all(&[0, 0]).unwrap();
            vec.write_all(&(k_length as i32).to_le_bytes()).unwrap();
            let chunks = series.array().unwrap().rechunk();
            let array = unsafe {
                chunks.chunks()[0]
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
                            vec.write_all(&[1, 0]).unwrap();
                            vec.write_all(&len_vec).unwrap();
                        }
                        if b {
                            vec.write_all(&[1u8]).unwrap();
                        } else {
                            vec.write_all(&[0u8]).unwrap();
                        }
                    }
                }
                PolarsDataType::UInt8 => serialize_fixed_size_list(&mut vec, array, 4, *size, 0u8),
                PolarsDataType::Int16 => {
                    serialize_fixed_size_list(&mut vec, array, 5, *size, i16::MIN)
                }
                PolarsDataType::Int32 => {
                    serialize_fixed_size_list(&mut vec, array, 6, *size, i32::MIN)
                }
                PolarsDataType::Int64 => {
                    serialize_fixed_size_list(&mut vec, array, 7, *size, i64::MIN)
                }
                PolarsDataType::Float32 => {
                    serialize_fixed_size_list(&mut vec, array, 8, *size, f32::NAN)
                }
                PolarsDataType::Float64 => {
                    serialize_fixed_size_list(&mut vec, array, 9, *size, f64::NAN)
                }
                _ => {
                    return Err(KolaError::NotSupportedPolarsNestedListTypeErr(
                        data_type.as_ref().clone(),
                    ))
                }
            }
        }
        PolarsDataType::List(data_type) => {
            vec.write_all(&[0, 0]).unwrap();
            vec.write_all(&(k_length as i32).to_le_bytes()).unwrap();
            let chunks = series.list().unwrap().rechunk();
            let list = unsafe {
                chunks.chunks()[0]
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
                        vec.write_all(&[1, 0]).unwrap();
                        vec.write_all(&((offsets[i + 1] - offsets[i]) as i32).to_le_bytes())
                            .unwrap();
                        for j in start_offset..end_offset {
                            if list.get_bit(j) {
                                vec.write_all(&[1u8]).unwrap();
                            } else {
                                vec.write_all(&[0u8]).unwrap();
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
                        vec.write_all(&[4, 0]).unwrap();
                        vec.write_all(&((offsets[i + 1] - offsets[i]) as i32).to_le_bytes())
                            .unwrap();
                        vec.write_all(&list[start_offset..end_offset]).unwrap();
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
                        core::slice::from_raw_parts(array.as_ptr().cast(), array.len() * k_size)
                    };
                    for i in 0..k_length {
                        let start_offset = k_size * offsets[i] as usize;
                        let end_offset = k_size * offsets[i + 1] as usize;
                        vec.write_all(&[k_type, 0]).unwrap();
                        vec.write_all(&((offsets[i + 1] - offsets[i]) as i32).to_le_bytes())
                            .unwrap();
                        vec.write_all(&v8[start_offset..end_offset]).unwrap();
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
                        core::slice::from_raw_parts(array.as_ptr().cast(), array.len() * k_size)
                    };
                    for i in 0..k_length {
                        let start_offset = k_size * offsets[i] as usize;
                        let end_offset = k_size * offsets[i + 1] as usize;
                        vec.write_all(&[k_type, 0]).unwrap();
                        vec.write_all(&((offsets[i + 1] - offsets[i]) as i32).to_le_bytes())
                            .unwrap();
                        vec.write_all(&v8[start_offset..end_offset]).unwrap();
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
                        core::slice::from_raw_parts(array.as_ptr().cast(), array.len() * k_size)
                    };
                    for i in 0..k_length {
                        let start_offset = k_size * offsets[i] as usize;
                        let end_offset = k_size * offsets[i + 1] as usize;
                        vec.write_all(&[k_type, 0]).unwrap();
                        vec.write_all(&((offsets[i + 1] - offsets[i]) as i32).to_le_bytes())
                            .unwrap();
                        vec.write_all(&v8[start_offset..end_offset]).unwrap();
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
                        core::slice::from_raw_parts(array.as_ptr().cast(), array.len() * k_size)
                    };
                    for i in 0..k_length {
                        let start_offset = k_size * offsets[i] as usize;
                        let end_offset = k_size * offsets[i + 1] as usize;
                        vec.write_all(&[k_type, 0]).unwrap();
                        vec.write_all(&((offsets[i + 1] - offsets[i]) as i32).to_le_bytes())
                            .unwrap();
                        vec.write_all(&v8[start_offset..end_offset]).unwrap();
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
                        core::slice::from_raw_parts(array.as_ptr().cast(), array.len() * k_size)
                    };
                    for i in 0..k_length {
                        let start_offset = k_size * offsets[i] as usize;
                        let end_offset = k_size * offsets[i + 1] as usize;
                        vec.write_all(&[k_type, 0]).unwrap();
                        vec.write_all(&((offsets[i + 1] - offsets[i]) as i32).to_le_bytes())
                            .unwrap();
                        vec.write_all(&v8[start_offset..end_offset]).unwrap();
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
            vec.write_all(&[11, 0]).unwrap();
            vec.write_all(&(k_length as i32).to_le_bytes()).unwrap();
            let cat = series.cat32().unwrap();
            cat.iter_str()
                .map(|s| {
                    if let Some(s) = s {
                        [s.as_bytes(), &[0u8]].concat()
                    } else {
                        vec![0u8]
                    }
                })
                .for_each(|v| {
                    vec.write_all(&v).unwrap();
                });
        }
        PolarsDataType::Binary => {
            vec.write_all(&[2, 0]).unwrap();
            vec.write_all(&(k_length as i32).to_le_bytes()).unwrap();
            let array = series.binary().unwrap();
            array.chunks().iter().for_each(|arr| {
                let arr = &**arr;
                let arr = unsafe { &*(arr as *const dyn Array as *const BinaryViewArray) };
                arr.into_iter().for_each(|b| match b {
                    Some(b) => {
                        vec.write_all(b).unwrap();
                    }
                    None => {
                        vec.write_all(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
                            .unwrap();
                    }
                });
            });
        }
        PolarsDataType::Null if k_length == 0 => {
            vec.write_all(&[0, 0, 0, 0, 0, 0]).unwrap();
        }
        _ => return Err(KolaError::NotSupportedSeriesTypeErr(series.dtype().clone())),
    }
    Ok(vec)
}

#[cfg(test)]
mod tests {
    use indexmap::IndexMap;
    use polars::prelude::{CompatLevel, NamedFrom};
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
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
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
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
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
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_short_list() {
        let vec = [5, 0, 4, 0, 0, 0, 0, 128, 1, 128, 0, 0, 255, 127].to_vec();
        let k = deserialize(&vec, &mut 0, false).unwrap();
        let name = K_TYPE_NAME[vec[0] as usize];
        let expect = Series::from_arrow(
            name.into(),
            Int16Array::from([None, None, Some(0), None]).boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        let vec = [5, 0, 4, 0, 0, 0, 0, 128, 0, 128, 0, 0, 0, 128].to_vec();
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
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
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
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
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
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
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
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
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
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
                Categories::global(),
                Categories::global().mapping(),
            ))
            .unwrap();
        assert_eq!(
            series.to_arrow(0, CompatLevel::newest()),
            expect.to_arrow(0, CompatLevel::newest())
        );
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
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
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
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
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
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
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
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
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
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
                vec![i64::MIN, 45_240_000_000_000, 0i64, NANOS_PER_DAY - 1].into(),
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
                vec![i64::MIN, 45_296_000_000_000, 0i64, NANOS_PER_DAY - 1].into(),
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
                vec![i64::MIN, 45_296_789_000_000, 0i64, 86_399_999_000_000].into(),
                Some(Bitmap::from([false, true, true, true])),
            )
            .boxed(),
        )
        .unwrap();
        let series: Series = k.try_into().unwrap();
        assert_eq!(series, expect);
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
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
        let array = BooleanArray::from([true; 6].map(Some));
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
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
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
        let array = BooleanArray::from([true, false, true, false, true, false].map(Some));
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
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
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
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
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
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
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
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
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
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
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
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
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
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
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
        assert_eq!(vec, serialize(&K::Series(expect)).unwrap());
    }

    #[test]
    fn deserialize_and_serialize_mixed_list() {
        let vec = [
            0, 0, 3, 0, 0, 0, 245, 117, 112, 100, 0, 245, 116, 0, 98, 0, 99, 11, 0, 1, 0, 0, 0, 97,
            0, 0, 0, 1, 0, 0, 0, 7, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
        ]
        .to_vec();
        let k = deserialize(&vec, &mut 0, false).unwrap();
        let expect = K::MixedList(vec![
            K::Symbol("upd".to_owned()),
            K::Symbol("t".to_owned()),
            K::DataFrame(
                DataFrame::new_infer_height(vec![Series::new("a".into(), [1i64].as_ref()).into()])
                    .unwrap(),
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
        let expect = DataFrame::new_infer_height(vec![s0.into(), s1.into()]).unwrap();
        assert_eq!(df, expect);
        assert_eq!(vec, serialize(&K::DataFrame(expect)).unwrap());
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
        let expect = DataFrame::new_infer_height(vec![s0.into(), s1.into()]).unwrap();
        assert_eq!(df, expect);
    }

    #[test]
    fn serialize_bool() {
        let k = K::Boolean(true);
        assert_eq!(serialize(&k).unwrap(), [255, 1]);
    }

    #[test]
    fn serialize_guid() {
        let k = K::Guid(
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
        let k = K::U8(99);
        assert_eq!(serialize(&k).unwrap(), [252, 99]);
    }

    #[test]
    fn serialize_short() {
        let k = K::I16(99);
        assert_eq!(serialize(&k).unwrap(), [251, 99, 0]);
    }

    #[test]
    fn serialize_int() {
        let k = K::I32(99999999);
        assert_eq!(serialize(&k).unwrap(), [250, 255, 224, 245, 5]);
    }

    #[test]
    fn serialize_long() {
        let k = K::I64(9999_9999_9999_9999);
        assert_eq!(
            serialize(&k).unwrap(),
            [249, 255, 255, 192, 111, 242, 134, 35, 0]
        );
    }

    #[test]
    fn serialize_real() {
        let k = K::F32(9.9e10);
        assert_eq!(serialize(&k).unwrap(), [248, 225, 102, 184, 81]);
    }

    #[test]
    fn serialize_float() {
        let k = K::F64(9.9e10);
        assert_eq!(serialize(&k).unwrap(), [247, 0, 0, 0, 30, 220, 12, 55, 66]);
    }

    #[test]
    fn serialize_symbol() {
        let k = K::Symbol("abc".to_string());
        assert_eq!(serialize(&k).unwrap(), [245, 97, 98, 99, 0]);
    }

    #[test]
    fn serialize_string() {
        let k = K::String("abc".to_string());
        assert_eq!(serialize(&k).unwrap(), [10, 0, 3, 0, 0, 0, 97, 98, 99]);
    }

    #[test]
    fn serialize_timestamp() {
        let k = K::DateTime(DateTime::<Utc>::from_timestamp(0, 123456789).unwrap());
        assert_eq!(
            serialize(&k).unwrap(),
            [244, 21, 205, 24, 181, 48, 179, 220, 242]
        );
    }

    #[test]
    fn serialize_date() {
        let k = K::Date(NaiveDate::from_ymd_opt(2023, 11, 15).unwrap());
        assert_eq!(serialize(&k).unwrap(), [242, 15, 34, 0, 0]);
    }

    #[test]
    fn serialize_time() {
        let k = K::Time(NaiveTime::from_hms_milli_opt(0, 17, 24, 70).unwrap());
        assert_eq!(serialize(&k).unwrap(), [237, 102, 238, 15, 0]);
    }

    #[test]
    fn serialize_duration() {
        let k = K::Duration(Duration::nanoseconds(822896123456789));
        assert_eq!(
            serialize(&k).unwrap(),
            [240, 21, 45, 32, 111, 107, 236, 2, 0]
        );
    }

    #[test]
    fn serialize_none() {
        let k = K::Null;
        assert_eq!(serialize(&k).unwrap(), [101, 0]);
    }

    #[test]
    fn k101_round_trip() {
        for code in (0u8..=44).chain(std::iter::once(255)) {
            let bytes = [101, code];
            let mut pos = 0;
            let k = deserialize(&bytes, &mut pos, false).unwrap();
            assert_eq!(pos, bytes.len());
            assert_eq!(k.j6_len().unwrap(), bytes.len());
            assert_eq!(serialize(&k).unwrap(), bytes);
            if code == 0 {
                assert_eq!(k, K::Null);
            } else {
                let operator = Operator::try_from(code).unwrap();
                assert_eq!(k, K::Operator(operator));
                assert_eq!(Operator::try_from(operator.as_str()).unwrap(), operator);
            }
        }
    }

    #[test]
    fn k102_round_trip() {
        let names = [
            ":", "+", "-", "*", "%", "&", "|", "^", "=", "<", ">", "$", ",", "#", "_", "~", "!",
            "?", "@", ".", "0:", "1:", "2:", "in", "within", "like", "bin", "ss", "insert", "wsum",
            "wavg", "div", "xexp", "setenv", "binr", "cov", "cor",
        ];
        for (code, name) in names.into_iter().enumerate() {
            let bytes = [102, code as u8];
            let operator = Operator::try_from(name).unwrap();
            assert_eq!(operator.k_type(), 102);
            assert_eq!(operator.code(), code as u8);
            assert_eq!(operator.as_str(), name);
            let k = K::Operator(operator);
            let mut pos = 0;
            assert_eq!(deserialize(&bytes, &mut pos, false).unwrap(), k);
            assert_eq!(pos, 2);
            assert_eq!(k.j6_len().unwrap(), 2);
            assert_eq!(serialize(&k).unwrap(), bytes);
        }
        for code in 37..=255 {
            assert!(matches!(deserialize(&[102, code], &mut 0, false),
                Err(KolaError::NotSupportedKOperatorErr(value)) if value == code));
        }
        assert!(matches!(
            deserialize(&[102], &mut 0, false),
            Err(KolaError::DeserializationErr(_))
        ));
    }

    #[test]
    fn unary_operator_aliases() {
        for (name, code) in [
            ("flip", 1),
            ("neg", 2),
            ("first", 3),
            ("reciprocal", 4),
            ("ltime", 4),
            ("where", 5),
            ("reverse", 6),
            ("null", 7),
            ("group", 8),
            ("hclose", 10),
            ("string", 11),
            ("count", 13),
            ("floor", 14),
            ("not", 15),
            ("hdel", 15),
            ("key", 16),
            ("inv", 16),
            ("distinct", 17),
            ("type", 18),
            ("value", 19),
            ("get", 19),
            ("read0", 20),
            ("read1", 21),
        ] {
            let operator = Operator::try_from(name).unwrap();
            assert_eq!(serialize(&K::Operator(operator)).unwrap(), [101, code]);
        }
    }

    #[test]
    fn binary_keyword_aliases() {
        for (name, code) in [("and", 5), ("or", 6), ("lsq", 16), ("mmu", 11)] {
            let operator = Operator::try_from(name).unwrap();
            assert_eq!(serialize(&K::Operator(operator)).unwrap(), [102, code]);
        }
    }

    #[test]
    fn k102_mixed_list() {
        let bytes = [0, 0, 4, 0, 0, 0, 102, 0, 101, 0, 102, 1, 101, 1];
        let mut pos = 0;
        let k = deserialize(&bytes, &mut pos, false).unwrap();
        assert_eq!(pos, bytes.len());
        assert_eq!(
            k,
            K::MixedList(vec![
                K::Operator(Operator::try_from(":").unwrap()),
                K::Null,
                K::Operator(Operator::try_from("+").unwrap()),
                K::Operator(Operator::try_from("+:").unwrap()),
            ])
        );
        assert_eq!(serialize(&k).unwrap(), bytes);
    }

    #[test]
    fn k101_names() {
        for (name, code) in [
            ("+:", 1),
            (".:", 19),
            ("0::", 20),
            ("2::", 22),
            ("avg", 23),
            ("sum", 25),
            ("enlist", 41),
            ("hopen", 44),
            ("::", 255),
        ] {
            let operator = Operator::try_from(name).unwrap();
            assert_eq!(operator.code(), code);
            assert_eq!(operator.as_str(), name);
            assert_eq!(serialize(&K::Operator(operator)).unwrap(), [101, code]);
        }
        assert!(Operator::try_from("unknown").is_err());
        assert!(Operator::try_from(0u8).is_err());
    }

    #[test]
    fn k101_invalid_input() {
        for code in 45..=254 {
            assert!(matches!(
                deserialize(&[101, code], &mut 0, false),
                Err(KolaError::NotSupportedKOperatorErr(value)) if value == code
            ));
        }
        assert!(matches!(
            deserialize(&[101], &mut 0, false),
            Err(KolaError::DeserializationErr(_))
        ));
    }

    #[test]
    fn k101_mixed_list() {
        let bytes = [0, 0, 4, 0, 0, 0, 101, 25, 101, 0, 101, 255, 255, 1];
        let expected = K::MixedList(vec![
            K::Operator(Operator::try_from("sum").unwrap()),
            K::Null,
            K::Operator(Operator::try_from("::").unwrap()),
            K::Boolean(true),
        ]);
        let mut pos = 0;
        assert_eq!(deserialize(&bytes, &mut pos, false).unwrap(), expected);
        assert_eq!(pos, bytes.len());
        assert_eq!(expected.j6_len().unwrap(), bytes.len());
        assert_eq!(serialize(&expected).unwrap(), bytes);
    }

    #[test]
    fn deserialize_and_serialize_dict() {
        let vec = [
            99, 11, 0, 2, 0, 0, 0, 97, 0, 98, 0, 0, 0, 2, 0, 0, 0, 249, 1, 0, 0, 0, 0, 0, 0, 0,
            247, 0, 0, 0, 0, 0, 0, 240, 63,
        ]
        .to_vec();
        let mut dict = IndexMap::with_capacity(2);
        dict.insert("a".to_string(), K::I64(1));
        dict.insert("b".to_string(), K::F64(1.0));
        let k = K::Dict(dict);
        assert_eq!(deserialize(&vec, &mut 0, false).unwrap(), k);
        assert_eq!(vec, serialize(&k).unwrap());
    }

    #[test]
    fn serialize_multi_chunk_series() {
        let series = [
            Series::new("".into(), [1u8, 2, 3]),
            Series::new("".into(), [Some(1i16), None, Some(3)]),
            Series::new("".into(), [Some(1i32), None, Some(3)]),
            Series::new("".into(), [Some(1i64), None, Some(3)]),
            Series::new("".into(), [Some(1.0f32), None, Some(3.0)]),
            Series::new("".into(), [Some(1.0f64), None, Some(3.0)]),
            Series::new("".into(), [Some(1i32), None, Some(3)])
                .cast(&PolarsDataType::Date)
                .unwrap(),
            Series::new("".into(), [Some(1i64), None, Some(3)])
                .cast(&PolarsDataType::Datetime(PolarTimeUnit::Nanoseconds, None))
                .unwrap(),
            Series::new("".into(), [Some(1i64), None, Some(3)])
                .cast(&PolarsDataType::Datetime(PolarTimeUnit::Milliseconds, None))
                .unwrap(),
            Series::new("".into(), [Some(1i64), None, Some(3)])
                .cast(&PolarsDataType::Duration(PolarTimeUnit::Nanoseconds))
                .unwrap(),
            Series::new("".into(), [Some(1i64), None, Some(3)])
                .cast(&PolarsDataType::Time)
                .unwrap(),
        ];
        for s in series {
            let mut multi_chunk = s.clone();
            multi_chunk.append(&s.head(Some(2))).unwrap();
            assert_eq!(multi_chunk.n_chunks(), 2);
            let single_chunk = multi_chunk.rechunk();
            assert_eq!(
                serialize(&K::Series(multi_chunk)).unwrap(),
                serialize(&K::Series(single_chunk)).unwrap(),
                "dtype: {}",
                s.dtype()
            );
        }
    }

    #[test]
    fn serialize_ragged_nested_list() {
        // more inner values than lists
        let series = Series::new(
            "".into(),
            [
                Series::new("".into(), [1i64, 2, 3]),
                Series::new("".into(), [4i64, 5]),
            ],
        );
        let vec = [
            0, 0, 2, 0, 0, 0, 7, 0, 3, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3,
            0, 0, 0, 0, 0, 0, 0, 7, 0, 2, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0,
        ]
        .to_vec();
        assert_eq!(vec, serialize(&K::Series(series.clone())).unwrap());

        let mut multi_chunk = series.clone();
        multi_chunk.append(&series).unwrap();
        assert_eq!(multi_chunk.n_chunks(), 2);
        let single_chunk = multi_chunk.rechunk();
        assert_eq!(
            serialize(&K::Series(multi_chunk)).unwrap(),
            serialize(&K::Series(single_chunk)).unwrap()
        );
    }

    #[test]
    fn serialize_bool_nested_list_with_false() {
        let series = Series::new(
            "".into(),
            [
                Series::new("".into(), [false, true, false, false]),
                Series::new("".into(), [false]),
            ],
        );
        let vec = [
            0, 0, 2, 0, 0, 0, 1, 0, 4, 0, 0, 0, 0, 1, 0, 0, 1, 0, 1, 0, 0, 0, 0,
        ]
        .to_vec();
        assert_eq!(vec, serialize(&K::Series(series)).unwrap());
    }

    #[test]
    fn deserialize_mixed_list_starting_with_symbol_list() {
        // (`a`b;1 2 3)
        let vec = [
            0, 0, 2, 0, 0, 0, 11, 0, 2, 0, 0, 0, 97, 0, 98, 0, 7, 0, 3, 0, 0, 0, 1, 0, 0, 0, 0, 0,
            0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
        ]
        .to_vec();
        let mut pos = 0;
        let k = deserialize(&vec, &mut pos, false).unwrap();
        assert_eq!(pos, vec.len());
        match k {
            K::MixedList(mut list) => {
                assert_eq!(list.len(), 2);
                let longs: Series = list.pop().unwrap().try_into().unwrap();
                assert_eq!(longs, Series::new("long".into(), [1i64, 2, 3]));
            }
            _ => panic!("expected a mixed list"),
        }
    }

    #[test]
    fn j6_len_matches_serialized_len() {
        let ints = Series::new("".into(), [Some(1i32), None, Some(3)]);
        let longs = Series::new("".into(), [Some(1i64), None, Some(3)]);
        let mut multi_chunk_list = Series::new(
            "".into(),
            [
                Series::new("".into(), [1i64, 2, 3]),
                Series::new("".into(), [4i64, 5]),
            ],
        );
        multi_chunk_list
            .append(&multi_chunk_list.head(Some(1)))
            .unwrap();
        let series = [
            Series::new("".into(), [true, false, true]),
            Series::new("".into(), [1u8, 2, 3]),
            Series::new("".into(), [Some("a"), None, Some("long string")]),
            ints.cast(&PolarsDataType::Date).unwrap(),
            longs.cast(&PolarsDataType::Time).unwrap(),
            longs
                .cast(&PolarsDataType::Datetime(PolarTimeUnit::Nanoseconds, None))
                .unwrap(),
            longs
                .cast(&PolarsDataType::Duration(PolarTimeUnit::Nanoseconds))
                .unwrap(),
            Series::new("".into(), [[0u8; 16].as_ref(), [1u8; 16].as_ref()]),
            multi_chunk_list,
        ];
        for s in series {
            let dtype = s.dtype().clone();
            let k = K::Series(s);
            assert_eq!(
                k.j6_len().unwrap(),
                serialize(&k).unwrap().len(),
                "dtype: {}",
                dtype
            );
        }
    }

    #[test]
    fn deserialize_char_value_dict() {
        // `a`b`c!"xyz"
        let vec = [
            99, 11, 0, 3, 0, 0, 0, 97, 0, 98, 0, 99, 0, 10, 0, 3, 0, 0, 0, 120, 121, 122,
        ]
        .to_vec();
        let mut dict = IndexMap::new();
        dict.insert("a".to_owned(), K::Char(b'x'));
        dict.insert("b".to_owned(), K::Char(b'y'));
        dict.insert("c".to_owned(), K::Char(b'z'));
        assert_eq!(deserialize(&vec, &mut 0, false).unwrap(), K::Dict(dict));
    }

    #[test]
    fn deserialize_not_supported_column_type() {
        // ([]a:2000.01 2000.02m)
        let month_table = [
            98, 0, 99, 11, 0, 1, 0, 0, 0, 97, 0, 0, 0, 1, 0, 0, 0, 13, 0, 2, 0, 0, 0, 0, 0, 0, 0,
            1, 0, 0, 0,
        ]
        .to_vec();
        assert!(deserialize(&month_table, &mut 0, false).is_err());
        // enumerated symbol column
        let enum_table = [
            98, 0, 99, 11, 0, 1, 0, 0, 0, 97, 0, 0, 0, 1, 0, 0, 0, 20, 0, 1, 0, 0, 0, 0, 0, 0, 0,
        ]
        .to_vec();
        assert!(deserialize(&enum_table, &mut 0, false).is_err());
    }

    #[test]
    fn deserialize_null_and_infinity_temporal_atoms() {
        // 0Nm
        let vec = [243, 0, 0, 0, 128].to_vec();
        assert_eq!(
            deserialize(&vec, &mut 0, false).unwrap(),
            K::Date(NaiveDate::MIN)
        );
        // 0Wm
        let vec = [243, 255, 255, 255, 127].to_vec();
        assert_eq!(
            deserialize(&vec, &mut 0, false).unwrap(),
            K::Date(NaiveDate::MAX)
        );
        // 1999.12m
        let vec = [243, 255, 255, 255, 255].to_vec();
        assert_eq!(
            deserialize(&vec, &mut 0, false).unwrap(),
            K::Date(NaiveDate::from_ymd_opt(1999, 12, 1).unwrap())
        );
        // 0Nz, same as 0Np
        let null_datetime = [241, 0, 0, 0, 0, 0, 0, 248, 255].to_vec();
        let null_timestamp = [244, 0, 0, 0, 0, 0, 0, 0, 128].to_vec();
        assert_eq!(
            deserialize(&null_datetime, &mut 0, false).unwrap(),
            deserialize(&null_timestamp, &mut 0, false).unwrap()
        );
        // 0Wz, same as 0Wp
        let inf_datetime = [241, 0, 0, 0, 0, 0, 0, 240, 127].to_vec();
        let inf_timestamp = [244, 255, 255, 255, 255, 255, 255, 255, 127].to_vec();
        assert_eq!(
            deserialize(&inf_datetime, &mut 0, false).unwrap(),
            deserialize(&inf_timestamp, &mut 0, false).unwrap()
        );
    }

    #[test]
    fn serialize_long_nested_array() {
        let vec = [
            0, 0, 2, 0, 0, 0, 7, 0, 2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128,
            7, 0, 2, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0,
        ]
        .to_vec();
        let series = Series::new(
            "".into(),
            [
                Series::new("".into(), [Some(1i64), None]),
                Series::new("".into(), [Some(3i64), Some(4)]),
            ],
        )
        .cast(&PolarsDataType::Array(Box::new(PolarsDataType::Int64), 2))
        .unwrap();
        let k = K::Series(series);
        assert_eq!(k.j6_len().unwrap(), vec.len());
        assert_eq!(vec, serialize(&k).unwrap());
    }

    #[test]
    fn deserialize_unterminated_strings() {
        // symbol atom without trailing 0x00
        let vec = [245, 97, 98].to_vec();
        assert_eq!(
            deserialize(&vec, &mut 0, false).unwrap(),
            K::Symbol("ab".to_owned())
        );
        // q error without trailing 0x00, not valid utf8
        let vec = [128, 97, 255].to_vec();
        assert!(matches!(
            deserialize(&vec, &mut 0, false),
            Err(KolaError::ServerErr(_))
        ));
        // symbol list without trailing 0x00
        let vec = [11, 0, 2, 0, 0, 0, 97, 0, 98].to_vec();
        assert!(matches!(
            deserialize(&vec, &mut 0, false),
            Err(KolaError::DeserializationErr(_))
        ));
        // nested symbol list without trailing 0x00
        let vec = [0, 0, 1, 0, 0, 0, 11, 0, 1, 0, 0, 0, 97].to_vec();
        assert!(deserialize(&vec, &mut 0, false).is_err());
    }

    #[test]
    fn deserialize_non_utf8_chars() {
        // ([]a:"a\351")
        let vec = [
            98, 0, 99, 11, 0, 1, 0, 0, 0, 97, 0, 0, 0, 1, 0, 0, 0, 10, 0, 2, 0, 0, 0, 97, 233,
        ]
        .to_vec();
        let df: DataFrame = deserialize(&vec, &mut 0, false)
            .unwrap()
            .try_into()
            .unwrap();
        let expect = Series::new("a".into(), ["a", "\u{FFFD}"]);
        assert_eq!(df.column("a").unwrap().as_materialized_series(), &expect);
        // ("a\351";"bc")
        let vec = [
            0, 0, 2, 0, 0, 0, 10, 0, 2, 0, 0, 0, 97, 233, 10, 0, 2, 0, 0, 0, 98, 99,
        ]
        .to_vec();
        let series: Series = deserialize(&vec, &mut 0, false)
            .unwrap()
            .try_into()
            .unwrap();
        let expect = Series::new("string".into(), ["a\u{FFFD}", "bc"]);
        assert_eq!(series, expect);
    }

    #[test]
    fn serialize_dict_with_not_supported_value() {
        let mut dict = IndexMap::new();
        dict.insert("a".to_owned(), K::Dict(IndexMap::new()));
        assert!(serialize(&K::Dict(dict)).is_err());
    }

    #[test]
    fn serialize_table_with_not_supported_column() {
        let df = DataFrame::new_infer_height(vec![
            Series::new("a".into(), [1i64].as_ref()).into(),
            Series::new("b".into(), [1u64].as_ref()).into(),
        ])
        .unwrap();
        assert!(serialize(&K::DataFrame(df)).is_err());
    }
}
