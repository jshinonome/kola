use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveTime, Timelike, Utc};
use polars::chunked_array::ops::ChunkFillNullValue;
use polars::datatypes::{DataType as PolarsDataType, TimeUnit as PolarTimeUnit};
use polars::prelude::{Categories, DataFrame};
use polars::series::{IntoSeries, Series};
use polars_arrow::array::{
    Array, BinaryViewArray, BooleanArray, FixedSizeBinaryArray, FixedSizeListArray, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, ListArray, PrimitiveArray, UInt8Array,
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
use std::mem::{size_of, size_of_val};
use std::panic::{catch_unwind, AssertUnwindSafe};
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

use crate::{
    errors::KolaError,
    types::{
        get_series_len, validate_guid_series, validate_q_symbol, validate_q_time_series, K,
        K_TYPE_SIZE, MAX_VALUE_DEPTH,
    },
};

fn downcast_array<'a, T: 'static>(
    array: &'a dyn Array,
    expected: &str,
) -> Result<&'a T, KolaError> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| KolaError::NotAbleToSerializeErr(format!("expected Arrow {expected} array")))
}

fn native_values_as_bytes<T: NativeType>(values: &[T]) -> &[u8] {
    // SAFETY: NativeType values are initialized POD scalars. The byte slice uses the
    // same allocation, lifetime, and exact size as the input slice.
    unsafe { core::slice::from_raw_parts(values.as_ptr().cast(), size_of_val(values)) }
}

fn q_list_length(length: i64) -> Result<[u8; 4], KolaError> {
    i32::try_from(length)
        .map(i32::to_le_bytes)
        .map_err(|_| KolaError::OverLengthErr())
}

fn q_list_length_from_usize(length: usize) -> Result<[u8; 4], KolaError> {
    i32::try_from(length)
        .map(i32::to_le_bytes)
        .map_err(|_| KolaError::OverLengthErr())
}

fn q_timestamp_nanoseconds(unix_nanoseconds: i64) -> Result<i64, KolaError> {
    unix_nanoseconds
        .checked_sub(NANOS_DIFF)
        .filter(|value| *value > i64::MIN + 1 && *value < i64::MAX)
        .ok_or_else(|| {
            KolaError::NotAbleToSerializeErr(
                "timestamp is outside q's representable nanosecond range".to_string(),
            )
        })
}
fn unix_timestamp_nanoseconds(q_nanoseconds: i64) -> Result<i64, KolaError> {
    if q_nanoseconds == i64::MAX {
        return Ok(i64::MAX);
    }
    q_nanoseconds.checked_add(NANOS_DIFF).ok_or_else(|| {
        KolaError::DeserializationErr(
            "finite q timestamp is outside the Unix nanosecond range".to_string(),
        )
    })
}

pub fn deserialize(vec: &[u8], pos: &mut usize, is_column: bool) -> Result<K, KolaError> {
    match catch_unwind(AssertUnwindSafe(|| {
        deserialize_unchecked(vec, pos, is_column, 0)
    })) {
        Ok(result) => result,
        Err(_) => Err(KolaError::DeserializationErr(
            "malformed q value caused an internal parser panic".to_string(),
        )),
    }
}

fn deserialize_unchecked(
    vec: &[u8],
    pos: &mut usize,
    is_column: bool,
    depth: usize,
) -> Result<K, KolaError> {
    if depth > MAX_VALUE_DEPTH {
        return Err(KolaError::DeserializationErr(format!(
            "q value nesting exceeds {MAX_VALUE_DEPTH} levels"
        )));
    }
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
                while eod_pos <= vec.len() && vec[eod_pos] != 0 {
                    eod_pos += 1;
                }
                *pos = eod_pos + 1;
                Ok(K::Symbol(
                    String::from_utf8_lossy(&vec[start_pos..eod_pos]).into_owned(),
                ))
            }
            // timestamp
            244 => {
                let q_ns = i64::from_le_bytes(vec[*pos..*pos + 8].try_into().unwrap());
                let ns = if q_ns <= i64::MIN + 1 {
                    0
                } else {
                    unix_timestamp_nanoseconds(q_ns)?
                };
                *pos += 8;
                Ok(K::DateTime(create_datetime(ns)))
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
                Ok(K::Date(
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
                Ok(K::Date(date))
            }
            // datetime
            241 => {
                let unit = f64::from_le_bytes(vec[*pos..*pos + 8].try_into().unwrap());
                let ns = NANOS_DIFF + (unit * NANOS_PER_DAY as f64) as i64;
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
                        let remaining_bytes = vec.len().saturating_sub(*pos);
                        if length > remaining_bytes / 2 {
                            return Err(KolaError::DeserializationErr(format!(
                                "mixed-list count {length} exceeds the available {remaining_bytes}-byte payload"
                            )));
                        }
                        let mut res = Vec::new();
                        res.try_reserve_exact(length).map_err(|error| {
                            KolaError::DeserializationErr(format!(
                                "unable to allocate mixed list with {length} values: {error}"
                            ))
                        })?;
                        for _ in 0..length {
                            res.push(deserialize_unchecked(vec, pos, false, depth + 1)?);
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
                let mut key_df: DataFrame =
                    deserialize_unchecked(vec, pos, true, depth + 1)?.try_into()?;
                let value_df: DataFrame =
                    deserialize_unchecked(vec, pos, true, depth + 1)?.try_into()?;
                key_df = key_df
                    .hstack(value_df.columns())
                    .map_err(|e| KolaError::Err(e.to_string()))?;
                Ok(K::DataFrame(key_df))
            } else if vec[*pos] == 11 {
                *pos += 1;
                let end_pos = calculate_array_end_index(vec, *pos, 11)?;
                let keys: Series = deserialize_series(&vec[*pos..end_pos], 11, true)?.try_into()?;
                *pos = end_pos;
                if vec[end_pos] > 19 {
                    return Err(KolaError::Err(format!(
                        "Not support k type {:?} values in dictionary",
                        vec[end_pos]
                    )));
                }
                let values = deserialize_unchecked(vec, pos, is_column, depth + 1)?;
                let keys = keys
                    .cat32()
                    .map_err(|error| KolaError::DeserializationErr(error.to_string()))?;
                let value_length = match &values {
                    K::Series(series) => series.len(),
                    K::MixedList(values) => values.len(),
                    K::CharVector(values) => values.len(),
                    value => {
                        return Err(KolaError::DeserializationErr(format!(
                            "dictionary values must be a list, got {value:?}"
                        )))
                    }
                };
                if keys.len() != value_length {
                    return Err(KolaError::DeserializationErr(format!(
                        "dictionary key/value length mismatch: {} keys and {value_length} values",
                        keys.len()
                    )));
                }
                let key_values = keys.iter_str().map(|key| key.unwrap_or("").to_owned());
                let values = match values {
                    K::Series(series) => series.iter().map(K::from_any_value).collect::<Vec<_>>(),
                    K::MixedList(values) => values,
                    K::CharVector(values) => values.into_iter().map(K::Char).collect(),
                    _ => {
                        return Err(KolaError::DeserializationErr(
                            "dictionary values changed type during decoding".to_string(),
                        ))
                    }
                };
                Ok(K::Dict(key_values.zip(values).collect()))
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
                .map(|(values, k_type)| deserialize_series(values, k_type, true)?.try_into())
                .collect::<Result<Vec<_>, KolaError>>()?;
            columns.iter_mut().zip(symbols.iter()).for_each(|(c, n)| {
                c.rename(n.unwrap_or("").into());
            });
            DataFrame::new_infer_height(columns.into_iter().map(|c| c.into()).collect())
                .map(K::DataFrame)
                .map_err(|error| KolaError::DeserializationErr(error.to_string()))
        }
        101 => {
            *pos += 1;
            if vec[start_pos] == 0 {
                Ok(K::Null)
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

fn deserialize_series(vec: &[u8], k_type: u8, as_column: bool) -> Result<K, KolaError> {
    let mut pos = 1;
    let length = u32::from_le_bytes(vec[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;
    if length == 0 {
        return if k_type == 10 && !as_column {
            Ok(K::CharVector(Vec::new()))
        } else {
            new_empty_series(k_type)
        };
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
            let array_vec = array_vec.to_vec();
            let new_ptr: *const i16 = array_vec.as_ptr().cast();
            let slice = unsafe { core::slice::from_raw_parts(new_ptr, array_vec.len() / k_size) };
            let bitmap =
                Bitmap::from_iter(slice.iter().map(|s| *s > i16::MIN + 1 && *s < i16::MAX));
            let mut array = Int16Array::from_slice(slice);
            array.set_validity(Some(bitmap));
            series = Series::from_arrow(name.into(), array.boxed()).unwrap();
            Ok(K::Series(series))
        }
        6 => {
            let array_vec = array_vec.to_vec();
            let new_ptr: *const i32 = array_vec.as_ptr().cast();
            let slice = unsafe { core::slice::from_raw_parts(new_ptr, array_vec.len() / k_size) };
            let bitmap =
                Bitmap::from_iter(slice.iter().map(|s| *s > i32::MIN + 1 && *s < i32::MAX));
            let mut array = Int32Array::from_slice(slice);
            array.set_validity(Some(bitmap));
            series = Series::from_arrow(name.into(), array.boxed()).unwrap();
            Ok(K::Series(series))
        }
        7 => {
            let array_vec = array_vec.to_vec();
            let new_ptr: *const i64 = array_vec.as_ptr().cast();
            let slice = unsafe { core::slice::from_raw_parts(new_ptr, array_vec.len() / k_size) };
            let bitmap =
                Bitmap::from_iter(slice.iter().map(|s| *s > i64::MIN + 1 && *s < i64::MAX));
            let mut array = Int64Array::from_slice(slice);
            array.set_validity(Some(bitmap));
            series = Series::from_arrow(name.into(), array.boxed()).unwrap();
            Ok(K::Series(series))
        }
        8 => {
            let array_vec = array_vec.to_vec();
            let new_ptr: *const f32 = array_vec.as_ptr().cast();
            let slice = unsafe { core::slice::from_raw_parts(new_ptr, array_vec.len() / k_size) };
            let bitmap = Bitmap::from_iter(slice.iter().map(|s| !f32::is_nan(*s)));
            let mut array = Float32Array::from_slice(slice);
            array.set_validity(Some(bitmap));
            series = Series::from_arrow(name.into(), array.boxed()).unwrap();
            Ok(K::Series(series))
        }
        9 => {
            let array_vec = array_vec.to_vec();
            let new_ptr: *const f64 = array_vec.as_ptr().cast();
            let slice = unsafe { core::slice::from_raw_parts(new_ptr, array_vec.len() / k_size) };
            let bitmap = Bitmap::from_iter(slice.iter().map(|s| !f64::is_nan(*s)));
            let mut array = Float64Array::from_slice(slice);
            array.set_validity(Some(bitmap));
            series = Series::from_arrow(name.into(), array.boxed()).unwrap();
            Ok(K::Series(series))
        }
        10 => {
            if as_column {
                if array_vec.iter().any(|byte| !byte.is_ascii()) {
                    return Err(KolaError::DeserializationErr(
                        "q char columns require valid UTF-8".to_string(),
                    ));
                }
                let offsets: Vec<i64> = (0..=length as i64).collect();
                array_box = Utf8Array::<i64>::new(
                    ArrowDataType::LargeUtf8,
                    OffsetsBuffer::try_from(offsets).unwrap(),
                    Buffer::from(array_vec.to_vec()),
                    None,
                )
                .boxed();
                series = Series::from_arrow(name.into(), array_box).unwrap();
                Ok(K::Series(series))
            } else {
                Ok(K::CharVector(array_vec.to_vec()))
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
            let array_vec = array_vec.to_vec();
            let new_ptr: *const i64 = array_vec.as_ptr().cast();
            let slice = unsafe { core::slice::from_raw_parts(new_ptr, array_vec.len() / k_size) };
            let slice = slice
                .iter()
                .map(|ns| match *ns {
                    i64::MIN => Ok(*ns),
                    value => unix_timestamp_nanoseconds(value),
                })
                .collect::<Result<Vec<_>, KolaError>>()?;
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
            let array_vec = array_vec.to_vec();
            let new_ptr: *const i32 = array_vec.as_ptr().cast();
            let slice = unsafe { core::slice::from_raw_parts(new_ptr, array_vec.len() / k_size) };
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
            let array_vec = array_vec.to_vec();
            let new_ptr: *const f64 = array_vec.as_ptr().cast();
            let slice = unsafe { core::slice::from_raw_parts(new_ptr, array_vec.len() / k_size) };
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
            Ok(K::Series(series))
        }
        // minutes, seconds, time
        17..=19 => {
            let array_vec = array_vec.to_vec();
            let new_ptr: *const i32 = array_vec.as_ptr().cast();
            let slice = unsafe { core::slice::from_raw_parts(new_ptr, array_vec.len() / k_size) };
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
                let bitmap = Bitmap::from_iter(slice.iter().map(|s| !f32::is_nan(*s)));
                let mut array = Float32Array::from_slice(slice);
                array.set_validity(Some(bitmap));
                array_box = array.boxed();
                field = create_field(k_type, "real").unwrap();
            } else if k_type == 9 {
                let new_ptr: *const f64 = v8.as_ptr().cast();
                let slice = unsafe { core::slice::from_raw_parts(new_ptr, v8.len() / k_size) };
                let bitmap = Bitmap::from_iter(slice.iter().map(|s| !f64::is_nan(*s)));
                let mut array = Float64Array::from_slice(slice);
                array.set_validity(Some(bitmap));
                array_box = array.boxed();
                field = create_field(k_type, "float").unwrap();
            } else if k_type == 12 {
                let new_ptr: *mut i64 = v8.as_mut_ptr().cast();
                let slice = unsafe { core::slice::from_raw_parts(new_ptr, v8.len() / k_size) };
                let slice = slice
                    .iter()
                    .map(|ns| match *ns {
                        i64::MIN => Ok(*ns),
                        value => unix_timestamp_nanoseconds(value),
                    })
                    .collect::<Result<Vec<_>, KolaError>>()?;
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
            for offsets in offsets_buf.as_ref().windows(2) {
                let start = offsets[0] as usize;
                let end = offsets[1] as usize;
                std::str::from_utf8(&v8[start..end]).map_err(|_| {
                    KolaError::DeserializationErr("q char columns require valid UTF-8".to_string())
                })?;
            }
            let array_box = Utf8Array::<i64>::new(
                ArrowDataType::LargeUtf8,
                offsets_buf,
                Buffer::from(v8),
                None,
            )
            .boxed();
            Ok(K::Series(
                Series::from_arrow(name.into(), array_box).unwrap(),
            ))
        }
        _ => unreachable!(),
    }
}

fn create_datetime(ns: i64) -> DateTime<Utc> {
    match DateTime::from_timestamp(
        ns.div_euclid(1_000_000_000),
        ns.rem_euclid(1_000_000_000) as u32,
    ) {
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

pub fn decompress(vec: &[u8], de_vec: &mut [u8], start_pos: usize) -> Result<(), KolaError> {
    let decompressed_total_length = match start_pos {
        4 => {
            let prefix: [u8; 4] = vec
                .get(..4)
                .ok_or_else(|| {
                    KolaError::DeserializationErr(
                        "compressed data is missing its 4-byte length prefix".to_string(),
                    )
                })?
                .try_into()
                .map_err(|_| {
                    KolaError::DeserializationErr(
                        "invalid 4-byte compressed-data length prefix".to_string(),
                    )
                })?;
            u64::from(u32::from_le_bytes(prefix))
        }
        8 => {
            let prefix: [u8; 8] = vec
                .get(..8)
                .ok_or_else(|| {
                    KolaError::DeserializationErr(
                        "compressed data is missing its 8-byte length prefix".to_string(),
                    )
                })?
                .try_into()
                .map_err(|_| {
                    KolaError::DeserializationErr(
                        "invalid 8-byte compressed-data length prefix".to_string(),
                    )
                })?;
            u64::from_le_bytes(prefix)
        }
        _ => {
            return Err(KolaError::DeserializationErr(format!(
                "unsupported compressed-data prefix length {start_pos}"
            )))
        }
    };
    let expected_body_length = decompressed_total_length
        .checked_sub(8)
        .and_then(|length| usize::try_from(length).ok())
        .ok_or_else(|| {
            KolaError::DeserializationErr(
                "invalid decompressed message length in compressed-data prefix".to_string(),
            )
        })?;
    if expected_body_length != de_vec.len() {
        return Err(KolaError::DeserializationErr(format!(
            "decompressed body length mismatch: prefix requires {expected_body_length} bytes, destination has {}",
            de_vec.len()
        )));
    }
    if !de_vec.is_empty() && vec.len() <= start_pos {
        return Err(KolaError::DeserializationErr(
            "compressed data ends immediately after its length prefix".to_string(),
        ));
    }

    match catch_unwind(AssertUnwindSafe(|| {
        decompress_unchecked(vec, de_vec, start_pos)
    })) {
        Ok(()) => Ok(()),
        Err(_) => Err(KolaError::DeserializationErr(
            "malformed compressed data caused an internal decompressor panic".to_string(),
        )),
    }
}

fn decompress_unchecked(vec: &[u8], de_vec: &mut [u8], start_pos: usize) {
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
    serialize_with_depth(k, 0)
}

fn serialize_with_depth(k: &K, depth: usize) -> Result<Vec<u8>, KolaError> {
    let k_length = k.j6_len_with_depth(depth)?;
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
            validate_q_symbol(k)?;
            vec = Vec::with_capacity(k_length);
            vec.write_all(&[245]).unwrap();
            vec.write_all(k.as_bytes()).unwrap();
            vec.write_all(&[0]).unwrap();
        }
        K::CharVector(k) => {
            let length = q_list_length_from_usize(k.len())?;
            vec = Vec::with_capacity(k_length);
            vec.write_all(&[10, 0]).unwrap();
            vec.write_all(&length).unwrap();
            vec.write_all(k).unwrap();
        }
        K::String(k) => {
            let length = q_list_length_from_usize(k.len())?;
            vec = Vec::with_capacity(k_length);
            vec.write_all(&[10, 0]).unwrap();
            vec.write_all(&length).unwrap();
            vec.write_all(k.as_bytes()).unwrap();
        }
        // to timestamp
        K::DateTime(k) => {
            let unix_nanoseconds = k.timestamp_nanos_opt().ok_or_else(|| {
                KolaError::NotAbleToSerializeErr(
                    "timestamp is outside q's representable nanosecond range".to_string(),
                )
            })?;
            let q_nanoseconds = q_timestamp_nanoseconds(unix_nanoseconds)?;
            vec = Vec::with_capacity(k_length);
            vec.write_all(&[244]).unwrap();
            vec.write_all(&q_nanoseconds.to_le_bytes()).unwrap();
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
            vec = Vec::with_capacity(k_length);
            vec.write_all(&[0, 0]).unwrap();
            vec.write_all(&q_list_length_from_usize(l.len())?).unwrap();
            for atom in l.iter() {
                vec.write_all(&serialize_with_depth(atom, depth + 1)?)
                    .unwrap();
            }
        }
        // to list
        K::Series(k) => {
            vec = serialize_series(k, k_length)?;
        }
        // to table
        K::DataFrame(k) => {
            vec = Vec::with_capacity(k_length);
            let column_names = k.get_column_names();
            let column_count =
                i32::try_from(column_names.len()).map_err(|_| KolaError::OverLengthErr())?;
            vec.write_all(&[98, 0, 99, 11, 0]).unwrap();
            vec.write_all(&column_count.to_le_bytes()).unwrap();
            for name in column_names {
                validate_q_symbol(name)?;
                vec.write_all(name.as_bytes()).unwrap();
                vec.write_all(&[0]).unwrap();
            }
            vec.write_all(&[0, 0]).unwrap();
            let columns = k.columns();
            vec.write_all(&column_count.to_le_bytes()).unwrap();
            let vectors = columns
                .into_par_iter()
                .map(|column| {
                    let series = column.as_materialized_series();
                    serialize_series(series, get_series_len(series)?)
                })
                .collect::<Result<Vec<Vec<u8>>, KolaError>>()?;
            for value in vectors {
                vec.write_all(&value).unwrap();
            }
        }
        // to (::)
        K::Null => {
            vec = Vec::with_capacity(k_length);
            vec.write_all(&[101, 0]).unwrap();
        }
        K::Dict(dict) => {
            let length = i32::try_from(dict.len()).map_err(|_| KolaError::OverLengthErr())?;
            if length == 0 {
                return Err(KolaError::Err("Not supported empty dictionary".to_string()));
            };
            vec = Vec::with_capacity(k_length);
            vec.write_all(&[99, 11, 0]).unwrap();
            vec.write_all(&length.to_le_bytes()).unwrap();
            for key in dict.keys() {
                validate_q_symbol(key)?;
                vec.write_all(key.as_bytes()).unwrap();
                vec.write_all(&[0]).unwrap();
            }
            vec.write_all(&[0, 0]).unwrap();
            vec.write_all(&length.to_le_bytes()).unwrap();
            for value in dict.values() {
                vec.write_all(&serialize_with_depth(value, depth + 1)?)
                    .unwrap();
            }
        }
    };
    Ok(vec)
}

fn serialize_series(series: &Series, k_length: usize) -> Result<Vec<u8>, KolaError> {
    let rechunked;
    let series = if series.n_chunks() > 1 {
        rechunked = series.rechunk();
        &rechunked
    } else {
        series
    };
    let mut vec: Vec<u8> = Vec::with_capacity(k_length);
    let k_length = series.len();
    if k_length > i32::MAX as usize {
        return Err(KolaError::OverLengthErr());
    }
    match series.dtype() {
        PolarsDataType::Boolean => {
            vec.write_all(&[1, 0]).unwrap();
            vec.write_all(&(k_length as i32).to_le_bytes()).unwrap();
            let physical = series.to_physical_repr();
            let values = physical
                .bool()
                .map_err(|error| KolaError::NotAbleToSerializeErr(error.to_string()))?;
            for value in values.iter() {
                vec.write_all(&[value.unwrap_or(false) as u8]).unwrap();
            }
        }
        PolarsDataType::UInt8 => {
            vec.write_all(&[4, 0]).unwrap();
            vec.write_all(&(k_length as i32).to_le_bytes()).unwrap();
            let physical = series.to_physical_repr();
            let values = physical
                .u8()
                .map_err(|error| KolaError::NotAbleToSerializeErr(error.to_string()))?;
            for value in values.iter() {
                vec.write_all(&[value.unwrap_or(0)]).unwrap();
            }
        }
        PolarsDataType::Int16 => {
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
                let v8 = native_values_as_bytes(array.as_ref());
                vec.write_all(v8).unwrap();
            })
        }
        PolarsDataType::Int32 => {
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
                let v8 = native_values_as_bytes(array.as_ref());
                vec.write_all(v8).unwrap();
            })
        }
        PolarsDataType::Int64 => {
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
                let v8 = native_values_as_bytes(array.as_ref());
                vec.write_all(v8).unwrap();
            })
        }
        PolarsDataType::Float32 => {
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
                let v8 = native_values_as_bytes(array.as_ref());
                vec.write_all(v8).unwrap();
            })
        }
        PolarsDataType::Float64 => {
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
                let v8 = native_values_as_bytes(array.as_ref());
                vec.write_all(v8).unwrap();
            })
        }
        PolarsDataType::String => {
            vec.write_all(&[0, 0]).unwrap();
            vec.write_all(&(k_length as i32).to_le_bytes()).unwrap();
            let physical = series.to_physical_repr();
            let values = physical
                .str()
                .map_err(|error| KolaError::NotAbleToSerializeErr(error.to_string()))?;
            for value in values.iter() {
                vec.write_all(&[10, 0]).unwrap();
                if let Some(value) = value {
                    vec.write_all(&q_list_length_from_usize(value.len())?)
                        .unwrap();
                    vec.write_all(value.as_bytes()).unwrap();
                } else {
                    vec.write_all(&[0, 0, 0, 0]).unwrap();
                }
            }
        }
        PolarsDataType::Date => {
            // max date - 95026601
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
                let v8 = native_values_as_bytes(array.as_ref());
                vec.write_all(v8).unwrap();
            })
        }
        PolarsDataType::Datetime(unit, _) => {
            let physical = series.cast(&PolarsDataType::Int64).unwrap();
            let chunks = physical.i64().unwrap();
            let timestamp_multiplier = match unit {
                PolarTimeUnit::Nanoseconds => Some(1i64),
                PolarTimeUnit::Microseconds => Some(1_000i64),
                PolarTimeUnit::Milliseconds => None,
            };
            if let Some(multiplier) = timestamp_multiplier {
                for value in chunks.iter().flatten() {
                    let unix_nanoseconds = value.checked_mul(multiplier).ok_or_else(|| {
                        KolaError::NotAbleToSerializeErr(
                            "timestamp is outside q's representable nanosecond range".to_string(),
                        )
                    })?;
                    q_timestamp_nanoseconds(unix_nanoseconds)?;
                }
            }
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
                        let v8 = native_values_as_bytes(array.as_ref());
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
                    for array in chunks.chunks() {
                        let buffer = unsafe {
                            array
                                .as_any()
                                .downcast_ref::<PrimitiveArray<i64>>()
                                .unwrap_unchecked()
                                .values()
                        };
                        let array = buffer
                            .as_slice()
                            .iter()
                            .map(|value| {
                                if *value == i64::MIN {
                                    Ok(i64::MIN)
                                } else {
                                    let unix_nanoseconds =
                                        value.checked_mul(multiplier).ok_or_else(|| {
                                            KolaError::NotAbleToSerializeErr(
                                                "timestamp is outside q's representable nanosecond range"
                                                    .to_string(),
                                            )
                                        })?;
                                    q_timestamp_nanoseconds(unix_nanoseconds)
                                }
                            })
                            .collect::<Result<Vec<_>, KolaError>>()?;
                        let v8 = native_values_as_bytes(array.as_ref());
                        vec.write_all(v8).unwrap();
                    }
                }
            }
        }
        PolarsDataType::Duration(_) => {
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
                let v8 = native_values_as_bytes(array.as_ref());
                vec.write_all(v8).unwrap();
            })
        }
        PolarsDataType::Time => {
            validate_q_time_series(series)?;
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

                let v8 = native_values_as_bytes(array.as_ref());
                vec.write_all(v8).unwrap();
            })
        }
        PolarsDataType::Array(data_type, size) => {
            vec.write_all(&[0, 0]).unwrap();
            vec.write_all(&(k_length as i32).to_le_bytes()).unwrap();
            let array = downcast_array::<FixedSizeListArray>(
                series.array().unwrap().chunks()[0].as_ref(),
                "FixedSizeList",
            )?;
            if array.null_count() > 0 {
                return Err(KolaError::NotAbleToSerializeErr(
                    "null values in Array columns".to_string(),
                ));
            }
            match data_type.as_ref() {
                PolarsDataType::Boolean => {
                    let array = downcast_array::<BooleanArray>(array.values().as_ref(), "Boolean")?;
                    let len_vec = q_list_length(
                        i64::try_from(*size).map_err(|_| KolaError::OverLengthErr())?,
                    )?;
                    for i in 0..array.len() {
                        if i % size == 0 {
                            vec.write_all(&[1, 0]).unwrap();
                            vec.write_all(&len_vec).unwrap();
                        }
                        vec.write_all(&[array.get(i).unwrap_or(false) as u8])
                            .unwrap();
                    }
                }
                PolarsDataType::UInt8
                | PolarsDataType::Int16
                | PolarsDataType::Int32
                | PolarsDataType::Int64
                | PolarsDataType::Float32
                | PolarsDataType::Float64 => {
                    return Err(KolaError::NotSupportedPolarsNestedListTypeErr(
                        data_type.as_ref().clone(),
                    ))
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
            let list = downcast_array::<ListArray<i64>>(
                series.list().unwrap().chunks()[0].as_ref(),
                "List",
            )?;
            let offsets = list.offsets().as_ref();
            if list.null_count() > 0 {
                return Err(KolaError::NotAbleToSerializeErr(
                    "null values in List columns".to_string(),
                ));
            }
            match data_type.as_ref() {
                PolarsDataType::Boolean => {
                    let list = downcast_array::<BooleanArray>(list.values().as_ref(), "Boolean")?;
                    for i in 0..k_length {
                        let start_offset = offsets[i] as usize;
                        let end_offset = offsets[i + 1] as usize;
                        vec.write_all(&[1, 0]).unwrap();
                        vec.write_all(&q_list_length(offsets[i + 1] - offsets[i])?)
                            .unwrap();
                        for j in start_offset..end_offset {
                            vec.write_all(&[list.get(j).unwrap_or(false) as u8])
                                .unwrap();
                        }
                    }
                }
                PolarsDataType::UInt8 => {
                    let list = downcast_array::<UInt8Array>(list.values().as_ref(), "UInt8")?;
                    let values = list.values().as_ref();
                    for i in 0..k_length {
                        let start_offset = offsets[i] as usize;
                        let end_offset = offsets[i + 1] as usize;
                        vec.write_all(&[4, 0]).unwrap();
                        vec.write_all(&q_list_length(offsets[i + 1] - offsets[i])?)
                            .unwrap();
                        for j in start_offset..end_offset {
                            let value = if list.is_null(j) { 0 } else { values[j] };
                            vec.write_all(&[value]).unwrap();
                        }
                    }
                }
                PolarsDataType::Int16 => {
                    let k_type = 5u8;
                    let k_size = size_of::<i16>();
                    let array = downcast_array::<Int16Array>(list.values().as_ref(), "Int16")?;
                    let p_array: PrimitiveArray<i16>;
                    let array = if array.null_count() > 0 {
                        p_array = set_at_nulls(array, i16::MIN);
                        p_array.values()
                    } else {
                        array.values()
                    };
                    let v8 = native_values_as_bytes(array.as_ref());
                    for i in 0..k_length {
                        let start_offset = k_size * offsets[i] as usize;
                        let end_offset = k_size * offsets[i + 1] as usize;
                        vec.write_all(&[k_type, 0]).unwrap();
                        vec.write_all(&q_list_length(offsets[i + 1] - offsets[i])?)
                            .unwrap();
                        vec.write_all(&v8[start_offset..end_offset]).unwrap();
                    }
                }
                PolarsDataType::Int32 => {
                    let k_type = 6u8;
                    let k_size = size_of::<i32>();
                    let array = downcast_array::<Int32Array>(list.values().as_ref(), "Int32")?;
                    let p_array: PrimitiveArray<i32>;
                    let array = if array.null_count() > 0 {
                        p_array = set_at_nulls(array, i32::MIN);
                        p_array.values()
                    } else {
                        array.values()
                    };
                    let v8 = native_values_as_bytes(array.as_ref());
                    for i in 0..k_length {
                        let start_offset = k_size * offsets[i] as usize;
                        let end_offset = k_size * offsets[i + 1] as usize;
                        vec.write_all(&[k_type, 0]).unwrap();
                        vec.write_all(&q_list_length(offsets[i + 1] - offsets[i])?)
                            .unwrap();
                        vec.write_all(&v8[start_offset..end_offset]).unwrap();
                    }
                }
                PolarsDataType::Int64 => {
                    let k_type = 7u8;
                    let k_size = size_of::<i64>();
                    let array = downcast_array::<Int64Array>(list.values().as_ref(), "Int64")?;
                    let p_array: PrimitiveArray<i64>;
                    let array = if array.null_count() > 0 {
                        p_array = set_at_nulls(array, i64::MIN);
                        p_array.values()
                    } else {
                        array.values()
                    };
                    let v8 = native_values_as_bytes(array.as_ref());
                    for i in 0..k_length {
                        let start_offset = k_size * offsets[i] as usize;
                        let end_offset = k_size * offsets[i + 1] as usize;
                        vec.write_all(&[k_type, 0]).unwrap();
                        vec.write_all(&q_list_length(offsets[i + 1] - offsets[i])?)
                            .unwrap();
                        vec.write_all(&v8[start_offset..end_offset]).unwrap();
                    }
                }
                PolarsDataType::Float32 => {
                    let k_type = 8u8;
                    let k_size = size_of::<f32>();
                    let array = downcast_array::<Float32Array>(list.values().as_ref(), "Float32")?;
                    let p_array: PrimitiveArray<f32>;
                    let array = if array.null_count() > 0 {
                        p_array = set_at_nulls(array, f32::NAN);
                        p_array.values()
                    } else {
                        array.values()
                    };
                    let v8 = native_values_as_bytes(array.as_ref());
                    for i in 0..k_length {
                        let start_offset = k_size * offsets[i] as usize;
                        let end_offset = k_size * offsets[i + 1] as usize;
                        vec.write_all(&[k_type, 0]).unwrap();
                        vec.write_all(&q_list_length(offsets[i + 1] - offsets[i])?)
                            .unwrap();
                        vec.write_all(&v8[start_offset..end_offset]).unwrap();
                    }
                }
                PolarsDataType::Float64 => {
                    let k_type = 9u8;
                    let k_size = size_of::<f64>();
                    let array = downcast_array::<Float64Array>(list.values().as_ref(), "Float64")?;
                    let p_array: PrimitiveArray<f64>;
                    let array = if array.null_count() > 0 {
                        p_array = set_at_nulls(array, f64::NAN);
                        p_array.values()
                    } else {
                        array.values()
                    };
                    let v8 = native_values_as_bytes(array.as_ref());
                    for i in 0..k_length {
                        let start_offset = k_size * offsets[i] as usize;
                        let end_offset = k_size * offsets[i + 1] as usize;
                        vec.write_all(&[k_type, 0]).unwrap();
                        vec.write_all(&q_list_length(offsets[i + 1] - offsets[i])?)
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
            let categorical = series
                .cat32()
                .map_err(|error| KolaError::NotAbleToSerializeErr(error.to_string()))?;
            for value in categorical.iter_str().flatten() {
                validate_q_symbol(value)?;
            }
            vec.write_all(&[11, 0]).unwrap();
            vec.write_all(&(k_length as i32).to_le_bytes()).unwrap();
            for value in categorical.iter_str() {
                vec.write_all(value.unwrap_or("").as_bytes()).unwrap();
                vec.write_all(&[0]).unwrap();
            }
        }
        PolarsDataType::Binary => {
            validate_guid_series(series)?;
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
    use crate::types::MIN_Q_TIMESTAMP_UNIX_NANOS;
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
        decompress(&vec, &mut de_vec, 4).expect("valid compressed message");
        let mut expected_vec = [1u8; 2006].to_vec();
        expected_vec[1] = 0;
        expected_vec[2] = 208;
        expected_vec[3] = 7;
        expected_vec[4] = 0;
        expected_vec[5] = 0;
        assert_eq!(de_vec, expected_vec);
    }

    #[test]
    fn malformed_atom_panics_are_contained_as_deserialization_errors() {
        assert!(matches!(
            deserialize(&[254, 0], &mut 0, false),
            Err(KolaError::DeserializationErr(_))
        ));
    }

    #[test]
    fn mixed_list_count_cannot_amplify_a_tiny_frame() {
        assert!(matches!(
            deserialize(&[0, 0, 0xff, 0xff, 0xff, 0xff, 101, 0], &mut 0, false),
            Err(KolaError::DeserializationErr(_))
        ));
    }

    #[test]
    fn decompress_rejects_a_prefix_only_compressed_body() {
        let compressed = 12u32.to_le_bytes();
        let mut destination = [0u8; 4];
        assert!(matches!(
            decompress(&compressed, &mut destination, 4),
            Err(KolaError::DeserializationErr(_))
        ));
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
    fn deserialize_and_serialize_char_vector_losslessly() {
        let vec = [10, 0, 4, 0, 0, 0, 0, 127, 128, 255].to_vec();
        let k = deserialize(&vec, &mut 0, false).unwrap();
        assert_eq!(k, K::CharVector(vec![0, 127, 128, 255]));
        assert_eq!(serialize(&k).unwrap(), vec);

        let empty = [10, 0, 0, 0, 0, 0].to_vec();
        assert_eq!(
            deserialize(&empty, &mut 0, false).unwrap(),
            K::CharVector(Vec::new())
        );
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
    fn char_vector_length_must_fit_q_list_length() {
        assert!(matches!(
            q_list_length_from_usize(i32::MAX as usize + 1),
            Err(KolaError::OverLengthErr())
        ));
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

    fn assert_ipc_header_matches_body(value: K) {
        let body_length = value.j6_len().expect("calculate body length");
        let message = crate::io::generate_j6_ipc_msg(crate::types::MsgType::Sync, false, value)
            .expect("generate IPC message");
        let header_length =
            u32::from_le_bytes(message[4..8].try_into().expect("IPC length header")) as usize;
        assert_eq!(header_length, message.len());
        assert_eq!(body_length + 8, message.len());
    }

    fn time_series(values: Vec<i64>, validity: Option<Bitmap>) -> Series {
        Series::from_arrow(
            "time".into(),
            PrimitiveArray::new(
                ArrowDataType::Time64(TimeUnit::Nanosecond),
                values.into(),
                validity,
            )
            .boxed(),
        )
        .expect("time series")
    }

    #[test]
    fn series_and_table_lengths_match_generated_ipc_headers() {
        let byte = Series::new("byte".into(), &[Some(7u8), None, Some(9)]);
        let date = Series::new("date".into(), &[Some(0i32), None, Some(1)])
            .cast(&PolarsDataType::Date)
            .expect("date series");
        let time = time_series(
            vec![0, i64::MIN, 86_399_999_000_000],
            Some(Bitmap::from([true, false, true])),
        );
        let long = "long string payload".repeat(8);
        let text = Series::new("text".into(), &[Some(long.as_str()), Some(""), None]);

        for series in [byte.clone(), date.clone(), time.clone(), text.clone()] {
            assert_ipc_header_matches_body(K::Series(series));
        }

        let table = DataFrame::new_infer_height(
            [byte, date, time, text]
                .into_iter()
                .map(Into::into)
                .collect(),
        )
        .expect("table");
        assert_ipc_header_matches_body(K::DataFrame(table));
    }

    #[test]
    fn nullable_boolean_and_byte_series_use_deterministic_q_values() {
        let boolean = Series::from_arrow(
            "boolean".into(),
            BooleanArray::from([Some(true), None, Some(false)]).boxed(),
        )
        .expect("boolean series");
        let byte = Series::from_arrow(
            "byte".into(),
            UInt8Array::from([Some(7), None, Some(9)]).boxed(),
        )
        .expect("byte series");

        let boolean_bytes = serialize(&K::Series(boolean.clone())).expect("serialize boolean");
        let byte_bytes = serialize(&K::Series(byte.clone())).expect("serialize byte");
        assert_eq!(boolean_bytes, [1, 0, 3, 0, 0, 0, 1, 0, 0]);
        assert_eq!(byte_bytes, [4, 0, 3, 0, 0, 0, 7, 0, 9]);
        assert_eq!(
            serialize(&deserialize(&boolean_bytes, &mut 0, false).expect("deserialize boolean"))
                .expect("reserialize boolean"),
            boolean_bytes
        );
        assert_eq!(
            serialize(&deserialize(&byte_bytes, &mut 0, false).expect("deserialize byte"))
                .expect("reserialize byte"),
            byte_bytes
        );

        let table = DataFrame::new_infer_height(vec![boolean.clone().into(), byte.clone().into()])
            .expect("nullable table");
        let table_bytes = serialize(&K::DataFrame(table.clone())).expect("serialize table");
        assert_eq!(
            serialize(&deserialize(&table_bytes, &mut 0, false).expect("deserialize table"))
                .expect("reserialize table"),
            table_bytes
        );
        assert_ipc_header_matches_body(K::Series(boolean));
        assert_ipc_header_matches_body(K::Series(byte));
        assert_ipc_header_matches_body(K::DataFrame(table));
    }

    #[test]
    fn rejects_sub_millisecond_polars_and_scalar_times() {
        let time = time_series(vec![1], None);
        assert!(matches!(
            get_series_len(&time),
            Err(KolaError::NotAbleToSerializeErr(_))
        ));
        assert!(matches!(
            serialize_series(&time, 10),
            Err(KolaError::NotAbleToSerializeErr(_))
        ));

        let scalar = K::Time(NaiveTime::from_hms_nano_opt(0, 0, 0, 1).expect("time"));
        assert!(matches!(
            serialize(&scalar),
            Err(KolaError::NotAbleToSerializeErr(_))
        ));
    }

    #[test]
    fn rejects_mixed_width_guid_series_and_zero_fills_nulls() {
        let exact = [7u8; 16];
        let valid = Series::new("guid".into(), &[Some(exact.as_slice()), None]);
        let serialized = serialize(&K::Series(valid)).expect("serialize GUIDs");
        assert_eq!(&serialized[6..22], exact.as_slice());
        assert_eq!(&serialized[22..38], &[0u8; 16]);

        let short = [1u8; 15];
        let malformed = Series::new(
            "guid".into(),
            &[Some(exact.as_slice()), Some(short.as_slice())],
        );
        assert!(matches!(
            get_series_len(&malformed),
            Err(KolaError::NotAbleToSerializeErr(_))
        ));
        assert!(matches!(
            serialize_series(&malformed, 38),
            Err(KolaError::NotAbleToSerializeErr(_))
        ));
    }

    #[test]
    fn rejects_non_utf8_q_char_columns_without_losing_top_level_bytes() {
        let top_level = [10, 0, 1, 0, 0, 0, 0xff];
        assert_eq!(
            deserialize(&top_level, &mut 0, false).expect("top-level bytes"),
            K::CharVector(vec![0xff])
        );

        let nested = [0, 0, 1, 0, 0, 0, 10, 0, 1, 0, 0, 0, 0xff];
        assert!(matches!(
            deserialize(&nested, &mut 0, false),
            Err(KolaError::DeserializationErr(_))
        ));

        let table = [
            98, 0, 99, 11, 0, 1, 0, 0, 0, b'c', 0, 0, 0, 1, 0, 0, 0, 10, 0, 1, 0, 0, 0, 0xff,
        ];
        assert!(matches!(
            deserialize(&table, &mut 0, false),
            Err(KolaError::DeserializationErr(_))
        ));
    }

    #[test]
    fn table_column_failures_are_propagated() {
        let table = [
            98, 0, 99, 11, 0, 1, 0, 0, 0, b'x', 0, 0, 0, 1, 0, 0, 0, 13, 0, 1, 0, 0, 0, 0, 0, 0, 0,
        ];
        assert!(matches!(
            deserialize(&table, &mut 0, false),
            Err(KolaError::NotSupportedKListErr(13))
        ));
    }

    #[test]
    fn bounds_mixed_list_and_dictionary_deserialization_depth() {
        let mut mixed = vec![101, 0];
        for _ in 0..=MAX_VALUE_DEPTH {
            let mut outer = vec![0, 0, 1, 0, 0, 0];
            outer.extend(mixed);
            mixed = outer;
        }
        assert!(matches!(
            deserialize(&mixed, &mut 0, false),
            Err(KolaError::DeserializationErr(_))
        ));

        let mut dictionary = vec![101, 0];
        for _ in 0..=MAX_VALUE_DEPTH / 2 {
            let mut outer = vec![99, 11, 0, 1, 0, 0, 0, b'k', 0, 0, 0, 1, 0, 0, 0];
            outer.extend(dictionary);
            dictionary = outer;
        }
        assert!(matches!(
            deserialize(&dictionary, &mut 0, false),
            Err(KolaError::DeserializationErr(_))
        ));
    }

    #[test]
    fn sizing_and_serialization_reject_sixty_five_nested_values() {
        fn nested_mixed(depth: usize) -> K {
            (0..depth).fold(K::Null, |value, _| K::MixedList(vec![value]))
        }

        fn nested_dict(depth: usize) -> K {
            (0..depth).fold(K::Null, |value, _| {
                K::Dict(IndexMap::from([("k".to_string(), value)]))
            })
        }

        assert!(serialize(&nested_mixed(MAX_VALUE_DEPTH)).is_ok());
        assert!(serialize(&nested_dict(MAX_VALUE_DEPTH)).is_ok());

        for value in [
            nested_mixed(MAX_VALUE_DEPTH + 1),
            nested_dict(MAX_VALUE_DEPTH + 1),
        ] {
            assert!(matches!(
                value.j6_len(),
                Err(KolaError::NotAbleToSerializeErr(_))
            ));
            assert!(matches!(
                serialize(&value),
                Err(KolaError::NotAbleToSerializeErr(_))
            ));
        }
    }

    #[test]
    fn decodes_char_and_guid_dictionary_values_without_truncation() {
        let chars = [
            99, 11, 0, 2, 0, 0, 0, b'a', 0, b'b', 0, 10, 0, 2, 0, 0, 0, b'x', b'y',
        ];
        let mut expected_chars = IndexMap::new();
        expected_chars.insert("a".to_string(), K::Char(b'x'));
        expected_chars.insert("b".to_string(), K::Char(b'y'));
        assert_eq!(
            deserialize(&chars, &mut 0, false).expect("char dictionary"),
            K::Dict(expected_chars)
        );

        let first = [1u8; 16];
        let second = [2u8; 16];
        let mut guids = vec![99, 11, 0, 2, 0, 0, 0, b'a', 0, b'b', 0, 2, 0, 2, 0, 0, 0];
        guids.extend(first);
        guids.extend(second);
        let mut expected_guids = IndexMap::new();
        expected_guids.insert("a".to_string(), K::Guid(Uuid::from_bytes(first)));
        expected_guids.insert("b".to_string(), K::Guid(Uuid::from_bytes(second)));
        assert_eq!(
            deserialize(&guids, &mut 0, false).expect("GUID dictionary"),
            K::Dict(expected_guids)
        );

        let mismatched = [
            99, 11, 0, 2, 0, 0, 0, b'a', 0, b'b', 0, 10, 0, 1, 0, 0, 0, b'x',
        ];
        assert!(matches!(
            deserialize(&mismatched, &mut 0, false),
            Err(KolaError::DeserializationErr(_))
        ));
    }

    #[test]
    fn rejects_nul_symbols_and_dictionary_keys_during_serialization() {
        assert!(matches!(
            serialize(&K::Symbol("bad\0symbol".to_string())),
            Err(KolaError::NotAbleToSerializeErr(_))
        ));
        let dictionary = K::Dict(IndexMap::from([("bad\0key".to_string(), K::I64(1))]));
        assert!(matches!(
            serialize(&dictionary),
            Err(KolaError::NotAbleToSerializeErr(_))
        ));

        let nested_empty_dictionary = K::Dict(IndexMap::from([(
            "nested".to_string(),
            K::Dict(IndexMap::new()),
        )]));
        assert!(serialize(&nested_empty_dictionary).is_err());

        let categorical = Series::new("symbol".into(), &["ok", "bad\0symbol"])
            .cast(&PolarsDataType::Categorical(
                Categories::global(),
                Categories::global().mapping(),
            ))
            .expect("categorical series");
        assert!(matches!(
            serialize(&K::Series(categorical)),
            Err(KolaError::NotAbleToSerializeErr(_))
        ));
    }

    #[test]
    fn timestamp_encoding_is_exact_at_q_boundaries() {
        let values = [MIN_Q_TIMESTAMP_UNIX_NANOS, -1, i64::MAX];
        for unix_nanoseconds in values {
            let timestamp = DateTime::from_timestamp(
                unix_nanoseconds.div_euclid(1_000_000_000),
                unix_nanoseconds.rem_euclid(1_000_000_000) as u32,
            )
            .expect("timestamp");
            let serialized = serialize(&K::DateTime(timestamp)).expect("serialize timestamp");
            let raw = i64::from_le_bytes(serialized[1..9].try_into().expect("q timestamp payload"));
            assert_eq!(raw, unix_nanoseconds - NANOS_DIFF);
            assert_eq!(
                deserialize(&serialized, &mut 0, false).expect("deserialize timestamp"),
                K::DateTime(timestamp)
            );
        }

        let below = MIN_Q_TIMESTAMP_UNIX_NANOS - 1;
        let below = DateTime::from_timestamp(
            below.div_euclid(1_000_000_000),
            below.rem_euclid(1_000_000_000) as u32,
        )
        .expect("timestamp below q range");
        assert!(matches!(
            serialize(&K::DateTime(below)),
            Err(KolaError::NotAbleToSerializeErr(_))
        ));

        let below_series = Series::from_arrow(
            "timestamp".into(),
            PrimitiveArray::new(
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
                vec![MIN_Q_TIMESTAMP_UNIX_NANOS - 1].into(),
                None,
            )
            .boxed(),
        )
        .expect("timestamp series");
        assert!(matches!(
            serialize(&K::Series(below_series)),
            Err(KolaError::NotAbleToSerializeErr(_))
        ));

        let above = DateTime::from_timestamp(i64::MAX.div_euclid(1_000_000_000) + 1, 0)
            .expect("timestamp above q range");
        assert!(matches!(
            serialize(&K::DateTime(above)),
            Err(KolaError::NotAbleToSerializeErr(_))
        ));

        let first_unrepresentable_q_timestamp = i64::MAX - NANOS_DIFF + 1;
        let mut atom = vec![244];
        atom.extend_from_slice(&first_unrepresentable_q_timestamp.to_le_bytes());
        assert!(matches!(
            deserialize(&atom, &mut 0, false),
            Err(KolaError::DeserializationErr(_))
        ));

        let mut list = vec![12, 0, 1, 0, 0, 0];
        list.extend_from_slice(&first_unrepresentable_q_timestamp.to_le_bytes());
        assert!(matches!(
            deserialize(&list, &mut 0, false),
            Err(KolaError::DeserializationErr(_))
        ));
    }
}
