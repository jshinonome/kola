use std::{
    io::{Cursor, Write},
    usize,
};

use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveTime, Timelike};
use indexmap::IndexMap;
// use ndarray::ArcArray2;
use polars::io::{
    ipc::{IpcCompression, IpcStreamReader, IpcStreamWriter},
    SerReader, SerWriter,
};
use polars_arrow::types::NativeType;

use crate::errors::KolaError;

use crate::types::J;

const DAY_DIFF: i32 = 719163;

const PADDING: [&[u8]; 8] = [
    &[],
    &[0, 0, 0, 0, 0, 0, 0],
    &[0, 0, 0, 0, 0, 0],
    &[0, 0, 0, 0, 0],
    &[0, 0, 0, 0],
    &[0, 0, 0],
    &[0, 0],
    &[0],
];

const IPC_COMPRESS_THRESHOLD: usize = 1048576;

pub fn deserialize(vec: &[u8], pos: &mut usize) -> Result<J, KolaError> {
    let code = vec[*pos];
    *pos += 4;
    let j;
    match code {
        255 => {
            j = J::Boolean(vec[*pos] == 1);
            *pos += 4
        }
        254 => {
            j = J::U8(vec[*pos]);
            *pos += 4
        }
        253 => {
            j = J::I16(i16::from_le_bytes(vec[*pos..*pos + 2].try_into().unwrap()));
            *pos += 4
        }
        252 | 250 => {
            let i = i32::from_le_bytes(vec[*pos..*pos + 4].try_into().unwrap());
            if code == 252 {
                j = J::I32(i);
            } else {
                j = J::Date(
                    NaiveDate::from_num_days_from_ce_opt(i + DAY_DIFF).unwrap_or(NaiveDate::MAX),
                );
            }
            *pos += 4
        }
        251 | 249 | 248 | 247 | 246 => {
            *pos += 4;
            let i = i64::from_le_bytes(vec[*pos..*pos + 8].try_into().unwrap());
            j = match code {
                251 => J::I64(i),
                249 => {
                    let seconds = i / 1000000000;
                    let ns = i % 1000000000;
                    if i <= 0 {
                        J::Time(NaiveTime::MIN)
                    } else {
                        J::Time(
                            NaiveTime::from_num_seconds_from_midnight_opt(
                                seconds as u32,
                                ns as u32,
                            )
                            .unwrap_or(
                                NaiveTime::from_num_seconds_from_midnight_opt(
                                    23 * 3600 + 59 * 60 + 59,
                                    999_999_999,
                                )
                                .unwrap(),
                            ),
                        )
                    }
                }
                248 => {
                    // microseconds
                    let seconds = i / 1000;
                    let ns = 1000000 * (i % 1000);
                    match DateTime::from_timestamp(seconds, ns as u32) {
                        Some(dt) => J::DateTime(dt),
                        None => {
                            if i > 0 {
                                J::DateTime(
                                    DateTime::from_timestamp(9223372036, 854775804).unwrap(),
                                )
                            } else {
                                J::DateTime(DateTime::from_timestamp(0, 0).unwrap())
                            }
                        }
                    }
                }
                247 => {
                    // nanoseconds
                    let seconds = i / 1000000000;
                    let ns = i % 1000000000;
                    match DateTime::from_timestamp(seconds, ns as u32) {
                        Some(dt) => J::DateTime(dt),
                        None => {
                            if i > 0 {
                                J::DateTime(
                                    DateTime::from_timestamp(9223372036, 854775804).unwrap(),
                                )
                            } else {
                                J::DateTime(DateTime::from_timestamp(0, 0).unwrap())
                            }
                        }
                    }
                }
                246 => J::Duration(Duration::nanoseconds(i)),
                _ => unreachable!(),
            };
            *pos += 8
        }
        245 => {
            let f = f32::from_le_bytes(vec[*pos..*pos + 4].try_into().unwrap());
            j = J::F32(f);
            *pos += 4
        }
        244 => {
            *pos += 4;
            let f = f64::from_le_bytes(vec[*pos..*pos + 8].try_into().unwrap());
            j = J::F64(f);
            *pos += 8
        }
        0 => {
            j = J::Null;
            *pos += 4
        }
        243 | 242 | 128 => {
            let byte_len = u32::from_le_bytes(vec[*pos..*pos + 4].try_into().unwrap()) as usize;
            *pos += 4;
            let s = String::from_utf8(vec[*pos..*pos + byte_len].to_vec()).unwrap();
            j = if code == 243 {
                J::String(s)
            } else if code == 242 {
                J::Symbol(s)
            } else {
                return Err(KolaError::Err(s));
            };
            *pos += byte_len + PADDING[byte_len % 8].len();
        }
        90 => {
            let list_len = u32::from_le_bytes(vec[*pos..*pos + 4].try_into().unwrap()) as usize;
            let mut list = Vec::with_capacity(list_len);
            *pos += 4;

            if list_len > 0 {
                let byte_len = u64::from_le_bytes(vec[*pos..*pos + 8].try_into().unwrap()) as usize;
                *pos += 8;
                let mut v_pos = 16;
                for _ in 0..list_len {
                    list.push(deserialize(vec, &mut v_pos)?)
                }
                *pos += byte_len + PADDING[byte_len % 8].len();
            }
            j = J::MixedList(list);
        }
        92 => {
            *pos += 4;
            let byte_len = u64::from_le_bytes(vec[*pos..*pos + 8].try_into().unwrap()) as usize;
            *pos += 8;
            let df = IpcStreamReader::new(Cursor::new(&vec[*pos..*pos + byte_len]))
                .finish()
                .map_err(|e| KolaError::Err(e.to_string()))?;
            j = J::DataFrame(df);
            *pos += byte_len + PADDING[byte_len % 8].len();
        }
        1..=15 => {
            *pos += 4;
            let byte_len = u64::from_le_bytes(vec[*pos..*pos + 8].try_into().unwrap()) as usize;
            *pos += 8;
            let df = IpcStreamReader::new(Cursor::new(&vec[*pos..*pos + byte_len]))
                .finish()
                .map_err(|e| KolaError::Err(e.to_string()))?;
            j = J::Series(
                df.select_at_idx(0)
                    .unwrap()
                    .as_materialized_series()
                    .clone(),
            );
            *pos += byte_len + PADDING[byte_len % 8].len();
        }
        // 21 => {
        //     let row = u32::from_le_bytes(v[..4].try_into().unwrap()) as usize;
        //     let col = u32::from_le_bytes(v[4..8].try_into().unwrap()) as usize;
        //     let v8 = v[8..].to_vec();
        //     let ptr = v8.as_ptr() as *const f64;
        //     let f64s = unsafe { core::slice::from_raw_parts(ptr, row * col) };
        //     K::Matrix(ArcArray2::from_shape_vec((row, col), f64s.into()).unwrap())
        // }
        91 => {
            let dict_len = u32::from_le_bytes(vec[*pos..*pos + 4].try_into().unwrap()) as usize;
            *pos += 4;

            j = if dict_len == 0 {
                J::Dict(IndexMap::new())
            } else {
                let mut dict: IndexMap<String, J> = IndexMap::with_capacity(dict_len);

                let byte_len = u64::from_le_bytes(vec[*pos..*pos + 8].try_into().unwrap()) as usize;
                *pos += 8;

                let keys = &vec[*pos..*pos + byte_len];
                let k_len = u64::from_le_bytes(keys[0..8].try_into().unwrap()) as usize;

                let array_vec = keys[8..4 * dict_len + 8].to_vec();
                let (_, offsets, _) = unsafe { array_vec.align_to::<u32>() };
                let keys_start = 4 * dict_len + 8;
                let keys_end = 8 + k_len as usize;

                let keys = &keys[keys_start..keys_end];
                *pos += keys_end + PADDING[keys_end % 8].len();
                let v_len = u64::from_le_bytes(vec[*pos..*pos + 8].try_into().unwrap()) as usize;
                *pos += 8;
                let values = &vec[*pos..*pos + v_len];

                *pos += v_len + PADDING[v_len % 8].len();
                let mut v_pos = 0;
                let mut prev_offset = 0;
                for i in 0..dict_len {
                    let offset = offsets[i] as usize;
                    dict.insert(
                        String::from_utf8(keys[prev_offset..offset].to_vec()).unwrap(),
                        deserialize(values, &mut v_pos)?,
                    );
                    prev_offset = offset;
                }
                J::Dict(dict)
            };
        }
        _ => return Err(KolaError::NotAbleDeserializeJTypeErr(code)),
    }
    Ok(j)
}

// pub fn serialize_err(err: &str) -> Vec<u8> {
//     let len = err.len() + 24;
//     let mut vec = Vec::with_capacity(len);
//     vec.write(&[1, 2, 0, 0, 0, 0, 0, 0]).unwrap();
//     vec.write(&(len as u64).to_le_bytes()).unwrap();
//     vec.write(&[128, 0, 0, 0]).unwrap();
//     vec.write(&(err.len() as u32).to_le_bytes()).unwrap();
//     vec.write(err.as_bytes()).unwrap();
//     vec.write(&PADDING[len % 8]).unwrap();
//     vec
// }

pub fn serialize(j: &J, compress: bool) -> Result<Vec<u8>, KolaError> {
    // -1 => 255
    let code = j.get_j_type_code() as u8;
    let size = estimate_size(j, compress)?;
    let mut vec: Vec<u8> = Vec::with_capacity(size);
    vec.write(&[code, 0, 0, 0]).unwrap();
    match j {
        J::Boolean(v) => {
            vec.write(&[(*v as u8), 0, 0, 0]).unwrap();
        }
        J::U8(v) => {
            vec.write(&[*v, 0, 0, 0]).unwrap();
        }
        J::I16(v) => {
            vec.write(&(*v as i32).to_le_bytes()).unwrap();
        }
        J::I32(v) => {
            vec.write(&v.to_le_bytes()).unwrap();
        }
        J::Date(v) => {
            vec.write(&(v.num_days_from_ce() - 719163).to_le_bytes())
                .unwrap();
        }
        J::I64(v) => {
            vec.write(&[0, 0, 0, 0]).unwrap();
            vec.write(&v.to_le_bytes()).unwrap();
        }
        J::Time(v) => {
            vec.write(&[0, 0, 0, 0]).unwrap();
            let ns = (v.num_seconds_from_midnight() as i64) * 1000_000_000 + v.nanosecond() as i64;
            vec.write(&ns.to_le_bytes()).unwrap();
        }
        J::DateTime(v) => {
            vec.write(&[0, 0, 0, 0]).unwrap();
            vec.write(&v.timestamp_nanos_opt().unwrap().to_le_bytes())
                .unwrap();
        }
        J::Duration(v) => {
            vec.write(&[0, 0, 0, 0]).unwrap();
            vec.write(&v.num_nanoseconds().unwrap().to_le_bytes())
                .unwrap();
        }
        J::F32(v) => {
            vec.write(&v.to_le_bytes()).unwrap();
        }
        J::F64(v) => {
            vec.write(&[0, 0, 0, 0]).unwrap();
            vec.write(&v.to_le_bytes()).unwrap();
        }
        J::Symbol(s) | J::String(s) => {
            vec.write(&(s.len() as u32).to_le_bytes()).unwrap();
            vec.write(s.as_bytes()).unwrap();
            vec.write(&PADDING[s.len() % 8]).unwrap();
        }
        J::Series(s) => {
            let mut df = s.clone().into_frame();
            vec.write(&[0, 0, 0, 0]).unwrap();
            vec.write(&0u64.to_le_bytes()).unwrap();
            let compression = if compress && s.estimated_size() > IPC_COMPRESS_THRESHOLD {
                Some(IpcCompression::LZ4)
            } else {
                None
            };
            IpcStreamWriter::new(&mut vec)
                .with_compression(compression)
                .finish(&mut df)
                .map_err(|e| KolaError::NotAbleToSerializeErr(e.to_string()))?;
            let length = vec.len() - 16;
            for (i, u) in length.to_le_bytes().into_iter().enumerate() {
                vec[i + 8] = u
            }
            vec.write(&PADDING[length % 8]).unwrap();
        }
        J::MixedList(l) => {
            // length of list
            vec.write(&(l.len() as u32).to_le_bytes()).unwrap();
            if l.len() > 0 {
                let v = l
                    .iter()
                    .map(|j| serialize(j, compress))
                    .collect::<Result<Vec<Vec<u8>>, KolaError>>()?;
                // length of list size
                let length: usize = v.iter().map(|b| b.len()).sum();
                vec.write(&(length as u64).to_le_bytes()).unwrap();
                v.into_iter().for_each(|b| {
                    vec.write(&b).unwrap();
                });
            }
        }
        // K::Matrix(m) => {
        //     let length = m.len() * 8 + 8;
        //     vec.write(&(length as u32).to_le_bytes()).unwrap();
        //     vec.write(&(m.nrows() as u32).to_le_bytes()).unwrap();
        //     vec.write(&(m.ncols() as u32).to_le_bytes()).unwrap();
        //     let ptr = m.as_slice().unwrap();
        //     let ptr = ptr.as_ptr() as *const u8;
        //     let v8 = unsafe { core::slice::from_raw_parts(ptr, m.len() * 8) };
        //     vec.write(v8).unwrap();
        // }
        J::Dict(d) => {
            let d_len = d.len();
            vec.write(&(d_len as u32).to_le_bytes()).unwrap();
            if d_len > 0 {
                // reserve d byte len
                vec.write(&0u64.to_le_bytes()).unwrap();

                // reserve keys byte len
                vec.write(&0u64.to_le_bytes()).unwrap();
                let k_lengths = d.keys().map(|s| s.len() as u32).collect::<Vec<u32>>();
                let mut offsets = vec![0u32; k_lengths.len()];
                offsets[0] = k_lengths[0];
                for i in 1..k_lengths.len() {
                    offsets[i] = k_lengths[i] + offsets[i - 1]
                }
                let v8: &[u8] =
                    unsafe { std::slice::from_raw_parts(offsets.as_ptr().cast(), d_len * 4) };
                vec.write(v8).unwrap();
                d.keys().for_each(|s| {
                    vec.write(s.as_bytes()).unwrap();
                });
                vec.write(&PADDING[vec.len() % 8]).unwrap();
                // fill keys byte len
                for (i, u) in ((vec.len() - 24) as u64)
                    .to_le_bytes()
                    .into_iter()
                    .enumerate()
                {
                    vec[i + 16] = u
                }
                // mark start of values
                let v_start = vec.len();
                // reserve values byte len
                vec.write(&0u64.to_le_bytes()).unwrap();

                d.values().for_each(|v| {
                    vec.write(&serialize(v, compress).unwrap()).unwrap();
                });
                let v_len = (vec.len() - v_start - 8) as u64;
                // write v len
                for (i, u) in v_len.to_le_bytes().into_iter().enumerate() {
                    vec[v_start + i] = u
                }

                let d_byte_len = (vec.len() - 16) as u64;
                // write d len
                for (i, u) in d_byte_len.to_le_bytes().into_iter().enumerate() {
                    vec[i + 8] = u
                }
            }
        }
        J::DataFrame(df) => {
            vec.write(&[0, 0, 0, 0]).unwrap();
            let compression = if compress && df.estimated_size() > IPC_COMPRESS_THRESHOLD {
                Some(IpcCompression::LZ4)
            } else {
                None
            };
            // reserve length
            vec.write(&0u64.to_le_bytes()).unwrap();
            IpcStreamWriter::new(&mut vec)
                .with_compression(compression)
                .finish(&mut df.clone())
                .map_err(|e| KolaError::NotAbleToSerializeErr(e.to_string()))?;
            let length = vec.len() - 16;
            for (i, u) in length.to_le_bytes().into_iter().enumerate() {
                vec[i + 8] = u
            }
            vec.write(&PADDING[length % 8]).unwrap();
        }
        J::Null => {
            vec.write(&[0, 0, 0, 0]).unwrap();
        }
        _ => return Err(KolaError::NotAbleToSerializeErr(format!("{:?}", j))),
    }
    Ok(vec)
}

fn estimate_size(j: &J, compress: bool) -> Result<usize, KolaError> {
    match j {
        // 4 bytes type code, 4 bytes value
        J::Null => Ok(8),
        J::Boolean(_) | J::U8(_) => Ok(8),
        J::I16(_) => Ok(8),
        J::I32(_) | J::Date(_) | J::F32(_) => Ok(8),
        // 8 bytes type code, 8 bytes value
        J::I64(_) | J::Time(_) | J::DateTime(_) | J::Duration(_) | J::F64(_) => Ok(16),
        // 4 bytes type code, 4 bytes length, string bytes, padding
        J::Symbol(s) | J::String(s) => Ok(16 + s.len()),
        // 4 bytes type code, 4 bytes length, series bytes, padding
        J::Series(s) => {
            let estimated_size = s.estimated_size();
            let ratio = if compress && estimated_size > IPC_COMPRESS_THRESHOLD {
                647
            } else {
                1009
            };
            Ok(1699 + (estimated_size * ratio / 1000) as usize)
        }
        // 4 bytes type code, 4 bytes length, list bytes, padding
        J::MixedList(l) => {
            let v = l
                .iter()
                .map(|j| estimate_size(j, compress))
                .collect::<Result<Vec<usize>, KolaError>>()?;
            Ok(v.iter().sum::<usize>() + 16)
        }
        // not supported yet
        // K::Matrix(m) => Ok(5 + m.len() * 8 + 8),
        J::Dict(d) => {
            // 4 - dict type code
            // 4 - item length of dict
            // 8 - byte length of j dict
            // 8 - byte length of keys
            // 4 * d_len - offsets
            // s - syms
            // ? - padding to align by 8 bytes
            // 8 - length of values - v
            // v - values
            if d.len() == 0 {
                Ok(8)
            } else {
                let mut length: usize = 40;
                for (s, j) in d.iter() {
                    length += 4 + s.len() + estimate_size(j, compress)?;
                }
                Ok(length)
            }
        }
        // 8 bytes type code, 8 bytes length, dataframe bytes, padding
        J::DataFrame(df) => {
            let estimated_size = df.estimated_size();
            let ratio = if compress && estimated_size > IPC_COMPRESS_THRESHOLD {
                647
            } else {
                1009
            };
            Ok(1699 + (estimated_size * ratio / 1000) as usize)
        }
        _ => {
            return Err(KolaError::NotAbleToSerializeErr(format!(
                "unable to calc len for k type {0:?}",
                j
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::types::J;
    use crate::{serde9::deserialize, serde9::serialize};
    use chrono::{DateTime, Duration, NaiveDate, NaiveTime};
    use indexmap::IndexMap;
    // use ndarray::ArcArray2;
    use polars::{df, prelude::NamedFrom, series::Series};

    #[test]
    fn serde_bool() {
        let j = J::Boolean(true);
        let v8: &[u8] = &[255, 0, 0, 0, 1, 0, 0, 0];
        assert_eq!(serialize(&j, false).unwrap(), v8);
        assert_eq!(deserialize(v8, &mut 0).unwrap(), j);
    }

    #[test]
    fn serde_u8() {
        let j = J::U8(1);
        let v8: &[u8] = &[254, 0, 0, 0, 1, 0, 0, 0];
        assert_eq!(serialize(&j, false).unwrap(), v8);
        assert_eq!(deserialize(v8, &mut 0).unwrap(), j);
    }

    #[test]
    fn serde_i16() {
        let j = J::I16(258);
        let v8: &[u8] = &[253, 0, 0, 0, 2, 1, 0, 0];
        assert_eq!(serialize(&j, false).unwrap(), v8);
        assert_eq!(deserialize(v8, &mut 0).unwrap(), j);
    }

    #[test]
    fn serde_i32() {
        let j = J::I32(16909060);
        let v8: &[u8] = &[252, 0, 0, 0, 4, 3, 2, 1];
        assert_eq!(serialize(&j, false).unwrap(), v8);
        assert_eq!(deserialize(v8, &mut 0).unwrap(), j);
    }

    #[test]
    fn serde_i64() {
        let j = J::I64(72623859790382856);
        let v8: &[u8] = &[251, 0, 0, 0, 0, 0, 0, 0, 8, 7, 6, 5, 4, 3, 2, 1];
        assert_eq!(serialize(&j, false).unwrap(), v8);
        assert_eq!(deserialize(v8, &mut 0).unwrap(), j);
    }

    #[test]
    fn serde_date() {
        let j = J::Date(NaiveDate::from_num_days_from_ce_opt(16909060 + 719163).unwrap());
        let v8: &[u8] = &[250, 0, 0, 0, 4, 3, 2, 1];
        assert_eq!(serialize(&j, false).unwrap(), v8);
        assert_eq!(deserialize(v8, &mut 0).unwrap(), j);
    }

    #[test]
    fn serde_time() {
        let j = J::Time(NaiveTime::from_num_seconds_from_midnight_opt(86399, 999_999_999).unwrap());
        let v8: &[u8] = &[249, 0, 0, 0, 0, 0, 0, 0, 255, 255, 78, 145, 148, 78, 0, 0];
        assert_eq!(serialize(&j, false).unwrap(), v8);
        assert_eq!(deserialize(v8, &mut 0).unwrap(), j);
    }

    #[test]
    fn serde_datetime() {
        let j = J::DateTime(DateTime::from_timestamp_nanos(86399999_999_999));
        let v8: &[u8] = &[247, 0, 0, 0, 0, 0, 0, 0, 255, 255, 78, 145, 148, 78, 0, 0];
        assert_eq!(serialize(&j, false).unwrap(), v8);
        assert_eq!(deserialize(v8, &mut 0).unwrap(), j);
    }

    #[test]
    fn serde_duration() {
        let j = J::Duration(Duration::nanoseconds(86399999999999));
        let v8: &[u8] = &[246, 0, 0, 0, 0, 0, 0, 0, 255, 255, 78, 145, 148, 78, 0, 0];
        assert_eq!(serialize(&j, false).unwrap(), v8);
        assert_eq!(deserialize(v8, &mut 0).unwrap(), j);
    }

    #[test]
    fn serde_f32() {
        let j = J::F32(9.9e10);
        let v8: &[u8] = &[245, 0, 0, 0, 225, 102, 184, 81];
        assert_eq!(serialize(&j, false).unwrap(), v8);
        assert_eq!(deserialize(v8, &mut 0).unwrap(), j);
    }

    #[test]
    fn serde_f64() {
        let j = J::F64(9.9e10);
        let v8: &[u8] = &[244, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 30, 220, 12, 55, 66];
        assert_eq!(serialize(&j, false).unwrap(), v8);
        assert_eq!(deserialize(v8, &mut 0).unwrap(), j);
    }

    #[test]
    fn serde_string() {
        let j = J::String("🍺".to_owned());
        let v8: &[u8] = &[243, 0, 0, 0, 4, 0, 0, 0, 240, 159, 141, 186, 0, 0, 0, 0];
        assert_eq!(serialize(&j, false).unwrap(), v8);
        assert_eq!(deserialize(v8, &mut 0).unwrap(), j);
    }

    #[test]
    fn serde_symbol() {
        let j = J::Symbol("".to_owned());
        let v8: &[u8] = &[242, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(serialize(&j, false).unwrap(), v8);
        assert_eq!(deserialize(v8, &mut 0).unwrap(), j);
    }

    // #[test]
    // fn serde_matrix() {
    //     let j = K::Matrix(ArcArray2::from_shape_vec((1, 2), [1.0, 2.0f64].into()).unwrap());
    //     let v8: &[u8] = &[
    //         21, 24, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0,
    //         0, 64,
    //     ];
    //     assert_eq!(serialize(&j, false).unwrap(), v8);
    //     assert_eq!(deserialize(v8, &mut 0).unwrap(), j);
    // }

    #[test]
    fn serde_series() {
        let j = J::Series(Series::new("".into(), [0, 1, 2, 3]));
        let v8: &[u8] = &[
            4, 0, 0, 0, 0, 0, 0, 0, 80, 1, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 120, 0, 0, 0, 4,
            0, 0, 0, 242, 255, 255, 255, 20, 0, 0, 0, 4, 0, 1, 0, 0, 0, 10, 0, 11, 0, 8, 0, 10, 0,
            4, 0, 248, 255, 255, 255, 12, 0, 0, 0, 8, 0, 8, 0, 0, 0, 4, 0, 1, 0, 0, 0, 4, 0, 0, 0,
            236, 255, 255, 255, 56, 0, 0, 0, 32, 0, 0, 0, 24, 0, 0, 0, 1, 2, 0, 0, 16, 0, 18, 0, 4,
            0, 16, 0, 17, 0, 8, 0, 0, 0, 12, 0, 0, 0, 0, 0, 244, 255, 255, 255, 32, 0, 0, 0, 1, 0,
            0, 0, 8, 0, 9, 0, 4, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 128, 0, 0, 0,
            4, 0, 0, 0, 236, 255, 255, 255, 64, 0, 0, 0, 0, 0, 0, 0, 20, 0, 0, 0, 4, 0, 3, 0, 12,
            0, 19, 0, 16, 0, 18, 0, 12, 0, 4, 0, 234, 255, 255, 255, 4, 0, 0, 0, 0, 0, 0, 0, 60, 0,
            0, 0, 16, 0, 0, 0, 0, 0, 10, 0, 20, 0, 4, 0, 12, 0, 16, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 1, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0,
            0, 2, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            255, 255, 255, 255, 0, 0, 0, 0,
        ];
        assert_eq!(serialize(&j, false).unwrap(), v8);
        assert_eq!(deserialize(v8, &mut 0).unwrap(), j);
    }

    #[test]
    fn serde_mixed_list() {
        let j = J::MixedList(vec![
            J::U8(9),
            J::Date(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()),
            J::Null,
        ]);
        let v8: &[u8] = &[
            90, 0, 0, 0, 3, 0, 0, 0, 24, 0, 0, 0, 0, 0, 0, 0, 254, 0, 0, 0, 9, 0, 0, 0, 250, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ];
        assert_eq!(serialize(&j, false).unwrap(), v8);
        assert_eq!(deserialize(v8, &mut 0).unwrap(), j);
    }

    #[test]
    fn serde_dict() {
        let mut dict = IndexMap::new();
        dict.insert("byte".to_owned(), J::U8(9));
        dict.insert(
            "date".to_owned(),
            J::Date(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()),
        );
        dict.insert("null".to_owned(), J::Null);
        let j = J::Dict(dict);
        let v8: &[u8] = &[
            91, 0, 0, 0, 3, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 24, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0,
            8, 0, 0, 0, 12, 0, 0, 0, 98, 121, 116, 101, 100, 97, 116, 101, 110, 117, 108, 108, 24,
            0, 0, 0, 0, 0, 0, 0, 254, 0, 0, 0, 9, 0, 0, 0, 250, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];
        assert_eq!(serialize(&j, false).unwrap(), v8);
        assert_eq!(deserialize(v8, &mut 0).unwrap(), j);
    }

    #[test]
    fn serde_df() {
        let df = df!("Element" => &["Copper", "Silver", "Gold"],
        "Melting Point (K)" => &[1357.77, 1234.93, 1337.33])
        .unwrap();
        let j = J::DataFrame(df);
        let v8: &[u8] = &[
            92, 0, 0, 0, 0, 0, 0, 0, 72, 2, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 176, 0, 0, 0, 4,
            0, 0, 0, 242, 255, 255, 255, 20, 0, 0, 0, 4, 0, 1, 0, 0, 0, 10, 0, 11, 0, 8, 0, 10, 0,
            4, 0, 248, 255, 255, 255, 12, 0, 0, 0, 8, 0, 8, 0, 0, 0, 4, 0, 2, 0, 0, 0, 68, 0, 0, 0,
            4, 0, 0, 0, 176, 255, 255, 255, 32, 0, 0, 0, 16, 0, 0, 0, 8, 0, 0, 0, 1, 3, 0, 0, 0, 0,
            0, 0, 250, 255, 255, 255, 2, 0, 6, 0, 6, 0, 4, 0, 17, 0, 0, 0, 77, 101, 108, 116, 105,
            110, 103, 32, 80, 111, 105, 110, 116, 32, 40, 75, 41, 0, 0, 0, 236, 255, 255, 255, 44,
            0, 0, 0, 32, 0, 0, 0, 24, 0, 0, 0, 1, 20, 0, 0, 16, 0, 18, 0, 4, 0, 16, 0, 17, 0, 8, 0,
            0, 0, 12, 0, 0, 0, 0, 0, 252, 255, 255, 255, 4, 0, 4, 0, 7, 0, 0, 0, 69, 108, 101, 109,
            101, 110, 116, 0, 255, 255, 255, 255, 192, 0, 0, 0, 4, 0, 0, 0, 236, 255, 255, 255,
            192, 0, 0, 0, 0, 0, 0, 0, 20, 0, 0, 0, 4, 0, 3, 0, 12, 0, 19, 0, 16, 0, 18, 0, 12, 0,
            4, 0, 234, 255, 255, 255, 3, 0, 0, 0, 0, 0, 0, 0, 108, 0, 0, 0, 16, 0, 0, 0, 0, 0, 10,
            0, 20, 0, 4, 0, 12, 0, 16, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 32, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 16, 0, 0,
            0, 0, 0, 0, 0, 128, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 0, 0, 0, 0, 0, 0,
            0, 24, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6,
            0, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 67,
            111, 112, 112, 101, 114, 83, 105, 108, 118, 101, 114, 71, 111, 108, 100, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 174, 71, 225, 122, 20, 55, 149, 64, 31, 133,
            235, 81, 184, 75, 147, 64, 184, 30, 133, 235, 81, 229, 148, 64, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 255, 255, 255, 255, 0, 0, 0, 0,
        ];
        assert_eq!(serialize(&j, false).unwrap(), v8);
        assert_eq!(deserialize(v8, &mut 0).unwrap(), j);
    }

    #[test]
    fn serde_null() {
        let j = J::Null;
        let v8: &[u8] = &[0, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(serialize(&j, false).unwrap(), v8);
        assert_eq!(deserialize(v8, &mut 0).unwrap(), j);
    }
}
