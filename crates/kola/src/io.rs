use std::fs::File;
use std::io::Cursor;
use std::io::{self, BufReader, Read, Write};
use std::str;

use lz4_flex::frame::FrameDecoder;
use polars::frame::DataFrame;
use xxhash_rust::xxh32;

use crate::errors::KolaError;
use crate::serde6;
use crate::types::{MsgType, J};

pub fn read_j6_binary_table(path: &str) -> Result<DataFrame, KolaError> {
    let f = File::open(path).map_err(|e| KolaError::IOError(e))?;
    let mut reader = BufReader::new(f);
    let mut buffer = Vec::new();
    reader
        .read_to_end(&mut buffer)
        .map_err(|e| KolaError::IOError(e))?;
    if buffer[0..8] == vec![107u8, 120, 122, 105, 112, 112, 101, 100] {
        buffer = unzip(&buffer)?;
    }
    match serde6::deserialize(&buffer, &mut 2, false)? {
        J::DataFrame(k) => Ok(k),
        _ => Err(KolaError::Err("Not a table".to_owned())),
    }
}

pub fn unzip(buf: &Vec<u8>) -> Result<Vec<u8>, KolaError> {
    let block_num = usize::from_le_bytes(buf[buf.len() - 8..].try_into().unwrap());
    let footer_index = buf.len() - (block_num + 5) * 8;
    match buf[footer_index + 4] {
        4 => Ok(unzip_lz4(&buf, footer_index, block_num)),
        _ => Err(KolaError::Err(format!(
            "Not supported compression algo - {0}",
            buf[footer_index + 4]
        ))),
    }
}

pub fn unzip_lz4(buf: &Vec<u8>, footer_index: usize, block_num: usize) -> Vec<u8> {
    let footer = &buf[footer_index..];
    // let compress_level = footer[5];
    let block_size = usize::from_le_bytes(footer[24..32].try_into().unwrap());
    let zipped_size =
        usize::from_le_bytes(footer[16..24].try_into().unwrap()) + 19 + block_size * 4 - 8;
    let unzipped_size = usize::from_le_bytes(footer[8..16].try_into().unwrap());
    let mut zipped_bytes: Vec<u8> = Vec::with_capacity(zipped_size);
    // magic word 0x04224D18
    // fix config 0x68
    zipped_bytes.write(&[4u8, 34, 77, 24, 104]).unwrap();
    // block size config 0x40 16, 0x50 18, 0x60 20, 0x70 22
    zipped_bytes
        .write(&[((block_size.ilog2() / 2 - 4) * 16) as u8])
        .unwrap();
    zipped_bytes.write(&footer[8..16]).unwrap();
    let chuck_sum = xxh32::xxh32(&zipped_bytes[4..], 0).to_le_bytes();
    zipped_bytes.write(&chuck_sum[1..2]).unwrap();
    let mut block_start_index = 8;
    for i in 0..block_num {
        let size = u32::from_le_bytes(footer[32 + i * 8..36 + i * 8].try_into().unwrap()) as usize;
        zipped_bytes.write(&footer[32 + i * 8..36 + i * 8]).unwrap();
        zipped_bytes
            .write(&buf[block_start_index..block_start_index + size])
            .unwrap();
        block_start_index += size;
    }
    zipped_bytes.write(&[0, 0, 0, 0]).unwrap();
    let reader = Cursor::new(zipped_bytes);
    let mut rdr = FrameDecoder::new(reader);
    let mut unzipped_bytes: Vec<u8> = Vec::with_capacity(unzipped_size);
    io::copy(&mut rdr, &mut unzipped_bytes).unwrap();
    unzipped_bytes
}

pub fn generate_j6_ipc_msg(
    msg_type: MsgType,
    enable_compression: bool,
    k: J,
) -> Result<Vec<u8>, KolaError> {
    let length = k.len()?;
    let mut vec: Vec<u8> = Vec::with_capacity(length + 8);
    vec.write(&[1, msg_type as u8, 0, 0]).unwrap();
    vec.write(&(length as u32 + 8).to_le_bytes()).unwrap();
    vec.write(&serde6::serialize(&k)?).unwrap();
    if enable_compression {
        Ok(serde6::compress(vec))
    } else {
        Ok(vec)
    }
}

pub fn deserialize_j6(buf: &[u8]) -> Result<J, KolaError> {
    serde6::deserialize(buf, &mut 0, false)
}

#[cfg(test)]
mod tests {
    use polars::{
        datatypes::{CategoricalOrdering, DataType},
        frame::DataFrame,
        prelude::NamedFrom,
        series::Series,
    };
    use polars_arrow::array::Utf8Array;

    use crate::{io, serde6::deserialize};

    #[test]
    fn unzip_lz4() {
        let zipped: Vec<u8> = [
            107, 120, 122, 105, 112, 112, 101, 100, 241, 11, 255, 1, 98, 0, 99, 11, 0, 3, 0, 0, 0,
            115, 121, 109, 0, 113, 116, 121, 0, 112, 114, 105, 99, 101, 0, 0, 20, 0, 177, 11, 0, 2,
            0, 0, 0, 97, 0, 98, 0, 7, 10, 0, 34, 1, 0, 1, 0, 4, 8, 0, 17, 9, 22, 0, 2, 1, 0, 160,
            240, 63, 0, 0, 0, 0, 0, 0, 240, 63, 3, 0, 0, 0, 4, 6, 0, 0, 85, 0, 0, 0, 0, 0, 0, 0,
            78, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 70, 0, 0, 0, 4, 0, 0, 0, 1, 0, 0, 0,
            0, 0, 0, 0,
        ]
        .to_vec();
        let unzipped: Vec<u8> = [
            255, 1, 98, 0, 99, 11, 0, 3, 0, 0, 0, 115, 121, 109, 0, 113, 116, 121, 0, 112, 114,
            105, 99, 101, 0, 0, 0, 3, 0, 0, 0, 11, 0, 2, 0, 0, 0, 97, 0, 98, 0, 7, 0, 2, 0, 0, 0,
            1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 9, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            240, 63, 0, 0, 0, 0, 0, 0, 240, 63,
        ]
        .to_vec();
        assert_eq!(io::unzip(&zipped).unwrap(), unzipped);
        let k = deserialize(&unzipped, &mut 2, false).unwrap();
        let df: DataFrame = k.try_into().unwrap();
        let sym = Series::from_arrow(
            "sym".into(),
            Utf8Array::<i64>::from([Some("a"), Some("b")]).boxed(),
        )
        .unwrap()
        .cast(&DataType::Categorical(None, CategoricalOrdering::Lexical))
        .unwrap();
        let qty = Series::new("qty".into(), [1i64, 1].as_ref());
        let price = Series::new("price".into(), [1.0f64, 1.0].as_ref());
        let expect = DataFrame::new(vec![sym.into(), qty.into(), price.into()]).unwrap();
        assert_eq!(df, expect);
    }
}
