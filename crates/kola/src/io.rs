use std::fs::File;
use std::io::Cursor;
use std::io::{self, BufReader, Read, Write};
use std::str;

use lz4_flex::frame::FrameDecoder;
use polars::frame::DataFrame;
use xxhash_rust::xxh32;

use crate::errors::KolaError;
use crate::serde6;
use crate::types::{MsgType, K};

const KXZIP_MAGIC: &[u8; 8] = b"kxzipped";
const KXZIP_HEADER_LENGTH: usize = 8;
const KXZIP_FOOTER_WORD_LENGTH: usize = 8;
const KXZIP_FIXED_FOOTER_WORDS: usize = 5;
const MAX_J6_BINARY_FILE_SIZE: u64 = 512 * 1024 * 1024;
const MAX_J6_BINARY_OUTPUT_SIZE: u64 = 512 * 1024 * 1024;
const MAX_J6_IPC_MESSAGE_SIZE: u64 = 512 * 1024 * 1024;

fn checked_j6_ipc_total_length(body_length: usize) -> Result<(usize, u32), KolaError> {
    let total_length = body_length
        .checked_add(8)
        .ok_or_else(|| KolaError::Err("J6 IPC message length overflowed".to_string()))?;
    let total_length_u64 = u64::try_from(total_length).map_err(|_| {
        KolaError::Err("J6 IPC message length cannot be represented as u64".to_string())
    })?;
    if total_length_u64 > MAX_J6_IPC_MESSAGE_SIZE {
        return Err(KolaError::Err(format!(
            "J6 IPC message length {total_length_u64} exceeds the {MAX_J6_IPC_MESSAGE_SIZE}-byte safety limit"
        )));
    }
    let header_length = u32::try_from(total_length_u64).map_err(|_| {
        KolaError::Err(format!(
            "J6 IPC message length {total_length_u64} cannot be represented in its header"
        ))
    })?;
    Ok((total_length, header_length))
}

fn kxzip_usize(bytes: &[u8], description: &str) -> Result<usize, KolaError> {
    let bytes: [u8; 8] = bytes.try_into().map_err(|_| {
        KolaError::Err(format!(
            "Kxzip {description} is shorter than the required 8 bytes"
        ))
    })?;
    usize::try_from(u64::from_le_bytes(bytes)).map_err(|_| {
        KolaError::Err(format!(
            "Kxzip {description} cannot be represented on this platform"
        ))
    })
}

fn kxzip_footer_length(block_num: usize) -> Result<usize, KolaError> {
    block_num
        .checked_add(KXZIP_FIXED_FOOTER_WORDS)
        .and_then(|words| words.checked_mul(KXZIP_FOOTER_WORD_LENGTH))
        .ok_or_else(|| KolaError::Err("Kxzip footer length overflowed".to_owned()))
}

fn lz4_block_size(footer: &[u8], block_index: usize) -> Result<u32, KolaError> {
    let offset = block_index
        .checked_mul(KXZIP_FOOTER_WORD_LENGTH)
        .and_then(|offset| 32usize.checked_add(offset))
        .ok_or_else(|| KolaError::Err("Kxzip LZ4 block metadata offset overflowed".to_owned()))?;
    let end = offset
        .checked_add(4)
        .ok_or_else(|| KolaError::Err("Kxzip LZ4 block metadata range overflowed".to_owned()))?;
    let bytes: [u8; 4] = footer
        .get(offset..end)
        .ok_or_else(|| {
            KolaError::Err(format!(
                "Kxzip footer is truncated at LZ4 block {block_index}"
            ))
        })?
        .try_into()
        .map_err(|_| KolaError::Err("Invalid Kxzip LZ4 block size".to_owned()))?;
    Ok(u32::from_le_bytes(bytes))
}

fn decompression_error(error: io::Error) -> KolaError {
    KolaError::IOError(io::Error::new(
        error.kind(),
        format!("Failed to decompress Kxzip LZ4 data: {error}"),
    ))
}

pub fn read_j6_binary_table(path: &str) -> Result<DataFrame, KolaError> {
    let metadata = std::fs::metadata(path).map_err(KolaError::IOError)?;
    if !metadata.is_file() {
        return Err(KolaError::Err(format!(
            "Q binary table path is not a regular file: {path}"
        )));
    }
    if metadata.len() > MAX_J6_BINARY_FILE_SIZE {
        return Err(KolaError::Err(format!(
            "Q binary table file exceeds the {MAX_J6_BINARY_FILE_SIZE}-byte size limit"
        )));
    }
    let f = File::open(path).map_err(KolaError::IOError)?;

    let file_length = usize::try_from(metadata.len()).map_err(|_| {
        KolaError::Err("Q binary table file size cannot be represented on this platform".to_owned())
    })?;
    let mut buffer = Vec::new();
    buffer.try_reserve_exact(file_length).map_err(|error| {
        KolaError::Err(format!(
            "Unable to allocate {file_length}-byte Q binary table buffer: {error}"
        ))
    })?;
    let mut reader = BufReader::new(f).take(MAX_J6_BINARY_FILE_SIZE + 1);
    reader
        .read_to_end(&mut buffer)
        .map_err(KolaError::IOError)?;
    if buffer.len() as u64 > MAX_J6_BINARY_FILE_SIZE {
        return Err(KolaError::Err(format!(
            "Q binary table file exceeds the {MAX_J6_BINARY_FILE_SIZE}-byte size limit"
        )));
    }
    if buffer.starts_with(KXZIP_MAGIC) {
        buffer = unzip(&buffer)?;
    }

    let k_type = buffer.get(2).copied().ok_or_else(|| {
        KolaError::DeserializationErr(
            "Q binary table file is shorter than its 2-byte header and value type".to_owned(),
        )
    })?;
    let minimum_length = match k_type {
        98 => 17,
        99 => 33,
        _ => return Err(KolaError::Err("Not a table".to_owned())),
    };
    if buffer.len() < minimum_length {
        return Err(KolaError::DeserializationErr(format!(
            "Q binary table file is structurally too short: expected at least {minimum_length} bytes, got {}",
            buffer.len()
        )));
    }

    match serde6::deserialize(&buffer, &mut 2, false)? {
        K::DataFrame(k) => Ok(k),
        _ => Err(KolaError::Err("Not a table".to_owned())),
    }
}

pub fn unzip(buf: &[u8]) -> Result<Vec<u8>, KolaError> {
    if !buf.starts_with(KXZIP_MAGIC) {
        return Err(KolaError::Err(
            "Invalid or truncated Kxzip header".to_owned(),
        ));
    }
    let block_count_start = buf
        .len()
        .checked_sub(KXZIP_FOOTER_WORD_LENGTH)
        .ok_or_else(|| KolaError::Err("Kxzip file is missing its block count".to_owned()))?;
    let block_num = kxzip_usize(&buf[block_count_start..], "block count")?;
    let footer_length = kxzip_footer_length(block_num)?;
    let footer_index = buf.len().checked_sub(footer_length).ok_or_else(|| {
        KolaError::Err(format!(
            "Kxzip footer length {footer_length} exceeds file length {}",
            buf.len()
        ))
    })?;
    if footer_index < KXZIP_HEADER_LENGTH {
        return Err(KolaError::Err(
            "Kxzip file is shorter than its 8-byte header and footer".to_owned(),
        ));
    }
    let algorithm_index = footer_index
        .checked_add(4)
        .ok_or_else(|| KolaError::Err("Kxzip compression metadata offset overflowed".to_owned()))?;
    let algorithm = buf.get(algorithm_index).copied().ok_or_else(|| {
        KolaError::Err("Kxzip footer is missing its compression algorithm".to_owned())
    })?;
    match algorithm {
        4 => unzip_lz4(buf, footer_index, block_num),
        _ => Err(KolaError::Err(format!(
            "Not supported compression algo - {algorithm}"
        ))),
    }
}

pub fn unzip_lz4(buf: &[u8], footer_index: usize, block_num: usize) -> Result<Vec<u8>, KolaError> {
    if !buf.starts_with(KXZIP_MAGIC) {
        return Err(KolaError::Err(
            "Invalid or truncated Kxzip header".to_owned(),
        ));
    }
    if footer_index < KXZIP_HEADER_LENGTH {
        return Err(KolaError::Err(
            "Kxzip compressed data starts inside its 8-byte header".to_owned(),
        ));
    }
    let expected_footer_length = kxzip_footer_length(block_num)?;
    let footer = buf
        .get(footer_index..)
        .ok_or_else(|| KolaError::Err("Kxzip footer offset exceeds file length".to_owned()))?;
    if footer.len() != expected_footer_length {
        return Err(KolaError::Err(format!(
            "Kxzip footer length mismatch: expected {expected_footer_length} bytes, got {}",
            footer.len()
        )));
    }
    if footer.get(4).copied() != Some(4) {
        return Err(KolaError::Err(
            "Kxzip footer does not describe LZ4 compression".to_owned(),
        ));
    }

    let unzipped_size = kxzip_usize(
        footer
            .get(8..16)
            .ok_or_else(|| KolaError::Err("Kxzip footer is missing output size".to_owned()))?,
        "decompressed size",
    )?;
    if u64::try_from(unzipped_size).unwrap_or(u64::MAX) > MAX_J6_BINARY_OUTPUT_SIZE {
        return Err(KolaError::Err(format!(
            "Kxzip decompressed size exceeds the {MAX_J6_BINARY_OUTPUT_SIZE}-byte safety limit"
        )));
    }
    let block_size = kxzip_usize(
        footer
            .get(24..32)
            .ok_or_else(|| KolaError::Err("Kxzip footer is missing LZ4 block size".to_owned()))?,
        "LZ4 block size",
    )?;
    let block_descriptor = match block_size {
        65_536 => 0x40,
        262_144 => 0x50,
        1_048_576 => 0x60,
        4_194_304 => 0x70,
        _ => {
            return Err(KolaError::Err(format!(
                "Unsupported Kxzip LZ4 block size {block_size}"
            )))
        }
    };

    let mut frame_capacity = 19usize;
    let mut block_start = KXZIP_HEADER_LENGTH;
    for block_index in 0..block_num {
        let size = usize::try_from(lz4_block_size(footer, block_index)?).map_err(|_| {
            KolaError::Err(format!(
                "Kxzip LZ4 block {block_index} size cannot be represented on this platform"
            ))
        })?;
        let block_end = block_start.checked_add(size).ok_or_else(|| {
            KolaError::Err(format!("Kxzip LZ4 block {block_index} range overflowed"))
        })?;
        if block_end > footer_index {
            return Err(KolaError::Err(format!(
                "Kxzip LZ4 block {block_index} ends at {block_end}, beyond compressed data ending at {footer_index}"
            )));
        }
        frame_capacity = frame_capacity
            .checked_add(4)
            .and_then(|capacity| capacity.checked_add(size))
            .ok_or_else(|| KolaError::Err("Kxzip LZ4 frame size overflowed".to_owned()))?;
        block_start = block_end;
    }
    if block_start != footer_index {
        return Err(KolaError::Err(format!(
            "Kxzip data contains {} trailing compressed bytes",
            footer_index - block_start
        )));
    }

    let mut zipped_bytes = Vec::new();
    zipped_bytes
        .try_reserve_exact(frame_capacity)
        .map_err(|error| {
            KolaError::Err(format!(
                "Unable to allocate {frame_capacity}-byte Kxzip LZ4 frame: {error}"
            ))
        })?;
    zipped_bytes.extend_from_slice(&[4, 34, 77, 24, 104, block_descriptor]);
    zipped_bytes.extend_from_slice(&footer[8..16]);
    let header_checksum = xxh32::xxh32(&zipped_bytes[4..], 0).to_le_bytes();
    zipped_bytes.push(header_checksum[1]);

    block_start = KXZIP_HEADER_LENGTH;
    for block_index in 0..block_num {
        let encoded_size = lz4_block_size(footer, block_index)?;
        let size = usize::try_from(encoded_size).map_err(|_| {
            KolaError::Err(format!(
                "Kxzip LZ4 block {block_index} size cannot be represented on this platform"
            ))
        })?;
        let block_end = block_start.checked_add(size).ok_or_else(|| {
            KolaError::Err(format!("Kxzip LZ4 block {block_index} range overflowed"))
        })?;
        let block = buf
            .get(block_start..block_end)
            .ok_or_else(|| KolaError::Err(format!("Kxzip LZ4 block {block_index} is truncated")))?;
        zipped_bytes.extend_from_slice(&encoded_size.to_le_bytes());
        zipped_bytes.extend_from_slice(block);
        block_start = block_end;
    }
    zipped_bytes.extend_from_slice(&[0, 0, 0, 0]);

    let reader = Cursor::new(zipped_bytes);
    let mut decoder = FrameDecoder::new(reader);
    let mut unzipped_bytes = Vec::new();
    unzipped_bytes
        .try_reserve_exact(unzipped_size)
        .map_err(|error| {
            KolaError::Err(format!(
                "Unable to allocate {unzipped_size}-byte Kxzip output: {error}"
            ))
        })?;
    unzipped_bytes.resize(unzipped_size, 0);
    decoder
        .read_exact(&mut unzipped_bytes)
        .map_err(decompression_error)?;
    let mut trailing = [0u8; 1];
    match decoder.read(&mut trailing).map_err(decompression_error)? {
        0 => Ok(unzipped_bytes),
        _ => Err(KolaError::Err(format!(
            "Kxzip LZ4 output exceeds declared size {unzipped_size}"
        ))),
    }
}

pub fn generate_j6_ipc_msg(
    msg_type: MsgType,
    enable_compression: bool,
    k: K,
) -> Result<Vec<u8>, KolaError> {
    let body_length = k.j6_len()?;
    let (total_length, header_length) = checked_j6_ipc_total_length(body_length)?;
    let serialized = serde6::serialize(&k)?;
    if serialized.len() != body_length {
        return Err(KolaError::Err(
            "Serialized q value length differs from its declared J6 length".to_string(),
        ));
    }
    let mut vec = Vec::new();
    vec.try_reserve_exact(total_length).map_err(|error| {
        KolaError::Err(format!(
            "Unable to allocate {total_length}-byte J6 IPC message: {error}"
        ))
    })?;
    vec.write_all(&[1, msg_type as u8, 0, 0])?;
    vec.write_all(&header_length.to_le_bytes())?;
    vec.write_all(&serialized)?;
    if enable_compression {
        Ok(serde6::compress(vec))
    } else {
        Ok(vec)
    }
}

pub fn deserialize_j6(buf: &[u8]) -> Result<K, KolaError> {
    serde6::deserialize(buf, &mut 0, false)
}

#[cfg(test)]
mod tests {
    use polars::{
        datatypes::DataType,
        frame::DataFrame,
        prelude::{Categories, NamedFrom},
        series::Series,
    };
    use polars_arrow::array::Utf8Array;

    use super::{checked_j6_ipc_total_length, MAX_J6_IPC_MESSAGE_SIZE};
    use crate::{
        io,
        serde6::{deserialize, serialize},
        types::K,
    };

    fn one_block_kxzip(block: &[u8], encoded_size: u32, unzipped_size: u64) -> Vec<u8> {
        let footer_index = 8 + block.len();
        let mut bytes = b"kxzipped".to_vec();
        bytes.extend_from_slice(block);
        let mut footer = vec![0u8; 48];
        footer[4] = 4;
        footer[8..16].copy_from_slice(&unzipped_size.to_le_bytes());
        footer[16..24].copy_from_slice(&(footer_index as u64).to_le_bytes());
        footer[24..32].copy_from_slice(&65_536u64.to_le_bytes());
        footer[32..36].copy_from_slice(&encoded_size.to_le_bytes());
        footer[40..48].copy_from_slice(&1u64.to_le_bytes());
        bytes.extend_from_slice(&footer);
        bytes
    }

    #[test]
    fn read_binary_table_rejects_short_files() {
        let path = std::env::temp_dir().join(format!("kola-short-j6-{}.bin", std::process::id()));
        let cases: &[&[u8]] = &[&[], &[255], &[255, 1], &[255, 1, 98], &[255, 1, 99]];
        for bytes in cases {
            std::fs::write(&path, bytes).expect("failed to write short q binary fixture");
            let error = io::read_j6_binary_table(
                path.to_str()
                    .expect("temporary q binary fixture path is not UTF-8"),
            )
            .expect_err("short q binary file should fail");
            assert!(matches!(
                error,
                crate::errors::KolaError::DeserializationErr(_)
            ));
        }
        std::fs::remove_file(path).expect("failed to remove short q binary fixture");
    }

    #[test]
    fn read_binary_table_rejects_missing_symbol_terminator_without_panicking() {
        let path =
            std::env::temp_dir().join(format!("kola-missing-nul-j6-{}.bin", std::process::id()));
        let bytes = [255, 1, 98, 0, 99, 11, 0, 1, 0, 0, 0, b'x', 1, 1, 1, 1, 1];
        std::fs::write(&path, bytes).expect("failed to write missing-NUL q binary fixture");
        let error = io::read_j6_binary_table(
            path.to_str()
                .expect("temporary q binary fixture path is not UTF-8"),
        )
        .expect_err("missing symbol terminator must fail");
        assert!(matches!(
            error,
            crate::errors::KolaError::DeserializationErr(_)
        ));
        std::fs::remove_file(path).expect("failed to remove missing-NUL q binary fixture");
    }

    #[test]
    fn generate_ipc_helper_rejects_oversized_lengths_before_header_conversion() {
        let maximum_body =
            usize::try_from(MAX_J6_IPC_MESSAGE_SIZE).expect("512 MiB fits usize") - 8;
        let (_, header_length) =
            checked_j6_ipc_total_length(maximum_body).expect("limit-sized helper frame");
        assert_eq!(u64::from(header_length), MAX_J6_IPC_MESSAGE_SIZE);
        assert!(checked_j6_ipc_total_length(maximum_body + 1).is_err());
        assert!(checked_j6_ipc_total_length(usize::MAX).is_err());
    }

    #[test]
    fn read_binary_table_rejects_non_file_path() {
        let error = io::read_j6_binary_table(
            std::env::temp_dir()
                .to_str()
                .expect("temporary directory path is not UTF-8"),
        )
        .expect_err("directory path should fail");
        assert!(error.to_string().contains("not a regular file"));
    }

    #[test]
    fn unzip_rejects_short_header_and_footer() {
        assert!(io::unzip(&[]).is_err());
        assert!(io::unzip(b"kxzipped").is_err());
    }

    #[test]
    fn unzip_rejects_footer_length_overflow() {
        let mut bytes = b"kxzipped".to_vec();
        bytes.extend_from_slice(&u64::MAX.to_le_bytes());
        assert!(io::unzip(&bytes).is_err());
    }

    #[test]
    fn unzip_rejects_truncated_block_range() {
        let bytes = one_block_kxzip(&[], 1, 1);
        let error = io::unzip(&bytes).expect_err("truncated LZ4 block should fail");
        assert!(error.to_string().contains("beyond compressed data"));
    }

    #[test]
    fn unzip_propagates_decompressor_error() {
        let bytes = one_block_kxzip(&[0], 1, 1);
        assert!(io::unzip(&bytes).is_err());
    }

    #[test]
    fn unzip_rejects_oversized_output() {
        let bytes = one_block_kxzip(&[0], 1, u64::MAX);
        let error = io::unzip(&bytes).expect_err("oversized Kxzip output should fail");
        assert!(error.to_string().contains("safety limit"));
    }

    #[test]
    fn unzip_rejects_trailing_compressed_bytes() {
        let bytes = one_block_kxzip(&[0, 0], 1, 1);
        let error = io::unzip(&bytes).expect_err("trailing compressed bytes should fail");
        assert!(error.to_string().contains("trailing compressed bytes"));
    }

    #[test]
    fn deserialize_j6_rejects_trailing_bytes() {
        let mut bytes = serialize(&K::I32(42)).expect("test value should serialize");
        bytes.push(0);
        let error = io::deserialize_j6(&bytes).expect_err("trailing J6 bytes should fail");
        assert!(error.to_string().contains("trailing byte"));
    }

    #[test]
    fn read_binary_table_rejects_trailing_bytes() {
        let table =
            DataFrame::new_infer_height(vec![Series::new("value".into(), [1i32].as_ref()).into()])
                .expect("test table should build");
        let mut bytes = vec![255, 1];
        bytes.extend_from_slice(
            &serialize(&K::DataFrame(table)).expect("test table should serialize"),
        );
        bytes.push(0);
        let path =
            std::env::temp_dir().join(format!("kola-trailing-j6-{}.bin", std::process::id()));
        std::fs::write(&path, bytes).expect("failed to write trailing-byte q binary fixture");
        let error = io::read_j6_binary_table(
            path.to_str()
                .expect("temporary q binary fixture path is not UTF-8"),
        )
        .expect_err("trailing q binary file bytes should fail");
        assert!(error.to_string().contains("trailing byte"));
        std::fs::remove_file(path).expect("failed to remove q binary fixture");
    }

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
        .cast(&DataType::Categorical(
            Categories::global(),
            Categories::global().mapping(),
        ))
        .unwrap();
        let qty = Series::new("qty".into(), [1i64, 1].as_ref());
        let price = Series::new("price".into(), [1.0f64, 1.0].as_ref());
        let expect =
            DataFrame::new_infer_height(vec![sym.into(), qty.into(), price.into()]).unwrap();
        assert_eq!(df, expect);
    }
}
