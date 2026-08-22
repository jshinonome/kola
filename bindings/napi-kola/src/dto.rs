use chrono::{DateTime, Duration, NaiveDate, NaiveTime, Timelike, Utc};
use indexmap::IndexMap;
use kola::types::{K, MIN_Q_TIMESTAMP_UNIX_NANOS};
use napi::bindgen_prelude::{BigInt, Buffer};
use napi_derive::napi;
use std::mem::size_of;
use uuid::Uuid;

use crate::arrow::{dataframe_from_ipc, dataframe_to_ipc, series_from_ipc, series_to_ipc};
use crate::error::BindingError;

const MAX_VALUE_DEPTH: usize = 64;
const MAX_SNAPSHOT_BYTES: usize = 64 * 1024 * 1024;
const NANOS_PER_MILLISECOND: i64 = 1_000_000;
const NANOS_PER_SECOND: i64 = 1_000_000_000;
const NANOS_PER_DAY: i64 = 86_400 * NANOS_PER_SECOND;

#[napi(object)]
pub struct NativeOptions {
    pub host: String,
    pub port: u16,
    pub user: Option<String>,
    pub password: Option<String>,
    pub tls: Option<bool>,
    pub timeout_seconds: Option<u32>,
}

#[napi(object)]
pub struct NativeEntry {
    pub key: String,
    pub value: NativeValue,
}

#[napi(object)]
pub struct NativeValue {
    pub tag: String,
    pub bool_value: Option<bool>,
    pub number_value: Option<f64>,
    pub bigint_value: Option<BigInt>,
    pub string_value: Option<String>,
    pub bytes_value: Option<Buffer>,
    pub items: Option<Vec<NativeValue>>,
    pub entries: Option<Vec<NativeEntry>>,
}

#[napi(object)]
pub struct NativeError {
    pub code: String,
    pub message: String,
}

#[napi(object)]
pub struct NativeResult {
    pub ok: bool,
    pub value: Option<NativeValue>,
    pub error: Option<NativeError>,
}

impl NativeResult {
    pub(crate) fn success(value: Option<NativeValue>) -> Self {
        Self {
            ok: true,
            value,
            error: None,
        }
    }

    pub(crate) fn failure(error: BindingError) -> Self {
        Self {
            ok: false,
            value: None,
            error: Some(NativeError {
                code: error.code.to_owned(),
                message: error.message,
            }),
        }
    }

    pub(crate) fn from_result(result: Result<Option<NativeValue>, BindingError>) -> Self {
        match result {
            Ok(value) => Self::success(value),
            Err(error) => Self::failure(error),
        }
    }
}

#[derive(Debug)]
pub(crate) struct OwnedNativeEntry {
    key: String,
    value: OwnedNativeValue,
}

#[derive(Debug)]
pub(crate) struct OwnedNativeValue {
    tag: String,
    bool_value: Option<bool>,
    number_value: Option<f64>,
    bigint_value: Option<i64>,
    string_value: Option<String>,
    bytes_value: Option<Vec<u8>>,
    items: Option<Vec<OwnedNativeValue>>,
    entries: Option<Vec<OwnedNativeEntry>>,
}
struct SnapshotBudget {
    limit: usize,
    remaining: usize,
}

impl SnapshotBudget {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            remaining: limit,
        }
    }

    fn charge(&mut self, bytes: usize) -> Result<(), BindingError> {
        self.remaining = self.remaining.checked_sub(bytes).ok_or_else(|| {
            BindingError::conversion(format!(
                "native value snapshot exceeds its {} byte limit",
                self.limit
            ))
        })?;
        Ok(())
    }
}

impl TryFrom<NativeValue> for OwnedNativeValue {
    type Error = BindingError;

    fn try_from(value: NativeValue) -> Result<Self, Self::Error> {
        Self::snapshot_with_limit(value, MAX_SNAPSHOT_BYTES)
    }
}

impl OwnedNativeValue {
    fn snapshot_with_limit(value: NativeValue, limit: usize) -> Result<Self, BindingError> {
        let mut budget = SnapshotBudget::new(limit);
        Self::snapshot(value, 0, &mut budget)
    }

    fn snapshot(
        value: NativeValue,
        depth: usize,
        budget: &mut SnapshotBudget,
    ) -> Result<Self, BindingError> {
        if depth > MAX_VALUE_DEPTH {
            return Err(BindingError::conversion(format!(
                "native value nesting exceeds {MAX_VALUE_DEPTH} levels"
            )));
        }
        budget.charge(size_of::<OwnedNativeValue>())?;
        budget.charge(value.tag.len())?;
        if let Some(string) = &value.string_value {
            budget.charge(string.len())?;
        }
        if let Some(bytes) = &value.bytes_value {
            budget.charge(bytes.len())?;
        }

        let tag = value.tag;
        let bigint_value = value
            .bigint_value
            .map(|value| {
                let (value, lossless) = value.get_i64();
                if lossless {
                    Ok(value)
                } else {
                    Err(BindingError::conversion(
                        "bigintValue is outside the signed 64-bit range",
                    ))
                }
            })
            .transpose()?;
        let items = value
            .items
            .map(|items| {
                items
                    .into_iter()
                    .map(|item| Self::snapshot(item, depth + 1, budget))
                    .collect()
            })
            .transpose()?;
        let entries = value
            .entries
            .map(|entries| {
                entries
                    .into_iter()
                    .map(|entry| {
                        budget.charge(size_of::<OwnedNativeEntry>())?;
                        budget.charge(entry.key.len())?;
                        Ok(OwnedNativeEntry {
                            key: entry.key,
                            value: Self::snapshot(entry.value, depth + 1, budget)?,
                        })
                    })
                    .collect::<Result<Vec<_>, BindingError>>()
            })
            .transpose()?;

        Ok(Self {
            tag,
            bool_value: value.bool_value,
            number_value: value.number_value,
            bigint_value,
            string_value: value.string_value,
            bytes_value: value.bytes_value.map(|bytes| bytes.as_ref().to_vec()),
            items,
            entries,
        })
    }

    pub(crate) fn into_k(mut self) -> Result<K, BindingError> {
        let tag = self.tag.clone();
        match tag.as_str() {
            "null" => Ok(K::Null),
            "boolean" => Ok(K::Boolean(self.required_bool()?)),
            "guid" => Ok(K::Guid(Uuid::parse_str(&self.required_string()?).map_err(
                |error| BindingError::conversion(format!("invalid guid: {error}")),
            )?)),
            "u8" => Ok(K::U8(self.required_integer(0, u8::MAX as i64)? as u8)),
            "i16" => Ok(K::I16(
                self.required_integer(i16::MIN as i64, i16::MAX as i64)? as i16,
            )),
            "i32" => Ok(K::I32(
                self.required_integer(i32::MIN as i64, i32::MAX as i64)? as i32,
            )),
            "i64" => Ok(K::I64(self.required_bigint()?)),
            "f32" => {
                let value = self.required_number()?;
                if value.is_finite() && (value < f32::MIN as f64 || value > f32::MAX as f64) {
                    return Err(BindingError::conversion(
                        "numberValue is outside the f32 range",
                    ));
                }
                Ok(K::F32(value as f32))
            }
            "f64" => Ok(K::F64(self.required_number()?)),
            "char" => Ok(K::Char(self.required_integer(0, u8::MAX as i64)? as u8)),
            "symbol" => {
                let value = self.required_string()?;
                validate_q_symbol(&value, "symbol stringValue")?;
                Ok(K::Symbol(value))
            }
            "string" => Ok(K::String(self.required_string()?)),
            "bytes" => Ok(K::CharVector(self.required_bytes()?)),
            "timestamp" => {
                let nanos = self.required_bigint()?;
                if nanos < MIN_Q_TIMESTAMP_UNIX_NANOS {
                    return Err(BindingError::conversion(
                        "bigintValue is outside q's representable timestamp range",
                    ));
                }
                let seconds = nanos.div_euclid(NANOS_PER_SECOND);
                let subsecond_nanos = nanos.rem_euclid(NANOS_PER_SECOND) as u32;
                let value =
                    DateTime::<Utc>::from_timestamp(seconds, subsecond_nanos).ok_or_else(|| {
                        BindingError::conversion("bigintValue is outside the timestamp range")
                    })?;
                Ok(K::DateTime(value))
            }
            "date" => Ok(K::Date(
                NaiveDate::parse_from_str(&self.required_string()?, "%Y-%m-%d")
                    .map_err(|error| BindingError::conversion(format!("invalid date: {error}")))?,
            )),
            "time" => {
                let nanos = self.required_bigint()?;
                if !(0..NANOS_PER_DAY).contains(&nanos) {
                    return Err(BindingError::conversion(
                        "bigintValue for time must be within one day",
                    ));
                }
                if nanos % NANOS_PER_MILLISECOND != 0 {
                    return Err(BindingError::conversion(
                        "bigintValue for time must use millisecond precision",
                    ));
                }
                let seconds = (nanos / NANOS_PER_SECOND) as u32;
                let subsecond_nanos = (nanos % NANOS_PER_SECOND) as u32;
                Ok(K::Time(
                    NaiveTime::from_num_seconds_from_midnight_opt(seconds, subsecond_nanos)
                        .ok_or_else(|| BindingError::conversion("invalid time"))?,
                ))
            }
            "timespan" => Ok(K::Duration(Duration::nanoseconds(self.required_bigint()?))),
            "list" => Ok(K::MixedList(
                self.required_items()?
                    .into_iter()
                    .map(OwnedNativeValue::into_k)
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            "dictionary" => {
                let mut dictionary = IndexMap::new();
                for entry in self.required_entries()? {
                    validate_q_symbol(&entry.key, "dictionary key")?;
                    if dictionary.contains_key(&entry.key) {
                        return Err(BindingError::conversion(format!(
                            "dictionary contains duplicate key {:?}",
                            entry.key
                        )));
                    }
                    dictionary.insert(entry.key, entry.value.into_k()?);
                }
                Ok(K::Dict(dictionary))
            }
            "series" => Ok(K::Series(series_from_ipc(self.required_bytes()?)?)),
            "table" => Ok(K::DataFrame(dataframe_from_ipc(self.required_bytes()?)?)),
            tag => Err(BindingError::conversion(format!(
                "unsupported native value tag {tag:?}"
            ))),
        }
    }

    fn required_bool(&self) -> Result<bool, BindingError> {
        self.bool_value
            .ok_or_else(|| BindingError::conversion(format!("{} requires boolValue", self.tag)))
    }

    fn required_number(&self) -> Result<f64, BindingError> {
        self.number_value
            .ok_or_else(|| BindingError::conversion(format!("{} requires numberValue", self.tag)))
    }

    fn required_integer(&self, minimum: i64, maximum: i64) -> Result<i64, BindingError> {
        let value = self.required_number()?;
        if !value.is_finite()
            || value.fract() != 0.0
            || value < minimum as f64
            || value > maximum as f64
        {
            return Err(BindingError::conversion(format!(
                "numberValue for {} must be an integer in {minimum}..={maximum}",
                self.tag
            )));
        }
        Ok(value as i64)
    }

    fn required_bigint(&self) -> Result<i64, BindingError> {
        self.bigint_value
            .ok_or_else(|| BindingError::conversion(format!("{} requires bigintValue", self.tag)))
    }

    fn required_string(&mut self) -> Result<String, BindingError> {
        self.string_value
            .take()
            .ok_or_else(|| BindingError::conversion(format!("{} requires stringValue", self.tag)))
    }

    fn required_bytes(&mut self) -> Result<Vec<u8>, BindingError> {
        self.bytes_value
            .take()
            .ok_or_else(|| BindingError::conversion(format!("{} requires bytesValue", self.tag)))
    }

    fn required_items(&mut self) -> Result<Vec<OwnedNativeValue>, BindingError> {
        self.items
            .take()
            .ok_or_else(|| BindingError::conversion(format!("{} requires items", self.tag)))
    }

    fn required_entries(&mut self) -> Result<Vec<OwnedNativeEntry>, BindingError> {
        self.entries
            .take()
            .ok_or_else(|| BindingError::conversion(format!("{} requires entries", self.tag)))
    }
}

fn validate_q_symbol(value: &str, field: &str) -> Result<(), BindingError> {
    if value.as_bytes().contains(&0) {
        return Err(BindingError::conversion(format!(
            "{field} cannot contain NUL bytes"
        )));
    }
    Ok(())
}

pub(crate) fn native_values_into_k(values: Vec<OwnedNativeValue>) -> Result<Vec<K>, BindingError> {
    values.into_iter().map(OwnedNativeValue::into_k).collect()
}

pub(crate) fn k_into_native(value: K) -> Result<NativeValue, BindingError> {
    k_into_native_with_depth(value, 0)
}

fn k_into_native_with_depth(value: K, depth: usize) -> Result<NativeValue, BindingError> {
    if depth > MAX_VALUE_DEPTH {
        return Err(BindingError::conversion(format!(
            "native value nesting exceeds {MAX_VALUE_DEPTH} levels"
        )));
    }
    let mut native = empty_native_value();
    match value {
        K::Null => native.tag = "null".into(),
        K::Boolean(value) => {
            native.tag = "boolean".into();
            native.bool_value = Some(value);
        }
        K::Guid(value) => {
            native.tag = "guid".into();
            native.string_value = Some(value.to_string());
        }
        K::U8(value) => set_number(&mut native, "u8", value as f64),
        K::I16(value) => set_number(&mut native, "i16", value as f64),
        K::I32(value) => set_number(&mut native, "i32", value as f64),
        K::I64(value) => set_bigint(&mut native, "i64", value),
        K::F32(value) => set_number(&mut native, "f32", value as f64),
        K::F64(value) => set_number(&mut native, "f64", value),
        K::Char(value) => set_number(&mut native, "char", value as f64),
        K::Symbol(value) => {
            validate_q_symbol(&value, "symbol")?;
            set_string(&mut native, "symbol", value);
        }
        K::String(value) => set_string(&mut native, "string", value),
        K::CharVector(value) => {
            native.tag = "bytes".into();
            native.bytes_value = Some(value.into());
        }
        K::DateTime(value) => {
            let nanos = value.timestamp_nanos_opt().ok_or_else(|| {
                BindingError::conversion("timestamp cannot be represented as signed nanoseconds")
            })?;
            set_bigint(&mut native, "timestamp", nanos);
        }
        K::Date(value) => set_string(&mut native, "date", value.format("%Y-%m-%d").to_string()),
        K::Time(value) => {
            let nanos = value.num_seconds_from_midnight() as i64 * NANOS_PER_SECOND
                + value.nanosecond() as i64;
            if nanos % NANOS_PER_MILLISECOND != 0 {
                return Err(BindingError::conversion(
                    "q time only supports millisecond precision",
                ));
            }
            set_bigint(&mut native, "time", nanos);
        }
        K::Duration(value) => {
            let nanos = value.num_nanoseconds().ok_or_else(|| {
                BindingError::conversion("timespan cannot be represented as signed nanoseconds")
            })?;
            set_bigint(&mut native, "timespan", nanos);
        }
        K::MixedList(values) => {
            native.tag = "list".into();
            native.items = Some(
                values
                    .into_iter()
                    .map(|value| k_into_native_with_depth(value, depth + 1))
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
        K::Series(series) => {
            native.tag = "series".into();
            native.bytes_value = Some(series_to_ipc(series)?.into());
        }
        K::DataFrame(dataframe) => {
            native.tag = "table".into();
            native.bytes_value = Some(dataframe_to_ipc(dataframe)?.into());
        }
        K::Dict(dictionary) => {
            native.tag = "dictionary".into();
            native.entries = Some(
                dictionary
                    .into_iter()
                    .map(|(key, value)| {
                        validate_q_symbol(&key, "dictionary key")?;
                        Ok(NativeEntry {
                            key,
                            value: k_into_native_with_depth(value, depth + 1)?,
                        })
                    })
                    .collect::<Result<Vec<_>, BindingError>>()?,
            );
        }
    }
    Ok(native)
}

pub(crate) fn snapshot_native_values(
    values: Vec<NativeValue>,
) -> Result<Vec<OwnedNativeValue>, BindingError> {
    snapshot_native_values_with_limit(values, MAX_SNAPSHOT_BYTES)
}

fn snapshot_native_values_with_limit(
    values: Vec<NativeValue>,
    limit: usize,
) -> Result<Vec<OwnedNativeValue>, BindingError> {
    if values.len() > 8 {
        return Err(BindingError::conversion("Too many arguments (8 max)"));
    }
    let mut budget = SnapshotBudget::new(limit);
    values
        .into_iter()
        .map(|value| OwnedNativeValue::snapshot(value, 0, &mut budget))
        .collect()
}

fn empty_native_value() -> NativeValue {
    NativeValue {
        tag: String::new(),
        bool_value: None,
        number_value: None,
        bigint_value: None,
        string_value: None,
        bytes_value: None,
        items: None,
        entries: None,
    }
}

fn set_number(native: &mut NativeValue, tag: &str, value: f64) {
    native.tag = tag.to_owned();
    native.number_value = Some(value);
}

fn set_bigint(native: &mut NativeValue, tag: &str, value: i64) {
    native.tag = tag.to_owned();
    native.bigint_value = Some(BigInt {
        sign_bit: value.is_negative(),
        words: vec![value.unsigned_abs()],
    });
}

fn set_string(native: &mut NativeValue, tag: &str, value: String) {
    native.tag = tag.to_owned();
    native.string_value = Some(value);
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Duration, NaiveDate, NaiveTime};
    use indexmap::IndexMap;
    use kola::types::{K, MIN_Q_TIMESTAMP_UNIX_NANOS};
    use napi::bindgen_prelude::{BigInt, Buffer};
    use std::mem::size_of;

    use super::{
        empty_native_value, k_into_native, snapshot_native_values_with_limit, NativeEntry,
        OwnedNativeValue, MAX_VALUE_DEPTH,
    };

    fn bigint_value(tag: &str, sign_bit: bool, word: u64) -> super::NativeValue {
        let mut value = empty_native_value();
        value.tag = tag.into();
        value.bigint_value = Some(BigInt {
            sign_bit,
            words: vec![word],
        });
        value
    }

    #[test]
    fn accepts_exact_i64_bigint_boundaries() {
        let minimum = OwnedNativeValue::try_from(bigint_value("i64", true, 1u64 << 63))
            .expect("snapshot i64::MIN")
            .into_k()
            .expect("convert i64::MIN");
        let maximum = OwnedNativeValue::try_from(bigint_value("i64", false, i64::MAX as u64))
            .expect("snapshot i64::MAX")
            .into_k()
            .expect("convert i64::MAX");
        assert_eq!(minimum, K::I64(i64::MIN));
        assert_eq!(maximum, K::I64(i64::MAX));

        for expected in [i64::MIN, i64::MAX] {
            let actual = OwnedNativeValue::try_from(
                k_into_native(K::I64(expected)).expect("encode i64 boundary"),
            )
            .expect("snapshot output")
            .into_k()
            .expect("decode output");
            assert_eq!(actual, K::I64(expected));
        }
    }

    #[test]
    fn rejects_bigints_outside_i64() {
        let positive = OwnedNativeValue::try_from(bigint_value("i64", false, 1u64 << 63))
            .expect_err("positive overflow must fail");
        let negative = OwnedNativeValue::try_from(bigint_value("i64", true, (1u64 << 63) + 1))
            .expect_err("negative overflow must fail");
        assert_eq!(positive.code, "KOLA_CONVERSION");
        assert_eq!(negative.code, "KOLA_CONVERSION");
    }

    #[test]
    fn rejects_unknown_tags_and_missing_payloads() {
        let mut unknown = empty_native_value();
        unknown.tag = "mystery".into();
        let unknown_error = OwnedNativeValue::try_from(unknown)
            .expect("snapshot unknown tag")
            .into_k()
            .expect_err("unknown tag must fail");

        let mut missing = empty_native_value();
        missing.tag = "i64".into();
        let missing_error = OwnedNativeValue::try_from(missing)
            .expect("snapshot missing payload")
            .into_k()
            .expect_err("missing payload must fail");

        assert_eq!(unknown_error.code, "KOLA_CONVERSION");
        assert_eq!(missing_error.code, "KOLA_CONVERSION");
    }

    #[test]
    fn preserves_arbitrary_character_vector_bytes() {
        let bytes = vec![0, 0x80, 0xff, b'q'];
        let native = k_into_native(K::CharVector(bytes.clone())).expect("encode char vector");
        assert_eq!(native.tag, "bytes");
        assert_eq!(native.bytes_value.as_deref(), Some(bytes.as_slice()));

        let mut input = empty_native_value();
        input.tag = "bytes".into();
        input.bytes_value = Some(Buffer::from(bytes.clone()));
        let decoded = OwnedNativeValue::try_from(input)
            .expect("snapshot bytes")
            .into_k()
            .expect("decode bytes");
        assert_eq!(decoded, K::CharVector(bytes));
    }

    #[test]
    fn round_trips_lossless_temporal_values() {
        let timestamp = DateTime::from_timestamp(1_700_000_000, 123_456_789).expect("timestamp");
        let date = NaiveDate::from_ymd_opt(2026, 8, 22).expect("date");
        let time = NaiveTime::from_hms_nano_opt(23, 59, 58, 987_000_000).expect("time");
        let values = [
            K::DateTime(timestamp.to_owned()),
            K::Date(date.to_owned()),
            K::Time(time.to_owned()),
            K::Duration(Duration::nanoseconds(-123_456_789)),
        ];

        for expected in values {
            let actual = OwnedNativeValue::try_from(k_into_native(expected).expect("encode"))
                .expect("snapshot")
                .into_k()
                .expect("decode");
            match actual {
                K::DateTime(value) => assert_eq!(value, timestamp),
                K::Date(value) => assert_eq!(value, date),
                K::Time(value) => assert_eq!(value, time),
                K::Duration(value) => assert_eq!(value, Duration::nanoseconds(-123_456_789)),
                value => panic!("unexpected temporal value: {value:?}"),
            }
        }
    }

    #[test]
    fn round_trips_pre_epoch_and_q_timestamp_boundaries() {
        let pre_epoch = DateTime::from_timestamp(-1, 999_999_999).expect("pre-epoch timestamp");
        let minimum = DateTime::from_timestamp(
            MIN_Q_TIMESTAMP_UNIX_NANOS.div_euclid(1_000_000_000),
            MIN_Q_TIMESTAMP_UNIX_NANOS.rem_euclid(1_000_000_000) as u32,
        )
        .expect("minimum q timestamp");
        let maximum = DateTime::from_timestamp(
            i64::MAX.div_euclid(1_000_000_000),
            i64::MAX.rem_euclid(1_000_000_000) as u32,
        )
        .expect("maximum q timestamp");

        for expected in [pre_epoch, minimum, maximum] {
            let actual = OwnedNativeValue::try_from(
                k_into_native(K::DateTime(expected)).expect("encode timestamp"),
            )
            .expect("snapshot timestamp")
            .into_k()
            .expect("decode timestamp");
            assert_eq!(actual, K::DateTime(expected));
        }

        let error = OwnedNativeValue::try_from(bigint_value(
            "timestamp",
            true,
            (MIN_Q_TIMESTAMP_UNIX_NANOS - 1).unsigned_abs(),
        ))
        .expect("snapshot lower timestamp")
        .into_k()
        .expect_err("timestamp below q range must fail");
        assert_eq!(error.code, "KOLA_CONVERSION");

        let error = OwnedNativeValue::try_from(bigint_value("timestamp", false, 1u64 << 63))
            .expect_err("timestamp above signed nanosecond range must fail");
        assert_eq!(error.code, "KOLA_CONVERSION");
    }

    #[test]
    fn rejects_sub_millisecond_time_in_both_directions() {
        let input = OwnedNativeValue::try_from(bigint_value("time", false, 1))
            .expect("snapshot time")
            .into_k()
            .expect_err("sub-millisecond input must fail");
        let output = match k_into_native(K::Time(
            NaiveTime::from_hms_nano_opt(0, 0, 0, 1).expect("time"),
        )) {
            Err(error) => error,
            Ok(_) => panic!("sub-millisecond output must fail"),
        };
        assert_eq!(input.code, "KOLA_CONVERSION");
        assert_eq!(output.code, "KOLA_CONVERSION");
    }

    #[test]
    fn rejects_nul_symbols_and_dictionary_keys() {
        let mut symbol = empty_native_value();
        symbol.tag = "symbol".into();
        symbol.string_value = Some("bad\0symbol".into());
        let symbol_error = OwnedNativeValue::try_from(symbol)
            .expect("snapshot symbol")
            .into_k()
            .expect_err("NUL symbol must fail");

        let mut dictionary = empty_native_value();
        dictionary.tag = "dictionary".into();
        dictionary.entries = Some(vec![NativeEntry {
            key: "bad\0key".into(),
            value: empty_native_value(),
        }]);
        let key_error = OwnedNativeValue::try_from(dictionary)
            .expect("snapshot dictionary")
            .into_k()
            .expect_err("NUL dictionary key must fail");
        assert_eq!(symbol_error.code, "KOLA_CONVERSION");
        assert_eq!(key_error.code, "KOLA_CONVERSION");
    }

    #[test]
    fn bounds_native_output_recursion() {
        let mut value = K::Null;
        for _ in 0..=MAX_VALUE_DEPTH {
            value = K::MixedList(vec![value]);
        }
        let error = match k_into_native(value) {
            Err(error) => error,
            Ok(_) => panic!("deep output must fail"),
        };
        assert_eq!(error.code, "KOLA_CONVERSION");

        let mut dictionary = K::Null;
        for depth in 0..=MAX_VALUE_DEPTH {
            dictionary = K::Dict(IndexMap::from([(format!("level{depth}"), dictionary)]));
        }
        let error = match k_into_native(dictionary) {
            Err(error) => error,
            Ok(_) => panic!("deep dictionary output must fail"),
        };
        assert_eq!(error.code, "KOLA_CONVERSION");
    }

    #[test]
    fn enforces_aggregate_snapshot_budget_before_copying() {
        let values = (0..2)
            .map(|_| {
                let mut value = empty_native_value();
                value.tag = "bytes".into();
                value.bytes_value = Some(Buffer::from(vec![0; 16]));
                value
            })
            .collect();
        let per_value = size_of::<OwnedNativeValue>() + "bytes".len() + 16;
        let error = snapshot_native_values_with_limit(values, per_value * 2 - 1)
            .expect_err("aggregate payload over budget must fail");
        assert_eq!(error.code, "KOLA_CONVERSION");

        let too_many = (0..9).map(|_| empty_native_value()).collect();
        let error = snapshot_native_values_with_limit(too_many, usize::MAX)
            .expect_err("argument count must fail before traversal");
        assert_eq!(error.code, "KOLA_CONVERSION");
    }
}
