use polars_arrow::datatypes::ArrowDataType;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum KolaError {
    #[error(transparent)]
    IOError(#[from] std::io::Error),

    #[error("Wrong credential.")]
    AuthErr(),

    #[error("Failed to connect {0:?}.")]
    FailedToConnectErr(String),

    #[error("{0:?}")]
    DeserializationErr(String),

    #[error("{0:?}")]
    Err(String),

    #[error("Not Connected.")]
    NotConnectedErr(),

    #[error("Require 3+ version")]
    VersionErr(),

    #[error("Not supported k type {0:?}.")]
    NotSupportedKTypeErr(u8),

    #[error("Not supported minus time - k type {0:?}.")]
    NotSupportedMinusTimeErr(u8),

    #[error("Not supported k operator - k value {0:?}.")]
    NotSupportedKOperatorErr(u8),

    #[error("Not supported nested list - k type {0:?}.")]
    NotSupportedKNestedListErr(u8),

    #[error("Not supported k list - k type {0:?}.")]
    NotSupportedKListErr(u8),

    #[error("Not supported mixed list - expected k type {0:?}, but got {1:?}.")]
    NotSupportedKMixedListErr(u8, u8),

    #[error("Not supported arrow type {0:?}.")]
    NotSupportedArrowTypeErr(ArrowDataType),

    #[error("Not supported series type {0:?}.")]
    NotSupportedSeriesTypeErr(polars::datatypes::DataType),

    #[error("Not supported nested list type {0:?}.")]
    NotSupportedArrowNestedListTypeErr(ArrowDataType),

    #[error("Not supported polars nested list type {0:?}.")]
    NotSupportedPolarsNestedListTypeErr(polars::datatypes::DataType),

    #[error("Not supported big endian.")]
    NotSupportedBigEndianErr(),

    #[error("Not able to deserialize j type {0:?}.")]
    NotAbleDeserializeJTypeErr(u8),

    #[error("Not able to serialize {0:?}.")]
    NotAbleToSerializeErr(String),

    #[error("Length over i32::MAX.")]
    OverLengthErr(),

    #[error("Too many arguments (8 max)")]
    TooManyArgumentErr(),

    #[error("Internal Server Error - {0:?}")]
    ServerErr(String),
}
