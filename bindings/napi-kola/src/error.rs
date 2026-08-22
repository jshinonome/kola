use kola::errors::KolaError;

pub(crate) const CODE_IO: &str = "KOLA_IO";
pub(crate) const CODE_AUTH: &str = "KOLA_AUTH";
pub(crate) const CODE_SERVER: &str = "KOLA_SERVER";
pub(crate) const CODE_CONVERSION: &str = "KOLA_CONVERSION";
pub(crate) const CODE_UNSUPPORTED: &str = "KOLA_UNSUPPORTED";
pub(crate) const CODE_ERROR: &str = "KOLA_ERROR";
pub(crate) const CODE_INTERNAL: &str = "KOLA_INTERNAL";
pub(crate) const CODE_BACKPRESSURE: &str = "KOLA_BACKPRESSURE";

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct BindingError {
    pub(crate) code: &'static str,
    pub(crate) message: String,
}

impl BindingError {
    pub(crate) fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub(crate) fn conversion(message: impl Into<String>) -> Self {
        Self::new(CODE_CONVERSION, message)
    }

    pub(crate) fn backpressure(message: impl Into<String>) -> Self {
        Self::new(CODE_BACKPRESSURE, message)
    }

    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self::new(CODE_INTERNAL, message)
    }
}

impl From<KolaError> for BindingError {
    fn from(error: KolaError) -> Self {
        let code = match &error {
            KolaError::IOError(_)
            | KolaError::FailedToConnectErr(_)
            | KolaError::NotConnectedErr() => CODE_IO,
            KolaError::AuthErr() => CODE_AUTH,
            KolaError::ServerErr(_) => CODE_SERVER,
            KolaError::DeserializationErr(_)
            | KolaError::NotAbleToSerializeErr(_)
            | KolaError::OverLengthErr()
            | KolaError::TooManyArgumentErr() => CODE_CONVERSION,
            KolaError::VersionErr()
            | KolaError::NotSupportedKTypeErr(_)
            | KolaError::NotSupportedMinusTimeErr(_)
            | KolaError::NotSupportedKOperatorErr(_)
            | KolaError::NotSupportedKNestedListErr(_)
            | KolaError::NotSupportedKListErr(_)
            | KolaError::NotSupportedKMixedListErr(_, _)
            | KolaError::NotSupportedArrowTypeErr(_)
            | KolaError::NotSupportedSeriesTypeErr(_)
            | KolaError::NotSupportedArrowNestedListTypeErr(_)
            | KolaError::NotSupportedPolarsNestedListTypeErr(_)
            | KolaError::NotSupportedBigEndianErr() => CODE_UNSUPPORTED,
            KolaError::Err(_) => CODE_ERROR,
        };
        Self::new(code, error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use kola::errors::KolaError;

    use super::{
        BindingError, CODE_AUTH, CODE_BACKPRESSURE, CODE_CONVERSION, CODE_IO, CODE_SERVER,
        CODE_UNSUPPORTED,
    };

    #[test]
    fn categorizes_core_errors_stably() {
        let cases = [
            (
                BindingError::from(KolaError::IOError(io::Error::other("io"))).code,
                CODE_IO,
            ),
            (BindingError::from(KolaError::AuthErr()).code, CODE_AUTH),
            (
                BindingError::from(KolaError::ServerErr("server".into())).code,
                CODE_SERVER,
            ),
            (
                BindingError::from(KolaError::DeserializationErr("bad value".into())).code,
                CODE_CONVERSION,
            ),
            (
                BindingError::from(KolaError::NotSupportedKTypeErr(42)).code,
                CODE_UNSUPPORTED,
            ),
        ];

        for (actual, expected) in cases {
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn backpressure_has_a_stable_code() {
        assert_eq!(
            BindingError::backpressure("queue full").code,
            CODE_BACKPRESSURE
        );
    }
}
