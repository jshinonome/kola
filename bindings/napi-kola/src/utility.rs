use std::panic::{catch_unwind, AssertUnwindSafe};

use kola::io::{generate_j6_ipc_msg, read_j6_binary_table};
use kola::types::{MsgType, K};
use napi::bindgen_prelude::{AsyncTask, Env, Task};
use napi_derive::napi;

use crate::dto::{k_into_native, NativeResult, NativeValue, OwnedNativeValue};
use crate::error::BindingError;

enum UtilityOperation {
    Ready(Option<NativeResult>),
    ReadBinary {
        path: String,
    },
    Serialize {
        message_type: String,
        compress: bool,
        value: OwnedNativeValue,
    },
}

fn is_windows_remote_or_device_path(path: &str) -> bool {
    let bytes = path.as_bytes();
    (bytes.len() >= 2 && matches!(bytes[0], b'\\' | b'/') && matches!(bytes[1], b'\\' | b'/'))
        || path.starts_with(r"\??\")
        || path.starts_with("/??/")
}

fn validate_read_binary_path(path: &str) -> Result<(), BindingError> {
    if cfg!(windows) && is_windows_remote_or_device_path(path) {
        return Err(BindingError::conversion(
            "readBinary6 does not accept Windows UNC or device paths",
        ));
    }
    Ok(())
}

pub struct UtilityTask {
    operation: UtilityOperation,
}

#[napi]
impl Task for UtilityTask {
    type Output = NativeResult;
    type JsValue = NativeResult;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        let operation = std::mem::replace(&mut self.operation, UtilityOperation::Ready(None));
        Ok(catch_unwind(AssertUnwindSafe(|| match operation {
            UtilityOperation::Ready(result) => result.unwrap_or_else(|| {
                NativeResult::failure(BindingError::internal(
                    "native utility task was computed more than once",
                ))
            }),
            UtilityOperation::ReadBinary { path } => {
                let result = validate_read_binary_path(&path)
                    .and_then(|()| {
                        read_j6_binary_table(&path)
                            .map(K::DataFrame)
                            .map_err(BindingError::from)
                    })
                    .and_then(k_into_native)
                    .map(Some);
                NativeResult::from_result(result)
            }
            UtilityOperation::Serialize {
                message_type,
                compress,
                value,
            } => {
                let result = parse_message_type(&message_type)
                    .and_then(|message_type| {
                        value.into_k().and_then(|value| {
                            generate_j6_ipc_msg(message_type, compress, value)
                                .map_err(BindingError::from)
                        })
                    })
                    .and_then(|bytes| k_into_native(K::CharVector(bytes)))
                    .map(Some);
                NativeResult::from_result(result)
            }
        }))
        .unwrap_or_else(|_| {
            NativeResult::failure(BindingError::internal("native utility operation panicked"))
        }))
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        Ok(output)
    }
}

#[napi(js_name = "readBinary6")]
pub fn read_binary6(path: String) -> AsyncTask<UtilityTask> {
    AsyncTask::new(UtilityTask {
        operation: UtilityOperation::ReadBinary { path },
    })
}

#[napi(js_name = "serializeAsIpcBytes6")]
pub fn serialize_as_ipc_bytes6(
    message_type: String,
    compress: bool,
    value: NativeValue,
) -> AsyncTask<UtilityTask> {
    let operation = match OwnedNativeValue::try_from(value) {
        Ok(value) => UtilityOperation::Serialize {
            message_type,
            compress,
            value,
        },
        Err(error) => UtilityOperation::Ready(Some(NativeResult::failure(error))),
    };
    AsyncTask::new(UtilityTask { operation })
}

fn parse_message_type(value: &str) -> Result<MsgType, BindingError> {
    match value {
        "async" => Ok(MsgType::Async),
        "sync" => Ok(MsgType::Sync),
        "response" => Ok(MsgType::Response),
        value => Err(BindingError::conversion(format!(
            "unsupported messageType {value:?}; expected async, sync, or response"
        ))),
    }
}

#[cfg(test)]
mod tests {
    #[cfg(windows)]
    use super::validate_read_binary_path;
    use super::{is_windows_remote_or_device_path, parse_message_type};

    #[test]
    fn detects_windows_unc_and_device_paths() {
        for path in [
            r"\\server\share\table.bin",
            r"\\?\C:\data\table.bin",
            r"\\.\PhysicalDrive0",
            "//server/share/table.bin",
            "//?/C:/data/table.bin",
            r"\/server\share\table.bin",
            r"/\server\share\table.bin",
            r"\??\C:\data\table.bin",
            "/??/C:/data/table.bin",
        ] {
            assert!(
                is_windows_remote_or_device_path(path),
                "expected unsafe Windows path: {path}"
            );
        }
    }

    #[test]
    fn accepts_local_path_syntax() {
        for path in [
            "table.bin",
            "data/table.bin",
            r"C:\data\table.bin",
            "/var/lib/kola/table.bin",
        ] {
            assert!(
                !is_windows_remote_or_device_path(path),
                "expected local path: {path}"
            );
        }
    }

    #[cfg(windows)]
    #[test]
    fn rejects_windows_remote_paths_before_reading() {
        let error = validate_read_binary_path(r"\\server\share\table.bin")
            .expect_err("UNC path should fail");
        assert_eq!(error.code, "KOLA_CONVERSION");
    }

    #[test]
    fn rejects_unknown_message_type_as_conversion_error() {
        let error = match parse_message_type("query") {
            Err(error) => error,
            Ok(_) => panic!("unknown message type"),
        };
        assert_eq!(error.code, "KOLA_CONVERSION");
    }
}
