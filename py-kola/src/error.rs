use kola::errors;
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyRuntimeError};
use pyo3::PyErr;
use std::fmt::Debug;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PyKolaError {
    #[error(transparent)]
    KolaErr(#[from] errors::KolaError),

    #[error("{0:?}")]
    PythonErr(String),
}

impl std::convert::From<PyKolaError> for PyErr {
    fn from(err: PyKolaError) -> PyErr {
        let default = || PyRuntimeError::new_err(format!("{:?}", &err));
        use PyKolaError::*;
        match &err {
            KolaErr(e) => match e {
                errors::KolaError::IOError(_) | errors::KolaError::NotConnectedErr() => {
                    KolaIOError::new_err(err.to_string())
                }
                errors::KolaError::AuthErr() => KolaAuthError::new_err(err.to_string()),
                _ => KolaError::new_err(err.to_string()),
            },

            PythonErr(_) => default(),
        }
    }
}

create_exception!(kola.exceptions, KolaError, PyException);
create_exception!(kola.exceptions, KolaIOError, PyException);
create_exception!(kola.exceptions, KolaAuthError, PyException);
