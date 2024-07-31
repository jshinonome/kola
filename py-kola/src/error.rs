use kola::errors::KolaError as KolaOrigError;
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyRuntimeError};
use pyo3::PyErr;
use std::fmt::Debug;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PyKolaError {
    #[error(transparent)]
    KolaError(#[from] KolaOrigError),

    #[error("{0:?}")]
    PythonError(String),
}

impl std::convert::From<PyKolaError> for PyErr {
    fn from(err: PyKolaError) -> PyErr {
        let default = || PyRuntimeError::new_err(format!("{:?}", &err));
        use PyKolaError::*;
        match &err {
            KolaError(e) => match e {
                KolaOrigError::IOError(_) => QKolaIOError::new_err(err.to_string()),
                KolaOrigError::AuthErr() => QKolaAuthError::new_err(err.to_string()),
                _ => QKolaError::new_err(err.to_string()),
            },

            PythonError(_) => default(),
        }
    }
}

create_exception!(kola.exceptions, QKolaError, PyException);
create_exception!(kola.exceptions, QKolaIOError, PyException);
create_exception!(kola.exceptions, QKolaAuthError, PyException);
