use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyRuntimeError};
use pyo3::PyErr;
use std::fmt::Debug;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PyKolaError {
    #[error(transparent)]
    KolaError(#[from] kola::errors::KolaError),

    #[error("{0:?}")]
    PythonError(String),
}

impl std::convert::From<PyKolaError> for PyErr {
    fn from(err: PyKolaError) -> PyErr {
        let default = || PyRuntimeError::new_err(format!("{:?}", &err));
        use PyKolaError::*;
        match &err {
            KolaError(_) => QKolaError::new_err(err.to_string()),
            PythonError(_) => default(),
        }
    }
}

create_exception!(kola.exceptions, QKolaError, PyException);
