use kola::errors;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::PyErr;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PyKolaError {
    #[error(transparent)]
    KolaErr(#[from] errors::KolaError),

    #[error(transparent)]
    PythonErr(#[from] PyErr),
}

impl From<PyKolaError> for PyErr {
    fn from(err: PyKolaError) -> PyErr {
        use PyKolaError::*;
        match err {
            KolaErr(e) => match &e {
                errors::KolaError::IOError(_)
                | errors::KolaError::FailedToConnectErr(_)
                | errors::KolaError::NotConnectedErr() => KolaIOError::new_err(e.to_string()),
                errors::KolaError::AuthErr() => KolaAuthError::new_err(e.to_string()),
                _ => KolaError::new_err(e.to_string()),
            },
            PythonErr(err) => err,
        }
    }
}

create_exception!(kola.exceptions, KolaError, PyException);
create_exception!(kola.exceptions, KolaIOError, PyException);
create_exception!(kola.exceptions, KolaAuthError, PyException);
