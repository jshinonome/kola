pub mod error;
pub mod q;

use crate::q::{read_binary_table, QConnector};
use error::{QKolaAuthError, QKolaError, QKolaIOError};
use pyo3::prelude::*;

#[pymodule]
fn kola(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<QConnector>()?;
    m.add("QKolaError", py.get_type_bound::<QKolaError>())?;
    m.add("QKolaIOError", py.get_type_bound::<QKolaIOError>())?;
    m.add("QKolaAuthError", py.get_type_bound::<QKolaAuthError>())?;
    m.add_function(wrap_pyfunction!(read_binary_table, m)?)?;
    Ok(())
}
