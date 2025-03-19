pub mod error;
pub mod q;

use crate::q::{generate_ipc_msg, read_binary_table, QConnector};
use error::{QKolaAuthError, QKolaError, QKolaIOError};
use pyo3::prelude::*;

#[pymodule]
fn kola(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<QConnector>()?;
    m.add("QKolaError", py.get_type::<QKolaError>())?;
    m.add("QKolaIOError", py.get_type::<QKolaIOError>())?;
    m.add("QKolaAuthError", py.get_type::<QKolaAuthError>())?;
    m.add_function(wrap_pyfunction!(read_binary_table, m)?)?;
    m.add_function(wrap_pyfunction!(generate_ipc_msg, m)?)?;
    Ok(())
}
