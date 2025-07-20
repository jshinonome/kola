pub mod connector;
pub mod error;

use crate::connector::{generate_j6_ipc_msg, read_j6_binary_table, KolaConnector};
use error::{KolaAuthError, KolaError, KolaIOError};
use pyo3::prelude::*;

#[pymodule]
fn kola(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<KolaConnector>()?;
    m.add("KolaError", py.get_type::<KolaError>())?;
    m.add("KolaIOError", py.get_type::<KolaIOError>())?;
    m.add("KolaAuthError", py.get_type::<KolaAuthError>())?;
    m.add_function(wrap_pyfunction!(read_j6_binary_table, m)?)?;
    m.add_function(wrap_pyfunction!(generate_j6_ipc_msg, m)?)?;
    Ok(())
}
