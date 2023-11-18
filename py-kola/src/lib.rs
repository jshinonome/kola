pub mod error;
pub mod q;

use crate::error::QKolaError;
use crate::q::QConnector;
use pyo3::prelude::*;

#[pymodule]
fn kola(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<QConnector>().unwrap();
    m.add("QKolaError", py.get_type::<QKolaError>()).unwrap();
    Ok(())
}
