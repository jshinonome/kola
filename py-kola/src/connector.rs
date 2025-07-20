use std::cmp::{max, min};

use crate::error::PyKolaError::{self, PythonErr};
use chrono::{Datelike, Timelike};
use indexmap::IndexMap;
use kola::connector::Connector;
use kola::types::{MsgType, J};
use pyo3::types::{
    PyBool, PyBytes, PyDate, PyDateTime, PyDelta, PyDict, PyFloat, PyInt, PyList, PyString, PyTime,
    PyTuple, PyTzInfo,
};
use pyo3::{intern, prelude::*, IntoPyObjectExt};
use pyo3_polars::{PyDataFrame, PySeries};

#[pyclass]
pub struct KolaConnector {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub enable_tls: bool,
    q: Connector,
}

impl KolaConnector {
    pub(crate) fn new(
        host: &str,
        port: u16,
        user: &str,
        password: &str,
        enable_tls: bool,
        timeout: u64,
        version: u8,
    ) -> Self {
        KolaConnector {
            host: host.to_string(),
            port,
            user: user.to_string(),
            password: password.to_string(),
            enable_tls,
            q: Connector::new(host, port, user, password, enable_tls, timeout, version),
        }
    }

    fn execute(&mut self, py: Python, expr: &str, args: Bound<PyTuple>) -> PyResult<PyObject> {
        let args = cast_to_k_vec(args)?;
        let k = py.allow_threads(move || self.q.execute(expr, &args));
        let k = match k {
            Ok(k) => k,
            Err(e) => return Err(PyKolaError::from(e).into()),
        };
        cast_k_to_py(py, k)
    }

    fn execute_async(
        &mut self,
        py: Python,
        expr: &str,
        args: Bound<PyTuple>,
    ) -> Result<(), PyKolaError> {
        let args = cast_to_k_vec(args)?;
        let _ = py.allow_threads(move || self.q.execute_async(expr, &args));
        Ok(())
    }
}

fn cast_k_to_py(py: Python, k: J) -> PyResult<PyObject> {
    match k {
        J::Boolean(k) => k.into_py_any(py),
        J::Guid(k) => k.to_string().into_py_any(py),
        J::U8(k) => k.into_py_any(py),
        J::I16(k) => k.into_py_any(py),
        J::I32(k) => k.into_py_any(py),
        J::I64(k) => k.into_py_any(py),
        J::F32(k) => k.into_py_any(py),
        J::F64(k) => k.into_py_any(py),
        J::Char(k) => (k as char).into_py_any(py),
        J::Symbol(k) => k.into_py_any(py),
        J::String(k) => k.into_py_any(py),
        J::DateTime(k) => {
            if let Some(ns) = k.timestamp_nanos_opt() {
                let datetime = PyDateTime::from_timestamp(
                    py,
                    ns as f64 / 1000000000.0,
                    Some(&PyTzInfo::utc(py).unwrap()),
                )?;
                datetime.into_py_any(py)
            } else {
                Err(PythonErr("failed to get nanoseconds".to_string()).into())
            }
        }
        J::Date(k) => {
            let mut days = k.num_days_from_ce() as i64 - 719163;
            days = min(days, 2932532);
            days = max(days, -719162);
            let date = PyDate::from_timestamp(py, 86400 * days)?;
            date.into_py_any(py)
        }
        J::Time(k) => {
            let time = PyTime::new(
                py,
                k.hour() as u8,
                k.minute() as u8,
                k.second() as u8,
                k.nanosecond() / 1000,
                None,
            )?;
            time.into_py_any(py)
        }
        J::Duration(k) => {
            let delta = PyDelta::new(
                py,
                0,
                k.num_seconds() as i32,
                (k.num_microseconds().unwrap_or(0) % 1000000) as i32,
                false,
            )?;
            delta.into_py_any(py)
        }
        J::MixedList(l) => {
            let py_objects = l
                .into_iter()
                .map(|k| cast_k_to_py(py, k))
                .collect::<PyResult<Vec<PyObject>>>()?;
            PyTuple::new(py, py_objects).unwrap().into_py_any(py)
        }
        J::Series(k) => PySeries(k).into_py_any(py),
        J::DataFrame(k) => PyDataFrame(k).into_py_any(py),
        J::Null => ().into_py_any(py),
        J::Dict(dict) => {
            let py_dict = PyDict::new(py);
            for (k, v) in dict.into_iter() {
                py_dict.set_item(k, cast_k_to_py(py, v)?)?;
            }
            Ok(py_dict.into())
        }
    }
}

#[pymethods]
impl KolaConnector {
    #[new]
    pub fn __init__(
        host: &str,
        port: u16,
        user: &str,
        password: &str,
        enable_tls: bool,
        timeout: u64,
        version: u8,
    ) -> PyResult<Self> {
        Ok(KolaConnector::new(
            host, port, user, password, enable_tls, timeout, version,
        ))
    }

    pub fn connect(&mut self, py: Python) -> Result<(), PyKolaError> {
        py.allow_threads(|| match self.q.connect() {
            Ok(_) => Ok(()),
            Err(e) => Err(PyKolaError::from(e)),
        })
    }

    pub fn shutdown(&mut self, py: Python) -> Result<(), PyKolaError> {
        py.allow_threads(|| match self.q.shutdown() {
            Ok(_) => Ok(()),
            Err(e) => Err(PyKolaError::from(e)),
        })
    }

    #[pyo3(signature = (expr, *args))]
    pub fn sync(&mut self, py: Python, expr: &str, args: Bound<PyTuple>) -> PyResult<PyObject> {
        self.execute(py, expr, args)
    }

    #[pyo3(signature = (expr, *args))]
    pub fn asyn(
        &mut self,
        py: Python,
        expr: &str,
        args: Bound<PyTuple>,
    ) -> Result<(), PyKolaError> {
        self.execute_async(py, expr, args)
    }

    pub fn receive(&mut self, py: Python) -> PyResult<PyObject> {
        let k = py.allow_threads(move || self.q.receive().map_err(|e| PyKolaError::from(e)));
        cast_k_to_py(py, k?)
    }
}

fn cast_to_k_vec(tuple: Bound<PyTuple>) -> Result<Vec<J>, PyKolaError> {
    let mut vec: Vec<J> = Vec::with_capacity(tuple.len());
    for obj in tuple.into_iter() {
        vec.push(cast_to_k(obj).map_err(|e| PythonErr(e.to_string()))?)
    }
    Ok(vec)
}

fn cast_to_k(any: Bound<PyAny>) -> PyResult<J> {
    if any.is_instance_of::<PyBool>() {
        Ok(J::Boolean(any.extract::<bool>()?))
        // TODO: this heap allocs on failure
    } else if any.is_instance_of::<PyInt>() {
        match any.extract::<i64>() {
            Ok(v) => Ok(J::I64(v)),
            Err(e) => Err(e),
        }
    } else if any.is_instance_of::<PyFloat>() {
        Ok(J::F64(any.extract::<f64>()?))
    } else if any.is_instance_of::<PyString>() {
        let value = any.extract::<&str>()?;
        Ok(J::Symbol(value.to_string()))
    } else if any.is_instance_of::<PyBytes>() {
        let value = any.downcast::<PyBytes>()?;
        Ok(J::String(String::from_utf8(value.as_bytes().to_vec())?))
    } else if any.hasattr(intern!(any.py(), "_s"))? {
        let series = any.extract::<PySeries>()?.into();
        Ok(J::Series(series))
    } else if any.hasattr(intern!(any.py(), "_df"))? {
        let df = any.extract::<PyDataFrame>()?.into();
        Ok(J::DataFrame(df))
    } else if any.is_none() {
        Ok(J::Null)
    } else if any.is_instance_of::<PyDateTime>() {
        let py_datetime = any.downcast::<PyDateTime>()?;
        Ok(J::DateTime(py_datetime.extract()?))
    } else if any.is_instance_of::<PyDate>() {
        let py_date = any.downcast::<PyDate>()?;
        Ok(J::Date(py_date.extract()?))
    } else if any.is_instance_of::<PyTime>() {
        let py_time = any.downcast::<PyTime>()?;
        Ok(J::Time(py_time.extract()?))
    } else if any.is_instance_of::<PyDelta>() {
        let py_delta = any.downcast::<PyDelta>()?;
        Ok(J::Duration(py_delta.extract()?))
    } else if any.is_instance_of::<PyDict>() {
        let py_dict = any.downcast::<PyDict>()?;
        let mut dict = IndexMap::with_capacity(py_dict.len());
        for (k, v) in py_dict.into_iter() {
            let k = match k.extract::<&str>() {
                Ok(s) => s.to_string(),
                Err(_) => {
                    return Err(
                        PythonErr(format!("Requires str as key, got {:?}", k.get_type())).into(),
                    )
                }
            };
            let v = cast_to_k(v)?;
            dict.insert(k, v);
        }
        Ok(J::Dict(dict))
    } else if any.is_instance_of::<PyList>() {
        let py_list = any.downcast::<PyList>()?;
        let mut k_list = Vec::with_capacity(py_list.len());
        for py_any in py_list {
            k_list.push(cast_to_k(py_any)?);
        }
        Ok(J::MixedList(k_list))
    } else {
        Err(PythonErr(format!("Not supported python type {:?}", any.get_type(),)).into())
    }
}

#[pyfunction]
pub fn read_j6_binary_table(filepath: &str) -> PyResult<PyDataFrame> {
    kola::io::read_j6_binary_table(filepath)
        .map_err(|e| PyKolaError::from(e).into())
        .map(|df| PyDataFrame(df))
}

#[pyfunction]
pub fn generate_j6_ipc_msg<'a>(
    py: Python<'a>,
    msg_type: u8,
    enable_compression: bool,
    any: Bound<PyAny>,
) -> PyResult<Bound<'a, PyBytes>> {
    let msg_type = if msg_type == 0 {
        MsgType::Async
    } else if msg_type == 1 {
        MsgType::Sync
    } else {
        MsgType::Response
    };
    match kola::io::generate_j6_ipc_msg(msg_type, enable_compression, cast_to_k(any)?) {
        Ok(bytes) => Ok(PyBytes::new(py, &bytes)),
        Err(e) => Err(PyKolaError::from(e).into()),
    }
}
