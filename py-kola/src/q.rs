use std::cmp::{max, min};

use crate::error::PyKolaError::{self, PythonError};
use chrono::{Datelike, Timelike};
use indexmap::IndexMap;
use kola::q::Q;
use kola::types::{MsgType, K};
use pyo3::types::{
    timezone_utc_bound, PyBool, PyBytes, PyDate, PyDateTime, PyDelta, PyDict, PyFloat, PyInt,
    PyList, PyString, PyTime, PyTuple,
};
use pyo3::{intern, prelude::*};
use pyo3_polars::{PyDataFrame, PySeries};

#[pyclass]
pub struct QConnector {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub enable_tls: bool,
    q: Q,
}

impl QConnector {
    pub(crate) fn new(
        host: &str,
        port: u16,
        user: &str,
        password: &str,
        enable_tls: bool,
        timeout: u64,
        version: u8,
    ) -> Self {
        QConnector {
            host: host.to_string(),
            port,
            user: user.to_string(),
            password: password.to_string(),
            enable_tls,
            q: Q::new(host, port, user, password, enable_tls, timeout, version),
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

fn cast_k_to_py(py: Python, k: K) -> PyResult<PyObject> {
    match k {
        K::Bool(k) => Ok(k.to_object(py)),
        K::Guid(k) => Ok(k.to_string().to_object(py)),
        K::Byte(k) => Ok(k.to_object(py)),
        K::Short(k) => Ok(k.to_object(py)),
        K::Int(k) => Ok(k.to_object(py)),
        K::Long(k) => Ok(k.to_object(py)),
        K::Real(k) => Ok(k.to_object(py)),
        K::Float(k) => Ok(k.to_object(py)),
        K::Char(k) => Ok((k as char).to_object(py)),
        K::Symbol(k) => Ok(k.to_object(py)),
        K::String(k) => Ok(k.to_object(py)),
        K::DateTime(k) => {
            if let Some(ns) = k.timestamp_nanos_opt() {
                let datetime = PyDateTime::from_timestamp_bound(
                    py,
                    ns as f64 / 1000000000.0,
                    Some(&timezone_utc_bound(py)),
                )?;
                Ok(datetime.to_object(py))
            } else {
                Err(PythonError("failed to get nanoseconds".to_string()).into())
            }
        }
        K::Date(k) => {
            let mut days = k.num_days_from_ce() as i64 - 719163;
            days = min(days, 2932532);
            days = max(days, -719162);
            let date = PyDate::from_timestamp_bound(py, 86400 * days)?;
            Ok(date.to_object(py))
        }
        K::Time(k) => {
            let time = PyTime::new_bound(
                py,
                k.hour() as u8,
                k.minute() as u8,
                k.second() as u8,
                k.nanosecond() / 1000,
                None,
            )?;
            Ok(time.to_object(py))
        }
        K::Duration(k) => {
            let delta = PyDelta::new_bound(
                py,
                0,
                k.num_seconds() as i32,
                (k.num_microseconds().unwrap_or(0) % 1000000) as i32,
                false,
            )?;
            Ok(delta.to_object(py))
        }
        K::MixedList(l) => {
            let py_objects = l
                .into_iter()
                .map(|k| cast_k_to_py(py, k))
                .collect::<PyResult<Vec<PyObject>>>()?;
            Ok(PyTuple::new_bound(py, py_objects).into())
        }
        K::Series(k) => Ok(PySeries(k).into_py(py)),
        K::DataFrame(k) => Ok(PyDataFrame(k).into_py(py)),
        K::None(_) => Ok(().to_object(py)),
        K::Dict(dict) => {
            let py_dict = PyDict::new_bound(py);
            for (k, v) in dict.into_iter() {
                py_dict.set_item(k, cast_k_to_py(py, v)?)?;
            }
            Ok(py_dict.into())
        }
    }
}

#[pymethods]
impl QConnector {
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
        Ok(QConnector::new(
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

fn cast_to_k_vec(tuple: Bound<PyTuple>) -> Result<Vec<K>, PyKolaError> {
    let mut vec: Vec<K> = Vec::with_capacity(tuple.len());
    for obj in tuple.into_iter() {
        vec.push(cast_to_k(obj).map_err(|e| PythonError(e.to_string()))?)
    }
    Ok(vec)
}

fn cast_to_k(any: Bound<PyAny>) -> PyResult<K> {
    if any.is_instance_of::<PyBool>() {
        Ok(K::Bool(any.extract::<bool>()?))
        // TODO: this heap allocs on failure
    } else if any.is_instance_of::<PyInt>() {
        match any.extract::<i64>() {
            Ok(v) => Ok(K::Long(v)),
            Err(e) => Err(e),
        }
    } else if any.is_instance_of::<PyFloat>() {
        Ok(K::Float(any.extract::<f64>()?))
    } else if any.is_instance_of::<PyString>() {
        let value = any.extract::<&str>()?;
        Ok(K::Symbol(value.to_string()))
    } else if any.is_instance_of::<PyBytes>() {
        let value = any.downcast::<PyBytes>()?;
        Ok(K::String(String::from_utf8(value.as_bytes().to_vec())?))
    } else if any.hasattr(intern!(any.py(), "_s"))? {
        let series = any.extract::<PySeries>()?.into();
        Ok(K::Series(series))
    } else if any.hasattr(intern!(any.py(), "_df"))? {
        let df = any.extract::<PyDataFrame>()?.into();
        Ok(K::DataFrame(df))
    } else if any.is_none() {
        Ok(K::None(0))
    } else if any.is_instance_of::<PyDateTime>() {
        let py_datetime = any.downcast::<PyDateTime>()?;
        Ok(K::DateTime(py_datetime.extract()?))
    } else if any.is_instance_of::<PyDate>() {
        let py_date = any.downcast::<PyDate>()?;
        Ok(K::Date(py_date.extract()?))
    } else if any.is_instance_of::<PyTime>() {
        let py_time = any.downcast::<PyTime>()?;
        Ok(K::Time(py_time.extract()?))
    } else if any.is_instance_of::<PyDelta>() {
        let py_delta = any.downcast::<PyDelta>()?;
        Ok(K::Duration(py_delta.extract()?))
    } else if any.is_instance_of::<PyDict>() {
        let py_dict = any.downcast::<PyDict>()?;
        let mut dict = IndexMap::with_capacity(py_dict.len());
        for (k, v) in py_dict.into_iter() {
            let k = match k.extract::<&str>() {
                Ok(s) => s.to_string(),
                Err(_) => {
                    return Err(
                        PythonError(format!("Requires str as key, got {:?}", k.get_type())).into(),
                    )
                }
            };
            let v = cast_to_k(v)?;
            dict.insert(k, v);
        }
        Ok(K::Dict(dict))
    } else if any.is_instance_of::<PyList>() {
        let py_list = any.downcast::<PyList>()?;
        let mut k_list = Vec::with_capacity(py_list.len());
        for py_any in py_list {
            k_list.push(cast_to_k(py_any)?);
        }
        Ok(K::MixedList(k_list))
    } else {
        Err(PythonError(format!("Not supported python type {:?}", any.get_type(),)).into())
    }
}

#[pyfunction]
pub fn read_binary_table(filepath: &str) -> PyResult<PyDataFrame> {
    kola::io::read_binary_table(filepath)
        .map_err(|e| PyKolaError::from(e).into())
        .map(|df| PyDataFrame(df))
}

#[pyfunction]
pub fn generate_ipc_msg<'a>(
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
    match kola::io::generate_ipc_msg(msg_type, enable_compression, cast_to_k(any)?) {
        Ok(bytes) => Ok(PyBytes::new_bound(py, &bytes)),
        Err(e) => Err(PyKolaError::from(e).into()),
    }
}

#[pyfunction]
pub fn deserialize<'a>(py: Python<'a>, buf: &[u8]) -> PyResult<PyObject> {
    let k = kola::io::deserialize(buf).map_err(|e| PyKolaError::from(e))?;
    cast_k_to_py(py, k)
}
