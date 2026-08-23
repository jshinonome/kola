use crate::error::PyKolaError;
use chrono::{Datelike, Timelike};
use indexmap::IndexMap;
use kola::connector::Connector;
use kola::types::{MsgType, QLambda, QOperator, K, MIN_Q_TIMESTAMP_UNIX_NANOS};
use pyo3::exceptions::{PyOverflowError, PyTypeError, PyValueError};
use pyo3::types::{
    PyBool, PyBytes, PyDate, PyDateTime, PyDelta, PyDict, PyFloat, PyInt, PyList, PyString, PyTime,
    PyTuple, PyTzInfo,
};
use pyo3::{intern, prelude::*, IntoPyObjectExt};
use pyo3_polars::{PyDataFrame, PySeries};
use std::collections::HashSet;

#[pyclass(frozen, eq, module = "kola", skip_from_py_object)]
#[derive(Clone, Eq, PartialEq)]
pub struct KolaQOperator {
    operator: QOperator,
}

#[pymethods]
impl KolaQOperator {
    #[new]
    fn new(name: &str) -> Result<Self, PyKolaError> {
        Ok(Self {
            operator: QOperator::new(name)?,
        })
    }

    #[classattr]
    #[pyo3(name = "PLUS")]
    fn plus() -> Self {
        Self {
            operator: QOperator::PLUS,
        }
    }

    #[getter]
    fn name(&self) -> &str {
        self.operator.name()
    }

    fn __repr__(&self) -> String {
        format!("KolaQOperator({:?})", self.operator.name())
    }
}

#[pyclass(frozen, eq, module = "kola", skip_from_py_object)]
#[derive(Clone, Eq, PartialEq)]
pub struct KolaQLambda {
    lambda: QLambda,
}

#[pymethods]
impl KolaQLambda {
    #[new]
    #[pyo3(signature = (source, context = ""))]
    fn new(source: &str, context: &str) -> Result<Self, PyKolaError> {
        Ok(Self {
            lambda: QLambda::with_context(source, context)?,
        })
    }

    #[getter]
    fn source(&self) -> &str {
        self.lambda.source()
    }

    #[getter]
    fn context(&self) -> &str {
        self.lambda.context()
    }

    fn __repr__(&self) -> String {
        if self.lambda.context().is_empty() {
            format!("KolaQLambda({:?})", self.lambda.source())
        } else {
            format!(
                "KolaQLambda({:?}, {:?})",
                self.lambda.source(),
                self.lambda.context()
            )
        }
    }
}

#[pyclass]
pub struct KolaConnector {
    q: Connector,
}

const MAX_CONVERSION_DEPTH: usize = 64;
const MAX_CALL_ARGUMENTS: usize = 8;
const MICROSECONDS_PER_SECOND: i64 = 1_000_000;
const MICROSECONDS_PER_DAY: i64 = 86_400 * MICROSECONDS_PER_SECOND;

impl KolaConnector {
    fn execute(&mut self, py: Python, expr: &str, args: Bound<PyTuple>) -> PyResult<Py<PyAny>> {
        let args = cast_to_k_vec(args)?;
        let k = py
            .detach(move || self.q.execute(expr, &args))
            .map_err(PyKolaError::from)?;
        cast_k_to_py(py, k)
    }

    fn execute_async(
        &mut self,
        py: Python,
        expr: &str,
        args: Bound<PyTuple>,
    ) -> Result<(), PyKolaError> {
        let args = cast_to_k_vec(args)?;
        py.detach(move || self.q.execute_async(expr, &args))
            .map_err(PyKolaError::from)
    }
}

fn python_date_parts(value: &impl Datelike) -> PyResult<(i32, u8, u8)> {
    let year = value.year();
    if !(1..=9999).contains(&year) {
        return Err(PyOverflowError::new_err(format!(
            "year {year} is outside Python's supported range"
        )));
    }
    Ok((year, value.month() as u8, value.day() as u8))
}

fn python_microseconds(nanoseconds: u32, type_name: &str) -> PyResult<u32> {
    if !nanoseconds.is_multiple_of(1_000) {
        return Err(PyValueError::new_err(format!(
            "{type_name} has sub-microsecond precision that Python cannot represent"
        )));
    }
    Ok(nanoseconds / 1_000)
}

fn cast_k_to_py(py: Python, k: K) -> PyResult<Py<PyAny>> {
    cast_k_to_py_inner(py, k, 0)
}

fn cast_k_to_py_inner(py: Python, k: K, depth: usize) -> PyResult<Py<PyAny>> {
    if depth > MAX_CONVERSION_DEPTH {
        return Err(PyValueError::new_err(format!(
            "q value nesting exceeds {MAX_CONVERSION_DEPTH} levels"
        )));
    }

    match k {
        K::Boolean(k) => k.into_py_any(py),
        K::Guid(k) => k.to_string().into_py_any(py),
        K::U8(k) => k.into_py_any(py),
        K::I16(k) => k.into_py_any(py),
        K::I32(k) => k.into_py_any(py),
        K::I64(k) => k.into_py_any(py),
        K::F32(k) => k.into_py_any(py),
        K::F64(k) => k.into_py_any(py),
        K::Char(k) => (k as char).into_py_any(py),
        K::CharVector(k) => match std::str::from_utf8(&k) {
            Ok(text) => text.into_py_any(py),
            Err(_) => PyBytes::new(py, &k).into_py_any(py),
        },
        K::Symbol(k) => k.into_py_any(py),
        K::String(k) => k.into_py_any(py),
        K::DateTime(k) => {
            let (year, month, day) = python_date_parts(&k)?;
            let microsecond = python_microseconds(k.nanosecond(), "q timestamp")?;
            let timezone = PyTzInfo::utc(py)?;
            PyDateTime::new(
                py,
                year,
                month,
                day,
                k.hour() as u8,
                k.minute() as u8,
                k.second() as u8,
                microsecond,
                Some(&timezone),
            )?
            .into_py_any(py)
        }
        K::Date(k) => {
            let (year, month, day) = python_date_parts(&k)?;
            PyDate::new(py, year, month, day)?.into_py_any(py)
        }
        K::Time(k) => {
            let microsecond = python_microseconds(k.nanosecond(), "q time")?;
            PyTime::new(
                py,
                k.hour() as u8,
                k.minute() as u8,
                k.second() as u8,
                microsecond,
                None,
            )?
            .into_py_any(py)
        }
        K::Duration(k) => {
            let nanoseconds = k.num_nanoseconds().ok_or_else(|| {
                PyOverflowError::new_err("q timespan is outside Python's supported range")
            })?;
            if nanoseconds % 1_000 != 0 {
                return Err(PyValueError::new_err(
                    "q timespan has sub-microsecond precision that Python cannot represent",
                ));
            }
            let microseconds = nanoseconds / 1_000;
            let days = microseconds.div_euclid(MICROSECONDS_PER_DAY);
            let day_microseconds = microseconds.rem_euclid(MICROSECONDS_PER_DAY);
            let seconds = day_microseconds / MICROSECONDS_PER_SECOND;
            let remaining_microseconds = day_microseconds % MICROSECONDS_PER_SECOND;
            let days = i32::try_from(days).map_err(|_| {
                PyOverflowError::new_err("q timespan is outside Python's supported range")
            })?;
            PyDelta::new(
                py,
                days,
                seconds as i32,
                remaining_microseconds as i32,
                false,
            )?
            .into_py_any(py)
        }
        K::MixedList(values) => {
            let py_objects = values
                .into_iter()
                .map(|value| cast_k_to_py_inner(py, value, depth + 1))
                .collect::<PyResult<Vec<_>>>()?;
            PyTuple::new(py, py_objects)?.into_py_any(py)
        }
        K::Series(k) => PySeries(k).into_py_any(py),
        K::DataFrame(k) => PyDataFrame(k).into_py_any(py),
        K::Operator(operator) => Ok(Py::new(py, KolaQOperator { operator })?.into_any()),
        K::Lambda(lambda) => Ok(Py::new(py, KolaQLambda { lambda })?.into_any()),
        K::Null => ().into_py_any(py),
        K::Dict(dict) => {
            let py_dict = PyDict::new(py);
            for (key, value) in dict {
                py_dict.set_item(key, cast_k_to_py_inner(py, value, depth + 1)?)?;
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
        Ok(Self {
            q: Connector::new(host, port, user, password, enable_tls, timeout, version),
        })
    }

    pub fn connect(&mut self, py: Python) -> Result<(), PyKolaError> {
        py.detach(|| self.q.connect().map_err(PyKolaError::from))
    }

    pub fn shutdown(&mut self, py: Python) -> Result<(), PyKolaError> {
        py.detach(|| self.q.shutdown().map_err(PyKolaError::from))
    }

    #[pyo3(signature = (expr, *args))]
    pub fn sync(&mut self, py: Python, expr: &str, args: Bound<PyTuple>) -> PyResult<Py<PyAny>> {
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

    pub fn receive(&mut self, py: Python) -> PyResult<Py<PyAny>> {
        let k = py.detach(move || self.q.receive().map_err(PyKolaError::from))?;
        cast_k_to_py(py, k)
    }
}

fn cast_to_k_vec(tuple: Bound<PyTuple>) -> Result<Vec<K>, PyKolaError> {
    if tuple.len() > MAX_CALL_ARGUMENTS {
        return Err(PyTypeError::new_err(format!(
            "q functions accept at most {MAX_CALL_ARGUMENTS} arguments"
        ))
        .into());
    }

    let mut active_containers = HashSet::new();
    tuple
        .into_iter()
        .map(|value| cast_to_k_inner(value, 0, &mut active_containers))
        .collect::<PyResult<Vec<_>>>()
        .map_err(PyKolaError::from)
}

fn cast_to_k(any: Bound<PyAny>) -> PyResult<K> {
    cast_to_k_inner(any, 0, &mut HashSet::new())
}

fn cast_to_k_inner(
    any: Bound<PyAny>,
    depth: usize,
    active_containers: &mut HashSet<usize>,
) -> PyResult<K> {
    if depth > MAX_CONVERSION_DEPTH {
        return Err(PyValueError::new_err(format!(
            "Python value nesting exceeds {MAX_CONVERSION_DEPTH} levels"
        )));
    }

    if any.is_instance_of::<KolaQOperator>() {
        let value = any.extract::<PyRef<KolaQOperator>>()?;
        Ok(K::Operator(value.operator))
    } else if any.is_instance_of::<KolaQLambda>() {
        let value = any.extract::<PyRef<KolaQLambda>>()?;
        Ok(K::Lambda(value.lambda.clone()))
    } else if any.is_instance_of::<PyBool>() {
        Ok(K::Boolean(any.extract()?))
    } else if any.is_instance_of::<PyInt>() {
        Ok(K::I64(any.extract()?))
    } else if any.is_instance_of::<PyFloat>() {
        Ok(K::F64(any.extract()?))
    } else if any.is_instance_of::<PyString>() {
        Ok(K::Symbol(any.extract::<&str>()?.to_owned()))
    } else if any.is_instance_of::<PyBytes>() {
        let value = any.cast::<PyBytes>()?;
        Ok(K::CharVector(value.as_bytes().to_vec()))
    } else if any.hasattr(intern!(any.py(), "_s"))? {
        Ok(K::Series(any.extract::<PySeries>()?.into()))
    } else if any.hasattr(intern!(any.py(), "_df"))? {
        Ok(K::DataFrame(any.extract::<PyDataFrame>()?.into()))
    } else if any.is_none() {
        Ok(K::Null)
    } else if any.is_instance_of::<PyDateTime>() {
        let value: chrono::DateTime<chrono::Utc> = any.cast::<PyDateTime>()?.extract()?;
        let nanoseconds = value.timestamp_nanos_opt().ok_or_else(|| {
            PyOverflowError::new_err("datetime is outside q's representable timestamp range")
        })?;
        if nanoseconds < MIN_Q_TIMESTAMP_UNIX_NANOS {
            return Err(PyOverflowError::new_err(
                "datetime is outside q's representable timestamp range",
            ));
        }
        Ok(K::DateTime(value))
    } else if any.is_instance_of::<PyDate>() {
        let value: chrono::NaiveDate = any.cast::<PyDate>()?.extract()?;
        Ok(K::Date(value))
    } else if any.is_instance_of::<PyTime>() {
        let value: chrono::NaiveTime = any.cast::<PyTime>()?.extract()?;
        if !value.nanosecond().is_multiple_of(1_000_000) {
            return Err(PyValueError::new_err(
                "q time only supports millisecond precision",
            ));
        }
        Ok(K::Time(value))
    } else if any.is_instance_of::<PyDelta>() {
        let value: chrono::Duration = any.cast::<PyDelta>()?.extract()?;
        if value.num_nanoseconds().is_none() {
            return Err(PyOverflowError::new_err(
                "timedelta is outside q's representable timespan range",
            ));
        }
        Ok(K::Duration(value))
    } else if any.is_instance_of::<PyDict>() {
        let identity = any.as_ptr() as usize;
        if !active_containers.insert(identity) {
            return Err(PyValueError::new_err(
                "cyclic Python containers cannot be converted to q",
            ));
        }
        let result = (|| {
            let py_dict = any.cast::<PyDict>()?;
            let mut dict = IndexMap::with_capacity(py_dict.len());
            for (key, value) in py_dict {
                let key = key.extract::<&str>()?.to_owned();
                dict.insert(key, cast_to_k_inner(value, depth + 1, active_containers)?);
            }
            Ok(K::Dict(dict))
        })();
        active_containers.remove(&identity);
        result
    } else if any.is_instance_of::<PyList>() {
        let identity = any.as_ptr() as usize;
        if !active_containers.insert(identity) {
            return Err(PyValueError::new_err(
                "cyclic Python containers cannot be converted to q",
            ));
        }
        let result = (|| {
            let py_list = any.cast::<PyList>()?;
            let mut values = Vec::with_capacity(py_list.len());
            for value in py_list {
                values.push(cast_to_k_inner(value, depth + 1, active_containers)?);
            }
            Ok(K::MixedList(values))
        })();
        active_containers.remove(&identity);
        result
    } else {
        Err(PyTypeError::new_err(format!(
            "unsupported Python type {:?}",
            any.get_type()
        )))
    }
}

#[pyfunction]
pub fn read_j6_binary_table(py: Python, filepath: &str) -> PyResult<PyDataFrame> {
    let filepath = filepath.to_owned();
    let frame = py
        .detach(move || kola::io::read_j6_binary_table(&filepath))
        .map_err(PyKolaError::from)?;
    Ok(PyDataFrame(frame))
}

#[pyfunction]
pub fn generate_j6_ipc_msg<'a>(
    py: Python<'a>,
    msg_type: u8,
    enable_compression: bool,
    any: Bound<PyAny>,
) -> PyResult<Bound<'a, PyBytes>> {
    let msg_type = match msg_type {
        0 => MsgType::Async,
        1 => MsgType::Sync,
        2 => MsgType::Response,
        value => {
            return Err(PyValueError::new_err(format!(
                "msg_type must be 0, 1, or 2; got {value}"
            )))
        }
    };
    let value = cast_to_k(any)?;
    let bytes = py
        .detach(move || kola::io::generate_j6_ipc_msg(msg_type, enable_compression, value))
        .map_err(PyKolaError::from)?;
    Ok(PyBytes::new(py, &bytes))
}
