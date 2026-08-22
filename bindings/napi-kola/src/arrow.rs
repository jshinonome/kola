use std::io::Cursor;

use polars::prelude::{DataFrame, IpcStreamReader, IpcStreamWriter, SerReader, SerWriter, Series};

use crate::error::BindingError;

pub(crate) fn dataframe_to_ipc(mut dataframe: DataFrame) -> Result<Vec<u8>, BindingError> {
    let mut bytes = Vec::new();
    IpcStreamWriter::new(&mut bytes)
        .finish(&mut dataframe)
        .map_err(|error| {
            BindingError::conversion(format!("failed to encode Arrow IPC stream: {error}"))
        })?;
    Ok(bytes)
}

pub(crate) fn series_to_ipc(series: Series) -> Result<Vec<u8>, BindingError> {
    let dataframe = DataFrame::new_infer_height(vec![series.into()]).map_err(|error| {
        BindingError::conversion(format!("failed to materialize series: {error}"))
    })?;
    dataframe_to_ipc(dataframe)
}

pub(crate) fn dataframe_from_ipc(bytes: Vec<u8>) -> Result<DataFrame, BindingError> {
    IpcStreamReader::new(Cursor::new(bytes))
        .finish()
        .map_err(|error| {
            BindingError::conversion(format!("invalid Arrow IPC table stream: {error}"))
        })
}

pub(crate) fn series_from_ipc(bytes: Vec<u8>) -> Result<Series, BindingError> {
    let dataframe = dataframe_from_ipc(bytes)?;
    let mut columns = dataframe.into_columns();
    if columns.len() != 1 {
        return Err(BindingError::conversion(format!(
            "Arrow IPC series stream must contain exactly one column, found {}",
            columns.len()
        )));
    }
    Ok(columns.remove(0).take_materialized_series())
}

#[cfg(test)]
mod tests {
    use polars::prelude::{DataFrame, NamedFrom, Series};

    use super::{dataframe_from_ipc, dataframe_to_ipc, series_from_ipc, series_to_ipc};

    #[test]
    fn round_trips_series_as_one_column_stream() {
        let series = Series::new("values".into(), [1i64, 2, 3]);
        let decoded = series_from_ipc(series_to_ipc(series.clone()).expect("encode series"))
            .expect("decode series");
        assert_eq!(decoded, series);
    }

    #[test]
    fn round_trips_dataframe_as_stream() {
        let dataframe = DataFrame::new_infer_height(vec![
            Series::new("id".into(), [1i64, 2]).into(),
            Series::new("price".into(), [10.5f64, 20.25]).into(),
        ])
        .expect("dataframe");
        let decoded =
            dataframe_from_ipc(dataframe_to_ipc(dataframe.clone()).expect("encode dataframe"))
                .expect("decode dataframe");
        assert_eq!(decoded, dataframe);
    }

    #[test]
    fn rejects_multi_column_series_stream() {
        let dataframe = DataFrame::new_infer_height(vec![
            Series::new("left".into(), [1i32]).into(),
            Series::new("right".into(), [2i32]).into(),
        ])
        .expect("dataframe");
        let error = series_from_ipc(dataframe_to_ipc(dataframe).expect("encode dataframe"))
            .expect_err("multiple columns must be rejected");
        assert_eq!(error.code, "KOLA_CONVERSION");
    }
}
