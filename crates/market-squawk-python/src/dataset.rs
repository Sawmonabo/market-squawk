//! Catalog-backed Task 11 dataset admission and opaque selection receipts.

use std::sync::Arc;

use market_squawk_data::{
    PythonDatasetRow, PythonDatasetSelection, PythonDatasetValue, PythonDatasetVerificationLimits,
    Sha256Digest, verify_python_dataset,
};
use market_squawk_domain::Timestamp;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyFloat, PyInt, PyMapping, PyString, PyTuple};
use uuid::Uuid;

use super::{CONTROL_CHECK_INTERVAL, OperationContext, encode_hex, invalid_input};

#[pyclass(frozen, module = "market_squawk._native", skip_from_py_object)]
#[derive(Clone, Debug)]
struct DatasetReceipt {
    selection: Arc<PythonDatasetSelection>,
    catalog_root: Arc<str>,
}

#[pymethods]
impl DatasetReceipt {
    /// Rehashes exact Python rows and descriptor bytes against the catalog-derived receipt.
    fn verify(
        &self,
        descriptor: &Bound<'_, PyBytes>,
        rows: &Bound<'_, PyTuple>,
        context: &OperationContext,
    ) -> PyResult<()> {
        context.check()?;
        if descriptor.as_bytes() != self.selection.descriptor()
            || rows.len() != self.selection.selected_rows()
        {
            return Err(invalid_input());
        }
        let work = u64::try_from(rows.len())
            .map_err(|_| invalid_input())?
            .max(1)
            .checked_mul(64)
            .ok_or_else(invalid_input)?;
        context.admit(work)?;
        let mut revalidation = self.selection.revalidation();
        for (index, item) in rows.iter().enumerate() {
            if index % CONTROL_CHECK_INTERVAL == 0 {
                context.check()?;
            }
            let row = python_dataset_row(&item)?;
            revalidation.update(&row).map_err(|_| invalid_input())?;
        }
        revalidation.finish().map_err(|_| invalid_input())?;
        context.check()
    }

    #[getter]
    fn catalog_root(&self) -> &str {
        &self.catalog_root
    }

    #[getter]
    fn export_sha256(&self) -> String {
        encode_hex(self.selection.export_sha256().bytes())
    }

    #[getter]
    fn catalog_identity(&self) -> String {
        encode_hex(self.selection.catalog_identity().bytes())
    }

    #[getter]
    fn selection_sha256(&self) -> String {
        encode_hex(self.selection.selection_sha256().bytes())
    }

    #[getter]
    fn as_of_unix_nanos(&self) -> i64 {
        self.selection.as_of().unix_nanos()
    }
}

#[pyfunction]
fn open_dataset_admission(
    py: Python<'_>,
    local_root: &str,
    export_sha256: &str,
    as_of_unix_nanos: i64,
    max_rows: usize,
    max_bytes: usize,
    context: &OperationContext,
) -> PyResult<(DatasetReceipt, Py<PyBytes>)> {
    context.check()?;
    let export_sha256 = Sha256Digest::new(decode_hex(export_sha256)?);
    let limits = PythonDatasetVerificationLimits::try_new(max_rows, max_bytes)
        .map_err(|_| invalid_input())?;
    let operator_root = std::path::PathBuf::from(local_root);
    context.admit(
        u64::try_from(max_rows)
            .map_err(|_| invalid_input())?
            .max(1)
            .checked_mul(64)
            .ok_or_else(invalid_input)?,
    )?;
    let deadline = context.deadline();
    let cancellation = context.cancellation();
    let selection = py.detach(move || {
        verify_python_dataset(
            &operator_root,
            export_sha256,
            Timestamp::from_unix_nanos(as_of_unix_nanos),
            limits,
            deadline,
            &cancellation,
        )
        .map_err(|_| invalid_input())
    })?;
    context.check()?;
    let root_text = selection
        .local_root()
        .to_str()
        .ok_or_else(invalid_input)?
        .to_owned();
    let descriptor = PyBytes::new(py, selection.descriptor()).unbind();
    Ok((
        DatasetReceipt {
            selection: Arc::new(selection),
            catalog_root: root_text.into(),
        },
        descriptor,
    ))
}

fn python_dataset_row(value: &Bound<'_, PyAny>) -> PyResult<PythonDatasetRow> {
    if !exact_python_type(value, "builtins", "mappingproxy")? {
        return Err(invalid_input());
    }
    let mapping = value.cast::<PyMapping>().map_err(|_| invalid_input())?;
    if mapping.len().map_err(|_| invalid_input())? != 14 {
        return Err(invalid_input());
    }
    let example_id = exact_string(
        &mapping
            .get_item("example_id")
            .map_err(|_| invalid_input())?,
    )?;
    let instrument_text = exact_string(
        &mapping
            .get_item("instrument_id")
            .map_err(|_| invalid_input())?,
    )?;
    let instrument = Uuid::parse_str(&instrument_text).map_err(|_| invalid_input())?;
    if instrument.to_string() != instrument_text {
        return Err(invalid_input());
    }
    let cutoff = mapping.get_item("cutoff_at").map_err(|_| invalid_input())?;
    if !exact_python_type(&cutoff, "market_squawk.data", "UtcNanoseconds")? {
        return Err(invalid_input());
    }
    let cutoff_at = exact_i64(&cutoff.getattr("unix_nanos").map_err(|_| invalid_input())?)?;
    let split =
        match exact_string(&mapping.get_item("split").map_err(|_| invalid_input())?)?.as_str() {
            "train" => 1,
            "validation" => 2,
            "test" => 3,
            _ => return Err(invalid_input()),
        };
    let component_kind = match exact_string(
        &mapping
            .get_item("component_kind")
            .map_err(|_| invalid_input())?,
    )?
    .as_str()
    {
        "feature" => 1,
        "label" => 2,
        _ => return Err(invalid_input()),
    };
    let component_name = exact_string(
        &mapping
            .get_item("component_name")
            .map_err(|_| invalid_input())?,
    )?;
    let component_version = exact_u32(
        &mapping
            .get_item("component_version")
            .map_err(|_| invalid_input())?,
    )?;
    let float = mapping.get_item("value_f64").map_err(|_| invalid_input())?;
    let decimal = mapping
        .get_item("value_decimal_mantissa")
        .map_err(|_| invalid_input())?;
    let scale = mapping
        .get_item("value_decimal_scale")
        .map_err(|_| invalid_input())?;
    let missing = mapping
        .get_item("missing_reason")
        .map_err(|_| invalid_input())?;
    let value = match (!float.is_none(), !decimal.is_none(), !missing.is_none()) {
        (true, false, false) => {
            let value = float.cast_exact::<PyFloat>().map_err(|_| invalid_input())?;
            PythonDatasetValue::Float(value.extract::<f64>().map_err(|_| invalid_input())?)
        }
        (false, true, false) => {
            if !exact_python_type(&decimal, "decimal", "Decimal")? {
                return Err(invalid_input());
            }
            let text = decimal
                .str()
                .and_then(|value| value.to_str().map(str::to_owned))
                .map_err(|_| invalid_input())?;
            if text.is_empty()
                || text.len() > 40
                || text
                    .bytes()
                    .enumerate()
                    .any(|(index, byte)| !byte.is_ascii_digit() && !(index == 0 && byte == b'-'))
            {
                return Err(invalid_input());
            }
            PythonDatasetValue::Decimal {
                mantissa: text.parse::<i128>().map_err(|_| invalid_input())?,
                scale: exact_u8(&scale)?,
            }
        }
        (false, false, true) => {
            if !scale.is_none() {
                return Err(invalid_input());
            }
            PythonDatasetValue::Missing(exact_string(&missing)?.into())
        }
        _ => return Err(invalid_input()),
    };
    if matches!(&value, PythonDatasetValue::Float(_)) && !scale.is_none() {
        return Err(invalid_input());
    }
    let unit = optional_exact_string(&mapping.get_item("unit").map_err(|_| invalid_input())?)?;
    let currency =
        optional_exact_string(&mapping.get_item("currency").map_err(|_| invalid_input())?)?;
    let lineage_value = mapping
        .get_item("lineage_sha256")
        .map_err(|_| invalid_input())?;
    let lineage: [u8; 32] = lineage_value
        .cast_exact::<PyBytes>()
        .map_err(|_| invalid_input())?
        .as_bytes()
        .try_into()
        .map_err(|_| invalid_input())?;
    PythonDatasetRow::try_new(
        &example_id,
        instrument.into_bytes(),
        Timestamp::from_unix_nanos(cutoff_at),
        split,
        component_kind,
        &component_name,
        component_version,
        value,
        unit.as_deref(),
        currency.as_deref(),
        lineage,
    )
    .map_err(|_| invalid_input())
}

fn exact_python_type(value: &Bound<'_, PyAny>, module: &str, name: &str) -> PyResult<bool> {
    let value_type = value.get_type();
    Ok(value_type
        .module()?
        .to_str()
        .is_ok_and(|value| value == module)
        && value_type.name()?.to_str().is_ok_and(|value| value == name))
}

fn exact_string(value: &Bound<'_, PyAny>) -> PyResult<String> {
    let value = value
        .cast_exact::<PyString>()
        .map_err(|_| invalid_input())?;
    value
        .to_str()
        .map(str::to_owned)
        .map_err(|_| invalid_input())
}

fn optional_exact_string(value: &Bound<'_, PyAny>) -> PyResult<Option<String>> {
    if value.is_none() {
        Ok(None)
    } else {
        exact_string(value).map(Some)
    }
}

fn exact_i64(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    let value = value.cast_exact::<PyInt>().map_err(|_| invalid_input())?;
    value.extract::<i64>().map_err(|_| invalid_input())
}

fn exact_u32(value: &Bound<'_, PyAny>) -> PyResult<u32> {
    let value = value.cast_exact::<PyInt>().map_err(|_| invalid_input())?;
    value.extract::<u32>().map_err(|_| invalid_input())
}

fn exact_u8(value: &Bound<'_, PyAny>) -> PyResult<u8> {
    let value = value.cast_exact::<PyInt>().map_err(|_| invalid_input())?;
    value.extract::<u8>().map_err(|_| invalid_input())
}

fn decode_hex(value: &str) -> PyResult<[u8; 32]> {
    if value.len() != 64
        || value
            .bytes()
            .any(|byte| !byte.is_ascii_hexdigit() || byte.is_ascii_uppercase())
    {
        return Err(invalid_input());
    }
    let mut bytes = [0_u8; 32];
    for (target, pair) in bytes.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
        let high = decode_nibble(pair[0]).ok_or_else(invalid_input)?;
        let low = decode_nibble(pair[1]).ok_or_else(invalid_input)?;
        *target = (high << 4) | low;
    }
    if bytes == [0; 32] {
        return Err(invalid_input());
    }
    Ok(bytes)
}

fn decode_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

pub(super) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<DatasetReceipt>()?;
    module.add_function(wrap_pyfunction!(open_dataset_admission, module)?)?;
    Ok(())
}
