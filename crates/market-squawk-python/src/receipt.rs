//! Verified installed training-environment authority exposed as an opaque Python value.

use std::path::Path;

use market_squawk_modeling::verify_python_training_environment;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use super::encode_hex;

/// Opaque builder-authored identity for the exact native training environment.
#[pyclass(frozen, module = "market_squawk._native", skip_from_py_object)]
#[derive(Clone, Debug)]
pub(crate) struct TrainingEnvironmentReceipt {
    sha256: String,
    training_code_revision: String,
}

#[pymethods]
impl TrainingEnvironmentReceipt {
    #[getter]
    fn sha256(&self) -> &str {
        &self.sha256
    }

    #[getter]
    fn training_code_revision(&self) -> &str {
        &self.training_code_revision
    }
}

#[pyfunction]
fn training_environment_receipt(py: Python<'_>) -> PyResult<TrainingEnvironmentReceipt> {
    let sys = py.import("sys").map_err(|_| invalid_receipt())?;
    let root: String = sys
        .getattr("prefix")
        .and_then(|value| value.extract())
        .map_err(|_| invalid_receipt())?;
    let executable: String = sys
        .getattr("executable")
        .and_then(|value| value.extract())
        .map_err(|_| invalid_receipt())?;
    let version = sys.getattr("version_info").map_err(|_| invalid_receipt())?;
    let major: u8 = version
        .getattr("major")
        .and_then(|value| value.extract())
        .map_err(|_| invalid_receipt())?;
    let minor: u8 = version
        .getattr("minor")
        .and_then(|value| value.extract())
        .map_err(|_| invalid_receipt())?;
    let micro: u8 = version
        .getattr("micro")
        .and_then(|value| value.extract())
        .map_err(|_| invalid_receipt())?;
    let implementation: String = sys
        .getattr("implementation")
        .and_then(|value| value.getattr("name"))
        .and_then(|value| value.extract())
        .map_err(|_| invalid_receipt())?;
    let module = py
        .import("market_squawk._native")
        .map_err(|_| invalid_receipt())?;
    let native_extension: String = module
        .getattr("__file__")
        .and_then(|value| value.extract())
        .map_err(|_| invalid_receipt())?;
    let version = format!("{major}.{minor}.{micro}");
    let python_tag = format!("cp{major}{minor}");
    let verified = verify_python_training_environment(
        Path::new(&root),
        Path::new(&executable),
        &implementation,
        &version,
        &python_tag,
        Path::new(&native_extension),
    )
    .map_err(|_| invalid_receipt())?;
    Ok(TrainingEnvironmentReceipt {
        sha256: encode_hex(verified.receipt_sha256()),
        training_code_revision: verified.training_code_revision().into(),
    })
}

fn invalid_receipt() -> PyErr {
    PyValueError::new_err("training environment receipt is absent or invalid")
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<TrainingEnvironmentReceipt>()?;
    module.add_function(wrap_pyfunction!(training_environment_receipt, module)?)
}
