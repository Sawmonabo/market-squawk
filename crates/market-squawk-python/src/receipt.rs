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
    application_sha256: String,
    onnx_worker_sha256: String,
    validator_sha256: String,
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

    #[getter]
    fn application_sha256(&self) -> &str {
        &self.application_sha256
    }

    #[getter]
    fn onnx_worker_sha256(&self) -> &str {
        &self.onnx_worker_sha256
    }

    #[getter]
    fn validator_sha256(&self) -> &str {
        &self.validator_sha256
    }
}

#[pyfunction]
fn training_environment_receipt(py: Python<'_>) -> PyResult<TrainingEnvironmentReceipt> {
    let module = py
        .import("market_squawk._native")
        .map_err(|_| invalid_receipt())?;
    let native_extension: String = module
        .filename()
        .and_then(|value| value.extract())
        .map_err(|_| invalid_receipt())?;
    verify_training_environment(py, Path::new(&native_extension))
}

pub(super) fn verify_at_import(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let native_extension: String = module
        .filename()
        .and_then(|value| value.extract())
        .map_err(|_| invalid_receipt())?;
    verify_training_environment(module.py(), Path::new(&native_extension)).map(drop)
}

fn verify_training_environment(
    py: Python<'_>,
    native_extension: &Path,
) -> PyResult<TrainingEnvironmentReceipt> {
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
    let version = format!("{major}.{minor}.{micro}");
    let python_tag = format!("cp{major}{minor}");
    let verified = verify_python_training_environment(
        Path::new(&root),
        Path::new(&executable),
        &implementation,
        &version,
        &python_tag,
        native_extension,
    )
    .map_err(|_| invalid_receipt())?;
    Ok(TrainingEnvironmentReceipt {
        sha256: encode_hex(verified.receipt_sha256()),
        training_code_revision: verified.training_code_revision().into(),
        application_sha256: encode_hex(verified.application_sha256()),
        onnx_worker_sha256: encode_hex(verified.onnx_worker_sha256()),
        validator_sha256: encode_hex(verified.validator_sha256()),
    })
}

fn invalid_receipt() -> PyErr {
    PyValueError::new_err("training environment receipt is absent or invalid")
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<TrainingEnvironmentReceipt>()?;
    module.add_function(wrap_pyfunction!(training_environment_receipt, module)?)
}
