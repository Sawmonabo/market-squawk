//! Explicit source provenance or verified installed authority exposed as an opaque value.

use std::path::Path;

use market_squawk_modeling::{
    ConfiguredTrainingEnvironment, SourceDevelopmentEnvironment, verify_python_training_environment,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use super::{NATIVE_BUILD_REVISION, SEALED_PYTHON_BUILD, encode_hex};

/// Opaque provenance coordinates; source metadata does not attest software bytes.
#[pyclass(frozen, module = "market_squawk._native", skip_from_py_object)]
#[derive(Clone, Debug)]
pub(crate) struct TrainingEnvironmentReceipt {
    sha256: String,
    training_code_revision: String,
    origin: &'static str,
    source_development_root: Option<String>,
    validator_path: Option<String>,
    native_build_revision: Option<String>,
    application_sha256: Option<String>,
    onnx_worker_sha256: Option<String>,
    validator_sha256: Option<String>,
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
    fn origin(&self) -> &str {
        self.origin
    }

    #[getter]
    fn source_development_root(&self) -> Option<&str> {
        self.source_development_root.as_deref()
    }

    #[getter]
    fn validator_path(&self) -> Option<&str> {
        self.validator_path.as_deref()
    }

    #[getter]
    fn native_build_revision(&self) -> Option<&str> {
        self.native_build_revision.as_deref()
    }

    #[getter]
    fn application_sha256(&self) -> Option<&str> {
        self.application_sha256.as_deref()
    }

    #[getter]
    fn onnx_worker_sha256(&self) -> Option<&str> {
        self.onnx_worker_sha256.as_deref()
    }

    #[getter]
    fn validator_sha256(&self) -> Option<&str> {
        self.validator_sha256.as_deref()
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
    sys.setattr("dont_write_bytecode", true)
        .map_err(|_| invalid_receipt())?;
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
    if !SEALED_PYTHON_BUILD {
        let environment = ConfiguredTrainingEnvironment::open_source(Path::new(&root), &|| Ok(()))
            .map_err(|_| {
                source_refresh("source descriptor or configured runtime is unavailable")
            })?;
        let source = environment.source().ok_or_else(invalid_receipt)?;
        verify_source_runtime(
            py,
            source,
            Path::new(&executable),
            &implementation,
            &version,
            &python_tag,
        )?;
        return Ok(TrainingEnvironmentReceipt {
            sha256: encode_hex(environment.receipt_sha256()),
            training_code_revision: environment.training_code_revision().into(),
            origin: "source-development",
            source_development_root: Some(source.root().to_string_lossy().into_owned()),
            validator_path: Some(source.validator().to_string_lossy().into_owned()),
            native_build_revision: Some(source.native_build_revision().into()),
            application_sha256: None,
            onnx_worker_sha256: None,
            validator_sha256: None,
        });
    }
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
        origin: "installed-release",
        source_development_root: None,
        validator_path: None,
        native_build_revision: None,
        application_sha256: Some(encode_hex(verified.application_sha256())),
        onnx_worker_sha256: Some(encode_hex(verified.onnx_worker_sha256())),
        validator_sha256: Some(encode_hex(verified.validator_sha256())),
    })
}

fn verify_source_runtime(
    py: Python<'_>,
    source: &SourceDevelopmentEnvironment,
    executable: &Path,
    implementation: &str,
    version: &str,
    python_tag: &str,
) -> PyResult<()> {
    if implementation != "cpython"
        || version != source.python_version()
        || python_tag != source.python_tag()
        || executable != source.interpreter()
    {
        return Err(source_refresh(
            "active Python differs from the managed interpreter",
        ));
    }
    if source.project_version() != env!("CARGO_PKG_VERSION")
        || source.native_build_revision() != NATIVE_BUILD_REVISION
    {
        return Err(source_refresh(
            "native extension version or build revision differs",
        ));
    }
    let metadata = py
        .import("importlib.metadata")
        .map_err(|_| source_refresh("package metadata is unavailable"))?;
    let project = metadata
        .call_method1("distribution", ("market-squawk",))
        .map_err(|_| source_refresh("editable project metadata is unavailable"))?;
    let project_version: String = project
        .getattr("version")
        .and_then(|value| value.extract())
        .map_err(|_| source_refresh("project version is unavailable"))?;
    if project_version != source.project_version() {
        return Err(source_refresh("project package version differs"));
    }
    let direct_url: String = project
        .call_method1("read_text", ("direct_url.json",))
        .and_then(|value| value.extract())
        .map_err(|_| source_refresh("project installation is not editable"))?;
    if direct_url.len() > 16 * 1024 {
        return Err(source_refresh(
            "editable project metadata exceeds its bound",
        ));
    }
    let editable: bool = py
        .import("json")
        .and_then(|json| json.call_method1("loads", (direct_url,)))
        .and_then(|value| value.get_item("dir_info"))
        .and_then(|value| value.get_item("editable"))
        .and_then(|value| value.extract())
        .map_err(|_| source_refresh("project installation is not editable"))?;
    if !editable {
        return Err(source_refresh("project installation is not editable"));
    }
    let training_file: String = py
        .import("market_squawk.training")
        .and_then(|module| module.filename())
        .and_then(|value| value.extract())
        .map_err(|_| source_refresh("editable training module is unavailable"))?;
    let training_file = Path::new(&training_file)
        .canonicalize()
        .map_err(|_| source_refresh("editable training module is unavailable"))?;
    let source_root = source
        .source_root()
        .canonicalize()
        .map_err(|_| source_refresh("editable source checkout is unavailable"))?;
    if training_file != source_root.join("python/market_squawk/training.py") {
        return Err(source_refresh(
            "training module is outside the configured editable checkout",
        ));
    }
    for (name, expected) in source.runtime_distributions() {
        let observed: String = metadata
            .call_method1("version", (name.as_str(),))
            .and_then(|value| value.extract())
            .map_err(|_| source_refresh(&format!("required dependency {name} is unavailable")))?;
        if &observed != expected {
            return Err(source_refresh(&format!(
                "dependency {name} version differs"
            )));
        }
    }
    Ok(())
}

fn source_refresh(reason: &str) -> PyErr {
    PyValueError::new_err(format!(
        "source development training environment: {reason}; refresh the managed Python environment and native extension"
    ))
}

fn invalid_receipt() -> PyErr {
    PyValueError::new_err("training environment receipt is absent or invalid")
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<TrainingEnvironmentReceipt>()?;
    module.add_function(wrap_pyfunction!(training_environment_receipt, module)?)
}
