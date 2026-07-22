//! Stable-ABI Python bindings for bounded pure Rust analytical kernels.

mod analytics;
mod dataset;
mod receipt;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use tokio_util::sync::CancellationToken;

const FEATURE_IMPLEMENTATION_REVISION: &str = "task14-python-v1";
const SEALED_PYTHON_BUILD: bool =
    option_env!("MARKET_SQUAWK_TRAINING_FOUNDATION_RECEIPT").is_some();
const PYTHON_BUILD_IDENTITY: &str = if SEALED_PYTHON_BUILD {
    "sealed-release-v1"
} else {
    "development-unsealed-v1"
};
const PYTHON_BUILD_IDENTITY_ATTRIBUTE: &str = "__market_squawk_build_identity__";
// Analytics kernels do not yet accept a cancellation callback internally. Keep every
// non-preemptible section small and charge a conservative worst-case operation bound first.
const MAX_ANALYTIC_VALUES: usize = 16_384;
const MAX_ANALYTIC_RETAINED_BYTES: usize = 64 * 1024 * 1024;
const MAX_DECIMAL_TEXT_BYTES: usize = 128;
const MAX_FEATURE_CONTRACTS: usize = 1_024;
const MAX_OPERATION_TIMEOUT_MILLISECONDS: u64 = 24 * 60 * 60 * 1_000;
const MAX_OPERATION_BUDGET: u64 = 100_000_000;
const CONTROL_CHECK_INTERVAL: usize = 1_024;
const MODEL_VALIDATOR_SHA256: Option<&str> = option_env!("MARKET_SQUAWK_MODEL_VALIDATOR_SHA256");

fn invalid_input() -> PyErr {
    PyValueError::new_err("financial input violates a bounded Rust analytics contract")
}

#[derive(Debug)]
struct OperationState {
    deadline: Instant,
    cancellation: CancellationToken,
    remaining_operations: AtomicU64,
}

/// Required deadline, cancellation, and work-budget authority for one Python batch operation.
#[pyclass(frozen, module = "market_squawk._native", skip_from_py_object)]
#[derive(Clone, Debug)]
struct OperationContext {
    state: Arc<OperationState>,
}

impl OperationContext {
    fn check(&self) -> PyResult<()> {
        if self.state.cancellation.is_cancelled() || Instant::now() >= self.state.deadline {
            return Err(invalid_input());
        }
        Ok(())
    }

    fn deadline(&self) -> Instant {
        self.state.deadline
    }

    fn cancellation(&self) -> CancellationToken {
        self.state.cancellation.clone()
    }

    fn admit(&self, operations: u64) -> PyResult<()> {
        self.check()?;
        if operations == 0 {
            return Err(invalid_input());
        }
        let mut current = self.state.remaining_operations.load(Ordering::Acquire);
        loop {
            let remaining = current.checked_sub(operations).ok_or_else(invalid_input)?;
            match self.state.remaining_operations.compare_exchange_weak(
                current,
                remaining,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return self.check(),
                Err(observed) => current = observed,
            }
        }
    }
}

#[pymethods]
impl OperationContext {
    #[new]
    fn new(timeout_milliseconds: u64, max_operations: u64) -> PyResult<Self> {
        if timeout_milliseconds == 0
            || timeout_milliseconds > MAX_OPERATION_TIMEOUT_MILLISECONDS
            || max_operations == 0
            || max_operations > MAX_OPERATION_BUDGET
        {
            return Err(invalid_input());
        }
        let deadline = Instant::now()
            .checked_add(Duration::from_millis(timeout_milliseconds))
            .ok_or_else(invalid_input)?;
        Ok(Self {
            state: Arc::new(OperationState {
                deadline,
                cancellation: CancellationToken::new(),
                remaining_operations: AtomicU64::new(max_operations),
            }),
        })
    }

    fn cancel(&self) {
        self.state.cancellation.cancel();
    }

    fn checkpoint(&self) -> PyResult<()> {
        self.check()
    }

    fn reserve(&self, operations: u64) -> PyResult<()> {
        self.admit(operations)
    }

    #[getter]
    fn remaining_operations(&self) -> u64 {
        self.state.remaining_operations.load(Ordering::Acquire)
    }
}

fn encode_hex(bytes: [u8; 32]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[pyfunction]
fn expected_model_validator_sha256() -> PyResult<&'static str> {
    let value = MODEL_VALIDATOR_SHA256.ok_or_else(invalid_input)?;
    if value.len() != 64
        || value == "0000000000000000000000000000000000000000000000000000000000000000"
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(invalid_input());
    }
    Ok(value)
}

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    if SEALED_PYTHON_BUILD {
        receipt::verify_at_import(module)?;
    }
    module.add(PYTHON_BUILD_IDENTITY_ATTRIBUTE, PYTHON_BUILD_IDENTITY)?;
    module.add_class::<OperationContext>()?;
    module.add_function(wrap_pyfunction!(expected_model_validator_sha256, module)?)?;
    dataset::register(module)?;
    analytics::register(module)?;
    receipt::register(module)
}
