//! Contained child-process initialization and runtime execution.

use std::io::{self, BufReader, BufWriter, Cursor, Read, Write};
#[cfg(feature = "onnx-runtime")]
use std::path::Path;
use std::sync::Arc;

use tract_onnx::prelude::*;
use tract_onnx::tract_hir::internal::DimLike;

use super::protocol::{
    DecodedInitialization, REQUEST_INFER, read_initialization, read_u32, write_response,
};
use super::resources::apply_resource_limits;
use super::{OnnxWorkerProcessError, WorkerError};

const MAX_ONNX_COMPUTE_UNITS: usize = 50_000_000;

/// Runs the private stdio protocol for the packaged helper binary.
pub fn run_onnx_worker_process() -> Result<(), OnnxWorkerProcessError> {
    let _resource_guard = apply_resource_limits()?;
    let stdin = io::stdin();
    let stdout = io::stdout();
    run_worker(BufReader::new(stdin.lock()), BufWriter::new(stdout.lock()))
}

fn run_worker(mut reader: impl Read, mut writer: impl Write) -> Result<(), OnnxWorkerProcessError> {
    let initialization = read_initialization(&mut reader)?;
    let mut runner = match build_runner(&initialization) {
        Ok(runner) => runner,
        Err(error) => {
            write_response(&mut writer, Err(error))?;
            return Ok(());
        }
    };
    write_response(&mut writer, Ok(0.0))?;
    loop {
        let mut opcode = [0_u8; 1];
        match reader.read_exact(&mut opcode) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(_) => return Err(OnnxWorkerProcessError::Protocol),
        }
        if opcode[0] != REQUEST_INFER {
            return Err(OnnxWorkerProcessError::Protocol);
        }
        let count = read_u32(&mut reader)? as usize;
        if count != initialization.input_elements || count > super::super::MAX_ONNX_REQUEST_ELEMENTS
        {
            write_response(&mut writer, Err(WorkerError::Resource))?;
            return Ok(());
        }
        let mut values = Vec::new();
        values
            .try_reserve_exact(count)
            .map_err(|_| OnnxWorkerProcessError::Runtime)?;
        for _ in 0..count {
            let value = f32::from_bits(read_u32(&mut reader)?);
            if !value.is_finite() {
                write_response(&mut writer, Err(WorkerError::Runtime))?;
                return Ok(());
            }
            values.push(value);
        }
        let result = runner.run(&values);
        write_response(&mut writer, result)?;
        if result.is_err() {
            return Ok(());
        }
    }
}

fn build_runner(
    initialization: &DecodedInitialization,
) -> Result<Box<dyn RuntimeRunner>, WorkerError> {
    if initialization.is_tract() {
        let model = tract_onnx::onnx()
            .model_for_read(&mut Cursor::new(&initialization.model))
            .map_err(|_| WorkerError::Load)?;
        let model = model.into_typed().map_err(|_| WorkerError::Load)?;
        validate_typed_model(&model)?;
        let model = model.into_optimized().map_err(|_| WorkerError::Load)?;
        validate_typed_model(&model)?;
        let runnable = model.into_runnable().map_err(|_| WorkerError::Load)?;
        return Ok(Box::new(TractRunner {
            runnable,
            input_shape: initialization.input_shape.clone(),
        }));
    }
    #[cfg(feature = "onnx-runtime")]
    if initialization.is_external() {
        return build_external_runner(initialization);
    }
    Err(WorkerError::Load)
}

trait RuntimeRunner {
    fn run(&mut self, values: &[f32]) -> Result<f32, WorkerError>;
}

struct TractRunner {
    runnable: Arc<TypedRunnableModel>,
    input_shape: Box<[usize]>,
}

impl RuntimeRunner for TractRunner {
    fn run(&mut self, values: &[f32]) -> Result<f32, WorkerError> {
        let tensor =
            Tensor::from_shape(&self.input_shape, values).map_err(|_| WorkerError::Runtime)?;
        let outputs = self
            .runnable
            .run(tvec!(tensor.into_tvalue()))
            .map_err(|_| WorkerError::Runtime)?;
        if outputs.len() != 1 {
            return Err(WorkerError::Runtime);
        }
        let output = outputs[0]
            .to_plain_array_view::<f32>()
            .map_err(|_| WorkerError::Runtime)?;
        finite_scalar(output.iter().copied())
    }
}

#[cfg(feature = "onnx-runtime")]
fn build_external_runner(
    initialization: &DecodedInitialization,
) -> Result<Box<dyn RuntimeRunner>, WorkerError> {
    let path = Path::new(initialization.runtime_path.as_ref());
    if !path.is_absolute()
        || initialization.runtime_version.as_ref() != super::super::OPTIONAL_ONNX_RUNTIME_VERSION
    {
        return Err(WorkerError::Load);
    }
    super::super::external::verify_sealed_runtime(
        path,
        initialization.runtime_digest,
        initialization.runtime_platform,
    )
    .map_err(|_| WorkerError::Load)?;
    let committed = ort::init_from(path)
        .map_err(|_| WorkerError::Load)?
        .with_name("market-squawk-local-onnx-runtime")
        .with_telemetry(false)
        .commit();
    if !committed {
        return Err(WorkerError::Load);
    }
    let builder = ort::session::Session::builder().map_err(|_| WorkerError::Load)?;
    let builder = builder
        .with_intra_threads(1)
        .map_err(|_| WorkerError::Load)?;
    let builder = builder
        .with_inter_threads(1)
        .map_err(|_| WorkerError::Load)?;
    let mut builder = builder
        .with_parallel_execution(false)
        .map_err(|_| WorkerError::Load)?;
    let session = builder
        .commit_from_memory(&initialization.model)
        .map_err(|_| WorkerError::Load)?;
    Ok(Box::new(ExternalRunner {
        session,
        input_shape: initialization.input_shape.clone(),
    }))
}

#[cfg(feature = "onnx-runtime")]
struct ExternalRunner {
    session: ort::session::Session,
    input_shape: Box<[usize]>,
}

#[cfg(feature = "onnx-runtime")]
impl RuntimeRunner for ExternalRunner {
    fn run(&mut self, values: &[f32]) -> Result<f32, WorkerError> {
        let input = ort::value::TensorRef::from_array_view((&*self.input_shape, values))
            .map_err(|_| WorkerError::Runtime)?;
        let outputs = self
            .session
            .run(ort::inputs![input])
            .map_err(|_| WorkerError::Runtime)?;
        if outputs.len() != 1 {
            return Err(WorkerError::Runtime);
        }
        let (_, output) = outputs[0]
            .try_extract_tensor::<f32>()
            .map_err(|_| WorkerError::Runtime)?;
        finite_scalar(output.iter().copied())
    }
}

fn finite_scalar(mut values: impl ExactSizeIterator<Item = f32>) -> Result<f32, WorkerError> {
    if values.len() != 1 {
        return Err(WorkerError::Runtime);
    }
    let value = values.next().ok_or(WorkerError::Runtime)?;
    value
        .is_finite()
        .then_some(value)
        .ok_or(WorkerError::Runtime)
}

fn validate_typed_model(model: &TypedModel) -> Result<(), WorkerError> {
    let mut aggregate_elements = 0_usize;
    let mut aggregate_compute = 0_usize;
    let mut outlet_count = 0_usize;
    for node in model.nodes() {
        let mut node_elements = 0_usize;
        for output in &node.outputs {
            outlet_count = outlet_count.checked_add(1).ok_or(WorkerError::Resource)?;
            let shape = output
                .fact
                .shape
                .as_concrete()
                .ok_or(WorkerError::Resource)?;
            let elements = shape.iter().try_fold(1_usize, |product, dimension| {
                product.checked_mul(*dimension).ok_or(WorkerError::Resource)
            })?;
            if elements > super::super::MAX_ONNX_REQUEST_ELEMENTS {
                return Err(WorkerError::Resource);
            }
            node_elements = node_elements
                .checked_add(elements)
                .ok_or(WorkerError::Resource)?;
            aggregate_elements = aggregate_elements
                .checked_add(elements)
                .filter(|elements| *elements <= super::super::MAX_ONNX_REQUEST_ELEMENTS)
                .ok_or(WorkerError::Resource)?;
        }
        let input_facts = node
            .inputs
            .iter()
            .map(|outlet| {
                model
                    .outlet_fact(*outlet)
                    .map_err(|_| WorkerError::Resource)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let reported_compute = node
            .op
            .cost(&input_facts)
            .map_err(|_| WorkerError::Resource)?
            .into_iter()
            .filter(|(cost, _)| cost.is_compute())
            .try_fold(0_usize, |total, (_, units)| {
                let units = units.to_usize().map_err(|_| WorkerError::Resource)?;
                total.checked_add(units).ok_or(WorkerError::Resource)
            })?;
        aggregate_compute = aggregate_compute
            .checked_add(reported_compute.max(node_elements))
            .filter(|units| *units <= MAX_ONNX_COMPUTE_UNITS)
            .ok_or(WorkerError::Resource)?;
    }
    if outlet_count > super::super::MAX_ONNX_TENSORS {
        return Err(WorkerError::Resource);
    }
    Ok(())
}
