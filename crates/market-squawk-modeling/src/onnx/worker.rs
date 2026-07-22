//! Single-owner bounded ONNX execution workers.

use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::thread;
use std::time::Duration;

use tract_onnx::prelude::*;

#[derive(Debug)]
pub(crate) struct OnnxWorker {
    sender: SyncSender<Request>,
    available: Arc<AtomicBool>,
    deadline: Duration,
}

impl OnnxWorker {
    pub(crate) fn start_tract(
        model_bytes: &[u8],
        input_shape: &[usize],
        input_elements: usize,
        deadline: Duration,
    ) -> Result<(Self, f32), WorkerError> {
        let model = tract_onnx::onnx()
            .model_for_read(&mut Cursor::new(model_bytes))
            .map_err(|_| WorkerError::Load)?;
        let model = model.into_typed().map_err(|_| WorkerError::Load)?;
        validate_typed_model(&model)?;
        let model = model.into_optimized().map_err(|_| WorkerError::Load)?;
        validate_typed_model(&model)?;
        let runnable = model.into_runnable().map_err(|_| WorkerError::Load)?;
        Self::start_runner(
            Box::new(TractRunner {
                runnable,
                input_shape: input_shape.into(),
            }),
            input_elements,
            deadline,
            "market-squawk-tract-onnx",
        )
    }

    #[cfg(feature = "onnx-runtime")]
    pub(crate) fn start_external(
        session: ort::session::Session,
        input_shape: &[usize],
        input_elements: usize,
        deadline: Duration,
    ) -> Result<(Self, f32), WorkerError> {
        Self::start_runner(
            Box::new(ExternalRunner {
                session,
                input_shape: input_shape.into(),
            }),
            input_elements,
            deadline,
            "market-squawk-external-onnx",
        )
    }

    fn start_runner(
        runner: Box<dyn RuntimeRunner>,
        input_elements: usize,
        deadline: Duration,
        thread_name: &str,
    ) -> Result<(Self, f32), WorkerError> {
        let (sender, receiver) = mpsc::sync_channel(1);
        let available = Arc::new(AtomicBool::new(true));
        let worker_available = Arc::clone(&available);
        thread::Builder::new()
            .name(thread_name.to_owned())
            .spawn(move || worker_loop(receiver, worker_available, runner))
            .map_err(|_| WorkerError::Load)?;
        let worker = Self {
            sender,
            available,
            deadline,
        };
        let warm_up = worker.execute(vec![0.0; input_elements])?;
        Ok((worker, warm_up))
    }

    pub(crate) fn execute(&self, values: Vec<f32>) -> Result<f32, WorkerError> {
        if !self.available.load(Ordering::Acquire) {
            return Err(WorkerError::Unavailable);
        }
        let (response_sender, response_receiver) = mpsc::sync_channel(1);
        let request = Request {
            values: values.into_boxed_slice(),
            response: response_sender,
        };
        match self.sender.try_send(request) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => return Err(WorkerError::Unavailable),
            Err(TrySendError::Disconnected(_)) => {
                self.available.store(false, Ordering::Release);
                return Err(WorkerError::Unavailable);
            }
        }
        match response_receiver.recv_timeout(self.deadline) {
            Ok(result) => result,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                self.available.store(false, Ordering::Release);
                Err(WorkerError::Deadline)
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                self.available.store(false, Ordering::Release);
                Err(WorkerError::Unavailable)
            }
        }
    }
}

struct Request {
    values: Box<[f32]>,
    response: SyncSender<Result<f32, WorkerError>>,
}

fn worker_loop(
    receiver: Receiver<Request>,
    available: Arc<AtomicBool>,
    mut runner: Box<dyn RuntimeRunner>,
) {
    while available.load(Ordering::Acquire) {
        let Ok(request) = receiver.recv() else {
            break;
        };
        let result = runner.run(&request.values);
        if result.is_err() {
            available.store(false, Ordering::Release);
        }
        let _ = request.response.send(result);
    }
    available.store(false, Ordering::Release);
}

trait RuntimeRunner: Send {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkerError {
    Load,
    Resource,
    Unavailable,
    Deadline,
    Runtime,
}

fn validate_typed_model(model: &TypedModel) -> Result<(), WorkerError> {
    let mut aggregate_elements = 0_usize;
    let mut outlet_count = 0_usize;
    for node in model.nodes() {
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
            if elements > super::MAX_ONNX_REQUEST_ELEMENTS {
                return Err(WorkerError::Resource);
            }
            aggregate_elements = aggregate_elements
                .checked_add(elements)
                .filter(|elements| *elements <= super::MAX_ONNX_REQUEST_ELEMENTS)
                .ok_or(WorkerError::Resource)?;
        }
    }
    if outlet_count > super::MAX_ONNX_TENSORS {
        return Err(WorkerError::Resource);
    }
    Ok(())
}
