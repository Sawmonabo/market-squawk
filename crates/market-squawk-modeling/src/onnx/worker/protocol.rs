//! Fixed-size framing and bounded startup payloads for the private ONNX worker.

use std::io::{BufReader, Read, Write};
#[cfg(feature = "onnx-runtime")]
use std::path::Path;
use std::sync::mpsc::SyncSender;

use super::{OnnxWorkerProcessError, WorkerError};

const WORKER_MAGIC: &[u8; 8] = b"MSQONX01";
const BACKEND_TRACT: u8 = 1;
#[cfg(feature = "onnx-runtime")]
const BACKEND_EXTERNAL: u8 = 2;
pub(super) const REQUEST_INFER: u8 = 1;
const RESPONSE_OK: u8 = 0;
const RESPONSE_LOAD: u8 = 1;
const RESPONSE_RESOURCE: u8 = 2;
const RESPONSE_RUNTIME: u8 = 3;
const MAX_RUNTIME_PATH_BYTES: usize = 1_024;
const MAX_RUNTIME_VERSION_BYTES: usize = 64;

#[derive(Debug)]
pub(super) struct WorkerInitialization {
    pub(super) bytes: Vec<u8>,
    pub(super) input_elements: usize,
}

impl WorkerInitialization {
    pub(super) fn tract(
        model: &[u8],
        input_shape: &[usize],
        input_elements: usize,
    ) -> Result<Self, WorkerError> {
        Self::new(BACKEND_TRACT, model, input_shape, input_elements, None)
    }

    #[cfg(feature = "onnx-runtime")]
    #[allow(
        clippy::too_many_arguments,
        reason = "the exact external runtime identity remains explicit at the process boundary"
    )]
    pub(super) fn external(
        model: &[u8],
        input_shape: &[usize],
        input_elements: usize,
        runtime_path: &Path,
        runtime_digest: [u8; 32],
        runtime_version: &str,
        runtime_platform: u8,
    ) -> Result<Self, WorkerError> {
        let runtime_path = runtime_path.to_str().ok_or(WorkerError::Load)?;
        if !Path::new(runtime_path).is_absolute()
            || runtime_path.len() > MAX_RUNTIME_PATH_BYTES
            || runtime_version.len() > MAX_RUNTIME_VERSION_BYTES
        {
            return Err(WorkerError::Load);
        }
        Self::new(
            BACKEND_EXTERNAL,
            model,
            input_shape,
            input_elements,
            Some(ExternalInitialization {
                path: runtime_path,
                digest: runtime_digest,
                version: runtime_version,
                platform: runtime_platform,
            }),
        )
    }

    fn new(
        backend: u8,
        model: &[u8],
        input_shape: &[usize],
        input_elements: usize,
        external: Option<ExternalInitialization<'_>>,
    ) -> Result<Self, WorkerError> {
        if model.is_empty()
            || model.len() > super::super::MAX_ONNX_MODEL_BYTES
            || input_shape.is_empty()
            || input_shape.len() > 8
            || input_elements == 0
            || input_elements > super::super::MAX_ONNX_REQUEST_ELEMENTS
        {
            return Err(WorkerError::Resource);
        }
        let (path, digest, version, platform) = external.map_or(("", [0; 32], "", 0), |value| {
            (value.path, value.digest, value.version, value.platform)
        });
        let capacity = 54_usize
            .checked_add(input_shape.len().saturating_mul(4))
            .and_then(|value| value.checked_add(path.len()))
            .and_then(|value| value.checked_add(version.len()))
            .and_then(|value| value.checked_add(model.len()))
            .ok_or(WorkerError::Resource)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(capacity)
            .map_err(|_| WorkerError::Resource)?;
        bytes.extend_from_slice(WORKER_MAGIC);
        bytes.push(backend);
        bytes.push(u8::try_from(input_shape.len()).map_err(|_| WorkerError::Resource)?);
        bytes.push(platform);
        bytes.push(0);
        bytes.extend_from_slice(
            &u32::try_from(input_elements)
                .map_err(|_| WorkerError::Resource)?
                .to_be_bytes(),
        );
        bytes.extend_from_slice(
            &u32::try_from(model.len())
                .map_err(|_| WorkerError::Resource)?
                .to_be_bytes(),
        );
        bytes.extend_from_slice(
            &u16::try_from(path.len())
                .map_err(|_| WorkerError::Resource)?
                .to_be_bytes(),
        );
        bytes.extend_from_slice(
            &u16::try_from(version.len())
                .map_err(|_| WorkerError::Resource)?
                .to_be_bytes(),
        );
        bytes.extend_from_slice(&digest);
        for dimension in input_shape {
            bytes.extend_from_slice(
                &u32::try_from(*dimension)
                    .map_err(|_| WorkerError::Resource)?
                    .to_be_bytes(),
            );
        }
        bytes.extend_from_slice(path.as_bytes());
        bytes.extend_from_slice(version.as_bytes());
        bytes.extend_from_slice(model);
        Ok(Self {
            bytes,
            input_elements,
        })
    }
}

#[derive(Clone, Copy, Debug)]
struct ExternalInitialization<'a> {
    path: &'a str,
    digest: [u8; 32],
    version: &'a str,
    platform: u8,
}

#[derive(Debug)]
pub(super) struct DecodedInitialization {
    pub(super) backend: u8,
    pub(super) input_shape: Box<[usize]>,
    pub(super) input_elements: usize,
    pub(super) model: Vec<u8>,
    pub(super) runtime_path: Box<str>,
    pub(super) runtime_digest: [u8; 32],
    pub(super) runtime_version: Box<str>,
    pub(super) runtime_platform: u8,
}

impl DecodedInitialization {
    pub(super) fn is_tract(&self) -> bool {
        self.backend == BACKEND_TRACT
            && self.runtime_path.is_empty()
            && self.runtime_version.is_empty()
            && self.runtime_digest == [0; 32]
            && self.runtime_platform == 0
    }

    #[cfg(feature = "onnx-runtime")]
    pub(super) fn is_external(&self) -> bool {
        self.backend == BACKEND_EXTERNAL
    }
}

pub(super) fn read_initialization(
    reader: &mut impl Read,
) -> Result<DecodedInitialization, OnnxWorkerProcessError> {
    let mut magic = [0_u8; 8];
    reader
        .read_exact(&mut magic)
        .map_err(|_| OnnxWorkerProcessError::Protocol)?;
    if &magic != WORKER_MAGIC {
        return Err(OnnxWorkerProcessError::Protocol);
    }
    let backend = read_u8(reader)?;
    let rank = read_u8(reader)? as usize;
    let runtime_platform = read_u8(reader)?;
    if read_u8(reader)? != 0 || rank == 0 || rank > 8 {
        return Err(OnnxWorkerProcessError::Protocol);
    }
    let input_elements = read_u32(reader)? as usize;
    let model_len = read_u32(reader)? as usize;
    let path_len = usize::from(read_u16(reader)?);
    let version_len = usize::from(read_u16(reader)?);
    let mut runtime_digest = [0_u8; 32];
    reader
        .read_exact(&mut runtime_digest)
        .map_err(|_| OnnxWorkerProcessError::Protocol)?;
    if input_elements == 0
        || input_elements > super::super::MAX_ONNX_REQUEST_ELEMENTS
        || model_len == 0
        || model_len > super::super::MAX_ONNX_MODEL_BYTES
        || path_len > MAX_RUNTIME_PATH_BYTES
        || version_len > MAX_RUNTIME_VERSION_BYTES
    {
        return Err(OnnxWorkerProcessError::Runtime);
    }
    let mut input_shape = Vec::new();
    input_shape
        .try_reserve_exact(rank)
        .map_err(|_| OnnxWorkerProcessError::Runtime)?;
    for _ in 0..rank {
        let dimension = read_u32(reader)? as usize;
        if dimension == 0 {
            return Err(OnnxWorkerProcessError::Protocol);
        }
        input_shape.push(dimension);
    }
    if input_shape.iter().try_fold(1_usize, |product, dimension| {
        product.checked_mul(*dimension)
    }) != Some(input_elements)
    {
        return Err(OnnxWorkerProcessError::Protocol);
    }
    let runtime_path = read_utf8(reader, path_len)?;
    let runtime_version = read_utf8(reader, version_len)?;
    let mut model = Vec::new();
    model
        .try_reserve_exact(model_len)
        .map_err(|_| OnnxWorkerProcessError::Runtime)?;
    model.resize(model_len, 0);
    reader
        .read_exact(&mut model)
        .map_err(|_| OnnxWorkerProcessError::Protocol)?;
    Ok(DecodedInitialization {
        backend,
        input_shape: input_shape.into_boxed_slice(),
        input_elements,
        model,
        runtime_path: runtime_path.into_boxed_str(),
        runtime_digest,
        runtime_version: runtime_version.into_boxed_str(),
        runtime_platform,
    })
}

pub(super) fn response_loop(stdout: impl Read, sender: SyncSender<Result<f32, WorkerError>>) {
    let mut reader = BufReader::new(stdout);
    loop {
        let mut response = [0_u8; 5];
        if reader.read_exact(&mut response).is_err() {
            let _ = sender.try_send(Err(WorkerError::Unavailable));
            break;
        }
        let result = decode_response(response);
        if sender.send(result).is_err() || result.is_err() {
            break;
        }
    }
}

pub(super) fn write_response(
    writer: &mut impl Write,
    result: Result<f32, WorkerError>,
) -> Result<(), OnnxWorkerProcessError> {
    let (status, value) = match result {
        Ok(value) if value.is_finite() => (RESPONSE_OK, value),
        Err(WorkerError::Load) => (RESPONSE_LOAD, 0.0),
        Err(WorkerError::Resource) => (RESPONSE_RESOURCE, 0.0),
        Err(WorkerError::Unavailable | WorkerError::Deadline | WorkerError::Runtime) | Ok(_) => {
            (RESPONSE_RUNTIME, 0.0)
        }
    };
    let value = value.to_bits().to_be_bytes();
    writer
        .write_all(&[status, value[0], value[1], value[2], value[3]])
        .and_then(|()| writer.flush())
        .map_err(|_| OnnxWorkerProcessError::Protocol)
}

pub(super) fn read_u32(reader: &mut impl Read) -> Result<u32, OnnxWorkerProcessError> {
    let mut bytes = [0_u8; 4];
    reader
        .read_exact(&mut bytes)
        .map_err(|_| OnnxWorkerProcessError::Protocol)?;
    Ok(u32::from_be_bytes(bytes))
}

fn read_u8(reader: &mut impl Read) -> Result<u8, OnnxWorkerProcessError> {
    let mut bytes = [0_u8; 1];
    reader
        .read_exact(&mut bytes)
        .map_err(|_| OnnxWorkerProcessError::Protocol)?;
    Ok(bytes[0])
}

fn read_u16(reader: &mut impl Read) -> Result<u16, OnnxWorkerProcessError> {
    let mut bytes = [0_u8; 2];
    reader
        .read_exact(&mut bytes)
        .map_err(|_| OnnxWorkerProcessError::Protocol)?;
    Ok(u16::from_be_bytes(bytes))
}

fn read_utf8(reader: &mut impl Read, length: usize) -> Result<String, OnnxWorkerProcessError> {
    let mut bytes = vec![0_u8; length];
    reader
        .read_exact(&mut bytes)
        .map_err(|_| OnnxWorkerProcessError::Protocol)?;
    String::from_utf8(bytes).map_err(|_| OnnxWorkerProcessError::Protocol)
}

fn decode_response(response: [u8; 5]) -> Result<f32, WorkerError> {
    let value = f32::from_bits(u32::from_be_bytes([
        response[1],
        response[2],
        response[3],
        response[4],
    ]));
    match response[0] {
        RESPONSE_OK if value.is_finite() => Ok(value),
        RESPONSE_LOAD => Err(WorkerError::Load),
        RESPONSE_RESOURCE => Err(WorkerError::Resource),
        RESPONSE_RUNTIME | RESPONSE_OK => Err(WorkerError::Runtime),
        _ => Err(WorkerError::Unavailable),
    }
}
