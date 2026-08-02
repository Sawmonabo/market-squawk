use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use market_squawk_data::{CatalogEndpointIdentity, Sha256Digest};
use market_squawk_domain::Timestamp;
use market_squawk_modeling::{
    BundleMetadataRef, ModelOutputSemantics, OnnxFallbackPolicy, OnnxModelPolicy,
    PythonDatasetAdmissionAuthority, TrainingWorkerCandidate, VerifiedTrainingEnvironment,
};
use serde::Deserialize;

use super::{
    ModelAdmissionRequest, ModelBackendAdmission, ProductionModelRuntimeError,
    WorkerCandidateExpectation,
};

impl ModelAdmissionRequest {
    /// Returns the bounded document's authority coordinate for a caller-owned no-follow read.
    pub fn authority_path_from_json(
        document: &[u8],
    ) -> Result<PathBuf, ProductionModelRuntimeError> {
        Ok(parse_wire(document)?.authority.path)
    }

    /// Decodes the closed transport-neutral document with independently supplied authority bytes.
    pub fn decode_json(
        document: &[u8],
        authority_bytes: Box<[u8]>,
    ) -> Result<Self, ProductionModelRuntimeError> {
        request_from_wire(parse_wire(document)?, authority_bytes)
    }

    /// Decodes the existing closed admission document only after every worker claim has been
    /// bound to service-owned candidate and authority coordinates.
    pub fn decode_training_worker(
        document: &[u8],
        authority_bytes: Box<[u8]>,
        expected_authority_path: &Path,
        candidate: &TrainingWorkerCandidate,
        environment: &VerifiedTrainingEnvironment,
    ) -> Result<Self, ProductionModelRuntimeError> {
        candidate
            .verify_environment(environment)
            .map_err(|_| ProductionModelRuntimeError::CandidateEvidenceMismatch)?;
        let wire = parse_wire(document)?;
        if wire.candidate_directory != candidate.candidate_directory()
            || wire.authority.path != expected_authority_path
        {
            return Err(ProductionModelRuntimeError::CandidateEvidenceMismatch);
        }
        let metadata_sha256 = parse_sha256(&wire.metadata.sha256)?;
        let authority_sha256 = parse_sha256(&wire.authority.sha256)?;
        let dataset_export_sha256 = parse_sha256(&wire.dataset.export_sha256)?;
        let dataset_selection_sha256 = parse_sha256(&wire.dataset.selection_sha256)?;
        let catalog_identity_sha256 = parse_sha256(&wire.dataset.catalog_identity_sha256)?;
        if metadata_sha256 != candidate.metadata_sha256()
            || authority_sha256 != candidate.authority_sha256()
            || dataset_export_sha256 != candidate.dataset_export_sha256()
            || dataset_selection_sha256 != candidate.dataset_selection_sha256()
            || catalog_identity_sha256 != candidate.catalog_identity_sha256()
        {
            return Err(ProductionModelRuntimeError::CandidateEvidenceMismatch);
        }
        let mut request = request_from_wire(wire, authority_bytes)?;
        if !matches!(&request.backend, ModelBackendAdmission::Onnx(policy)
            if policy.model_digest().bytes() == candidate.artifact_sha256())
        {
            return Err(ProductionModelRuntimeError::CandidateEvidenceMismatch);
        }
        request.worker_expectation = Some(WorkerCandidateExpectation {
            metadata_sha256,
            artifact_sha256: candidate.artifact_sha256(),
            training_run_sha256: candidate.training_run_sha256(),
            authority_sha256,
            dataset_export_sha256,
            dataset_selection_sha256,
            catalog_identity_sha256,
            training_environment_sha256: candidate.training_environment_sha256(),
            training_code_revision: candidate.training_code_revision().into(),
        });
        Ok(request)
    }
}

fn parse_wire(document: &[u8]) -> Result<ModelAdmissionRequestWire, ProductionModelRuntimeError> {
    let wire: ModelAdmissionRequestWire = serde_json::from_slice(document)
        .map_err(|_| ProductionModelRuntimeError::InvalidAdmission)?;
    if wire.schema_version != 1 {
        return Err(ProductionModelRuntimeError::InvalidAdmission);
    }
    Ok(wire)
}

fn request_from_wire(
    wire: ModelAdmissionRequestWire,
    authority_bytes: Box<[u8]>,
) -> Result<ModelAdmissionRequest, ProductionModelRuntimeError> {
    let metadata_sha256 = parse_sha256(&wire.metadata.sha256)?;
    let authority_sha256 = parse_sha256(&wire.authority.sha256)?;
    let dataset_export_sha256 = parse_sha256(&wire.dataset.export_sha256)?;
    let dataset_selection_sha256 = parse_sha256(&wire.dataset.selection_sha256)?;
    let catalog_identity_sha256 = parse_sha256(&wire.dataset.catalog_identity_sha256)?;
    let metadata = BundleMetadataRef::try_new(
        wire.metadata.relative_path,
        Sha256Digest::new(metadata_sha256),
    )
    .map_err(|_| ProductionModelRuntimeError::InvalidAdmission)?;
    let catalog_identity = CatalogEndpointIdentity::try_from_bytes(catalog_identity_sha256)
        .ok_or(ProductionModelRuntimeError::InvalidAdmission)?;
    let dataset = PythonDatasetAdmissionAuthority::try_new(
        Sha256Digest::new(dataset_export_sha256),
        Timestamp::from_unix_nanos(wire.dataset.as_of_unix_nanos),
        Sha256Digest::new(dataset_selection_sha256),
        catalog_identity,
    )?;
    ModelAdmissionRequest::try_new(
        wire.candidate_directory,
        metadata,
        authority_bytes,
        Sha256Digest::new(authority_sha256),
        dataset,
        wire.backend.into_domain()?,
    )
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ModelAdmissionRequestWire {
    schema_version: u16,
    candidate_directory: String,
    metadata: MetadataReferenceWire,
    authority: AuthorityReferenceWire,
    dataset: DatasetAuthorityWire,
    backend: BackendAdmissionWire,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MetadataReferenceWire {
    relative_path: String,
    sha256: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AuthorityReferenceWire {
    path: PathBuf,
    sha256: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DatasetAuthorityWire {
    export_sha256: String,
    as_of_unix_nanos: i64,
    selection_sha256: String,
    catalog_identity_sha256: String,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum BackendAdmissionWire {
    Native,
    Onnx {
        #[serde(rename = "modelSha256")]
        model_sha256: String,
        opset: u32,
        #[serde(rename = "inputShape")]
        input_shape: Vec<usize>,
        #[serde(rename = "outputShape")]
        output_shape: Vec<usize>,
        #[serde(rename = "outputSemantics")]
        output_semantics: Option<OnnxOutputSemanticsWire>,
        #[serde(rename = "inferenceDeadlineMillis")]
        inference_deadline_millis: u64,
        fallback: OnnxFallbackWire,
    },
}

impl BackendAdmissionWire {
    fn into_domain(self) -> Result<ModelBackendAdmission, ProductionModelRuntimeError> {
        match self {
            Self::Native => Ok(ModelBackendAdmission::Native),
            Self::Onnx {
                model_sha256,
                opset,
                input_shape,
                output_shape,
                output_semantics,
                inference_deadline_millis,
                fallback,
            } => {
                let model_digest = Sha256Digest::new(parse_sha256(&model_sha256)?);
                let deadline = Duration::from_millis(inference_deadline_millis);
                let policy = if let Some(output_semantics) = output_semantics {
                    OnnxModelPolicy::try_new_with_output_semantics(
                        model_digest,
                        opset,
                        &input_shape,
                        &output_shape,
                        output_semantics.into_domain(),
                        deadline,
                        fallback.into_domain(),
                    )
                } else {
                    OnnxModelPolicy::try_new(
                        model_digest,
                        opset,
                        &input_shape,
                        &output_shape,
                        deadline,
                        fallback.into_domain(),
                    )
                }
                .map_err(|_| ProductionModelRuntimeError::InvalidAdmission)?;
                Ok(ModelBackendAdmission::Onnx(policy))
            }
        }
    }
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OnnxFallbackWire {
    NoAction,
}

impl OnnxFallbackWire {
    const fn into_domain(self) -> OnnxFallbackPolicy {
        match self {
            Self::NoAction => OnnxFallbackPolicy::NoAction,
        }
    }
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OnnxOutputSemanticsWire {
    Regression,
    BinaryProbability,
}

impl OnnxOutputSemanticsWire {
    const fn into_domain(self) -> ModelOutputSemantics {
        match self {
            Self::Regression => ModelOutputSemantics::Regression,
            Self::BinaryProbability => ModelOutputSemantics::BinaryProbability,
        }
    }
}

fn parse_sha256(value: &str) -> Result<[u8; 32], ProductionModelRuntimeError> {
    if value.len() != 64
        || value
            .bytes()
            .any(|byte| !byte.is_ascii_digit() && !matches!(byte, b'a'..=b'f'))
    {
        return Err(ProductionModelRuntimeError::InvalidAdmission);
    }
    let mut output = [0_u8; 32];
    for (target, pair) in output.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
        let high = hex_nibble(pair[0]).ok_or(ProductionModelRuntimeError::InvalidAdmission)?;
        let low = hex_nibble(pair[1]).ok_or(ProductionModelRuntimeError::InvalidAdmission)?;
        *target = (high << 4) | low;
    }
    Ok(output)
}

const fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}
