//! Manifest-bound portfolio import for the local CLI product boundary.

use std::io::{self, Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use market_squawk_adapter_portfolio::PortfolioImportLimits;
use market_squawk_domain::{
    AccountId, AuthorizationBasis, ChecksumCapability, CoverageDelay, DataQuality,
    DeliveryEvidence, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, MetadataRevision,
    RevisionBoundPayloadEvidence, SchemaVersion, SequenceCapability, SourceId, SourceIdentifier,
    Timestamp,
};
use market_squawk_platform::{
    ArtifactPathError, ArtifactRoot, LocalAuthorityStateStore, UserAuthorizedInputRoot,
};
use market_squawk_services::{
    JsonStructureLimits, RequestContext, RequestId, ServiceError, ServiceLimits, TypedToolResult,
};
use market_squawk_sources::{
    AuthorizationGrant, AuthorizationMode, CoverageDomain, FreshnessPolicy, HistoricalCapability,
    NetworkAccessPolicy, SourceCapabilities, SourceClass, SourceCoverage, SourceMetadata,
    SourceMetadataInput, SourceProtocolProfile,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::{
    PortfolioAdapterActivation, ProviderActivationOutcome, ProviderAdapterActivationRequest,
    StartOnboardingRequest,
};

use super::LocalProduct;

const PORTFOLIO_PROFILE: &str = "local.portfolio-imports";
const PORTFOLIO_ARCHIVE_NAMESPACE: &str = "sources/portfolio-manifests";
const PORTFOLIO_ARTIFACT_NAMESPACE: &str = "portfolio/imports";
const PORTFOLIO_MANIFEST_SCHEMA_VERSION: u16 = 1;
const PORTFOLIO_MANIFEST_MAXIMUM_BYTES: u64 = 8 * 1024 * 1024;
const PORTFOLIO_ARTIFACT_MAXIMUM_BYTES: usize = 8 * 1024 * 1024;
const CLI_REQUEST_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const CLI_RESULT_MAXIMUM_ITEMS: usize = 10_000;
const CLI_RESULT_MAXIMUM_BYTES: usize = 8 * 1024 * 1024;
const CLI_HARD_MAXIMUM_BYTES: usize = 64 * 1024 * 1024;

/// Imports one versioned, user-owned portfolio manifest through shared product authorities.
///
/// Only `schema_version`, `dataset`, and `object_id` are parsed here for exact adapter routing.
/// The portfolio adapter remains the sole validator of the full manifest, raw records, bounds,
/// provenance, and reconciliation semantics.
pub(super) async fn import_portfolio_manifest(
    product: &LocalProduct,
    manifest_path: &Path,
    account: String,
    confirm: bool,
) -> Result<Value, CliPortfolioImportError> {
    if !confirm {
        return Err(CliPortfolioImportError::ConfirmationRequired);
    }
    let account_id = account
        .parse::<AccountId>()
        .map_err(|_error| CliPortfolioImportError::InvalidAccount)?;
    let context = request_context()?;
    ensure_live(&context)?;

    let absolute = admitted_absolute_path(manifest_path)?;
    let root_path = absolute
        .parent()
        .ok_or(CliPortfolioImportError::InputUnavailable)?;
    let manifest_name = absolute
        .file_name()
        .ok_or(CliPortfolioImportError::InputUnavailable)?;
    let manifest_reference = PathBuf::from(manifest_name);
    let (root, ownership) = UserAuthorizedInputRoot::open_with_ownership_authority(root_path)
        .map_err(|_error| CliPortfolioImportError::InputUnavailable)?;
    let manifest = root
        .resolve(&manifest_reference)
        .and_then(|file| file.open_bounded(PORTFOLIO_MANIFEST_MAXIMUM_BYTES))
        .and_then(|file| file.read_bounded())
        .map_err(|_error| CliPortfolioImportError::InputUnavailable)?;
    let ownership_evidence = ownership
        .issue_manifest_evidence(&manifest)
        .map_err(|_error| CliPortfolioImportError::InputUnavailable)?;
    let routing = parse_routing(manifest.as_bytes())?;
    let digest = manifest.digest();
    let digest_hex = hex(&digest.bytes());
    let metadata = portfolio_metadata(digest, &digest_hex)?;
    let archive = LocalAuthorityStateStore::try_open(
        product
            .paths()
            .control_root()
            .map_err(|_error| CliPortfolioImportError::AuthorityUnavailable)?
            .root()
            .join(PORTFOLIO_ARCHIVE_NAMESPACE)
            .join(&digest_hex),
    )
    .map_err(|_error| CliPortfolioImportError::AuthorityUnavailable)?;
    ensure_live(&context)?;

    let cancellation = context.cancellation().child_token();
    let session = product
        .provider_onboarding()
        .start(
            StartOnboardingRequest::try_new(PORTFOLIO_PROFILE, None, None)
                .map_err(|_error| CliPortfolioImportError::Onboarding)?,
            cancellation.clone(),
        )
        .await
        .map_err(|_error| CliPortfolioImportError::Onboarding)?;
    let activation = PortfolioAdapterActivation::new(
        metadata,
        root,
        manifest_reference,
        manifest,
        archive,
        None,
        PortfolioImportLimits::standard(),
        ownership_evidence,
    );
    let activated = product
        .provider_activation()
        .activate_ready_profile(
            session.session_id(),
            ProviderAdapterActivationRequest::Portfolio(activation),
            cancellation,
        )
        .await
        .map_err(|_error| CliPortfolioImportError::Activation)?;
    let ProviderActivationOutcome::Research(activated) = activated else {
        return Err(CliPortfolioImportError::Activation);
    };
    let batch = product
        .research_ingest()
        .extract_registered_batch(
            activated.profile(),
            &routing.dataset,
            &routing.object_id,
            &context,
        )
        .await
        .map_err(CliPortfolioImportError::Extraction)?;
    ensure_live(&context)?;

    let serialized = serialize_batch(&batch)?;
    let artifact_sha256: [u8; 32] = Sha256::digest(&serialized).into();
    let artifact_reference = format!(
        "{PORTFOLIO_ARTIFACT_NAMESPACE}/{}.json",
        hex(&artifact_sha256)
    );
    let artifacts = product
        .paths()
        .artifacts()
        .map_err(|_error| CliPortfolioImportError::Artifact)?
        .clone();
    let persisted_reference = artifact_reference.clone();
    tokio::task::spawn_blocking(move || {
        persist_immutable(&artifacts, &persisted_reference, &serialized)
    })
    .await
    .map_err(|_error| CliPortfolioImportError::Artifact)??;
    ensure_live(&context)?;

    let arguments = json_object(json!({
        "accountId": account_id.to_string(),
        "artifactId": artifact_reference,
        "confirm": true,
        "resultLimits": {
            "maximumItems": CLI_RESULT_MAXIMUM_ITEMS,
            "maximumBytes": CLI_RESULT_MAXIMUM_BYTES,
        },
    }))?;
    let result = product
        .application()
        .invoke("Portfolio.Import", arguments, context)
        .await
        .map_err(CliPortfolioImportError::Application)?;
    Ok(result_envelope(&result))
}

#[derive(Deserialize)]
struct PortfolioManifestRouting {
    schema_version: u16,
    dataset: String,
    object_id: String,
}

struct ExactRouting {
    dataset: SourceIdentifier,
    object_id: SourceIdentifier,
}

fn parse_routing(bytes: &[u8]) -> Result<ExactRouting, CliPortfolioImportError> {
    let routing: PortfolioManifestRouting =
        serde_json::from_slice(bytes).map_err(|_error| CliPortfolioImportError::InvalidManifest)?;
    if routing.schema_version != PORTFOLIO_MANIFEST_SCHEMA_VERSION {
        return Err(CliPortfolioImportError::InvalidManifest);
    }
    Ok(ExactRouting {
        dataset: SourceIdentifier::try_from(routing.dataset)
            .map_err(|_error| CliPortfolioImportError::InvalidManifest)?,
        object_id: SourceIdentifier::try_from(routing.object_id)
            .map_err(|_error| CliPortfolioImportError::InvalidManifest)?,
    })
}

fn portfolio_metadata(
    digest: EvidenceDigest,
    digest_hex: &str,
) -> Result<SourceMetadata, CliPortfolioImportError> {
    let short = digest_hex
        .get(..24)
        .ok_or(CliPortfolioImportError::InvalidManifest)?;
    let source_id = SourceId::try_from(format!("portfolio-manifest-{short}").as_str())
        .map_err(|_error| CliPortfolioImportError::Metadata)?;
    let revision = MetadataRevision::new(
        SourceIdentifier::try_from(format!("manifest-{short}").as_str())
            .map_err(|_error| CliPortfolioImportError::Metadata)?,
    );
    let provider = SourceIdentifier::try_from("user-owned-portfolio-export")
        .map_err(|_error| CliPortfolioImportError::Metadata)?;
    let basis = AuthorizationBasis::new(
        SourceIdentifier::try_from("user-owned-portfolio-manifest")
            .map_err(|_error| CliPortfolioImportError::Metadata)?,
    );
    let evidence = ExactPayloadEvidence::from_content_digest(digest);
    let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)
        .map_err(|_error| CliPortfolioImportError::Metadata)?;
    SourceMetadata::try_new(SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        source_id,
        RevisionBoundPayloadEvidence::new(revision, evidence.clone()),
        SourceClass::PortfolioExport,
        provider,
        AuthorizationGrant::new(
            AuthorizationMode::UserOwnedLocal,
            basis,
            evidence.clone(),
            effective,
        ),
        SourceCoverage::try_non_instrument(
            evidence,
            effective,
            CoverageDomain::Portfolio,
            CoverageDelay::Delayed(1),
            DeliveryEvidence::Unknown,
        )
        .map_err(|_error| CliPortfolioImportError::Metadata)?,
        DataQuality::DirectUnverified,
        NetworkAccessPolicy::Denied,
        FreshnessPolicy::try_new(1, 1, 1, 1, 0)
            .map_err(|_error| CliPortfolioImportError::Metadata)?,
        None,
        SourceCapabilities::new(
            false,
            true,
            SequenceCapability::Unsupported,
            ChecksumCapability::Unsupported,
            HistoricalCapability::RevisionPreserving,
            false,
        ),
        SourceProtocolProfile::NotLive,
    ))
    .map_err(|_error| CliPortfolioImportError::Metadata)
}

fn admitted_absolute_path(path: &Path) -> Result<PathBuf, CliPortfolioImportError> {
    let absolute =
        std::path::absolute(path).map_err(|_error| CliPortfolioImportError::InputUnavailable)?;
    if absolute
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return Err(CliPortfolioImportError::InputUnavailable);
    }
    Ok(absolute)
}

fn serialize_batch(
    batch: &market_squawk_sources::ExtractionBatch,
) -> Result<Vec<u8>, CliPortfolioImportError> {
    let mut writer = BoundedJsonWriter::new(PORTFOLIO_ARTIFACT_MAXIMUM_BYTES);
    serde_json::to_writer(&mut writer, batch)
        .map_err(|_error| CliPortfolioImportError::Serialization)?;
    Ok(writer.into_bytes())
}

fn persist_immutable(
    artifacts: &ArtifactRoot,
    reference: &str,
    bytes: &[u8],
) -> Result<(), CliPortfolioImportError> {
    let resolved = artifacts
        .resolve(reference)
        .map_err(|_error| CliPortfolioImportError::Artifact)?;
    match resolved.create_new() {
        Ok(mut file) => {
            file.write_all(bytes)
                .map_err(|_error| CliPortfolioImportError::Artifact)?;
            file.sync_all()
                .map_err(|_error| CliPortfolioImportError::Artifact)?;
            drop(file);
        }
        Err(ArtifactPathError::Io { source })
            if source.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(_error) => return Err(CliPortfolioImportError::Artifact),
    }
    verify_immutable_artifact(&resolved, bytes)
}

fn verify_immutable_artifact(
    resolved: &market_squawk_platform::ResolvedArtifactPath,
    expected: &[u8],
) -> Result<(), CliPortfolioImportError> {
    let file = resolved
        .open_read()
        .map_err(|_error| CliPortfolioImportError::Artifact)?;
    let metadata = file
        .metadata()
        .map_err(|_error| CliPortfolioImportError::Artifact)?;
    if usize::try_from(metadata.len()).ok() != Some(expected.len()) {
        return Err(CliPortfolioImportError::ArtifactReplayConflict);
    }
    let maximum = u64::try_from(expected.len())
        .ok()
        .and_then(|bytes| bytes.checked_add(1))
        .ok_or(CliPortfolioImportError::Artifact)?;
    let mut persisted = Vec::new();
    persisted
        .try_reserve_exact(expected.len())
        .map_err(|_error| CliPortfolioImportError::Artifact)?;
    file.take(maximum)
        .read_to_end(&mut persisted)
        .map_err(|_error| CliPortfolioImportError::Artifact)?;
    if persisted == expected {
        Ok(())
    } else {
        Err(CliPortfolioImportError::ArtifactReplayConflict)
    }
}

fn request_context() -> Result<RequestContext, CliPortfolioImportError> {
    let structure = JsonStructureLimits::try_new(32, 1024 * 1024, 100_000, 10_000)
        .map_err(|_error| CliPortfolioImportError::Limits)?;
    let limits = ServiceLimits::try_new(
        CLI_RESULT_MAXIMUM_BYTES,
        CLI_RESULT_MAXIMUM_ITEMS,
        CLI_HARD_MAXIMUM_BYTES,
        100_000,
        structure,
    )
    .map_err(|_error| CliPortfolioImportError::Limits)?;
    let request_id = RequestId::try_string(format!("cli-portfolio-{}", uuid::Uuid::new_v4()))
        .map_err(|_error| CliPortfolioImportError::Limits)?;
    let deadline = Instant::now()
        .checked_add(CLI_REQUEST_TIMEOUT)
        .ok_or(CliPortfolioImportError::Limits)?;
    Ok(RequestContext::new(
        request_id,
        CancellationToken::new(),
        deadline,
        limits,
    ))
}

fn ensure_live(context: &RequestContext) -> Result<(), CliPortfolioImportError> {
    if context.cancellation().is_cancelled() {
        Err(CliPortfolioImportError::Extraction(ServiceError::Cancelled))
    } else if Instant::now() >= context.deadline() {
        Err(CliPortfolioImportError::Extraction(
            ServiceError::DeadlineExceeded,
        ))
    } else {
        Ok(())
    }
}

fn json_object(value: Value) -> Result<Map<String, Value>, CliPortfolioImportError> {
    value
        .as_object()
        .cloned()
        .ok_or(CliPortfolioImportError::Serialization)
}

fn result_envelope(result: &TypedToolResult) -> Value {
    let metadata = result.metadata();
    json!({
        "data": result.structured_content(),
        "metadata": {
            "completeness": metadata.completeness(),
            "returnedItems": result.item_count(),
            "availableItems": metadata.available_items().unwrap_or(result.item_count()),
            "sourceCoverage": metadata.source_coverage(),
            "dataQuality": metadata.data_quality(),
            "sourceEvidence": metadata.source_evidence(),
        },
        "encodedBytes": result.encoded_bytes(),
    })
}

fn hex(bytes: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(64);
    for byte in bytes {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    value
}

struct BoundedJsonWriter {
    bytes: Vec<u8>,
    maximum: usize,
}

impl BoundedJsonWriter {
    const fn new(maximum: usize) -> Self {
        Self {
            bytes: Vec::new(),
            maximum,
        }
    }

    fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

impl io::Write for BoundedJsonWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let required = self
            .bytes
            .len()
            .checked_add(buffer.len())
            .filter(|required| *required <= self.maximum)
            .ok_or_else(|| io::Error::other("portfolio artifact exceeded its byte ceiling"))?;
        self.bytes
            .try_reserve(required.saturating_sub(self.bytes.len()))
            .map_err(|_error| io::Error::other("portfolio artifact allocation failed"))?;
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Portfolio CLI admission or product-boundary failure.
#[derive(Debug, Error)]
pub enum CliPortfolioImportError {
    /// Mutation confirmation was absent.
    #[error("portfolio import requires explicit confirmation")]
    ConfirmationRequired,
    /// Destination account identity was malformed.
    #[error("portfolio import account identity is invalid")]
    InvalidAccount,
    /// The selected manifest was not a confined bounded regular file.
    #[error("portfolio manifest is unavailable")]
    InputUnavailable,
    /// Routing fields or the manifest version were malformed.
    #[error("portfolio manifest routing is invalid")]
    InvalidManifest,
    /// Portfolio source metadata could not represent the manifest authority.
    #[error("portfolio source metadata is invalid")]
    Metadata,
    /// Durable controlled source state was unavailable.
    #[error("portfolio source authority is unavailable")]
    AuthorityUnavailable,
    /// Local provider onboarding did not admit the portfolio profile.
    #[error("portfolio source onboarding failed")]
    Onboarding,
    /// The provider activation did not produce the exact research adapter.
    #[error("portfolio source activation failed")]
    Activation,
    /// Registered extraction failed its authority, rights, bounds, cancellation, or deadline gate.
    #[error("portfolio extraction failed: {0}")]
    Extraction(ServiceError),
    /// The exact extraction batch could not be encoded within the artifact ceiling.
    #[error("portfolio extraction artifact serialization failed")]
    Serialization,
    /// Controlled immutable artifact access or publication failed.
    #[error("portfolio extraction artifact is unavailable")]
    Artifact,
    /// An existing content-addressed artifact did not contain the exact expected bytes.
    #[error("portfolio extraction artifact replay conflicts with retained bytes")]
    ArtifactReplayConflict,
    /// Shared application import admission or publication failed.
    #[error("portfolio application import failed: {0}")]
    Application(ServiceError),
    /// CLI-owned request or output ceilings were invalid.
    #[error("portfolio CLI limits are invalid")]
    Limits,
}
