//! User-authorized local-file activation and ingestion boundary.

use std::path::{Path, PathBuf};

use market_squawk_adapter_files::{ExtractionLimits, ExtractionLimitsInput};
use market_squawk_domain::{
    AuthorizationBasis, ChecksumCapability, CoverageDelay, DataQuality, DeliveryEvidence,
    EffectiveInterval, ExactPayloadEvidence, MetadataRevision, RevisionBoundPayloadEvidence,
    SchemaVersion, SequenceCapability, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_platform::UserAuthorizedInputRoot;
use market_squawk_sources::{
    AuthorizationGrant, AuthorizationMode, CoverageDomain, FreshnessPolicy, HistoricalCapability,
    NetworkAccessPolicy, SourceCapabilities, SourceClass, SourceCoverage, SourceMetadata,
    SourceMetadataInput, SourceProtocolProfile,
};
use serde_json::json;
use tokio_util::sync::CancellationToken;

use crate::{
    LocalFileAdapterActivation, ProviderActivationOutcome, ProviderAdapterActivationRequest,
    StartOnboardingRequest,
};

use super::{
    CliProductError, CliProductResult, LocalProduct, admitted_absolute_path, hex, invoke,
    json_object,
};

pub(super) async fn ingest_local_file(
    product: &LocalProduct,
    manifest_path: &Path,
    object: String,
    dataset: String,
    confirm: bool,
) -> Result<CliProductResult, CliProductError> {
    if !confirm {
        return Err(CliProductError::Application(
            market_squawk_services::ServiceError::InvalidRequest,
        ));
    }
    let absolute = admitted_absolute_path(manifest_path)?;
    let root_path = absolute.parent().ok_or(CliProductError::RequestFile)?;
    let manifest_name = absolute.file_name().ok_or(CliProductError::RequestFile)?;
    let (root, ownership) = UserAuthorizedInputRoot::open_with_ownership_authority(root_path)
        .map_err(|_| CliProductError::RequestFile)?;
    let limits = ExtractionLimits::try_new(ExtractionLimitsInput::standard())
        .map_err(|_| CliProductError::RequestShape)?;
    let manifest = root
        .resolve(PathBuf::from(manifest_name))
        .and_then(|file| file.open_bounded(4 * 1024 * 1024))
        .and_then(|file| file.read_bounded())
        .map_err(|_| CliProductError::RequestFile)?;
    let ownership_evidence = ownership
        .issue_manifest_evidence(&manifest)
        .map_err(|_| CliProductError::RequestFile)?;
    let digest = manifest.digest();
    let digest_hex = hex(&digest.bytes());
    let metadata = local_file_metadata(digest, &digest_hex)?;
    let representation_state = product
        .paths()
        .control_root()
        .map_err(|_| CliProductError::RequestFile)?
        .root()
        .join("sources/file-representations")
        .join(&digest_hex);

    let onboarding = product.provider_onboarding();
    let session = onboarding
        .start(
            StartOnboardingRequest::try_new("local.files", None, None)
                .map_err(|_| CliProductError::RequestShape)?,
            CancellationToken::new(),
        )
        .await
        .map_err(|_| CliProductError::RequestShape)?;
    let activation = LocalFileAdapterActivation::new(
        metadata,
        root,
        representation_state,
        manifest,
        limits,
        ownership_evidence,
    );
    let activated = product
        .provider_activation()
        .activate_ready_profile(
            session.session_id(),
            ProviderAdapterActivationRequest::LocalFiles(activation),
            CancellationToken::new(),
        )
        .await
        .map_err(|_| CliProductError::RequestShape)?;
    let ProviderActivationOutcome::Research(activated) = activated else {
        return Err(CliProductError::RequestShape);
    };
    let provider = activated.profile().as_str().to_owned();
    let mut discovery_arguments = json_object(json!({
        "provider": provider,
        "dataset": dataset,
        "confirm": confirm,
        "sourceCoverage": [provider],
    }))?;
    let discovery = invoke(
        product,
        "Source.Discover",
        &mut discovery_arguments,
        None,
        "local manifest objects discovered",
    )
    .await?;
    let discovered_objects = discovery
        .value()
        .pointer("/data/objects")
        .and_then(serde_json::Value::as_array)
        .ok_or(CliProductError::Application(
            market_squawk_services::ServiceError::InvalidResult,
        ))?;
    let mut matches = discovered_objects.iter().filter(|candidate| {
        candidate
            .get("object_id")
            .and_then(serde_json::Value::as_str)
            == Some(object.as_str())
            && candidate.get("dataset").and_then(serde_json::Value::as_str)
                == Some(dataset.as_str())
    });
    let selected = matches.next().ok_or(CliProductError::Application(
        market_squawk_services::ServiceError::NotFound,
    ))?;
    if matches.next().is_some() {
        return Err(CliProductError::Application(
            market_squawk_services::ServiceError::InvalidResult,
        ));
    }
    let discovery_receipt = selected
        .get("discovery_receipt")
        .and_then(serde_json::Value::as_str)
        .ok_or(CliProductError::Application(
            market_squawk_services::ServiceError::InvalidResult,
        ))?;
    let mut arguments = json_object(json!({
        "provider": provider,
        "object": object,
        "dataset": dataset,
        "discoveryReceipt": discovery_receipt,
        "confirm": confirm,
        "sourceCoverage": [provider],
    }))?;
    invoke(
        product,
        "Research.IngestSource",
        &mut arguments,
        None,
        "local manifest object ingested",
    )
    .await
}

fn local_file_metadata(
    digest: market_squawk_domain::EvidenceDigest,
    digest_hex: &str,
) -> Result<SourceMetadata, CliProductError> {
    let short = digest_hex.get(..24).ok_or(CliProductError::RequestShape)?;
    let source_id = SourceId::try_from(format!("local-files-{short}").as_str())
        .map_err(|_| CliProductError::RequestShape)?;
    let revision = MetadataRevision::new(
        SourceIdentifier::try_from(format!("manifest-{short}").as_str())
            .map_err(|_| CliProductError::RequestShape)?,
    );
    let provider = SourceIdentifier::try_from("user-owned-local-files")
        .map_err(|_| CliProductError::RequestShape)?;
    let basis = AuthorizationBasis::new(
        SourceIdentifier::try_from("user-owned-file").map_err(|_| CliProductError::RequestShape)?,
    );
    let evidence = ExactPayloadEvidence::from_content_digest(digest);
    let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)
        .map_err(|_| CliProductError::RequestShape)?;
    SourceMetadata::try_new(SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        source_id,
        RevisionBoundPayloadEvidence::new(revision, evidence.clone()),
        SourceClass::LocalFile,
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
            CoverageDomain::AlternativeData,
            CoverageDelay::Delayed(1),
            DeliveryEvidence::Unknown,
        )
        .map_err(|_| CliProductError::RequestShape)?,
        DataQuality::DirectUnverified,
        NetworkAccessPolicy::Denied,
        FreshnessPolicy::try_new(1, 1, 1, 1, 0).map_err(|_| CliProductError::RequestShape)?,
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
    .map_err(|_| CliProductError::RequestShape)
}
