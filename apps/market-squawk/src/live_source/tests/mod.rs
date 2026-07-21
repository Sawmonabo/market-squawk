mod kraken_vertical;
mod pipeline;
mod sink;
mod subscription;
mod supervisor;

use market_squawk_sources::{
    AuthorizationGrant, AuthorizationMode, NetworkAccessPolicy, SourceClass, SourceMetadata,
    SourceMetadataInput,
};

type SharedTestResult<T> = Result<T, Box<dyn std::error::Error>>;

fn budget_free_metadata(metadata: &SourceMetadata) -> SharedTestResult<SourceMetadata> {
    Ok(SourceMetadata::try_new(SourceMetadataInput::new(
        metadata.schema_version(),
        metadata.source_id().clone(),
        metadata.revision_evidence().clone(),
        SourceClass::LocalFile,
        metadata.provider().clone(),
        AuthorizationGrant::new(
            AuthorizationMode::UserOwnedLocal,
            metadata.authorization().basis().clone(),
            metadata.authorization().evidence().clone(),
            metadata.authorization().effective_interval(),
        ),
        metadata.coverage().clone(),
        metadata.quality_ceiling(),
        NetworkAccessPolicy::Denied,
        metadata.freshness_policy(),
        None,
        metadata.capabilities(),
        metadata.protocol_profile().clone(),
    ))?)
}
