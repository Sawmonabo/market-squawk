//! Bounded logical-original custody through the existing shared catalog and raw store.

use super::*;
use crate::{ProviderLogicalOriginalReceipt, ProviderLogicalPublicationOrigin};
use market_squawk_platform::SealedResearchJournalStore;
use market_squawk_sources::SealedLogicalObjectInput;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LogicalOriginalSourceRevisionKind {
    Metadata,
    ContractPayload,
}
impl LogicalOriginalSourceRevisionKind {
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Metadata => "metadata",
            Self::ContractPayload => "contract_payload",
        }
    }
    pub(crate) fn digest(
        self,
        metadata: &SourceMetadata,
        registered: EvidenceDigest,
    ) -> EvidenceDigest {
        match self {
            Self::Metadata => registered,
            Self::ContractPayload => metadata
                .revision_evidence()
                .payload_evidence()
                .content_digest(),
        }
    }
}

impl AnalyticalDataService {
    /// Retains an already sealed original under exact source Persist rights. The producer holds
    /// the existing analytical operation lease continuously from raw sealing through publication;
    /// this synchronous method runs on the retained research I/O worker without reacquiring it.
    #[allow(clippy::too_many_arguments)]
    pub fn retain_provider_logical_original(
        &self,
        metadata: &SourceMetadata,
        dataset: &DatasetId,
        native_schema: EvidenceDigest,
        original_digest: EvidenceDigest,
        received_at: Timestamp,
        checkpoint: &[u8],
        objects: &[SealedLogicalObjectInput],
        rights: &RightsDecisionInput,
        store: &SealedResearchJournalStore,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<ProviderLogicalOriginalReceipt, IngestError> {
        self.retain_provider_logical_original_with_revision(
            metadata,
            dataset,
            native_schema,
            LogicalOriginalSourceRevisionKind::Metadata,
            original_digest,
            received_at,
            checkpoint,
            objects,
            rights,
            store,
            deadline,
            cancellation,
        )
    }

    /// Retains a logical original using the exact source contract revision payload identity.
    /// This derives authority from the supplied typed metadata, never a caller-provided digest.
    #[allow(clippy::too_many_arguments)]
    pub fn retain_provider_logical_original_for_contract(
        &self,
        metadata: &SourceMetadata,
        dataset: &DatasetId,
        native_schema: EvidenceDigest,
        original_digest: EvidenceDigest,
        received_at: Timestamp,
        checkpoint: &[u8],
        objects: &[SealedLogicalObjectInput],
        rights: &RightsDecisionInput,
        store: &SealedResearchJournalStore,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<ProviderLogicalOriginalReceipt, IngestError> {
        self.retain_provider_logical_original_with_revision(
            metadata,
            dataset,
            native_schema,
            LogicalOriginalSourceRevisionKind::ContractPayload,
            original_digest,
            received_at,
            checkpoint,
            objects,
            rights,
            store,
            deadline,
            cancellation,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn retain_provider_logical_original_with_revision(
        &self,
        metadata: &SourceMetadata,
        dataset: &DatasetId,
        native_schema: EvidenceDigest,
        revision_kind: LogicalOriginalSourceRevisionKind,
        original_digest: EvidenceDigest,
        received_at: Timestamp,
        checkpoint: &[u8],
        objects: &[SealedLogicalObjectInput],
        rights: &RightsDecisionInput,
        store: &SealedResearchJournalStore,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<ProviderLogicalOriginalReceipt, IngestError> {
        check_market_event_read(deadline, cancellation)?;
        let registered_revision = EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            Sha256::digest(serde_json::to_vec(metadata)?).into(),
        );
        let revision = revision_kind.digest(metadata, registered_revision);
        if rights.source_id != *metadata.source_id()
            || rights.payload_digest != original_digest
            || !rights
                .permitted_operations
                .contains(&SourceOperation::Persist)
            || checkpoint.is_empty()
            || checkpoint.len() > crate::catalog::MAX_PROVIDER_LOGICAL_ORIGINAL_CHECKPOINT_BYTES
            || objects.is_empty()
            || objects.len() > market_squawk_sources::MAX_PROVIDER_LOGICAL_OBJECTS
        {
            return Err(IngestError::ReservationPayloadMismatch);
        }
        let control = MarketEventReadControl {
            deadline,
            cancellation,
        };
        for object in objects {
            store
                .open_verified_logical_object(object.object(), &control)
                .map_err(map_provider_recovery_store_error)?
                .reverify_for_commit(&control)
                .map_err(map_provider_recovery_store_error)?;
        }
        let authority = self.market_recovery_authority(deadline, cancellation)?;
        authority
            .catalog()
            .market_recovery_read(deadline, cancellation, || {
                if authority.source(metadata.source_id())?.as_ref() != Some(metadata) {
                    authority.register_source(metadata, received_at)?;
                }
                let grant = authority.admit_source_rights(rights.clone())?;
                authority.retain_provider_logical_original(
                    dataset,
                    metadata.source_id(),
                    native_schema,
                    revision,
                    registered_revision,
                    revision_kind,
                    original_digest,
                    received_at,
                    checkpoint,
                    objects,
                    &grant,
                    deadline,
                    cancellation,
                )
            })
            .map_err(map_market_recovery_catalog_error)
    }

    /// Returns a pending original, or the exact specified original including its publication
    /// relation. A missing pending original never silently selects a newer or published one.
    #[allow(clippy::too_many_arguments)]
    pub fn provider_logical_original(
        &self,
        dataset: &DatasetId,
        source: &SourceId,
        native_schema: EvidenceDigest,
        source_revision: EvidenceDigest,
        original_digest: Option<EvidenceDigest>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<ProviderLogicalOriginalReceipt>, IngestError> {
        let authority = self.market_recovery_authority(deadline, cancellation)?;
        authority
            .catalog()
            .market_recovery_read(deadline, cancellation, || {
                authority.provider_logical_original(
                    dataset,
                    source,
                    native_schema,
                    source_revision,
                    original_digest,
                    deadline,
                    cancellation,
                )
            })
            .map_err(map_market_recovery_catalog_error)
    }

    /// Lists bounded original creating generations by descending version at the exact knowledge
    /// cutoff. These are inert locators; the canonical reader separately authorizes serving use.
    #[allow(clippy::too_many_arguments)]
    pub fn provider_logical_origin_candidates(
        &self,
        dataset: &DatasetId,
        source: &SourceId,
        native_schema: EvidenceDigest,
        knowledge_cutoff: Timestamp,
        before_version: Option<u64>,
        limit: usize,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(Vec<ProviderLogicalPublicationOrigin>, bool), IngestError> {
        let snapshot = self
            .manifests
            .read_snapshot(self.catalog_read_limits, deadline, cancellation)
            .map_err(map_market_recovery_catalog_error)?;
        snapshot
            .read(|snapshot| {
                snapshot.provider_logical_origins(
                    dataset,
                    source,
                    native_schema,
                    knowledge_cutoff,
                    before_version,
                    limit,
                    None,
                    deadline,
                    cancellation,
                )
            })
            .map_err(map_market_recovery_catalog_error)
    }

    /// Reopens only the exact creating generation identified by logical binding and content hash.
    #[allow(clippy::too_many_arguments)]
    pub fn provider_logical_origin(
        &self,
        dataset: &DatasetId,
        source: &SourceId,
        native_schema: EvidenceDigest,
        binding_digest: EvidenceDigest,
        content: Sha256Digest,
        knowledge_cutoff: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<ProviderLogicalPublicationOrigin>, IngestError> {
        let snapshot = self
            .manifests
            .read_snapshot(self.catalog_read_limits, deadline, cancellation)
            .map_err(map_market_recovery_catalog_error)?;
        let (mut origins, has_more) = snapshot
            .read(|snapshot| {
                snapshot.provider_logical_origins(
                    dataset,
                    source,
                    native_schema,
                    knowledge_cutoff,
                    None,
                    1,
                    Some((binding_digest, content)),
                    deadline,
                    cancellation,
                )
            })
            .map_err(map_market_recovery_catalog_error)?;
        if has_more {
            return Err(IngestError::ReplayConflict);
        }
        Ok(origins.pop())
    }
}
