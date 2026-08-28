//! Application-owned SEC fund logical publication and exact PIT restart bridge.
//!
//! The SEC adapter prepares one noncloneable filing-scoped graph whose raw objects and aligned
//! native/row-map partitions are already sealed in the application-owned research store. This
//! leaf consumes that graph once, closes it against the registered canonical fund schema, and
//! publishes the canonical records and logical evidence in one analytical transaction. Reads are
//! pinned to the returned manifest and binding digest; neither family permits latest-generation
//! or cross-family substitution.

use std::sync::Arc;

use market_squawk_adapter_sec::{SecFundPublicationScope, SecPreparedFundLogicalPublication};
use market_squawk_data::{
    CommittedDataset, DatasetId, DatasetManifestRef, DatasetSchemaError, DatasetSchemaRegistry,
    FundPointInTimeRequest, FundPointInTimeRevisionMode, FundPointInTimeSelection, IngestError,
    IngestIdentity, IngestPrecommitAuthority, RightsError, SourceOperation,
};
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, FundEvidenceRecord, FundSourceFamily, InstrumentId,
    SourceIdentifier, Timestamp,
};
use market_squawk_services::ServiceError;
use market_squawk_sources::{
    CanonicalPartitionExpectation, LogicalPartitionFamily, ProviderLogicalPublicationError,
    SealedProviderLogicalPublicationBinding, SourceMetadata,
};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::ResearchRightsAuthority;
use crate::ResearchService;

const SEC_SOURCE_ID: &str = "sec-edgar";
const SEC_FUND_INGEST_IDENTITY_DOMAIN: &[u8] =
    b"market-squawk/application/sec-fund-logical-ingest/v1";

/// Fixed typed operation for an exact-manifest N-PORT fund-evidence read.
pub(crate) const SEC_NPORT_FUND_POINT_IN_TIME_OPERATION: &str =
    "Research.GetSecNportFundPointInTime";

/// Fixed typed operation for an exact-manifest N-CEN fund-evidence read.
pub(crate) const SEC_NCEN_FUND_POINT_IN_TIME_OPERATION: &str = "Research.GetSecNcenFundPointInTime";

/// Application closure binding SEC source/rights authority to the sole analytical/raw store.
pub(crate) struct SecFundApplicationBridge {
    research: Arc<ResearchService>,
    source: SourceMetadata,
    rights: ResearchRightsAuthority,
    source_registered_at: Timestamp,
}

impl std::fmt::Debug for SecFundApplicationBridge {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SecFundApplicationBridge")
            .field("source_id", self.source.source_id())
            .field("metadata_revision", self.source.revision())
            .field("source_registered_at", &self.source_registered_at)
            .finish_non_exhaustive()
    }
}

impl SecFundApplicationBridge {
    /// Composes the exact SEC source declaration, persistence rights, and application stores.
    pub(crate) fn try_new(
        research: Arc<ResearchService>,
        source: SourceMetadata,
        rights: ResearchRightsAuthority,
        source_registered_at: Timestamp,
    ) -> Result<Self, SecFundApplicationError> {
        if source.source_id().as_str() != SEC_SOURCE_ID
            || source.source_id() != rights.source_id()
            || !source.is_effective_at(source_registered_at)
        {
            return Err(SecFundApplicationError::AuthorityInvalid);
        }
        Ok(Self {
            research,
            source,
            rights,
            source_registered_at,
        })
    }

    /// Consumes and atomically publishes one exact N-PORT or N-CEN logical preparation.
    ///
    /// The common binding owns the adapter's live sealed receipts. Its digest is both the rights
    /// payload and ingest payload, so a replay can reconcile only the same immutable generation.
    pub(crate) async fn publish(
        &self,
        prepared: SecPreparedFundLogicalPublication,
        analytical_dataset: DatasetId,
        observed_at: Timestamp,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
        cancellation: CancellationToken,
    ) -> Result<SecFundPublicationReceipt, SecFundApplicationError> {
        self.validate_current_authority(observed_at, precommit_authority.as_ref())?;

        let (terminal, scope, objects, partitions, canonical, preparation_digest) =
            prepared.into_parts();
        if terminal.source_id != *self.source.source_id() {
            return Err(SecFundApplicationError::AuthorityInvalid);
        }

        let schema = DatasetSchemaRegistry::local().canonical_fund_holdings()?;
        let canonical_schema_identity =
            EvidenceDigest::new(DigestAlgorithm::Sha256, schema.fingerprint());
        let expected_rows = usize::try_from(terminal.total_canonical_rows)
            .map_err(|_| SecFundApplicationError::AllocationFailed)?;
        let prepared_rows = canonical.iter().try_fold(0usize, |total, partition| {
            total
                .checked_add(partition.records().len())
                .ok_or(SecFundApplicationError::AllocationFailed)
        })?;
        if prepared_rows != expected_rows {
            return Err(SecFundApplicationError::FamilyMismatch);
        }
        let mut expectations = Vec::new();
        expectations
            .try_reserve_exact(canonical.len())
            .map_err(|_| SecFundApplicationError::AllocationFailed)?;
        let mut records = Vec::<FundEvidenceRecord>::new();
        records
            .try_reserve_exact(expected_rows)
            .map_err(|_| SecFundApplicationError::AllocationFailed)?;
        for partition in &canonical {
            expectations.push(CanonicalPartitionExpectation::try_new(
                partition.partition_ordinal(),
                partition.row_range(),
                canonical_schema_identity,
                partition.typed_input_digest(),
                partition.partition_ordinal(),
                partition.partition_ordinal(),
            )?);
            records.extend(partition.records().iter().cloned());
        }
        if records.len() != expected_rows {
            return Err(SecFundApplicationError::FamilyMismatch);
        }
        let fund_instrument_id = records
            .first()
            .map(sec_fund_record_instrument_id)
            .ok_or(SecFundApplicationError::FamilyMismatch)?;
        if records
            .iter()
            .any(|record| sec_fund_record_instrument_id(record) != fund_instrument_id)
        {
            return Err(SecFundApplicationError::FamilyMismatch);
        }

        let binding = SealedProviderLogicalPublicationBinding::try_new(
            terminal,
            &[
                LogicalPartitionFamily::ProviderNative,
                LogicalPartitionFamily::CanonicalRowMap,
            ],
            objects.into_vec(),
            partitions.into_vec(),
            expectations,
        )?;
        let binding_digest = binding.binding_digest();
        let identity = IngestIdentity::try_new(
            self.source.source_id().clone(),
            binding_digest,
            SourceOperation::Persist,
            sec_fund_ingest_identity(&analytical_dataset, binding_digest)?,
        )?;
        let rights = self.rights.decision(binding_digest, observed_at)?;
        let reservation = self
            .research
            .analytical()
            .reserve_source_ingest(
                &self.source,
                self.source_registered_at,
                rights,
                &identity,
                &cancellation,
            )
            .await?;
        let (committed, retained_binding_digest) = self
            .research
            .analytical()
            .ingest_sec_fund_logical_publication(
                reservation,
                analytical_dataset,
                binding,
                records,
                cancellation,
                precommit_authority,
            )
            .await?;
        if retained_binding_digest != binding_digest {
            return Err(SecFundApplicationError::RestartInvalid);
        }

        let binding = SecFundRestartBinding {
            manifest: committed.manifest().clone(),
            binding_digest,
            preparation_digest,
            fund_instrument_id,
        };
        Ok(match scope {
            SecFundPublicationScope::Nport { accession } => {
                SecFundPublicationReceipt::Nport(SecNportFundPublicationReceipt {
                    committed,
                    restart: SecNportFundRestartSelector { binding, accession },
                })
            }
            SecFundPublicationScope::Ncen { accession, fund_id } => {
                SecFundPublicationReceipt::Ncen(SecNcenFundPublicationReceipt {
                    committed,
                    restart: SecNcenFundRestartSelector {
                        binding,
                        accession,
                        fund_id,
                    },
                })
            }
        })
    }

    fn validate_current_authority(
        &self,
        observed_at: Timestamp,
        precommit_authority: &dyn IngestPrecommitAuthority,
    ) -> Result<(), SecFundApplicationError> {
        if observed_at < self.source_registered_at || !self.source.is_effective_at(observed_at) {
            return Err(SecFundApplicationError::AuthorityInvalid);
        }
        precommit_authority.validate_precommit()?;
        Ok(())
    }
}

/// Exact immutable SEC fund publication, retaining its filing-family-specific restart selector.
#[derive(Debug)]
pub(crate) enum SecFundPublicationReceipt {
    Nport(SecNportFundPublicationReceipt),
    Ncen(SecNcenFundPublicationReceipt),
}

/// One immutable N-PORT generation and its exact raw/logical restart coordinate.
#[derive(Debug)]
pub(crate) struct SecNportFundPublicationReceipt {
    committed: CommittedDataset,
    restart: SecNportFundRestartSelector,
}

impl SecNportFundPublicationReceipt {
    pub(crate) const fn committed(&self) -> &CommittedDataset {
        &self.committed
    }

    pub(crate) const fn restart_selector(&self) -> &SecNportFundRestartSelector {
        &self.restart
    }
}

/// One immutable N-CEN generation and its exact raw/logical restart coordinate.
#[derive(Debug)]
pub(crate) struct SecNcenFundPublicationReceipt {
    committed: CommittedDataset,
    restart: SecNcenFundRestartSelector,
}

impl SecNcenFundPublicationReceipt {
    pub(crate) const fn committed(&self) -> &CommittedDataset {
        &self.committed
    }

    pub(crate) const fn restart_selector(&self) -> &SecNcenFundRestartSelector {
        &self.restart
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SecFundRestartBinding {
    manifest: DatasetManifestRef,
    binding_digest: EvidenceDigest,
    preparation_digest: EvidenceDigest,
    fund_instrument_id: InstrumentId,
}

impl SecFundRestartBinding {
    async fn read(
        &self,
        research: &ResearchService,
        request: &FundPointInTimeRequest,
        family: FundSourceFamily,
        accession: &SourceIdentifier,
        cancellation: CancellationToken,
    ) -> Result<FundPointInTimeSelection, SecFundApplicationError> {
        self.validate_request(request, family, accession)?;
        let store = research.provider_capture_store();
        let selection = research
            .analytical()
            .read_sec_fund_point_in_time(request, self.binding_digest, store.as_ref(), cancellation)
            .await?;
        if selection.manifest() != &self.manifest {
            return Err(SecFundApplicationError::RestartInvalid);
        }
        Ok(selection)
    }

    async fn verify_restart(
        &self,
        research: &ResearchService,
        request: &FundPointInTimeRequest,
        original: &FundPointInTimeSelection,
        family: FundSourceFamily,
        accession: &SourceIdentifier,
        cancellation: CancellationToken,
    ) -> Result<FundPointInTimeSelection, SecFundApplicationError> {
        self.validate_request(request, family, accession)?;
        if original.manifest() != &self.manifest {
            return Err(SecFundApplicationError::RestartInvalid);
        }
        let store = research.provider_capture_store();
        let replay = research
            .analytical()
            .verify_sec_fund_point_in_time_restart(
                request,
                self.binding_digest,
                original,
                store.as_ref(),
                cancellation,
            )
            .await?;
        if replay.manifest() != &self.manifest {
            return Err(SecFundApplicationError::RestartInvalid);
        }
        Ok(replay)
    }

    fn validate_request(
        &self,
        request: &FundPointInTimeRequest,
        family: FundSourceFamily,
        accession: &SourceIdentifier,
    ) -> Result<(), SecFundApplicationError> {
        if request.exact_manifest() != Some(&self.manifest)
            || request.dataset() != self.manifest.dataset_id()
            || request.fund_instrument_id() != self.fund_instrument_id
            || request.source_family() != Some(family)
            || matches!(
                request.revision_mode(),
                FundPointInTimeRevisionMode::AsFiled(requested) if requested != accession
            )
        {
            return Err(SecFundApplicationError::RestartInvalid);
        }
        Ok(())
    }
}

/// Exact N-PORT filing scope, immutable generation, and logical-publication binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SecNportFundRestartSelector {
    binding: SecFundRestartBinding,
    accession: SourceIdentifier,
}

impl SecNportFundRestartSelector {
    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        &self.binding.manifest
    }

    pub(crate) const fn binding_digest(&self) -> EvidenceDigest {
        self.binding.binding_digest
    }

    pub(crate) const fn preparation_digest(&self) -> EvidenceDigest {
        self.binding.preparation_digest
    }

    pub(crate) const fn accession(&self) -> &SourceIdentifier {
        &self.accession
    }

    pub(crate) const fn fund_instrument_id(&self) -> InstrumentId {
        self.binding.fund_instrument_id
    }

    /// Reads only this immutable N-PORT generation and re-verifies its sealed raw graph.
    pub(crate) async fn read_point_in_time(
        &self,
        research: &ResearchService,
        request: &FundPointInTimeRequest,
        cancellation: CancellationToken,
    ) -> Result<FundPointInTimeSelection, SecFundApplicationError> {
        self.binding
            .read(
                research,
                request,
                FundSourceFamily::Nport,
                &self.accession,
                cancellation,
            )
            .await
    }

    /// Repeats the same N-PORT read after restart and rejects manifest or outcome drift.
    pub(crate) async fn verify_restart(
        &self,
        research: &ResearchService,
        request: &FundPointInTimeRequest,
        original: &FundPointInTimeSelection,
        cancellation: CancellationToken,
    ) -> Result<FundPointInTimeSelection, SecFundApplicationError> {
        self.binding
            .verify_restart(
                research,
                request,
                original,
                FundSourceFamily::Nport,
                &self.accession,
                cancellation,
            )
            .await
    }
}

/// Exact N-CEN filing/fund scope, immutable generation, and logical-publication binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SecNcenFundRestartSelector {
    binding: SecFundRestartBinding,
    accession: SourceIdentifier,
    fund_id: SourceIdentifier,
}

impl SecNcenFundRestartSelector {
    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        &self.binding.manifest
    }

    pub(crate) const fn binding_digest(&self) -> EvidenceDigest {
        self.binding.binding_digest
    }

    pub(crate) const fn preparation_digest(&self) -> EvidenceDigest {
        self.binding.preparation_digest
    }

    pub(crate) const fn accession(&self) -> &SourceIdentifier {
        &self.accession
    }

    pub(crate) const fn fund_id(&self) -> &SourceIdentifier {
        &self.fund_id
    }

    pub(crate) const fn fund_instrument_id(&self) -> InstrumentId {
        self.binding.fund_instrument_id
    }

    /// Reads only this immutable N-CEN generation and re-verifies its sealed raw graph.
    pub(crate) async fn read_point_in_time(
        &self,
        research: &ResearchService,
        request: &FundPointInTimeRequest,
        cancellation: CancellationToken,
    ) -> Result<FundPointInTimeSelection, SecFundApplicationError> {
        self.binding
            .read(
                research,
                request,
                FundSourceFamily::Ncen,
                &self.accession,
                cancellation,
            )
            .await
    }

    /// Repeats the same N-CEN read after restart and rejects manifest or outcome drift.
    pub(crate) async fn verify_restart(
        &self,
        research: &ResearchService,
        request: &FundPointInTimeRequest,
        original: &FundPointInTimeSelection,
        cancellation: CancellationToken,
    ) -> Result<FundPointInTimeSelection, SecFundApplicationError> {
        self.binding
            .verify_restart(
                research,
                request,
                original,
                FundSourceFamily::Ncen,
                &self.accession,
                cancellation,
            )
            .await
    }
}

fn sec_fund_record_instrument_id(record: &FundEvidenceRecord) -> InstrumentId {
    match record {
        FundEvidenceRecord::Report(value) => value.filing().fund().instrument_id(),
        FundEvidenceRecord::ShareClass(value) => value.filing().fund().instrument_id(),
        FundEvidenceRecord::PortfolioHolding(value) => value.filing().fund().instrument_id(),
    }
}

fn sec_fund_ingest_identity(
    dataset: &DatasetId,
    binding_digest: EvidenceDigest,
) -> Result<String, SecFundApplicationError> {
    let mut digest = Sha256::new();
    digest.update(SEC_FUND_INGEST_IDENTITY_DOMAIN);
    let dataset_length = u64::try_from(dataset.as_str().len())
        .map_err(|_| SecFundApplicationError::IdentityOverflow)?;
    digest.update(dataset_length.to_be_bytes());
    digest.update(dataset.as_str().as_bytes());
    digest.update(binding_digest.bytes());
    Ok(format!(
        "sec-fund-v1-{}",
        encode_lower_hex(digest.finalize().into())
    ))
}

fn encode_lower_hex(bytes: [u8; 32]) -> String {
    use std::fmt::Write as _;

    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

/// Failure before SEC logical closure, atomic publication, exact PIT selection, or restart proof.
#[derive(Debug, Error)]
pub(crate) enum SecFundApplicationError {
    #[error("SEC fund source or persistence authority is invalid")]
    AuthorityInvalid,
    #[error("SEC fund filing family or canonical row closure does not match")]
    FamilyMismatch,
    #[error("SEC fund bounded publication allocation failed")]
    AllocationFailed,
    #[error("SEC fund ingest identity length overflow")]
    IdentityOverflow,
    #[error("SEC fund exact restart selector did not reproduce its immutable generation")]
    RestartInvalid,
    #[error("SEC fund registered canonical schema is unavailable")]
    Schema(#[from] DatasetSchemaError),
    #[error("SEC fund common logical publication closure failed")]
    Logical(#[from] ProviderLogicalPublicationError),
    #[error("SEC fund ingest identity is invalid")]
    Rights(#[from] RightsError),
    #[error("SEC fund persistence rights are unavailable")]
    RightsDecision(#[from] ServiceError),
    #[error("SEC fund analytical publication or exact read failed")]
    Ingest(#[from] IngestError),
}
