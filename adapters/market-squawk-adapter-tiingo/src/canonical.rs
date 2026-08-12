use std::num::NonZeroU64;

use market_squawk_domain::{
    DataQuality, EvidenceDigest, ExactPayloadEvidence, FundNavCompleteness, FundNavCorrectionState,
    FundNavDisposition, FundNavEntitlementEvidence, FundNavFinality, FundNavLineage,
    FundNavMissingState, FundNavNativeSchema, FundNavObservation, FundNavObservationInput,
    FundNavRevisionEvidence, FundNavValuationBasis, FundNavValue, MetadataRevision, PayloadHash,
    PayloadReference, ProviderChannel, ProviderProduct, ResearchContext, ResearchObservation,
    ResearchProvenance, ResearchProvenanceInput, ResearchTemporalCoordinate, ResearchTime,
    RevisionNumber, SourceId, SourceIdentifier, Timestamp, VersionPinnedSourceLocator,
};
use market_squawk_sources::{
    ExtractionRevisionEvidence, ExtractionRevisionPlan, ObservedRevisionError,
    ProviderCaptureTerminalDisposition, SealedProviderCaptureSetReceipt,
};
use thiserror::Error;

use crate::{
    TiingoNavObservationCandidate, TiingoNavValueState, TiingoPaginationEvidence,
    TiingoProviderRevisionEvidence,
};

const TIINGO_PROVIDER_PRODUCT: &str = "starter";
const TIINGO_PROVIDER_CHANNEL: &str = "daily-eod";
const TIINGO_NAV_NATIVE_SCHEMA: &str = "tiingo.daily-prices.eod-row";

/// Exact policy/schema/entitlement evidence required by canonical Tiingo NAV mapping.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoFundNavContractEvidence {
    source_id: SourceId,
    source_contract_revision: MetadataRevision,
    source_contract_evidence: ExactPayloadEvidence,
    native_schema_evidence: ExactPayloadEvidence,
    entitlement_generation: NonZeroU64,
    entitlement_evidence: EvidenceDigest,
}

impl TiingoFundNavContractEvidence {
    /// Binds the activated source contract, reviewed native schema, and gated token generation.
    pub const fn new(
        source_id: SourceId,
        source_contract_revision: MetadataRevision,
        source_contract_evidence: ExactPayloadEvidence,
        native_schema_evidence: ExactPayloadEvidence,
        entitlement_generation: NonZeroU64,
        entitlement_evidence: EvidenceDigest,
    ) -> Self {
        Self {
            source_id,
            source_contract_revision,
            source_contract_evidence,
            native_schema_evidence,
            entitlement_generation,
            entitlement_evidence,
        }
    }

    /// Returns the exact activated Tiingo source namespace.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact activated source-contract revision.
    pub const fn source_contract_revision(&self) -> &MetadataRevision {
        &self.source_contract_revision
    }

    /// Returns the exact source-contract payload evidence.
    pub const fn source_contract_evidence(&self) -> &ExactPayloadEvidence {
        &self.source_contract_evidence
    }

    /// Returns the reviewed Tiingo native-schema payload evidence.
    pub const fn native_schema_evidence(&self) -> &ExactPayloadEvidence {
        &self.native_schema_evidence
    }

    /// Returns the nonzero protected-credential generation used for retrieval.
    pub const fn entitlement_generation(&self) -> NonZeroU64 {
        self.entitlement_generation
    }

    /// Returns exact admission evidence for that entitlement generation.
    pub const fn entitlement_evidence(&self) -> EvidenceDigest {
        self.entitlement_evidence
    }
}

/// Caller-owned local revision relationship; Tiingo supplies no revision or finality token.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TiingoFundNavRevisionLinks {
    predecessor: Option<EvidenceDigest>,
    successor: Option<EvidenceDigest>,
    superseded_at: Option<Timestamp>,
}

impl TiingoFundNavRevisionLinks {
    /// Retains exact shared-authority links without allocating or inferring a revision number.
    ///
    /// A successor and its local supersession clock are one coherent fact and must be supplied
    /// together. Tiingo's reviewed wire contract supplies neither source correction state nor an
    /// exact finality event; the canonical mapper therefore records both as `Unspecified`.
    pub fn try_new(
        predecessor: Option<EvidenceDigest>,
        successor: Option<EvidenceDigest>,
        superseded_at: Option<Timestamp>,
    ) -> Result<Self, TiingoFundNavMapError> {
        if successor.is_some() != superseded_at.is_some()
            || predecessor.is_some_and(|digest| digest.bytes() == [0; 32])
            || successor.is_some_and(|digest| digest.bytes() == [0; 32])
            || predecessor.is_some() && predecessor == successor
        {
            return Err(TiingoFundNavMapError::InvalidRevisionLinks);
        }
        Ok(Self {
            predecessor,
            successor,
            superseded_at,
        })
    }

    /// Returns exact predecessor evidence supplied by shared durable state.
    pub const fn predecessor(self) -> Option<EvidenceDigest> {
        self.predecessor
    }

    /// Returns exact successor evidence supplied by shared durable state.
    pub const fn successor(self) -> Option<EvidenceDigest> {
        self.successor
    }

    /// Returns when the supplied successor made this revision non-current locally.
    pub const fn superseded_at(self) -> Option<Timestamp> {
        self.superseded_at
    }
}

/// Complete pure-mapping input for one sealed Tiingo daily NAV result.
#[derive(Debug)]
pub struct TiingoFundNavMappingInput<'a> {
    /// Strict provider-native NAV candidate.
    pub candidate: &'a TiingoNavObservationCandidate,
    /// Exact raw response already sealed into the shared `MSJ1` journal.
    pub sealed_capture: &'a SealedProviderCaptureSetReceipt,
    /// Activated source, native-schema, and gated-entitlement evidence.
    pub contract: &'a TiingoFundNavContractEvidence,
    /// Caller-supplied nonzero seed used only to construct the pre-assignment observation.
    /// Shared observed-revision authority must replace it before durable publication.
    pub authority_seed_revision: RevisionNumber,
    /// Time canonical ingestion completed locally.
    pub ingested_at: Timestamp,
    /// Time this canonical observation became publishable locally.
    pub canonical_published_at: Timestamp,
    /// Optional predecessor/successor facts already established by shared durable authority.
    pub revision_links: TiingoFundNavRevisionLinks,
}

/// Validated canonical NAV plus its source-neutral durable-revision evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoMappedFundNav {
    observation: FundNavObservation,
    observed_revision: ExtractionRevisionEvidence,
}

impl TiingoMappedFundNav {
    /// Returns the validated pre-assignment canonical FundNav value.
    pub const fn observation(&self) -> &FundNavObservation {
        &self.observation
    }

    /// Returns local-content version evidence for shared observed-revision authority.
    pub const fn observed_revision(&self) -> &ExtractionRevisionEvidence {
        &self.observed_revision
    }

    /// Consumes the mapping into the exact one-row shared revision-authority input.
    ///
    /// The returned observation must not be published directly. The existing research ingestor
    /// atomically assigns its durable revision and reconstructs the canonical observation through
    /// `ResearchObservation::with_revision` before Arrow/Parquet publication.
    pub fn into_revision_authority_input(
        self,
    ) -> Result<(Vec<ResearchObservation>, ExtractionRevisionPlan), ObservedRevisionError> {
        let observations = vec![ResearchObservation::FundNav(self.observation)];
        let plan = ExtractionRevisionPlan::try_new(vec![self.observed_revision])?;
        Ok((observations, plan))
    }
}

/// Purely maps one sealed strict Tiingo result into canonical FundNav contracts.
///
/// Equity/ETF rows cannot reach this function through `normalize_mutual_fund_row`, which admits
/// only exact `MF` metadata. The mapper additionally revalidates the sealed request/body/clock and
/// disposition bindings. It never turns adjusted OHLC into NAV and never allocates a revision.
pub fn map_fund_nav(
    input: TiingoFundNavMappingInput<'_>,
) -> Result<TiingoMappedFundNav, TiingoFundNavMapError> {
    validate_capture(&input)?;
    validate_chronology(&input)?;
    validate_disposition(input.candidate)?;

    let source_identifier = SourceIdentifier::try_from(format!(
        "tiingo-nav:{}:{}",
        input.candidate.context().ticker(),
        input.candidate.nav_date()
    ))
    .map_err(|_| TiingoFundNavMapError::InvalidContractIdentity)?;
    // When no provider row exists, the exact complete response body is the raw evidence for the
    // closed absence state; it is not reinterpreted as a fabricated row or zero NAV.
    let payload_digest = input
        .candidate
        .provider_row_digest()
        .unwrap_or_else(|| input.candidate.raw_object_digest());
    let provenance = ResearchProvenance::try_new(ResearchProvenanceInput {
        source_id: input.contract.source_id().clone(),
        instrument_id: Some(input.candidate.context().instrument_id()),
        venue_id: None,
        source_identifier,
        source_timestamp: None,
        received_at: input.candidate.clocks().received_at(),
        ingested_at: input.ingested_at,
        quality: DataQuality::Aggregated,
        payload_reference: PayloadReference::ContentHash(PayloadHash::new(
            payload_digest.algorithm(),
            payload_digest.bytes(),
        )),
        availability: input.candidate.clocks().availability().clone(),
    })?;
    let time = ResearchTime::try_new_with_coordinates(
        ResearchTemporalCoordinate::calendar_date(input.candidate.nav_date()),
        None,
        input.authority_seed_revision,
        input
            .revision_links
            .superseded_at()
            .map(ResearchTemporalCoordinate::exact),
    )?;
    let context = ResearchContext::new(provenance, time)?;

    let native_schema = FundNavNativeSchema::new(
        input.contract.source_contract_revision().clone(),
        input.contract.source_contract_evidence().clone(),
        identifier(TIINGO_NAV_NATIVE_SCHEMA)?,
        MetadataRevision::new(input.candidate.context().native_schema_revision().clone()),
        input.contract.native_schema_evidence().clone(),
    );
    let raw_object = ExactPayloadEvidence::with_version_pinned_locator(
        input.candidate.raw_object_digest(),
        VersionPinnedSourceLocator::new(
            identifier(&format!(
                "tiingo-response:{}",
                input.candidate.context().ticker()
            ))?,
            input.candidate.context().entitlement_generation().clone(),
        ),
    );
    let raw_row = ExactPayloadEvidence::with_version_pinned_locator(
        payload_digest,
        VersionPinnedSourceLocator::new(
            identifier(&format!(
                "tiingo-nav-row:{}:{}",
                input.candidate.context().ticker(),
                input.candidate.nav_date()
            ))?,
            input
                .candidate
                .context()
                .mutual_fund_classification_revision()
                .clone(),
        ),
    );
    let (value, completeness, disposition) = canonical_value(input.candidate.value());
    let page_identity = match input.candidate.pagination() {
        TiingoPaginationEvidence::NotApplicable => None,
        TiingoPaginationEvidence::ApplicationDateWindow(_) => {
            Some(input.candidate.request_identity())
        }
    };
    let lineage = FundNavLineage::try_new(
        native_schema,
        FundNavEntitlementEvidence::Gated {
            generation: input.contract.entitlement_generation(),
            evidence: input.contract.entitlement_evidence(),
        },
        input.candidate.request_identity(),
        raw_object,
        raw_row,
        page_identity,
        input.sealed_capture.receipt_digest(),
        completeness,
        disposition,
    )?;
    let revision_evidence = FundNavRevisionEvidence::try_new(
        None,
        FundNavCorrectionState::Unspecified,
        FundNavFinality::Unspecified,
        input.revision_links.predecessor(),
        input.revision_links.successor(),
    )?;
    let observation = FundNavObservation::try_new(FundNavObservationInput {
        context,
        provider_instrument_id: input.candidate.context().provider_instrument_id().clone(),
        instrument_reference_revision: MetadataRevision::new(
            input
                .candidate
                .context()
                .instrument_definition_revision()
                .clone(),
        ),
        provider_product: ProviderProduct::new(identifier(TIINGO_PROVIDER_PRODUCT)?),
        provider_channel: ProviderChannel::new(identifier(TIINGO_PROVIDER_CHANNEL)?),
        nav_date: input.candidate.nav_date(),
        valuation_basis: FundNavValuationBasis::PerShare,
        currency: input.candidate.context().currency(),
        value,
        canonical_published_at: input.canonical_published_at,
        lineage,
        revision_evidence,
    })?;
    Ok(TiingoMappedFundNav {
        observation,
        observed_revision: ExtractionRevisionEvidence::locally_observed_content(),
    })
}

fn validate_capture(input: &TiingoFundNavMappingInput<'_>) -> Result<(), TiingoFundNavMapError> {
    let capture = input.sealed_capture.capture();
    let Some(page) = capture.pages().first() else {
        return Err(TiingoFundNavMapError::CaptureMismatch);
    };
    if capture.pages().len() != 1
        || capture.source_id() != input.contract.source_id()
        || capture.metadata_revision() != input.contract.source_contract_revision()
        || input.candidate.provider_revision() != TiingoProviderRevisionEvidence::NotSupplied
        || capture.terminal() != ProviderCaptureTerminalDisposition::StandaloneResponse
        || capture.request_set_identity() != input.candidate.request_identity()
        || capture.total_body_bytes() != input.candidate.request_disposition().response_bytes()
        || page.request_identity() != input.candidate.request_identity()
        || page.http_status() != input.candidate.response_status()
        || page.body_bytes() != input.candidate.request_disposition().response_bytes()
        || page.body_digest() != input.candidate.raw_object_digest()
        || page.received_at() != input.candidate.clocks().received_at()
    {
        return Err(TiingoFundNavMapError::CaptureMismatch);
    }
    Ok(())
}

fn validate_chronology(input: &TiingoFundNavMappingInput<'_>) -> Result<(), TiingoFundNavMapError> {
    let clocks = input.candidate.clocks();
    if clocks.received_at() > clocks.decoded_at()
        || clocks.decoded_at() > input.ingested_at
        || input.ingested_at > input.canonical_published_at
        || input
            .revision_links
            .superseded_at()
            .is_some_and(|superseded| superseded <= input.canonical_published_at)
    {
        return Err(TiingoFundNavMapError::InvalidChronology);
    }
    Ok(())
}

fn validate_disposition(
    candidate: &TiingoNavObservationCandidate,
) -> Result<(), TiingoFundNavMapError> {
    let disposition = candidate.request_disposition();
    if disposition.requested_symbols() != 1
        || disposition.returned_symbols() + disposition.missing_symbols() != 1
        || disposition.response_bytes() == 0
        || !(200..300).contains(&candidate.response_status())
    {
        return Err(TiingoFundNavMapError::InvalidDisposition);
    }
    let returned_row = candidate.provider_row_digest().is_some();
    let row_count = disposition.returned_rows();
    let consistent = match candidate.value() {
        TiingoNavValueState::Observed(_) | TiingoNavValueState::Invalid(_) => {
            returned_row
                && row_count > 0
                && disposition.returned_symbols() == 1
                && disposition.missing_symbols() == 0
        }
        TiingoNavValueState::SourceMissing => {
            returned_row == (row_count > 0)
                && disposition.returned_symbols() == u16::from(row_count > 0)
                && disposition.missing_symbols() == u16::from(row_count == 0)
        }
        TiingoNavValueState::NotYetPublished
        | TiingoNavValueState::Unsupported
        | TiingoNavValueState::Unavailable => {
            !returned_row
                && row_count == 0
                && disposition.returned_symbols() == 0
                && disposition.missing_symbols() == 1
        }
    };
    if consistent {
        Ok(())
    } else {
        Err(TiingoFundNavMapError::InvalidDisposition)
    }
}

fn canonical_value(
    value: TiingoNavValueState,
) -> (FundNavValue, FundNavCompleteness, FundNavDisposition) {
    match value {
        TiingoNavValueState::Observed(money) => (
            FundNavValue::Observed(money),
            FundNavCompleteness::Complete,
            FundNavDisposition::Returned,
        ),
        TiingoNavValueState::NotYetPublished => (
            FundNavValue::Missing(FundNavMissingState::NotYetPublished),
            FundNavCompleteness::Complete,
            FundNavDisposition::NotYetPublished,
        ),
        TiingoNavValueState::Unsupported => (
            FundNavValue::Missing(FundNavMissingState::Unsupported),
            FundNavCompleteness::Complete,
            FundNavDisposition::Unsupported,
        ),
        TiingoNavValueState::SourceMissing => (
            FundNavValue::Missing(FundNavMissingState::SourceMissing),
            FundNavCompleteness::Complete,
            FundNavDisposition::SourceMissing,
        ),
        TiingoNavValueState::Invalid(_) => (
            FundNavValue::Missing(FundNavMissingState::Invalid),
            FundNavCompleteness::Complete,
            FundNavDisposition::Invalid,
        ),
        TiingoNavValueState::Unavailable => (
            FundNavValue::Missing(FundNavMissingState::Unavailable),
            FundNavCompleteness::Incomplete,
            FundNavDisposition::Unavailable,
        ),
    }
}

fn identifier(value: &str) -> Result<SourceIdentifier, TiingoFundNavMapError> {
    SourceIdentifier::try_from(value).map_err(|_| TiingoFundNavMapError::InvalidContractIdentity)
}

/// Closed failure to construct exact canonical Tiingo fund-NAV evidence.
#[derive(Debug, Error)]
pub enum TiingoFundNavMapError {
    /// The sealed source-neutral receipt does not bind the candidate request/body/clock exactly.
    #[error("sealed Tiingo capture does not match the NAV candidate")]
    CaptureMismatch,
    /// Decode, ingest, canonical publication, or supersession clocks regressed.
    #[error("Tiingo NAV canonical chronology is invalid")]
    InvalidChronology,
    /// Requested, returned, missing, row, byte, and NAV-state evidence disagree.
    #[error("Tiingo NAV request disposition is inconsistent")]
    InvalidDisposition,
    /// Predecessor, successor, and supersession evidence is incomplete or contradictory.
    #[error("Tiingo NAV revision links are invalid")]
    InvalidRevisionLinks,
    /// A code-owned source/product/channel/schema identity could not satisfy domain bounds.
    #[error("Tiingo NAV canonical contract identity is invalid")]
    InvalidContractIdentity,
    /// Canonical provenance or time invariants rejected the supplied evidence.
    #[error(transparent)]
    Provenance(#[from] market_squawk_domain::ProvenanceError),
    /// Canonical FundNav invariants rejected the supplied evidence.
    #[error(transparent)]
    Research(#[from] market_squawk_domain::ResearchError),
}
