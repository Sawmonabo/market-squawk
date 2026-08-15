//! Strict Tiingo equity/ETF EOD mapping into separate raw and adjusted canonical bars.

use std::{
    io,
    num::NonZeroU64,
};

use market_squawk_domain::{
    AvailabilityEvidence, BarTimeSemantics, CalendarDate, Currency, DataQuality, DigestAlgorithm,
    EvidenceDigest, ExactPayloadEvidence, InstrumentId, MarketBarAdjustment,
    MarketBarObservation, MetadataRevision, Money, PayloadHash, PayloadReference,
    ProviderInstrumentId, ResearchContext, ResearchObservation, ResearchProvenance,
    ResearchProvenanceInput, ResearchTime, RevisionBoundPayloadEvidence, RevisionNumber, SourceId,
    SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_sources::{
    ExtractionRevisionPlan, ObservedRevisionError, ProviderCaptureTerminalDisposition,
    SealedProviderCaptureSetReceipt,
};
use rust_decimal::Decimal;
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::{
    TiingoCompletedHistoryCapture, TiingoCoverage, TiingoEndpointFamily, TiingoEodReceipt,
    TiingoEodRow, TiingoHistoryPlan, TiingoHistoryTerminalDisposition, TiingoMetadataReceipt,
    TiingoPaginationEvidence, TiingoRequestScope, TiingoRequestSpec, TiingoTicker,
};

const TIINGO_SOURCE_ID: &str = "tiingo-starter";
const TIINGO_MUTUAL_FUND_EXCHANGE_CODE: &str = "MF";
const TIINGO_LATEST_DATASET: &str = "tiingo-daily-latest";
const TIINGO_HISTORY_DATASET: &str = "tiingo-daily-history-window";
const TIINGO_DAILY_INTERVAL: &str = "tiingo-calendar-day";
const MAX_EOD_HANDOFF_CANONICAL_BYTES: u64 = 16 * 1024 * 1024;

/// Closed canonical asset classification admitted by the Tiingo EOD bar mapper.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TiingoEodInstrumentKind {
    /// A resolved exchange-listed equity.
    Equity,
    /// A resolved exchange-traded fund. Mutual funds are deliberately excluded.
    ExchangeTradedFund,
}

/// Canonical identity and exact provider-to-venue mapping for one Tiingo EOD instrument.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoEodInstrumentAuthority {
    instrument_id: InstrumentId,
    venue_id: VenueId,
    provider_instrument_id: ProviderInstrumentId,
    ticker: TiingoTicker,
    provider_exchange_code: SourceIdentifier,
    kind: TiingoEodInstrumentKind,
    instrument_definition: RevisionBoundPayloadEvidence,
    provider_mapping_evidence: ExactPayloadEvidence,
    resolved_at: Timestamp,
    currency: Currency,
}

impl TiingoEodInstrumentAuthority {
    /// Constructs externally resolved equity/ETF identity without inferring asset class from a
    /// ticker, metadata name, or archive membership.
    #[allow(
        clippy::too_many_arguments,
        reason = "canonical identity, provider identity, classification, revision, and currency remain explicit"
    )]
    pub fn try_new(
        instrument_id: InstrumentId,
        venue_id: VenueId,
        provider_instrument_id: ProviderInstrumentId,
        ticker: TiingoTicker,
        provider_exchange_code: SourceIdentifier,
        kind: TiingoEodInstrumentKind,
        instrument_definition: RevisionBoundPayloadEvidence,
        provider_mapping_evidence: ExactPayloadEvidence,
        resolved_at: Timestamp,
        currency: Currency,
    ) -> Result<Self, TiingoEodMapError> {
        if provider_instrument_id.as_str() != ticker.as_str()
            || instrument_definition
                .payload_evidence()
                .content_digest()
                .bytes()
                == [0; 32]
            || provider_mapping_evidence.content_digest().bytes() == [0; 32]
        {
            return Err(TiingoEodMapError::AuthorityMismatch);
        }
        Ok(Self {
            instrument_id,
            venue_id,
            provider_instrument_id,
            ticker,
            provider_exchange_code,
            kind,
            instrument_definition,
            provider_mapping_evidence,
            resolved_at,
            currency,
        })
    }

    /// Returns the stable canonical instrument identity.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the exact canonical venue identity.
    pub const fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }

    /// Returns the exact Tiingo provider instrument identity.
    pub const fn provider_instrument_id(&self) -> &ProviderInstrumentId {
        &self.provider_instrument_id
    }

    /// Returns the exact requested Tiingo ticker.
    pub const fn ticker(&self) -> &TiingoTicker {
        &self.ticker
    }

    /// Returns the exact Tiingo metadata exchange code mapped to the canonical venue.
    pub const fn provider_exchange_code(&self) -> &SourceIdentifier {
        &self.provider_exchange_code
    }

    /// Returns the externally established equity/ETF class.
    pub const fn kind(&self) -> TiingoEodInstrumentKind {
        self.kind
    }

    /// Returns the exact instrument-definition revision used by mapping.
    pub const fn instrument_definition(&self) -> &RevisionBoundPayloadEvidence {
        &self.instrument_definition
    }

    /// Returns exact evidence for the Tiingo ticker/exchange-to-canonical mapping.
    pub const fn provider_mapping_evidence(&self) -> &ExactPayloadEvidence {
        &self.provider_mapping_evidence
    }

    /// Returns when the exact canonical/provider/classification mapping became locally available.
    pub const fn resolved_at(&self) -> Timestamp {
        self.resolved_at
    }

    /// Returns the externally resolved price currency.
    pub const fn currency(&self) -> Currency {
        self.currency
    }

    fn mapping_identity(&self) -> EvidenceDigest {
        let mut hasher = Sha256::new();
        append_field(&mut hasher, b"market-squawk/tiingo/eod-instrument/v1");
        append_field(&mut hasher, self.instrument_id.to_string().as_bytes());
        append_field(&mut hasher, self.venue_id.as_str().as_bytes());
        append_field(
            &mut hasher,
            self.provider_instrument_id.as_str().as_bytes(),
        );
        append_field(&mut hasher, self.ticker.as_str().as_bytes());
        append_field(&mut hasher, self.provider_exchange_code.as_str().as_bytes());
        append_field(
            &mut hasher,
            &[match self.kind {
                TiingoEodInstrumentKind::Equity => 0,
                TiingoEodInstrumentKind::ExchangeTradedFund => 1,
            }],
        );
        append_field(
            &mut hasher,
            self.instrument_definition
                .metadata_revision()
                .as_source_identifier()
                .as_str()
                .as_bytes(),
        );
        append_field(
            &mut hasher,
            &self
                .instrument_definition
                .payload_evidence()
                .content_digest()
                .bytes(),
        );
        append_field(
            &mut hasher,
            &self.provider_mapping_evidence.content_digest().bytes(),
        );
        append_field(&mut hasher, &self.resolved_at.unix_nanos().to_be_bytes());
        append_field(&mut hasher, self.currency.as_str().as_bytes());
        EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into())
    }
}

/// Exact source, schema, entitlement, and raw/adjusted surface evidence for EOD mapping.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoEodContractEvidence {
    source_id: SourceId,
    source_contract_revision: MetadataRevision,
    source_contract_evidence: ExactPayloadEvidence,
    native_schema_revision: SourceIdentifier,
    native_schema_evidence: ExactPayloadEvidence,
    entitlement_generation: NonZeroU64,
    entitlement_generation_identity: SourceIdentifier,
    entitlement_evidence: EvidenceDigest,
    raw_feed: SourceIdentifier,
    adjusted_feed: SourceIdentifier,
    adjusted_adjustment: MarketBarAdjustment,
    adjusted_surface_evidence: ExactPayloadEvidence,
}

impl TiingoEodContractEvidence {
    /// Binds one activated Tiingo contract and distinct canonical raw/adjusted feed identities.
    #[allow(
        clippy::too_many_arguments,
        reason = "source, schema, entitlement, and two price-surface identities remain explicit"
    )]
    pub fn try_new(
        source_id: SourceId,
        source_contract_revision: MetadataRevision,
        source_contract_evidence: ExactPayloadEvidence,
        native_schema_revision: SourceIdentifier,
        native_schema_evidence: ExactPayloadEvidence,
        entitlement_generation: NonZeroU64,
        entitlement_generation_identity: SourceIdentifier,
        entitlement_evidence: EvidenceDigest,
        raw_feed: SourceIdentifier,
        adjusted_feed: SourceIdentifier,
        adjusted_adjustment: MarketBarAdjustment,
        adjusted_surface_evidence: ExactPayloadEvidence,
    ) -> Result<Self, TiingoEodMapError> {
        if source_id.as_str() != TIINGO_SOURCE_ID
            || raw_feed == adjusted_feed
            || adjusted_adjustment == MarketBarAdjustment::Raw
            || [
                source_contract_evidence.content_digest(),
                native_schema_evidence.content_digest(),
                entitlement_evidence,
                adjusted_surface_evidence.content_digest(),
            ]
            .into_iter()
            .any(|digest| digest.bytes() == [0; 32])
        {
            return Err(TiingoEodMapError::InvalidContractEvidence);
        }
        Ok(Self {
            source_id,
            source_contract_revision,
            source_contract_evidence,
            native_schema_revision,
            native_schema_evidence,
            entitlement_generation,
            entitlement_generation_identity,
            entitlement_evidence,
            raw_feed,
            adjusted_feed,
            adjusted_adjustment,
            adjusted_surface_evidence,
        })
    }

    /// Returns the exact selected Tiingo source namespace.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the activated source-contract revision.
    pub const fn source_contract_revision(&self) -> &MetadataRevision {
        &self.source_contract_revision
    }

    /// Returns exact source-contract payload evidence.
    pub const fn source_contract_evidence(&self) -> &ExactPayloadEvidence {
        &self.source_contract_evidence
    }

    /// Returns exact reviewed native-schema evidence.
    pub const fn native_schema_evidence(&self) -> &ExactPayloadEvidence {
        &self.native_schema_evidence
    }

    /// Returns the exact reviewed provider-native decoder contract revision.
    pub const fn native_schema_revision(&self) -> &SourceIdentifier {
        &self.native_schema_revision
    }

    /// Returns the protected-token generation used for retrieval.
    pub const fn entitlement_generation(&self) -> NonZeroU64 {
        self.entitlement_generation
    }

    /// Returns the exact source-local credential/entitlement generation identity.
    pub const fn entitlement_generation_identity(&self) -> &SourceIdentifier {
        &self.entitlement_generation_identity
    }

    /// Returns exact admission evidence for that token generation.
    pub const fn entitlement_evidence(&self) -> EvidenceDigest {
        self.entitlement_evidence
    }

    /// Returns the canonical feed identity for unadjusted Tiingo values.
    pub const fn raw_feed(&self) -> &SourceIdentifier {
        &self.raw_feed
    }

    /// Returns the distinct canonical feed identity for Tiingo-adjusted values.
    pub const fn adjusted_feed(&self) -> &SourceIdentifier {
        &self.adjusted_feed
    }

    /// Returns the exact reviewed canonical adjustment class for the adjusted surface.
    pub const fn adjusted_adjustment(&self) -> MarketBarAdjustment {
        self.adjusted_adjustment
    }

    /// Returns exact reviewed evidence defining the provider-adjusted surface.
    pub const fn adjusted_surface_evidence(&self) -> &ExactPayloadEvidence {
        &self.adjusted_surface_evidence
    }

    fn mapping_identity(&self) -> EvidenceDigest {
        let mut hasher = Sha256::new();
        append_field(&mut hasher, b"market-squawk/tiingo/eod-contract/v1");
        append_field(&mut hasher, self.source_id.as_str().as_bytes());
        append_field(
            &mut hasher,
            self.source_contract_revision
                .as_source_identifier()
                .as_str()
                .as_bytes(),
        );
        for digest in [
            self.source_contract_evidence.content_digest(),
            self.native_schema_evidence.content_digest(),
            self.entitlement_evidence,
            self.adjusted_surface_evidence.content_digest(),
        ] {
            append_field(&mut hasher, &digest.bytes());
        }
        append_field(
            &mut hasher,
            &self.entitlement_generation.get().to_be_bytes(),
        );
        append_field(
            &mut hasher,
            self.entitlement_generation_identity.as_str().as_bytes(),
        );
        append_field(
            &mut hasher,
            self.native_schema_revision.as_str().as_bytes(),
        );
        append_field(&mut hasher, self.raw_feed.as_str().as_bytes());
        append_field(&mut hasher, self.adjusted_feed.as_str().as_bytes());
        append_field(
            &mut hasher,
            &[adjustment_discriminant(self.adjusted_adjustment)],
        );
        EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into())
    }
}

/// Least-authority request for externally governed EOD session/calendar semantics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoEodBarTimeRequest {
    instrument_id: InstrumentId,
    venue_id: VenueId,
    provider_instrument_id: ProviderInstrumentId,
    ticker: TiingoTicker,
    provider_date: CalendarDate,
}

impl TiingoEodBarTimeRequest {
    /// Returns the canonical instrument identity.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the canonical venue identity.
    pub const fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }

    /// Returns the exact provider instrument identity.
    pub const fn provider_instrument_id(&self) -> &ProviderInstrumentId {
        &self.provider_instrument_id
    }

    /// Returns the exact Tiingo ticker.
    pub const fn ticker(&self) -> &TiingoTicker {
        &self.ticker
    }

    /// Returns Tiingo's exact daily civil date without inventing midnight precision.
    pub const fn provider_date(&self) -> CalendarDate {
        self.provider_date
    }
}

/// Revocable authority for exact completed EOD periods and venue-session evidence.
///
/// Tiingo's wire row supplies a UTC-midnight-shaped date string, not proof of an exchange session
/// boundary. The mapper therefore delegates the exact provider timestamp basis, period, and
/// session rules to independently governed calendar authority.
pub trait TiingoEodBarTimeAuthority: Send + Sync {
    /// Rejects use after the independently governed session/calendar mapping is revoked.
    fn validate_current(&self) -> Result<(), TiingoEodMapError>;

    /// Resolves one source civil date to exact provider timestamp and completed-session semantics.
    fn resolve(
        &self,
        request: &TiingoEodBarTimeRequest,
    ) -> Result<BarTimeSemantics, TiingoEodMapError>;
}

/// Exact financial-date scope submitted to independently governed market-calendar authority.
///
/// This request is distinct from the Tiingo HTTP plan. It asks which venue sessions were expected
/// for the resolved instrument inside that plan's inclusive civil-date interval.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoEodExpectedSessionRequest {
    history_plan_identity: EvidenceDigest,
    start_date: CalendarDate,
    end_date: CalendarDate,
    instrument_id: InstrumentId,
    venue_id: VenueId,
    provider_instrument_id: ProviderInstrumentId,
    ticker: TiingoTicker,
    instrument_authority_identity: EvidenceDigest,
    request_identity: EvidenceDigest,
}

impl TiingoEodExpectedSessionRequest {
    fn new(plan: &TiingoHistoryPlan, instrument: &TiingoEodInstrumentAuthority) -> Self {
        let history_plan_identity = plan.request_set_identity();
        let (start_date, end_date) = plan.interval();
        let instrument_authority_identity = instrument.mapping_identity();
        let mut hasher = Sha256::new();
        append_field(
            &mut hasher,
            b"market-squawk/tiingo/eod-expected-session-request/v1",
        );
        append_field(&mut hasher, &history_plan_identity.bytes());
        append_field(&mut hasher, start_date.to_string().as_bytes());
        append_field(&mut hasher, end_date.to_string().as_bytes());
        append_field(
            &mut hasher,
            instrument.instrument_id().to_string().as_bytes(),
        );
        append_field(&mut hasher, instrument.venue_id().as_str().as_bytes());
        append_field(
            &mut hasher,
            instrument.provider_instrument_id().as_str().as_bytes(),
        );
        append_field(&mut hasher, instrument.ticker().as_str().as_bytes());
        append_field(&mut hasher, &instrument_authority_identity.bytes());
        let request_identity =
            EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into());
        Self {
            history_plan_identity,
            start_date,
            end_date,
            instrument_id: instrument.instrument_id(),
            venue_id: instrument.venue_id().clone(),
            provider_instrument_id: instrument.provider_instrument_id().clone(),
            ticker: instrument.ticker().clone(),
            instrument_authority_identity,
            request_identity,
        }
    }

    /// Returns the exact HTTP request-graph identity whose dates require reconciliation.
    pub const fn history_plan_identity(&self) -> EvidenceDigest {
        self.history_plan_identity
    }

    /// Returns the inclusive first requested civil date.
    pub const fn start_date(&self) -> CalendarDate {
        self.start_date
    }

    /// Returns the inclusive last requested civil date.
    pub const fn end_date(&self) -> CalendarDate {
        self.end_date
    }

    /// Returns the resolved canonical instrument identity.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the exact canonical venue whose calendar governs expected sessions.
    pub const fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }

    /// Returns the exact provider instrument identity.
    pub const fn provider_instrument_id(&self) -> &ProviderInstrumentId {
        &self.provider_instrument_id
    }

    /// Returns the exact Tiingo ticker.
    pub const fn ticker(&self) -> &TiingoTicker {
        &self.ticker
    }

    /// Returns the complete canonical/provider mapping identity used by the request.
    pub const fn instrument_authority_identity(&self) -> EvidenceDigest {
        self.instrument_authority_identity
    }

    /// Returns the complete expected-session request identity.
    pub const fn request_identity(&self) -> EvidenceDigest {
        self.request_identity
    }
}

/// Exact versioned market-calendar evidence and complete expected-session enumeration.
///
/// The retained dates must be strictly increasing, unique, and inside the exact request interval.
/// This is calendar evidence only; it cannot publish data or mint point-in-time authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoEodExpectedSessionEvidence {
    request_identity: EvidenceDigest,
    calendar_id: SourceIdentifier,
    calendar_revision: RevisionBoundPayloadEvidence,
    authority_generation: SourceIdentifier,
    calendar_available_at: Timestamp,
    resolved_at: Timestamp,
    resolution_receipt: EvidenceDigest,
    expected_sessions: Box<[CalendarDate]>,
    evidence_identity: EvidenceDigest,
}

impl TiingoEodExpectedSessionEvidence {
    /// Binds an exact reviewed calendar revision to every expected venue session in the request.
    pub fn try_new(
        request: &TiingoEodExpectedSessionRequest,
        calendar_id: SourceIdentifier,
        calendar_revision: RevisionBoundPayloadEvidence,
        authority_generation: SourceIdentifier,
        calendar_available_at: Timestamp,
        resolved_at: Timestamp,
        resolution_receipt: EvidenceDigest,
        expected_sessions: Vec<CalendarDate>,
    ) -> Result<Self, TiingoEodMapError> {
        let inclusive_calendar_days = request
            .end_date()
            .days_since_unix_epoch()
            .checked_sub(request.start_date().days_since_unix_epoch())
            .and_then(|days| days.checked_add(1))
            .and_then(|days| usize::try_from(days).ok())
            .ok_or(TiingoEodMapError::InvalidExpectedSessionEvidence)?;
        if calendar_revision
            .payload_evidence()
            .content_digest()
            .bytes()
            == [0; 32]
            || calendar_available_at.unix_nanos() < 0
            || resolved_at < calendar_available_at
            || resolution_receipt.bytes() == [0; 32]
            || expected_sessions.len() > inclusive_calendar_days
            || expected_sessions.iter().any(|date| {
                *date < request.start_date() || *date > request.end_date()
            })
            || expected_sessions.windows(2).any(|dates| dates[0] >= dates[1])
        {
            return Err(TiingoEodMapError::InvalidExpectedSessionEvidence);
        }
        let evidence_identity = expected_session_evidence_identity(
            request.request_identity(),
            &calendar_id,
            &calendar_revision,
            &authority_generation,
            calendar_available_at,
            resolved_at,
            resolution_receipt,
            &expected_sessions,
        );
        Ok(Self {
            request_identity: request.request_identity(),
            calendar_id,
            calendar_revision,
            authority_generation,
            calendar_available_at,
            resolved_at,
            resolution_receipt,
            expected_sessions: expected_sessions.into_boxed_slice(),
            evidence_identity,
        })
    }

    /// Returns the exact request this evidence answers.
    pub const fn request_identity(&self) -> EvidenceDigest {
        self.request_identity
    }

    /// Returns the independently governed calendar identity.
    pub const fn calendar_id(&self) -> &SourceIdentifier {
        &self.calendar_id
    }

    /// Returns the exact version and payload evidence for the selected calendar generation.
    pub const fn calendar_revision(&self) -> &RevisionBoundPayloadEvidence {
        &self.calendar_revision
    }

    /// Returns the exact durable calendar-authority generation that resolved this request.
    pub const fn authority_generation(&self) -> &SourceIdentifier {
        &self.authority_generation
    }

    /// Returns the conservative first instant this exact calendar generation was knowable locally.
    pub const fn calendar_available_at(&self) -> Timestamp {
        self.calendar_available_at
    }

    /// Returns when authority resolved this exact expected-session request.
    pub const fn resolved_at(&self) -> Timestamp {
        self.resolved_at
    }

    /// Returns the durable authority receipt for the exact request/generation resolution.
    pub const fn resolution_receipt(&self) -> EvidenceDigest {
        self.resolution_receipt
    }

    /// Returns every expected venue session in strictly increasing civil-date order.
    pub fn expected_sessions(&self) -> &[CalendarDate] {
        &self.expected_sessions
    }

    /// Returns the complete request/calendar/revision/session-set identity.
    pub const fn evidence_identity(&self) -> EvidenceDigest {
        self.evidence_identity
    }
}

/// Durable currentness receipt for one exact expected-session evidence generation.
///
/// Shared publication must transactionally consume this receipt or revalidate the same retained
/// evidence generation immediately before committing an immutable manifest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoEodExpectedSessionValidationReceipt {
    evidence_identity: EvidenceDigest,
    authority_generation: SourceIdentifier,
    validated_at: Timestamp,
    authority_receipt: EvidenceDigest,
    receipt_identity: EvidenceDigest,
}

impl TiingoEodExpectedSessionValidationReceipt {
    /// Binds one durable authority validation to exact expected-session evidence.
    pub fn try_new(
        evidence: &TiingoEodExpectedSessionEvidence,
        authority_generation: SourceIdentifier,
        validated_at: Timestamp,
        authority_receipt: EvidenceDigest,
    ) -> Result<Self, TiingoEodMapError> {
        if authority_generation != *evidence.authority_generation()
            || validated_at < evidence.resolved_at()
            || authority_receipt.bytes() == [0; 32]
        {
            return Err(TiingoEodMapError::InvalidExpectedSessionEvidence);
        }
        let mut hasher = Sha256::new();
        append_field(
            &mut hasher,
            b"market-squawk/tiingo/eod-expected-session-validation/v1",
        );
        append_evidence_digest(&mut hasher, evidence.evidence_identity());
        append_field(&mut hasher, authority_generation.as_str().as_bytes());
        append_field(&mut hasher, &validated_at.unix_nanos().to_be_bytes());
        append_evidence_digest(&mut hasher, authority_receipt);
        let receipt_identity =
            EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into());
        Ok(Self {
            evidence_identity: evidence.evidence_identity(),
            authority_generation,
            validated_at,
            authority_receipt,
            receipt_identity,
        })
    }

    /// Returns the exact calendar evidence validated by this receipt.
    pub const fn evidence_identity(&self) -> EvidenceDigest {
        self.evidence_identity
    }

    /// Returns the exact calendar-authority generation that performed validation.
    pub const fn authority_generation(&self) -> &SourceIdentifier {
        &self.authority_generation
    }

    /// Returns when the exact retained calendar generation was validated current.
    pub const fn validated_at(&self) -> Timestamp {
        self.validated_at
    }

    /// Returns the durable validation coordinate minted by calendar authority.
    pub const fn authority_receipt(&self) -> EvidenceDigest {
        self.authority_receipt
    }

    /// Returns the complete validation-receipt identity retained by history completion.
    pub const fn receipt_identity(&self) -> EvidenceDigest {
        self.receipt_identity
    }
}

/// Revocable authority for a complete expected-session enumeration over one exact history plan.
///
/// Implementations are expected to use retained, versioned calendar evidence. They must not infer
/// weekday sessions locally, mint publication authority, or treat Tiingo response rows as the
/// expected calendar.
pub trait TiingoEodExpectedSessionAuthority: Send + Sync {
    /// Resolves the exact complete expected-session set for one request.
    fn resolve_expected_sessions(
        &self,
        request: &TiingoEodExpectedSessionRequest,
    ) -> Result<TiingoEodExpectedSessionEvidence, TiingoEodMapError>;

    /// Revalidates the exact returned calendar generation before and after reconciliation.
    fn validate_current(
        &self,
        evidence: &TiingoEodExpectedSessionEvidence,
    ) -> Result<TiingoEodExpectedSessionValidationReceipt, TiingoEodMapError>;
}

/// Financial-date coverage after the complete HTTP request graph is calendar-reconciled.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TiingoEodFinancialCoverageDisposition {
    /// Every expected session has one returned provider-native row.
    Complete,
    /// The HTTP graph completed, but one or more expected sessions returned no row.
    MissingExpectedSessions,
}

/// Raw or adjusted Tiingo daily-price surface.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TiingoEodSurface {
    /// Unadjusted provider OHLCV.
    Raw,
    /// Provider-adjusted OHLCV under the exact reviewed adjustment evidence.
    Adjusted,
}

/// Why one provider-native daily surface did not become a canonical market bar.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TiingoEodSurfaceGapReason {
    /// At least one OHLC component was null.
    MissingOhlc,
    /// The surface-specific volume was null.
    MissingVolume,
}

/// Explicit noncanonical surface gap. Missing values are never filled from the other surface.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TiingoEodSurfaceGap {
    provider_date: CalendarDate,
    surface: TiingoEodSurface,
    reason: TiingoEodSurfaceGapReason,
    row_digest: EvidenceDigest,
}

impl TiingoEodSurfaceGap {
    /// Returns the exact provider daily date.
    pub const fn provider_date(&self) -> CalendarDate {
        self.provider_date
    }

    /// Returns the incomplete raw or adjusted surface.
    pub const fn surface(&self) -> TiingoEodSurface {
        self.surface
    }

    /// Returns the exact missing-field class.
    pub const fn reason(&self) -> TiingoEodSurfaceGapReason {
        self.reason
    }

    /// Returns the complete provider-native row identity.
    pub const fn row_digest(&self) -> EvidenceDigest {
        self.row_digest
    }
}

/// Source-reported action fields retained beside bars without promoting them to canonical actions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TiingoEodProviderActionEvidence {
    provider_date: CalendarDate,
    cash_dividend: Option<Decimal>,
    split_factor: Option<Decimal>,
    row_digest: EvidenceDigest,
}

impl TiingoEodProviderActionEvidence {
    /// Returns the exact provider daily date.
    pub const fn provider_date(&self) -> CalendarDate {
        self.provider_date
    }

    /// Returns source-reported `divCash`; no corporate action semantics are inferred.
    pub const fn cash_dividend(&self) -> Option<Decimal> {
        self.cash_dividend
    }

    /// Returns source-reported `splitFactor`; no corporate action semantics are inferred.
    pub const fn split_factor(&self) -> Option<Decimal> {
        self.split_factor
    }

    /// Returns the complete provider-native row identity.
    pub const fn row_digest(&self) -> EvidenceDigest {
        self.row_digest
    }
}

/// Complete pure-mapping input for one sealed Tiingo latest or history-window response.
pub struct TiingoEodMappingInput<'a> {
    /// Strict provider-native daily-price receipt.
    pub response: &'a TiingoEodReceipt,
    /// Exact per-ticker metadata admission receipt.
    pub metadata: &'a TiingoMetadataReceipt,
    /// Exact raw response already sealed into the shared immutable journal.
    pub sealed_capture: &'a SealedProviderCaptureSetReceipt,
    /// Exact per-ticker metadata admission response sealed into the same journal authority.
    pub sealed_metadata_capture: &'a SealedProviderCaptureSetReceipt,
    /// Exact canonical/provider identity and externally proven asset class.
    pub instrument: &'a TiingoEodInstrumentAuthority,
    /// Activated source, schema, entitlement, and adjustment-surface evidence.
    pub contract: &'a TiingoEodContractEvidence,
    /// Independent provider-calendar/session authority.
    pub bar_time_authority: &'a dyn TiingoEodBarTimeAuthority,
    /// Nonzero placeholder replaced by shared observed-revision authority before publication.
    pub authority_seed_revision: RevisionNumber,
    /// Time canonical ingestion completed locally.
    pub ingested_at: Timestamp,
}

impl std::fmt::Debug for TiingoEodMappingInput<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TiingoEodMappingInput")
            .field("request", self.response.evidence().request())
            .field("instrument", self.instrument)
            .field("contract", self.contract)
            .field("bar_time_authority", &"[REVOCABLE AUTHORITY]")
            .field("authority_seed_revision", &self.authority_seed_revision)
            .field("ingested_at", &self.ingested_at)
            .finish()
    }
}

/// Provider-local canonical handoff for shared revision assignment and immutable publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoMappedEodPage {
    request: TiingoRequestSpec,
    observations: Vec<ResearchObservation>,
    revision_plan: Option<ExtractionRevisionPlan>,
    gaps: Vec<TiingoEodSurfaceGap>,
    provider_actions: Vec<TiingoEodProviderActionEvidence>,
    request_identity: EvidenceDigest,
    response_bytes: u64,
    sealed_capture_receipt: EvidenceDigest,
    sealed_metadata_capture_receipt: EvidenceDigest,
    contract_identity: EvidenceDigest,
    instrument_authority_identity: EvidenceDigest,
    handoff_identity: EvidenceDigest,
}

impl TiingoMappedEodPage {
    /// Returns the exact latest or one-window history request this page proves.
    pub const fn request(&self) -> &TiingoRequestSpec {
        &self.request
    }

    /// Returns raw and adjusted canonical bars in row order, raw before adjusted for each date.
    pub fn observations(&self) -> &[ResearchObservation] {
        &self.observations
    }

    /// Returns aligned local-content evidence for shared durable revision assignment.
    pub const fn revision_plan(&self) -> Option<&ExtractionRevisionPlan> {
        self.revision_plan.as_ref()
    }

    /// Returns every provider-native surface that was incomplete and therefore not fabricated.
    pub fn gaps(&self) -> &[TiingoEodSurfaceGap] {
        &self.gaps
    }

    /// Returns exact source-reported dividend/split fields retained as native evidence only.
    pub fn provider_actions(&self) -> &[TiingoEodProviderActionEvidence] {
        &self.provider_actions
    }

    /// Returns the credential-free exact request identity.
    pub const fn request_identity(&self) -> EvidenceDigest {
        self.request_identity
    }

    /// Returns exact retained response-body bytes for aggregate history admission.
    pub const fn response_bytes(&self) -> u64 {
        self.response_bytes
    }

    /// Returns evidence binding the raw response to immutable physical storage.
    pub const fn sealed_capture_receipt(&self) -> EvidenceDigest {
        self.sealed_capture_receipt
    }

    /// Returns evidence binding the raw metadata admission response to immutable storage.
    pub const fn sealed_metadata_capture_receipt(&self) -> EvidenceDigest {
        self.sealed_metadata_capture_receipt
    }

    /// Returns the exact source/schema/entitlement/adjustment mapping identity.
    pub const fn contract_identity(&self) -> EvidenceDigest {
        self.contract_identity
    }

    /// Returns the exact canonical/provider/classification/reference mapping identity.
    pub const fn instrument_authority_identity(&self) -> EvidenceDigest {
        self.instrument_authority_identity
    }

    /// Returns the exact provider-local handoff identity consumed by shared publication authority.
    pub const fn handoff_identity(&self) -> EvidenceDigest {
        self.handoff_identity
    }

    /// Consumes the complete provider-local publication handoff without dropping gaps or native
    /// action evidence.
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        TiingoRequestSpec,
        Vec<ResearchObservation>,
        Option<ExtractionRevisionPlan>,
        Vec<TiingoEodSurfaceGap>,
        Vec<TiingoEodProviderActionEvidence>,
        EvidenceDigest,
        EvidenceDigest,
        EvidenceDigest,
        EvidenceDigest,
        EvidenceDigest,
        EvidenceDigest,
        u64,
    ) {
        (
            self.request,
            self.observations,
            self.revision_plan,
            self.gaps,
            self.provider_actions,
            self.request_identity,
            self.sealed_capture_receipt,
            self.sealed_metadata_capture_receipt,
            self.contract_identity,
            self.instrument_authority_identity,
            self.handoff_identity,
            self.response_bytes,
        )
    }
}

/// Provider-local history handoff assembled only after every sealed page is mapped and the
/// returned financial dates are reconciled against exact versioned calendar evidence.
///
/// This remains a pre-publication candidate. The shared data plane must bind this exact completion
/// identity and financial coverage disposition into the immutable generation manifest. HTTP
/// request-graph completion does not by itself prove complete financial-date coverage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoCompletedHistoryCandidate {
    capture: TiingoCompletedHistoryCapture,
    pages: Box<[TiingoMappedEodPage]>,
    expected_session_evidence: TiingoEodExpectedSessionEvidence,
    expected_session_validation: TiingoEodExpectedSessionValidationReceipt,
    returned_sessions: Box<[CalendarDate]>,
    missing_expected_sessions: Box<[CalendarDate]>,
    financial_coverage: TiingoEodFinancialCoverageDisposition,
    total_observations: u64,
    total_gaps: u64,
    total_provider_actions: u64,
    completion_identity: EvidenceDigest,
}

impl TiingoCompletedHistoryCandidate {
    /// Closes one history handoff only when the HTTP graph and exact calendar reconciliation agree.
    pub fn try_new(
        capture: TiingoCompletedHistoryCapture,
        pages: Vec<TiingoMappedEodPage>,
        instrument: &TiingoEodInstrumentAuthority,
        expected_session_authority: &dyn TiingoEodExpectedSessionAuthority,
    ) -> Result<Self, TiingoEodMapError> {
        if pages.len() != capture.pages().len()
            || pages
                .iter()
                .zip(capture.pages())
                .any(|(page, sealed_page)| {
                    page.request() != sealed_page.request()
                        || page.request_identity() != sealed_page.request().request_identity()
                        || page.sealed_capture_receipt()
                            != sealed_page.sealed_capture_receipt()
                        || page.response_bytes() != sealed_page.response_bytes()
                })
        {
            return Err(TiingoEodMapError::IncompleteHistory);
        }
        let Some(first) = pages.first() else {
            return Err(TiingoEodMapError::IncompleteHistory);
        };
        if pages.iter().any(|page| {
            page.contract_identity() != first.contract_identity()
                || page.instrument_authority_identity()
                    != first.instrument_authority_identity()
                || page.sealed_metadata_capture_receipt()
                    != first.sealed_metadata_capture_receipt()
        }) {
            return Err(TiingoEodMapError::IncompleteHistory);
        }
        if capture.plan().ticker() != instrument.ticker()
            || first.instrument_authority_identity() != instrument.mapping_identity()
        {
            return Err(TiingoEodMapError::AuthorityMismatch);
        }
        let mut total_observations = 0_u64;
        let mut total_gaps = 0_u64;
        let mut total_provider_actions = 0_u64;
        for page in &pages {
            total_observations = total_observations
                .checked_add(
                    u64::try_from(page.observations().len())
                        .map_err(|_| TiingoEodMapError::Allocation)?,
                )
                .ok_or(TiingoEodMapError::Allocation)?;
            total_gaps = total_gaps
                .checked_add(
                    u64::try_from(page.gaps().len())
                        .map_err(|_| TiingoEodMapError::Allocation)?,
                )
                .ok_or(TiingoEodMapError::Allocation)?;
            total_provider_actions = total_provider_actions
                .checked_add(
                    u64::try_from(page.provider_actions().len())
                        .map_err(|_| TiingoEodMapError::Allocation)?,
                )
                .ok_or(TiingoEodMapError::Allocation)?;
        }
        if total_provider_actions != capture.total_rows() {
            return Err(TiingoEodMapError::IncompleteHistory);
        }
        let returned_sessions = collect_returned_sessions(&capture, &pages)?;
        let expected_session_request =
            TiingoEodExpectedSessionRequest::new(capture.plan(), instrument);
        let expected_session_evidence = expected_session_authority
            .resolve_expected_sessions(&expected_session_request)?;
        validate_expected_session_evidence(
            &expected_session_request,
            &expected_session_evidence,
        )?;
        let initial_validation =
            expected_session_authority.validate_current(&expected_session_evidence)?;
        validate_expected_session_validation(
            &expected_session_evidence,
            &initial_validation,
        )?;
        let missing_expected_sessions = reconcile_expected_sessions(
            expected_session_evidence.expected_sessions(),
            &returned_sessions,
        )?;
        let expected_session_validation =
            expected_session_authority.validate_current(&expected_session_evidence)?;
        validate_expected_session_validation(
            &expected_session_evidence,
            &expected_session_validation,
        )?;
        if expected_session_validation.validated_at() < initial_validation.validated_at() {
            return Err(TiingoEodMapError::InvalidExpectedSessionEvidence);
        }
        let financial_coverage = if missing_expected_sessions.is_empty() {
            TiingoEodFinancialCoverageDisposition::Complete
        } else {
            TiingoEodFinancialCoverageDisposition::MissingExpectedSessions
        };
        let completion_identity = history_completion_identity(
            &capture,
            &pages,
            &expected_session_evidence,
            &expected_session_validation,
            &returned_sessions,
            &missing_expected_sessions,
            financial_coverage,
            total_observations,
            total_gaps,
            total_provider_actions,
        );
        Ok(Self {
            capture,
            pages: pages.into_boxed_slice(),
            expected_session_evidence,
            expected_session_validation,
            returned_sessions: returned_sessions.into_boxed_slice(),
            missing_expected_sessions: missing_expected_sessions.into_boxed_slice(),
            financial_coverage,
            total_observations,
            total_gaps,
            total_provider_actions,
            completion_identity,
        })
    }

    /// Returns the complete exact request plan.
    pub const fn plan(&self) -> &TiingoHistoryPlan {
        self.capture.plan()
    }

    /// Returns the complete surface-neutral raw/native history evidence.
    pub const fn capture(&self) -> &TiingoCompletedHistoryCapture {
        &self.capture
    }

    /// Returns every sealed/mapped page in exact plan order.
    pub fn pages(&self) -> &[TiingoMappedEodPage] {
        &self.pages
    }

    /// Returns exact versioned calendar evidence and the complete expected-session set.
    pub const fn expected_session_evidence(&self) -> &TiingoEodExpectedSessionEvidence {
        &self.expected_session_evidence
    }

    /// Returns the durable currentness validation retained for shared publication authority.
    pub const fn expected_session_validation(
        &self,
    ) -> &TiingoEodExpectedSessionValidationReceipt {
        &self.expected_session_validation
    }

    /// Returns every provider date actually returned, independently of surface null gaps.
    pub fn returned_sessions(&self) -> &[CalendarDate] {
        &self.returned_sessions
    }

    /// Returns expected sessions with no provider-native row.
    ///
    /// These are financial-date gaps and are deliberately separate from raw/adjusted OHLCV null
    /// gaps inside a row that did arrive.
    pub fn missing_expected_sessions(&self) -> &[CalendarDate] {
        &self.missing_expected_sessions
    }

    /// Returns whether the completed HTTP graph covered every expected financial session.
    pub const fn financial_coverage(&self) -> TiingoEodFinancialCoverageDisposition {
        self.financial_coverage
    }

    /// Returns explicit exhaustion of application windows without claiming a provider cursor.
    pub const fn terminal(&self) -> TiingoHistoryTerminalDisposition {
        self.capture.terminal()
    }

    /// Returns exact retained raw response bytes across every page.
    pub const fn total_response_bytes(&self) -> u64 {
        self.capture.total_response_bytes()
    }

    /// Returns canonical observation cardinality across every page.
    pub const fn total_observations(&self) -> u64 {
        self.total_observations
    }

    /// Returns explicit incomplete-surface cardinality across every page.
    pub const fn total_gaps(&self) -> u64 {
        self.total_gaps
    }

    /// Returns provider-native dividend/split evidence cardinality across every page.
    pub const fn total_provider_actions(&self) -> u64 {
        self.total_provider_actions
    }

    /// Returns the exact plan/page/calendar/financial-coverage identity the shared manifest must
    /// retain.
    pub const fn completion_identity(&self) -> EvidenceDigest {
        self.completion_identity
    }
}

#[allow(clippy::too_many_arguments)]
fn history_completion_identity(
    capture: &TiingoCompletedHistoryCapture,
    pages: &[TiingoMappedEodPage],
    expected_session_evidence: &TiingoEodExpectedSessionEvidence,
    expected_session_validation: &TiingoEodExpectedSessionValidationReceipt,
    returned_sessions: &[CalendarDate],
    missing_expected_sessions: &[CalendarDate],
    financial_coverage: TiingoEodFinancialCoverageDisposition,
    total_observations: u64,
    total_gaps: u64,
    total_provider_actions: u64,
) -> EvidenceDigest {
    let mut hasher = Sha256::new();
    append_field(
        &mut hasher,
        b"market-squawk/tiingo/eod-history-completion/v3",
    );
    append_field(&mut hasher, &capture.completion_identity().bytes());
    append_field(
        &mut hasher,
        &u64::try_from(pages.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    for page in pages {
        append_field(&mut hasher, &page.request_identity().bytes());
        append_field(&mut hasher, &page.handoff_identity().bytes());
        append_field(&mut hasher, &page.sealed_capture_receipt().bytes());
        append_field(&mut hasher, &page.response_bytes().to_be_bytes());
    }
    append_field(
        &mut hasher,
        &expected_session_evidence.evidence_identity().bytes(),
    );
    append_field(
        &mut hasher,
        expected_session_evidence.calendar_id().as_str().as_bytes(),
    );
    append_field(
        &mut hasher,
        expected_session_evidence
            .calendar_revision()
            .metadata_revision()
            .as_source_identifier()
            .as_str()
            .as_bytes(),
    );
    append_evidence_digest(
        &mut hasher,
        expected_session_evidence
            .calendar_revision()
            .payload_evidence()
            .content_digest(),
    );
    append_field(
        &mut hasher,
        expected_session_evidence
            .authority_generation()
            .as_str()
            .as_bytes(),
    );
    append_field(
        &mut hasher,
        &expected_session_evidence
            .calendar_available_at()
            .unix_nanos()
            .to_be_bytes(),
    );
    append_field(
        &mut hasher,
        &expected_session_evidence
            .resolved_at()
            .unix_nanos()
            .to_be_bytes(),
    );
    append_evidence_digest(
        &mut hasher,
        expected_session_evidence.resolution_receipt(),
    );
    append_evidence_digest(
        &mut hasher,
        expected_session_validation.receipt_identity(),
    );
    append_field(
        &mut hasher,
        &expected_session_validation
            .validated_at()
            .unix_nanos()
            .to_be_bytes(),
    );
    append_dates(
        &mut hasher,
        expected_session_evidence.expected_sessions(),
    );
    append_dates(&mut hasher, returned_sessions);
    append_dates(&mut hasher, missing_expected_sessions);
    append_field(
        &mut hasher,
        &[match financial_coverage {
            TiingoEodFinancialCoverageDisposition::Complete => 0,
            TiingoEodFinancialCoverageDisposition::MissingExpectedSessions => 1,
        }],
    );
    for total in [
        total_observations,
        total_gaps,
        total_provider_actions,
    ] {
        append_field(&mut hasher, &total.to_be_bytes());
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into())
}

fn expected_session_evidence_identity(
    request_identity: EvidenceDigest,
    calendar_id: &SourceIdentifier,
    calendar_revision: &RevisionBoundPayloadEvidence,
    authority_generation: &SourceIdentifier,
    calendar_available_at: Timestamp,
    resolved_at: Timestamp,
    resolution_receipt: EvidenceDigest,
    expected_sessions: &[CalendarDate],
) -> EvidenceDigest {
    let mut hasher = Sha256::new();
    append_field(
        &mut hasher,
        b"market-squawk/tiingo/eod-expected-session-evidence/v2",
    );
    append_evidence_digest(&mut hasher, request_identity);
    append_field(&mut hasher, calendar_id.as_str().as_bytes());
    append_field(
        &mut hasher,
        calendar_revision
            .metadata_revision()
            .as_source_identifier()
            .as_str()
            .as_bytes(),
    );
    append_evidence_digest(
        &mut hasher,
        calendar_revision
            .payload_evidence()
            .content_digest(),
    );
    append_field(&mut hasher, authority_generation.as_str().as_bytes());
    append_field(
        &mut hasher,
        &calendar_available_at.unix_nanos().to_be_bytes(),
    );
    append_field(&mut hasher, &resolved_at.unix_nanos().to_be_bytes());
    append_evidence_digest(&mut hasher, resolution_receipt);
    append_dates(&mut hasher, expected_sessions);
    EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into())
}

fn validate_expected_session_evidence(
    request: &TiingoEodExpectedSessionRequest,
    evidence: &TiingoEodExpectedSessionEvidence,
) -> Result<(), TiingoEodMapError> {
    let inclusive_calendar_days = request
        .end_date()
        .days_since_unix_epoch()
        .checked_sub(request.start_date().days_since_unix_epoch())
        .and_then(|days| days.checked_add(1))
        .and_then(|days| usize::try_from(days).ok())
        .ok_or(TiingoEodMapError::InvalidExpectedSessionEvidence)?;
    let rebuilt_identity = expected_session_evidence_identity(
        request.request_identity(),
        evidence.calendar_id(),
        evidence.calendar_revision(),
        evidence.authority_generation(),
        evidence.calendar_available_at(),
        evidence.resolved_at(),
        evidence.resolution_receipt(),
        evidence.expected_sessions(),
    );
    if evidence.request_identity() != request.request_identity()
        || evidence
            .calendar_revision()
            .payload_evidence()
            .content_digest()
            .bytes()
            == [0; 32]
        || evidence.calendar_available_at().unix_nanos() < 0
        || evidence.resolved_at() < evidence.calendar_available_at()
        || evidence.resolution_receipt().bytes() == [0; 32]
        || evidence.expected_sessions().len() > inclusive_calendar_days
        || evidence.expected_sessions().iter().any(|date| {
            *date < request.start_date() || *date > request.end_date()
        })
        || evidence
            .expected_sessions()
            .windows(2)
            .any(|dates| dates[0] >= dates[1])
        || evidence.evidence_identity() != rebuilt_identity
    {
        return Err(TiingoEodMapError::InvalidExpectedSessionEvidence);
    }
    Ok(())
}

fn validate_expected_session_validation(
    evidence: &TiingoEodExpectedSessionEvidence,
    receipt: &TiingoEodExpectedSessionValidationReceipt,
) -> Result<(), TiingoEodMapError> {
    let rebuilt = TiingoEodExpectedSessionValidationReceipt::try_new(
        evidence,
        receipt.authority_generation().clone(),
        receipt.validated_at(),
        receipt.authority_receipt(),
    )?;
    if receipt != &rebuilt {
        return Err(TiingoEodMapError::InvalidExpectedSessionEvidence);
    }
    Ok(())
}

fn collect_returned_sessions(
    capture: &TiingoCompletedHistoryCapture,
    pages: &[TiingoMappedEodPage],
) -> Result<Vec<CalendarDate>, TiingoEodMapError> {
    let capacity = usize::try_from(capture.total_rows())
        .map_err(|_| TiingoEodMapError::Allocation)?;
    let mut returned_sessions = Vec::new();
    returned_sessions
        .try_reserve_exact(capacity)
        .map_err(|_| TiingoEodMapError::Allocation)?;
    let mut previous_date = None;
    for (page, sealed_page) in pages.iter().zip(capture.pages()) {
        let (start_date, end_date) = match page.request().scope() {
            TiingoRequestScope::History {
                start_date,
                end_date,
                ..
            } => (*start_date, *end_date),
            _ => return Err(TiingoEodMapError::IncompleteHistory),
        };
        if page.provider_actions().len() != sealed_page.row_digests().len() {
            return Err(TiingoEodMapError::IncompleteHistory);
        }
        for (action, row_digest) in page
            .provider_actions()
            .iter()
            .zip(sealed_page.row_digests())
        {
            let provider_date = action.provider_date();
            if action.row_digest() != *row_digest
                || provider_date < start_date
                || provider_date > end_date
                || previous_date.is_some_and(|previous| previous >= provider_date)
            {
                return Err(TiingoEodMapError::IncompleteHistory);
            }
            returned_sessions.push(provider_date);
            previous_date = Some(provider_date);
        }
    }
    if returned_sessions.len() != capacity {
        return Err(TiingoEodMapError::IncompleteHistory);
    }
    Ok(returned_sessions)
}

fn reconcile_expected_sessions(
    expected_sessions: &[CalendarDate],
    returned_sessions: &[CalendarDate],
) -> Result<Vec<CalendarDate>, TiingoEodMapError> {
    let mut missing = Vec::new();
    missing
        .try_reserve_exact(expected_sessions.len())
        .map_err(|_| TiingoEodMapError::Allocation)?;
    let mut expected_index = 0_usize;
    let mut returned_index = 0_usize;
    while expected_index < expected_sessions.len() {
        let expected = expected_sessions[expected_index];
        match returned_sessions.get(returned_index).copied() {
            Some(returned) if returned < expected => {
                return Err(TiingoEodMapError::UnexpectedReturnedSession);
            }
            Some(returned) if returned == expected => {
                returned_index = returned_index
                    .checked_add(1)
                    .ok_or(TiingoEodMapError::Allocation)?;
            }
            Some(_) | None => missing.push(expected),
        }
        expected_index = expected_index
            .checked_add(1)
            .ok_or(TiingoEodMapError::Allocation)?;
    }
    if returned_index != returned_sessions.len() {
        return Err(TiingoEodMapError::UnexpectedReturnedSession);
    }
    Ok(missing)
}

fn append_dates(hasher: &mut Sha256, dates: &[CalendarDate]) {
    append_field(
        hasher,
        &u64::try_from(dates.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    for date in dates {
        append_field(hasher, date.to_string().as_bytes());
    }
}

fn append_evidence_digest(hasher: &mut Sha256, digest: EvidenceDigest) {
    append_field(
        hasher,
        &[match digest.algorithm() {
            DigestAlgorithm::Sha256 => 1,
            DigestAlgorithm::Blake3 => 2,
        }],
    );
    append_field(hasher, &digest.bytes());
}

/// Maps strict Tiingo equity/ETF daily rows to distinct raw and adjusted canonical bars.
///
/// Missing OHLCV components remain explicit gaps. Source-reported dividend and split fields stay
/// native evidence until a separate corporate-action contract proves their full semantics.
pub fn map_eod_bars(
    input: TiingoEodMappingInput<'_>,
) -> Result<TiingoMappedEodPage, TiingoEodMapError> {
    validate_authority(&input)?;
    validate_capture(&input)?;
    validate_response(&input)?;

    let surface_capacity = input
        .response
        .rows()
        .len()
        .checked_mul(2)
        .ok_or(TiingoEodMapError::Allocation)?;
    let mut observations = Vec::new();
    observations
        .try_reserve_exact(surface_capacity)
        .map_err(|_| TiingoEodMapError::Allocation)?;
    let mut gaps = Vec::new();
    gaps
        .try_reserve_exact(surface_capacity)
        .map_err(|_| TiingoEodMapError::Allocation)?;
    let mut provider_actions = Vec::new();
    provider_actions
        .try_reserve_exact(input.response.rows().len())
        .map_err(|_| TiingoEodMapError::Allocation)?;

    let coverage = input.metadata.metadata().coverage();
    for row in input.response.rows() {
        if !coverage.contains(row.date()) {
            return Err(TiingoEodMapError::OutsideMetadataCoverage);
        }
        let time_semantics = resolve_time(&input, row.date())?;
        map_surface(
            &input,
            row,
            TiingoEodSurface::Raw,
            time_semantics.clone(),
            &mut observations,
            &mut gaps,
        )?;
        map_surface(
            &input,
            row,
            TiingoEodSurface::Adjusted,
            time_semantics,
            &mut observations,
            &mut gaps,
        )?;
        provider_actions.push(TiingoEodProviderActionEvidence {
            provider_date: row.date(),
            cash_dividend: row.cash_dividend(),
            split_factor: row.split_factor(),
            row_digest: row.row_digest(),
        });
    }
    let revision_plan = (!observations.is_empty())
        .then(|| ExtractionRevisionPlan::locally_observed(observations.len()))
        .transpose()?;
    let request_identity = input.response.evidence().request().request_identity();
    let response_bytes = input.response.evidence().response_bytes();
    let sealed_capture_receipt = input.sealed_capture.receipt_digest();
    let sealed_metadata_capture_receipt = input.sealed_metadata_capture.receipt_digest();
    let contract_identity = input.contract.mapping_identity();
    let instrument_authority_identity = input.instrument.mapping_identity();
    let handoff_identity = eod_handoff_identity(
        request_identity,
        response_bytes,
        sealed_capture_receipt,
        sealed_metadata_capture_receipt,
        contract_identity,
        instrument_authority_identity,
        &observations,
        revision_plan.as_ref(),
        &gaps,
        &provider_actions,
    )?;
    Ok(TiingoMappedEodPage {
        request: input.response.evidence().request().clone(),
        observations,
        revision_plan,
        gaps,
        provider_actions,
        request_identity,
        sealed_capture_receipt,
        sealed_metadata_capture_receipt,
        contract_identity,
        instrument_authority_identity,
        handoff_identity,
        response_bytes,
    })
}

#[allow(clippy::too_many_arguments)]
fn eod_handoff_identity(
    request_identity: EvidenceDigest,
    response_bytes: u64,
    sealed_capture_receipt: EvidenceDigest,
    sealed_metadata_capture_receipt: EvidenceDigest,
    contract_identity: EvidenceDigest,
    instrument_authority_identity: EvidenceDigest,
    observations: &[ResearchObservation],
    revision_plan: Option<&ExtractionRevisionPlan>,
    gaps: &[TiingoEodSurfaceGap],
    provider_actions: &[TiingoEodProviderActionEvidence],
) -> Result<EvidenceDigest, TiingoEodMapError> {
    if revision_plan.map(ExtractionRevisionPlan::len) != (!observations.is_empty()).then_some(observations.len())
        || revision_plan.is_some_and(|plan| !plan.is_locally_observed())
        || observations
            .iter()
            .any(|observation| !matches!(observation, ResearchObservation::MarketBar(_)))
    {
        return Err(TiingoEodMapError::InvalidCanonicalEvidence);
    }
    let mut canonical_writer = BoundedCanonicalDigestWriter::new(
        MAX_EOD_HANDOFF_CANONICAL_BYTES,
    );
    serde_json::to_writer(&mut canonical_writer, &observations)
        .map_err(|_| TiingoEodMapError::InvalidCanonicalEvidence)?;
    let (canonical_bytes, canonical_digest) = canonical_writer.finish();
    let mut hasher = Sha256::new();
    append_field(&mut hasher, b"market-squawk/tiingo/eod-handoff/v3");
    for digest in [
        request_identity,
        sealed_capture_receipt,
        sealed_metadata_capture_receipt,
        contract_identity,
        instrument_authority_identity,
    ] {
        append_field(&mut hasher, &digest.bytes());
    }
    append_field(&mut hasher, &response_bytes.to_be_bytes());
    append_field(
        &mut hasher,
        &u64::try_from(observations.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    append_field(&mut hasher, &canonical_bytes.to_be_bytes());
    append_evidence_digest(&mut hasher, canonical_digest);
    match revision_plan {
        Some(revision_plan) => {
            append_field(
                &mut hasher,
                &u64::try_from(revision_plan.len())
                    .unwrap_or(u64::MAX)
                    .to_be_bytes(),
            );
            append_field(
                &mut hasher,
                &u64::try_from(revision_plan.retained_bytes())
                    .unwrap_or(u64::MAX)
                    .to_be_bytes(),
            );
            append_field(&mut hasher, b"locally-observed-content");
        }
        None => append_field(&mut hasher, b"no-canonical-observations"),
    }
    append_field(
        &mut hasher,
        &u64::try_from(gaps.len()).unwrap_or(u64::MAX).to_be_bytes(),
    );
    for gap in gaps {
        append_field(&mut hasher, gap.provider_date.to_string().as_bytes());
        append_field(
            &mut hasher,
            &[
                match gap.surface {
                    TiingoEodSurface::Raw => 0,
                    TiingoEodSurface::Adjusted => 1,
                },
                match gap.reason {
                    TiingoEodSurfaceGapReason::MissingOhlc => 0,
                    TiingoEodSurfaceGapReason::MissingVolume => 1,
                },
            ],
        );
        append_field(&mut hasher, &gap.row_digest.bytes());
    }
    append_field(
        &mut hasher,
        &u64::try_from(provider_actions.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    for action in provider_actions {
        append_field(&mut hasher, action.provider_date.to_string().as_bytes());
        for value in [action.cash_dividend, action.split_factor] {
            match value {
                Some(value) => {
                    append_field(&mut hasher, b"present");
                    append_field(&mut hasher, value.to_string().as_bytes());
                }
                None => append_field(&mut hasher, b"missing"),
            }
        }
        append_field(&mut hasher, &action.row_digest.bytes());
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hasher.finalize().into(),
    ))
}

struct BoundedCanonicalDigestWriter {
    hasher: Sha256,
    written: u64,
    maximum: u64,
}

impl BoundedCanonicalDigestWriter {
    fn new(maximum: u64) -> Self {
        Self {
            hasher: Sha256::new(),
            written: 0,
            maximum,
        }
    }

    fn finish(self) -> (u64, EvidenceDigest) {
        (
            self.written,
            EvidenceDigest::new(DigestAlgorithm::Sha256, self.hasher.finalize().into()),
        )
    }
}

impl io::Write for BoundedCanonicalDigestWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let next = self
            .written
            .checked_add(u64::try_from(buffer.len()).map_err(|_| {
                io::Error::other("canonical serialization byte count overflow")
            })?)
            .ok_or_else(|| io::Error::other("canonical serialization byte count overflow"))?;
        if next > self.maximum {
            return Err(io::Error::other(
                "canonical serialization exceeded admitted byte ceiling",
            ));
        }
        self.hasher.update(buffer);
        self.written = next;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn validate_authority(input: &TiingoEodMappingInput<'_>) -> Result<(), TiingoEodMapError> {
    let request = input.response.evidence().request();
    let metadata = input.metadata.metadata();
    if input.contract.source_id().as_str() != TIINGO_SOURCE_ID
        || request.ticker() != input.instrument.ticker()
        || metadata.ticker() != input.instrument.ticker()
        || metadata.exchange_code() != input.instrument.provider_exchange_code().as_str()
        || input.instrument.provider_instrument_id().as_str() != input.instrument.ticker().as_str()
        || metadata.exchange_code() == TIINGO_MUTUAL_FUND_EXCHANGE_CODE
        || matches!(metadata.coverage(), TiingoCoverage::Unsupported)
        || input.metadata.evidence().request().endpoint() != TiingoEndpointFamily::Metadata
        || input.metadata.evidence().request().ticker() != input.instrument.ticker()
        || !(200..300).contains(&input.metadata.evidence().status())
        || input.metadata.evidence().response_bytes() == 0
        || input.metadata.evidence().received_at() > input.metadata.evidence().decoded_at()
        || input.metadata.evidence().decoded_at() > input.response.evidence().received_at()
        || input.instrument.resolved_at() > input.response.evidence().received_at()
        || input.metadata.evidence().native_contract_revision()
            != input.contract.native_schema_revision()
        || input.response.evidence().native_contract_revision()
            != input.contract.native_schema_revision()
        || input.metadata.evidence().entitlement_generation()
            != input.contract.entitlement_generation_identity()
        || input.response.evidence().entitlement_generation()
            != input.contract.entitlement_generation_identity()
    {
        return Err(TiingoEodMapError::AuthorityMismatch);
    }
    Ok(())
}

fn validate_capture(input: &TiingoEodMappingInput<'_>) -> Result<(), TiingoEodMapError> {
    let response = input.response.evidence();
    let request = response.request();
    let capture = input.sealed_capture.capture();
    let Some(page) = capture.pages().first() else {
        return Err(TiingoEodMapError::CaptureMismatch);
    };
    let metadata_capture = input.sealed_metadata_capture.capture();
    let Some(metadata_page) = metadata_capture.pages().first() else {
        return Err(TiingoEodMapError::CaptureMismatch);
    };
    let metadata_response = input.metadata.evidence();
    let expected_dataset = match request.endpoint() {
        TiingoEndpointFamily::LatestDailyPrices => TIINGO_LATEST_DATASET,
        TiingoEndpointFamily::HistoricalDailyPrices => TIINGO_HISTORY_DATASET,
        TiingoEndpointFamily::Metadata => return Err(TiingoEodMapError::WrongResponseFamily),
    };
    if capture.pages().len() != 1
        || capture.source_id() != input.contract.source_id()
        || capture.metadata_revision() != input.contract.source_contract_revision()
        || capture.dataset().as_str() != expected_dataset
        || capture.terminal() != ProviderCaptureTerminalDisposition::StandaloneResponse
        || capture.request_set_identity() != request.request_identity()
        || capture.total_body_bytes() != response.response_bytes()
        || page.request_identity() != request.request_identity()
        || page.http_status() != response.status()
        || page.body_bytes() != response.response_bytes()
        || page.body_digest() != response.body_digest()
        || page.received_at() != response.received_at()
        || metadata_capture.pages().len() != 1
        || metadata_capture.source_id() != input.contract.source_id()
        || metadata_capture.metadata_revision() != input.contract.source_contract_revision()
        || metadata_capture.dataset().as_str() != "tiingo-daily-metadata"
        || metadata_capture.terminal()
            != ProviderCaptureTerminalDisposition::StandaloneResponse
        || metadata_capture.request_set_identity()
            != metadata_response.request().request_identity()
        || metadata_capture.total_body_bytes() != metadata_response.response_bytes()
        || metadata_page.request_identity() != metadata_response.request().request_identity()
        || metadata_page.http_status() != metadata_response.status()
        || metadata_page.body_bytes() != metadata_response.response_bytes()
        || metadata_page.body_digest() != metadata_response.body_digest()
        || metadata_page.received_at() != metadata_response.received_at()
    {
        return Err(TiingoEodMapError::CaptureMismatch);
    }
    Ok(())
}

fn validate_response(input: &TiingoEodMappingInput<'_>) -> Result<(), TiingoEodMapError> {
    let evidence = input.response.evidence();
    let disposition = input.response.disposition();
    let row_count = u32::try_from(input.response.rows().len()).unwrap_or(u32::MAX);
    let pagination_matches = match (
        evidence.request().scope(),
        input.response.pagination(),
    ) {
        (TiingoRequestScope::Latest, TiingoPaginationEvidence::NotApplicable) => true,
        (
            TiingoRequestScope::History { page: expected, .. },
            TiingoPaginationEvidence::ApplicationDateWindow(actual),
        ) => expected == &actual,
        _ => false,
    };
    if evidence.received_at() > evidence.decoded_at()
        || evidence.decoded_at() > input.ingested_at
        || !(200..300).contains(&evidence.status())
        || evidence.response_bytes() == 0
        || disposition.requested_symbols() != 1
        || disposition.returned_rows() != row_count
        || disposition.returned_symbols() != u16::from(row_count != 0)
        || disposition.missing_symbols() != u16::from(row_count == 0)
        || disposition.response_bytes() != evidence.response_bytes()
        || !pagination_matches
    {
        return Err(TiingoEodMapError::InvalidDisposition);
    }
    Ok(())
}

fn resolve_time(
    input: &TiingoEodMappingInput<'_>,
    provider_date: CalendarDate,
) -> Result<BarTimeSemantics, TiingoEodMapError> {
    input.bar_time_authority.validate_current()?;
    let request = TiingoEodBarTimeRequest {
        instrument_id: input.instrument.instrument_id(),
        venue_id: input.instrument.venue_id().clone(),
        provider_instrument_id: input.instrument.provider_instrument_id().clone(),
        ticker: input.instrument.ticker().clone(),
        provider_date,
    };
    let semantics = input.bar_time_authority.resolve(&request)?;
    input.bar_time_authority.validate_current()?;
    if semantics.period_end_exclusive() > input.response.evidence().received_at() {
        return Err(TiingoEodMapError::InvalidTimeAuthority);
    }
    Ok(semantics)
}

fn map_surface(
    input: &TiingoEodMappingInput<'_>,
    row: &TiingoEodRow,
    surface: TiingoEodSurface,
    time_semantics: BarTimeSemantics,
    observations: &mut Vec<ResearchObservation>,
    gaps: &mut Vec<TiingoEodSurfaceGap>,
) -> Result<(), TiingoEodMapError> {
    let (ohlc, volume) = match surface {
        TiingoEodSurface::Raw => (row.raw_ohlc(), row.volume()),
        TiingoEodSurface::Adjusted => (row.adjusted_ohlc(), row.adjusted_volume()),
    };
    let (Some(open), Some(high), Some(low), Some(close)) = ohlc else {
        gaps.push(TiingoEodSurfaceGap {
            provider_date: row.date(),
            surface,
            reason: TiingoEodSurfaceGapReason::MissingOhlc,
            row_digest: row.row_digest(),
        });
        return Ok(());
    };
    let Some(volume) = volume else {
        gaps.push(TiingoEodSurfaceGap {
            provider_date: row.date(),
            surface,
            reason: TiingoEodSurfaceGapReason::MissingVolume,
            row_digest: row.row_digest(),
        });
        return Ok(());
    };
    let provider_timestamp = time_semantics.provider_timestamp();
    let source_identifier = SourceIdentifier::try_from(format!(
        "tiingo-eod-{}-{}-{}",
        input.instrument.ticker(),
        row.date(),
        match surface {
            TiingoEodSurface::Raw => "raw",
            TiingoEodSurface::Adjusted => "adjusted",
        }
    ))
    .map_err(|_| TiingoEodMapError::InvalidCanonicalIdentity)?;
    let provenance = ResearchProvenance::try_new(ResearchProvenanceInput {
        source_id: input.contract.source_id().clone(),
        instrument_id: Some(input.instrument.instrument_id()),
        venue_id: Some(input.instrument.venue_id().clone()),
        source_identifier,
        source_timestamp: Some(provider_timestamp),
        received_at: input.response.evidence().received_at(),
        ingested_at: input.ingested_at,
        quality: DataQuality::Aggregated,
        payload_reference: PayloadReference::ContentHash(PayloadHash::new(
            row.row_digest().algorithm(),
            row.row_digest().bytes(),
        )),
        availability: AvailabilityEvidence::local_first_observed(
            input.response.evidence().received_at(),
        ),
    })
    .map_err(|_| TiingoEodMapError::InvalidCanonicalEvidence)?;
    let time = ResearchTime::new(
        provider_timestamp,
        None,
        input.authority_seed_revision,
        None,
    )
    .map_err(|_| TiingoEodMapError::InvalidCanonicalEvidence)?;
    let context = ResearchContext::new(provenance, time)
        .map_err(|_| TiingoEodMapError::InvalidCanonicalEvidence)?;
    let (feed, adjustment) = match surface {
        TiingoEodSurface::Raw => (input.contract.raw_feed().clone(), MarketBarAdjustment::Raw),
        TiingoEodSurface::Adjusted => (
            input.contract.adjusted_feed().clone(),
            input.contract.adjusted_adjustment(),
        ),
    };
    let interval = SourceIdentifier::try_from(TIINGO_DAILY_INTERVAL)
        .map_err(|_| TiingoEodMapError::InvalidCanonicalIdentity)?;
    let observation = MarketBarObservation::new(
        context,
        input.instrument.provider_instrument_id().clone(),
        feed,
        interval,
        time_semantics,
        adjustment,
        Money::new(open, input.instrument.currency()),
        Money::new(high, input.instrument.currency()),
        Money::new(low, input.instrument.currency()),
        Money::new(close, input.instrument.currency()),
        volume,
        None,
        None,
    )
    .map_err(|_| TiingoEodMapError::InvalidCanonicalEvidence)?;
    observations.push(ResearchObservation::MarketBar(observation));
    Ok(())
}

fn append_field(hasher: &mut Sha256, value: &[u8]) {
    hasher.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(value);
}

const fn adjustment_discriminant(adjustment: MarketBarAdjustment) -> u8 {
    match adjustment {
        MarketBarAdjustment::Raw => 0,
        MarketBarAdjustment::Split => 1,
        MarketBarAdjustment::Dividend => 2,
        MarketBarAdjustment::SpinOff => 3,
        MarketBarAdjustment::All => 4,
    }
}

/// Closed failure to construct canonical Tiingo EOD evidence.
#[derive(Debug, Error)]
pub enum TiingoEodMapError {
    /// Canonical/provider identity, ticker metadata, or equity/ETF classification disagreed.
    #[error("Tiingo EOD authority does not match the provider response")]
    AuthorityMismatch,
    /// Source/schema/entitlement/adjustment evidence was empty or self-contradictory.
    #[error("Tiingo EOD contract evidence is invalid")]
    InvalidContractEvidence,
    /// The sealed source-neutral receipt did not bind the response exactly.
    #[error("sealed Tiingo capture does not match the EOD response")]
    CaptureMismatch,
    /// The mapper received metadata instead of a latest/history daily-price response.
    #[error("Tiingo EOD mapper received the wrong response family")]
    WrongResponseFamily,
    /// Request, rows, bytes, pagination, status, or clocks disagreed.
    #[error("Tiingo EOD request disposition is inconsistent")]
    InvalidDisposition,
    /// A returned row fell outside the exact non-null metadata coverage interval.
    #[error("Tiingo EOD row is outside admitted metadata coverage")]
    OutsideMetadataCoverage,
    /// The independently governed session/calendar authority was invalid or revoked.
    #[error("Tiingo EOD time authority is unavailable or inconsistent")]
    InvalidTimeAuthority,
    /// Exact calendar revision, request binding, expected-session ordering, or currentness failed.
    #[error("Tiingo EOD expected-session evidence is unavailable or inconsistent")]
    InvalidExpectedSessionEvidence,
    /// Tiingo returned a provider date that the exact venue calendar did not expect.
    #[error("Tiingo EOD returned a date outside the expected venue sessions")]
    UnexpectedReturnedSession,
    /// One or more exact HTTP plan pages or mapped provider rows were absent or inconsistent.
    #[error("Tiingo EOD HTTP history request graph is incomplete")]
    IncompleteHistory,
    /// A code-owned canonical identity crossed the domain grammar or byte limit.
    #[error("Tiingo EOD canonical identity is invalid")]
    InvalidCanonicalIdentity,
    /// Canonical provenance, time, or OHLCV invariants rejected the supplied evidence.
    #[error("Tiingo EOD canonical evidence is invalid")]
    InvalidCanonicalEvidence,
    /// Bounded output allocation failed.
    #[error("Tiingo EOD canonical output allocation failed")]
    Allocation,
    /// Shared observed-revision plan construction failed.
    #[error(transparent)]
    Revision(#[from] ObservedRevisionError),
}
