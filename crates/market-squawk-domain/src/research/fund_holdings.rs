//! Provider-neutral fund filing, share-class, and portfolio-holding evidence.

use std::fmt;
use std::num::{NonZeroU32, NonZeroU64};

use serde::{Deserialize, Deserializer, Serialize};

use crate::{
    AvailabilityEvidence, CalendarDate, Currency, Cusip, DigestAlgorithm, EvidenceDigest,
    ExactPayloadEvidence, InstrumentId, Isin, MetadataRevision, SchemaVersion, SchemaVersionError,
    SourceId, SourceIdentifier, Timestamp,
};

/// Logical canonical schema identity for fund reports, share classes, and holdings.
pub const FUND_HOLDINGS_SCHEMA_NAME: &str = "market_squawk.fund_holdings";
/// Greenfield logical schema version; the data registry owns its exact Arrow fingerprint.
pub const FUND_HOLDINGS_SCHEMA_VERSION: u16 = 1;
/// Maximum exact source rows bound to one canonical fund record.
pub const MAX_FUND_SOURCE_ROWS: usize = 100_000;
/// Exact number of N-PORT holding-supplement table states required per holding.
pub const FUND_HOLDING_SUPPLEMENT_TABLE_COUNT: usize = 19;
/// Maximum source exchange/ticker associations retained for one share class.
pub const MAX_FUND_EXCHANGE_ASSOCIATIONS: usize = 64;
/// Maximum equally knowable accessions retained for one revision conflict.
pub const MAX_FUND_COMPETING_ACCESSIONS: usize = 64;

const MAX_FUND_TEXT_BYTES: usize = 2_048;
const MAX_FUND_DECIMAL_BYTES: usize = 128;

/// SEC investment-company filing family supplying one record.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FundSourceFamily {
    /// Form N-PORT portfolio reports and holdings.
    Nport,
    /// Form N-CEN annual reports and fund/share-class metadata.
    Ncen,
}

/// Explicit reason why a canonical fund field has no reported value.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FundMissingState {
    /// The source field or joined row was absent.
    SourceAbsent,
    /// The field does not apply to this form or instrument.
    NotApplicable,
    /// The source withheld or confidentially omitted the value.
    ConfidentialOrOmitted,
    /// A present source value failed the canonical contract.
    Invalid,
    /// The source generation cannot establish a complete answer.
    Unavailable,
    /// Required canonical identity could not be resolved.
    UnresolvedIdentity,
    /// The accepted provider schema expressly excludes part of the source population.
    DeclaredCoverageGap,
}

/// Explicit reason why a canonical fund field has no single value.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FundConflictState {
    /// Duplicate source rows assert different values for the same key.
    CompetingSourceRows,
    /// Equally knowable filing revisions disagree at the point-in-time cutoff.
    CompetingRevisions,
    /// More than one governed instrument mapping remains possible.
    ConflictingIdentity,
    /// Reported number, unit, or currency fields cannot be reconciled.
    IncompatibleUnitOrCurrency,
}

/// One reported value, an explicit missing state, or an explicit conflict state.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(tag = "state", content = "value", rename_all = "snake_case")]
pub enum FundReportedValue<T> {
    /// The source supplied one admitted value.
    Reported(T),
    /// No value exists for the retained reason.
    Missing(FundMissingState),
    /// No single value can be selected for the retained reason.
    Conflict(FundConflictState),
}

impl<T> FundReportedValue<T> {
    /// Returns the reported value without converting missing or conflict states to defaults.
    pub const fn reported(&self) -> Option<&T> {
        match self {
            Self::Reported(value) => Some(value),
            Self::Missing(_) | Self::Conflict(_) => None,
        }
    }

    /// Returns the missing state, if this field is explicitly missing.
    pub const fn missing(&self) -> Option<FundMissingState> {
        match self {
            Self::Missing(state) => Some(*state),
            Self::Reported(_) | Self::Conflict(_) => None,
        }
    }

    /// Returns the conflict state, if this field has no single value.
    pub const fn conflict(&self) -> Option<FundConflictState> {
        match self {
            Self::Conflict(state) => Some(*state),
            Self::Reported(_) | Self::Missing(_) => None,
        }
    }
}

/// Exact source decimal preserved beyond `rust_decimal`'s 96-bit coefficient ceiling.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct FundReportedDecimal(String);

impl FundReportedDecimal {
    /// Validates and preserves an exact fixed-point provider lexical value.
    pub fn try_from_str(value: &str) -> Result<Self, FundHoldingsError> {
        validate_reported_decimal(value)?;
        Ok(Self(value.to_owned()))
    }

    /// Validates and consumes an exact fixed-point value whose storage is already owned.
    pub fn try_from_boxed_str(value: Box<str>) -> Result<Self, FundHoldingsError> {
        validate_reported_decimal(&value)?;
        Ok(Self(value.into_string()))
    }

    /// Returns the exact provider lexical value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn validate_reported_decimal(value: &str) -> Result<(), FundHoldingsError> {
    let unsigned = value.strip_prefix('-').unwrap_or(value);
    let mut parts = unsigned.split('.');
    let integer = parts.next().unwrap_or_default();
    let fraction = parts.next();
    let valid = !value.is_empty()
        && value.len() <= MAX_FUND_DECIMAL_BYTES
        && !value.starts_with('+')
        && parts.next().is_none()
        && !integer.is_empty()
        && integer.bytes().all(|byte| byte.is_ascii_digit())
        && fraction.is_none_or(|digits| {
            !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
        });
    if valid {
        Ok(())
    } else {
        Err(FundHoldingsError::InvalidDecimal)
    }
}

impl<'de> Deserialize<'de> for FundReportedDecimal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::try_from_str(&value).map_err(serde::de::Error::custom)
    }
}

/// Bounded provider-authored display/classification text retained only as evidence.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct FundSourceText(String);

impl FundSourceText {
    /// Constructs bounded nonempty, trimmed text without control characters.
    pub fn try_from_string(value: impl Into<String>) -> Result<Self, FundHoldingsError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_FUND_TEXT_BYTES
            || value.trim() != value
            || value.chars().any(char::is_control)
        {
            return Err(FundHoldingsError::InvalidText);
        }
        Ok(Self(value))
    }

    /// Returns exact provider-authored text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for FundSourceText {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::try_from_string(value).map_err(serde::de::Error::custom)
    }
}

/// Exact unit attached to one source-reported holding balance.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum FundHoldingUnit {
    /// Shares or units of beneficial interest.
    Shares,
    /// Principal amount of a debt-like instrument.
    Principal,
    /// Derivative contracts.
    Contracts,
    /// A currency-denominated balance.
    Currency(Currency),
    /// An exact source-defined unit with no invented conversion.
    Other(SourceIdentifier),
}

/// Exact reported quantity and its non-collapsed unit.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FundHoldingQuantity {
    amount: FundReportedDecimal,
    unit: FundHoldingUnit,
}

impl FundHoldingQuantity {
    /// Constructs one exact source quantity.
    pub const fn new(amount: FundReportedDecimal, unit: FundHoldingUnit) -> Self {
        Self { amount, unit }
    }

    /// Returns the exact reported amount.
    pub const fn amount(&self) -> &FundReportedDecimal {
        &self.amount
    }

    /// Returns the exact source unit.
    pub const fn unit(&self) -> &FundHoldingUnit {
        &self.unit
    }
}

/// Exact source-reported monetary amount and currency.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FundCurrencyAmount {
    amount: FundReportedDecimal,
    currency: Currency,
}

impl FundCurrencyAmount {
    /// Constructs one exact currency amount without rounding.
    pub const fn new(amount: FundReportedDecimal, currency: Currency) -> Self {
        Self { amount, currency }
    }

    /// Returns the exact amount.
    pub const fn amount(&self) -> &FundReportedDecimal {
        &self.amount
    }

    /// Returns the reported accounting currency.
    pub const fn currency(&self) -> Currency {
        self.currency
    }
}

/// Complete provider-availability coverage for one source generation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum FundReleaseCoverage {
    /// The accepted source contract covers the represented release.
    Complete,
    /// The accepted bulk schema excludes an identified newer provider schema.
    AcceptedSchemaExclusion {
        /// Exact accepted schema revision.
        accepted_schema: SourceIdentifier,
        /// Exact provider schema known to be excluded.
        excluded_schema: SourceIdentifier,
    },
    /// Another explicit condition prevents complete coverage.
    Incomplete {
        /// Closed reason the generation is incomplete.
        reason: FundMissingState,
    },
}

impl FundReleaseCoverage {
    /// Returns whether absence within this generation may be treated as a complete negative result.
    pub const fn is_complete(&self) -> bool {
        matches!(self, Self::Complete)
    }
}

/// Separate filing and availability clocks without invented timestamp precision.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FundFilingChronology {
    report_period_end: FundReportedValue<CalendarDate>,
    report_date: FundReportedValue<CalendarDate>,
    filed_date: FundReportedValue<CalendarDate>,
    accepted_at: FundReportedValue<Timestamp>,
    provider_published_at: FundReportedValue<Timestamp>,
    availability: AvailabilityEvidence,
    received_at: Timestamp,
    ingested_at: Timestamp,
}

impl FundFilingChronology {
    /// Constructs non-collapsed report, filing, publication, and local clocks.
    #[allow(
        clippy::too_many_arguments,
        reason = "every point-in-time clock remains explicit"
    )]
    pub fn try_new(
        report_period_end: FundReportedValue<CalendarDate>,
        report_date: FundReportedValue<CalendarDate>,
        filed_date: FundReportedValue<CalendarDate>,
        accepted_at: FundReportedValue<Timestamp>,
        provider_published_at: FundReportedValue<Timestamp>,
        availability: AvailabilityEvidence,
        received_at: Timestamp,
        ingested_at: Timestamp,
    ) -> Result<Self, FundHoldingsError> {
        let filed = filed_date.reported().copied();
        if report_period_end
            .reported()
            .copied()
            .zip(filed)
            .is_some_and(|(report, filed)| report > filed)
            || report_date
                .reported()
                .copied()
                .zip(filed)
                .is_some_and(|(report, filed)| report > filed)
            || received_at > ingested_at
        {
            return Err(FundHoldingsError::InvalidChronology);
        }
        let accepted = accepted_at.reported().copied();
        let published = provider_published_at.reported().copied();
        if accepted.is_some_and(|time| time > received_at)
            || published.is_some_and(|time| time > received_at)
            || accepted
                .zip(published)
                .is_some_and(|(accepted, published)| accepted > published)
            || availability
                .reported_at()
                .is_some_and(|available| available > ingested_at)
        {
            return Err(FundHoldingsError::InvalidChronology);
        }
        Ok(Self {
            report_period_end,
            report_date,
            filed_date,
            accepted_at,
            provider_published_at,
            availability,
            received_at,
            ingested_at,
        })
    }

    /// Returns the source report-period end without assigning a time of day.
    pub const fn report_period_end(&self) -> &FundReportedValue<CalendarDate> {
        &self.report_period_end
    }
    /// Returns the source portfolio/report date without assigning a time of day.
    pub const fn report_date(&self) -> &FundReportedValue<CalendarDate> {
        &self.report_date
    }
    /// Returns the source filing date without assigning a time of day.
    pub const fn filed_date(&self) -> &FundReportedValue<CalendarDate> {
        &self.filed_date
    }
    /// Returns exact filing acceptance time or its explicit state.
    pub const fn accepted_at(&self) -> &FundReportedValue<Timestamp> {
        &self.accepted_at
    }
    /// Returns provider publication time or its explicit state.
    pub const fn provider_published_at(&self) -> &FundReportedValue<Timestamp> {
        &self.provider_published_at
    }
    /// Returns conservative availability evidence.
    pub const fn availability(&self) -> &AvailabilityEvidence {
        &self.availability
    }
    /// Returns the local receipt clock.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }
    /// Returns the canonical ingestion clock.
    pub const fn ingested_at(&self) -> Timestamp {
        self.ingested_at
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FundFilingChronologyWire {
    report_period_end: FundReportedValue<CalendarDate>,
    report_date: FundReportedValue<CalendarDate>,
    filed_date: FundReportedValue<CalendarDate>,
    accepted_at: FundReportedValue<Timestamp>,
    provider_published_at: FundReportedValue<Timestamp>,
    availability: AvailabilityEvidence,
    received_at: Timestamp,
    ingested_at: Timestamp,
}

impl<'de> Deserialize<'de> for FundFilingChronology {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = FundFilingChronologyWire::deserialize(deserializer)?;
        Self::try_new(
            wire.report_period_end,
            wire.report_date,
            wire.filed_date,
            wire.accepted_at,
            wire.provider_published_at,
            wire.availability,
            wire.received_at,
            wire.ingested_at,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Whether an exact accession is an original filing or amendment.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FundAmendmentState {
    /// Original source filing.
    Original,
    /// Source form explicitly identifies an amendment.
    Amendment,
}

/// Explicit predecessor/successor link state for a filing revision.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum FundRevisionLink {
    /// This directional link does not apply.
    NotApplicable,
    /// No successor has been observed; this is not proof that none exists.
    NotObserved,
    /// One exact accession and evidence identify the linked revision.
    Exact {
        /// Exact EDGAR accession.
        accession: SourceIdentifier,
        /// Exact canonical revision evidence.
        evidence: EvidenceDigest,
    },
    /// A link is expected but cannot be established.
    Unresolved,
    /// More than one link remains equally supported.
    Conflict,
}

/// Point-in-time status of one filing within its revision family.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FundRevisionStatus {
    /// This is the one current known revision under complete evidence.
    Current,
    /// A later exact revision supersedes this accession.
    Superseded,
    /// Two or more equally knowable accessions conflict.
    Conflict,
    /// Currentness cannot be established under available coverage.
    Unavailable,
}

/// Amendment, supersession, and conflict evidence for one accession family.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FundRevisionEvidence {
    amendment: FundAmendmentState,
    status: FundRevisionStatus,
    predecessor: FundRevisionLink,
    successor: FundRevisionLink,
    competing_accessions: Box<[SourceIdentifier]>,
}

impl FundRevisionEvidence {
    /// Constructs one revision-family state without inventing predecessor/successor links.
    pub fn try_new(
        amendment: FundAmendmentState,
        status: FundRevisionStatus,
        predecessor: FundRevisionLink,
        successor: FundRevisionLink,
        mut competing_accessions: Vec<SourceIdentifier>,
    ) -> Result<Self, FundHoldingsError> {
        validate_revision_link(&predecessor)?;
        validate_revision_link(&successor)?;
        competing_accessions.sort();
        if competing_accessions.len() > MAX_FUND_COMPETING_ACCESSIONS
            || competing_accessions
                .windows(2)
                .any(|pair| pair[0] == pair[1])
        {
            return Err(FundHoldingsError::InvalidRevision);
        }
        let conflict = status == FundRevisionStatus::Conflict;
        if conflict != (competing_accessions.len() >= 2)
            || status == FundRevisionStatus::Superseded
                && !matches!(successor, FundRevisionLink::Exact { .. })
            || status == FundRevisionStatus::Current
                && !matches!(successor, FundRevisionLink::NotObserved)
            || amendment == FundAmendmentState::Original
                && !matches!(predecessor, FundRevisionLink::NotApplicable)
            || amendment == FundAmendmentState::Amendment
                && matches!(predecessor, FundRevisionLink::NotApplicable)
        {
            return Err(FundHoldingsError::InvalidRevision);
        }
        Ok(Self {
            amendment,
            status,
            predecessor,
            successor,
            competing_accessions: competing_accessions.into_boxed_slice(),
        })
    }

    /// Returns original/amendment state.
    pub const fn amendment(&self) -> FundAmendmentState {
        self.amendment
    }
    /// Returns current/superseded/conflict/unavailable state.
    pub const fn status(&self) -> FundRevisionStatus {
        self.status
    }
    /// Returns explicit predecessor state.
    pub const fn predecessor(&self) -> &FundRevisionLink {
        &self.predecessor
    }
    /// Returns explicit successor state.
    pub const fn successor(&self) -> &FundRevisionLink {
        &self.successor
    }
    /// Returns sorted distinct equally knowable accessions for a conflict.
    pub fn competing_accessions(&self) -> &[SourceIdentifier] {
        &self.competing_accessions
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FundRevisionEvidenceWire {
    amendment: FundAmendmentState,
    status: FundRevisionStatus,
    predecessor: FundRevisionLink,
    successor: FundRevisionLink,
    competing_accessions: Vec<SourceIdentifier>,
}

impl<'de> Deserialize<'de> for FundRevisionEvidence {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = FundRevisionEvidenceWire::deserialize(deserializer)?;
        Self::try_new(
            wire.amendment,
            wire.status,
            wire.predecessor,
            wire.successor,
            wire.competing_accessions,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Exact governed fund/share-class identity; ticker and name are not construction inputs.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FundShareClassIdentity {
    instrument_id: InstrumentId,
    provider_series_id: SourceIdentifier,
    authority_source_id: SourceId,
    reference_revision: MetadataRevision,
    reference_evidence: ExactPayloadEvidence,
    available_at: Timestamp,
    observed_at: Timestamp,
}

impl FundShareClassIdentity {
    /// Constructs an exact series-to-instrument bridge from separately governed evidence.
    pub fn try_new(
        instrument_id: InstrumentId,
        provider_series_id: SourceIdentifier,
        authority_source_id: SourceId,
        reference_revision: MetadataRevision,
        reference_evidence: ExactPayloadEvidence,
        available_at: Timestamp,
        observed_at: Timestamp,
    ) -> Result<Self, FundHoldingsError> {
        validate_evidence(reference_evidence.content_digest())?;
        if available_at > observed_at {
            return Err(FundHoldingsError::InvalidIdentity);
        }
        Ok(Self {
            instrument_id,
            provider_series_id,
            authority_source_id,
            reference_revision,
            reference_evidence,
            available_at,
            observed_at,
        })
    }

    /// Returns stable canonical fund/share-class identity.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
    /// Returns exact provider series identity.
    pub const fn provider_series_id(&self) -> &SourceIdentifier {
        &self.provider_series_id
    }
    /// Returns identity-registry authority source.
    pub const fn authority_source_id(&self) -> &SourceId {
        &self.authority_source_id
    }
    /// Returns exact reference revision.
    pub const fn reference_revision(&self) -> &MetadataRevision {
        &self.reference_revision
    }
    /// Returns exact payload evidence for the identity bridge.
    pub const fn reference_evidence(&self) -> &ExactPayloadEvidence {
        &self.reference_evidence
    }
    /// Returns conservative authority availability.
    pub const fn available_at(&self) -> Timestamp {
        self.available_at
    }
    /// Returns first local observation of the authority assertion.
    pub const fn observed_at(&self) -> Timestamp {
        self.observed_at
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FundShareClassIdentityWire {
    instrument_id: InstrumentId,
    provider_series_id: SourceIdentifier,
    authority_source_id: SourceId,
    reference_revision: MetadataRevision,
    reference_evidence: ExactPayloadEvidence,
    available_at: Timestamp,
    observed_at: Timestamp,
}

impl<'de> Deserialize<'de> for FundShareClassIdentity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = FundShareClassIdentityWire::deserialize(deserializer)?;
        Self::try_new(
            wire.instrument_id,
            wire.provider_series_id,
            wire.authority_source_id,
            wire.reference_revision,
            wire.reference_evidence,
            wire.available_at,
            wire.observed_at,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Exact authoritative held-security identifier used by a governed identity mapping.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum FundSecurityIdentifier {
    /// Checksum-valid CUSIP syntax retained with independent authority evidence.
    Cusip(Cusip),
    /// Checksum-valid ISIN syntax retained with independent authority evidence.
    Isin(Isin),
}

/// Held-security resolution; names and tickers have no exact-construction path.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum FundHoldingSecurityIdentity {
    /// One governed identifier maps to one exact canonical instrument.
    Exact {
        /// Stable held-security identity.
        instrument_id: InstrumentId,
        /// Exact authoritative provider identifier.
        authoritative_identifier: FundSecurityIdentifier,
        /// Source that owns the selected crosswalk.
        authority_source_id: SourceId,
        /// Exact crosswalk revision.
        authority_revision: MetadataRevision,
        /// Exact evidence for the selected mapping.
        authority_evidence: ExactPayloadEvidence,
        /// Optional governed company/security relationship; absence remains explicit.
        company_security_link: FundReportedValue<EvidenceDigest>,
        /// Conservative availability of the mapping.
        available_at: Timestamp,
        /// First local observation of the mapping.
        observed_at: Timestamp,
    },
    /// More than one governed mapping remains possible.
    Ambiguous {
        /// Exact evidence binding the competing mapping set.
        conflict_evidence: EvidenceDigest,
    },
    /// No governed mapping is available.
    Unresolved {
        /// Closed reason no identity was selected.
        reason: FundMissingState,
    },
}

impl FundHoldingSecurityIdentity {
    /// Constructs an exact held-security resolution.
    #[allow(
        clippy::too_many_arguments,
        reason = "identity authority remains fully explicit"
    )]
    pub fn try_exact(
        instrument_id: InstrumentId,
        authoritative_identifier: FundSecurityIdentifier,
        authority_source_id: SourceId,
        authority_revision: MetadataRevision,
        authority_evidence: ExactPayloadEvidence,
        company_security_link: FundReportedValue<EvidenceDigest>,
        available_at: Timestamp,
        observed_at: Timestamp,
    ) -> Result<Self, FundHoldingsError> {
        validate_evidence(authority_evidence.content_digest())?;
        if let FundReportedValue::Reported(evidence) = company_security_link {
            validate_evidence(evidence)?;
        }
        if available_at > observed_at {
            return Err(FundHoldingsError::InvalidIdentity);
        }
        Ok(Self::Exact {
            instrument_id,
            authoritative_identifier,
            authority_source_id,
            authority_revision,
            authority_evidence,
            company_security_link,
            available_at,
            observed_at,
        })
    }

    /// Constructs an ambiguous held-security resolution from exact conflict-set evidence.
    pub fn try_ambiguous(conflict_evidence: EvidenceDigest) -> Result<Self, FundHoldingsError> {
        validate_evidence(conflict_evidence)?;
        Ok(Self::Ambiguous { conflict_evidence })
    }

    /// Constructs an unresolved state without admitting ambiguous/invalid as exact identity.
    pub fn unresolved(reason: FundMissingState) -> Result<Self, FundHoldingsError> {
        if !matches!(
            reason,
            FundMissingState::SourceAbsent
                | FundMissingState::Invalid
                | FundMissingState::Unavailable
                | FundMissingState::UnresolvedIdentity
                | FundMissingState::DeclaredCoverageGap
        ) {
            return Err(FundHoldingsError::InvalidIdentity);
        }
        Ok(Self::Unresolved { reason })
    }

    /// Returns canonical identity only for an exact governed mapping.
    pub const fn instrument_id(&self) -> Option<InstrumentId> {
        match self {
            Self::Exact { instrument_id, .. } => Some(*instrument_id),
            Self::Ambiguous { .. } | Self::Unresolved { .. } => None,
        }
    }

    /// Returns the exact authoritative identifier only for a governed exact mapping.
    pub const fn authoritative_identifier(&self) -> Option<&FundSecurityIdentifier> {
        match self {
            Self::Exact {
                authoritative_identifier,
                ..
            } => Some(authoritative_identifier),
            Self::Ambiguous { .. } | Self::Unresolved { .. } => None,
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum FundHoldingSecurityIdentityWire {
    Exact {
        instrument_id: InstrumentId,
        authoritative_identifier: FundSecurityIdentifier,
        authority_source_id: SourceId,
        authority_revision: MetadataRevision,
        authority_evidence: ExactPayloadEvidence,
        company_security_link: FundReportedValue<EvidenceDigest>,
        available_at: Timestamp,
        observed_at: Timestamp,
    },
    Ambiguous {
        conflict_evidence: EvidenceDigest,
    },
    Unresolved {
        reason: FundMissingState,
    },
}

impl<'de> Deserialize<'de> for FundHoldingSecurityIdentity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match FundHoldingSecurityIdentityWire::deserialize(deserializer)? {
            FundHoldingSecurityIdentityWire::Exact {
                instrument_id,
                authoritative_identifier,
                authority_source_id,
                authority_revision,
                authority_evidence,
                company_security_link,
                available_at,
                observed_at,
            } => Self::try_exact(
                instrument_id,
                authoritative_identifier,
                authority_source_id,
                authority_revision,
                authority_evidence,
                company_security_link,
                available_at,
                observed_at,
            ),
            FundHoldingSecurityIdentityWire::Ambiguous { conflict_evidence } => {
                Self::try_ambiguous(conflict_evidence)
            }
            FundHoldingSecurityIdentityWire::Unresolved { reason } => Self::unresolved(reason),
        }
        .map_err(serde::de::Error::custom)
    }
}

/// Closed SEC bulk source table used by canonical fund evidence.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FundSourceTable {
    /// N-PORT submission table.
    NportSubmission,
    /// N-PORT registrant table.
    NportRegistrant,
    /// N-PORT fund-reported-info table.
    NportFund,
    /// N-PORT fund-reported-holding table.
    NportHolding,
    /// N-PORT identifiers table.
    NportIdentifiers,
    /// N-PORT debt-security supplement.
    NportDebtSecurity,
    /// N-PORT debt reference-instrument supplement.
    NportDebtSecurityReferenceInstrument,
    /// N-PORT convertible-security currency supplement.
    NportConvertibleSecurityCurrency,
    /// N-PORT repurchase-agreement supplement.
    NportRepurchaseAgreement,
    /// N-PORT repurchase counterparty supplement.
    NportRepurchaseCounterparty,
    /// N-PORT repurchase collateral supplement.
    NportRepurchaseCollateral,
    /// N-PORT derivative counterparty supplement.
    NportDerivativeCounterparty,
    /// N-PORT swaption/option/warrant derivative supplement.
    NportSwaptionOptionWarrantDerivative,
    /// N-PORT reference-index/basket supplement.
    NportDescriptionReferenceIndexBasket,
    /// N-PORT reference-index component supplement.
    NportDescriptionReferenceIndexComponent,
    /// N-PORT other-reference supplement.
    NportDescriptionReferenceOther,
    /// N-PORT future/forward non-FX supplement.
    NportFutureForwardNonforeignCurrencyContract,
    /// N-PORT forward-FX/swap supplement.
    NportForwardForeignCurrencyContractSwap,
    /// N-PORT non-FX swap supplement.
    NportNonforeignExchangeSwap,
    /// N-PORT floating-rate reset-tenor supplement.
    NportFloatingRateResetTenor,
    /// N-PORT other-derivative supplement.
    NportOtherDerivative,
    /// N-PORT other-derivative notional supplement.
    NportOtherDerivativeNotionalAmount,
    /// N-PORT securities-lending supplement.
    NportSecuritiesLending,
    /// N-PORT explanatory-note supplement.
    NportExplanatoryNote,
    /// N-CEN submission table.
    NcenSubmission,
    /// N-CEN registrant table.
    NcenRegistrant,
    /// N-CEN fund-reported-info table.
    NcenFund,
    /// N-CEN ETF mechanics table.
    NcenEtf,
    /// N-CEN exchange/ticker association table.
    NcenSecurityExchange,
}

impl FundSourceTable {
    /// Returns the provider filing family that owns this table.
    pub const fn family(self) -> FundSourceFamily {
        match self {
            Self::NcenSubmission
            | Self::NcenRegistrant
            | Self::NcenFund
            | Self::NcenEtf
            | Self::NcenSecurityExchange => FundSourceFamily::Ncen,
            _ => FundSourceFamily::Nport,
        }
    }

    /// Returns whether this is one of the 19 holding supplement tables.
    pub const fn is_holding_supplement(self) -> bool {
        matches!(
            self,
            Self::NportDebtSecurity
                | Self::NportDebtSecurityReferenceInstrument
                | Self::NportConvertibleSecurityCurrency
                | Self::NportRepurchaseAgreement
                | Self::NportRepurchaseCounterparty
                | Self::NportRepurchaseCollateral
                | Self::NportDerivativeCounterparty
                | Self::NportSwaptionOptionWarrantDerivative
                | Self::NportDescriptionReferenceIndexBasket
                | Self::NportDescriptionReferenceIndexComponent
                | Self::NportDescriptionReferenceOther
                | Self::NportFutureForwardNonforeignCurrencyContract
                | Self::NportForwardForeignCurrencyContractSwap
                | Self::NportNonforeignExchangeSwap
                | Self::NportFloatingRateResetTenor
                | Self::NportOtherDerivative
                | Self::NportOtherDerivativeNotionalAmount
                | Self::NportSecuritiesLending
                | Self::NportExplanatoryNote
        )
    }
}

/// Exact coordinate of one provider-native row inside a sealed logical-object publication.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FundSourceRowEvidence {
    table: FundSourceTable,
    logical_object_component_ordinal: u32,
    logical_object_component: EvidenceDigest,
    row_number: NonZeroU64,
    row_evidence: EvidenceDigest,
    native_generation: EvidenceDigest,
    layout_evidence: EvidenceDigest,
    terminal_handoff_evidence: EvidenceDigest,
}

impl FundSourceRowEvidence {
    /// Constructs one exact provider-row coordinate.
    #[allow(
        clippy::too_many_arguments,
        reason = "logical publication lineage is non-collapsible"
    )]
    pub fn try_new(
        table: FundSourceTable,
        logical_object_component_ordinal: u32,
        logical_object_component: EvidenceDigest,
        row_number: NonZeroU64,
        row_evidence: EvidenceDigest,
        native_generation: EvidenceDigest,
        layout_evidence: EvidenceDigest,
        terminal_handoff_evidence: EvidenceDigest,
    ) -> Result<Self, FundHoldingsError> {
        for evidence in [
            logical_object_component,
            row_evidence,
            native_generation,
            layout_evidence,
            terminal_handoff_evidence,
        ] {
            validate_evidence(evidence)?;
        }
        Ok(Self {
            table,
            logical_object_component_ordinal,
            logical_object_component,
            row_number,
            row_evidence,
            native_generation,
            layout_evidence,
            terminal_handoff_evidence,
        })
    }

    /// Returns the exact closed source table.
    pub const fn table(&self) -> FundSourceTable {
        self.table
    }
    /// Returns the zero-based ordered component within the sealed logical object.
    pub const fn logical_object_component_ordinal(&self) -> u32 {
        self.logical_object_component_ordinal
    }
    /// Returns the exact sealed logical-object component identity.
    pub const fn logical_object_component(&self) -> EvidenceDigest {
        self.logical_object_component
    }
    /// Returns the one-based physical source row.
    pub const fn row_number(&self) -> NonZeroU64 {
        self.row_number
    }
    /// Returns the exact decoded provider-row identity.
    pub const fn row_evidence(&self) -> EvidenceDigest {
        self.row_evidence
    }
    /// Returns the immutable provider-native generation identity.
    pub const fn native_generation(&self) -> EvidenceDigest {
        self.native_generation
    }
    /// Returns the inspected layout identity.
    pub const fn layout_evidence(&self) -> EvidenceDigest {
        self.layout_evidence
    }
    /// Returns the terminal whole-object handoff identity.
    pub const fn terminal_handoff_evidence(&self) -> EvidenceDigest {
        self.terminal_handoff_evidence
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FundSourceRowEvidenceWire {
    table: FundSourceTable,
    logical_object_component_ordinal: u32,
    logical_object_component: EvidenceDigest,
    row_number: NonZeroU64,
    row_evidence: EvidenceDigest,
    native_generation: EvidenceDigest,
    layout_evidence: EvidenceDigest,
    terminal_handoff_evidence: EvidenceDigest,
}

impl<'de> Deserialize<'de> for FundSourceRowEvidence {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = FundSourceRowEvidenceWire::deserialize(deserializer)?;
        Self::try_new(
            wire.table,
            wire.logical_object_component_ordinal,
            wire.logical_object_component,
            wire.row_number,
            wire.row_evidence,
            wire.native_generation,
            wire.layout_evidence,
            wire.terminal_handoff_evidence,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Ordered source-row lineage for one canonical fund record.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FundSourceLineage {
    family: FundSourceFamily,
    rows: Box<[FundSourceRowEvidence]>,
}

impl FundSourceLineage {
    /// Constructs a bounded ordered lineage without silently sorting provider roles.
    ///
    /// Every row must belong to one native generation, inspected layout, and terminal logical
    /// handoff. Rows remain ordered by closed table role, logical component, then physical row.
    /// The referenced logical components retain complete typed provider-native rows; omission of a
    /// display field from a canonical projection never authorizes dropping its native evidence.
    pub fn try_new(
        family: FundSourceFamily,
        rows: Vec<FundSourceRowEvidence>,
    ) -> Result<Self, FundHoldingsError> {
        if rows.is_empty() || rows.len() > MAX_FUND_SOURCE_ROWS {
            return Err(FundHoldingsError::CollectionLimitExceeded);
        }
        if rows.iter().any(|row| row.table.family() != family) {
            return Err(FundHoldingsError::InconsistentFamily);
        }
        let first = &rows[0];
        if rows.iter().any(|row| {
            row.native_generation != first.native_generation
                || row.layout_evidence != first.layout_evidence
                || row.terminal_handoff_evidence != first.terminal_handoff_evidence
        }) {
            return Err(FundHoldingsError::InvalidLineage);
        }
        for pair in rows.windows(2) {
            let left = &pair[0];
            let right = &pair[1];
            let left_key = (
                left.table,
                left.logical_object_component_ordinal,
                left.row_number,
            );
            let right_key = (
                right.table,
                right.logical_object_component_ordinal,
                right.row_number,
            );
            if left_key > right_key {
                return Err(FundHoldingsError::InvalidLineage);
            }
            if left_key == right_key
                && left.logical_object_component == right.logical_object_component
            {
                return Err(FundHoldingsError::DuplicateEntry);
            }
        }
        Ok(Self {
            family,
            rows: rows.into_boxed_slice(),
        })
    }

    /// Returns the exact source family.
    pub const fn family(&self) -> FundSourceFamily {
        self.family
    }
    /// Returns source rows in canonical role order.
    pub fn rows(&self) -> &[FundSourceRowEvidence] {
        &self.rows
    }
    /// Returns the one immutable provider-native generation bound by every row.
    pub const fn native_generation(&self) -> EvidenceDigest {
        self.rows[0].native_generation
    }
    /// Returns the one inspected layout identity bound by every row.
    pub const fn layout_evidence(&self) -> EvidenceDigest {
        self.rows[0].layout_evidence
    }
    /// Returns the one terminal whole-object handoff bound by every row.
    pub const fn terminal_handoff_evidence(&self) -> EvidenceDigest {
        self.rows[0].terminal_handoff_evidence
    }
    /// Returns whether at least one exact row from a required table is bound.
    pub fn contains_table(&self, table: FundSourceTable) -> bool {
        self.rows.iter().any(|row| row.table == table)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FundSourceLineageWire {
    family: FundSourceFamily,
    rows: Vec<FundSourceRowEvidence>,
}

impl<'de> Deserialize<'de> for FundSourceLineage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = FundSourceLineageWire::deserialize(deserializer)?;
        Self::try_new(wire.family, wire.rows).map_err(serde::de::Error::custom)
    }
}

/// Shared exact source and canonical identity for one fund filing record.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FundFilingIdentity {
    schema_version: SchemaVersion,
    source_id: SourceId,
    family: FundSourceFamily,
    registrant_cik: SourceIdentifier,
    accession: SourceIdentifier,
    form: SourceIdentifier,
    provider_fund_id: FundReportedValue<SourceIdentifier>,
    fund: FundShareClassIdentity,
    chronology: FundFilingChronology,
    revision: FundRevisionEvidence,
    coverage: FundReleaseCoverage,
}

impl FundFilingIdentity {
    /// Constructs one exact SEC report/share-class filing identity.
    #[allow(
        clippy::too_many_arguments,
        reason = "source identity and PIT clocks remain explicit"
    )]
    pub fn try_new(
        schema_version: SchemaVersion,
        source_id: SourceId,
        family: FundSourceFamily,
        registrant_cik: SourceIdentifier,
        accession: SourceIdentifier,
        form: SourceIdentifier,
        provider_fund_id: FundReportedValue<SourceIdentifier>,
        fund: FundShareClassIdentity,
        chronology: FundFilingChronology,
        revision: FundRevisionEvidence,
        coverage: FundReleaseCoverage,
    ) -> Result<Self, FundHoldingsError> {
        schema_version.ensure_supported()?;
        if !is_sec_cik(registrant_cik.as_str())
            || !is_sec_accession(accession.as_str())
            || !is_sec_series_id(fund.provider_series_id().as_str())
            || family == FundSourceFamily::Nport
                && !matches!(
                    provider_fund_id,
                    FundReportedValue::Missing(FundMissingState::NotApplicable)
                )
            || family == FundSourceFamily::Ncen
                && !matches!(provider_fund_id, FundReportedValue::Reported(_))
            || form.as_str().ends_with("/A")
                != (revision.amendment() == FundAmendmentState::Amendment)
            || revision.status() == FundRevisionStatus::Conflict
                && revision
                    .competing_accessions()
                    .binary_search(&accession)
                    .is_err()
            || revision_link_accession(revision.predecessor()) == Some(&accession)
            || revision_link_accession(revision.successor()) == Some(&accession)
        {
            return Err(FundHoldingsError::InvalidIdentity);
        }
        Ok(Self {
            schema_version,
            source_id,
            family,
            registrant_cik,
            accession,
            form,
            provider_fund_id,
            fund,
            chronology,
            revision,
            coverage,
        })
    }

    /// Returns the domain schema version.
    pub const fn schema_version(&self) -> SchemaVersion {
        self.schema_version
    }
    /// Returns the source namespace.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }
    /// Returns N-PORT or N-CEN family.
    pub const fn family(&self) -> FundSourceFamily {
        self.family
    }
    /// Returns exact zero-padded SEC registrant CIK.
    pub const fn registrant_cik(&self) -> &SourceIdentifier {
        &self.registrant_cik
    }
    /// Returns exact EDGAR accession.
    pub const fn accession(&self) -> &SourceIdentifier {
        &self.accession
    }
    /// Returns exact SEC form.
    pub const fn form(&self) -> &SourceIdentifier {
        &self.form
    }
    /// Returns exact N-CEN `FUND_ID`, or N-PORT not-applicable state.
    pub const fn provider_fund_id(&self) -> &FundReportedValue<SourceIdentifier> {
        &self.provider_fund_id
    }
    /// Returns exact governed fund/share-class identity and `SERIES_ID`.
    pub const fn fund(&self) -> &FundShareClassIdentity {
        &self.fund
    }
    /// Returns non-collapsed source clocks.
    pub const fn chronology(&self) -> &FundFilingChronology {
        &self.chronology
    }
    /// Returns amendment/supersession/conflict evidence.
    pub const fn revision(&self) -> &FundRevisionEvidence {
        &self.revision
    }
    /// Returns complete or explicitly incomplete release coverage.
    pub const fn coverage(&self) -> &FundReleaseCoverage {
        &self.coverage
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FundFilingIdentityWire {
    schema_version: SchemaVersion,
    source_id: SourceId,
    family: FundSourceFamily,
    registrant_cik: SourceIdentifier,
    accession: SourceIdentifier,
    form: SourceIdentifier,
    provider_fund_id: FundReportedValue<SourceIdentifier>,
    fund: FundShareClassIdentity,
    chronology: FundFilingChronology,
    revision: FundRevisionEvidence,
    coverage: FundReleaseCoverage,
}

impl<'de> Deserialize<'de> for FundFilingIdentity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = FundFilingIdentityWire::deserialize(deserializer)?;
        Self::try_new(
            wire.schema_version,
            wire.source_id,
            wire.family,
            wire.registrant_cik,
            wire.accession,
            wire.form,
            wire.provider_fund_id,
            wire.fund,
            wire.chronology,
            wire.revision,
            wire.coverage,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Provider-native report metadata that differs between N-PORT and N-CEN.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FundReportAttributes {
    is_last_filing: FundReportedValue<bool>,
    report_period_less_than_twelve_months: FundReportedValue<bool>,
}

impl FundReportAttributes {
    /// Constructs explicit N-PORT/N-CEN report attributes.
    pub const fn new(
        is_last_filing: FundReportedValue<bool>,
        report_period_less_than_twelve_months: FundReportedValue<bool>,
    ) -> Self {
        Self {
            is_last_filing,
            report_period_less_than_twelve_months,
        }
    }

    /// Returns N-PORT final-filing state or an explicit non-value state.
    pub const fn is_last_filing(&self) -> &FundReportedValue<bool> {
        &self.is_last_filing
    }
    /// Returns N-CEN short-reporting-period state or an explicit non-value state.
    pub const fn report_period_less_than_twelve_months(&self) -> &FundReportedValue<bool> {
        &self.report_period_less_than_twelve_months
    }
}

/// Canonical filing/report evidence for one governed fund/share class.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FundReportEvidence {
    filing: FundFilingIdentity,
    attributes: FundReportAttributes,
    lineage: FundSourceLineage,
}

impl FundReportEvidence {
    /// Constructs one filing report with exact submission/registrant/fund lineage.
    pub fn try_new(
        filing: FundFilingIdentity,
        attributes: FundReportAttributes,
        lineage: FundSourceLineage,
    ) -> Result<Self, FundHoldingsError> {
        validate_common_lineage(&filing, &lineage)?;
        let attributes_match = match filing.family() {
            FundSourceFamily::Nport => matches!(
                attributes.report_period_less_than_twelve_months,
                FundReportedValue::Missing(FundMissingState::NotApplicable)
            ),
            FundSourceFamily::Ncen => matches!(
                attributes.is_last_filing,
                FundReportedValue::Missing(FundMissingState::NotApplicable)
            ),
        };
        if !attributes_match {
            return Err(FundHoldingsError::InvalidValueState);
        }
        Ok(Self {
            filing,
            attributes,
            lineage,
        })
    }

    /// Returns source and canonical filing identity.
    pub const fn filing(&self) -> &FundFilingIdentity {
        &self.filing
    }
    /// Returns family-specific report attributes.
    pub const fn attributes(&self) -> &FundReportAttributes {
        &self.attributes
    }
    /// Returns ordered provider-native source rows.
    pub const fn lineage(&self) -> &FundSourceLineage {
        &self.lineage
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FundReportEvidenceWire {
    filing: FundFilingIdentity,
    attributes: FundReportAttributes,
    lineage: FundSourceLineage,
}

impl<'de> Deserialize<'de> for FundReportEvidence {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = FundReportEvidenceWire::deserialize(deserializer)?;
        Self::try_new(wire.filing, wire.attributes, wire.lineage).map_err(serde::de::Error::custom)
    }
}

/// Provider-reported ETF mechanics kept separate from market price or NAV.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FundEtfMechanics {
    collateral_required: FundReportedValue<bool>,
    shares_per_creation_unit: FundReportedValue<FundReportedDecimal>,
    shares_per_redemption_unit: FundReportedValue<FundReportedDecimal>,
    in_kind: FundReportedValue<bool>,
}

impl FundEtfMechanics {
    /// Constructs source-reported ETF creation/redemption mechanics.
    pub const fn new(
        collateral_required: FundReportedValue<bool>,
        shares_per_creation_unit: FundReportedValue<FundReportedDecimal>,
        shares_per_redemption_unit: FundReportedValue<FundReportedDecimal>,
        in_kind: FundReportedValue<bool>,
    ) -> Self {
        Self {
            collateral_required,
            shares_per_creation_unit,
            shares_per_redemption_unit,
            in_kind,
        }
    }

    /// Returns collateral-required state.
    pub const fn collateral_required(&self) -> &FundReportedValue<bool> {
        &self.collateral_required
    }
    /// Returns exact shares per creation unit.
    pub const fn shares_per_creation_unit(&self) -> &FundReportedValue<FundReportedDecimal> {
        &self.shares_per_creation_unit
    }
    /// Returns exact shares per redemption unit.
    pub const fn shares_per_redemption_unit(&self) -> &FundReportedValue<FundReportedDecimal> {
        &self.shares_per_redemption_unit
    }
    /// Returns source-reported in-kind state.
    pub const fn in_kind(&self) -> &FundReportedValue<bool> {
        &self.in_kind
    }
}

/// Provider-reported exchange/ticker association with no identity authority.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FundExchangeAssociation {
    exchange: FundReportedValue<SourceIdentifier>,
    ticker: FundReportedValue<SourceIdentifier>,
    row: FundSourceRowEvidence,
}

impl FundExchangeAssociation {
    /// Constructs association evidence without promoting ticker/name to canonical identity.
    pub fn try_new(
        exchange: FundReportedValue<SourceIdentifier>,
        ticker: FundReportedValue<SourceIdentifier>,
        row: FundSourceRowEvidence,
    ) -> Result<Self, FundHoldingsError> {
        if row.table != FundSourceTable::NcenSecurityExchange {
            return Err(FundHoldingsError::InvalidLineage);
        }
        Ok(Self {
            exchange,
            ticker,
            row,
        })
    }

    /// Returns provider exchange label only as association evidence.
    pub const fn exchange(&self) -> &FundReportedValue<SourceIdentifier> {
        &self.exchange
    }
    /// Returns provider ticker only as association evidence.
    pub const fn ticker(&self) -> &FundReportedValue<SourceIdentifier> {
        &self.ticker
    }
    /// Returns exact source row for this association.
    pub const fn row(&self) -> &FundSourceRowEvidence {
        &self.row
    }
}

/// Exact share-class financial and operational fields.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FundShareClassAttributes {
    reporting_currency: FundReportedValue<Currency>,
    total_assets: FundReportedValue<FundReportedDecimal>,
    total_liabilities: FundReportedValue<FundReportedDecimal>,
    net_assets: FundReportedValue<FundReportedDecimal>,
    monthly_average_net_assets: FundReportedValue<FundReportedDecimal>,
    daily_average_net_assets: FundReportedValue<FundReportedDecimal>,
    is_etf: FundReportedValue<bool>,
    is_index: FundReportedValue<bool>,
    etf_mechanics: FundEtfMechanics,
}

impl FundShareClassAttributes {
    /// Constructs source-reported share-class values with explicit missing/conflict states.
    #[allow(
        clippy::too_many_arguments,
        reason = "financial values retain independent states"
    )]
    pub const fn new(
        reporting_currency: FundReportedValue<Currency>,
        total_assets: FundReportedValue<FundReportedDecimal>,
        total_liabilities: FundReportedValue<FundReportedDecimal>,
        net_assets: FundReportedValue<FundReportedDecimal>,
        monthly_average_net_assets: FundReportedValue<FundReportedDecimal>,
        daily_average_net_assets: FundReportedValue<FundReportedDecimal>,
        is_etf: FundReportedValue<bool>,
        is_index: FundReportedValue<bool>,
        etf_mechanics: FundEtfMechanics,
    ) -> Self {
        Self {
            reporting_currency,
            total_assets,
            total_liabilities,
            net_assets,
            monthly_average_net_assets,
            daily_average_net_assets,
            is_etf,
            is_index,
            etf_mechanics,
        }
    }

    /// Returns source reporting currency or explicit non-value state.
    pub const fn reporting_currency(&self) -> &FundReportedValue<Currency> {
        &self.reporting_currency
    }
    /// Returns exact N-PORT total assets.
    pub const fn total_assets(&self) -> &FundReportedValue<FundReportedDecimal> {
        &self.total_assets
    }
    /// Returns exact N-PORT total liabilities.
    pub const fn total_liabilities(&self) -> &FundReportedValue<FundReportedDecimal> {
        &self.total_liabilities
    }
    /// Returns exact N-PORT net assets.
    pub const fn net_assets(&self) -> &FundReportedValue<FundReportedDecimal> {
        &self.net_assets
    }
    /// Returns exact N-CEN monthly average net assets.
    pub const fn monthly_average_net_assets(&self) -> &FundReportedValue<FundReportedDecimal> {
        &self.monthly_average_net_assets
    }
    /// Returns exact N-CEN daily average net assets.
    pub const fn daily_average_net_assets(&self) -> &FundReportedValue<FundReportedDecimal> {
        &self.daily_average_net_assets
    }
    /// Returns source ETF classification.
    pub const fn is_etf(&self) -> &FundReportedValue<bool> {
        &self.is_etf
    }
    /// Returns source index-fund classification.
    pub const fn is_index(&self) -> &FundReportedValue<bool> {
        &self.is_index
    }
    /// Returns ETF creation/redemption mechanics.
    pub const fn etf_mechanics(&self) -> &FundEtfMechanics {
        &self.etf_mechanics
    }
}

/// Canonical share-class financial and operational evidence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FundShareClassEvidence {
    filing: FundFilingIdentity,
    attributes: FundShareClassAttributes,
    exchange_associations: Box<[FundExchangeAssociation]>,
    lineage: FundSourceLineage,
}

impl FundShareClassEvidence {
    /// Constructs one fund/share-class record with exact source lineage.
    pub fn try_new(
        filing: FundFilingIdentity,
        attributes: FundShareClassAttributes,
        exchange_associations: Vec<FundExchangeAssociation>,
        lineage: FundSourceLineage,
    ) -> Result<Self, FundHoldingsError> {
        validate_common_lineage(&filing, &lineage)?;
        let incompatible_fields_are_absent = match filing.family() {
            FundSourceFamily::Nport => {
                is_not_applicable(&attributes.monthly_average_net_assets)
                    && is_not_applicable(&attributes.daily_average_net_assets)
                    && is_not_applicable(&attributes.is_etf)
                    && is_not_applicable(&attributes.is_index)
                    && etf_mechanics_not_applicable(&attributes.etf_mechanics)
            }
            FundSourceFamily::Ncen => {
                is_not_applicable(&attributes.total_assets)
                    && is_not_applicable(&attributes.total_liabilities)
                    && is_not_applicable(&attributes.net_assets)
            }
        };
        if exchange_associations.len() > MAX_FUND_EXCHANGE_ASSOCIATIONS
            || filing.family() == FundSourceFamily::Nport && !exchange_associations.is_empty()
            || !incompatible_fields_are_absent
            || exchange_associations
                .iter()
                .any(|association| !lineage.rows.iter().any(|row| row == association.row()))
        {
            return Err(FundHoldingsError::InvalidLineage);
        }
        Ok(Self {
            filing,
            attributes,
            exchange_associations: exchange_associations.into_boxed_slice(),
            lineage,
        })
    }

    /// Returns source and canonical filing identity.
    pub const fn filing(&self) -> &FundFilingIdentity {
        &self.filing
    }
    /// Returns financial and operational attributes.
    pub const fn attributes(&self) -> &FundShareClassAttributes {
        &self.attributes
    }
    /// Returns bounded provider exchange/ticker associations with no identity authority.
    pub fn exchange_associations(&self) -> &[FundExchangeAssociation] {
        &self.exchange_associations
    }
    /// Returns ordered provider-native source rows.
    pub const fn lineage(&self) -> &FundSourceLineage {
        &self.lineage
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FundShareClassEvidenceWire {
    filing: FundFilingIdentity,
    attributes: FundShareClassAttributes,
    exchange_associations: Vec<FundExchangeAssociation>,
    lineage: FundSourceLineage,
}

impl<'de> Deserialize<'de> for FundShareClassEvidence {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = FundShareClassEvidenceWire::deserialize(deserializer)?;
        Self::try_new(
            wire.filing,
            wire.attributes,
            wire.exchange_associations,
            wire.lineage,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Provider associations retained for a held security without identity authority.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FundHoldingAssociations {
    issuer_name: FundReportedValue<FundSourceText>,
    issuer_lei: FundReportedValue<SourceIdentifier>,
    title: FundReportedValue<FundSourceText>,
    cusip: FundReportedValue<Cusip>,
    isin: FundReportedValue<Isin>,
    ticker: FundReportedValue<SourceIdentifier>,
}

impl FundHoldingAssociations {
    /// Constructs provider associations; none can mint canonical identity.
    pub const fn new(
        issuer_name: FundReportedValue<FundSourceText>,
        issuer_lei: FundReportedValue<SourceIdentifier>,
        title: FundReportedValue<FundSourceText>,
        cusip: FundReportedValue<Cusip>,
        isin: FundReportedValue<Isin>,
        ticker: FundReportedValue<SourceIdentifier>,
    ) -> Self {
        Self {
            issuer_name,
            issuer_lei,
            title,
            cusip,
            isin,
            ticker,
        }
    }

    /// Returns issuer-name association.
    pub const fn issuer_name(&self) -> &FundReportedValue<FundSourceText> {
        &self.issuer_name
    }
    /// Returns issuer-LEI association.
    pub const fn issuer_lei(&self) -> &FundReportedValue<SourceIdentifier> {
        &self.issuer_lei
    }
    /// Returns issue title/description association.
    pub const fn title(&self) -> &FundReportedValue<FundSourceText> {
        &self.title
    }
    /// Returns source CUSIP association.
    pub const fn cusip(&self) -> &FundReportedValue<Cusip> {
        &self.cusip
    }
    /// Returns source ISIN association.
    pub const fn isin(&self) -> &FundReportedValue<Isin> {
        &self.isin
    }
    /// Returns source ticker association.
    pub const fn ticker(&self) -> &FundReportedValue<SourceIdentifier> {
        &self.ticker
    }
}

/// Presence/completeness state for one N-PORT holding supplement table.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FundSupplementDisposition {
    /// One or more source rows were reported for this holding.
    Reported,
    /// The official table was present but contained no matching rows.
    PresentEmpty,
    /// Provider metadata declared the table but the archive omitted it as empty.
    DeclaredAbsent,
    /// The table exists, but no derived row applies to this holding.
    NoRowForHolding,
    /// Coverage cannot establish whether a row should exist.
    CoverageGap,
}

/// Bounded contiguous row range inside one record's [`FundSourceLineage`].
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FundLineageRowRange {
    start: u32,
    count: NonZeroU32,
}

impl FundLineageRowRange {
    /// Constructs a zero-based start and nonzero bounded count.
    pub const fn new(start: u32, count: NonZeroU32) -> Self {
        Self { start, count }
    }

    /// Returns the zero-based lineage start.
    pub const fn start(self) -> u32 {
        self.start
    }

    /// Returns the nonzero number of ordered rows.
    pub const fn count(self) -> NonZeroU32 {
        self.count
    }
}

/// Complete state and exact row coordinates for one N-PORT supplement table.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FundHoldingSupplementEvidence {
    table: FundSourceTable,
    disposition: FundSupplementDisposition,
    state_evidence: EvidenceDigest,
    lineage_rows: Option<FundLineageRowRange>,
}

impl FundHoldingSupplementEvidence {
    /// Constructs one exact supplement-table state.
    pub fn try_new(
        table: FundSourceTable,
        disposition: FundSupplementDisposition,
        state_evidence: EvidenceDigest,
        lineage_rows: Option<FundLineageRowRange>,
    ) -> Result<Self, FundHoldingsError> {
        validate_evidence(state_evidence)?;
        if !table.is_holding_supplement()
            || (disposition == FundSupplementDisposition::Reported) != lineage_rows.is_some()
        {
            return Err(FundHoldingsError::InvalidSupplementSet);
        }
        Ok(Self {
            table,
            disposition,
            state_evidence,
            lineage_rows,
        })
    }

    /// Returns the exact supplement table.
    pub const fn table(&self) -> FundSourceTable {
        self.table
    }
    /// Returns reported/empty/absent/no-row/coverage-gap state.
    pub const fn disposition(&self) -> FundSupplementDisposition {
        self.disposition
    }
    /// Returns exact evidence establishing this table state.
    pub const fn state_evidence(&self) -> EvidenceDigest {
        self.state_evidence
    }
    /// Returns the exact contiguous lineage range when rows were reported.
    pub const fn lineage_rows(&self) -> Option<FundLineageRowRange> {
        self.lineage_rows
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FundHoldingSupplementEvidenceWire {
    table: FundSourceTable,
    disposition: FundSupplementDisposition,
    state_evidence: EvidenceDigest,
    lineage_rows: Option<FundLineageRowRange>,
}

impl<'de> Deserialize<'de> for FundHoldingSupplementEvidence {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = FundHoldingSupplementEvidenceWire::deserialize(deserializer)?;
        Self::try_new(
            wire.table,
            wire.disposition,
            wire.state_evidence,
            wire.lineage_rows,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Complete canonical N-PORT holding values and classifications.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FundPortfolioHoldingAttributes {
    quantity: FundReportedValue<FundHoldingQuantity>,
    value: FundReportedValue<FundCurrencyAmount>,
    exchange_rate: FundReportedValue<FundReportedDecimal>,
    percentage_of_net_assets: FundReportedValue<FundReportedDecimal>,
    payoff_profile: FundReportedValue<SourceIdentifier>,
    asset_category: FundReportedValue<SourceIdentifier>,
    other_asset: FundReportedValue<FundSourceText>,
    issuer_type: FundReportedValue<SourceIdentifier>,
    other_issuer: FundReportedValue<FundSourceText>,
    investment_country: FundReportedValue<SourceIdentifier>,
    restricted_security: FundReportedValue<bool>,
    fair_value_level: FundReportedValue<SourceIdentifier>,
    derivative_category: FundReportedValue<SourceIdentifier>,
}

impl FundPortfolioHoldingAttributes {
    /// Constructs source values without replacing missing or conflict states with zero.
    #[allow(
        clippy::too_many_arguments,
        reason = "holding fields retain independent source states"
    )]
    pub const fn new(
        quantity: FundReportedValue<FundHoldingQuantity>,
        value: FundReportedValue<FundCurrencyAmount>,
        exchange_rate: FundReportedValue<FundReportedDecimal>,
        percentage_of_net_assets: FundReportedValue<FundReportedDecimal>,
        payoff_profile: FundReportedValue<SourceIdentifier>,
        asset_category: FundReportedValue<SourceIdentifier>,
        other_asset: FundReportedValue<FundSourceText>,
        issuer_type: FundReportedValue<SourceIdentifier>,
        other_issuer: FundReportedValue<FundSourceText>,
        investment_country: FundReportedValue<SourceIdentifier>,
        restricted_security: FundReportedValue<bool>,
        fair_value_level: FundReportedValue<SourceIdentifier>,
        derivative_category: FundReportedValue<SourceIdentifier>,
    ) -> Self {
        Self {
            quantity,
            value,
            exchange_rate,
            percentage_of_net_assets,
            payoff_profile,
            asset_category,
            other_asset,
            issuer_type,
            other_issuer,
            investment_country,
            restricted_security,
            fair_value_level,
            derivative_category,
        }
    }

    /// Returns exact quantity/unit state.
    pub const fn quantity(&self) -> &FundReportedValue<FundHoldingQuantity> {
        &self.quantity
    }
    /// Returns exact value/currency state.
    pub const fn value(&self) -> &FundReportedValue<FundCurrencyAmount> {
        &self.value
    }
    /// Returns exact exchange-rate state.
    pub const fn exchange_rate(&self) -> &FundReportedValue<FundReportedDecimal> {
        &self.exchange_rate
    }
    /// Returns exact percentage-of-net-assets state.
    pub const fn percentage_of_net_assets(&self) -> &FundReportedValue<FundReportedDecimal> {
        &self.percentage_of_net_assets
    }
    /// Returns payoff-profile state.
    pub const fn payoff_profile(&self) -> &FundReportedValue<SourceIdentifier> {
        &self.payoff_profile
    }
    /// Returns asset-category state.
    pub const fn asset_category(&self) -> &FundReportedValue<SourceIdentifier> {
        &self.asset_category
    }
    /// Returns other-asset description state.
    pub const fn other_asset(&self) -> &FundReportedValue<FundSourceText> {
        &self.other_asset
    }
    /// Returns issuer-type state.
    pub const fn issuer_type(&self) -> &FundReportedValue<SourceIdentifier> {
        &self.issuer_type
    }
    /// Returns other-issuer description state.
    pub const fn other_issuer(&self) -> &FundReportedValue<FundSourceText> {
        &self.other_issuer
    }
    /// Returns investment-country state.
    pub const fn investment_country(&self) -> &FundReportedValue<SourceIdentifier> {
        &self.investment_country
    }
    /// Returns restricted-security state.
    pub const fn restricted_security(&self) -> &FundReportedValue<bool> {
        &self.restricted_security
    }
    /// Returns fair-value-level state.
    pub const fn fair_value_level(&self) -> &FundReportedValue<SourceIdentifier> {
        &self.fair_value_level
    }
    /// Returns derivative-category state.
    pub const fn derivative_category(&self) -> &FundReportedValue<SourceIdentifier> {
        &self.derivative_category
    }
}

/// Canonical portfolio-holding evidence for one N-PORT accession and `HOLDING_ID`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FundPortfolioHoldingEvidence {
    filing: FundFilingIdentity,
    holding_id: SourceIdentifier,
    held_security: FundHoldingSecurityIdentity,
    associations: FundHoldingAssociations,
    attributes: FundPortfolioHoldingAttributes,
    supplements: Box<[FundHoldingSupplementEvidence]>,
    lineage: FundSourceLineage,
}

impl FundPortfolioHoldingEvidence {
    /// Constructs one exact, accession-scoped N-PORT holding.
    #[allow(
        clippy::too_many_arguments,
        reason = "identity, values, supplements, and lineage stay distinct"
    )]
    pub fn try_new(
        filing: FundFilingIdentity,
        holding_id: SourceIdentifier,
        held_security: FundHoldingSecurityIdentity,
        associations: FundHoldingAssociations,
        attributes: FundPortfolioHoldingAttributes,
        mut supplements: Vec<FundHoldingSupplementEvidence>,
        lineage: FundSourceLineage,
    ) -> Result<Self, FundHoldingsError> {
        validate_common_lineage(&filing, &lineage)?;
        if filing.family() != FundSourceFamily::Nport
            || !lineage.contains_table(FundSourceTable::NportHolding)
            || matches!(
                held_security.authoritative_identifier(),
                Some(FundSecurityIdentifier::Isin(_))
            ) && !lineage.contains_table(FundSourceTable::NportIdentifiers)
            || (associations.isin.reported().is_some() || associations.ticker.reported().is_some())
                && !lineage.contains_table(FundSourceTable::NportIdentifiers)
        {
            return Err(FundHoldingsError::InconsistentFamily);
        }
        supplements.sort_by_key(FundHoldingSupplementEvidence::table);
        if supplements.len() != FUND_HOLDING_SUPPLEMENT_TABLE_COUNT
            || supplements
                .windows(2)
                .any(|pair| pair[0].table == pair[1].table)
            || supplements
                .iter()
                .any(|supplement| !supplement.table.is_holding_supplement())
        {
            return Err(FundHoldingsError::InvalidSupplementSet);
        }
        for supplement in &supplements {
            if let Some(range) = supplement.lineage_rows {
                let start =
                    usize::try_from(range.start).map_err(|_| FundHoldingsError::InvalidLineage)?;
                let count = usize::try_from(range.count.get())
                    .map_err(|_| FundHoldingsError::InvalidLineage)?;
                let end = start
                    .checked_add(count)
                    .ok_or(FundHoldingsError::InvalidLineage)?;
                let rows = lineage
                    .rows
                    .get(start..end)
                    .ok_or(FundHoldingsError::InvalidLineage)?;
                if rows.iter().any(|row| row.table != supplement.table) {
                    return Err(FundHoldingsError::InvalidLineage);
                }
            }
        }
        Ok(Self {
            filing,
            holding_id,
            held_security,
            associations,
            attributes,
            supplements: supplements.into_boxed_slice(),
            lineage,
        })
    }

    /// Returns exact fund/source filing identity, including accession.
    pub const fn filing(&self) -> &FundFilingIdentity {
        &self.filing
    }
    /// Returns provider-native `HOLDING_ID`, scoped by the filing accession.
    pub const fn holding_id(&self) -> &SourceIdentifier {
        &self.holding_id
    }
    /// Returns exact/ambiguous/unresolved held-security identity.
    pub const fn held_security(&self) -> &FundHoldingSecurityIdentity {
        &self.held_security
    }
    /// Returns non-authoritative issuer/identifier associations.
    pub const fn associations(&self) -> &FundHoldingAssociations {
        &self.associations
    }
    /// Returns exact holding values/classifications.
    pub const fn attributes(&self) -> &FundPortfolioHoldingAttributes {
        &self.attributes
    }
    /// Returns all 19 supplement-table states.
    pub fn supplements(&self) -> &[FundHoldingSupplementEvidence] {
        &self.supplements
    }
    /// Returns ordered provider-native source rows.
    pub const fn lineage(&self) -> &FundSourceLineage {
        &self.lineage
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FundPortfolioHoldingEvidenceWire {
    filing: FundFilingIdentity,
    holding_id: SourceIdentifier,
    held_security: FundHoldingSecurityIdentity,
    associations: FundHoldingAssociations,
    attributes: FundPortfolioHoldingAttributes,
    supplements: Vec<FundHoldingSupplementEvidence>,
    lineage: FundSourceLineage,
}

impl<'de> Deserialize<'de> for FundPortfolioHoldingEvidence {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = FundPortfolioHoldingEvidenceWire::deserialize(deserializer)?;
        Self::try_new(
            wire.filing,
            wire.holding_id,
            wire.held_security,
            wire.associations,
            wire.attributes,
            wire.supplements,
            wire.lineage,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Closed canonical fund evidence family for `market_squawk.fund_holdings` partitions.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "record", content = "payload", rename_all = "snake_case")]
pub enum FundEvidenceRecord {
    /// Filing/report identity and form-specific report state.
    Report(Box<FundReportEvidence>),
    /// Fund/share-class financial and operational evidence.
    ShareClass(Box<FundShareClassEvidence>),
    /// N-PORT portfolio holding evidence.
    PortfolioHolding(Box<FundPortfolioHoldingEvidence>),
}

/// Fund holdings contract construction failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FundHoldingsError {
    /// Exact provider decimal lexical representation is invalid.
    InvalidDecimal,
    /// Provider text is empty, untrimmed, oversized, or contains controls.
    InvalidText,
    /// Evidence is empty or does not use canonical SHA-256.
    InvalidEvidence,
    /// Filing/report clocks are inconsistent.
    InvalidChronology,
    /// Canonical or provider identity is inconsistent.
    InvalidIdentity,
    /// Amendment, predecessor, successor, or conflict state is inconsistent.
    InvalidRevision,
    /// Source-row lineage is missing, duplicated, or inconsistent.
    InvalidLineage,
    /// Family-specific value state is inconsistent.
    InvalidValueState,
    /// Supplement tables are incomplete or internally inconsistent.
    InvalidSupplementSet,
    /// A bounded collection is empty or exceeds its hard ceiling.
    CollectionLimitExceeded,
    /// A collection repeats an exact identity.
    DuplicateEntry,
    /// N-PORT/N-CEN family identity disagrees with its rows or record type.
    InconsistentFamily,
    /// Domain schema version is unsupported.
    Schema(SchemaVersionError),
}

impl fmt::Display for FundHoldingsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidDecimal => formatter.write_str("fund decimal lexical value is invalid"),
            Self::InvalidText => formatter.write_str("fund source text is invalid"),
            Self::InvalidEvidence => formatter.write_str("fund evidence must be nonzero SHA-256"),
            Self::InvalidChronology => formatter.write_str("fund filing clocks are inconsistent"),
            Self::InvalidIdentity => formatter.write_str("fund identity is inconsistent"),
            Self::InvalidRevision => formatter.write_str("fund revision state is inconsistent"),
            Self::InvalidLineage => formatter.write_str("fund source-row lineage is inconsistent"),
            Self::InvalidValueState => formatter.write_str("fund value state is inconsistent"),
            Self::InvalidSupplementSet => {
                formatter.write_str("fund supplement set is inconsistent")
            }
            Self::CollectionLimitExceeded => {
                formatter.write_str("fund collection exceeds its hard bound")
            }
            Self::DuplicateEntry => {
                formatter.write_str("fund collection contains a duplicate entry")
            }
            Self::InconsistentFamily => formatter.write_str("fund source family is inconsistent"),
            Self::Schema(error) => write!(formatter, "fund schema version is unsupported: {error}"),
        }
    }
}

impl std::error::Error for FundHoldingsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Schema(error) => Some(error),
            _ => None,
        }
    }
}

impl From<SchemaVersionError> for FundHoldingsError {
    fn from(value: SchemaVersionError) -> Self {
        Self::Schema(value)
    }
}

fn validate_revision_link(link: &FundRevisionLink) -> Result<(), FundHoldingsError> {
    if let FundRevisionLink::Exact { evidence, .. } = link {
        validate_evidence(*evidence)?;
    }
    Ok(())
}

fn revision_link_accession(link: &FundRevisionLink) -> Option<&SourceIdentifier> {
    match link {
        FundRevisionLink::Exact { accession, .. } => Some(accession),
        FundRevisionLink::NotApplicable
        | FundRevisionLink::NotObserved
        | FundRevisionLink::Unresolved
        | FundRevisionLink::Conflict => None,
    }
}

fn is_not_applicable<T>(value: &FundReportedValue<T>) -> bool {
    matches!(
        value,
        FundReportedValue::Missing(FundMissingState::NotApplicable)
    )
}

fn etf_mechanics_not_applicable(value: &FundEtfMechanics) -> bool {
    is_not_applicable(&value.collateral_required)
        && is_not_applicable(&value.shares_per_creation_unit)
        && is_not_applicable(&value.shares_per_redemption_unit)
        && is_not_applicable(&value.in_kind)
}

fn validate_evidence(evidence: EvidenceDigest) -> Result<(), FundHoldingsError> {
    if evidence.algorithm() != DigestAlgorithm::Sha256 || evidence.bytes() == [0; 32] {
        Err(FundHoldingsError::InvalidEvidence)
    } else {
        Ok(())
    }
}

fn validate_common_lineage(
    filing: &FundFilingIdentity,
    lineage: &FundSourceLineage,
) -> Result<(), FundHoldingsError> {
    if filing.family != lineage.family {
        return Err(FundHoldingsError::InconsistentFamily);
    }
    let required = match filing.family {
        FundSourceFamily::Nport => [
            FundSourceTable::NportSubmission,
            FundSourceTable::NportRegistrant,
            FundSourceTable::NportFund,
        ],
        FundSourceFamily::Ncen => [
            FundSourceTable::NcenSubmission,
            FundSourceTable::NcenRegistrant,
            FundSourceTable::NcenFund,
        ],
    };
    if required.iter().any(|table| !lineage.contains_table(*table)) {
        Err(FundHoldingsError::InvalidLineage)
    } else {
        Ok(())
    }
}

fn is_sec_cik(value: &str) -> bool {
    value.len() == 10 && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn is_sec_series_id(value: &str) -> bool {
    value.len() == 10
        && value.starts_with('S')
        && value[1..].bytes().all(|byte| byte.is_ascii_digit())
}

fn is_sec_accession(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 20
        && bytes.get(10) == Some(&b'-')
        && bytes.get(13) == Some(&b'-')
        && bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| matches!(index, 10 | 13) || byte.is_ascii_digit())
}
