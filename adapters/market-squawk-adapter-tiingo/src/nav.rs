use market_squawk_domain::{
    AvailabilityEvidence, CalendarDate, Currency, DigestAlgorithm, EvidenceDigest, InstrumentId,
    Money, ProviderInstrumentId, SourceIdentifier, Timestamp,
};
use rust_decimal::Decimal;
use sha2::{Digest as _, Sha256};

use crate::decoder::append_field;
use crate::{
    TiingoAdapterError, TiingoCoverage, TiingoEodReceipt, TiingoEodRow, TiingoMetadataReceipt,
    TiingoPaginationEvidence, TiingoRequestDisposition, TiingoRequestScope, TiingoResponseEvidence,
    TiingoTicker,
};

const TIINGO_MUTUAL_FUND_EXCHANGE_CODE: &str = "MF";

/// Exact canonical-reference context required before a Tiingo ticker can become mutual-fund NAV.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoFundContext {
    instrument_id: InstrumentId,
    provider_instrument_id: ProviderInstrumentId,
    ticker: TiingoTicker,
    instrument_definition_revision: SourceIdentifier,
    mutual_fund_classification_revision: SourceIdentifier,
    entitlement_generation: SourceIdentifier,
    native_schema_revision: SourceIdentifier,
    currency: Currency,
}

impl TiingoFundContext {
    /// Constructs exact mutual-fund/share-class context from external reference authority.
    ///
    /// # Errors
    ///
    /// Rejects a provider identifier that differs from the exact Tiingo ticker. A caller must not
    /// construct this context for an ETF, equity, or unresolved fund/share class.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        instrument_id: InstrumentId,
        provider_instrument_id: ProviderInstrumentId,
        ticker: TiingoTicker,
        instrument_definition_revision: SourceIdentifier,
        mutual_fund_classification_revision: SourceIdentifier,
        entitlement_generation: SourceIdentifier,
        native_schema_revision: SourceIdentifier,
        currency: Currency,
    ) -> Result<Self, TiingoAdapterError> {
        if provider_instrument_id.as_str() != ticker.as_str() {
            return Err(TiingoAdapterError::InvalidFundContext);
        }
        Ok(Self {
            instrument_id,
            provider_instrument_id,
            ticker,
            instrument_definition_revision,
            mutual_fund_classification_revision,
            entitlement_generation,
            native_schema_revision,
            currency,
        })
    }

    /// Returns the stable canonical fund/share-class identity.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the exact Tiingo provider instrument.
    pub const fn provider_instrument_id(&self) -> &ProviderInstrumentId {
        &self.provider_instrument_id
    }

    /// Returns the exact Tiingo ticker.
    pub const fn ticker(&self) -> &TiingoTicker {
        &self.ticker
    }

    /// Returns the exact external instrument-definition revision.
    pub const fn instrument_definition_revision(&self) -> &SourceIdentifier {
        &self.instrument_definition_revision
    }

    /// Returns the evidence revision proving this share class is a mutual fund.
    pub const fn mutual_fund_classification_revision(&self) -> &SourceIdentifier {
        &self.mutual_fund_classification_revision
    }

    /// Returns the credential/entitlement generation authorizing the response.
    pub const fn entitlement_generation(&self) -> &SourceIdentifier {
        &self.entitlement_generation
    }

    /// Returns the reviewed provider-native decoder revision.
    pub const fn native_schema_revision(&self) -> &SourceIdentifier {
        &self.native_schema_revision
    }

    /// Returns exact externally resolved currency. Tiingo wire data never supplies this field.
    pub const fn currency(&self) -> Currency {
        self.currency
    }
}

/// Availability established only by exact per-ticker metadata coverage dates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TiingoFundSupport {
    /// Tiingo supplied a non-null inclusive coverage interval.
    Supported {
        /// First supported date.
        start_date: CalendarDate,
        /// Last supported date in this metadata response.
        end_date: CalendarDate,
    },
    /// Coverage dates were both null. Archive membership or ticker shape is not support.
    Unsupported,
}

/// Classifies only metadata coverage; it does not infer a mutual fund from a symbol or name.
pub const fn classify_fund_support(metadata: &TiingoMetadataReceipt) -> TiingoFundSupport {
    match metadata.metadata().coverage() {
        TiingoCoverage::Supported {
            start_date,
            end_date,
        } => TiingoFundSupport::Supported {
            start_date,
            end_date,
        },
        TiingoCoverage::Unsupported => TiingoFundSupport::Unsupported,
    }
}

/// Tiingo's documented availability guidance, retained only as guidance rather than an instant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TiingoAvailabilityGuidance {
    /// Mutual-fund NAV is described as available after midnight Eastern.
    MutualFundAfterMidnightEastern,
}

/// Provider publication coordinate for the reviewed EOD wire contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TiingoSourcePublicationEvidence {
    /// The reviewed daily row contains no source publication date or timestamp.
    NotSupplied,
}

/// Provider revision/finality coordinate for the reviewed EOD wire contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TiingoProviderRevisionEvidence {
    /// No immutable provider revision identifier or exact finality event is supplied.
    NotSupplied,
}

/// Why a syntactically valid mutual-fund row cannot be admitted as observed NAV.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TiingoNavInvalidReason {
    /// A present raw open/high/low value differed from raw close.
    RawOhlcDoesNotRepresentOneNav,
    /// Raw close was zero or negative.
    NonPositiveNav,
    /// The row date fell outside the exact metadata coverage interval.
    OutsideMetadataCoverage,
}

/// Closed exact NAV value-or-missing state. No state is converted to zero or market price.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TiingoNavValueState {
    /// Exact raw-close NAV after mutual-fund row-shape validation.
    Observed(Money),
    /// The scheduler proved the requested daily NAV was not yet expected to be published.
    NotYetPublished,
    /// Exact per-ticker metadata did not establish provider support.
    Unsupported,
    /// A supported response omitted the NAV value or returned no row for an expected date.
    SourceMissing,
    /// A provider row could not safely represent NAV.
    Invalid(TiingoNavInvalidReason),
    /// Retrieval, entitlement, quota, or durable-publication evidence was unavailable.
    Unavailable,
}

/// Precision-preserving NAV clocks before shared canonical ingestion/publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoNavClocks {
    effective_nav_date: CalendarDate,
    source_publication: TiingoSourcePublicationEvidence,
    availability_guidance: TiingoAvailabilityGuidance,
    availability: AvailabilityEvidence,
    received_at: Timestamp,
    decoded_at: Timestamp,
}

impl TiingoNavClocks {
    fn from_response(date: CalendarDate, response: &TiingoResponseEvidence) -> Self {
        Self {
            effective_nav_date: date,
            source_publication: TiingoSourcePublicationEvidence::NotSupplied,
            availability_guidance: TiingoAvailabilityGuidance::MutualFundAfterMidnightEastern,
            availability: AvailabilityEvidence::local_first_observed(response.received_at()),
            received_at: response.received_at(),
            decoded_at: response.decoded_at(),
        }
    }

    /// Returns the exact source valuation date without an invented midnight instant.
    pub const fn effective_nav_date(&self) -> CalendarDate {
        self.effective_nav_date
    }

    /// Returns the absent provider-publication coordinate explicitly.
    pub const fn source_publication(&self) -> TiingoSourcePublicationEvidence {
        self.source_publication
    }

    /// Returns source schedule guidance, never promoted to an evidenced instant.
    pub const fn availability_guidance(&self) -> TiingoAvailabilityGuidance {
        self.availability_guidance
    }

    /// Returns conservative first-local-observation PIT evidence.
    pub const fn availability(&self) -> &AvailabilityEvidence {
        &self.availability
    }

    /// Returns when the exact response body completed receipt locally.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns when strict native decode completed locally.
    pub const fn decoded_at(&self) -> Timestamp {
        self.decoded_at
    }
}

/// Source-neutral provider-native candidate awaiting sealed canonical FundNav mapping.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoNavObservationCandidate {
    context: TiingoFundContext,
    nav_date: CalendarDate,
    value: TiingoNavValueState,
    clocks: TiingoNavClocks,
    provider_revision: TiingoProviderRevisionEvidence,
    response_status: u16,
    request_identity: EvidenceDigest,
    raw_object_digest: EvidenceDigest,
    provider_row_digest: Option<EvidenceDigest>,
    pagination: TiingoPaginationEvidence,
    request_disposition: TiingoRequestDisposition,
    family_identity: EvidenceDigest,
    payload_identity: EvidenceDigest,
    provenance_identity: EvidenceDigest,
}

impl TiingoNavObservationCandidate {
    /// Returns exact fund/share-class and source-contract context.
    pub const fn context(&self) -> &TiingoFundContext {
        &self.context
    }

    /// Returns the exact NAV date.
    pub const fn nav_date(&self) -> CalendarDate {
        self.nav_date
    }

    /// Returns exact observed money or a closed missing/unavailable state.
    pub const fn value(&self) -> TiingoNavValueState {
        self.value
    }

    /// Returns effective/publication/availability/receive/decode clock evidence.
    pub const fn clocks(&self) -> &TiingoNavClocks {
        &self.clocks
    }

    /// Returns explicit absence of a source revision/finality identifier.
    pub const fn provider_revision(&self) -> TiingoProviderRevisionEvidence {
        self.provider_revision
    }

    /// Returns the exact successful provider status retained by native decoding.
    pub const fn response_status(&self) -> u16 {
        self.response_status
    }

    /// Returns the credential-free identity of the exact provider request.
    pub const fn request_identity(&self) -> EvidenceDigest {
        self.request_identity
    }

    /// Returns the exact raw response-body identity.
    pub const fn raw_object_digest(&self) -> EvidenceDigest {
        self.raw_object_digest
    }

    /// Returns the exact provider-native row identity when a row existed.
    pub const fn provider_row_digest(&self) -> Option<EvidenceDigest> {
        self.provider_row_digest
    }

    /// Returns application date-window evidence without inventing a Tiingo cursor.
    pub const fn pagination(&self) -> TiingoPaginationEvidence {
        self.pagination
    }

    /// Returns actual request/row/byte disposition.
    pub const fn request_disposition(&self) -> TiingoRequestDisposition {
        self.request_disposition
    }

    /// Returns the natural family identity excluding revision, payload, and local receipt time.
    pub const fn family_identity(&self) -> EvidenceDigest {
        self.family_identity
    }

    /// Returns the NAV value-or-missing payload identity used to detect corrections.
    pub const fn payload_identity(&self) -> EvidenceDigest {
        self.payload_identity
    }

    /// Returns source contract, lineage, and local clock identity separate from NAV payload.
    pub const fn provenance_identity(&self) -> EvidenceDigest {
        self.provenance_identity
    }
}

/// Converts one strict daily row into NAV only with exact mutual-fund context.
///
/// Adjusted OHLC, volume, dividends, and split factors remain separate EOD evidence and never
/// replace raw close as NAV.
pub fn normalize_mutual_fund_row(
    context: TiingoFundContext,
    metadata: &TiingoMetadataReceipt,
    response: &TiingoEodReceipt,
    row: &TiingoEodRow,
) -> Result<TiingoNavObservationCandidate, TiingoAdapterError> {
    validate_context(&context, metadata, response.evidence())?;
    if metadata.metadata().exchange_code() != TIINGO_MUTUAL_FUND_EXCHANGE_CODE {
        return Err(TiingoAdapterError::InvalidFundContext);
    }
    let value = match classify_fund_support(metadata) {
        TiingoFundSupport::Unsupported => return Err(TiingoAdapterError::InvalidFundContext),
        TiingoFundSupport::Supported {
            start_date,
            end_date,
        } if row.date() < start_date || row.date() > end_date => {
            TiingoNavValueState::Invalid(TiingoNavInvalidReason::OutsideMetadataCoverage)
        }
        TiingoFundSupport::Supported { .. } => nav_value_from_row(context.currency(), row),
    };
    Ok(build_candidate(
        context,
        row.date(),
        value,
        response.evidence(),
        Some(row.row_digest()),
        response.pagination(),
        response.disposition(),
    ))
}

/// Constructs explicit unsupported/not-yet-published/source-missing/unavailable evidence.
///
/// # Errors
///
/// Rejects `Observed`, which must come only from strict row normalization, and rejects a response
/// for a different provider ticker.
pub fn unavailable_nav_candidate(
    context: TiingoFundContext,
    nav_date: CalendarDate,
    state: TiingoNavValueState,
    evidence: &TiingoResponseEvidence,
    disposition: TiingoRequestDisposition,
) -> Result<TiingoNavObservationCandidate, TiingoAdapterError> {
    if matches!(state, TiingoNavValueState::Observed(_))
        || context.ticker() != evidence.request().ticker()
    {
        return Err(TiingoAdapterError::InvalidFundContext);
    }
    Ok(build_candidate(
        context,
        nav_date,
        state,
        evidence,
        None,
        pagination_from_request(evidence),
        disposition,
    ))
}

fn pagination_from_request(evidence: &TiingoResponseEvidence) -> TiingoPaginationEvidence {
    match evidence.request().scope() {
        TiingoRequestScope::History { page, .. } => {
            TiingoPaginationEvidence::ApplicationDateWindow(*page)
        }
        TiingoRequestScope::Latest | TiingoRequestScope::Metadata => {
            TiingoPaginationEvidence::NotApplicable
        }
    }
}

fn validate_context(
    context: &TiingoFundContext,
    metadata: &TiingoMetadataReceipt,
    response: &TiingoResponseEvidence,
) -> Result<(), TiingoAdapterError> {
    if context.ticker() != metadata.metadata().ticker()
        || context.ticker() != response.request().ticker()
        || context.provider_instrument_id().as_str() != context.ticker().as_str()
    {
        return Err(TiingoAdapterError::InvalidFundContext);
    }
    Ok(())
}

fn nav_value_from_row(currency: Currency, row: &TiingoEodRow) -> TiingoNavValueState {
    let Some(close) = row.close() else {
        return TiingoNavValueState::SourceMissing;
    };
    if close <= Decimal::ZERO {
        return TiingoNavValueState::Invalid(TiingoNavInvalidReason::NonPositiveNav);
    }
    let (open, high, low, _) = row.raw_ohlc();
    if [open, high, low]
        .into_iter()
        .flatten()
        .any(|value| value != close)
    {
        return TiingoNavValueState::Invalid(TiingoNavInvalidReason::RawOhlcDoesNotRepresentOneNav);
    }
    TiingoNavValueState::Observed(Money::new(close, currency))
}

fn build_candidate(
    context: TiingoFundContext,
    nav_date: CalendarDate,
    value: TiingoNavValueState,
    response: &TiingoResponseEvidence,
    provider_row_digest: Option<EvidenceDigest>,
    pagination: TiingoPaginationEvidence,
    request_disposition: TiingoRequestDisposition,
) -> TiingoNavObservationCandidate {
    let family_identity = nav_family_identity(&context, nav_date);
    let payload_identity = nav_payload_identity(family_identity, value);
    let clocks = TiingoNavClocks::from_response(nav_date, response);
    let provenance_identity =
        nav_provenance_identity(&context, response, provider_row_digest, request_disposition);
    TiingoNavObservationCandidate {
        context,
        nav_date,
        value,
        clocks,
        provider_revision: TiingoProviderRevisionEvidence::NotSupplied,
        response_status: response.status(),
        request_identity: response.request().request_identity(),
        raw_object_digest: response.body_digest(),
        provider_row_digest,
        pagination,
        request_disposition,
        family_identity,
        payload_identity,
        provenance_identity,
    }
}

fn nav_family_identity(context: &TiingoFundContext, nav_date: CalendarDate) -> EvidenceDigest {
    let mut hasher = Sha256::new();
    append_field(&mut hasher, b"market-squawk.fund-nav-family.v1");
    append_field(&mut hasher, b"tiingo");
    append_field(&mut hasher, b"starter");
    append_field(&mut hasher, b"daily-eod");
    append_field(&mut hasher, context.instrument_id().to_string().as_bytes());
    append_field(
        &mut hasher,
        context.provider_instrument_id().as_str().as_bytes(),
    );
    append_field(&mut hasher, nav_date.to_string().as_bytes());
    append_field(&mut hasher, b"per_share");
    append_field(&mut hasher, context.currency().as_str().as_bytes());
    EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into())
}

fn nav_payload_identity(family: EvidenceDigest, value: TiingoNavValueState) -> EvidenceDigest {
    let mut hasher = Sha256::new();
    append_field(&mut hasher, b"market-squawk.fund-nav-payload.v1");
    append_field(&mut hasher, &family.bytes());
    match value {
        TiingoNavValueState::Observed(money) => {
            append_field(&mut hasher, b"observed");
            append_field(&mut hasher, money.amount().to_string().as_bytes());
            append_field(&mut hasher, money.currency().as_str().as_bytes());
        }
        TiingoNavValueState::NotYetPublished => append_field(&mut hasher, b"not_yet_published"),
        TiingoNavValueState::Unsupported => append_field(&mut hasher, b"unsupported"),
        TiingoNavValueState::SourceMissing => append_field(&mut hasher, b"source_missing"),
        TiingoNavValueState::Invalid(reason) => {
            append_field(&mut hasher, b"invalid");
            let reason = match reason {
                TiingoNavInvalidReason::RawOhlcDoesNotRepresentOneNav => {
                    b"raw_ohlc_conflict".as_slice()
                }
                TiingoNavInvalidReason::NonPositiveNav => b"non_positive_nav".as_slice(),
                TiingoNavInvalidReason::OutsideMetadataCoverage => b"outside_coverage".as_slice(),
            };
            append_field(&mut hasher, reason);
        }
        TiingoNavValueState::Unavailable => append_field(&mut hasher, b"unavailable"),
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into())
}

fn nav_provenance_identity(
    context: &TiingoFundContext,
    response: &TiingoResponseEvidence,
    provider_row_digest: Option<EvidenceDigest>,
    disposition: TiingoRequestDisposition,
) -> EvidenceDigest {
    let mut hasher = Sha256::new();
    append_field(&mut hasher, b"market-squawk.fund-nav-provenance.v1");
    append_field(
        &mut hasher,
        context.instrument_definition_revision().as_str().as_bytes(),
    );
    append_field(
        &mut hasher,
        context
            .mutual_fund_classification_revision()
            .as_str()
            .as_bytes(),
    );
    append_field(
        &mut hasher,
        context.entitlement_generation().as_str().as_bytes(),
    );
    append_field(
        &mut hasher,
        context.native_schema_revision().as_str().as_bytes(),
    );
    append_field(&mut hasher, &response.body_digest().bytes());
    if let Some(row) = provider_row_digest {
        append_field(&mut hasher, &row.bytes());
    } else {
        append_field(&mut hasher, b"no-provider-row");
    }
    append_field(
        &mut hasher,
        &response.received_at().unix_nanos().to_be_bytes(),
    );
    append_field(
        &mut hasher,
        &response.decoded_at().unix_nanos().to_be_bytes(),
    );
    append_field(&mut hasher, &disposition.requested_symbols().to_be_bytes());
    append_field(&mut hasher, &disposition.returned_symbols().to_be_bytes());
    append_field(&mut hasher, &disposition.missing_symbols().to_be_bytes());
    append_field(&mut hasher, &disposition.returned_rows().to_be_bytes());
    append_field(&mut hasher, &disposition.response_bytes().to_be_bytes());
    EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into())
}
