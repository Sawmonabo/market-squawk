//! Closed, durable runtime-verification evidence for provider onboarding.

use market_squawk_domain::{
    CalendarDate, DataQuality, DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp,
};
use market_squawk_platform::SecretGeneration;
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use url::Url;

use super::ProviderCapabilityRevision;

/// Exact Alpaca onboarding surface admitted by the Paper/IEX doctor receipt.
pub const ALPACA_BASIC_MARKET_DATA_SURFACE_ID: &str = "alpaca.basic-market-data";
/// Schema identity for the self-digested Alpaca Paper/IEX doctor receipt.
pub const ALPACA_PAPER_IEX_DOCTOR_RECEIPT_SCHEMA: &str = "market-squawk.alpaca-paper-iex-doctor/v1";
/// Maximum canonical bytes retained by one Alpaca Paper/IEX doctor receipt.
pub const MAX_ALPACA_PAPER_IEX_DOCTOR_RECEIPT_BYTES: usize = 16 * 1024;

const ALPACA_PAPER_IEX_DOCTOR_IMPLEMENTATION_REVISION: &str =
    "market-squawk.alpaca-paper-iex-doctor-implementation.v1";
const ALPACA_PAPER_IEX_DOCTOR_CONTRACT_DOMAIN: &[u8] =
    b"market-squawk/alpaca-paper-iex-doctor-contract/v1\0";
const ALPACA_PAPER_IEX_PROVIDER_OBSERVATION_ORIGIN: &str =
    "market-squawk.alpaca-paper-iex-doctor.provider-observed.v1";
const ALPACA_PAPER_IEX_DOCTOR_VALIDITY_NANOS: i64 = 15 * 60 * 1_000_000_000;
const ALPACA_DOCTOR_BATCH_REQUESTED: u32 = 50;
const MAX_ALPACA_DOCTOR_BATCH_COMPONENTS: u32 = 101;
const MAX_ALPACA_DOCTOR_HTTP_BYTES: u64 = 8 * 1024 * 1024;
const MAX_ALPACA_DOCTOR_HISTORY_PAGES: usize = 8;
const MAX_ALPACA_DOCTOR_HISTORY_BYTES: u64 = 16 * 1024 * 1024;
const MAX_ALPACA_DOCTOR_STREAM_FRAMES: u32 = 26;
const MAX_ALPACA_DOCTOR_STREAM_BYTES: u64 = 26 * 16 * 1024 * 1024;
const ALPACA_DOCTOR_QUOTE_ENDPOINT: &str =
    "https://data.alpaca.markets/v2/stocks/AAPL/quotes/latest?feed=iex";
const ALPACA_DOCTOR_BATCH_ENDPOINT: &str = "https://data.alpaca.markets/v2/stocks/snapshots";
const ALPACA_DOCTOR_HISTORY_ENDPOINT: &str = "https://data.alpaca.markets/v2/stocks/AAPL/bars";
const ALPACA_DOCTOR_STREAM_ENDPOINT: &str = "wss://stream.data.alpaca.markets/v2/iex";
const ALPACA_DOCTOR_STREAM_SUBSCRIPTION: &str =
    r#"{"action":"subscribe","trades":["AAPL"],"quotes":["AAPL"]}"#;
const ALPACA_DOCTOR_BATCH_SYMBOLS: [&str; ALPACA_DOCTOR_BATCH_REQUESTED as usize] = [
    "AAPL", "ABBV", "ABT", "ACN", "ADBE", "AMD", "AMGN", "AMZN", "AVGO", "BAC", "BKNG", "BLK",
    "BMY", "CAT", "CMCSA", "COP", "COST", "CRM", "CSCO", "CVS", "CVX", "DIS", "GE", "GILD", "GOOG",
    "GOOGL", "GS", "HD", "HON", "IBM", "INTC", "ISRG", "JNJ", "JPM", "KO", "LIN", "LLY", "LMT",
    "LOW", "MA", "MCD", "META", "MRK", "MS", "MSFT", "NFLX", "NVDA", "ORCL", "PEP", "PG",
];

/// Immutable reservation coordinates required before typed runtime evidence can be admitted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeVerificationContext {
    session_identifier: SourceIdentifier,
    public_configuration_digest: EvidenceDigest,
}

impl RuntimeVerificationContext {
    /// Binds canonical onboarding-session UUID text and exact public configuration.
    pub fn try_new(
        session_identifier: SourceIdentifier,
        public_configuration_digest: EvidenceDigest,
    ) -> Result<Self, RuntimeVerificationEvidenceError> {
        if !canonical_uuid_text(session_identifier.as_str()) {
            return Err(RuntimeVerificationEvidenceError::InvalidEvidence);
        }
        require_sha256(public_configuration_digest)?;
        Ok(Self {
            session_identifier,
            public_configuration_digest,
        })
    }

    /// Returns canonical lowercase-hyphenated onboarding-session UUID text.
    pub const fn session_identifier(&self) -> &SourceIdentifier {
        &self.session_identifier
    }

    /// Returns the exact public-configuration digest owned by that reservation.
    pub const fn public_configuration_digest(&self) -> EvidenceDigest {
        self.public_configuration_digest
    }
}

/// Closed runtime disposition for one independently probed capability.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeCapabilityDisposition {
    /// The exact bounded probe established current availability.
    Available,
    /// The exact bounded probe succeeded with a material declared limitation.
    Degraded,
    /// The exact bounded attempt established current unavailability.
    Unavailable,
    /// This receipt makes no runtime claim for the capability.
    NotProbed,
}

/// Exact credential realm admitted by the Paper/IEX receipt.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AlpacaDoctorCredentialRealm {
    /// Alpaca Paper Only / Paper Trading key issuance realm.
    Paper,
}

/// A provider rate field is either exactly observed or explicitly missing.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "state", content = "value", rename_all = "snake_case")]
pub enum AlpacaRateLimitField<T> {
    /// The provider supplied and the doctor parsed the exact value.
    Observed(T),
    /// The provider did not supply the field.
    Missing,
}

/// Typed `Retry-After` evidence without collapsing its two wire forms.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum AlpacaRetryAfterEvidence {
    /// Provider-supplied nonnegative delay in seconds.
    DelaySeconds(u64),
    /// Provider-supplied HTTP-date normalized to Unix seconds.
    AtUnixSeconds(i64),
}

/// Exact observed-or-missing rate evidence from one response boundary.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AlpacaDoctorRateEvidence {
    /// Provider-declared request ceiling.
    pub limit: AlpacaRateLimitField<u32>,
    /// Provider-declared remaining requests.
    pub remaining: AlpacaRateLimitField<u32>,
    /// Provider-declared reset coordinate in Unix seconds.
    pub reset_unix_seconds: AlpacaRateLimitField<i64>,
    /// Provider-declared retry boundary.
    pub retry_after: AlpacaRateLimitField<AlpacaRetryAfterEvidence>,
}

impl AlpacaDoctorRateEvidence {
    fn validate(self) -> Result<(), RuntimeVerificationEvidenceError> {
        if matches!(self.limit, AlpacaRateLimitField::Observed(0))
            || matches!(self.reset_unix_seconds, AlpacaRateLimitField::Observed(value) if value < 0)
            || matches!(self.retry_after, AlpacaRateLimitField::Observed(AlpacaRetryAfterEvidence::AtUnixSeconds(value)) if value < 0)
        {
            return Err(RuntimeVerificationEvidenceError::InvalidEvidence);
        }
        if let (AlpacaRateLimitField::Observed(limit), AlpacaRateLimitField::Observed(remaining)) =
            (self.limit, self.remaining)
            && remaining > limit
        {
            return Err(RuntimeVerificationEvidenceError::InvalidEvidence);
        }
        Ok(())
    }
}

/// Secret-free identity, size, timing, and rate evidence for one HTTP response.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AlpacaDoctorHttpEvidence {
    /// Digest of the exact code-owned endpoint, method, and fixed query contract.
    pub endpoint_contract_digest: EvidenceDigest,
    /// Digest of the exact secret-free canonical request.
    pub request_digest: EvidenceDigest,
    /// HTTP response status.
    pub status_code: u16,
    /// SHA-256 of the exact bounded response body; raw bytes are not retained here.
    pub body_digest: EvidenceDigest,
    /// Exact received response-body bytes.
    pub response_bytes: u64,
    /// Trusted application receipt time.
    pub received_at: Timestamp,
    /// Monotonic request latency in nanoseconds.
    pub latency_nanos: u64,
    /// Typed observed-or-missing response rate evidence.
    pub rate: AlpacaDoctorRateEvidence,
}

impl AlpacaDoctorHttpEvidence {
    fn validate(&self) -> Result<(), RuntimeVerificationEvidenceError> {
        require_sha256(self.endpoint_contract_digest)?;
        require_sha256(self.request_digest)?;
        require_sha256(self.body_digest)?;
        if !(100..=599).contains(&self.status_code)
            || self.response_bytes > MAX_ALPACA_DOCTOR_HTTP_BYTES
        {
            return Err(RuntimeVerificationEvidenceError::InvalidEvidence);
        }
        self.rate.validate()
    }

    fn successful(&self) -> bool {
        (200..=299).contains(&self.status_code)
    }
}

/// One independently dispositioned probe and its optional exact observation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AlpacaDoctorProbeEvidence<T> {
    /// Independent current disposition for this capability.
    pub disposition: RuntimeCapabilityDisposition,
    /// Digest of the exact disposition decision, including failures or omission policy.
    pub disposition_evidence_digest: EvidenceDigest,
    /// Exact bounded observation when the probe reached that evidence boundary.
    pub observation: Option<T>,
}

impl<T> AlpacaDoctorProbeEvidence<T> {
    fn validate(
        &self,
        validate_observation: impl FnOnce(&T) -> Result<bool, RuntimeVerificationEvidenceError>,
    ) -> Result<(), RuntimeVerificationEvidenceError> {
        require_sha256(self.disposition_evidence_digest)?;
        match (self.disposition, self.observation.as_ref()) {
            (RuntimeCapabilityDisposition::NotProbed, None) => Ok(()),
            (RuntimeCapabilityDisposition::Unavailable, None) => Ok(()),
            (RuntimeCapabilityDisposition::NotProbed, Some(_)) => {
                Err(RuntimeVerificationEvidenceError::InvalidEvidence)
            }
            (RuntimeCapabilityDisposition::Available, Some(observation)) => {
                if validate_observation(observation)? {
                    Ok(())
                } else {
                    Err(RuntimeVerificationEvidenceError::InvalidEvidence)
                }
            }
            (RuntimeCapabilityDisposition::Degraded, Some(observation)) => {
                let _ = validate_observation(observation)?;
                Ok(())
            }
            (RuntimeCapabilityDisposition::Unavailable, Some(observation)) => {
                let _ = validate_observation(observation)?;
                Ok(())
            }
            (RuntimeCapabilityDisposition::Available, None)
            | (RuntimeCapabilityDisposition::Degraded, None) => {
                Err(RuntimeVerificationEvidenceError::InvalidEvidence)
            }
        }
    }
}

/// Parsed one-symbol latest-quote evidence; no raw body or credential is retained.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AlpacaDoctorQuoteObservation {
    /// Exact HTTP boundary evidence.
    pub http: AlpacaDoctorHttpEvidence,
    /// Digest of the exact validated AAPL/IEX quote fields.
    pub semantic_result_digest: EvidenceDigest,
    /// Provider-authored quote timestamp, absent only from a non-Available observation.
    pub quote_timestamp: Option<Timestamp>,
    /// Exact provider bid price; serialized as a decimal string.
    #[serde(with = "rust_decimal::serde::str_option")]
    pub bid_price: Option<Decimal>,
    /// Exact provider ask price; serialized as a decimal string.
    #[serde(with = "rust_decimal::serde::str_option")]
    pub ask_price: Option<Decimal>,
    /// Exact provider bid size.
    pub bid_size: Option<u64>,
    /// Exact provider ask size.
    pub ask_size: Option<u64>,
}

/// Exact requested/returned cardinality and set evidence for the stock snapshot batch.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AlpacaDoctorBatchObservation {
    /// Exact HTTP boundary evidence.
    pub http: AlpacaDoctorHttpEvidence,
    /// Digest of the validated semantic batch result.
    pub semantic_result_digest: EvidenceDigest,
    /// Exact requested sentinel cardinality; version 1 requires 50.
    pub requested_count: u32,
    /// Unique requested symbols present in the provider response before semantic filtering.
    pub returned_count: u32,
    /// Requested symbols absent from the provider response.
    pub missing_count: u32,
    /// Provider-returned symbols outside the exact request.
    pub unexpected_count: u32,
    /// Duplicate provider components.
    pub duplicate_count: u32,
    /// Structurally or semantically invalid provider components.
    pub invalid_count: u32,
    /// Unique valid requested components available to product composition.
    pub effective_cardinality: u32,
    /// Canonical exact requested-set digest.
    pub requested_set_digest: EvidenceDigest,
    /// Canonical provider-returned-set digest.
    pub returned_set_digest: EvidenceDigest,
    /// Canonical missing-set digest, including the exact empty-set representation.
    pub missing_set_digest: EvidenceDigest,
    /// Canonical unexpected-set digest, including the exact empty-set representation.
    pub unexpected_set_digest: EvidenceDigest,
}

/// Exact bounded IEX authentication, subscription, and controlled-close evidence.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AlpacaDoctorStreamObservation {
    /// Digest of the exact WSS/IEX endpoint and message contract.
    pub endpoint_contract_digest: EvidenceDigest,
    /// Digest of the secret-free connection/subscription request material.
    pub request_digest: EvidenceDigest,
    /// Digest of the validated connected frame.
    pub connected_frame_digest: EvidenceDigest,
    /// Digest of the validated authenticated frame.
    pub authenticated_frame_digest: EvidenceDigest,
    /// Digest of the validated subscription acknowledgement.
    pub subscription_frame_digest: EvidenceDigest,
    /// Digest of the complete bounded stream result.
    pub semantic_result_digest: EvidenceDigest,
    /// HTTP upgrade status.
    pub handshake_status: u16,
    /// Typed rate evidence from the handshake response.
    pub handshake_rate: AlpacaDoctorRateEvidence,
    /// Exact acknowledged trade-symbol count.
    pub subscribed_trade_count: u32,
    /// Exact acknowledged quote-symbol count.
    pub subscribed_quote_count: u32,
    /// Exact bounded frames observed during the doctor exchange.
    pub frames_observed: u32,
    /// Exact bounded frame bytes observed during the doctor exchange.
    pub bytes_observed: u64,
    /// Trusted time when provider authentication was acknowledged.
    pub authenticated_at: Timestamp,
    /// Trusted time when the exact subscription was acknowledged.
    pub subscribed_at: Timestamp,
    /// Whether the client sent a controlled close frame.
    pub close_sent: bool,
    /// Whether the provider completed a clean close.
    pub clean_close_observed: bool,
    /// Trusted completion time.
    pub completed_at: Timestamp,
}

/// One exact historical response and its incoming/outgoing pagination-token identities.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AlpacaDoctorHistoricalPageEvidence {
    /// Exact HTTP response evidence for this page.
    pub http: AlpacaDoctorHttpEvidence,
    /// Digest of the incoming continuation token; absent only on page zero.
    pub request_page_token_digest: Option<EvidenceDigest>,
    /// Digest of the outgoing continuation token; absent only on the terminal page.
    pub response_page_token_digest: Option<EvidenceDigest>,
}

/// Exact bounded historical AAPL/IEX page-graph and cardinality evidence.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AlpacaDoctorHistoricalObservation {
    /// Digest of exact AAPL/IEX/1Day/raw/ascending endpoint semantics.
    pub endpoint_contract_digest: EvidenceDigest,
    /// Digest of the exact secret-free bounded date request.
    pub request_digest: EvidenceDigest,
    /// Digest of the complete parsed historical result.
    pub semantic_result_digest: EvidenceDigest,
    /// Inclusive requested start date.
    pub start_date: CalendarDate,
    /// Inclusive requested end date.
    pub end_date: CalendarDate,
    /// Exact number of provider pages.
    pub page_count: u32,
    /// Exact valid returned bar count.
    pub returned_bar_count: u32,
    /// Exact distinct returned UTC date count.
    pub distinct_date_count: u32,
    /// First provider-authored bar time, when a bar was returned.
    pub first_bar_timestamp: Option<Timestamp>,
    /// Last provider-authored bar time, when a bar was returned.
    pub last_bar_timestamp: Option<Timestamp>,
    /// Canonical digest of exact returned dates.
    pub returned_dates_digest: EvidenceDigest,
    /// Canonical digest of exact page requests, responses, and next-token edges.
    pub pagination_graph_digest: EvidenceDigest,
    /// Whether the retained last provider page omitted a next token.
    pub terminal_page_observed: bool,
    /// Exact per-page HTTP evidence in request order.
    pub pages: Box<[AlpacaDoctorHistoricalPageEvidence]>,
}

/// Exact read-only Paper calendar reconciliation evidence for historical dates.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AlpacaDoctorCalendarObservation {
    /// Exact HTTP boundary evidence for the fixed Paper `/v3/calendar/IEX` route.
    pub http: AlpacaDoctorHttpEvidence,
    /// Digest of the complete parsed and reconciled calendar result.
    pub semantic_result_digest: EvidenceDigest,
    /// Inclusive requested start date.
    pub start_date: CalendarDate,
    /// Inclusive requested end date.
    pub end_date: CalendarDate,
    /// Exact returned IEX session count.
    pub session_count: u32,
    /// Exact distinct historical date count supplied for reconciliation.
    pub history_date_count: u32,
    /// Exact matched date count.
    pub matched_count: u32,
    /// Historical dates absent from the calendar response.
    pub missing_history_count: u32,
    /// Calendar dates outside the historical result.
    pub unexpected_history_count: u32,
    /// Canonical digest of exact session dates.
    pub session_dates_digest: EvidenceDigest,
    /// Canonical digest of exact historical dates.
    pub history_dates_digest: EvidenceDigest,
    /// Whether exact date reconciliation completed without missing or unexpected dates.
    pub exact_date_reconciliation: bool,
}

/// Capability families intentionally outside the five mandatory Paper/IEX probes.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AlpacaDoctorAdditionalCapability {
    /// Options REST data.
    OptionsRest,
    /// Options streaming data.
    OptionsStream,
    /// Fixed-income data.
    FixedIncome,
    /// Corporate-action data.
    CorporateActions,
    /// Consolidated SIP delivery.
    Sip,
    /// Consolidated NBBO.
    Nbbo,
    /// OPRA delivery.
    Opra,
    /// Price-level depth.
    PriceLevelDepth,
    /// Order-level depth.
    OrderLevelDepth,
    /// Brokerage-account routes.
    BrokerageAccount,
    /// Position routes.
    Positions,
    /// Order routes.
    Orders,
    /// Trading authority.
    Trading,
}

/// One explicit disposition for a capability outside the five mandatory probes.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AlpacaDoctorCapabilityEvidence {
    /// Exact closed capability family.
    pub capability: AlpacaDoctorAdditionalCapability,
    /// `NotProbed` for deferred data families; `Unavailable` for unsupported authority.
    pub disposition: RuntimeCapabilityDisposition,
    /// Digest of the exact code-owned omission or unsupported-capability decision.
    pub disposition_evidence_digest: EvidenceDigest,
}

const REQUIRED_ADDITIONAL_CAPABILITIES: [(
    AlpacaDoctorAdditionalCapability,
    RuntimeCapabilityDisposition,
); 13] = [
    (
        AlpacaDoctorAdditionalCapability::OptionsRest,
        RuntimeCapabilityDisposition::NotProbed,
    ),
    (
        AlpacaDoctorAdditionalCapability::OptionsStream,
        RuntimeCapabilityDisposition::NotProbed,
    ),
    (
        AlpacaDoctorAdditionalCapability::FixedIncome,
        RuntimeCapabilityDisposition::NotProbed,
    ),
    (
        AlpacaDoctorAdditionalCapability::CorporateActions,
        RuntimeCapabilityDisposition::NotProbed,
    ),
    (
        AlpacaDoctorAdditionalCapability::Sip,
        RuntimeCapabilityDisposition::Unavailable,
    ),
    (
        AlpacaDoctorAdditionalCapability::Nbbo,
        RuntimeCapabilityDisposition::Unavailable,
    ),
    (
        AlpacaDoctorAdditionalCapability::Opra,
        RuntimeCapabilityDisposition::Unavailable,
    ),
    (
        AlpacaDoctorAdditionalCapability::PriceLevelDepth,
        RuntimeCapabilityDisposition::Unavailable,
    ),
    (
        AlpacaDoctorAdditionalCapability::OrderLevelDepth,
        RuntimeCapabilityDisposition::Unavailable,
    ),
    (
        AlpacaDoctorAdditionalCapability::BrokerageAccount,
        RuntimeCapabilityDisposition::Unavailable,
    ),
    (
        AlpacaDoctorAdditionalCapability::Positions,
        RuntimeCapabilityDisposition::Unavailable,
    ),
    (
        AlpacaDoctorAdditionalCapability::Orders,
        RuntimeCapabilityDisposition::Unavailable,
    ),
    (
        AlpacaDoctorAdditionalCapability::Trading,
        RuntimeCapabilityDisposition::Unavailable,
    ),
];

/// Fixed closed input to one version-1 Alpaca Paper/IEX doctor receipt.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AlpacaPaperIexDoctorReceiptInput {
    /// Exact source-owned provider-observation origin; fixture observations use a different
    /// adapter type and digest domain and are never admitted here.
    pub provider_observation_origin: SourceIdentifier,
    /// SHA-256 of the complete provider-observed adapter result, including origin and all probes.
    pub provider_observation_sha256: EvidenceDigest,
    /// Exact provider surface.
    pub surface_id: SourceIdentifier,
    /// Canonical lowercase-hyphenated UUID text of the owning onboarding session.
    pub session_identifier: SourceIdentifier,
    /// Exact secret generation used for every authenticated probe.
    pub generation: SecretGeneration,
    /// Exact credential issuance realm; version 1 admits only Paper.
    pub realm: AlpacaDoctorCredentialRealm,
    /// SHA-256 of key identifier plus Paper realm; explicitly not a brokerage account ID.
    pub market_data_principal_sha256: EvidenceDigest,
    /// Exact immutable capability revision.
    pub capability_revision: ProviderCapabilityRevision,
    /// Canonical capability digest.
    pub capability_digest: EvidenceDigest,
    /// Canonical public-configuration digest.
    pub public_configuration_digest: EvidenceDigest,
    /// Exact admitted rights-decision digest.
    pub rights_decision_digest: EvidenceDigest,
    /// Exact shared Paper-account rate-policy digest.
    pub rate_policy_digest: EvidenceDigest,
    /// Maximum quality admitted by this receipt; version 1 requires DirectUnverified.
    pub data_quality: DataQuality,
    /// One-symbol latest AAPL/IEX quote evidence.
    pub quote: AlpacaDoctorProbeEvidence<AlpacaDoctorQuoteObservation>,
    /// Exact 50-symbol IEX snapshot-batch evidence.
    pub batch: AlpacaDoctorProbeEvidence<AlpacaDoctorBatchObservation>,
    /// Exact bounded IEX stream auth/subscription/close evidence.
    pub stream: AlpacaDoctorProbeEvidence<AlpacaDoctorStreamObservation>,
    /// Exact bounded AAPL/IEX historical page-graph evidence.
    pub historical: AlpacaDoctorProbeEvidence<AlpacaDoctorHistoricalObservation>,
    /// Exact read-only Paper IEX calendar reconciliation evidence.
    pub calendar: AlpacaDoctorProbeEvidence<AlpacaDoctorCalendarObservation>,
    /// Exact closed omissions and unsupported capability boundaries, in canonical order.
    pub additional_capabilities: Box<[AlpacaDoctorCapabilityEvidence]>,
    /// Trusted time after every included probe and validation completed.
    pub verified_at: Timestamp,
    /// Exclusive application-owned currentness deadline.
    pub exclusive_expires_at: Timestamp,
    /// Exact prior receipt self-digest for same-generation renewal only.
    pub predecessor_digest: Option<EvidenceDigest>,
}

/// Self-digested, secret-free Alpaca Paper/IEX doctor receipt.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AlpacaPaperIexDoctorReceiptV1 {
    schema: SourceIdentifier,
    doctor_revision: SourceIdentifier,
    doctor_contract_digest: EvidenceDigest,
    input: AlpacaPaperIexDoctorReceiptInput,
    receipt_sha256: EvidenceDigest,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AlpacaPaperIexDoctorReceiptWire {
    schema: SourceIdentifier,
    doctor_revision: SourceIdentifier,
    doctor_contract_digest: EvidenceDigest,
    input: AlpacaPaperIexDoctorReceiptInput,
    receipt_sha256: EvidenceDigest,
}

impl AlpacaPaperIexDoctorReceiptV1 {
    /// Exact exclusive validity interval source-owned by this receipt revision.
    pub const VALIDITY_NANOS: i64 = ALPACA_PAPER_IEX_DOCTOR_VALIDITY_NANOS;

    /// Returns the only provider-observation origin admitted by this receipt revision.
    pub fn provider_observed_origin() -> Result<SourceIdentifier, RuntimeVerificationEvidenceError>
    {
        SourceIdentifier::try_from(ALPACA_PAPER_IEX_PROVIDER_OBSERVATION_ORIGIN)
            .map_err(|_| RuntimeVerificationEvidenceError::InvalidEvidence)
    }

    /// Validates exact receipt semantics and computes its content self-digest.
    pub fn try_new(
        input: AlpacaPaperIexDoctorReceiptInput,
    ) -> Result<Self, RuntimeVerificationEvidenceError> {
        validate_alpaca_receipt_input(&input)?;
        let schema = SourceIdentifier::try_from(ALPACA_PAPER_IEX_DOCTOR_RECEIPT_SCHEMA)
            .map_err(|_| RuntimeVerificationEvidenceError::InvalidEvidence)?;
        let doctor_revision = source_owned_doctor_revision()?;
        let doctor_contract_digest = source_owned_doctor_contract_digest(
            input.capability_revision,
            input.capability_digest,
            &doctor_revision,
        );
        let receipt_sha256 =
            receipt_content_digest(&schema, &doctor_revision, doctor_contract_digest, &input)?;
        let receipt = Self {
            schema,
            doctor_revision,
            doctor_contract_digest,
            input,
            receipt_sha256,
        };
        receipt.revalidate()?;
        Ok(receipt)
    }

    /// Returns the stable schema identity.
    pub const fn schema(&self) -> &SourceIdentifier {
        &self.schema
    }

    /// Returns the exact provider surface.
    pub const fn surface_id(&self) -> &SourceIdentifier {
        &self.input.surface_id
    }

    /// Returns the canonical onboarding-session UUID text.
    pub const fn session_identifier(&self) -> &SourceIdentifier {
        &self.input.session_identifier
    }

    /// Returns the exact credential generation.
    pub const fn generation(&self) -> SecretGeneration {
        self.input.generation
    }

    /// Returns the exact credential issuance realm.
    pub const fn realm(&self) -> AlpacaDoctorCredentialRealm {
        self.input.realm
    }

    /// Returns the non-brokerage market-data credential principal digest.
    pub const fn market_data_principal_sha256(&self) -> EvidenceDigest {
        self.input.market_data_principal_sha256
    }

    /// Returns the complete provider-observation digest retained by this receipt.
    pub const fn provider_observation_sha256(&self) -> EvidenceDigest {
        self.input.provider_observation_sha256
    }

    /// Returns the exact capability revision.
    pub const fn capability_revision(&self) -> ProviderCapabilityRevision {
        self.input.capability_revision
    }

    /// Returns the canonical capability digest.
    pub const fn capability_digest(&self) -> EvidenceDigest {
        self.input.capability_digest
    }

    /// Returns the bound public-configuration digest.
    pub const fn public_configuration_digest(&self) -> EvidenceDigest {
        self.input.public_configuration_digest
    }

    /// Returns the bound rights-decision digest.
    pub const fn rights_decision_digest(&self) -> EvidenceDigest {
        self.input.rights_decision_digest
    }

    /// Returns the bound rate-policy digest.
    pub const fn rate_policy_digest(&self) -> EvidenceDigest {
        self.input.rate_policy_digest
    }

    /// Returns the source-owned doctor implementation revision.
    pub const fn doctor_revision(&self) -> &SourceIdentifier {
        &self.doctor_revision
    }

    /// Returns the exact code-owned doctor contract digest.
    pub const fn doctor_contract_digest(&self) -> EvidenceDigest {
        self.doctor_contract_digest
    }

    /// Returns the complete validated secret-free receipt input.
    pub const fn input(&self) -> &AlpacaPaperIexDoctorReceiptInput {
        &self.input
    }

    /// Returns the doctor verification time.
    pub const fn verified_at(&self) -> Timestamp {
        self.input.verified_at
    }

    /// Returns the exclusive doctor currentness deadline.
    pub const fn exclusive_expires_at(&self) -> Timestamp {
        self.input.exclusive_expires_at
    }

    /// Returns the exact predecessor self-digest for renewal, when present.
    pub const fn predecessor_digest(&self) -> Option<EvidenceDigest> {
        self.input.predecessor_digest
    }

    /// Returns the receipt's SHA-256 self-digest.
    pub const fn receipt_sha256(&self) -> EvidenceDigest {
        self.receipt_sha256
    }

    /// Returns exact canonical JSON for durable receipt transport and collision checks.
    pub fn canonical_json(&self) -> Result<Vec<u8>, RuntimeVerificationEvidenceError> {
        let bytes = serde_json::to_vec(self)
            .map_err(|_| RuntimeVerificationEvidenceError::Serialization)?;
        if bytes.is_empty() || bytes.len() > MAX_ALPACA_PAPER_IEX_DOCTOR_RECEIPT_BYTES {
            return Err(RuntimeVerificationEvidenceError::ResourceLimit);
        }
        Ok(bytes)
    }

    /// Returns whether this receipt is current at the supplied trusted application time.
    pub fn is_current_at(&self, observed_at: Timestamp) -> bool {
        self.input.verified_at <= observed_at && observed_at < self.input.exclusive_expires_at
    }

    /// Returns whether all five required Paper/IEX capabilities are exactly available.
    pub const fn admits_source_start(&self) -> bool {
        matches!(
            self.input.quote.disposition,
            RuntimeCapabilityDisposition::Available
        ) && matches!(
            self.input.batch.disposition,
            RuntimeCapabilityDisposition::Available
        ) && matches!(
            self.input.stream.disposition,
            RuntimeCapabilityDisposition::Available
        ) && matches!(
            self.input.historical.disposition,
            RuntimeCapabilityDisposition::Available
        ) && matches!(
            self.input.calendar.disposition,
            RuntimeCapabilityDisposition::Available
        )
    }

    pub(crate) fn revalidate(&self) -> Result<(), RuntimeVerificationEvidenceError> {
        if self.schema.as_str() != ALPACA_PAPER_IEX_DOCTOR_RECEIPT_SCHEMA {
            return Err(RuntimeVerificationEvidenceError::InvalidEvidence);
        }
        validate_alpaca_receipt_input(&self.input)?;
        let expected_revision = source_owned_doctor_revision()?;
        let expected_contract_digest = source_owned_doctor_contract_digest(
            self.input.capability_revision,
            self.input.capability_digest,
            &expected_revision,
        );
        if self.doctor_revision != expected_revision
            || self.doctor_contract_digest != expected_contract_digest
            || receipt_content_digest(
                &self.schema,
                &self.doctor_revision,
                self.doctor_contract_digest,
                &self.input,
            )? != self.receipt_sha256
            || self.input.predecessor_digest == Some(self.receipt_sha256)
        {
            return Err(RuntimeVerificationEvidenceError::InvalidEvidence);
        }
        let _ = self.canonical_json()?;
        Ok(())
    }

    pub(crate) fn same_authority_as(&self, prior: &Self) -> bool {
        self.input.provider_observation_origin == prior.input.provider_observation_origin
            && self.input.surface_id == prior.input.surface_id
            && self.input.session_identifier == prior.input.session_identifier
            && self.input.generation == prior.input.generation
            && self.input.realm == prior.input.realm
            && self.input.market_data_principal_sha256 == prior.input.market_data_principal_sha256
            && self.input.capability_revision == prior.input.capability_revision
            && self.input.capability_digest == prior.input.capability_digest
            && self.input.public_configuration_digest == prior.input.public_configuration_digest
            && self.input.rights_decision_digest == prior.input.rights_decision_digest
            && self.input.rate_policy_digest == prior.input.rate_policy_digest
            && self.doctor_revision == prior.doctor_revision
            && self.doctor_contract_digest == prior.doctor_contract_digest
            && self.input.data_quality == prior.input.data_quality
            && self.input.additional_capabilities == prior.input.additional_capabilities
    }
}

impl<'de> Deserialize<'de> for AlpacaPaperIexDoctorReceiptV1 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = AlpacaPaperIexDoctorReceiptWire::deserialize(deserializer)?;
        let receipt = Self {
            schema: wire.schema,
            doctor_revision: wire.doctor_revision,
            doctor_contract_digest: wire.doctor_contract_digest,
            input: wire.input,
            receipt_sha256: wire.receipt_sha256,
        };
        receipt.revalidate().map_err(serde::de::Error::custom)?;
        Ok(receipt)
    }
}

/// Legacy digest-only runtime evidence retained without semantic drift.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeVerificationDigestV1 {
    evidence_digest: EvidenceDigest,
}

impl RuntimeVerificationDigestV1 {
    /// Wraps one existing nonzero runtime evidence digest.
    pub fn try_new(
        evidence_digest: EvidenceDigest,
    ) -> Result<Self, RuntimeVerificationEvidenceError> {
        require_nonzero(evidence_digest)?;
        Ok(Self { evidence_digest })
    }

    /// Returns the unchanged legacy semantic digest.
    pub const fn evidence_digest(self) -> EvidenceDigest {
        self.evidence_digest
    }
}

/// Closed runtime evidence retained by a `RuntimeVerified` onboarding event.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "evidence", rename_all = "snake_case")]
pub enum RuntimeVerificationEvidence {
    /// Existing non-Alpaca digest-only evidence.
    DigestV1(RuntimeVerificationDigestV1),
    /// Full, self-digested Alpaca Paper/IEX doctor receipt.
    AlpacaPaperIexDoctorReceiptV1(Box<AlpacaPaperIexDoctorReceiptV1>),
}

impl RuntimeVerificationEvidence {
    /// Preserves an existing non-Alpaca runtime digest.
    pub fn digest_v1(
        evidence_digest: EvidenceDigest,
    ) -> Result<Self, RuntimeVerificationEvidenceError> {
        RuntimeVerificationDigestV1::try_new(evidence_digest).map(Self::DigestV1)
    }

    /// Returns the semantic digest used by existing runtime lease bindings.
    pub const fn evidence_digest(&self) -> EvidenceDigest {
        match self {
            Self::DigestV1(evidence) => evidence.evidence_digest(),
            Self::AlpacaPaperIexDoctorReceiptV1(receipt) => receipt.receipt_sha256(),
        }
    }

    /// Returns the exact Alpaca receipt when this is the typed Paper/IEX evidence variant.
    pub fn alpaca_paper_iex_receipt(&self) -> Option<&AlpacaPaperIexDoctorReceiptV1> {
        match self {
            Self::AlpacaPaperIexDoctorReceiptV1(receipt) => Some(receipt),
            Self::DigestV1(_) => None,
        }
    }

    /// Returns whether the evidence remains current at a trusted application time.
    pub fn is_current_at(&self, observed_at: Timestamp) -> bool {
        match self {
            Self::DigestV1(_) => true,
            Self::AlpacaPaperIexDoctorReceiptV1(receipt) => receipt.is_current_at(observed_at),
        }
    }

    /// Returns whether the evidence can admit initial activation or active renewal.
    pub fn admits_activation_at(&self, observed_at: Timestamp) -> bool {
        self.is_current_at(observed_at) && self.is_activation_ready()
    }

    /// Returns whether the closed evidence has every required activation disposition.
    pub const fn is_activation_ready(&self) -> bool {
        match self {
            Self::DigestV1(_) => true,
            Self::AlpacaPaperIexDoctorReceiptV1(receipt) => receipt.admits_source_start(),
        }
    }

    pub(crate) fn revalidate(&self) -> Result<(), RuntimeVerificationEvidenceError> {
        match self {
            Self::DigestV1(evidence) => require_nonzero(evidence.evidence_digest()),
            Self::AlpacaPaperIexDoctorReceiptV1(receipt) => receipt.revalidate(),
        }
    }
}

/// Runtime-verification evidence validation failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum RuntimeVerificationEvidenceError {
    /// Evidence was empty, contradictory, inexact, or outside the closed contract.
    #[error("runtime verification evidence is invalid")]
    InvalidEvidence,
    /// Canonical retained evidence exceeded its fixed bound.
    #[error("runtime verification evidence exceeds its resource bound")]
    ResourceLimit,
    /// Canonical receipt serialization failed.
    #[error("runtime verification evidence serialization failed")]
    Serialization,
}

#[derive(Serialize)]
struct ReceiptDigestMaterial<'a> {
    schema: &'a SourceIdentifier,
    doctor_revision: &'a SourceIdentifier,
    doctor_contract_digest: EvidenceDigest,
    input: &'a AlpacaPaperIexDoctorReceiptInput,
}

fn receipt_content_digest(
    schema: &SourceIdentifier,
    doctor_revision: &SourceIdentifier,
    doctor_contract_digest: EvidenceDigest,
    input: &AlpacaPaperIexDoctorReceiptInput,
) -> Result<EvidenceDigest, RuntimeVerificationEvidenceError> {
    let bytes = serde_json::to_vec(&ReceiptDigestMaterial {
        schema,
        doctor_revision,
        doctor_contract_digest,
        input,
    })
    .map_err(|_| RuntimeVerificationEvidenceError::Serialization)?;
    if bytes.is_empty() || bytes.len() > MAX_ALPACA_PAPER_IEX_DOCTOR_RECEIPT_BYTES {
        return Err(RuntimeVerificationEvidenceError::ResourceLimit);
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        Sha256::digest(bytes).into(),
    ))
}

fn validate_alpaca_receipt_input(
    input: &AlpacaPaperIexDoctorReceiptInput,
) -> Result<(), RuntimeVerificationEvidenceError> {
    let expected_expiry = input
        .verified_at
        .unix_nanos()
        .checked_add(ALPACA_PAPER_IEX_DOCTOR_VALIDITY_NANOS)
        .map(Timestamp::from_unix_nanos)
        .ok_or(RuntimeVerificationEvidenceError::InvalidEvidence)?;
    if input.provider_observation_origin.as_str() != ALPACA_PAPER_IEX_PROVIDER_OBSERVATION_ORIGIN
        || input.surface_id.as_str() != ALPACA_BASIC_MARKET_DATA_SURFACE_ID
        || !canonical_uuid_text(input.session_identifier.as_str())
        || input.data_quality != DataQuality::DirectUnverified
        || input.exclusive_expires_at != expected_expiry
        || input.additional_capabilities.len() != REQUIRED_ADDITIONAL_CAPABILITIES.len()
    {
        return Err(RuntimeVerificationEvidenceError::InvalidEvidence);
    }
    for (evidence, (capability, disposition)) in input
        .additional_capabilities
        .iter()
        .zip(REQUIRED_ADDITIONAL_CAPABILITIES)
    {
        if evidence.capability != capability || evidence.disposition != disposition {
            return Err(RuntimeVerificationEvidenceError::InvalidEvidence);
        }
        require_sha256(evidence.disposition_evidence_digest)?;
    }
    for digest in [
        input.provider_observation_sha256,
        input.market_data_principal_sha256,
        input.capability_digest,
        input.public_configuration_digest,
        input.rights_decision_digest,
        input.rate_policy_digest,
    ] {
        require_sha256(digest)?;
    }
    if let Some(predecessor) = input.predecessor_digest {
        require_sha256(predecessor)?;
    }
    input.quote.validate(validate_quote)?;
    input.batch.validate(validate_batch)?;
    input.stream.validate(validate_stream)?;
    input.historical.validate(validate_history)?;
    input.calendar.validate(validate_calendar)?;
    validate_history_calendar_binding(input)?;
    validate_observation_times(input)?;
    validate_code_owned_alpaca_contract(input)?;
    if provider_observation_digest(input)? != input.provider_observation_sha256 {
        return Err(RuntimeVerificationEvidenceError::InvalidEvidence);
    }
    Ok(())
}

fn source_owned_doctor_revision() -> Result<SourceIdentifier, RuntimeVerificationEvidenceError> {
    SourceIdentifier::try_from(ALPACA_PAPER_IEX_DOCTOR_IMPLEMENTATION_REVISION)
        .map_err(|_| RuntimeVerificationEvidenceError::InvalidEvidence)
}

fn source_owned_doctor_contract_digest(
    capability_revision: ProviderCapabilityRevision,
    capability_digest: EvidenceDigest,
    doctor_revision: &SourceIdentifier,
) -> EvidenceDigest {
    let mut hasher = Sha256::new();
    hasher.update(ALPACA_PAPER_IEX_DOCTOR_CONTRACT_DOMAIN);
    hasher.update(capability_revision.get().to_be_bytes());
    hasher.update(capability_digest.bytes());
    hasher.update(doctor_revision.as_str().as_bytes());
    EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into())
}

#[derive(Clone, Copy)]
enum AlpacaDoctorEndpointContract {
    Quote,
    Batch,
    Stream,
    Historical,
    Calendar,
}

fn validate_code_owned_alpaca_contract(
    input: &AlpacaPaperIexDoctorReceiptInput,
) -> Result<(), RuntimeVerificationEvidenceError> {
    let quote = required_observation(&input.quote)?;
    let batch = required_observation(&input.batch)?;
    let stream = required_observation(&input.stream)?;
    let historical = required_observation(&input.historical)?;
    let calendar = required_observation(&input.calendar)?;

    let quote_endpoint = alpaca_endpoint_contract_digest(AlpacaDoctorEndpointContract::Quote)?;
    let batch_endpoint = alpaca_endpoint_contract_digest(AlpacaDoctorEndpointContract::Batch)?;
    let stream_endpoint = alpaca_endpoint_contract_digest(AlpacaDoctorEndpointContract::Stream)?;
    let historical_endpoint =
        alpaca_endpoint_contract_digest(AlpacaDoctorEndpointContract::Historical)?;
    let calendar_endpoint =
        alpaca_endpoint_contract_digest(AlpacaDoctorEndpointContract::Calendar)?;
    let batch_url = alpaca_batch_url()?;
    let first_history_url = alpaca_history_url(historical.start_date, historical.end_date)?;
    let calendar_url = alpaca_calendar_url(calendar.start_date, calendar.end_date);

    if quote.http.endpoint_contract_digest != quote_endpoint
        || quote.http.request_digest
            != alpaca_http_request_digest("GET", ALPACA_DOCTOR_QUOTE_ENDPOINT)?
        || input.quote.disposition_evidence_digest
            != alpaca_quote_semantic_digest(input.quote.disposition, quote)?
        || quote.semantic_result_digest != input.quote.disposition_evidence_digest
        || batch.http.endpoint_contract_digest != batch_endpoint
        || batch.http.request_digest != alpaca_http_request_digest("GET", batch_url.as_str())?
        || input.batch.disposition_evidence_digest
            != alpaca_batch_semantic_digest(input.batch.disposition, batch)
        || batch.semantic_result_digest != input.batch.disposition_evidence_digest
        || stream.endpoint_contract_digest != stream_endpoint
        || stream.request_digest != alpaca_stream_request_digest()?
        || input.stream.disposition_evidence_digest
            != alpaca_stream_semantic_digest(input.stream.disposition, stream)
        || stream.semantic_result_digest != input.stream.disposition_evidence_digest
        || historical.endpoint_contract_digest != historical_endpoint
        || historical.request_digest
            != alpaca_historical_request_digest(historical.start_date, historical.end_date)?
        || input.historical.disposition_evidence_digest
            != alpaca_historical_semantic_digest(input.historical.disposition, historical)
        || historical.semantic_result_digest != input.historical.disposition_evidence_digest
        || calendar.http.endpoint_contract_digest != calendar_endpoint
        || calendar.http.request_digest != alpaca_http_request_digest("GET", &calendar_url)?
        || input.calendar.disposition_evidence_digest
            != alpaca_calendar_semantic_digest(input.calendar.disposition, calendar)
        || calendar.semantic_result_digest != input.calendar.disposition_evidence_digest
    {
        return Err(RuntimeVerificationEvidenceError::InvalidEvidence);
    }

    let requested_set_digest =
        alpaca_symbol_set_digest(ALPACA_DOCTOR_BATCH_SYMBOLS.iter().copied())?;
    let empty_set_digest = alpaca_symbol_set_digest(std::iter::empty())?;
    if batch.requested_set_digest != requested_set_digest
        || batch.returned_set_digest != requested_set_digest
        || batch.missing_set_digest != empty_set_digest
        || batch.unexpected_set_digest != empty_set_digest
        || historical.pagination_graph_digest
            != alpaca_pagination_graph_digest(&historical.pages, historical.terminal_page_observed)?
        || historical.pages[0].http.endpoint_contract_digest != historical_endpoint
        || historical.pages[0].http.request_digest
            != alpaca_http_request_digest("GET", first_history_url.as_str())?
        || historical
            .pages
            .iter()
            .any(|page| page.http.endpoint_contract_digest != historical_endpoint)
    {
        return Err(RuntimeVerificationEvidenceError::InvalidEvidence);
    }

    for evidence in input.additional_capabilities.iter() {
        if evidence.disposition_evidence_digest
            != alpaca_additional_capability_digest(evidence.capability, evidence.disposition)
        {
            return Err(RuntimeVerificationEvidenceError::InvalidEvidence);
        }
    }
    Ok(())
}

fn required_observation<T>(
    probe: &AlpacaDoctorProbeEvidence<T>,
) -> Result<&T, RuntimeVerificationEvidenceError> {
    probe
        .observation
        .as_ref()
        .ok_or(RuntimeVerificationEvidenceError::InvalidEvidence)
}

fn alpaca_endpoint_contract_digest(
    contract: AlpacaDoctorEndpointContract,
) -> Result<EvidenceDigest, RuntimeVerificationEvidenceError> {
    let contract = match contract {
        AlpacaDoctorEndpointContract::Quote => {
            "GET https://data.alpaca.markets/v2/stocks/AAPL/quotes/latest?feed=iex"
        }
        AlpacaDoctorEndpointContract::Batch => {
            "GET https://data.alpaca.markets/v2/stocks/snapshots?symbols=<code-owned-50>&feed=iex"
        }
        AlpacaDoctorEndpointContract::Stream => {
            "WSS wss://stream.data.alpaca.markets/v2/iex;header-auth;subscribe=AAPL-trades+quotes;close"
        }
        AlpacaDoctorEndpointContract::Historical => {
            "GET https://data.alpaca.markets/v2/stocks/AAPL/bars?timeframe=1Day&start=<utc>&end=<utc>&limit=1000&adjustment=raw&feed=iex&sort=asc&page_token=<optional>"
        }
        AlpacaDoctorEndpointContract::Calendar => {
            "GET https://paper-api.alpaca.markets/v3/calendar/IEX?start=<date>&end=<date>&timezone=UTC"
        }
    };
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-paper-iex-doctor-endpoint-contract/v1\0");
    alpaca_hash_text(&mut digest, contract)?;
    Ok(alpaca_sha256(digest))
}

fn alpaca_http_request_digest(
    method: &str,
    url: &str,
) -> Result<EvidenceDigest, RuntimeVerificationEvidenceError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-paper-iex-doctor-http-request/v1\0");
    alpaca_hash_text(&mut digest, method)?;
    alpaca_hash_text(&mut digest, url)?;
    Ok(alpaca_sha256(digest))
}

fn alpaca_stream_request_digest() -> Result<EvidenceDigest, RuntimeVerificationEvidenceError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-paper-iex-doctor-stream-request/v1\0");
    alpaca_hash_text(&mut digest, ALPACA_DOCTOR_STREAM_ENDPOINT)?;
    alpaca_hash_text(&mut digest, "header-auth")?;
    alpaca_hash_text(&mut digest, ALPACA_DOCTOR_STREAM_SUBSCRIPTION)?;
    Ok(alpaca_sha256(digest))
}

fn alpaca_historical_request_digest(
    start_date: CalendarDate,
    end_date: CalendarDate,
) -> Result<EvidenceDigest, RuntimeVerificationEvidenceError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-paper-iex-doctor-historical-request/v1\0");
    alpaca_hash_evidence(
        &mut digest,
        alpaca_endpoint_contract_digest(AlpacaDoctorEndpointContract::Historical)?,
    );
    alpaca_hash_date(&mut digest, start_date);
    alpaca_hash_date(&mut digest, end_date);
    Ok(alpaca_sha256(digest))
}

fn alpaca_batch_url() -> Result<Url, RuntimeVerificationEvidenceError> {
    let mut url = Url::parse(ALPACA_DOCTOR_BATCH_ENDPOINT)
        .map_err(|_| RuntimeVerificationEvidenceError::InvalidEvidence)?;
    url.query_pairs_mut()
        .append_pair("symbols", &ALPACA_DOCTOR_BATCH_SYMBOLS.join(","))
        .append_pair("feed", "iex");
    Ok(url)
}

fn alpaca_history_url(
    start_date: CalendarDate,
    end_date: CalendarDate,
) -> Result<Url, RuntimeVerificationEvidenceError> {
    let mut url = Url::parse(ALPACA_DOCTOR_HISTORY_ENDPOINT)
        .map_err(|_| RuntimeVerificationEvidenceError::InvalidEvidence)?;
    url.query_pairs_mut()
        .append_pair("timeframe", "1Day")
        .append_pair("start", &format!("{start_date}T00:00:00Z"))
        .append_pair("end", &format!("{end_date}T23:59:59.999999999Z"))
        .append_pair("limit", "1000")
        .append_pair("adjustment", "raw")
        .append_pair("feed", "iex")
        .append_pair("sort", "asc");
    Ok(url)
}

fn alpaca_calendar_url(start_date: CalendarDate, end_date: CalendarDate) -> String {
    format!(
        "https://paper-api.alpaca.markets/v3/calendar/IEX?start={start_date}&end={end_date}&timezone=UTC"
    )
}

fn alpaca_quote_semantic_digest(
    disposition: RuntimeCapabilityDisposition,
    observation: &AlpacaDoctorQuoteObservation,
) -> Result<EvidenceDigest, RuntimeVerificationEvidenceError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-paper-iex-doctor-quote-result/v1\0");
    digest.update([alpaca_disposition_tag(disposition)]);
    alpaca_hash_optional_timestamp(&mut digest, observation.quote_timestamp);
    alpaca_hash_optional_decimal(&mut digest, observation.bid_price)?;
    alpaca_hash_optional_decimal(&mut digest, observation.ask_price)?;
    alpaca_hash_optional_u64(&mut digest, observation.bid_size);
    alpaca_hash_optional_u64(&mut digest, observation.ask_size);
    Ok(alpaca_sha256(digest))
}

fn alpaca_batch_semantic_digest(
    disposition: RuntimeCapabilityDisposition,
    observation: &AlpacaDoctorBatchObservation,
) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-paper-iex-doctor-batch-result/v1\0");
    digest.update([alpaca_disposition_tag(disposition)]);
    for count in [
        observation.requested_count,
        observation.returned_count,
        observation.missing_count,
        observation.unexpected_count,
        observation.duplicate_count,
        observation.invalid_count,
        observation.effective_cardinality,
    ] {
        digest.update(count.to_be_bytes());
    }
    for evidence in [
        observation.requested_set_digest,
        observation.returned_set_digest,
        observation.missing_set_digest,
        observation.unexpected_set_digest,
    ] {
        alpaca_hash_evidence(&mut digest, evidence);
    }
    alpaca_sha256(digest)
}

fn alpaca_stream_semantic_digest(
    disposition: RuntimeCapabilityDisposition,
    observation: &AlpacaDoctorStreamObservation,
) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-paper-iex-doctor-stream-result/v1\0");
    digest.update([alpaca_disposition_tag(disposition)]);
    digest.update(observation.handshake_status.to_be_bytes());
    alpaca_hash_rate(&mut digest, observation.handshake_rate);
    for evidence in [
        observation.connected_frame_digest,
        observation.authenticated_frame_digest,
        observation.subscription_frame_digest,
    ] {
        alpaca_hash_evidence(&mut digest, evidence);
    }
    digest.update(observation.subscribed_trade_count.to_be_bytes());
    digest.update(observation.subscribed_quote_count.to_be_bytes());
    digest.update(observation.frames_observed.to_be_bytes());
    digest.update(observation.bytes_observed.to_be_bytes());
    digest.update(observation.authenticated_at.unix_nanos().to_be_bytes());
    digest.update(observation.subscribed_at.unix_nanos().to_be_bytes());
    digest.update([
        u8::from(observation.close_sent),
        u8::from(observation.clean_close_observed),
    ]);
    digest.update(observation.completed_at.unix_nanos().to_be_bytes());
    alpaca_sha256(digest)
}

fn alpaca_historical_semantic_digest(
    disposition: RuntimeCapabilityDisposition,
    observation: &AlpacaDoctorHistoricalObservation,
) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-paper-iex-doctor-history-result/v1\0");
    digest.update([alpaca_disposition_tag(disposition)]);
    alpaca_hash_date(&mut digest, observation.start_date);
    alpaca_hash_date(&mut digest, observation.end_date);
    digest.update(observation.page_count.to_be_bytes());
    digest.update(observation.returned_bar_count.to_be_bytes());
    digest.update(observation.distinct_date_count.to_be_bytes());
    alpaca_hash_optional_timestamp(&mut digest, observation.first_bar_timestamp);
    alpaca_hash_optional_timestamp(&mut digest, observation.last_bar_timestamp);
    alpaca_hash_evidence(&mut digest, observation.returned_dates_digest);
    alpaca_hash_evidence(&mut digest, observation.pagination_graph_digest);
    digest.update([u8::from(observation.terminal_page_observed)]);
    alpaca_sha256(digest)
}

fn alpaca_calendar_semantic_digest(
    disposition: RuntimeCapabilityDisposition,
    observation: &AlpacaDoctorCalendarObservation,
) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-paper-iex-doctor-calendar-result/v1\0");
    digest.update([alpaca_disposition_tag(disposition)]);
    alpaca_hash_date(&mut digest, observation.start_date);
    alpaca_hash_date(&mut digest, observation.end_date);
    for count in [
        observation.session_count,
        observation.history_date_count,
        observation.matched_count,
        observation.missing_history_count,
        observation.unexpected_history_count,
    ] {
        digest.update(count.to_be_bytes());
    }
    alpaca_hash_evidence(&mut digest, observation.session_dates_digest);
    alpaca_hash_evidence(&mut digest, observation.history_dates_digest);
    digest.update([u8::from(observation.exact_date_reconciliation)]);
    alpaca_sha256(digest)
}

fn alpaca_pagination_graph_digest(
    pages: &[AlpacaDoctorHistoricalPageEvidence],
    terminal: bool,
) -> Result<EvidenceDigest, RuntimeVerificationEvidenceError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-paper-iex-doctor-pagination-graph/v1\0");
    digest.update([u8::from(terminal)]);
    digest.update(
        u32::try_from(pages.len())
            .map_err(|_| RuntimeVerificationEvidenceError::ResourceLimit)?
            .to_be_bytes(),
    );
    for page in pages {
        alpaca_hash_http(&mut digest, &page.http);
        alpaca_hash_optional_evidence(&mut digest, page.request_page_token_digest);
        alpaca_hash_optional_evidence(&mut digest, page.response_page_token_digest);
    }
    Ok(alpaca_sha256(digest))
}

fn alpaca_symbol_set_digest<'a>(
    symbols: impl Iterator<Item = &'a str>,
) -> Result<EvidenceDigest, RuntimeVerificationEvidenceError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-paper-iex-doctor-symbol-set/v1\0");
    for symbol in symbols {
        alpaca_hash_text(&mut digest, symbol)?;
    }
    Ok(alpaca_sha256(digest))
}

fn alpaca_additional_capability_digest(
    capability: AlpacaDoctorAdditionalCapability,
    disposition: RuntimeCapabilityDisposition,
) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-paper-iex-doctor-additional-capability/v1\0");
    digest.update([alpaca_additional_capability_tag(capability)]);
    digest.update([alpaca_disposition_tag(disposition)]);
    alpaca_sha256(digest)
}

fn provider_observation_digest(
    input: &AlpacaPaperIexDoctorReceiptInput,
) -> Result<EvidenceDigest, RuntimeVerificationEvidenceError> {
    let quote = required_observation(&input.quote)?;
    let batch = required_observation(&input.batch)?;
    let stream = required_observation(&input.stream)?;
    let historical = required_observation(&input.historical)?;
    let calendar = required_observation(&input.calendar)?;
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-paper-iex-doctor-observation/v2\0");
    digest.update([1]);
    digest.update([alpaca_disposition_tag(input.quote.disposition)]);
    alpaca_hash_http(&mut digest, &quote.http);
    alpaca_hash_evidence(&mut digest, quote.semantic_result_digest);
    digest.update([alpaca_disposition_tag(input.batch.disposition)]);
    alpaca_hash_http(&mut digest, &batch.http);
    alpaca_hash_evidence(&mut digest, batch.semantic_result_digest);
    for count in [
        batch.returned_count,
        batch.missing_count,
        batch.unexpected_count,
        batch.duplicate_count,
        batch.invalid_count,
        batch.effective_cardinality,
    ] {
        digest.update(count.to_be_bytes());
    }
    for evidence in [
        batch.requested_set_digest,
        batch.returned_set_digest,
        batch.missing_set_digest,
        batch.unexpected_set_digest,
        stream.endpoint_contract_digest,
        stream.request_digest,
        stream.connected_frame_digest,
        stream.authenticated_frame_digest,
        stream.subscription_frame_digest,
        stream.semantic_result_digest,
        historical.endpoint_contract_digest,
        historical.request_digest,
        historical.semantic_result_digest,
        historical.pagination_graph_digest,
        historical.returned_dates_digest,
        calendar.semantic_result_digest,
        calendar.session_dates_digest,
        calendar.history_dates_digest,
    ] {
        alpaca_hash_evidence(&mut digest, evidence);
    }
    digest.update([alpaca_disposition_tag(input.stream.disposition)]);
    digest.update(stream.handshake_status.to_be_bytes());
    alpaca_hash_rate(&mut digest, stream.handshake_rate);
    digest.update(stream.subscribed_trade_count.to_be_bytes());
    digest.update(stream.subscribed_quote_count.to_be_bytes());
    digest.update(stream.frames_observed.to_be_bytes());
    digest.update(stream.bytes_observed.to_be_bytes());
    digest.update(stream.authenticated_at.unix_nanos().to_be_bytes());
    digest.update(stream.subscribed_at.unix_nanos().to_be_bytes());
    digest.update([
        u8::from(stream.close_sent),
        u8::from(stream.clean_close_observed),
    ]);
    digest.update(stream.completed_at.unix_nanos().to_be_bytes());
    digest.update([alpaca_disposition_tag(input.historical.disposition)]);
    alpaca_hash_date(&mut digest, historical.start_date);
    alpaca_hash_date(&mut digest, historical.end_date);
    digest.update(historical.page_count.to_be_bytes());
    digest.update(historical.returned_bar_count.to_be_bytes());
    digest.update(historical.distinct_date_count.to_be_bytes());
    alpaca_hash_optional_timestamp(&mut digest, historical.first_bar_timestamp);
    alpaca_hash_optional_timestamp(&mut digest, historical.last_bar_timestamp);
    digest.update([u8::from(historical.terminal_page_observed)]);
    digest.update([alpaca_disposition_tag(input.calendar.disposition)]);
    alpaca_hash_http(&mut digest, &calendar.http);
    alpaca_hash_date(&mut digest, calendar.start_date);
    alpaca_hash_date(&mut digest, calendar.end_date);
    digest.update(calendar.session_count.to_be_bytes());
    digest.update(calendar.history_date_count.to_be_bytes());
    digest.update(calendar.matched_count.to_be_bytes());
    digest.update(calendar.missing_history_count.to_be_bytes());
    digest.update(calendar.unexpected_history_count.to_be_bytes());
    digest.update([u8::from(calendar.exact_date_reconciliation)]);
    for (capability, _) in REQUIRED_ADDITIONAL_CAPABILITIES {
        digest.update([match capability {
            AlpacaDoctorAdditionalCapability::OptionsRest
            | AlpacaDoctorAdditionalCapability::OptionsStream
            | AlpacaDoctorAdditionalCapability::FixedIncome
            | AlpacaDoctorAdditionalCapability::CorporateActions => 4,
            _ => 5,
        }]);
    }
    digest.update(input.verified_at.unix_nanos().to_be_bytes());
    Ok(alpaca_sha256(digest))
}

fn alpaca_hash_http(digest: &mut Sha256, value: &AlpacaDoctorHttpEvidence) {
    alpaca_hash_evidence(digest, value.endpoint_contract_digest);
    alpaca_hash_evidence(digest, value.request_digest);
    digest.update(value.status_code.to_be_bytes());
    alpaca_hash_evidence(digest, value.body_digest);
    digest.update(value.response_bytes.to_be_bytes());
    digest.update(value.received_at.unix_nanos().to_be_bytes());
    digest.update(value.latency_nanos.to_be_bytes());
    alpaca_hash_rate(digest, value.rate);
}

fn alpaca_hash_rate(digest: &mut Sha256, value: AlpacaDoctorRateEvidence) {
    alpaca_hash_observed_u32(digest, value.limit);
    alpaca_hash_observed_u32(digest, value.remaining);
    match value.reset_unix_seconds {
        AlpacaRateLimitField::Observed(reset) => {
            digest.update([1]);
            digest.update(reset.to_be_bytes());
        }
        AlpacaRateLimitField::Missing => digest.update([0]),
    }
    match value.retry_after {
        AlpacaRateLimitField::Observed(AlpacaRetryAfterEvidence::DelaySeconds(seconds)) => {
            digest.update([1]);
            digest.update(seconds.to_be_bytes());
        }
        AlpacaRateLimitField::Observed(AlpacaRetryAfterEvidence::AtUnixSeconds(seconds)) => {
            digest.update([2]);
            digest.update(seconds.to_be_bytes());
        }
        AlpacaRateLimitField::Missing => digest.update([0]),
    }
}

fn alpaca_hash_observed_u32(digest: &mut Sha256, value: AlpacaRateLimitField<u32>) {
    match value {
        AlpacaRateLimitField::Observed(value) => {
            digest.update([1]);
            digest.update(value.to_be_bytes());
        }
        AlpacaRateLimitField::Missing => digest.update([0]),
    }
}

fn alpaca_hash_optional_evidence(digest: &mut Sha256, value: Option<EvidenceDigest>) {
    match value {
        Some(value) => {
            digest.update([1]);
            alpaca_hash_evidence(digest, value);
        }
        None => digest.update([0]),
    }
}

fn alpaca_hash_optional_timestamp(digest: &mut Sha256, value: Option<Timestamp>) {
    match value {
        Some(value) => {
            digest.update([1]);
            digest.update(value.unix_nanos().to_be_bytes());
        }
        None => digest.update([0]),
    }
}

fn alpaca_hash_optional_decimal(
    digest: &mut Sha256,
    value: Option<Decimal>,
) -> Result<(), RuntimeVerificationEvidenceError> {
    match value {
        Some(value) => {
            digest.update([1]);
            alpaca_hash_text(digest, &value.normalize().to_string())?;
        }
        None => digest.update([0]),
    }
    Ok(())
}

fn alpaca_hash_optional_u64(digest: &mut Sha256, value: Option<u64>) {
    match value {
        Some(value) => {
            digest.update([1]);
            digest.update(value.to_be_bytes());
        }
        None => digest.update([0]),
    }
}

fn alpaca_hash_date(digest: &mut Sha256, value: CalendarDate) {
    digest.update(value.year().to_be_bytes());
    digest.update([value.month(), value.day()]);
}

fn alpaca_hash_evidence(digest: &mut Sha256, value: EvidenceDigest) {
    digest.update([match value.algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }]);
    digest.update(value.bytes());
}

fn alpaca_hash_text(
    digest: &mut Sha256,
    value: &str,
) -> Result<(), RuntimeVerificationEvidenceError> {
    digest.update(
        u32::try_from(value.len())
            .map_err(|_| RuntimeVerificationEvidenceError::ResourceLimit)?
            .to_be_bytes(),
    );
    digest.update(value.as_bytes());
    Ok(())
}

const fn alpaca_disposition_tag(value: RuntimeCapabilityDisposition) -> u8 {
    match value {
        RuntimeCapabilityDisposition::Available => 1,
        RuntimeCapabilityDisposition::Degraded => 2,
        RuntimeCapabilityDisposition::Unavailable => 3,
        RuntimeCapabilityDisposition::NotProbed => 4,
    }
}

const fn alpaca_additional_capability_tag(value: AlpacaDoctorAdditionalCapability) -> u8 {
    match value {
        AlpacaDoctorAdditionalCapability::OptionsRest => 1,
        AlpacaDoctorAdditionalCapability::OptionsStream => 2,
        AlpacaDoctorAdditionalCapability::FixedIncome => 3,
        AlpacaDoctorAdditionalCapability::CorporateActions => 4,
        AlpacaDoctorAdditionalCapability::Sip => 5,
        AlpacaDoctorAdditionalCapability::Nbbo => 6,
        AlpacaDoctorAdditionalCapability::Opra => 7,
        AlpacaDoctorAdditionalCapability::PriceLevelDepth => 8,
        AlpacaDoctorAdditionalCapability::OrderLevelDepth => 9,
        AlpacaDoctorAdditionalCapability::BrokerageAccount => 10,
        AlpacaDoctorAdditionalCapability::Positions => 11,
        AlpacaDoctorAdditionalCapability::Orders => 12,
        AlpacaDoctorAdditionalCapability::Trading => 13,
    }
}

fn alpaca_sha256(digest: Sha256) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
}

#[cfg(test)]
pub(super) fn seal_test_alpaca_provider_observation(
    input: &mut AlpacaPaperIexDoctorReceiptInput,
) -> Result<(), RuntimeVerificationEvidenceError> {
    input.provider_observation_origin = AlpacaPaperIexDoctorReceiptV1::provider_observed_origin()?;
    input.exclusive_expires_at = input
        .verified_at
        .unix_nanos()
        .checked_add(ALPACA_PAPER_IEX_DOCTOR_VALIDITY_NANOS)
        .map(Timestamp::from_unix_nanos)
        .ok_or(RuntimeVerificationEvidenceError::InvalidEvidence)?;

    let quote_endpoint = alpaca_endpoint_contract_digest(AlpacaDoctorEndpointContract::Quote)?;
    let batch_endpoint = alpaca_endpoint_contract_digest(AlpacaDoctorEndpointContract::Batch)?;
    let stream_endpoint = alpaca_endpoint_contract_digest(AlpacaDoctorEndpointContract::Stream)?;
    let historical_endpoint =
        alpaca_endpoint_contract_digest(AlpacaDoctorEndpointContract::Historical)?;
    let calendar_endpoint =
        alpaca_endpoint_contract_digest(AlpacaDoctorEndpointContract::Calendar)?;

    let quote = input
        .quote
        .observation
        .as_mut()
        .ok_or(RuntimeVerificationEvidenceError::InvalidEvidence)?;
    quote.http.endpoint_contract_digest = quote_endpoint;
    quote.http.request_digest = alpaca_http_request_digest("GET", ALPACA_DOCTOR_QUOTE_ENDPOINT)?;
    quote.semantic_result_digest = alpaca_quote_semantic_digest(input.quote.disposition, quote)?;
    input.quote.disposition_evidence_digest = quote.semantic_result_digest;

    let requested_set = alpaca_symbol_set_digest(ALPACA_DOCTOR_BATCH_SYMBOLS.iter().copied())?;
    let empty_set = alpaca_symbol_set_digest(std::iter::empty())?;
    let batch = input
        .batch
        .observation
        .as_mut()
        .ok_or(RuntimeVerificationEvidenceError::InvalidEvidence)?;
    batch.http.endpoint_contract_digest = batch_endpoint;
    batch.http.request_digest = alpaca_http_request_digest("GET", alpaca_batch_url()?.as_str())?;
    batch.requested_set_digest = requested_set;
    batch.returned_set_digest = requested_set;
    batch.missing_set_digest = empty_set;
    batch.unexpected_set_digest = empty_set;
    batch.semantic_result_digest = alpaca_batch_semantic_digest(input.batch.disposition, batch);
    input.batch.disposition_evidence_digest = batch.semantic_result_digest;

    let stream = input
        .stream
        .observation
        .as_mut()
        .ok_or(RuntimeVerificationEvidenceError::InvalidEvidence)?;
    stream.endpoint_contract_digest = stream_endpoint;
    stream.request_digest = alpaca_stream_request_digest()?;
    stream.semantic_result_digest = alpaca_stream_semantic_digest(input.stream.disposition, stream);
    input.stream.disposition_evidence_digest = stream.semantic_result_digest;

    let historical = input
        .historical
        .observation
        .as_mut()
        .ok_or(RuntimeVerificationEvidenceError::InvalidEvidence)?;
    historical.endpoint_contract_digest = historical_endpoint;
    historical.request_digest =
        alpaca_historical_request_digest(historical.start_date, historical.end_date)?;
    for page in historical.pages.iter_mut() {
        page.http.endpoint_contract_digest = historical_endpoint;
    }
    historical.pages[0].http.request_digest = alpaca_http_request_digest(
        "GET",
        alpaca_history_url(historical.start_date, historical.end_date)?.as_str(),
    )?;
    historical.pagination_graph_digest =
        alpaca_pagination_graph_digest(&historical.pages, historical.terminal_page_observed)?;
    historical.semantic_result_digest =
        alpaca_historical_semantic_digest(input.historical.disposition, historical);
    input.historical.disposition_evidence_digest = historical.semantic_result_digest;

    let calendar = input
        .calendar
        .observation
        .as_mut()
        .ok_or(RuntimeVerificationEvidenceError::InvalidEvidence)?;
    calendar.http.endpoint_contract_digest = calendar_endpoint;
    calendar.http.request_digest = alpaca_http_request_digest(
        "GET",
        &alpaca_calendar_url(calendar.start_date, calendar.end_date),
    )?;
    calendar.semantic_result_digest =
        alpaca_calendar_semantic_digest(input.calendar.disposition, calendar);
    input.calendar.disposition_evidence_digest = calendar.semantic_result_digest;

    for evidence in input.additional_capabilities.iter_mut() {
        evidence.disposition_evidence_digest =
            alpaca_additional_capability_digest(evidence.capability, evidence.disposition);
    }
    input.provider_observation_sha256 = provider_observation_digest(input)?;
    Ok(())
}

fn validate_history_calendar_binding(
    input: &AlpacaPaperIexDoctorReceiptInput,
) -> Result<(), RuntimeVerificationEvidenceError> {
    let Some(calendar) = input.calendar.observation.as_ref() else {
        return Ok(());
    };
    let history = input
        .historical
        .observation
        .as_ref()
        .ok_or(RuntimeVerificationEvidenceError::InvalidEvidence)?;
    if calendar.start_date != history.start_date
        || calendar.end_date != history.end_date
        || calendar.history_date_count != history.distinct_date_count
        || calendar.history_date_count != history.returned_bar_count
        || calendar.history_dates_digest != history.returned_dates_digest
    {
        return Err(RuntimeVerificationEvidenceError::InvalidEvidence);
    }
    Ok(())
}

fn validate_quote(
    observation: &AlpacaDoctorQuoteObservation,
) -> Result<bool, RuntimeVerificationEvidenceError> {
    observation.http.validate()?;
    require_sha256(observation.semantic_result_digest)?;
    let complete = observation
        .quote_timestamp
        .zip(observation.bid_price)
        .zip(observation.ask_price)
        .zip(observation.bid_size)
        .zip(observation.ask_size);
    if let Some(((((quote_timestamp, bid_price), ask_price), _bid_size), _ask_size)) = complete {
        let _ = quote_timestamp;
        if bid_price.is_sign_negative() || ask_price.is_sign_negative() {
            return Err(RuntimeVerificationEvidenceError::InvalidEvidence);
        }
        Ok(observation.http.successful()
            && !bid_price.is_zero()
            && !ask_price.is_zero()
            && bid_price <= ask_price)
    } else if observation.quote_timestamp.is_some()
        || observation.bid_price.is_some()
        || observation.ask_price.is_some()
        || observation.bid_size.is_some()
        || observation.ask_size.is_some()
    {
        Err(RuntimeVerificationEvidenceError::InvalidEvidence)
    } else {
        Ok(false)
    }
}

fn validate_batch(
    observation: &AlpacaDoctorBatchObservation,
) -> Result<bool, RuntimeVerificationEvidenceError> {
    observation.http.validate()?;
    for digest in [
        observation.semantic_result_digest,
        observation.requested_set_digest,
        observation.returned_set_digest,
        observation.missing_set_digest,
        observation.unexpected_set_digest,
    ] {
        require_sha256(digest)?;
    }
    if observation.requested_count != ALPACA_DOCTOR_BATCH_REQUESTED
        || observation.effective_cardinality > observation.requested_count
        || observation.missing_count > observation.requested_count
        || observation
            .returned_count
            .checked_add(observation.missing_count)
            != Some(observation.requested_count)
        || observation
            .effective_cardinality
            .checked_add(observation.invalid_count)
            != Some(observation.returned_count)
        || observation.duplicate_count > MAX_ALPACA_DOCTOR_BATCH_COMPONENTS
        || observation.unexpected_count > MAX_ALPACA_DOCTOR_BATCH_COMPONENTS
    {
        return Err(RuntimeVerificationEvidenceError::InvalidEvidence);
    }
    Ok(observation.http.successful()
        && observation.returned_count == ALPACA_DOCTOR_BATCH_REQUESTED
        && observation.missing_count == 0
        && observation.unexpected_count == 0
        && observation.duplicate_count == 0
        && observation.invalid_count == 0
        && observation.effective_cardinality == ALPACA_DOCTOR_BATCH_REQUESTED)
}

fn validate_stream(
    observation: &AlpacaDoctorStreamObservation,
) -> Result<bool, RuntimeVerificationEvidenceError> {
    for digest in [
        observation.endpoint_contract_digest,
        observation.request_digest,
        observation.connected_frame_digest,
        observation.authenticated_frame_digest,
        observation.subscription_frame_digest,
        observation.semantic_result_digest,
    ] {
        require_sha256(digest)?;
    }
    observation.handshake_rate.validate()?;
    if observation.handshake_status != 101
        || observation.subscribed_trade_count > 1
        || observation.subscribed_quote_count > 1
        || !(3..=MAX_ALPACA_DOCTOR_STREAM_FRAMES).contains(&observation.frames_observed)
        || observation.bytes_observed == 0
        || observation.bytes_observed > MAX_ALPACA_DOCTOR_STREAM_BYTES
        || observation.authenticated_at > observation.subscribed_at
        || observation.subscribed_at > observation.completed_at
    {
        return Err(RuntimeVerificationEvidenceError::InvalidEvidence);
    }
    Ok(observation.subscribed_trade_count == 1
        && observation.subscribed_quote_count == 1
        && observation.close_sent
        && observation.clean_close_observed)
}

fn validate_history(
    observation: &AlpacaDoctorHistoricalObservation,
) -> Result<bool, RuntimeVerificationEvidenceError> {
    for digest in [
        observation.endpoint_contract_digest,
        observation.request_digest,
        observation.semantic_result_digest,
        observation.returned_dates_digest,
        observation.pagination_graph_digest,
    ] {
        require_sha256(digest)?;
    }
    if observation.start_date > observation.end_date
        || usize::try_from(observation.page_count).ok() != Some(observation.pages.len())
        || observation.pages.is_empty()
        || observation.pages.len() > MAX_ALPACA_DOCTOR_HISTORY_PAGES
        || observation.distinct_date_count != observation.returned_bar_count
        || if observation.returned_bar_count == 0 {
            observation.first_bar_timestamp.is_some() || observation.last_bar_timestamp.is_some()
        } else {
            observation.first_bar_timestamp.is_none() || observation.last_bar_timestamp.is_none()
        }
        || observation
            .first_bar_timestamp
            .zip(observation.last_bar_timestamp)
            .is_some_and(|(first, last)| first > last)
    {
        return Err(RuntimeVerificationEvidenceError::InvalidEvidence);
    }
    let mut total_bytes = 0_u64;
    for (index, page) in observation.pages.iter().enumerate() {
        page.http.validate()?;
        if let Some(digest) = page.request_page_token_digest {
            require_sha256(digest)?;
        }
        if let Some(digest) = page.response_page_token_digest {
            require_sha256(digest)?;
        }
        if index == 0 {
            if page.request_page_token_digest.is_some() {
                return Err(RuntimeVerificationEvidenceError::InvalidEvidence);
            }
        } else {
            let prior_response = observation.pages[index - 1]
                .response_page_token_digest
                .ok_or(RuntimeVerificationEvidenceError::InvalidEvidence)?;
            if page.request_page_token_digest != Some(prior_response) {
                return Err(RuntimeVerificationEvidenceError::InvalidEvidence);
            }
        }
        let last_page = index + 1 == observation.pages.len();
        if (last_page && observation.terminal_page_observed)
            != page.response_page_token_digest.is_none()
        {
            return Err(RuntimeVerificationEvidenceError::InvalidEvidence);
        }
        if let Some(response_token) = page.response_page_token_digest
            && observation.pages[..index]
                .iter()
                .filter_map(|prior| prior.response_page_token_digest)
                .any(|prior| prior == response_token)
        {
            return Err(RuntimeVerificationEvidenceError::InvalidEvidence);
        }
        total_bytes = total_bytes
            .checked_add(page.http.response_bytes)
            .filter(|bytes| *bytes <= MAX_ALPACA_DOCTOR_HISTORY_BYTES)
            .ok_or(RuntimeVerificationEvidenceError::ResourceLimit)?;
    }
    Ok(observation.returned_bar_count > 0
        && observation.terminal_page_observed
        && observation.pages.iter().all(|page| page.http.successful()))
}

fn validate_calendar(
    observation: &AlpacaDoctorCalendarObservation,
) -> Result<bool, RuntimeVerificationEvidenceError> {
    observation.http.validate()?;
    for digest in [
        observation.semantic_result_digest,
        observation.session_dates_digest,
        observation.history_dates_digest,
    ] {
        require_sha256(digest)?;
    }
    if observation.start_date > observation.end_date
        || observation.matched_count > observation.history_date_count
        || observation.matched_count > observation.session_count
        || observation
            .matched_count
            .checked_add(observation.missing_history_count)
            != Some(observation.history_date_count)
        || observation
            .matched_count
            .checked_add(observation.unexpected_history_count)
            != Some(observation.session_count)
        || observation.exact_date_reconciliation
            != (observation.missing_history_count == 0
                && observation.unexpected_history_count == 0
                && observation.history_date_count == observation.matched_count
                && observation.session_count == observation.matched_count
                && observation.matched_count > 0)
        || observation.exact_date_reconciliation
            && observation.session_dates_digest != observation.history_dates_digest
    {
        return Err(RuntimeVerificationEvidenceError::InvalidEvidence);
    }
    Ok(observation.http.successful() && observation.exact_date_reconciliation)
}

fn validate_observation_times(
    input: &AlpacaPaperIexDoctorReceiptInput,
) -> Result<(), RuntimeVerificationEvidenceError> {
    for observed_at in [
        input
            .quote
            .observation
            .as_ref()
            .map(|observation| observation.http.received_at),
        input
            .batch
            .observation
            .as_ref()
            .map(|observation| observation.http.received_at),
        input
            .stream
            .observation
            .as_ref()
            .map(|observation| observation.completed_at),
        input
            .calendar
            .observation
            .as_ref()
            .map(|observation| observation.http.received_at),
    ]
    .into_iter()
    .flatten()
    {
        if observed_at > input.verified_at {
            return Err(RuntimeVerificationEvidenceError::InvalidEvidence);
        }
    }
    if input
        .historical
        .observation
        .as_ref()
        .is_some_and(|observation| {
            observation
                .pages
                .iter()
                .any(|page| page.http.received_at > input.verified_at)
        })
    {
        return Err(RuntimeVerificationEvidenceError::InvalidEvidence);
    }
    Ok(())
}

fn canonical_uuid_text(value: &str) -> bool {
    if value.len() != 36 || value == "00000000-0000-0000-0000-000000000000" {
        return false;
    }
    value.bytes().enumerate().all(|(index, byte)| match index {
        8 | 13 | 18 | 23 => byte == b'-',
        _ => byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte),
    })
}

fn require_nonzero(digest: EvidenceDigest) -> Result<(), RuntimeVerificationEvidenceError> {
    if digest.bytes() == [0; 32] {
        Err(RuntimeVerificationEvidenceError::InvalidEvidence)
    } else {
        Ok(())
    }
}

fn require_sha256(digest: EvidenceDigest) -> Result<(), RuntimeVerificationEvidenceError> {
    if digest.algorithm() != DigestAlgorithm::Sha256 {
        return Err(RuntimeVerificationEvidenceError::InvalidEvidence);
    }
    require_nonzero(digest)
}
