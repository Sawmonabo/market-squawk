//! Provider-local doctor, activation/currentness, publication, and typed query seams.

use std::fmt;
use std::marker::PhantomData;
use std::num::NonZeroU64;
use std::time::Duration;

use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, MarketBarObservation, MarketEvent, SourceId, SourceIdentifier,
    Timestamp,
};
use market_squawk_sources::SealedProviderCaptureSetReceipt;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::{
    AccessTokenGeneration, CapacityObservation, CapacityUnit, ExecutedRestResponse,
    REFRESH_TOKEN_LIFETIME_SECONDS, RawRestResponseReceipt, ReadOnlyRoute,
    SchwabCanonicalStreamerRecord, SchwabInstrumentCandidate, SchwabOAuthAuthorityReceipt,
    SchwabOAuthAuthorityStatus, SchwabOptionSnapshotCandidate, SchwabRestPayload,
    SchwabTransportTelemetrySnapshot, StreamerMicrobatchReceipt,
};

const ACTIVATION_LEASE_VERSION: u16 = 1;
const MAX_ACTIVATION_LEASE_BYTES: usize = 2 * 1024;

/// Frozen owner-authorized private research usage. Commercial sale/redistribution is impossible.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SchwabOwnerUseAuthorization {
    private_retrieval: bool,
    private_display: bool,
    persistence: bool,
    transformation: bool,
    backtesting: bool,
    forecasting: bool,
    model_training_and_operation: bool,
    sale: bool,
    redistribution: bool,
}

/// Explicit downstream purpose checked at every provider-local query boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchwabDataUsePurpose {
    PrivateRetrieval,
    PrivateDisplay,
    Persistence,
    Transformation,
    Backtesting,
    Forecasting,
    ModelTrainingAndOperation,
    Sale,
    Redistribution,
}

impl SchwabOwnerUseAuthorization {
    /// Sole admitted Schwab data-use authorization for this owner-operated product.
    pub const OWNER_PRIVATE_RESEARCH: Self = Self {
        private_retrieval: true,
        private_display: true,
        persistence: true,
        transformation: true,
        backtesting: true,
        forecasting: true,
        model_training_and_operation: true,
        sale: false,
        redistribution: false,
    };

    pub const fn private_retrieval(self) -> bool {
        self.private_retrieval
    }
    pub const fn private_display(self) -> bool {
        self.private_display
    }
    pub const fn persistence(self) -> bool {
        self.persistence
    }
    pub const fn transformation(self) -> bool {
        self.transformation
    }
    pub const fn backtesting(self) -> bool {
        self.backtesting
    }
    pub const fn forecasting(self) -> bool {
        self.forecasting
    }
    pub const fn model_training_and_operation(self) -> bool {
        self.model_training_and_operation
    }
    pub const fn sale(self) -> bool {
        self.sale
    }
    pub const fn redistribution(self) -> bool {
        self.redistribution
    }

    pub const fn allows(self, purpose: SchwabDataUsePurpose) -> bool {
        match purpose {
            SchwabDataUsePurpose::PrivateRetrieval => self.private_retrieval,
            SchwabDataUsePurpose::PrivateDisplay => self.private_display,
            SchwabDataUsePurpose::Persistence => self.persistence,
            SchwabDataUsePurpose::Transformation => self.transformation,
            SchwabDataUsePurpose::Backtesting => self.backtesting,
            SchwabDataUsePurpose::Forecasting => self.forecasting,
            SchwabDataUsePurpose::ModelTrainingAndOperation => self.model_training_and_operation,
            SchwabDataUsePurpose::Sale => self.sale,
            SchwabDataUsePurpose::Redistribution => self.redistribution,
        }
    }

    const fn is_admitted(self) -> bool {
        self.private_retrieval
            && self.private_display
            && self.persistence
            && self.transformation
            && self.backtesting
            && self.forecasting
            && self.model_training_and_operation
            && !self.sale
            && !self.redistribution
    }
}

/// Full provider doctor disposition from a fresh, dedicated observation generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchwabDoctorDisposition {
    Available,
    Degraded,
    Unavailable,
}

/// Adaptive requested-versus-returned evidence plus dense provider record count.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SchwabRestCapacityEvidence {
    observation: CapacityObservation,
    provider_records: u64,
}

impl SchwabRestCapacityEvidence {
    pub const fn observation(self) -> CapacityObservation {
        self.observation
    }
    pub const fn provider_records(self) -> u64 {
        self.provider_records
    }
}

/// Redacted OAuth + REST + Streamer provider observation.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct SchwabDoctorObservation {
    disposition: SchwabDoctorDisposition,
    observed_at_unix_seconds: u64,
    expires_at_unix_seconds: u64,
    oauth: SchwabOAuthAuthorityReceipt,
    market_data_principal_sha256: [u8; 32],
    bootstrap_sha256: [u8; 32],
    user_preference_receipt_sha256: [u8; 32],
    rest_response_sha256: [u8; 32],
    market_rest_receipt_sha256: [u8; 32],
    stream_observation_sha256: [u8; 32],
    rest_capacity: SchwabRestCapacityEvidence,
    streamer_connections: u64,
    streamer_frames_captured: u64,
    streamer_events: u64,
    receipt_sha256: [u8; 32],
}

impl fmt::Debug for SchwabDoctorObservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabDoctorObservation")
            .field("disposition", &self.disposition)
            .field("observed_at_unix_seconds", &self.observed_at_unix_seconds)
            .field("expires_at_unix_seconds", &self.expires_at_unix_seconds)
            .field("oauth_generation", &self.oauth.generation())
            .field("market_data_principal", &"[ONE-WAY BINDING]")
            .field("bootstrap", &"[DIGEST]")
            .field("user_preference_receipt", &"[DIGEST]")
            .field("rest_response", &"[DIGEST]")
            .field("market_rest_receipt", &"[DIGEST]")
            .field("stream_observation", &"[DIGEST]")
            .field("rest_capacity", &self.rest_capacity)
            .field("streamer_connections", &self.streamer_connections)
            .field("streamer_frames_captured", &self.streamer_frames_captured)
            .field("streamer_events", &self.streamer_events)
            .field("receipt", &"[DIGEST]")
            .finish()
    }
}

impl SchwabDoctorObservation {
    /// Builds one full-capability doctor receipt from a fresh token/bootstrap/REST/Streamer probe.
    ///
    /// The supplied telemetry must be a fresh doctor-local generation. Cumulative production
    /// telemetry cannot truthfully prove this exact observation.
    pub fn try_observe_full(
        oauth: SchwabOAuthAuthorityReceipt,
        user_preference_probe: &ExecutedRestResponse,
        market_probe: &ExecutedRestResponse,
        stream_probe: &StreamerMicrobatchReceipt,
        telemetry: SchwabTransportTelemetrySnapshot,
        observed_at_unix_seconds: u64,
        valid_for: Duration,
    ) -> Result<Self, SchwabVerticalError> {
        let preference_receipt = user_preference_probe.capture().receipt();
        let market_receipt = market_probe.capture().receipt();
        let SchwabRestPayload::StreamerBootstrap(bootstrap) = user_preference_probe.payload()
        else {
            return Err(SchwabVerticalError::InvalidDoctorEvidence);
        };
        if valid_for.as_secs() == 0
            || preference_receipt.route() != ReadOnlyRoute::UserPreference
            || market_receipt.route() == ReadOnlyRoute::UserPreference
            || preference_receipt.token_generation() != oauth.generation()
            || market_receipt.token_generation() != oauth.generation()
            || stream_probe.token_generation() != oauth.generation()
            || preference_receipt.body_sha256() != bootstrap.raw_sha256()
            || !matches!(
                (preference_receipt.status(), market_receipt.status()),
                (200..=299, 200..=299)
            )
            || bootstrap.value().market_data_principal_sha256() == [0; 32]
            || stream_probe.frame_count() == 0
            || stream_probe.payload_bytes() == 0
            || stream_probe.content_sha256() == [0; 32]
            || stream_probe.observation_sha256() == [0; 32]
            || observed_at_unix_seconds < oauth.access_issued_at_unix_seconds()
            || observed_at_unix_seconds >= oauth.access_expires_at_unix_seconds()
        {
            return Err(SchwabVerticalError::InvalidDoctorEvidence);
        }
        validate_doctor_rest_generation(
            user_preference_probe,
            market_probe,
            telemetry,
            observed_at_unix_seconds,
            valid_for,
            oauth,
        )?;
        validate_doctor_stream_generation(
            stream_probe,
            telemetry,
            observed_at_unix_seconds,
            valid_for,
            oauth,
        )?;
        let accounting = market_probe.accounting();
        let unit = match market_receipt.route() {
            ReadOnlyRoute::Quotes
            | ReadOnlyRoute::SingleQuote
            | ReadOnlyRoute::Chains
            | ReadOnlyRoute::ExpirationChain
            | ReadOnlyRoute::PriceHistory
            | ReadOnlyRoute::Movers => CapacityUnit::Symbols,
            ReadOnlyRoute::Markets | ReadOnlyRoute::SingleMarket => CapacityUnit::MarketSegments,
            ReadOnlyRoute::Instruments | ReadOnlyRoute::InstrumentByCusip => {
                CapacityUnit::LookupKeys
            }
            ReadOnlyRoute::UserPreference => {
                return Err(SchwabVerticalError::InvalidDoctorEvidence);
            }
        };
        let observation = CapacityObservation {
            unit,
            requested: accounting.requested,
            returned: accounting.returned,
            missing: accounting.missing,
            duplicates: 0,
            malformed: 0,
            unexpected: accounting.unexpected,
            request_bytes: u64::try_from(market_receipt.request_url().len())
                .map_err(|_| SchwabVerticalError::Overflow)?,
            response_bytes: market_receipt.body_bytes(),
            latency_ms: market_receipt.latency_ms(),
            status: market_receipt.status(),
            retry_after_present: market_receipt.retry_after_present(),
            validation_failed: telemetry.validation_failures_total > 0,
        }
        .validate()
        .map_err(|_| SchwabVerticalError::InvalidDoctorEvidence)?;
        let rest_capacity = SchwabRestCapacityEvidence {
            observation,
            provider_records: accounting.provider_records,
        };
        let rest_available = accounting.returned > 0
            && accounting.provider_records > 0
            && accounting.unexpected == 0
            && (200..=299).contains(&market_receipt.status());
        let streamer_available = telemetry.streamer_connections_total > 0
            && telemetry.streamer_frames_captured_total == stream_probe.frame_count()
            && telemetry.streamer_events_total > 0;
        let pressure = accounting.missing > 0
            || preference_receipt.retry_after_present()
            || telemetry.rest_429_total > 0
            || telemetry.rest_failures_total > 0
            || telemetry.validation_failures_total > 0
            || telemetry.streamer_reconnects_total > 0
            || telemetry.streamer_connect_failures_total > 0
            || telemetry.streamer_disconnects_total > 0;
        let disposition = if !rest_available || !streamer_available {
            SchwabDoctorDisposition::Unavailable
        } else if pressure {
            SchwabDoctorDisposition::Degraded
        } else {
            SchwabDoctorDisposition::Available
        };
        let requested_expiry = observed_at_unix_seconds
            .checked_add(valid_for.as_secs())
            .ok_or(SchwabVerticalError::Overflow)?;
        let expires_at_unix_seconds = requested_expiry.min(oauth.refresh_expires_at_unix_seconds());
        if expires_at_unix_seconds <= observed_at_unix_seconds {
            return Err(SchwabVerticalError::InvalidDoctorEvidence);
        }
        let market_data_principal_sha256 = bootstrap.value().market_data_principal_sha256();
        let bootstrap_sha256 = bootstrap.raw_sha256();
        let user_preference_receipt_sha256 = rest_receipt_digest(preference_receipt);
        let rest_response_sha256 = market_receipt.body_sha256();
        let market_rest_receipt_sha256 = rest_receipt_digest(market_receipt);
        let stream_observation_sha256 = stream_probe.observation_sha256();
        let receipt_sha256 = doctor_digest(
            disposition,
            observed_at_unix_seconds,
            expires_at_unix_seconds,
            oauth,
            market_data_principal_sha256,
            bootstrap_sha256,
            user_preference_receipt_sha256,
            rest_response_sha256,
            market_rest_receipt_sha256,
            stream_observation_sha256,
            rest_capacity,
            telemetry,
        );
        Ok(Self {
            disposition,
            observed_at_unix_seconds,
            expires_at_unix_seconds,
            oauth,
            market_data_principal_sha256,
            bootstrap_sha256,
            user_preference_receipt_sha256,
            rest_response_sha256,
            market_rest_receipt_sha256,
            stream_observation_sha256,
            rest_capacity,
            streamer_connections: telemetry.streamer_connections_total,
            streamer_frames_captured: telemetry.streamer_frames_captured_total,
            streamer_events: telemetry.streamer_events_total,
            receipt_sha256,
        })
    }

    pub const fn disposition(self) -> SchwabDoctorDisposition {
        self.disposition
    }
    pub const fn expires_at_unix_seconds(self) -> u64 {
        self.expires_at_unix_seconds
    }
    pub const fn receipt_sha256(self) -> [u8; 32] {
        self.receipt_sha256
    }
    pub const fn rest_capacity(self) -> SchwabRestCapacityEvidence {
        self.rest_capacity
    }
}

/// Durable-safe activation fence minted only by an available full doctor.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct SchwabActivationLease {
    activated_at_unix_seconds: u64,
    expires_at_unix_seconds: u64,
    minimum_oauth_generation: AccessTokenGeneration,
    refresh_authorized_at_unix_seconds: u64,
    refresh_expires_at_unix_seconds: u64,
    market_data_principal_sha256: [u8; 32],
    doctor_receipt_sha256: [u8; 32],
    use_authorization: SchwabOwnerUseAuthorization,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct SchwabActivationLeaseWire {
    version: u16,
    activated_at_unix_seconds: u64,
    expires_at_unix_seconds: u64,
    minimum_oauth_generation: u64,
    refresh_authorized_at_unix_seconds: u64,
    refresh_expires_at_unix_seconds: u64,
    market_data_principal_sha256: [u8; 32],
    doctor_receipt_sha256: [u8; 32],
}

impl fmt::Debug for SchwabActivationLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabActivationLease")
            .field("activated_at_unix_seconds", &self.activated_at_unix_seconds)
            .field("expires_at_unix_seconds", &self.expires_at_unix_seconds)
            .field("minimum_oauth_generation", &self.minimum_oauth_generation)
            .field("market_data_principal", &"[ONE-WAY BINDING]")
            .field("doctor_receipt", &"[DIGEST]")
            .field("use_authorization", &self.use_authorization)
            .finish()
    }
}

impl SchwabActivationLease {
    pub fn activate(
        doctor: SchwabDoctorObservation,
        now_unix_seconds: u64,
    ) -> Result<Self, SchwabVerticalError> {
        if doctor.disposition != SchwabDoctorDisposition::Available
            || now_unix_seconds < doctor.observed_at_unix_seconds
            || now_unix_seconds >= doctor.expires_at_unix_seconds
        {
            return Err(SchwabVerticalError::ProviderUnavailable);
        }
        let use_authorization = SchwabOwnerUseAuthorization::OWNER_PRIVATE_RESEARCH;
        if !use_authorization.is_admitted() {
            return Err(SchwabVerticalError::UseNotAuthorized);
        }
        let lease = Self {
            activated_at_unix_seconds: now_unix_seconds,
            expires_at_unix_seconds: doctor.expires_at_unix_seconds,
            minimum_oauth_generation: doctor.oauth.generation(),
            refresh_authorized_at_unix_seconds: doctor.oauth.refresh_authorized_at_unix_seconds(),
            refresh_expires_at_unix_seconds: doctor.oauth.refresh_expires_at_unix_seconds(),
            market_data_principal_sha256: doctor.market_data_principal_sha256,
            doctor_receipt_sha256: doctor.receipt_sha256,
            use_authorization,
        };
        lease.validate()?;
        Ok(lease)
    }

    /// Encodes a bounded, secret-free restart lease. Provider payloads and credentials are absent.
    pub fn encode_durable(self) -> Result<Vec<u8>, SchwabVerticalError> {
        self.validate()?;
        let bytes = serde_json::to_vec(&SchwabActivationLeaseWire {
            version: ACTIVATION_LEASE_VERSION,
            activated_at_unix_seconds: self.activated_at_unix_seconds,
            expires_at_unix_seconds: self.expires_at_unix_seconds,
            minimum_oauth_generation: self.minimum_oauth_generation.get(),
            refresh_authorized_at_unix_seconds: self.refresh_authorized_at_unix_seconds,
            refresh_expires_at_unix_seconds: self.refresh_expires_at_unix_seconds,
            market_data_principal_sha256: self.market_data_principal_sha256,
            doctor_receipt_sha256: self.doctor_receipt_sha256,
        })
        .map_err(|_| SchwabVerticalError::InvalidActivationLease)?;
        if bytes.len() > MAX_ACTIVATION_LEASE_BYTES {
            return Err(SchwabVerticalError::InvalidActivationLease);
        }
        Ok(bytes)
    }

    /// Restores a durable lease only if current OAuth and User Preference identity still match.
    pub fn try_restore(
        bytes: &[u8],
        oauth: SchwabOAuthAuthorityStatus,
        market_data_principal_sha256: [u8; 32],
        now_unix_seconds: u64,
    ) -> Result<Self, SchwabVerticalError> {
        if bytes.is_empty() || bytes.len() > MAX_ACTIVATION_LEASE_BYTES {
            return Err(SchwabVerticalError::InvalidActivationLease);
        }
        let wire: SchwabActivationLeaseWire = serde_json::from_slice(bytes)
            .map_err(|_| SchwabVerticalError::InvalidActivationLease)?;
        if wire.version != ACTIVATION_LEASE_VERSION {
            return Err(SchwabVerticalError::InvalidActivationLease);
        }
        let lease = Self {
            activated_at_unix_seconds: wire.activated_at_unix_seconds,
            expires_at_unix_seconds: wire.expires_at_unix_seconds,
            minimum_oauth_generation: AccessTokenGeneration::new(
                NonZeroU64::new(wire.minimum_oauth_generation)
                    .ok_or(SchwabVerticalError::InvalidActivationLease)?,
            ),
            refresh_authorized_at_unix_seconds: wire.refresh_authorized_at_unix_seconds,
            refresh_expires_at_unix_seconds: wire.refresh_expires_at_unix_seconds,
            market_data_principal_sha256: wire.market_data_principal_sha256,
            doctor_receipt_sha256: wire.doctor_receipt_sha256,
            use_authorization: SchwabOwnerUseAuthorization::OWNER_PRIVATE_RESEARCH,
        };
        lease.validate()?;
        if lease.currentness(oauth, market_data_principal_sha256, now_unix_seconds)
            != SchwabProviderCurrentness::Current
        {
            return Err(SchwabVerticalError::ProviderUnavailable);
        }
        Ok(lease)
    }

    pub fn currentness(
        self,
        oauth: SchwabOAuthAuthorityStatus,
        market_data_principal_sha256: [u8; 32],
        now_unix_seconds: u64,
    ) -> SchwabProviderCurrentness {
        if now_unix_seconds < self.activated_at_unix_seconds
            || now_unix_seconds >= self.expires_at_unix_seconds
            || now_unix_seconds >= self.refresh_expires_at_unix_seconds
        {
            return SchwabProviderCurrentness::Expired;
        }
        if market_data_principal_sha256 != self.market_data_principal_sha256 {
            return SchwabProviderCurrentness::PrincipalChanged;
        }
        let SchwabOAuthAuthorityStatus::Active(oauth) = oauth else {
            return SchwabProviderCurrentness::AuthorizationUnavailable;
        };
        if oauth.generation() < self.minimum_oauth_generation
            || oauth.refresh_authorized_at_unix_seconds() != self.refresh_authorized_at_unix_seconds
            || oauth.refresh_expires_at_unix_seconds() != self.refresh_expires_at_unix_seconds
        {
            return SchwabProviderCurrentness::AuthorizationChanged;
        }
        SchwabProviderCurrentness::Current
    }

    pub const fn doctor_receipt_sha256(self) -> [u8; 32] {
        self.doctor_receipt_sha256
    }
    pub const fn use_authorization(self) -> SchwabOwnerUseAuthorization {
        self.use_authorization
    }

    fn validate(self) -> Result<(), SchwabVerticalError> {
        if self.activated_at_unix_seconds < self.refresh_authorized_at_unix_seconds
            || self.activated_at_unix_seconds >= self.expires_at_unix_seconds
            || self.expires_at_unix_seconds > self.refresh_expires_at_unix_seconds
            || self
                .refresh_authorized_at_unix_seconds
                .checked_add(REFRESH_TOKEN_LIFETIME_SECONDS)
                != Some(self.refresh_expires_at_unix_seconds)
            || self.market_data_principal_sha256 == [0; 32]
            || self.doctor_receipt_sha256 == [0; 32]
            || !self.use_authorization.is_admitted()
        {
            return Err(SchwabVerticalError::InvalidActivationLease);
        }
        Ok(())
    }
}

/// Exact provider currentness result used on activation, restore, publication, and query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchwabProviderCurrentness {
    Current,
    Expired,
    AuthorizationUnavailable,
    AuthorizationChanged,
    PrincipalChanged,
}

/// Provider-local canonical family carried by an immutable publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchwabPublicationFamily {
    MarketEvents,
    HistoricalBars,
    OptionSnapshots,
    InstrumentCandidates,
    StreamerCandidates,
}

mod sealed {
    pub trait Sealed {}
}

/// Closed record types admitted by the typed Schwab publication seam.
pub trait SchwabPublishedRecord: sealed::Sealed + Clone + fmt::Debug + Eq + Send + Sync {
    const FAMILY: SchwabPublicationFamily;
}

macro_rules! published_record {
    ($record:ty, $family:expr) => {
        impl sealed::Sealed for $record {}
        impl SchwabPublishedRecord for $record {
            const FAMILY: SchwabPublicationFamily = $family;
        }
    };
}

published_record!(MarketEvent, SchwabPublicationFamily::MarketEvents);
published_record!(
    MarketBarObservation,
    SchwabPublicationFamily::HistoricalBars
);
published_record!(
    SchwabOptionSnapshotCandidate,
    SchwabPublicationFamily::OptionSnapshots
);
published_record!(
    SchwabInstrumentCandidate,
    SchwabPublicationFamily::InstrumentCandidates
);
published_record!(
    SchwabCanonicalStreamerRecord,
    SchwabPublicationFamily::StreamerCandidates
);

/// Typed canonical generation bound to exact sealed raw capture and activation/currentness proof.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchwabProviderPublication<T: SchwabPublishedRecord> {
    source_id: SourceId,
    generation: SourceIdentifier,
    canonical_digest: EvidenceDigest,
    published_at: Timestamp,
    doctor_receipt_sha256: [u8; 32],
    capture: SealedProviderCaptureSetReceipt,
    records: Box<[T]>,
}

impl<T: SchwabPublishedRecord> SchwabProviderPublication<T> {
    #[allow(
        clippy::too_many_arguments,
        reason = "publication identity, lineage, currentness, and payload stay explicit"
    )]
    pub fn try_new(
        lease: SchwabActivationLease,
        oauth: SchwabOAuthAuthorityStatus,
        market_data_principal_sha256: [u8; 32],
        now_unix_seconds: u64,
        generation: SourceIdentifier,
        canonical_digest: EvidenceDigest,
        published_at: Timestamp,
        capture: SealedProviderCaptureSetReceipt,
        records: Vec<T>,
    ) -> Result<Self, SchwabVerticalError> {
        if lease.currentness(oauth, market_data_principal_sha256, now_unix_seconds)
            != SchwabProviderCurrentness::Current
            || !lease.use_authorization().is_admitted()
            || canonical_digest.bytes() == [0; 32]
            || canonical_digest.algorithm() != DigestAlgorithm::Sha256
            || records.is_empty()
        {
            return Err(SchwabVerticalError::ProviderUnavailable);
        }
        if !lease
            .use_authorization()
            .allows(SchwabDataUsePurpose::Persistence)
            || !lease
                .use_authorization()
                .allows(SchwabDataUsePurpose::Transformation)
        {
            return Err(SchwabVerticalError::UseNotAuthorized);
        }
        let last_received_at = capture
            .capture()
            .pages()
            .last()
            .ok_or(SchwabVerticalError::ProviderUnavailable)?
            .received_at();
        if published_at < last_received_at {
            return Err(SchwabVerticalError::ProviderUnavailable);
        }
        let source_id = capture.capture().source_id().clone();
        Ok(Self {
            source_id,
            generation,
            canonical_digest,
            published_at,
            doctor_receipt_sha256: lease.doctor_receipt_sha256(),
            capture,
            records: records.into_boxed_slice(),
        })
    }

    pub const fn family(&self) -> SchwabPublicationFamily {
        T::FAMILY
    }
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }
    pub const fn generation(&self) -> &SourceIdentifier {
        &self.generation
    }
    pub const fn canonical_digest(&self) -> EvidenceDigest {
        self.canonical_digest
    }
    pub const fn capture(&self) -> &SealedProviderCaptureSetReceipt {
        &self.capture
    }

    /// Returns a typed view only while the exact lease remains current.
    pub fn query<'a>(
        &'a self,
        lease: SchwabActivationLease,
        oauth: SchwabOAuthAuthorityStatus,
        market_data_principal_sha256: [u8; 32],
        now_unix_seconds: u64,
        purpose: SchwabDataUsePurpose,
    ) -> Result<SchwabProviderQuery<'a, T>, SchwabVerticalError> {
        if lease.currentness(oauth, market_data_principal_sha256, now_unix_seconds)
            != SchwabProviderCurrentness::Current
            || lease.doctor_receipt_sha256() != self.doctor_receipt_sha256
        {
            return Err(SchwabVerticalError::ProviderUnavailable);
        }
        if !lease.use_authorization().allows(purpose) {
            return Err(SchwabVerticalError::UseNotAuthorized);
        }
        Ok(SchwabProviderQuery {
            publication: self,
            purpose,
            _private: PhantomData,
        })
    }
}

/// Read-only typed provider query; it contains no network, token, account, or execution authority.
#[derive(Debug)]
pub struct SchwabProviderQuery<'a, T: SchwabPublishedRecord> {
    publication: &'a SchwabProviderPublication<T>,
    purpose: SchwabDataUsePurpose,
    _private: PhantomData<&'a T>,
}

impl<T: SchwabPublishedRecord> SchwabProviderQuery<'_, T> {
    pub fn records(&self) -> &[T] {
        &self.publication.records
    }
    pub const fn generation(&self) -> &SourceIdentifier {
        &self.publication.generation
    }
    pub const fn canonical_digest(&self) -> EvidenceDigest {
        self.publication.canonical_digest
    }
    pub const fn published_at(&self) -> Timestamp {
        self.publication.published_at
    }
    pub const fn purpose(&self) -> SchwabDataUsePurpose {
        self.purpose
    }
}

fn validate_doctor_rest_generation(
    user_preference_probe: &ExecutedRestResponse,
    market_probe: &ExecutedRestResponse,
    telemetry: SchwabTransportTelemetrySnapshot,
    observed_at_unix_seconds: u64,
    valid_for: Duration,
    oauth: SchwabOAuthAuthorityReceipt,
) -> Result<(), SchwabVerticalError> {
    let preference = user_preference_probe.accounting();
    let market = market_probe.accounting();
    if preference.requested != 1
        || preference.returned != 1
        || preference.missing != 0
        || preference.unexpected != 0
        || preference.provider_records != 1
    {
        return Err(SchwabVerticalError::InvalidDoctorEvidence);
    }
    let requested = preference
        .requested
        .checked_add(market.requested)
        .ok_or(SchwabVerticalError::Overflow)?;
    let returned = preference
        .returned
        .checked_add(market.returned)
        .ok_or(SchwabVerticalError::Overflow)?;
    let missing = preference
        .missing
        .checked_add(market.missing)
        .ok_or(SchwabVerticalError::Overflow)?;
    let unexpected = preference
        .unexpected
        .checked_add(market.unexpected)
        .ok_or(SchwabVerticalError::Overflow)?;
    let records = preference
        .provider_records
        .checked_add(market.provider_records)
        .ok_or(SchwabVerticalError::Overflow)?;
    let preference_receipt = user_preference_probe.capture().receipt();
    let market_receipt = market_probe.capture().receipt();
    let request_target_bytes = u64::try_from(preference_receipt.request_url().len())
        .ok()
        .and_then(|value| {
            u64::try_from(market_receipt.request_url().len())
                .ok()
                .and_then(|market| value.checked_add(market))
        })
        .ok_or(SchwabVerticalError::Overflow)?;
    let response_bytes = preference_receipt
        .body_bytes()
        .checked_add(market_receipt.body_bytes())
        .ok_or(SchwabVerticalError::Overflow)?;
    let latency = preference_receipt
        .latency_ms()
        .checked_add(market_receipt.latency_ms())
        .ok_or(SchwabVerticalError::Overflow)?;
    if telemetry.rest_requests_total != 2
        || telemetry.rest_responses_total != 2
        || telemetry.rest_failures_total != 0
        || telemetry.rest_429_total != 0
        || telemetry.validation_failures_total != 0
        || telemetry.requested_items_total != requested
        || telemetry.returned_items_total != returned
        || telemetry.missing_items_total != missing
        || telemetry.unexpected_items_total != unexpected
        || telemetry.rest_records_total != records
        || telemetry.request_target_bytes_total != request_target_bytes
        || telemetry.rest_response_bytes_total != response_bytes
        || telemetry.rest_latency_ms_total != latency
        || telemetry.rest_latency_ms_max
            != preference_receipt
                .latency_ms()
                .max(market_receipt.latency_ms())
    {
        return Err(SchwabVerticalError::InvalidDoctorEvidence);
    }
    validate_probe_times(
        &[
            preference_receipt.received_at_unix_millis(),
            market_receipt.received_at_unix_millis(),
        ],
        observed_at_unix_seconds,
        valid_for,
        oauth,
    )
}

fn validate_doctor_stream_generation(
    stream_probe: &StreamerMicrobatchReceipt,
    telemetry: SchwabTransportTelemetrySnapshot,
    observed_at_unix_seconds: u64,
    valid_for: Duration,
    oauth: SchwabOAuthAuthorityReceipt,
) -> Result<(), SchwabVerticalError> {
    if telemetry.streamer_connect_attempts_total == 0
        || telemetry.streamer_connections_total == 0
        || telemetry.streamer_connections_total > telemetry.streamer_connect_attempts_total
        || telemetry.streamer_requests_total < 2
        || telemetry.streamer_frames_total < stream_probe.frame_count()
        || telemetry.streamer_frame_bytes_total < stream_probe.payload_bytes()
        || telemetry.streamer_frames_captured_total != stream_probe.frame_count()
        || telemetry.streamer_frame_bytes_captured_total != stream_probe.payload_bytes()
        || telemetry.streamer_microbatches_total != 1
        || telemetry.streamer_events_total == 0
    {
        return Err(SchwabVerticalError::InvalidDoctorEvidence);
    }
    validate_probe_times(
        &[
            stream_probe.first_received_at_unix_millis(),
            stream_probe.last_received_at_unix_millis(),
        ],
        observed_at_unix_seconds,
        valid_for,
        oauth,
    )
}

fn validate_probe_times(
    received_times: &[u64],
    observed_at_unix_seconds: u64,
    valid_for: Duration,
    oauth: SchwabOAuthAuthorityReceipt,
) -> Result<(), SchwabVerticalError> {
    let observed_ceiling_millis = observed_at_unix_seconds
        .checked_add(1)
        .and_then(|value| value.checked_mul(1_000))
        .and_then(|value| value.checked_sub(1))
        .ok_or(SchwabVerticalError::Overflow)?;
    let issued_floor_millis = oauth
        .access_issued_at_unix_seconds()
        .checked_mul(1_000)
        .ok_or(SchwabVerticalError::Overflow)?;
    let valid_for_millis =
        u64::try_from(valid_for.as_millis()).map_err(|_| SchwabVerticalError::Overflow)?;
    for &received_at in received_times {
        if received_at < issued_floor_millis
            || received_at > observed_ceiling_millis
            || observed_ceiling_millis - received_at > valid_for_millis
        {
            return Err(SchwabVerticalError::InvalidDoctorEvidence);
        }
    }
    Ok(())
}

fn rest_receipt_digest(receipt: &RawRestResponseReceipt) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk/schwab-rest-receipt/v1");
    hasher.update([match receipt.route() {
        ReadOnlyRoute::Quotes => 1,
        ReadOnlyRoute::SingleQuote => 2,
        ReadOnlyRoute::Chains => 3,
        ReadOnlyRoute::ExpirationChain => 4,
        ReadOnlyRoute::PriceHistory => 5,
        ReadOnlyRoute::Markets => 6,
        ReadOnlyRoute::SingleMarket => 7,
        ReadOnlyRoute::Movers => 8,
        ReadOnlyRoute::Instruments => 9,
        ReadOnlyRoute::InstrumentByCusip => 10,
        ReadOnlyRoute::UserPreference => 11,
    }]);
    hasher.update(receipt.token_generation().get().to_be_bytes());
    hasher.update(receipt.request_sha256());
    hasher.update(receipt.status().to_be_bytes());
    hasher.update(receipt.received_at_unix_millis().to_be_bytes());
    hasher.update(receipt.body_bytes().to_be_bytes());
    hasher.update(receipt.body_sha256());
    match receipt.declared_body_bytes() {
        Some(value) => {
            hasher.update([1]);
            hasher.update(value.to_be_bytes());
        }
        None => hasher.update([0]),
    }
    hasher.update(receipt.latency_ms().to_be_bytes());
    hasher.update((receipt.headers().len() as u64).to_be_bytes());
    for header in receipt.headers() {
        hasher.update((header.name().len() as u64).to_be_bytes());
        hasher.update(header.name().as_bytes());
        hasher.update((header.value().len() as u64).to_be_bytes());
        hasher.update(header.value());
    }
    hasher.finalize().into()
}

#[allow(
    clippy::too_many_arguments,
    reason = "doctor receipt binds every independent evidence dimension"
)]
fn doctor_digest(
    disposition: SchwabDoctorDisposition,
    observed_at: u64,
    expires_at: u64,
    oauth: SchwabOAuthAuthorityReceipt,
    principal: [u8; 32],
    bootstrap: [u8; 32],
    user_preference_receipt: [u8; 32],
    rest: [u8; 32],
    market_rest_receipt: [u8; 32],
    stream_observation: [u8; 32],
    capacity: SchwabRestCapacityEvidence,
    telemetry: SchwabTransportTelemetrySnapshot,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk/schwab-full-doctor/v4");
    hasher.update([match disposition {
        SchwabDoctorDisposition::Available => 1,
        SchwabDoctorDisposition::Degraded => 2,
        SchwabDoctorDisposition::Unavailable => 3,
    }]);
    hasher.update(observed_at.to_be_bytes());
    hasher.update(expires_at.to_be_bytes());
    hasher.update(oauth.generation().get().to_be_bytes());
    hasher.update(oauth.refresh_authorized_at_unix_seconds().to_be_bytes());
    hasher.update(oauth.refresh_expires_at_unix_seconds().to_be_bytes());
    hasher.update(principal);
    hasher.update(bootstrap);
    hasher.update(user_preference_receipt);
    hasher.update(rest);
    hasher.update(market_rest_receipt);
    hasher.update(stream_observation);
    let observation = capacity.observation;
    hasher.update([match observation.unit {
        CapacityUnit::Requests => 1,
        CapacityUnit::Symbols => 2,
        CapacityUnit::MarketSegments => 3,
        CapacityUnit::LookupKeys => 4,
        CapacityUnit::OptionContracts => 5,
        CapacityUnit::Candles => 6,
        CapacityUnit::Frames => 7,
        CapacityUnit::StreamEvents => 8,
    }]);
    for value in [
        observation.requested,
        observation.returned,
        observation.missing,
        observation.duplicates,
        observation.malformed,
        observation.unexpected,
        observation.request_bytes,
        observation.response_bytes,
        observation.latency_ms,
        capacity.provider_records,
    ] {
        hasher.update(value.to_be_bytes());
    }
    hasher.update(observation.status.to_be_bytes());
    hasher.update([
        u8::from(observation.retry_after_present),
        u8::from(observation.validation_failed),
    ]);
    for value in [
        telemetry.rest_requests_total,
        telemetry.rest_responses_total,
        telemetry.rest_failures_total,
        telemetry.rest_429_total,
        telemetry.requested_items_total,
        telemetry.returned_items_total,
        telemetry.missing_items_total,
        telemetry.unexpected_items_total,
        telemetry.rest_records_total,
        telemetry.request_target_bytes_total,
        telemetry.rest_response_bytes_total,
        telemetry.rest_latency_ms_total,
        telemetry.rest_latency_ms_max,
        telemetry.validation_failures_total,
        telemetry.streamer_connect_attempts_total,
        telemetry.streamer_connections_total,
        telemetry.streamer_reconnects_total,
        telemetry.streamer_connect_failures_total,
        telemetry.streamer_disconnects_total,
        telemetry.streamer_clean_closes_total,
        telemetry.streamer_gap_ms_total,
        telemetry.streamer_gap_ms_max,
        telemetry.streamer_requests_total,
        telemetry.streamer_request_bytes_total,
        telemetry.streamer_frames_total,
        telemetry.streamer_frame_bytes_total,
        telemetry.streamer_frames_captured_total,
        telemetry.streamer_frame_bytes_captured_total,
        telemetry.streamer_microbatches_total,
        telemetry.streamer_events_total,
        telemetry.streamer_responses_total,
        telemetry.streamer_notifications_total,
    ] {
        hasher.update(value.to_be_bytes());
    }
    hasher.finalize().into()
}

/// Secret-free provider-vertical failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum SchwabVerticalError {
    #[error("Schwab doctor evidence is incomplete or inconsistent")]
    InvalidDoctorEvidence,
    #[error("Schwab activation lease is invalid")]
    InvalidActivationLease,
    #[error("Schwab provider is not currently available")]
    ProviderUnavailable,
    #[error("Schwab data use is outside owner authorization")]
    UseNotAuthorized,
    #[error("Schwab provider evidence arithmetic overflowed")]
    Overflow,
}
