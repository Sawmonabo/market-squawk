//! Family-scoped observed capability and sealed provider-capture inputs.
//!
//! A successful request from one Schwab family never authorizes another family. The only durable
//! publication input currently exposed by this adapter is daily price history. Streamer capture
//! can be sealed without granting canonical-publication authority. The shared data authority
//! remains responsible for Parquet generation and manifest publication.

use std::fmt;
use std::time::Duration;

use market_squawk_domain::{
    BarTimeSemantics, Currency, EvidenceDigest, InstrumentId, MarketBarAdjustment,
    MarketBarObservation, MetadataRevision, ProviderInstrumentId, SourceId, SourceIdentifier,
    Timestamp, VenueId,
};
use market_squawk_platform::SealedResearchJournalStore;
use market_squawk_sources::{ProviderCaptureTerminalDisposition, SealedProviderCaptureSetReceipt};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::{
    ACCESS_TOKEN_MAX_LIFETIME_SECONDS, ExecutedRestResponse, ProviderIdentifier,
    RawRestResponseReceipt, ReadOnlyRoute, SchwabCaptureCoordinates, SchwabOAuthAuthorityReceipt,
    SchwabRestPayload, StreamerMicrobatch, StreamerMicrobatchReceipt,
};

/// The sole family for which this adapter can currently mint observed publication capability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchwabObservedCapabilityFamily {
    DailyPriceHistory,
}

/// Currentness of one exact family observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchwabCapabilityCurrentness {
    Current,
    Expired,
    TokenGenerationChanged,
    OAuthAuthorityChanged,
    PrincipalChanged,
    BootstrapChanged,
    ProbeChanged,
}

/// Fresh User Preference plus exact daily price-history response evidence.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct SchwabPriceHistoryCapabilityObservation {
    observed_at_unix_seconds: u64,
    expires_at_unix_seconds: u64,
    oauth_authority: SchwabOAuthAuthorityReceipt,
    market_data_principal_sha256: [u8; 32],
    user_preference_receipt_sha256: [u8; 32],
    price_history_receipt_sha256: [u8; 32],
    request_sha256: [u8; 32],
    response_sha256: [u8; 32],
    returned_candles: u64,
    receipt_sha256: [u8; 32],
}

impl fmt::Debug for SchwabPriceHistoryCapabilityObservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabPriceHistoryCapabilityObservation")
            .field("family", &self.family())
            .field("observed_at_unix_seconds", &self.observed_at_unix_seconds)
            .field("expires_at_unix_seconds", &self.expires_at_unix_seconds)
            .field("oauth_authority", &self.oauth_authority)
            .field("market_data_principal", &"[ONE-WAY BINDING]")
            .field("user_preference_receipt", &"[DIGEST]")
            .field("price_history_receipt", &"[DIGEST]")
            .field("returned_candles", &self.returned_candles)
            .field("receipt", &"[DIGEST]")
            .finish()
    }
}

impl SchwabPriceHistoryCapabilityObservation {
    /// Observes only an exact daily, frequency-one, explicit-range price-history probe.
    pub fn try_observe(
        oauth_authority: SchwabOAuthAuthorityReceipt,
        user_preference_probe: &ExecutedRestResponse,
        price_history_probe: &ExecutedRestResponse,
        observed_at_unix_seconds: u64,
        valid_for: Duration,
    ) -> Result<Self, SchwabVerticalError> {
        let preference_receipt = user_preference_probe.capture().receipt();
        let history_receipt = price_history_probe.capture().receipt();
        let SchwabRestPayload::StreamerBootstrap(bootstrap) = user_preference_probe.payload()
        else {
            return Err(SchwabVerticalError::InvalidCapabilityEvidence);
        };
        let SchwabRestPayload::PriceHistory(history) = price_history_probe.payload() else {
            return Err(SchwabVerticalError::InvalidCapabilityEvidence);
        };
        let Some((_start, _end)) = exact_daily_range(history_receipt.request_url()) else {
            return Err(SchwabVerticalError::InvalidCapabilityEvidence);
        };
        let preference_accounting = user_preference_probe.accounting();
        let history_accounting = price_history_probe.accounting();
        let expires_at_unix_seconds = observed_at_unix_seconds
            .checked_add(valid_for.as_secs())
            .ok_or(SchwabVerticalError::Overflow)?;
        if valid_for.is_zero()
            || valid_for > Duration::from_secs(ACCESS_TOKEN_MAX_LIFETIME_SECONDS)
            || observed_at_unix_seconds < oauth_authority.access_issued_at_unix_seconds()
            || observed_at_unix_seconds >= oauth_authority.access_expires_at_unix_seconds()
            || expires_at_unix_seconds > oauth_authority.access_expires_at_unix_seconds()
            || oauth_authority.generation() != preference_receipt.token_generation()
            || oauth_authority.generation() != history_receipt.token_generation()
            || preference_receipt.route() != ReadOnlyRoute::UserPreference
            || history_receipt.route() != ReadOnlyRoute::PriceHistory
            || preference_receipt.token_generation() != history_receipt.token_generation()
            || preference_receipt.status() != 200
            || history_receipt.status() != 200
            || preference_receipt.body_sha256() != bootstrap.raw_sha256()
            || history_receipt.body_sha256() != history.raw_sha256()
            || bootstrap.value().market_data_principal_sha256() == [0; 32]
            || preference_accounting.requested != 1
            || preference_accounting.returned != 1
            || preference_accounting.missing != 0
            || preference_accounting.unexpected != 0
            || preference_accounting.provider_records != 1
            || history_accounting.requested != 1
            || history_accounting.returned != 1
            || history_accounting.missing != 0
            || history_accounting.unexpected != 0
            || history_accounting.provider_records == 0
            || history.value().empty
            || history.value().candles().is_empty()
            || u64::try_from(history.value().candles().len())
                .ok()
                .is_none_or(|count| count != history_accounting.provider_records)
            || preference_receipt.received_at_unix_millis()
                > history_receipt.received_at_unix_millis()
        {
            return Err(SchwabVerticalError::InvalidCapabilityEvidence);
        }
        validate_observed_clock(
            preference_receipt.received_at_unix_millis(),
            observed_at_unix_seconds,
            valid_for,
        )?;
        validate_observed_clock(
            history_receipt.received_at_unix_millis(),
            observed_at_unix_seconds,
            valid_for,
        )?;
        if expires_at_unix_seconds <= observed_at_unix_seconds {
            return Err(SchwabVerticalError::InvalidCapabilityEvidence);
        }
        let market_data_principal_sha256 = bootstrap.value().market_data_principal_sha256();
        let user_preference_receipt_sha256 = rest_receipt_digest(user_preference_probe);
        let price_history_receipt_sha256 = rest_receipt_digest(price_history_probe);
        let request_sha256 = history_receipt.request_sha256();
        let response_sha256 = history_receipt.body_sha256();
        let returned_candles = history_accounting.provider_records;
        let receipt_sha256 = capability_digest(
            observed_at_unix_seconds,
            expires_at_unix_seconds,
            oauth_authority,
            market_data_principal_sha256,
            user_preference_receipt_sha256,
            price_history_receipt_sha256,
            request_sha256,
            response_sha256,
            returned_candles,
        );
        Ok(Self {
            observed_at_unix_seconds,
            expires_at_unix_seconds,
            oauth_authority,
            market_data_principal_sha256,
            user_preference_receipt_sha256,
            price_history_receipt_sha256,
            request_sha256,
            response_sha256,
            returned_candles,
            receipt_sha256,
        })
    }

    pub const fn family(self) -> SchwabObservedCapabilityFamily {
        SchwabObservedCapabilityFamily::DailyPriceHistory
    }
    pub const fn receipt_sha256(self) -> [u8; 32] {
        self.receipt_sha256
    }
    pub const fn expires_at_unix_seconds(self) -> u64 {
        self.expires_at_unix_seconds
    }

    pub fn currentness(
        self,
        oauth_authority: SchwabOAuthAuthorityReceipt,
        user_preference_probe: &ExecutedRestResponse,
        price_history_probe: &ExecutedRestResponse,
        now_unix_seconds: u64,
    ) -> SchwabCapabilityCurrentness {
        if now_unix_seconds < self.observed_at_unix_seconds
            || now_unix_seconds >= self.expires_at_unix_seconds
            || now_unix_seconds < oauth_authority.access_issued_at_unix_seconds()
            || now_unix_seconds >= oauth_authority.access_expires_at_unix_seconds()
        {
            return SchwabCapabilityCurrentness::Expired;
        }
        let preference_receipt = user_preference_probe.capture().receipt();
        let receipt = price_history_probe.capture().receipt();
        if preference_receipt.token_generation() != oauth_authority.generation()
            || receipt.token_generation() != oauth_authority.generation()
        {
            return SchwabCapabilityCurrentness::TokenGenerationChanged;
        }
        if oauth_authority != self.oauth_authority {
            return SchwabCapabilityCurrentness::OAuthAuthorityChanged;
        }
        let SchwabRestPayload::StreamerBootstrap(bootstrap) = user_preference_probe.payload()
        else {
            return SchwabCapabilityCurrentness::BootstrapChanged;
        };
        if bootstrap.value().market_data_principal_sha256() != self.market_data_principal_sha256 {
            return SchwabCapabilityCurrentness::PrincipalChanged;
        }
        let accounting = user_preference_probe.accounting();
        if preference_receipt.route() != ReadOnlyRoute::UserPreference
            || preference_receipt.status() != 200
            || preference_receipt.body_sha256() != bootstrap.raw_sha256()
            || accounting.requested != 1
            || accounting.returned != 1
            || accounting.missing != 0
            || accounting.unexpected != 0
            || accounting.provider_records != 1
            || rest_receipt_digest(user_preference_probe) != self.user_preference_receipt_sha256
            || preference_receipt.received_at_unix_millis() > receipt.received_at_unix_millis()
        {
            return SchwabCapabilityCurrentness::BootstrapChanged;
        }
        if receipt.route() != ReadOnlyRoute::PriceHistory
            || receipt.request_sha256() != self.request_sha256
            || receipt.body_sha256() != self.response_sha256
            || rest_receipt_digest(price_history_probe) != self.price_history_receipt_sha256
        {
            return SchwabCapabilityCurrentness::ProbeChanged;
        }
        SchwabCapabilityCurrentness::Current
    }
}

/// Typed authority lineage retained for downstream manifest, PIT, and model consumers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchwabPriceHistoryLineage {
    pub capability: SchwabPriceHistoryCapabilityObservation,
    pub oauth_authority: SchwabOAuthAuthorityReceipt,
    pub capture_coordinates: SchwabCaptureCoordinates,
    pub user_preference_receipt: RawRestResponseReceipt,
    pub user_preference_observation_sha256: [u8; 32],
    pub instrument_revision_digest: EvidenceDigest,
    pub admitted_plan_digest: EvidenceDigest,
    pub provider_symbol: ProviderIdentifier,
    pub resolution_evidence: EvidenceDigest,
    pub time_semantics: Box<[BarTimeSemantics]>,
    pub ingested_at: Timestamp,
}

/// One exact Streamer microbatch inseparably bound to its durable raw-capture receipt.
///
/// Access-token bytes are absent by construction. The retained token generation is only the
/// opaque coordinate issued by the protected OAuth authority for the connection generation that
/// produced these frames.
#[derive(Debug, Eq, PartialEq)]
pub struct SchwabSealedStreamerMicrobatchCapture {
    coordinates: SchwabCaptureCoordinates,
    microbatch_receipt: StreamerMicrobatchReceipt,
    receipt: SealedProviderCaptureSetReceipt,
}

impl SchwabSealedStreamerMicrobatchCapture {
    /// Consumes and seals one already bounded Streamer microbatch.
    pub fn try_seal(
        microbatch: StreamerMicrobatch,
        coordinates: SchwabCaptureCoordinates,
        event_ids: Vec<uuid::Uuid>,
        store: &SealedResearchJournalStore,
    ) -> Result<Self, SchwabVerticalError> {
        let microbatch_receipt = microbatch.receipt().clone();
        let frame_count = usize::try_from(microbatch_receipt.frame_count())
            .map_err(|_| SchwabVerticalError::StreamerCaptureBindingMismatch)?;
        if frame_count == 0
            || frame_count > market_squawk_sources::MAX_PROVIDER_CAPTURE_PAGES
            || event_ids.len() != frame_count
        {
            return Err(SchwabVerticalError::StreamerCaptureBindingMismatch);
        }
        if event_ids
            .iter()
            .enumerate()
            .any(|(index, event_id)| event_ids[..index].contains(event_id))
        {
            return Err(SchwabVerticalError::StreamerCaptureBindingMismatch);
        }
        let material = microbatch
            .try_into_provider_capture_material(coordinates.clone(), event_ids)
            .map_err(|_| SchwabVerticalError::StreamerCaptureBindingMismatch)?;
        let receipt = material
            .seal(store)
            .map_err(|_| SchwabVerticalError::StreamerCaptureBindingMismatch)?;
        Ok(Self {
            coordinates,
            microbatch_receipt,
            receipt,
        })
    }

    pub const fn coordinates(&self) -> &SchwabCaptureCoordinates {
        &self.coordinates
    }

    pub const fn microbatch_receipt(&self) -> &StreamerMicrobatchReceipt {
        &self.microbatch_receipt
    }

    pub const fn receipt(&self) -> &SealedProviderCaptureSetReceipt {
        &self.receipt
    }
}

/// An exact daily price-history response sealed with the registered capture coordinates.
///
/// The private fields make the capture receipt and the coordinates used to create its raw record
/// inseparable. Publication still requires the independently registered coordinates and compares
/// them with this evidence before admitting the capture.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchwabSealedPriceHistoryCapture {
    coordinates: SchwabCaptureCoordinates,
    receipt: SealedProviderCaptureSetReceipt,
}

impl SchwabSealedPriceHistoryCapture {
    pub fn try_seal(
        response: &ExecutedRestResponse,
        coordinates: SchwabCaptureCoordinates,
        event_id: uuid::Uuid,
        store: &SealedResearchJournalStore,
    ) -> Result<Self, SchwabVerticalError> {
        admitted_daily_range(response)?;
        if !matches!(response.payload(), SchwabRestPayload::PriceHistory(_)) {
            return Err(SchwabVerticalError::PublicationBindingMismatch);
        }
        let material = response
            .capture()
            .clone()
            .try_into_provider_capture_material(coordinates.clone(), event_id)
            .map_err(|_| SchwabVerticalError::PublicationBindingMismatch)?;
        let receipt = material
            .seal(store)
            .map_err(|_| SchwabVerticalError::PublicationBindingMismatch)?;
        Ok(Self {
            coordinates,
            receipt,
        })
    }

    pub const fn coordinates(&self) -> &SchwabCaptureCoordinates {
        &self.coordinates
    }

    pub const fn receipt(&self) -> &SealedProviderCaptureSetReceipt {
        &self.receipt
    }
}

/// Adapter-owned immutable input for the shared daily-history Parquet/manifest authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchwabSealedPriceHistoryPublicationInput {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    dataset: SourceIdentifier,
    requested_start: Timestamp,
    requested_end: Timestamp,
    instrument_id: InstrumentId,
    provider_instrument_id: ProviderInstrumentId,
    venue_id: VenueId,
    feed: SourceIdentifier,
    interval: SourceIdentifier,
    adjustment: MarketBarAdjustment,
    currency: Currency,
    completeness_evidence: EvidenceDigest,
    expected_provider_timestamps: Box<[Timestamp]>,
    lineage: SchwabPriceHistoryLineage,
    canonical_digest: EvidenceDigest,
    published_at: Timestamp,
    capture: SealedProviderCaptureSetReceipt,
    bars: Box<[MarketBarObservation]>,
}

impl SchwabSealedPriceHistoryPublicationInput {
    #[allow(
        clippy::too_many_arguments,
        reason = "all history authority stays explicit"
    )]
    pub(crate) fn try_new(
        capability: SchwabPriceHistoryCapabilityObservation,
        oauth_authority: SchwabOAuthAuthorityReceipt,
        user_preference_probe: &ExecutedRestResponse,
        price_history_probe: &ExecutedRestResponse,
        capture: SchwabSealedPriceHistoryCapture,
        capture_coordinates: SchwabCaptureCoordinates,
        requested_start: Timestamp,
        requested_end: Timestamp,
        instrument_id: InstrumentId,
        instrument_revision_digest: EvidenceDigest,
        admitted_plan_digest: EvidenceDigest,
        provider_symbol: ProviderIdentifier,
        provider_instrument_id: ProviderInstrumentId,
        resolution_evidence: EvidenceDigest,
        venue_id: VenueId,
        feed: SourceIdentifier,
        interval: SourceIdentifier,
        adjustment: MarketBarAdjustment,
        currency: Currency,
        completeness_evidence: EvidenceDigest,
        expected_provider_timestamps: Vec<Timestamp>,
        time_semantics: &[BarTimeSemantics],
        ingested_at: Timestamp,
        published_at: Timestamp,
        bars: Vec<MarketBarObservation>,
    ) -> Result<Self, SchwabVerticalError> {
        let receipt = price_history_probe.capture().receipt();
        let sealed = capture.receipt.capture();
        let pages = sealed.pages();
        let Some(page) = pages.first() else {
            return Err(SchwabVerticalError::PublicationBindingMismatch);
        };
        let frames = capture.receipt.segment().frames();
        let Some(frame) = frames.first() else {
            return Err(SchwabVerticalError::PublicationBindingMismatch);
        };
        let published_seconds = u64::try_from(published_at.unix_nanos())
            .ok()
            .map(|value| value / 1_000_000_000)
            .ok_or(SchwabVerticalError::PublicationBindingMismatch)?;
        let received_nanos = i64::try_from(receipt.received_at_unix_millis())
            .ok()
            .and_then(|value| value.checked_mul(1_000_000))
            .ok_or(SchwabVerticalError::PublicationBindingMismatch)?;
        if capability.currentness(
            oauth_authority,
            user_preference_probe,
            price_history_probe,
            published_seconds,
        ) != SchwabCapabilityCurrentness::Current
            || requested_start >= requested_end
            || pages.len() != 1
            || frames.len() != 1
            || sealed.terminal() != ProviderCaptureTerminalDisposition::StandaloneResponse
            || capture.coordinates != capture_coordinates
            || sealed.source_id() != capture_coordinates.source_id()
            || sealed.metadata_revision() != capture_coordinates.metadata_revision()
            || sealed.dataset() != capture_coordinates.dataset()
            || sealed.request_set_identity().bytes() != receipt.request_sha256()
            || page.request_identity().bytes() != receipt.request_sha256()
            || page.http_status() != receipt.status()
            || page.body_bytes() != receipt.body_bytes()
            || page.body_digest().bytes() != receipt.body_sha256()
            || page.received_at().unix_nanos() != received_nanos
            || frame.ordinal() != 0
            || frame.provider_payload_bytes() != receipt.body_bytes()
            || frame.provider_payload_digest().bytes() != receipt.body_sha256()
            || frame.received_at().unix_nanos() != received_nanos
            || frame.source_sequence() != Some(0)
            || expected_provider_timestamps.is_empty()
            || expected_provider_timestamps.len() != bars.len()
            || expected_provider_timestamps.len() != time_semantics.len()
            || bars.is_empty()
            || completeness_evidence.bytes() == [0; 32]
            || instrument_revision_digest.bytes() == [0; 32]
            || admitted_plan_digest.bytes() == [0; 32]
            || resolution_evidence.bytes() == [0; 32]
            || ingested_at < page.received_at()
            || published_at < ingested_at
        {
            return Err(SchwabVerticalError::PublicationBindingMismatch);
        }
        let source_id = sealed.source_id().clone();
        let metadata_revision = sealed.metadata_revision().clone();
        let dataset = sealed.dataset().clone();
        let capture = capture.receipt;
        let lineage = SchwabPriceHistoryLineage {
            capability,
            oauth_authority,
            capture_coordinates,
            user_preference_receipt: user_preference_probe.capture().receipt().clone(),
            user_preference_observation_sha256: rest_receipt_digest(user_preference_probe),
            instrument_revision_digest,
            admitted_plan_digest,
            provider_symbol,
            resolution_evidence,
            time_semantics: time_semantics.to_vec().into_boxed_slice(),
            ingested_at,
        };
        let wire = PublicationDigestWire {
            version: 1,
            family: "schwab.daily-price-history",
            capability_receipt_sha256: capability.receipt_sha256,
            oauth_generation: lineage.oauth_authority.generation().get(),
            oauth_access_issued_at_unix_seconds: lineage
                .oauth_authority
                .access_issued_at_unix_seconds(),
            oauth_access_expires_at_unix_seconds: lineage
                .oauth_authority
                .access_expires_at_unix_seconds(),
            oauth_refresh_authorized_at_unix_seconds: lineage
                .oauth_authority
                .refresh_authorized_at_unix_seconds(),
            oauth_refresh_expires_at_unix_seconds: lineage
                .oauth_authority
                .refresh_expires_at_unix_seconds(),
            registered_source_id: lineage.capture_coordinates.source_id(),
            registered_metadata_revision: lineage.capture_coordinates.metadata_revision(),
            registered_dataset: lineage.capture_coordinates.dataset(),
            registered_connection_id: lineage.capture_coordinates.connection_id(),
            request_url: receipt.request_url(),
            request_sha256: receipt.request_sha256(),
            response_sha256: receipt.body_sha256(),
            response_bytes: receipt.body_bytes(),
            response_received_at_unix_millis: receipt.received_at_unix_millis(),
            sealed_capture: &capture,
            source_id: &source_id,
            metadata_revision: &metadata_revision,
            dataset: &dataset,
            requested_start,
            requested_end,
            instrument_id,
            instrument_revision_digest,
            admitted_plan_digest,
            user_preference_observation_sha256: lineage.user_preference_observation_sha256,
            provider_symbol: lineage.provider_symbol.as_str(),
            provider_instrument_id: &provider_instrument_id,
            resolution_evidence,
            venue_id: &venue_id,
            feed: &feed,
            interval: &interval,
            adjustment,
            currency,
            completeness_evidence,
            expected_provider_timestamps: &expected_provider_timestamps,
            time_semantics: &lineage.time_semantics,
            ingested_at,
            published_at,
            bars: &bars,
        };
        let encoded = serde_json::to_vec(&wire)
            .map_err(|_| SchwabVerticalError::PublicationBindingMismatch)?;
        let canonical_digest = EvidenceDigest::new(
            market_squawk_domain::DigestAlgorithm::Sha256,
            Sha256::digest(encoded).into(),
        );
        Ok(Self {
            source_id,
            metadata_revision,
            dataset,
            requested_start,
            requested_end,
            instrument_id,
            provider_instrument_id,
            venue_id,
            feed,
            interval,
            adjustment,
            currency,
            completeness_evidence,
            expected_provider_timestamps: expected_provider_timestamps.into_boxed_slice(),
            lineage,
            canonical_digest,
            published_at,
            capture,
            bars: bars.into_boxed_slice(),
        })
    }

    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }
    pub const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }
    pub const fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }
    pub const fn requested_range(&self) -> (Timestamp, Timestamp) {
        (self.requested_start, self.requested_end)
    }
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
    pub const fn provider_instrument_id(&self) -> &ProviderInstrumentId {
        &self.provider_instrument_id
    }
    pub const fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }
    pub const fn feed(&self) -> &SourceIdentifier {
        &self.feed
    }
    pub const fn interval(&self) -> &SourceIdentifier {
        &self.interval
    }
    pub const fn adjustment(&self) -> MarketBarAdjustment {
        self.adjustment
    }
    pub const fn currency(&self) -> Currency {
        self.currency
    }
    pub const fn completeness_evidence(&self) -> EvidenceDigest {
        self.completeness_evidence
    }
    pub fn expected_provider_timestamps(&self) -> &[Timestamp] {
        &self.expected_provider_timestamps
    }
    pub const fn lineage(&self) -> &SchwabPriceHistoryLineage {
        &self.lineage
    }
    pub const fn canonical_digest(&self) -> EvidenceDigest {
        self.canonical_digest
    }
    pub const fn published_at(&self) -> Timestamp {
        self.published_at
    }
    pub const fn capture(&self) -> &SealedProviderCaptureSetReceipt {
        &self.capture
    }
    pub fn bars(&self) -> &[MarketBarObservation] {
        &self.bars
    }
}

#[derive(Serialize)]
struct PublicationDigestWire<'a> {
    version: u16,
    family: &'static str,
    capability_receipt_sha256: [u8; 32],
    oauth_generation: u64,
    oauth_access_issued_at_unix_seconds: u64,
    oauth_access_expires_at_unix_seconds: u64,
    oauth_refresh_authorized_at_unix_seconds: u64,
    oauth_refresh_expires_at_unix_seconds: u64,
    registered_source_id: &'a SourceId,
    registered_metadata_revision: &'a MetadataRevision,
    registered_dataset: &'a SourceIdentifier,
    registered_connection_id: uuid::Uuid,
    user_preference_observation_sha256: [u8; 32],
    request_url: &'a str,
    request_sha256: [u8; 32],
    response_sha256: [u8; 32],
    response_bytes: u64,
    response_received_at_unix_millis: u64,
    sealed_capture: &'a SealedProviderCaptureSetReceipt,
    source_id: &'a SourceId,
    metadata_revision: &'a MetadataRevision,
    dataset: &'a SourceIdentifier,
    requested_start: Timestamp,
    requested_end: Timestamp,
    instrument_id: InstrumentId,
    instrument_revision_digest: EvidenceDigest,
    admitted_plan_digest: EvidenceDigest,
    provider_symbol: &'a str,
    provider_instrument_id: &'a ProviderInstrumentId,
    resolution_evidence: EvidenceDigest,
    venue_id: &'a VenueId,
    feed: &'a SourceIdentifier,
    interval: &'a SourceIdentifier,
    adjustment: MarketBarAdjustment,
    currency: Currency,
    completeness_evidence: EvidenceDigest,
    expected_provider_timestamps: &'a [Timestamp],
    time_semantics: &'a [BarTimeSemantics],
    ingested_at: Timestamp,
    published_at: Timestamp,
    bars: &'a [MarketBarObservation],
}

fn exact_daily_range(url: &str) -> Option<(Timestamp, Timestamp)> {
    let url = url::Url::parse(url).ok()?;
    let mut frequency_type = None;
    let mut frequency = None;
    let mut start = None;
    let mut end = None;
    let mut has_period_type = false;
    let mut has_period = false;
    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "frequencyType" => frequency_type = Some(value.into_owned()),
            "frequency" => frequency = Some(value.into_owned()),
            "startDate" => start = value.parse::<u64>().ok(),
            "endDate" => end = value.parse::<u64>().ok(),
            "periodType" => has_period_type = true,
            "period" => has_period = true,
            _ => {}
        }
    }
    if frequency_type.as_deref() != Some("daily")
        || frequency.as_deref() != Some("1")
        || has_period_type
        || has_period
    {
        return None;
    }
    let start = start?;
    let end = end?;
    if start >= end {
        return None;
    }
    Some((millis_timestamp(start)?, millis_timestamp(end)?))
}

pub(crate) fn admitted_daily_range(
    response: &ExecutedRestResponse,
) -> Result<(Timestamp, Timestamp), SchwabVerticalError> {
    if response.capture().receipt().route() != ReadOnlyRoute::PriceHistory {
        return Err(SchwabVerticalError::InvalidCapabilityEvidence);
    }
    exact_daily_range(response.capture().receipt().request_url())
        .ok_or(SchwabVerticalError::InvalidCapabilityEvidence)
}

fn millis_timestamp(value: u64) -> Option<Timestamp> {
    i64::try_from(value)
        .ok()
        .and_then(|value| value.checked_mul(1_000_000))
        .map(Timestamp::from_unix_nanos)
}

fn validate_observed_clock(
    received_at_unix_millis: u64,
    observed_at_unix_seconds: u64,
    valid_for: Duration,
) -> Result<(), SchwabVerticalError> {
    let ceiling = observed_at_unix_seconds
        .checked_add(1)
        .and_then(|value| value.checked_mul(1_000))
        .and_then(|value| value.checked_sub(1))
        .ok_or(SchwabVerticalError::Overflow)?;
    let maximum_age =
        u64::try_from(valid_for.as_millis()).map_err(|_| SchwabVerticalError::Overflow)?;
    if received_at_unix_millis > ceiling || ceiling - received_at_unix_millis > maximum_age {
        return Err(SchwabVerticalError::InvalidCapabilityEvidence);
    }
    Ok(())
}

fn rest_receipt_digest(response: &ExecutedRestResponse) -> [u8; 32] {
    let receipt = response.capture().receipt();
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk/schwab-rest-observation/v1");
    hasher.update([route_tag(receipt.route())]);
    hasher.update(receipt.token_generation().get().to_be_bytes());
    hasher.update(receipt.request_sha256());
    hasher.update(receipt.status().to_be_bytes());
    hasher.update(receipt.received_at_unix_millis().to_be_bytes());
    hasher.update(receipt.body_bytes().to_be_bytes());
    hasher.update(receipt.body_sha256());
    let accounting = response.accounting();
    for value in [
        accounting.requested,
        accounting.returned,
        accounting.missing,
        accounting.unexpected,
        accounting.provider_records,
    ] {
        hasher.update(value.to_be_bytes());
    }
    hasher.finalize().into()
}

#[allow(
    clippy::too_many_arguments,
    reason = "all observed evidence stays explicit"
)]
fn capability_digest(
    observed_at: u64,
    expires_at: u64,
    oauth_authority: SchwabOAuthAuthorityReceipt,
    principal: [u8; 32],
    preference_receipt: [u8; 32],
    history_receipt: [u8; 32],
    request: [u8; 32],
    response: [u8; 32],
    returned_candles: u64,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk/schwab-daily-price-history-capability/v1");
    hasher.update(observed_at.to_be_bytes());
    hasher.update(expires_at.to_be_bytes());
    hasher.update(oauth_authority.generation().get().to_be_bytes());
    hasher.update(
        oauth_authority
            .access_issued_at_unix_seconds()
            .to_be_bytes(),
    );
    hasher.update(
        oauth_authority
            .access_expires_at_unix_seconds()
            .to_be_bytes(),
    );
    hasher.update(
        oauth_authority
            .refresh_authorized_at_unix_seconds()
            .to_be_bytes(),
    );
    hasher.update(
        oauth_authority
            .refresh_expires_at_unix_seconds()
            .to_be_bytes(),
    );
    hasher.update(principal);
    hasher.update(preference_receipt);
    hasher.update(history_receipt);
    hasher.update(request);
    hasher.update(response);
    hasher.update(returned_candles.to_be_bytes());
    hasher.finalize().into()
}

const fn route_tag(route: ReadOnlyRoute) -> u8 {
    match route {
        ReadOnlyRoute::Quotes => 1,
        ReadOnlyRoute::SingleQuote => 2,
        ReadOnlyRoute::Chains => 3,
        ReadOnlyRoute::ExpirationChain => 4,
        ReadOnlyRoute::PriceHistory => 5,
        ReadOnlyRoute::Movers => 6,
        ReadOnlyRoute::Markets => 7,
        ReadOnlyRoute::SingleMarket => 8,
        ReadOnlyRoute::Instruments => 9,
        ReadOnlyRoute::InstrumentByCusip => 10,
        ReadOnlyRoute::UserPreference => 11,
    }
}

/// Secret-free provider-vertical failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum SchwabVerticalError {
    #[error("Schwab family capability evidence is incomplete or inconsistent")]
    InvalidCapabilityEvidence,
    #[error("Schwab daily price-history response, capture, canonical records, or clocks differ")]
    PublicationBindingMismatch,
    #[error("Schwab Streamer microbatch, event coordinates, or sealed receipt differ")]
    StreamerCaptureBindingMismatch,
    #[error("Schwab provider evidence arithmetic overflowed")]
    Overflow,
}
