//! Family-scoped observed capability evidence.
//!
//! A successful request from one Schwab family never authorizes another family. Raw market-data
//! capture is handed to the shared provider-capture authority; this module retains only the
//! observations needed to qualify the actual requested response.

use std::fmt;
use std::num::NonZeroU64;

use market_squawk_domain::{
    DataQuality, DigestAlgorithm, EvidenceDigest, MarketDepth, ProviderChannel, ProviderProduct,
    SourceIdentifier, Timestamp,
};
use market_squawk_sources::{RuntimeCapabilityDisposition, SchwabMarketDataFamily};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::{
    AccessTokenGeneration, ConnectionGeneration, ExecutedRestResponse, MarketDataService,
    ReadOnlyRoute, SchwabCredentialAuthorityBinding, SchwabOAuthAuthorityReceipt,
    SchwabRestPayload, SchwabSealedStreamerCapture, SchwabStreamerServiceResponseEvidence,
};

/// One observed read-only Schwab market-data family.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchwabObservedCapabilityFamily {
    Quotes,
    OptionChain,
    ExpirationChain,
    DailyPriceHistory,
    MarketHours,
    Movers,
    Instruments,
    Streamer(MarketDataService),
}

/// Exact delivery timing established by the current family qualification.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "nanoseconds")]
pub enum SchwabMarketDataDelay {
    RealTime,
    Delayed(NonZeroU64),
    Unknown,
}

/// Exact market depth established by the current family qualification.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SchwabMarketDataDepth {
    TopOfBook,
    PriceLevel,
    NotReported,
}

impl SchwabMarketDataDepth {
    pub(crate) const fn canonical(self) -> Option<MarketDepth> {
        match self {
            Self::TopOfBook => Some(MarketDepth::TopOfBook),
            Self::PriceLevel => Some(MarketDepth::PriceLevel),
            Self::NotReported => None,
        }
    }
}

/// Opaque qualification of one actual response under configured rights and current OAuth.
///
/// Callers can retain or clone this proof, but cannot manufacture any of its market semantics or
/// receipt digests independently.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchwabMarketDataQualification {
    family: SchwabMarketDataFamily,
    disposition: RuntimeCapabilityDisposition,
    response_observed_at: Timestamp,
    family_observed_at: Timestamp,
    token_generation: AccessTokenGeneration,
    credential_authority: SchwabCredentialAuthorityBinding,
    session_identifier: SourceIdentifier,
    market_data_principal_sha256: Option<EvidenceDigest>,
    request_evidence: Option<EvidenceDigest>,
    receipt_evidence: EvidenceDigest,
    observation_evidence: EvidenceDigest,
    disposition_evidence: EvidenceDigest,
    entitlement_evidence: EvidenceDigest,
    capability_evidence: EvidenceDigest,
    feed: SourceIdentifier,
    depth: SchwabMarketDataDepth,
    delay: SchwabMarketDataDelay,
    quality: DataQuality,
    provider_product: ProviderProduct,
    provider_channel: ProviderChannel,
}

impl SchwabMarketDataQualification {
    /// Qualifies this actual response, without requiring a prior family probe.
    pub fn try_from_rest_response(
        response: &ExecutedRestResponse,
        oauth_authority: SchwabOAuthAuthorityReceipt,
        session_identifier: SourceIdentifier,
        entitlement_evidence: EvidenceDigest,
        capability_evidence: EvidenceDigest,
    ) -> Result<Self, SchwabVerticalError> {
        Self::try_from_rest_parts(
            response.capture().receipt(),
            response.payload(),
            response.accounting(),
            oauth_authority,
            session_identifier,
            entitlement_evidence,
            capability_evidence,
        )
    }

    /// Qualifies the original already sealed response using the same response validation.
    pub fn try_from_sealed_rest_response(
        response: &crate::SchwabSealedRestResponse,
        oauth_authority: SchwabOAuthAuthorityReceipt,
        session_identifier: SourceIdentifier,
        entitlement_evidence: EvidenceDigest,
        capability_evidence: EvidenceDigest,
    ) -> Result<Self, SchwabVerticalError> {
        let parts = response.parts();
        Self::try_from_rest_parts(
            &parts.receipt,
            &parts.payload,
            parts.accounting,
            oauth_authority,
            session_identifier,
            entitlement_evidence,
            capability_evidence,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "original response and configured authority remain explicit"
    )]
    fn try_from_rest_parts(
        receipt: &crate::RawRestResponseReceipt,
        payload: &SchwabRestPayload,
        accounting: crate::RestItemAccounting,
        oauth_authority: SchwabOAuthAuthorityReceipt,
        session_identifier: SourceIdentifier,
        entitlement_evidence: EvidenceDigest,
        capability_evidence: EvidenceDigest,
    ) -> Result<Self, SchwabVerticalError> {
        let family = match (receipt.route(), payload) {
            (ReadOnlyRoute::Quotes | ReadOnlyRoute::SingleQuote, SchwabRestPayload::Quotes(_)) => {
                SchwabMarketDataFamily::Quotes
            }
            (ReadOnlyRoute::Chains, SchwabRestPayload::OptionChain(_)) => {
                SchwabMarketDataFamily::OptionChains
            }
            (ReadOnlyRoute::ExpirationChain, SchwabRestPayload::Expirations(_)) => {
                SchwabMarketDataFamily::ExpirationChains
            }
            (ReadOnlyRoute::PriceHistory, SchwabRestPayload::PriceHistory(_)) => {
                SchwabMarketDataFamily::PriceHistory
            }
            (
                ReadOnlyRoute::Markets | ReadOnlyRoute::SingleMarket,
                SchwabRestPayload::MarketHours(_),
            ) => SchwabMarketDataFamily::MarketHours,
            (ReadOnlyRoute::Movers, SchwabRestPayload::Movers(_)) => SchwabMarketDataFamily::Movers,
            (
                ReadOnlyRoute::Instruments | ReadOnlyRoute::InstrumentByCusip,
                SchwabRestPayload::Instruments(_),
            ) => SchwabMarketDataFamily::Instruments,
            _ => return Err(SchwabVerticalError::InvalidCapabilityEvidence),
        };
        let response_observed_at = millis_timestamp(receipt.received_at_unix_millis())
            .ok_or(SchwabVerticalError::Overflow)?;
        validate_qualification_authority(
            oauth_authority,
            response_observed_at,
            entitlement_evidence,
            capability_evidence,
        )?;
        if receipt.status() != 200
            || receipt.token_generation() != oauth_authority.generation()
            || receipt.credential_authority() != oauth_authority.credential_authority()
            || receipt.body_sha256() != payload.raw_sha256()
            || accounting.requested == 0
            || accounting.returned == 0
            || accounting.provider_records == 0
        {
            return Err(SchwabVerticalError::InvalidCapabilityEvidence);
        }
        let disposition = if accounting.missing == 0 && accounting.unexpected == 0 {
            RuntimeCapabilityDisposition::Available
        } else {
            RuntimeCapabilityDisposition::Degraded
        };
        // Real-time status belongs to this response. Missing/false status proves no delay duration.
        let delay = match payload {
            SchwabRestPayload::Quotes(quotes)
                if !quotes.value().quotes().is_empty()
                    && quotes.value().quotes().iter().all(|quote| {
                        matches!(quote.realtime(), crate::NativeField::Value(true))
                    }) =>
            {
                SchwabMarketDataDelay::RealTime
            }
            _ => SchwabMarketDataDelay::Unknown,
        };
        let (product, channel, depth) = qualification_semantics(family);
        let feed = SourceIdentifier::try_from(channel)
            .map_err(|_| SchwabVerticalError::InvalidCapabilityEvidence)?;
        let receipt_evidence =
            EvidenceDigest::new(DigestAlgorithm::Sha256, receipt_digest(receipt, accounting));
        let observation_evidence =
            EvidenceDigest::new(DigestAlgorithm::Sha256, receipt.body_sha256());
        let request_evidence =
            EvidenceDigest::new(DigestAlgorithm::Sha256, receipt.request_sha256());
        for digest in [receipt_evidence, observation_evidence, request_evidence] {
            require_qualification_digest(digest)?;
        }
        Ok(Self {
            family,
            disposition,
            response_observed_at,
            family_observed_at: response_observed_at,
            token_generation: oauth_authority.generation(),
            credential_authority: oauth_authority.credential_authority(),
            session_identifier,
            market_data_principal_sha256: None,
            request_evidence: Some(request_evidence),
            receipt_evidence,
            observation_evidence,
            disposition_evidence: receipt_evidence,
            entitlement_evidence,
            capability_evidence,
            provider_product: ProviderProduct::new(
                SourceIdentifier::try_from(product)
                    .map_err(|_| SchwabVerticalError::InvalidCapabilityEvidence)?,
            ),
            provider_channel: ProviderChannel::new(feed.clone()),
            feed,
            depth,
            delay,
            quality: DataQuality::DirectUnverified,
        })
    }

    /// Qualifies only the service proved by this original sealed SUBS acknowledgement and data.
    pub fn try_from_streamer_handoff(
        handoff: &SchwabStreamerFamilyDoctorHandoff,
        response_observed_at: Timestamp,
        oauth_authority: SchwabOAuthAuthorityReceipt,
        session_identifier: SourceIdentifier,
        entitlement_evidence: EvidenceDigest,
        capability_evidence: EvidenceDigest,
    ) -> Result<Self, SchwabVerticalError> {
        let family = match handoff.service() {
            MarketDataService::LevelOneEquities => SchwabMarketDataFamily::LevelOneEquities,
            MarketDataService::LevelOneOptions => SchwabMarketDataFamily::LevelOneOptions,
            MarketDataService::LevelOneFutures => SchwabMarketDataFamily::LevelOneFutures,
            MarketDataService::LevelOneFuturesOptions => {
                SchwabMarketDataFamily::LevelOneFuturesOptions
            }
            MarketDataService::LevelOneForex => SchwabMarketDataFamily::LevelOneForex,
            MarketDataService::NyseBook => SchwabMarketDataFamily::NyseBook,
            MarketDataService::NasdaqBook => SchwabMarketDataFamily::NasdaqBook,
            MarketDataService::OptionsBook => SchwabMarketDataFamily::OptionsBook,
            MarketDataService::ChartEquity => SchwabMarketDataFamily::ChartEquity,
            MarketDataService::ChartFutures => SchwabMarketDataFamily::ChartFutures,
            MarketDataService::ScreenerEquity => SchwabMarketDataFamily::ScreenerEquity,
            MarketDataService::ScreenerOption => SchwabMarketDataFamily::ScreenerOption,
        };
        let credential_authority = oauth_authority.credential_authority();
        validate_qualification_authority(
            oauth_authority,
            response_observed_at,
            entitlement_evidence,
            capability_evidence,
        )?;
        validate_qualification_authority(
            oauth_authority,
            handoff.observed_at,
            entitlement_evidence,
            capability_evidence,
        )?;
        if handoff.token_generation() != oauth_authority.generation()
            || handoff.credential_authority() != credential_authority
            || handoff.session_identifier() != &session_identifier
            || handoff.observed_at > response_observed_at
            || handoff.provider_records() == 0
        {
            return Err(SchwabVerticalError::InvalidCapabilityEvidence);
        }
        for digest in [
            entitlement_evidence,
            capability_evidence,
            handoff.market_data_principal_sha256(),
            handoff.capture_set_sha256(),
        ] {
            require_qualification_digest(digest)?;
        }
        let (product, channel, depth) = qualification_semantics(family);
        let feed = SourceIdentifier::try_from(channel)
            .map_err(|_| SchwabVerticalError::InvalidCapabilityEvidence)?;
        Ok(Self {
            family,
            disposition: RuntimeCapabilityDisposition::Degraded,
            response_observed_at,
            family_observed_at: handoff.observed_at,
            token_generation: oauth_authority.generation(),
            credential_authority,
            session_identifier,
            market_data_principal_sha256: Some(handoff.market_data_principal_sha256()),
            request_evidence: None,
            receipt_evidence: handoff.capture_set_sha256(),
            observation_evidence: handoff.capture_set_sha256(),
            disposition_evidence: handoff.capture_set_sha256(),
            entitlement_evidence,
            capability_evidence,
            provider_product: ProviderProduct::new(
                SourceIdentifier::try_from(product)
                    .map_err(|_| SchwabVerticalError::InvalidCapabilityEvidence)?,
            ),
            provider_channel: ProviderChannel::new(feed.clone()),
            feed,
            depth,
            delay: SchwabMarketDataDelay::Unknown,
            quality: DataQuality::DirectUnverified,
        })
    }

    pub const fn family(&self) -> SchwabMarketDataFamily {
        self.family
    }
    pub const fn disposition(&self) -> RuntimeCapabilityDisposition {
        self.disposition
    }
    pub const fn response_observed_at(&self) -> Timestamp {
        self.response_observed_at
    }
    pub const fn family_observed_at(&self) -> Timestamp {
        self.family_observed_at
    }
    pub const fn token_generation(&self) -> AccessTokenGeneration {
        self.token_generation
    }
    pub const fn credential_authority(&self) -> SchwabCredentialAuthorityBinding {
        self.credential_authority
    }
    pub const fn session_identifier(&self) -> &SourceIdentifier {
        &self.session_identifier
    }
    pub const fn market_data_principal_sha256(&self) -> Option<EvidenceDigest> {
        self.market_data_principal_sha256
    }
    pub const fn receipt_evidence(&self) -> EvidenceDigest {
        self.receipt_evidence
    }
    pub const fn observation_evidence(&self) -> EvidenceDigest {
        self.observation_evidence
    }
    pub const fn disposition_evidence(&self) -> EvidenceDigest {
        self.disposition_evidence
    }
    pub const fn entitlement_evidence(&self) -> EvidenceDigest {
        self.entitlement_evidence
    }
    pub const fn capability_evidence(&self) -> EvidenceDigest {
        self.capability_evidence
    }
    pub const fn feed(&self) -> &SourceIdentifier {
        &self.feed
    }
    pub const fn depth(&self) -> SchwabMarketDataDepth {
        self.depth
    }
    pub const fn delay(&self) -> SchwabMarketDataDelay {
        self.delay
    }
    pub const fn quality(&self) -> DataQuality {
        self.quality
    }
    pub const fn provider_product(&self) -> &ProviderProduct {
        &self.provider_product
    }
    pub const fn provider_channel(&self) -> &ProviderChannel {
        &self.provider_channel
    }
    pub const fn rest_service(&self) -> Option<&'static str> {
        match self.family {
            SchwabMarketDataFamily::Quotes
            | SchwabMarketDataFamily::PriceHistory
            | SchwabMarketDataFamily::OptionChains
            | SchwabMarketDataFamily::ExpirationChains
            | SchwabMarketDataFamily::Movers
            | SchwabMarketDataFamily::MarketHours
            | SchwabMarketDataFamily::Instruments => Some("schwab-market-data-rest"),
            _ => None,
        }
    }
    pub const fn streamer_service(&self) -> Option<MarketDataService> {
        family_streamer_service(self.family)
    }

    pub(crate) fn validates_rest_receipt(
        &self,
        family: SchwabMarketDataFamily,
        receipt: &crate::RawRestResponseReceipt,
    ) -> bool {
        self.family == family
            && self.rest_service().is_some()
            && receipt.status() == 200
            && self.observation_evidence.bytes() == receipt.body_sha256()
            && self
                .request_evidence
                .is_some_and(|digest| digest.bytes() == receipt.request_sha256())
            && receipt.token_generation() == self.token_generation
            && receipt.credential_authority() == self.credential_authority
            && millis_timestamp(receipt.received_at_unix_millis())
                .is_some_and(|received_at| received_at == self.response_observed_at)
    }

    pub(crate) fn validates_streamer_publication_coordinate(
        &self,
        service: MarketDataService,
        handoff: &SchwabStreamerFamilyDoctorHandoff,
        capture: &SchwabSealedStreamerCapture,
        frame_ordinal: u16,
        data_batch_ordinal: u16,
        content_ordinal: u16,
    ) -> bool {
        let receipt = capture.streamer_receipt();
        let last_ack_ordinal = handoff
            .capture_frame_ordinals(handoff.capture_count().saturating_sub(1))
            .map(|(_, last)| last);
        let coordinate = usize::from(frame_ordinal);
        let Some((frame, Some(parsed))) = capture
            .frames()
            .get(coordinate)
            .zip(capture.parsed_frames().get(coordinate))
        else {
            return false;
        };
        let Some(batch) = parsed.value().data.get(usize::from(data_batch_ordinal)) else {
            return false;
        };
        self.streamer_service() == Some(service)
            && handoff.service() == service
            && handoff.token_generation() == self.token_generation
            && handoff.credential_authority() == self.credential_authority
            && handoff.session_identifier() == &self.session_identifier
            && Some(handoff.market_data_principal_sha256()) == self.market_data_principal_sha256
            && receipt.token_generation() == self.token_generation
            && receipt.credential_authority() == self.credential_authority
            && receipt.session_identifier() == &self.session_identifier
            && Some(receipt.market_data_principal_sha256()) == self.market_data_principal_sha256
            && handoff.generation() == receipt.generation()
            && (last_ack_ordinal.is_some_and(|last| frame.transport_ordinal() > last)
                || handoff
                    .initial_publication
                    .as_ref()
                    .is_some_and(|(original, first, last)| {
                        original == capture.persisted_receipt()
                            && frame.transport_ordinal() >= *first
                            && frame.transport_ordinal() <= *last
                    }))
            && capture.service_responses().is_empty()
            && capture.parsed_frames().iter().all(Option::is_some)
            && parsed.raw_sha256() == frame.payload_digest().bytes()
            && batch.service == service
            && batch.content.get(usize::from(content_ordinal)).is_some()
            && millis_timestamp(frame.received_at_unix_millis())
                .is_some_and(|received_at| received_at == self.response_observed_at)
    }
}

fn validate_qualification_authority(
    oauth: SchwabOAuthAuthorityReceipt,
    observed_at: Timestamp,
    rights: EvidenceDigest,
    capability: EvidenceDigest,
) -> Result<(), SchwabVerticalError> {
    let seconds = u64::try_from(observed_at.unix_nanos())
        .map_err(|_| SchwabVerticalError::InvalidCapabilityEvidence)?
        / 1_000_000_000;
    if seconds < oauth.access_issued_at_unix_seconds()
        || seconds >= oauth.access_expires_at_unix_seconds()
    {
        return Err(SchwabVerticalError::InvalidCapabilityEvidence);
    }
    for digest in [
        rights,
        capability,
        oauth
            .credential_authority()
            .application_credential_reference_sha256(),
    ] {
        require_qualification_digest(digest)?;
    }
    Ok(())
}

fn require_qualification_digest(digest: EvidenceDigest) -> Result<(), SchwabVerticalError> {
    if digest.algorithm() != DigestAlgorithm::Sha256 || digest.bytes() == [0; 32] {
        return Err(SchwabVerticalError::InvalidCapabilityEvidence);
    }
    Ok(())
}

const fn qualification_semantics(
    family: SchwabMarketDataFamily,
) -> (&'static str, &'static str, SchwabMarketDataDepth) {
    match family {
        SchwabMarketDataFamily::Quotes => (
            "schwab-rest",
            "schwab-rest-quotes",
            SchwabMarketDataDepth::TopOfBook,
        ),
        SchwabMarketDataFamily::PriceHistory => (
            "schwab-rest",
            "schwab-rest-price-history",
            SchwabMarketDataDepth::NotReported,
        ),
        SchwabMarketDataFamily::OptionChains => (
            "schwab-rest",
            "schwab-rest-option-chains",
            SchwabMarketDataDepth::TopOfBook,
        ),
        SchwabMarketDataFamily::ExpirationChains => (
            "schwab-rest",
            "schwab-rest-expiration-chains",
            SchwabMarketDataDepth::NotReported,
        ),
        SchwabMarketDataFamily::Movers => (
            "schwab-rest",
            "schwab-rest-movers",
            SchwabMarketDataDepth::NotReported,
        ),
        SchwabMarketDataFamily::MarketHours => (
            "schwab-rest",
            "schwab-rest-market-hours",
            SchwabMarketDataDepth::NotReported,
        ),
        SchwabMarketDataFamily::Instruments => (
            "schwab-rest",
            "schwab-rest-instruments",
            SchwabMarketDataDepth::NotReported,
        ),
        SchwabMarketDataFamily::LevelOneEquities => (
            "schwab-streamer",
            "schwab-streamer-level-one-equities",
            SchwabMarketDataDepth::TopOfBook,
        ),
        SchwabMarketDataFamily::LevelOneOptions => (
            "schwab-streamer",
            "schwab-streamer-level-one-options",
            SchwabMarketDataDepth::TopOfBook,
        ),
        SchwabMarketDataFamily::LevelOneFutures => (
            "schwab-streamer",
            "schwab-streamer-level-one-futures",
            SchwabMarketDataDepth::TopOfBook,
        ),
        SchwabMarketDataFamily::LevelOneFuturesOptions => (
            "schwab-streamer",
            "schwab-streamer-level-one-futures-options",
            SchwabMarketDataDepth::TopOfBook,
        ),
        SchwabMarketDataFamily::LevelOneForex => (
            "schwab-streamer",
            "schwab-streamer-level-one-forex",
            SchwabMarketDataDepth::TopOfBook,
        ),
        SchwabMarketDataFamily::NyseBook => (
            "schwab-streamer",
            "schwab-streamer-nyse-book",
            SchwabMarketDataDepth::PriceLevel,
        ),
        SchwabMarketDataFamily::NasdaqBook => (
            "schwab-streamer",
            "schwab-streamer-nasdaq-book",
            SchwabMarketDataDepth::PriceLevel,
        ),
        SchwabMarketDataFamily::OptionsBook => (
            "schwab-streamer",
            "schwab-streamer-options-book",
            SchwabMarketDataDepth::PriceLevel,
        ),
        SchwabMarketDataFamily::ChartEquity => (
            "schwab-streamer",
            "schwab-streamer-chart-equity",
            SchwabMarketDataDepth::NotReported,
        ),
        SchwabMarketDataFamily::ChartFutures => (
            "schwab-streamer",
            "schwab-streamer-chart-futures",
            SchwabMarketDataDepth::NotReported,
        ),
        SchwabMarketDataFamily::ScreenerEquity => (
            "schwab-streamer",
            "schwab-streamer-screener-equity",
            SchwabMarketDataDepth::NotReported,
        ),
        SchwabMarketDataFamily::ScreenerOption => (
            "schwab-streamer",
            "schwab-streamer-screener-option",
            SchwabMarketDataDepth::NotReported,
        ),
    }
}

const fn family_streamer_service(family: SchwabMarketDataFamily) -> Option<MarketDataService> {
    Some(match family {
        SchwabMarketDataFamily::LevelOneEquities => MarketDataService::LevelOneEquities,
        SchwabMarketDataFamily::LevelOneOptions => MarketDataService::LevelOneOptions,
        SchwabMarketDataFamily::LevelOneFutures => MarketDataService::LevelOneFutures,
        SchwabMarketDataFamily::LevelOneFuturesOptions => MarketDataService::LevelOneFuturesOptions,
        SchwabMarketDataFamily::LevelOneForex => MarketDataService::LevelOneForex,
        SchwabMarketDataFamily::NyseBook => MarketDataService::NyseBook,
        SchwabMarketDataFamily::NasdaqBook => MarketDataService::NasdaqBook,
        SchwabMarketDataFamily::OptionsBook => MarketDataService::OptionsBook,
        SchwabMarketDataFamily::ChartEquity => MarketDataService::ChartEquity,
        SchwabMarketDataFamily::ChartFutures => MarketDataService::ChartFutures,
        SchwabMarketDataFamily::ScreenerEquity => MarketDataService::ScreenerEquity,
        SchwabMarketDataFamily::ScreenerOption => MarketDataService::ScreenerOption,
        _ => return None,
    })
}

/// Typed REST doctor input. The constructor proves route, decoded response family, accounting,
/// raw-body identity, and at least one provider record before the input can be observed.
#[derive(Clone, Copy, Debug)]
pub struct SchwabRestFamilyDoctorInput<'a> {
    family: SchwabObservedCapabilityFamily,
    response: &'a ExecutedRestResponse,
}

impl<'a> SchwabRestFamilyDoctorInput<'a> {
    pub fn try_new(
        family: SchwabObservedCapabilityFamily,
        response: &'a ExecutedRestResponse,
    ) -> Result<Self, SchwabVerticalError> {
        if !rest_family_matches(family, response) {
            return Err(SchwabVerticalError::InvalidCapabilityEvidence);
        }
        let receipt = response.capture().receipt();
        let accounting = response.accounting();
        if receipt.status() != 200
            || receipt.body_sha256() != response.payload().raw_sha256()
            || accounting.requested == 0
            || accounting.returned == 0
            || accounting.missing != 0
            || accounting.unexpected != 0
            || accounting.provider_records == 0
        {
            return Err(SchwabVerticalError::InvalidCapabilityEvidence);
        }
        Ok(Self { family, response })
    }

    pub const fn family(self) -> SchwabObservedCapabilityFamily {
        self.family
    }

    pub const fn response(self) -> &'a ExecutedRestResponse {
        self.response
    }
}

/// Non-cloneable selected-service doctor accumulator beginning with one exact sealed ACK capture.
pub struct SchwabStreamerFamilyDoctorAccumulator {
    service: MarketDataService,
    captures: Vec<SchwabSealedStreamerCapture>,
    command: Box<str>,
    request_id: Box<str>,
    request_payload_sha256: EvidenceDigest,
    acknowledgement: SchwabStreamerServiceResponseEvidence,
    generation: ConnectionGeneration,
    token_generation: AccessTokenGeneration,
    credential_authority: SchwabCredentialAuthorityBinding,
    session_identifier: SourceIdentifier,
    market_data_principal_sha256: EvidenceDigest,
    last_frame_ordinal: NonZeroU64,
    provider_records: u64,
}

impl fmt::Debug for SchwabStreamerFamilyDoctorAccumulator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabStreamerFamilyDoctorAccumulator")
            .field("service", &self.service)
            .field("capture_count", &self.captures.len())
            .field("command", &self.command)
            .field("request_id", &self.request_id)
            .field("request_payload_sha256", &self.request_payload_sha256)
            .field("last_frame_ordinal", &self.last_frame_ordinal)
            .field("provider_records", &self.provider_records)
            .finish()
    }
}

impl SchwabStreamerFamilyDoctorAccumulator {
    /// Starts one cross-capture proof from an exact successful subscription acknowledgement.
    pub fn try_from_ack_capture(
        service: MarketDataService,
        capture: SchwabSealedStreamerCapture,
    ) -> Result<Self, SchwabStreamerDoctorCaptureRejection> {
        match validate_ack_capture(service, &capture) {
            Ok((acknowledgement, last_frame_ordinal)) => {
                let command = acknowledgement.command().to_owned().into_boxed_str();
                let request_id = acknowledgement.request_id().to_owned().into_boxed_str();
                let Some(request_payload_sha256) = acknowledgement.request_payload_sha256() else {
                    return Err(SchwabStreamerDoctorCaptureRejection::new(
                        SchwabVerticalError::InvalidCapabilityEvidence,
                        capture,
                    ));
                };
                let mut captures = Vec::new();
                if captures.try_reserve_exact(2).is_err() {
                    return Err(SchwabStreamerDoctorCaptureRejection::new(
                        SchwabVerticalError::ResourceLimit,
                        capture,
                    ));
                }
                let generation = capture.streamer_receipt().generation();
                let token_generation = capture.streamer_receipt().token_generation();
                let credential_authority = capture.streamer_receipt().credential_authority();
                let session_identifier = capture.streamer_receipt().session_identifier().clone();
                let market_data_principal_sha256 =
                    capture.streamer_receipt().market_data_principal_sha256();
                captures.push(capture);
                Ok(Self {
                    service,
                    captures,
                    command,
                    request_id,
                    request_payload_sha256,
                    acknowledgement,
                    generation,
                    token_generation,
                    credential_authority,
                    session_identifier,
                    market_data_principal_sha256,
                    last_frame_ordinal,
                    provider_records: 0,
                })
            }
            Err(error) => Err(SchwabStreamerDoctorCaptureRejection::new(error, capture)),
        }
    }

    /// Adds one exact physically sealed data capture from the acknowledged subscription.
    pub fn try_push_data_capture(
        &mut self,
        capture: SchwabSealedStreamerCapture,
    ) -> Result<(), SchwabStreamerDoctorCaptureRejection> {
        let Some(anchor) = self.captures.first() else {
            return Err(SchwabStreamerDoctorCaptureRejection::new(
                SchwabVerticalError::InvalidCapabilityEvidence,
                capture,
            ));
        };
        let records = match validate_data_capture(
            self.service,
            &self.command,
            self.last_frame_ordinal,
            anchor,
            &capture,
        ) {
            Ok(records) => records,
            Err(error) => {
                return Err(SchwabStreamerDoctorCaptureRejection::new(error, capture));
            }
        };
        if self.captures.try_reserve(1).is_err() {
            return Err(SchwabStreamerDoctorCaptureRejection::new(
                SchwabVerticalError::ResourceLimit,
                capture,
            ));
        }
        let Some(last_frame_ordinal) = capture
            .frames()
            .last()
            .map(|frame| frame.transport_ordinal())
        else {
            return Err(SchwabStreamerDoctorCaptureRejection::new(
                SchwabVerticalError::InvalidCapabilityEvidence,
                capture,
            ));
        };
        let provider_records = match self.provider_records.checked_add(records) {
            Some(value) => value,
            None => {
                return Err(SchwabStreamerDoctorCaptureRejection::new(
                    SchwabVerticalError::Overflow,
                    capture,
                ));
            }
        };
        self.captures.push(capture);
        self.last_frame_ordinal = last_frame_ordinal;
        self.provider_records = provider_records;
        Ok(())
    }

    /// Completes only after at least one separate sealed data capture supplied nonzero records.
    pub fn try_finish(self) -> Result<SchwabStreamerFamilyDoctorHandoff, SchwabVerticalError> {
        if self.captures.len() < 2 || self.provider_records == 0 {
            return Err(SchwabVerticalError::InvalidCapabilityEvidence);
        }
        let capture_set_sha256 = streamer_doctor_capture_set_sha256(
            self.service,
            self.generation,
            self.token_generation,
            &self.command,
            &self.request_id,
            self.request_payload_sha256,
            &self.captures.iter().collect::<Vec<_>>(),
        )?;
        let total_payload_bytes = self.captures.iter().try_fold(0_u64, |total, capture| {
            total
                .checked_add(capture.streamer_receipt().payload_bytes())
                .ok_or(SchwabVerticalError::Overflow)
        })?;
        let sealed_evidence = self
            .captures
            .iter()
            .map(sealed_capture_evidence)
            .collect::<Result<Vec<_>, _>>()?
            .into_boxed_slice();
        let observed_at = capture_observed_at(
            self.captures
                .last()
                .ok_or(SchwabVerticalError::InvalidCapabilityEvidence)?,
        )?;
        Ok(SchwabStreamerFamilyDoctorHandoff {
            service: self.service,
            sealed_evidence,
            observed_at,
            captures: self.captures.into_boxed_slice(),
            initial_publication: None,
            command: self.command,
            request_id: self.request_id,
            request_payload_sha256: self.request_payload_sha256,
            acknowledgement: self.acknowledgement,
            generation: self.generation,
            token_generation: self.token_generation,
            credential_authority: self.credential_authority,
            session_identifier: self.session_identifier,
            market_data_principal_sha256: self.market_data_principal_sha256,
            capture_set_sha256,
            total_payload_bytes,
            provider_records: self.provider_records,
        })
    }
}

/// Rejection retaining ownership of the exact sealed capture that could not join the doctor proof.
pub struct SchwabStreamerDoctorCaptureRejection {
    error: SchwabVerticalError,
    capture: SchwabSealedStreamerCapture,
}

impl SchwabStreamerDoctorCaptureRejection {
    fn new(error: SchwabVerticalError, capture: SchwabSealedStreamerCapture) -> Self {
        Self { error, capture }
    }

    pub const fn error(&self) -> SchwabVerticalError {
        self.error
    }

    pub fn into_capture(self) -> SchwabSealedStreamerCapture {
        self.capture
    }
}

impl fmt::Debug for SchwabStreamerDoctorCaptureRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabStreamerDoctorCaptureRejection")
            .field("error", &self.error)
            .field("capture", &self.capture)
            .finish()
    }
}

/// Non-cloneable complete cross-capture Streamer doctor proof.
pub struct SchwabStreamerFamilyDoctorHandoff {
    sealed_evidence: Box<
        [(
            market_squawk_sources::SealedProviderEventMicrobatchReceipt,
            NonZeroU64,
            NonZeroU64,
        )],
    >,
    observed_at: Timestamp,
    service: MarketDataService,
    captures: Box<[SchwabSealedStreamerCapture]>,
    initial_publication: Option<(
        market_squawk_sources::SealedProviderEventMicrobatchReceipt,
        NonZeroU64,
        NonZeroU64,
    )>,
    command: Box<str>,
    request_id: Box<str>,
    request_payload_sha256: EvidenceDigest,
    acknowledgement: SchwabStreamerServiceResponseEvidence,
    generation: ConnectionGeneration,
    token_generation: AccessTokenGeneration,
    credential_authority: SchwabCredentialAuthorityBinding,
    session_identifier: SourceIdentifier,
    market_data_principal_sha256: EvidenceDigest,
    capture_set_sha256: EvidenceDigest,
    total_payload_bytes: u64,
    provider_records: u64,
}

impl fmt::Debug for SchwabStreamerFamilyDoctorHandoff {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabStreamerFamilyDoctorHandoff")
            .field("service", &self.service)
            .field("capture_count", &self.capture_count())
            .field("command", &self.command)
            .field("request_id", &self.request_id)
            .field("request_payload_sha256", &self.request_payload_sha256)
            .field("provider_records", &self.provider_records)
            .finish()
    }
}

impl SchwabStreamerFamilyDoctorHandoff {
    /// Borrows the exact sealed originals; no capture token is cloned, removed, or resealed.
    /// Several services may prove their own ACK/data relation while the shared frame publishes once.
    pub fn try_from_sealed_captures(
        service: MarketDataService,
        acknowledgement_capture: &SchwabSealedStreamerCapture,
        data_capture: &SchwabSealedStreamerCapture,
    ) -> Result<Self, SchwabVerticalError> {
        let (acknowledgement, last_ack) = validate_ack_capture(service, acknowledgement_capture)?;
        let provider_records = validate_data_capture(
            service,
            acknowledgement.command(),
            last_ack,
            acknowledgement_capture,
            data_capture,
        )?;
        let request_payload_sha256 = acknowledgement
            .request_payload_sha256()
            .ok_or(SchwabVerticalError::InvalidCapabilityEvidence)?;
        let receipt = acknowledgement_capture.streamer_receipt();
        let capture_set_sha256 = streamer_doctor_capture_set_sha256(
            service,
            receipt.generation(),
            receipt.token_generation(),
            acknowledgement.command(),
            acknowledgement.request_id(),
            request_payload_sha256,
            &[acknowledgement_capture, data_capture],
        )?;
        let total_payload_bytes = receipt
            .payload_bytes()
            .checked_add(data_capture.streamer_receipt().payload_bytes())
            .ok_or(SchwabVerticalError::Overflow)?;
        let ack_evidence = sealed_capture_evidence(acknowledgement_capture)?;
        let data_evidence = sealed_capture_evidence(data_capture)?;
        Ok(Self {
            service,
            captures: Box::new([]),
            sealed_evidence: vec![ack_evidence, data_evidence.clone()].into_boxed_slice(),
            initial_publication: Some(data_evidence),
            observed_at: capture_observed_at(data_capture)?,
            command: acknowledgement.command().into(),
            request_id: acknowledgement.request_id().into(),
            request_payload_sha256,
            acknowledgement,
            generation: receipt.generation(),
            token_generation: receipt.token_generation(),
            credential_authority: receipt.credential_authority(),
            session_identifier: receipt.session_identifier().clone(),
            market_data_principal_sha256: receipt.market_data_principal_sha256(),
            capture_set_sha256,
            total_payload_bytes,
            provider_records,
        })
    }

    pub const fn family_input(&self) -> SchwabStreamerFamilyDoctorInput<'_> {
        SchwabStreamerFamilyDoctorInput { handoff: self }
    }

    pub const fn service(&self) -> MarketDataService {
        self.service
    }

    pub fn command(&self) -> &str {
        &self.command
    }

    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    pub const fn request_payload_sha256(&self) -> EvidenceDigest {
        self.request_payload_sha256
    }

    pub const fn generation(&self) -> ConnectionGeneration {
        self.generation
    }

    pub const fn token_generation(&self) -> AccessTokenGeneration {
        self.token_generation
    }

    pub const fn credential_authority(&self) -> SchwabCredentialAuthorityBinding {
        self.credential_authority
    }

    pub const fn session_identifier(&self) -> &SourceIdentifier {
        &self.session_identifier
    }

    pub const fn market_data_principal_sha256(&self) -> EvidenceDigest {
        self.market_data_principal_sha256
    }

    pub const fn provider_records(&self) -> u64 {
        self.provider_records
    }

    /// Digest over every physical receipt, exact ordinal range, and cross-capture authority.
    pub const fn capture_set_sha256(&self) -> EvidenceDigest {
        self.capture_set_sha256
    }

    pub const fn total_payload_bytes(&self) -> u64 {
        self.total_payload_bytes
    }

    /// Moves the original doctor data capture once into publication. The handoff retains only
    /// immutable receipt/ordinal evidence for this capture, never another live capture token.
    pub fn take_initial_publication_capture(&mut self) -> Option<SchwabSealedStreamerCapture> {
        if self.initial_publication.is_some() || self.captures.len() < 2 {
            return None;
        }
        let last = self.captures.last()?;
        let first_ordinal = last.frames().first()?.transport_ordinal();
        let last_ordinal = last.frames().last()?.transport_ordinal();
        let receipt = last.persisted_receipt().clone();
        let mut captures = std::mem::take(&mut self.captures).into_vec();
        let original = captures.pop()?;
        self.captures = captures.into_boxed_slice();
        self.initial_publication = Some((receipt, first_ordinal, last_ordinal));
        Some(original)
    }

    pub fn capture_count(&self) -> usize {
        self.sealed_evidence.len()
    }

    /// Exact physically sealed original, even after the unique token was consumed for publication.
    pub fn capture_receipt(
        &self,
        index: usize,
    ) -> Option<&market_squawk_sources::SealedProviderEventMicrobatchReceipt> {
        self.sealed_evidence
            .get(index)
            .map(|(receipt, _, _)| receipt)
    }
    pub fn capture_frame_ordinals(&self, index: usize) -> Option<(NonZeroU64, NonZeroU64)> {
        self.sealed_evidence
            .get(index)
            .map(|(_, first, last)| (*first, *last))
    }

    pub fn acknowledgement(&self) -> &SchwabStreamerServiceResponseEvidence {
        &self.acknowledgement
    }
}

/// Borrowed provider-record evidence from one complete non-cloneable cross-capture handoff.
#[derive(Clone, Copy, Debug)]
pub struct SchwabStreamerFamilyDoctorInput<'a> {
    handoff: &'a SchwabStreamerFamilyDoctorHandoff,
}

impl<'a> SchwabStreamerFamilyDoctorInput<'a> {
    pub const fn family(self) -> SchwabObservedCapabilityFamily {
        SchwabObservedCapabilityFamily::Streamer(self.handoff.service)
    }

    pub const fn handoff(self) -> &'a SchwabStreamerFamilyDoctorHandoff {
        self.handoff
    }

    pub const fn provider_records(self) -> u64 {
        self.handoff.provider_records
    }

    pub fn service_response(self) -> &'a SchwabStreamerServiceResponseEvidence {
        self.handoff.acknowledgement()
    }
}

fn sealed_capture_evidence(
    capture: &SchwabSealedStreamerCapture,
) -> Result<
    (
        market_squawk_sources::SealedProviderEventMicrobatchReceipt,
        NonZeroU64,
        NonZeroU64,
    ),
    SchwabVerticalError,
> {
    Ok((
        capture.persisted_receipt().clone(),
        capture
            .frames()
            .first()
            .ok_or(SchwabVerticalError::InvalidCapabilityEvidence)?
            .transport_ordinal(),
        capture
            .frames()
            .last()
            .ok_or(SchwabVerticalError::InvalidCapabilityEvidence)?
            .transport_ordinal(),
    ))
}
fn capture_observed_at(
    capture: &SchwabSealedStreamerCapture,
) -> Result<Timestamp, SchwabVerticalError> {
    millis_timestamp(
        capture
            .frames()
            .last()
            .ok_or(SchwabVerticalError::InvalidCapabilityEvidence)?
            .received_at_unix_millis(),
    )
    .ok_or(SchwabVerticalError::InvalidCapabilityEvidence)
}

fn validate_ack_capture(
    service: MarketDataService,
    capture: &SchwabSealedStreamerCapture,
) -> Result<(SchwabStreamerServiceResponseEvidence, NonZeroU64), SchwabVerticalError> {
    validate_sealed_capture_shape(capture)?;
    let mut selected = capture
        .service_responses()
        .iter()
        .filter(|response| response.service() == service);
    let response = selected
        .next()
        .ok_or(SchwabVerticalError::InvalidCapabilityEvidence)?;
    if selected.next().is_some() {
        return Err(SchwabVerticalError::InvalidCapabilityEvidence);
    }
    let Some(request_payload_sha256) = response.request_payload_sha256() else {
        return Err(SchwabVerticalError::InvalidCapabilityEvidence);
    };
    if response.service() != service
        || response.command() != "SUBS"
        || response.request_id().is_empty()
        || !response.succeeded()
        || response.round_trip_latency_ms().is_none()
        || request_payload_sha256.algorithm() != DigestAlgorithm::Sha256
        || request_payload_sha256.bytes() == [0; 32]
        || response.sealed_capture_receipt_sha256() != capture.persisted_receipt().receipt_digest()
        || capture.frames().iter().all(|frame| {
            frame.transport_ordinal() != response.transport_ordinal()
                || frame.event_id() != response.event_id()
                || frame.payload_digest() != response.payload_digest()
        })
        || capture.parsed_frames().iter().any(Option::is_none)
    {
        return Err(SchwabVerticalError::InvalidCapabilityEvidence);
    }
    let last_frame_ordinal = capture
        .frames()
        .last()
        .map(|frame| frame.transport_ordinal())
        .ok_or(SchwabVerticalError::InvalidCapabilityEvidence)?;
    Ok((response.clone(), last_frame_ordinal))
}

fn validate_data_capture(
    service: MarketDataService,
    command: &str,
    prior_last_frame_ordinal: NonZeroU64,
    anchor: &SchwabSealedStreamerCapture,
    capture: &SchwabSealedStreamerCapture,
) -> Result<u64, SchwabVerticalError> {
    validate_sealed_capture_shape(capture)?;
    if !capture.service_responses().is_empty()
        || capture.streamer_receipt().generation() != anchor.streamer_receipt().generation()
        || capture.streamer_receipt().token_generation()
            != anchor.streamer_receipt().token_generation()
        || capture.streamer_receipt().credential_authority()
            != anchor.streamer_receipt().credential_authority()
        || capture.streamer_receipt().session_identifier()
            != anchor.streamer_receipt().session_identifier()
        || capture.streamer_receipt().market_data_principal_sha256()
            != anchor.streamer_receipt().market_data_principal_sha256()
        || capture.coordinates() != anchor.coordinates()
        || capture.stream_identity() != anchor.stream_identity()
        || capture
            .frames()
            .first()
            .is_none_or(|frame| frame.transport_ordinal() <= prior_last_frame_ordinal)
    {
        return Err(SchwabVerticalError::InvalidCapabilityEvidence);
    }
    let mut provider_records = 0_u64;
    for frame in capture.parsed_frames() {
        let Some(frame) = frame else {
            return Err(SchwabVerticalError::InvalidCapabilityEvidence);
        };
        if !frame.value().responses.is_empty() {
            return Err(SchwabVerticalError::InvalidCapabilityEvidence);
        }
        for batch in &frame.value().data {
            if batch.service != service {
                continue;
            }
            if batch.command.as_ref() != command || batch.content.is_empty() {
                return Err(SchwabVerticalError::InvalidCapabilityEvidence);
            }
            provider_records = provider_records
                .checked_add(
                    u64::try_from(batch.content.len())
                        .map_err(|_| SchwabVerticalError::Overflow)?,
                )
                .ok_or(SchwabVerticalError::Overflow)?;
        }
    }
    if provider_records == 0 {
        return Err(SchwabVerticalError::InvalidCapabilityEvidence);
    }
    Ok(provider_records)
}

fn validate_sealed_capture_shape(
    capture: &SchwabSealedStreamerCapture,
) -> Result<(), SchwabVerticalError> {
    let frames = capture.frames();
    let receipt = capture.streamer_receipt();
    let persisted = capture.persisted_receipt();
    if frames.is_empty()
        || frames.len() != capture.parsed_frames().len()
        || frames.len() != persisted.capture().frames().len()
        || frames.len() != persisted.segment().frames().len()
        || receipt.frame_count()
            != u64::try_from(frames.len()).map_err(|_| SchwabVerticalError::Overflow)?
        || receipt.first_ordinal()
            != frames
                .first()
                .map(|frame| frame.transport_ordinal())
                .ok_or(SchwabVerticalError::InvalidCapabilityEvidence)?
        || receipt.last_ordinal()
            != frames
                .last()
                .map(|frame| frame.transport_ordinal())
                .ok_or(SchwabVerticalError::InvalidCapabilityEvidence)?
    {
        return Err(SchwabVerticalError::InvalidCapabilityEvidence);
    }
    let mut prior = None;
    for frame in frames {
        if frame.generation() != receipt.generation()
            || prior.is_some_and(|ordinal: u64| {
                ordinal
                    .checked_add(1)
                    .is_none_or(|next| frame.transport_ordinal().get() != next)
            })
        {
            return Err(SchwabVerticalError::InvalidCapabilityEvidence);
        }
        prior = Some(frame.transport_ordinal().get());
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "the exact cross-capture doctor authority remains explicit"
)]
fn streamer_doctor_capture_set_sha256(
    service: MarketDataService,
    generation: ConnectionGeneration,
    token_generation: AccessTokenGeneration,
    command: &str,
    request_id: &str,
    request_payload_sha256: EvidenceDigest,
    captures: &[&SchwabSealedStreamerCapture],
) -> Result<EvidenceDigest, SchwabVerticalError> {
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk/schwab-streamer-doctor-capture-set/v1");
    hash_vertical_text(&mut hasher, service.as_str())?;
    hasher.update(generation.get().to_be_bytes());
    hasher.update(token_generation.get().to_be_bytes());
    hash_vertical_text(&mut hasher, command)?;
    hash_vertical_text(&mut hasher, request_id)?;
    hasher.update(request_payload_sha256.bytes());
    hasher.update(
        u64::try_from(captures.len())
            .map_err(|_| SchwabVerticalError::Overflow)?
            .to_be_bytes(),
    );
    for capture in captures {
        let receipt = capture.streamer_receipt();
        let physical = capture.persisted_receipt();
        let first = capture
            .frames()
            .first()
            .ok_or(SchwabVerticalError::InvalidCapabilityEvidence)?;
        let last = capture
            .frames()
            .last()
            .ok_or(SchwabVerticalError::InvalidCapabilityEvidence)?;
        hasher.update(physical.receipt_digest().bytes());
        hasher.update(receipt.content_sha256());
        hasher.update(receipt.observation_sha256());
        hasher.update(receipt.frame_count().to_be_bytes());
        hasher.update(receipt.payload_bytes().to_be_bytes());
        hasher.update(first.transport_ordinal().get().to_be_bytes());
        hasher.update(last.transport_ordinal().get().to_be_bytes());
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hasher.finalize().into(),
    ))
}

fn hash_vertical_text(hasher: &mut Sha256, value: &str) -> Result<(), SchwabVerticalError> {
    hasher.update(
        u64::try_from(value.len())
            .map_err(|_| SchwabVerticalError::Overflow)?
            .to_be_bytes(),
    );
    hasher.update(value.as_bytes());
    Ok(())
}

/// Closed typed doctor input across every admitted read-only market-data family.
#[derive(Clone, Copy, Debug)]
pub enum SchwabFamilyDoctorInput<'a> {
    Rest(SchwabRestFamilyDoctorInput<'a>),
    Streamer(SchwabStreamerFamilyDoctorInput<'a>),
}

impl SchwabFamilyDoctorInput<'_> {
    pub const fn family(self) -> SchwabObservedCapabilityFamily {
        match self {
            Self::Rest(input) => input.family(),
            Self::Streamer(input) => input.family(),
        }
    }
}

fn rest_family_matches(
    family: SchwabObservedCapabilityFamily,
    response: &ExecutedRestResponse,
) -> bool {
    matches!(
        (
            family,
            response.capture().receipt().route(),
            response.payload()
        ),
        (
            SchwabObservedCapabilityFamily::Quotes,
            ReadOnlyRoute::Quotes | ReadOnlyRoute::SingleQuote,
            SchwabRestPayload::Quotes(_)
        ) | (
            SchwabObservedCapabilityFamily::OptionChain,
            ReadOnlyRoute::Chains,
            SchwabRestPayload::OptionChain(_)
        ) | (
            SchwabObservedCapabilityFamily::ExpirationChain,
            ReadOnlyRoute::ExpirationChain,
            SchwabRestPayload::Expirations(_)
        ) | (
            SchwabObservedCapabilityFamily::DailyPriceHistory,
            ReadOnlyRoute::PriceHistory,
            SchwabRestPayload::PriceHistory(_)
        ) | (
            SchwabObservedCapabilityFamily::MarketHours,
            ReadOnlyRoute::Markets | ReadOnlyRoute::SingleMarket,
            SchwabRestPayload::MarketHours(_)
        ) | (
            SchwabObservedCapabilityFamily::Movers,
            ReadOnlyRoute::Movers,
            SchwabRestPayload::Movers(_)
        ) | (
            SchwabObservedCapabilityFamily::Instruments,
            ReadOnlyRoute::Instruments | ReadOnlyRoute::InstrumentByCusip,
            SchwabRestPayload::Instruments(_)
        )
    )
}

/// Currentness of one exact family observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchwabCapabilityCurrentness {
    Current,
    Expired,
    TokenGenerationChanged,
    OAuthAuthorityChanged,
    ResponseChanged,
}

/// Evidence for one exact daily-history response; no unrelated bootstrap grants access.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SchwabPriceHistoryCapabilityObservation {
    observed_at_unix_seconds: u64,
    oauth_authority: SchwabOAuthAuthorityReceipt,
    price_history_receipt_sha256: [u8; 32],
}

impl SchwabPriceHistoryCapabilityObservation {
    /// Observes the actual daily, frequency-one, explicit-range response.
    pub fn try_observe(
        oauth_authority: SchwabOAuthAuthorityReceipt,
        price_history_response: &ExecutedRestResponse,
        observed_at_unix_seconds: u64,
    ) -> Result<Self, SchwabVerticalError> {
        let receipt = price_history_response.capture().receipt();
        let SchwabRestPayload::PriceHistory(history) = price_history_response.payload() else {
            return Err(SchwabVerticalError::InvalidCapabilityEvidence);
        };
        let accounting = price_history_response.accounting();
        if exact_daily_range(receipt.request_url()).is_none()
            || observed_at_unix_seconds < oauth_authority.access_issued_at_unix_seconds()
            || observed_at_unix_seconds >= oauth_authority.access_expires_at_unix_seconds()
            || receipt.received_at_unix_millis() / 1_000 > observed_at_unix_seconds
            || receipt.received_at_unix_millis() / 1_000
                < oauth_authority.access_issued_at_unix_seconds()
            || oauth_authority.generation() != receipt.token_generation()
            || oauth_authority.credential_authority() != receipt.credential_authority()
            || receipt.route() != ReadOnlyRoute::PriceHistory
            || receipt.status() != 200
            || receipt.body_sha256() != history.raw_sha256()
            || accounting.requested != 1
            || accounting.returned != 1
            || accounting.missing != 0
            || accounting.unexpected != 0
            || accounting.provider_records == 0
            || history.value().empty
            || history.value().candles().is_empty()
            || u64::try_from(history.value().candles().len()).ok()
                != Some(accounting.provider_records)
        {
            return Err(SchwabVerticalError::InvalidCapabilityEvidence);
        }
        Ok(Self {
            observed_at_unix_seconds,
            oauth_authority,
            price_history_receipt_sha256: rest_receipt_digest(price_history_response),
        })
    }

    pub const fn family(self) -> SchwabObservedCapabilityFamily {
        SchwabObservedCapabilityFamily::DailyPriceHistory
    }
    pub const fn receipt_sha256(self) -> [u8; 32] {
        self.price_history_receipt_sha256
    }
    pub const fn expires_at_unix_seconds(self) -> u64 {
        self.oauth_authority.access_expires_at_unix_seconds()
    }

    pub fn currentness(
        self,
        oauth_authority: SchwabOAuthAuthorityReceipt,
        price_history_response: &ExecutedRestResponse,
        now_unix_seconds: u64,
    ) -> SchwabCapabilityCurrentness {
        self.currentness_from_receipt(
            oauth_authority,
            price_history_response.capture().receipt(),
            price_history_response.accounting(),
            now_unix_seconds,
        )
    }

    pub(crate) fn currentness_from_receipt(
        self,
        oauth_authority: SchwabOAuthAuthorityReceipt,
        receipt: &crate::RawRestResponseReceipt,
        accounting: crate::RestItemAccounting,
        now_unix_seconds: u64,
    ) -> SchwabCapabilityCurrentness {
        if now_unix_seconds < self.observed_at_unix_seconds
            || now_unix_seconds < oauth_authority.access_issued_at_unix_seconds()
            || now_unix_seconds >= oauth_authority.access_expires_at_unix_seconds()
        {
            return SchwabCapabilityCurrentness::Expired;
        }
        if receipt.token_generation() != oauth_authority.generation() {
            return SchwabCapabilityCurrentness::TokenGenerationChanged;
        }
        if receipt.credential_authority() != oauth_authority.credential_authority()
            || oauth_authority != self.oauth_authority
        {
            return SchwabCapabilityCurrentness::OAuthAuthorityChanged;
        }
        if receipt.route() != ReadOnlyRoute::PriceHistory
            || rest_receipt_digest_from_parts(receipt, accounting)
                != self.price_history_receipt_sha256
        {
            return SchwabCapabilityCurrentness::ResponseChanged;
        }
        SchwabCapabilityCurrentness::Current
    }
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
    receipt: &crate::RawRestResponseReceipt,
) -> Result<(Timestamp, Timestamp), SchwabVerticalError> {
    if receipt.route() != ReadOnlyRoute::PriceHistory {
        return Err(SchwabVerticalError::InvalidCapabilityEvidence);
    }
    exact_daily_range(receipt.request_url()).ok_or(SchwabVerticalError::InvalidCapabilityEvidence)
}

fn millis_timestamp(value: u64) -> Option<Timestamp> {
    i64::try_from(value)
        .ok()
        .and_then(|value| value.checked_mul(1_000_000))
        .map(Timestamp::from_unix_nanos)
}

pub(crate) fn rest_receipt_digest(response: &ExecutedRestResponse) -> [u8; 32] {
    receipt_digest(response.capture().receipt(), response.accounting())
}

pub(crate) fn rest_receipt_digest_from_parts(
    receipt: &crate::RawRestResponseReceipt,
    accounting: crate::RestItemAccounting,
) -> [u8; 32] {
    receipt_digest(receipt, accounting)
}

fn receipt_digest(
    receipt: &crate::RawRestResponseReceipt,
    accounting: crate::RestItemAccounting,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk/schwab-rest-observation/v1");
    hasher.update([route_tag(receipt.route())]);
    hasher.update(receipt.token_generation().get().to_be_bytes());
    hasher.update(
        receipt
            .credential_authority()
            .application_credential_generation()
            .get()
            .to_be_bytes(),
    );
    hasher.update(
        receipt
            .credential_authority()
            .application_credential_reference_sha256()
            .bytes(),
    );
    hasher.update(receipt.request_sha256());
    hasher.update(receipt.status().to_be_bytes());
    hasher.update(receipt.received_at_unix_millis().to_be_bytes());
    hasher.update(receipt.body_bytes().to_be_bytes());
    hasher.update(receipt.body_sha256());
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
    #[error("Schwab provider evidence exceeded its local resource bound")]
    ResourceLimit,
    #[error("Schwab provider evidence arithmetic overflowed")]
    Overflow,
}
