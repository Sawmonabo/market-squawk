//! Family-scoped observed capability evidence.
//!
//! A successful request from one Schwab family never authorizes another family. Raw market-data
//! capture is handed to the shared provider-capture authority; this module retains only the
//! minimum parsed User Preference evidence needed to establish a price-history capability.

use std::fmt;
use std::num::NonZeroU64;
use std::time::Duration;

use market_squawk_domain::{
    CoverageDelay, DataQuality, DigestAlgorithm, EvidenceDigest, MarketDepth, ProviderChannel,
    ProviderProduct, SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    RuntimeCapabilityDisposition, SchwabMarketDataDoctorReceiptV1, SchwabMarketDataFamily,
};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::{
    ACCESS_TOKEN_MAX_LIFETIME_SECONDS, AccessTokenGeneration, ConnectionGeneration,
    ExecutedRestResponse, MarketDataService, ReadOnlyRoute, SchwabCredentialAuthorityBinding,
    SchwabOAuthAuthorityReceipt, SchwabRestPayload, SchwabSealedStreamerCapture,
    SchwabStreamerServiceResponseEvidence, SchwabUserPreferenceEvidence,
};

/// One independently probed read-only Schwab market-data family.
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

/// Opaque, family-scoped authority minted only from a current Schwab doctor receipt.
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
    market_data_principal_sha256: EvidenceDigest,
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
    pub fn try_from_doctor_receipt(
        doctor: &SchwabMarketDataDoctorReceiptV1,
        family: SchwabMarketDataFamily,
        response_observed_at: Timestamp,
        oauth_authority: SchwabOAuthAuthorityReceipt,
    ) -> Result<Self, SchwabVerticalError> {
        let token_generation = oauth_authority.generation();
        let credential_authority = oauth_authority.credential_authority();
        let market_data_principal_sha256 = doctor.market_data_principal_sha256();
        let evidence = doctor
            .observation()
            .families
            .iter()
            .find(|evidence| evidence.family == family)
            .ok_or(SchwabVerticalError::InvalidCapabilityEvidence)?;
        let family_observed_at = evidence
            .observed_at
            .ok_or(SchwabVerticalError::InvalidCapabilityEvidence)?;
        let observation_evidence = evidence
            .observation_sha256
            .ok_or(SchwabVerticalError::InvalidCapabilityEvidence)?;
        let receipt_evidence = doctor.receipt_sha256();
        let entitlement_evidence = doctor.rights_decision_digest();
        let capability_evidence = doctor.capability_digest();
        for digest in [
            receipt_evidence,
            observation_evidence,
            evidence.disposition_evidence_sha256,
            entitlement_evidence,
            capability_evidence,
            credential_authority.application_credential_reference_sha256(),
            market_data_principal_sha256,
        ] {
            require_qualification_digest(digest)?;
        }
        if !doctor.is_current_at(response_observed_at)
            || doctor.access_token_generation() != token_generation.get()
            || doctor.application_credential_generation()
                != credential_authority.application_credential_generation()
            || doctor.application_credential_reference_sha256()
                != credential_authority.application_credential_reference_sha256()
            || family_observed_at > doctor.verified_at()
            || family_observed_at > response_observed_at
            || !matches!(
                evidence.disposition,
                RuntimeCapabilityDisposition::Available | RuntimeCapabilityDisposition::Degraded
            )
        {
            return Err(SchwabVerticalError::InvalidCapabilityEvidence);
        }

        let (product, channel, depth) = qualification_semantics(family);
        let feed = SourceIdentifier::try_from(channel)
            .map_err(|_| SchwabVerticalError::InvalidCapabilityEvidence)?;
        let provider_product = ProviderProduct::new(
            SourceIdentifier::try_from(product)
                .map_err(|_| SchwabVerticalError::InvalidCapabilityEvidence)?,
        );
        let provider_channel = ProviderChannel::new(feed.clone());
        let delay = match (family, doctor.quote_delay()) {
            (SchwabMarketDataFamily::Quotes, Some(CoverageDelay::RealTime)) => {
                SchwabMarketDataDelay::RealTime
            }
            (SchwabMarketDataFamily::Quotes, Some(CoverageDelay::Delayed(value))) => {
                SchwabMarketDataDelay::Delayed(
                    NonZeroU64::new(value).ok_or(SchwabVerticalError::InvalidCapabilityEvidence)?,
                )
            }
            (SchwabMarketDataFamily::Quotes, None) => {
                return Err(SchwabVerticalError::InvalidCapabilityEvidence);
            }
            _ => SchwabMarketDataDelay::Unknown,
        };
        let quality = if matches!(delay, SchwabMarketDataDelay::Delayed(_)) {
            DataQuality::OfficialDelayed
        } else {
            DataQuality::DirectUnverified
        };
        Ok(Self {
            family,
            disposition: evidence.disposition,
            response_observed_at,
            family_observed_at,
            token_generation,
            credential_authority,
            session_identifier: doctor.session_identifier().clone(),
            market_data_principal_sha256,
            receipt_evidence,
            observation_evidence,
            disposition_evidence: evidence.disposition_evidence_sha256,
            entitlement_evidence,
            capability_evidence,
            feed,
            depth,
            delay,
            quality,
            provider_product,
            provider_channel,
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
    pub const fn market_data_principal_sha256(&self) -> EvidenceDigest {
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

    pub(crate) fn validates_rest_response(
        &self,
        family: SchwabMarketDataFamily,
        response: &ExecutedRestResponse,
    ) -> bool {
        self.validates_rest_receipt(family, response.capture().receipt())
    }

    pub(crate) fn validates_rest_receipt(
        &self,
        family: SchwabMarketDataFamily,
        receipt: &crate::RawRestResponseReceipt,
    ) -> bool {
        self.family == family
            && self.rest_service().is_some()
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
            && handoff.market_data_principal_sha256() == self.market_data_principal_sha256
            && receipt.token_generation() == self.token_generation
            && receipt.credential_authority() == self.credential_authority
            && receipt.session_identifier() == &self.session_identifier
            && receipt.market_data_principal_sha256() == self.market_data_principal_sha256
            && handoff.generation() == receipt.generation()
            && last_ack_ordinal.is_some_and(|last| frame.transport_ordinal() > last)
            && capture.service_responses().is_empty()
            && capture.parsed_frames().iter().all(Option::is_some)
            && parsed.raw_sha256() == frame.payload_digest().bytes()
            && batch.service == service
            && batch.content.get(usize::from(content_ordinal)).is_some()
            && millis_timestamp(frame.received_at_unix_millis())
                .is_some_and(|received_at| received_at == self.response_observed_at)
    }
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
            &self.captures,
        )?;
        let total_payload_bytes = self.captures.iter().try_fold(0_u64, |total, capture| {
            total
                .checked_add(capture.streamer_receipt().payload_bytes())
                .ok_or(SchwabVerticalError::Overflow)
        })?;
        Ok(SchwabStreamerFamilyDoctorHandoff {
            service: self.service,
            captures: self.captures.into_boxed_slice(),
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
    service: MarketDataService,
    captures: Box<[SchwabSealedStreamerCapture]>,
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
            .field("capture_count", &self.captures.len())
            .field("command", &self.command)
            .field("request_id", &self.request_id)
            .field("request_payload_sha256", &self.request_payload_sha256)
            .field("provider_records", &self.provider_records)
            .finish()
    }
}

impl SchwabStreamerFamilyDoctorHandoff {
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

    pub fn capture_count(&self) -> usize {
        self.captures.len()
    }

    /// Returns one exact physical receipt; parsed provider frames remain private.
    pub fn capture_receipt(
        &self,
        index: usize,
    ) -> Option<&market_squawk_sources::SealedProviderEventMicrobatchReceipt> {
        self.captures
            .get(index)
            .map(SchwabSealedStreamerCapture::persisted_receipt)
    }

    /// Returns the exact inclusive transport-ordinal range retained by one physical capture.
    pub fn capture_frame_ordinals(&self, index: usize) -> Option<(NonZeroU64, NonZeroU64)> {
        let capture = self.captures.get(index)?;
        Some((
            capture.frames().first()?.transport_ordinal(),
            capture.frames().last()?.transport_ordinal(),
        ))
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

fn validate_ack_capture(
    service: MarketDataService,
    capture: &SchwabSealedStreamerCapture,
) -> Result<(SchwabStreamerServiceResponseEvidence, NonZeroU64), SchwabVerticalError> {
    validate_sealed_capture_shape(capture)?;
    let [response] = capture.service_responses() else {
        return Err(SchwabVerticalError::InvalidCapabilityEvidence);
    };
    let Some(request_payload_sha256) = response.request_payload_sha256() else {
        return Err(SchwabVerticalError::InvalidCapabilityEvidence);
    };
    if response.service() != service
        || response.command() != "SUBS"
        || response.request_id().is_empty()
        || response.status_code() != 0
        || response.round_trip_latency_ms().is_none()
        || request_payload_sha256.algorithm() != DigestAlgorithm::Sha256
        || request_payload_sha256.bytes() == [0; 32]
        || response.sealed_capture_receipt_sha256() != capture.persisted_receipt().receipt_digest()
        || capture.frames().iter().all(|frame| {
            frame.transport_ordinal() != response.transport_ordinal()
                || frame.event_id() != response.event_id()
                || frame.payload_digest() != response.payload_digest()
        })
        || capture.parsed_frames().iter().any(|frame| {
            frame
                .as_ref()
                .is_none_or(|frame| !frame.value().data.is_empty())
        })
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
    captures: &[SchwabSealedStreamerCapture],
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
        user_preference_probe: &SchwabUserPreferenceEvidence,
        price_history_probe: &ExecutedRestResponse,
        observed_at_unix_seconds: u64,
        valid_for: Duration,
    ) -> Result<Self, SchwabVerticalError> {
        let preference_receipt = user_preference_probe.receipt();
        let history_receipt = price_history_probe.capture().receipt();
        let bootstrap = user_preference_probe.bootstrap();
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
            || oauth_authority.credential_authority() != preference_receipt.credential_authority()
            || oauth_authority.credential_authority() != history_receipt.credential_authority()
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
        let user_preference_receipt_sha256 = user_preference_receipt_digest(user_preference_probe);
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
        user_preference_probe: &SchwabUserPreferenceEvidence,
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
        let preference_receipt = user_preference_probe.receipt();
        let receipt = price_history_probe.capture().receipt();
        if preference_receipt.token_generation() != oauth_authority.generation()
            || receipt.token_generation() != oauth_authority.generation()
        {
            return SchwabCapabilityCurrentness::TokenGenerationChanged;
        }
        if preference_receipt.credential_authority() != oauth_authority.credential_authority()
            || receipt.credential_authority() != oauth_authority.credential_authority()
        {
            return SchwabCapabilityCurrentness::OAuthAuthorityChanged;
        }
        if oauth_authority != self.oauth_authority {
            return SchwabCapabilityCurrentness::OAuthAuthorityChanged;
        }
        let bootstrap = user_preference_probe.bootstrap();
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
            || user_preference_receipt_digest(user_preference_probe)
                != self.user_preference_receipt_sha256
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

pub(crate) fn rest_receipt_digest(response: &ExecutedRestResponse) -> [u8; 32] {
    receipt_digest(response.capture().receipt(), response.accounting())
}

pub(crate) fn rest_receipt_digest_from_parts(
    receipt: &crate::RawRestResponseReceipt,
    accounting: crate::RestItemAccounting,
) -> [u8; 32] {
    receipt_digest(receipt, accounting)
}

pub(crate) fn user_preference_receipt_digest(response: &SchwabUserPreferenceEvidence) -> [u8; 32] {
    receipt_digest(response.receipt(), response.accounting())
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
    #[error("Schwab provider evidence exceeded its local resource bound")]
    ResourceLimit,
    #[error("Schwab provider evidence arithmetic overflowed")]
    Overflow,
}
