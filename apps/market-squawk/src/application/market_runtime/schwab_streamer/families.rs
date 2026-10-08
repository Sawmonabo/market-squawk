//! Family-specific original proofs and record mappings under one account connection.
use super::*;
use crate::provider_activation::{MarketReferenceIdentityApprovalV1, SchwabQuoteReferenceBinding};
use market_squawk_domain::{
    BookStateBinding, CanonicalStateDigest, CanonicalizationRule,
    ConnectionGeneration as CanonicalConnectionGeneration, CoverageStatus,
    DecodedLiveProvenanceInput, DigestAlgorithm, EvidenceDigest, LiveEventClass,
    LiveEvidenceBinding, LiveProvenance, MarketDataReference, MarketDepth, PayloadHash,
    PayloadReference, RuleVersion, VenueId,
};
use market_squawk_sources::SourceMetadata;
use sha2::{Digest as _, Sha256};
use std::collections::BTreeMap;

type Bindings = [(
    SchwabQuoteReferenceBinding,
    Option<MarketReferenceIdentityApprovalV1>,
)];

/// Exact original ACK custody is bounded by the closed selected service set.
#[derive(Default)]
pub(super) struct Proofs {
    acknowledgements: Vec<SchwabSealedStreamerCapture>,
    handoffs: BTreeMap<MarketDataService, SchwabStreamerFamilyDoctorHandoff>,
}
impl Proofs {
    pub(super) fn retain_control(
        &mut self,
        capture: SchwabSealedStreamerCapture,
    ) -> Result<(), ServiceError> {
        if capture.service_responses().is_empty() {
            return Err(ServiceError::InvalidResult);
        }
        // A service is subscribed once in this owned connection. Reconnect mints a new owner.
        if self.acknowledgements.len() >= 12 {
            return Err(ServiceError::InvalidResult);
        }
        self.acknowledgements.push(capture);
        Ok(())
    }
    pub(super) fn observe_data(
        &mut self,
        capture: &SchwabSealedStreamerCapture,
    ) -> Result<(), ServiceError> {
        let services = capture
            .parsed_frames()
            .iter()
            .filter_map(Option::as_ref)
            .flat_map(|frame| frame.value().data.iter().map(|batch| batch.service))
            .collect::<BTreeSet<_>>();
        for service in services {
            if self.handoffs.contains_key(&service) {
                continue;
            }
            let Some(ack) = self.acknowledgements.iter().find(|capture| {
                capture.service_responses().iter().any(|response| {
                    response.service() == service
                        && response.command() == "SUBS"
                        && response.succeeded()
                })
            }) else {
                // No same-family success evidence: canonical mapping remains unavailable; raw is sealed.
                continue;
            };
            let proof =
                SchwabStreamerFamilyDoctorHandoff::try_from_sealed_captures(service, ack, capture)
                    .map_err(|_| ServiceError::InvalidResult)?;
            self.handoffs.insert(service, proof);
        }
        Ok(())
    }
    pub(super) fn handoff(
        &self,
        service: MarketDataService,
    ) -> Option<&SchwabStreamerFamilyDoctorHandoff> {
        self.handoffs.get(&service)
    }
}

/// Default installed market views request only genuine selected identities, plus explicit activity cohorts.
/// Unsupported asset families do not acquire a fabricated reference or a quote interpretation.
pub(super) fn selections(
    bindings: &Bindings,
) -> Result<BTreeMap<MarketDataService, Vec<ProviderIdentifier>>, ServiceError> {
    crate::provider_activation::schwab_streamer_selections(bindings.iter().map(|(binding, _)| binding))
}

pub(super) const fn class(service: MarketDataService) -> LiveEventClass {
    match service {
        MarketDataService::NyseBook
        | MarketDataService::NasdaqBook
        | MarketDataService::OptionsBook => LiveEventClass::BookSnapshot,
        MarketDataService::ChartEquity | MarketDataService::ChartFutures => LiveEventClass::Chart,
        MarketDataService::ScreenerEquity | MarketDataService::ScreenerOption => {
            LiveEventClass::Screener
        }
        _ => LiveEventClass::Quote,
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "exact source coordinate, original proof and canonical identity are independent"
)]
pub(super) fn provenance(
    metadata: &SourceMetadata,
    capture: &SchwabSealedStreamerCapture,
    record: &SchwabCanonicalStreamerRecord,
    reference: Option<&MarketDataReference>,
    item_references: &[MarketDataReference],
    qualification: &SchwabMarketDataQualification,
    venue: &VenueId,
    batch: u16,
    content: u16,
    received: Timestamp,
    observed: Timestamp,
) -> Result<LiveProvenance, ServiceError> {
    let frame = capture
        .frames()
        .first()
        .ok_or(ServiceError::InvalidResult)?;
    let event_class = class(record.service);
    let symbol = SourceIdentifier::try_from(record.provider_identifier.as_str())
        .map_err(|_| ServiceError::InvalidResult)?;
    let depth = (event_class == LiveEventClass::BookSnapshot).then_some(MarketDepth::PriceLevel);
    let rule = metadata
        .coverage()
        .live_for(
            qualification.provider_product(),
            qualification.provider_channel(),
        )
        .and_then(|live| live.rule_for(event_class, depth))
        .ok_or(ServiceError::Unavailable)?;
    if event_class == LiveEventClass::Screener && !rule.permits_source_cohort(&symbol) {
        return Err(ServiceError::InvalidResult);
    }
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/schwab-streamer-record/v1\0");
    hash.update(frame.payload_digest().bytes());
    hash.update(record.service.as_str().as_bytes());
    hash.update(batch.to_be_bytes());
    hash.update(content.to_be_bytes());
    if let Some(reference) = reference {
        hash.update(reference.definition_digest().bytes());
    }
    hash.update(
        u64::try_from(item_references.len())
            .map_err(|_| ServiceError::InvalidResult)?
            .to_be_bytes(),
    );
    for reference in item_references {
        let symbol = reference.source_symbol().as_str().as_bytes();
        hash.update(
            u64::try_from(symbol.len())
                .map_err(|_| ServiceError::InvalidResult)?
                .to_be_bytes(),
        );
        hash.update(symbol);
        hash.update(reference.definition_digest().bytes());
    }
    let state = CanonicalStateDigest::new(
        EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into()),
        CanonicalizationRule::new(
            SourceIdentifier::try_from("schwab-streamer-record")
                .map_err(|_| ServiceError::Internal)?,
            RuleVersion::new(1).map_err(|_| ServiceError::Internal)?,
        ),
    );
    let connection =
        CanonicalConnectionGeneration::new(capture.streamer_receipt().generation().get())
            .map_err(|_| ServiceError::InvalidResult)?;
    let binding = if event_class == LiveEventClass::Screener {
        if reference.is_some() {
            return Err(ServiceError::InvalidResult);
        }
        LiveEvidenceBinding::new_source_cohort(
            metadata.source_id().clone(),
            capture.stream_identity().clone(),
            metadata.revision().clone(),
            metadata.authorization().basis().clone(),
            venue.clone(),
            connection,
            qualification.provider_product().clone(),
            qualification.provider_channel().clone(),
            symbol,
            frame.payload_digest(),
            state,
        )
    } else {
        let reference = reference.ok_or(ServiceError::InvalidResult)?;
        let book = (event_class == LiveEventClass::BookSnapshot)
            .then(|| BookStateBinding::new(MarketDepth::PriceLevel, symbol.clone(), state.clone()));
        LiveEvidenceBinding::new(
            metadata.source_id().clone(),
            capture.stream_identity().clone(),
            metadata.revision().clone(),
            metadata.authorization().basis().clone(),
            venue.clone(),
            reference.instrument_id(),
            connection,
            qualification.provider_product().clone(),
            qualification.provider_channel().clone(),
            event_class,
            symbol,
            frame.payload_digest(),
            state,
            book,
        )
        .map_err(|_| ServiceError::InvalidResult)?
    };
    let source_at = if event_class == LiveEventClass::Quote {
        streamer_quote_source_timestamp(record).map_err(|_| ServiceError::InvalidResult)?
    } else {
        Some(streamer_family_source_timestamp(record).map_err(|_| ServiceError::InvalidResult)?)
    };
    LiveProvenance::decoded(DecodedLiveProvenanceInput::new(
        binding,
        source_at,
        received,
        observed,
        observed,
        qualification.quality(),
        CoverageStatus::Unknown,
        PayloadReference::ContentHash(PayloadHash::new(
            DigestAlgorithm::Sha256,
            frame.payload_digest().bytes(),
        )),
    ))
    .map_err(|_| ServiceError::InvalidResult)
}
