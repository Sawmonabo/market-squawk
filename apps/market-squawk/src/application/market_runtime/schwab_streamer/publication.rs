//! Same-family original ACK/data proofs and mixed market-event publication.
use super::*;
use crate::application::research::{
    MarketEventPublicationReceipt, MarketEventSealedReceiptEvidence,
};
use crate::application::{ResearchProviderRuntimeGeneration, SchwabStreamerPublicationPackage};
use crate::provider_activation::{
    MarketReferenceIdentityApprovalV1, SchwabMarketDataAccountActivation,
    SchwabQuotePublicationSelection, SchwabQuoteReferenceBinding,
};
use market_squawk_data::{
    ListingReferenceGenerationReceipt, ListingReferenceReadCapability,
    MarketDataInstrumentReadCapability,
};
use market_squawk_domain::{LiveEventClass, VenueId};
use market_squawk_sources::ProviderNativeLineageImplementation;

pub(super) struct Consumer {
    activation: Arc<SchwabMarketDataAccountActivation>,
    generation: ResearchProviderRuntimeGeneration,
    publication: SchwabStreamerPublicationPackage,
    bindings: Vec<(
        SchwabQuoteReferenceBinding,
        Option<MarketReferenceIdentityApprovalV1>,
    )>,
    canonical: MarketDataInstrumentReadCapability,
    listing: Option<ListingReferenceReadCapability>,
    nasdaq_generation: Option<ListingReferenceGenerationReceipt>,
    venue: VenueId,
    dictionary: SchwabStreamerFieldDictionary,
    proofs: super::families::Proofs,
    selected: std::collections::BTreeMap<MarketDataService, BTreeSet<String>>,
    ready: Option<oneshot::Sender<()>>,
    physical_capture_failed: bool,
}
impl Consumer {
    #[allow(
        clippy::too_many_arguments,
        reason = "source generation retains original account, reference and readiness authorities"
    )]
    pub(super) fn new(
        activation: Arc<SchwabMarketDataAccountActivation>,
        generation: ResearchProviderRuntimeGeneration,
        publication: SchwabStreamerPublicationPackage,
        bindings: Vec<(
            SchwabQuoteReferenceBinding,
            Option<MarketReferenceIdentityApprovalV1>,
        )>,
        canonical: MarketDataInstrumentReadCapability,
        listing: Option<ListingReferenceReadCapability>,
        nasdaq_generation: Option<ListingReferenceGenerationReceipt>,
        venue: VenueId,
        dictionary: SchwabStreamerFieldDictionary,
        ready: oneshot::Sender<()>,
    ) -> Result<Self, ServiceError> {
        let selected = super::families::selections(&bindings)?
            .into_iter()
            .map(|(service, keys)| {
                (
                    service,
                    keys.into_iter()
                        .map(|key| key.as_str().to_owned())
                        .collect(),
                )
            })
            .collect();
        Ok(Self {
            activation,
            generation,
            publication,
            bindings,
            canonical,
            listing,
            nasdaq_generation,
            venue,
            dictionary,
            proofs: super::families::Proofs::default(),
            selected,
            ready: Some(ready),
            physical_capture_failed: false,
        })
    }
    pub(super) fn capture_cleanup(&self) -> Result<(), ServiceError> {
        if self.physical_capture_failed {
            Err(ServiceError::Unavailable)
        } else {
            Ok(())
        }
    }
    pub(super) async fn consume(
        &mut self,
        batch: StreamerMicrobatch,
        current: &mut SchwabRestQuoteCurrentSessionInput,
        parse: ParseBounds,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        let original = if batch.frames().len() == 1 {
            Some((
                batch.frames()[0].kind(),
                bytes::Bytes::copy_from_slice(batch.frames()[0].payload()),
            ))
        } else {
            None
        };
        let sealed = match self
            .publication
            .authority
            .seal_microbatch(batch, parse)
            .await
        {
            Ok(sealed) => sealed,
            Err(error) => {
                self.physical_capture_failed = true;
                tracing::error!(%error, "Schwab completed native capture did not seal");
                return Err(ServiceError::Unavailable);
            }
        };
        if cancellation.is_cancelled() {
            return Ok(());
        }
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(20))
            .ok_or(ServiceError::Internal)?;
        if sealed.streamer_receipt().generation().get() != current.connection_generation().get() {
            return Err(ServiceError::InvalidResult);
        }
        let has_data = sealed
            .parsed_frames()
            .iter()
            .filter_map(Option::as_ref)
            .any(|frame| !frame.value().data.is_empty());
        if !sealed.service_responses().is_empty() {
            self.proofs.retain_control(sealed)?;
            return Ok(());
        }
        if !has_data {
            return Ok(());
        }
        self.proofs.observe_data(&sealed)?;
        let (kind, body) = original.ok_or(ServiceError::InvalidResult)?;
        if sealed.frames().len() != 1 {
            return Err(ServiceError::InvalidResult);
        }
        let frame = &sealed.frames()[0];
        let received = timestamp_millis(frame.received_at_unix_millis())?;
        let observed = now()?;
        if observed < received {
            return Err(ServiceError::InvalidResult);
        }
        let (token, epoch) = tokio::select! {biased; ()=cancellation.cancelled()=>return Err(ServiceError::Cancelled), ()=tokio::time::sleep_until(deadline.into())=>return Err(ServiceError::DeadlineExceeded), value=self.activation.acquire_publication_attempt()=>value.map_err(|_|ServiceError::Unauthorized)?,};
        drop(token);
        let oauth = epoch.receipt();
        if oauth.generation() != sealed.streamer_receipt().token_generation() {
            return Err(ServiceError::Unauthorized);
        }
        self.activation
            .validate_current_quote_bindings(
                self.generation.metadata(),
                &self.bindings,
                self.nasdaq_generation.as_ref(),
                observed,
                50,
                true,
            )
            .map_err(|_| ServiceError::Unavailable)?;
        if let Some(expected) = &self.nasdaq_generation {
            if self
                .listing
                .as_ref()
                .ok_or(ServiceError::Unavailable)?
                .current(deadline, cancellation)
                .map_err(|_| ServiceError::Unavailable)?
                .as_ref()
                != Some(expected)
            {
                return Err(ServiceError::Unavailable);
            }
        }
        for (binding, _) in &self.bindings {
            if self
                .canonical
                .latest(binding.instrument_id(), deadline, cancellation)
                .map_err(|_| ServiceError::Unavailable)?
                .as_ref()
                != Some(binding.canonical_record())
            {
                return Err(ServiceError::Unavailable);
            }
        }
        let parsed = sealed.parsed_frames()[0]
            .as_ref()
            .ok_or(ServiceError::InvalidResult)?;
        let mut inputs = Vec::new();
        let mut family_inputs = Vec::new();
        let mut current_records = Vec::new();
        let mut selected_services = BTreeSet::new();
        let mut qualifications = std::collections::BTreeMap::new();
        for (batch_index, batch) in parsed.value().data.iter().enumerate() {
            let Some(handoff) = self.proofs.handoff(batch.service) else {
                continue;
            };
            let qualification = SchwabMarketDataQualification::try_from_streamer_handoff(
                self.activation.doctor_receipt(),
                handoff,
                received,
                oauth,
            )
            .map_err(|_| ServiceError::Unavailable)?;
            if qualifications
                .insert(batch.service, qualification.clone())
                .is_some_and(|previous| previous != qualification)
            {
                return Err(ServiceError::InvalidResult);
            }
            let dictionary = super::dictionary::dictionary(batch.service)?;
            for (content_index, record) in canonicalize_streamer_batch(batch, &dictionary)
                .map_err(|_| ServiceError::InvalidResult)?
                .into_iter()
                .enumerate()
            {
                if !self
                    .selected
                    .get(&batch.service)
                    .is_some_and(|keys| keys.contains(record.provider_identifier.as_str()))
                {
                    return Err(ServiceError::InvalidResult);
                }
                let b = u16::try_from(batch_index).map_err(|_| ServiceError::InvalidResult)?;
                let c = u16::try_from(content_index).map_err(|_| ServiceError::InvalidResult)?;
                let class = super::families::class(batch.service);
                let reference = if class == LiveEventClass::Screener {
                    None
                } else {
                    Some(
                        self.bindings
                            .iter()
                            .find(|(binding, _)| {
                                binding.provider_symbol() == record.provider_identifier.as_str()
                            })
                            .ok_or(ServiceError::InvalidResult)?
                            .0
                            .quote_reference(received)
                            .map_err(|_| ServiceError::InvalidResult)?,
                    )
                };
                let mut item_references = Vec::new();
                if class == LiveEventClass::Screener {
                    for field in &record.fields {
                        if let StreamerNativeValue::ScreenerItems(items) = &field.value {
                            for item in items {
                                let symbol = item
                                    .fields()
                                    .iter()
                                    .find(|field| {
                                        *field.name() == SchwabStreamerScreenerField::Symbol
                                    })
                                    .map(|field| field.value());
                                if let Some(NativeScalar::Text(symbol)) = symbol {
                                    if let Some((binding, _)) =
                                        self.bindings.iter().find(|(binding, _)| {
                                            binding.provider_symbol() == symbol.as_ref()
                                        })
                                    {
                                        item_references.push(
                                            binding
                                                .quote_reference(received)
                                                .map_err(|_| ServiceError::InvalidResult)?,
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
                let provenance = super::families::provenance(
                    self.generation.metadata(),
                    &sealed,
                    &record,
                    reference.as_ref(),
                    &item_references,
                    &qualification,
                    &self.venue,
                    b,
                    c,
                    received,
                    observed,
                )?;
                if class == LiveEventClass::Quote {
                    let reference = reference.ok_or(ServiceError::InvalidResult)?;
                    if batch.service == MarketDataService::LevelOneEquities {
                        current_records.push((record.clone(), reference.clone(), b, c));
                    }
                    inputs.push(SchwabStreamerQuoteRecordRequest::new(
                        0,
                        b,
                        c,
                        dictionary.clone(),
                        reference,
                        provenance,
                        SchwabStreamerQuoteMarketDataEvidence::try_new(
                            self.venue.clone(),
                            qualification.clone(),
                        )
                        .map_err(|_| ServiceError::InvalidResult)?,
                    ));
                } else {
                    family_inputs.push(
                        SchwabStreamerFamilyRecordRequest::try_new(
                            0,
                            b,
                            c,
                            dictionary.clone(),
                            reference,
                            item_references,
                            provenance,
                            qualification.clone(),
                        )
                        .map_err(|_| ServiceError::InvalidResult)?,
                    );
                }
                selected_services.insert(batch.service);
            }
        }
        let observational_only = self.generation.metadata().coverage().live_channels().len() != 1
            || self.generation.metadata().coverage().delay()
                == market_squawk_domain::CoverageDelay::Unknown;
        let current_evidence = if current_records.is_empty() || observational_only {
            None
        } else {
            Some(
                SchwabStreamerCurrentEvidence::from_original_capture(
                    &sealed,
                    &self.dictionary,
                    current_records,
                )
                .and_then(|evidence| evidence.attach_original_body(kind, body))
                .map_err(|_| ServiceError::InvalidResult)?,
            )
        };
        let had_current_quote = current_evidence.is_some();
        let health_service = if had_current_quote {
            MarketDataService::LevelOneEquities
        } else {
            *selected_services
                .iter()
                .next()
                .ok_or(ServiceError::InvalidResult)?
        };
        let health_qualification = qualifications
            .get(&health_service)
            .ok_or(ServiceError::InvalidResult)?;
        let qualified = current
            .qualify_streamer_current(
                current_evidence,
                &sealed,
                self.generation.metadata(),
                health_qualification,
                deadline,
            )
            .map_err(|_| ServiceError::Unavailable)?;
        let selection = SchwabQuotePublicationSelection::try_new(
            qualified.source_lease().clone(),
            qualified.selected_provider_identities().to_vec(),
            self.bindings.iter().map(|(binding, _)| binding),
            &self.venue,
            observed,
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        let account = self
            .activation
            .currentness()
            .try_acquire_publication_authority()
            .map_err(|_| ServiceError::Unauthorized)?;
        let request = SchwabStreamerQuotePublicationRequest::new(
            selected_services
                .iter()
                .map(|service| {
                    self.proofs
                        .handoff(*service)
                        .ok_or(ServiceError::InvalidResult)
                })
                .collect::<Result<Vec<_>, _>>()?,
            inputs,
        )
        .with_family_records(family_inputs);
        let listing = match (&self.listing, &self.nasdaq_generation) {
            (Some(reader), Some(expected)) => Some((reader.clone(), expected.clone())),
            (_, None) => None,
            _ => return Err(ServiceError::Unavailable),
        };
        let references = crate::provider_activation::SchwabQuoteReferencePrecommit::new(
            self.canonical.clone(),
            self.bindings
                .iter()
                .map(|(binding, _)| binding.canonical_record().clone())
                .collect(),
            listing,
            deadline,
            cancellation.clone(),
        );
        let outcome = self
            .publication
            .authority
            .publish(
                sealed,
                request,
                epoch,
                account,
                references,
                selection,
                observed,
                cancellation.child_token(),
                deadline,
            )
            .await
            .map_err(|_| ServiceError::Unavailable)?;
        let dispositions = match &outcome {
            SchwabStreamerApplicationOutcome::Published(published) => published.dispositions(),
            SchwabStreamerApplicationOutcome::SealedRaw(raw) => raw.dispositions(),
        };
        if dispositions.iter().any(|item| {
            matches!(
                item.reason(),
                SchwabStreamerRecordDispositionReason::CanonicalMappingRejected
            )
        }) {
            return Err(ServiceError::InvalidResult);
        }
        // Missing same-family entitlement/identity produces raw-only disposition, never an invented event.
        let durably_published = matches!(&outcome, SchwabStreamerApplicationOutcome::Published(_));
        if let SchwabStreamerApplicationOutcome::Published(published) = outcome {
            let generation = published.generation();
            let receipt = MarketEventPublicationReceipt::try_new(
                generation.restart_selector().manifest().clone(),
                generation.publication_digest(),
                market_squawk_data::ProviderMarketEventPublicationKind::EventMicrobatch,
                ProviderNativeLineageImplementation::SchwabStreamerMarketDataV1,
                self.generation.metadata().source_id().clone(),
                generation.provider_dataset().clone(),
                MarketEventSealedReceiptEvidence::Single(generation.sealed_receipt_digest()),
                generation.event_count(),
            )
            .map_err(|_| ServiceError::InvalidResult)?;
            self.publication
                .durable_writer
                .retain(receipt)
                .await
                .map_err(|_| ServiceError::Unavailable)?;
        }
        if cancellation.is_cancelled() {
            return Err(ServiceError::Cancelled);
        }
        for (binding, _) in &self.bindings {
            if self
                .canonical
                .latest(binding.instrument_id(), deadline, cancellation)
                .map_err(|_| ServiceError::Unavailable)?
                .as_ref()
                != Some(binding.canonical_record())
            {
                return Err(ServiceError::Unavailable);
            }
        }
        let _current_account = self
            .activation
            .currentness()
            .try_acquire_publication_authority()
            .map_err(|_| ServiceError::Unauthorized)?;
        if had_current_quote {
            let result = current
                .publish_qualified_streamer(qualified, deadline)
                .map_err(|_| ServiceError::Unavailable)?;
            if result.published() > 0 {
                if let Some(ready) = self.ready.take() {
                    let _ = ready.send(());
                }
            }
        } else if observational_only && durably_published {
            // This is durable observational readiness only. The current-price registry remains
            // unqualified and cannot supply an executable mark from these family observations.
            if let Some(ready) = self.ready.take() {
                let _ = ready.send(());
            }
        }
        Ok(())
    }
}
fn timestamp_millis(value: u64) -> Result<Timestamp, ServiceError> {
    Ok(Timestamp::from_unix_nanos(
        i64::try_from(value)
            .ok()
            .and_then(|value| value.checked_mul(1_000_000))
            .ok_or(ServiceError::InvalidResult)?,
    ))
}
