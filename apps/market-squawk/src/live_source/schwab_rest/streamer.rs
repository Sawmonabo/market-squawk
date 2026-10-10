//! Source-owned accumulated quote state over exact sealed current-generation native deltas.
use super::*;
use market_squawk_adapter_schwab::{
    MarketDataService, NativeScalar, RawStreamerFrameKind, SchwabCanonicalStreamerRecord,
    SchwabMarketDataQualification, SchwabSealedStreamerCapture, SchwabStreamerFieldDictionary,
    SchwabStreamerSemanticField, StreamerNativeValue, canonicalize_streamer_batch,
    streamer_quote_source_timestamp,
};
use market_squawk_domain::{EvidenceDigest, MarketDataReference, VenueSymbol};
use market_squawk_sources::{
    ProviderAccumulatedQuoteEvidence, ProviderNativeInstrumentIdentity, ProviderQuoteFieldOrigin,
    ProviderQuoteSizeUnit,
};
use std::{collections::BTreeMap, num::NonZeroU64};

#[derive(Clone, Debug, Default)]
pub(super) struct SchwabStreamerQuoteState {
    instruments: BTreeMap<InstrumentId, QuoteFields>,
}
#[derive(Clone, Debug, Default)]
struct QuoteFields {
    bid: Option<Field>,
    ask: Option<Field>,
    bid_size: Option<Field>,
    ask_size: Option<Field>,
    published: bool,
}
#[derive(Clone, Debug)]
struct Field {
    value: ProviderDecimalLexeme,
    origin: ProviderQuoteFieldOrigin,
}

#[derive(Debug)]
pub(crate) struct SchwabStreamerCurrentEvidence {
    body: Bytes,
    payload_digest: [u8; 32],
    generation: u64,
    sealed_receipt: EvidenceDigest,
    native_ordinal: NonZeroU64,
    native_received_at: Timestamp,
    records: Vec<(SchwabCanonicalStreamerRecord, MarketDataReference, u16, u16)>,
}
impl SchwabStreamerCurrentEvidence {
    /// Original parsed rows are checked against their exact sealed native frame. Neither a durable
    /// canonical read nor a caller-authored quote can substitute for this non-serializable input.
    pub(crate) fn from_original_capture(
        capture: &SchwabSealedStreamerCapture,
        dictionary: &SchwabStreamerFieldDictionary,
        records: Vec<(SchwabCanonicalStreamerRecord, MarketDataReference, u16, u16)>,
    ) -> Result<Self, SchwabRestQuoteCurrentUnavailable> {
        if capture.frames().len() != 1 || capture.parsed_frames().len() != 1 {
            return Err(SchwabRestQuoteCurrentUnavailable::Decode);
        }
        let frame = &capture.frames()[0];
        let parsed = capture.parsed_frames()[0]
            .as_ref()
            .ok_or(SchwabRestQuoteCurrentUnavailable::Decode)?;
        let mut index = 0usize;
        for (batch_index, batch) in parsed.value().data.iter().enumerate() {
            if batch.service != dictionary.service() {
                continue;
            }
            for (content_index, record) in canonicalize_streamer_batch(batch, dictionary)
                .map_err(|_| SchwabRestQuoteCurrentUnavailable::Decode)?
                .into_iter()
                .enumerate()
            {
                let Some((actual, reference, b, c)) = records.get(index) else {
                    return Err(SchwabRestQuoteCurrentUnavailable::Decode);
                };
                if actual != &record
                    || usize::from(*b) != batch_index
                    || usize::from(*c) != content_index
                    || reference
                        .provider_identity()
                        .ok_or(SchwabRestQuoteCurrentUnavailable::Decode)?
                        .provider_instrument_id()
                        .as_str()
                        != record.provider_identifier.as_str()
                {
                    return Err(SchwabRestQuoteCurrentUnavailable::Decode);
                }
                index = index
                    .checked_add(1)
                    .ok_or(SchwabRestQuoteCurrentUnavailable::Allocation)?;
            }
        }
        if index != records.len()
            || index > 50
            || parsed.raw_sha256() != frame.payload_digest().bytes()
        {
            return Err(SchwabRestQuoteCurrentUnavailable::Decode);
        }
        let native_received_at = Timestamp::from_unix_nanos(
            i64::try_from(frame.received_at_unix_millis())
                .ok()
                .and_then(|v| v.checked_mul(1_000_000))
                .ok_or(SchwabRestQuoteCurrentUnavailable::Decode)?,
        );
        Ok(Self {
            body: Bytes::new(),
            payload_digest: frame.payload_digest().bytes(),
            generation: capture.streamer_receipt().generation().get(),
            sealed_receipt: capture.persisted_receipt().receipt_digest(),
            native_ordinal: frame.transport_ordinal(),
            native_received_at,
            records,
        })
    }
    pub(crate) fn attach_original_body(
        mut self,
        kind: RawStreamerFrameKind,
        body: Bytes,
    ) -> Result<Self, SchwabRestQuoteCurrentUnavailable> {
        use sha2::{Digest as _, Sha256};
        if kind != RawStreamerFrameKind::Text
            || <[u8; 32]>::from(Sha256::digest(&body)) != self.payload_digest
        {
            return Err(SchwabRestQuoteCurrentUnavailable::Decode);
        }
        self.body = body;
        Ok(self)
    }
}
impl SchwabRestQuoteCurrentSessionInput {
    pub(crate) fn qualify_streamer_current(
        &mut self,
        evidence: Option<SchwabStreamerCurrentEvidence>,
        sealed: &SchwabSealedStreamerCapture,
        metadata: &SourceMetadata,
        qualification: &SchwabMarketDataQualification,
        oauth: SchwabOAuthAuthorityReceipt,
        deadline: Instant,
    ) -> Result<SchwabQualifiedCurrent, SchwabRestQuoteCurrentUnavailable> {
        require_deadline(deadline)?;
        let [sealed_frame] = sealed.frames() else {
            return Err(SchwabRestQuoteCurrentUnavailable::Decode);
        };
        let [Some(parsed)] = sealed.parsed_frames() else {
            return Err(SchwabRestQuoteCurrentUnavailable::Decode);
        };
        let service = qualification
            .streamer_service()
            .ok_or(SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth)?;
        if qualification.token_generation() != oauth.generation()
            || qualification.credential_authority() != oauth.credential_authority()
            || sealed.streamer_receipt().token_generation() != oauth.generation()
            || sealed.streamer_receipt().credential_authority() != oauth.credential_authority()
            || !parsed
                .value()
                .data
                .iter()
                .any(|batch| batch.service == service)
            || parsed.raw_sha256() != sealed_frame.payload_digest().bytes()
            || sealed_frame.generation().get() != self.session.generation().get()
            || sealed.streamer_receipt().generation().get() != self.session.generation().get()
        {
            return Err(SchwabRestQuoteCurrentUnavailable::Decode);
        }
        let live = metadata
            .coverage()
            .live_for(
                qualification.provider_product(),
                qualification.provider_channel(),
            )
            .ok_or(SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth)?;
        let Some(evidence) = evidence else {
            if metadata.source_id() != self.session.source_id()
                || metadata.revision() != self.session.revision()
            {
                return Err(SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth);
            }
            let observed_at = timestamp_from_millis(sealed_frame.received_at_unix_millis())?;
            self.record_current_health(
                metadata,
                oauth,
                live,
                observed_at,
                None,
                sealed_frame.payload_digest().bytes(),
            )
            .map_err(|error| {
                tracing::warn!(
                    ?service,
                    generation = self.session.generation().get(),
                    first_ordinal = sealed.streamer_receipt().first_ordinal().get(),
                    received_at_millis = sealed_frame.received_at_unix_millis(),
                    "stream health qualification failed"
                );
                error
            })?;
            let venue = self
                .display_ingresses
                .first()
                .ok_or(SchwabRestQuoteCurrentUnavailable::Display)?
                .key()
                .venue_id()
                .clone();
            if self
                .display_ingresses
                .iter()
                .any(|ingress| ingress.key().venue_id() != &venue)
            {
                return Err(SchwabRestQuoteCurrentUnavailable::Display);
            }
            return self.qualify_selected(
                &venue,
                self.display_ingresses
                    .iter()
                    .map(|ingress| ingress.key().instrument_id()),
                observed_at,
                Vec::new(),
                0,
            );
        };
        if service != MarketDataService::LevelOneEquities {
            return Err(SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth);
        }
        if evidence.body.is_empty()
            || evidence.generation != self.session.generation().get()
            || metadata.source_id() != self.session.source_id()
            || metadata.revision() != self.session.revision()
            || evidence.payload_digest != sealed_frame.payload_digest().bytes()
        {
            return Err(SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth);
        }
        let SourceProtocolProfile::Live(protocol) = metadata.protocol_profile() else {
            return Err(SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth);
        };
        let SequenceValidationProfile::Unsupported { rule: sequence } = protocol.sequence() else {
            return Err(SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth);
        };
        let ChecksumValidationProfile::Unsupported { rule: checksum } = protocol.checksum() else {
            return Err(SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth);
        };
        let snapshot = metadata
            .coverage()
            .live_for(
                &market_squawk_domain::ProviderProduct::new(
                    SourceIdentifier::try_from("schwab-streamer")
                        .map_err(|_| SchwabRestQuoteCurrentUnavailable::Decode)?,
                ),
                &market_squawk_domain::ProviderChannel::new(
                    SourceIdentifier::try_from("schwab-streamer-level-one-equities")
                        .map_err(|_| SchwabRestQuoteCurrentUnavailable::Decode)?,
                ),
            )
            .and_then(|live| live.rule_for(market_squawk_domain::LiveEventClass::Quote, None))
            .and_then(|rule| match rule.snapshot_applicability() {
                SnapshotApplicability::NotApplicable { metadata_rule } => {
                    Some(metadata_rule.clone())
                }
                _ => None,
            })
            .ok_or(SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth)?;
        let frame = self
            .raw_frames
            .try_frame(TransportFrameKind::Text, evidence.body)
            .map_err(|_| SchwabRestQuoteCurrentUnavailable::Capture)?;
        let receipt = self
            .capture
            .try_publish(&frame)
            .map_err(|_| SchwabRestQuoteCurrentUnavailable::Capture)?;
        let validated = self
            .session
            .validate_live_frame(&frame)
            .map_err(|_| SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth)?;
        let decoder =
            DecoderEvidence::from_validated_frame(&validated, protocol.decoder_rule().clone());
        if decoder.payload_digest().bytes() != evidence.payload_digest {
            return Err(SchwabRestQuoteCurrentUnavailable::Decode);
        }
        let mut observations = Vec::new();
        let mut latest: Option<Timestamp> = None;
        let mut next_streamer_state = self.streamer_state.clone();
        let venue =
            VenueId::try_from("schwab").map_err(|_| SchwabRestQuoteCurrentUnavailable::Decode)?;
        for (record, reference, batch_ordinal, content_ordinal) in evidence.records {
            let instrument = reference.instrument_id();
            let source_identifier = SourceIdentifier::try_from(record.provider_identifier.as_str())
                .map_err(|_| SchwabRestQuoteCurrentUnavailable::Decode)?;
            let source_at = streamer_quote_source_timestamp(&record)
                .map_err(|_| SchwabRestQuoteCurrentUnavailable::Decode)?;
            if let Some(clock) = source_at {
                latest = Some(latest.map_or(clock, |prior| prior.max(clock)));
            }
            let origin = ProviderQuoteFieldOrigin::try_new(
                decoder.clone(),
                instrument,
                venue.clone(),
                source_identifier.clone(),
                evidence.sealed_receipt,
                evidence.native_ordinal,
                batch_ordinal,
                content_ordinal,
                evidence.native_received_at,
                source_at,
            )
            .map_err(|_| SchwabRestQuoteCurrentUnavailable::Decode)?;
            if next_streamer_state.instruments.len() >= 50
                && !next_streamer_state.instruments.contains_key(&instrument)
            {
                return Err(SchwabRestQuoteCurrentUnavailable::Allocation);
            }
            let fields = next_streamer_state
                .instruments
                .entry(instrument)
                .or_default();
            let mut changed = false;
            for value in &record.fields {
                let target = match value.meaning {
                    SchwabStreamerSemanticField::BidPrice => &mut fields.bid,
                    SchwabStreamerSemanticField::AskPrice => &mut fields.ask,
                    SchwabStreamerSemanticField::BidSize => &mut fields.bid_size,
                    SchwabStreamerSemanticField::AskSize => &mut fields.ask_size,
                    _ => continue,
                };
                changed = true;
                *target = match &value.value {
                    StreamerNativeValue::Scalar(NativeScalar::Null) => None,
                    StreamerNativeValue::Scalar(NativeScalar::Number(number)) => {
                        let value = ProviderDecimalLexeme::try_new(number.as_str())
                            .map_err(|_| SchwabRestQuoteCurrentUnavailable::Decode)?;
                        if value.decimal() < rust_decimal::Decimal::ZERO {
                            return Err(SchwabRestQuoteCurrentUnavailable::Decode);
                        }
                        Some(Field {
                            value,
                            origin: origin.clone(),
                        })
                    }
                    _ => return Err(SchwabRestQuoteCurrentUnavailable::Decode),
                };
            }
            if !changed {
                continue;
            }
            let bid = accumulated_side(fields.bid.as_ref(), fields.bid_size.as_ref())?;
            let ask = accumulated_side(fields.ask.as_ref(), fields.ask_size.as_ref())?;
            if bid.is_none() && ask.is_none() {
                // An explicit clearing update must not leave a formerly available old quote
                // current. Revoking this generation is conservative when no side remains.
                if fields.published {
                    return Err(SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth);
                }
                continue;
            }
            fields.published = true;
            // This is the minimum original source clock across retained fields, never a new
            // timestamp copied onto unrelated old values. Missing native clocks stay explicit.
            let origins = bid
                .iter()
                .chain(ask.iter())
                .filter_map(ProviderBookLevel::accumulated_evidence)
                .flat_map(|e| [e.price(), e.quantity()])
                .collect::<Vec<_>>();
            let clock = if origins.iter().all(|o| o.source_at().is_some()) {
                origins.iter().filter_map(|o| o.source_at()).min()
            } else {
                None
            };
            let timestamp = clock.map_or_else(
                || {
                    ProviderTimestampEvidence::AuthoritativelyAbsent(
                        protocol.timestamp_rule().clone(),
                    )
                },
                |value| ProviderTimestampEvidence::Provided {
                    value,
                    rule: protocol.timestamp_rule().clone(),
                },
            );
            let provider_identity = reference
                .provider_identity()
                .ok_or(SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth)?;
            let native_identity = ProviderNativeInstrumentIdentity::new(
                provider_identity.source_id().clone(),
                provider_identity.provider_instrument_id().clone(),
                VenueSymbol::try_from(record.provider_identifier.as_str())
                    .map_err(|_| SchwabRestQuoteCurrentUnavailable::Decode)?,
            );
            observations.push(
                ProviderNormalizedObservation::try_new(
                    source_identifier,
                    venue.clone(),
                    instrument,
                    native_identity,
                    timestamp,
                    ProviderSequenceEvidence::Unsupported {
                        rule: sequence.clone(),
                    },
                    ProviderSnapshotEvidence::NotApplicable(snapshot.clone()),
                    ProviderChecksumEvidence::Unsupported {
                        rule: checksum.clone(),
                    },
                    ProviderObservationPayload::quote(bid, ask)
                        .map_err(|_| SchwabRestQuoteCurrentUnavailable::Decode)?,
                )
                .map_err(|_| SchwabRestQuoteCurrentUnavailable::Decode)?,
            );
        }
        if observations.is_empty() {
            let observed_at = frame.received_at();
            self.record_current_health(
                metadata,
                oauth,
                live,
                observed_at,
                latest,
                evidence.payload_digest,
            )?;
            let mut qualified = self.qualify_selected(
                &venue,
                self.display_ingresses
                    .iter()
                    .map(|ingress| ingress.key().instrument_id()),
                observed_at,
                Vec::new(),
                0,
            )?;
            qualified.streamer_state = Some(next_streamer_state);
            return Ok(qualified);
        }
        let batch = DecodedProviderBatch::try_new(decoder, observations)
            .map_err(|_| SchwabRestQuoteCurrentUnavailable::Decode)?;
        let selected_instruments = self
            .display_ingresses
            .iter()
            .map(|ingress| ingress.key().instrument_id())
            .collect::<Vec<_>>();
        let mut qualified = self.qualify_decoded(
            metadata,
            oauth,
            live,
            evidence.payload_digest,
            &venue,
            selected_instruments.into_iter(),
            DecodedQuotes {
                batch,
                latest_source_at: latest,
            },
            receipt,
        )?;
        qualified.streamer_state = Some(next_streamer_state);
        Ok(qualified)
    }

    pub(crate) fn publish_qualified_streamer(
        &mut self,
        qualified: SchwabQualifiedCurrent,
        deadline: Instant,
    ) -> Result<SchwabRestQuoteCurrentPublication, SchwabRestQuoteCurrentUnavailable> {
        self.publish_qualified_batches(qualified, deadline)
    }
}
fn accumulated_side(
    price: Option<&Field>,
    size: Option<&Field>,
) -> Result<Option<ProviderBookLevel>, SchwabRestQuoteCurrentUnavailable> {
    let (Some(price), Some(size)) = (price, size) else {
        return Ok(None);
    };
    if price.value.decimal() <= rust_decimal::Decimal::ZERO {
        return Ok(None);
    }
    let original = ProviderAccumulatedQuoteEvidence::try_new(
        price.origin.clone(),
        size.origin.clone(),
        ProviderQuoteSizeUnit::UnresolvedNative,
    )
    .map_err(|_| SchwabRestQuoteCurrentUnavailable::Decode)?;
    Ok(Some(ProviderBookLevel::from_accumulated(
        ProviderPrice::new(price.value.clone()),
        ProviderQuantity::new(size.value.clone()),
        original,
    )))
}
