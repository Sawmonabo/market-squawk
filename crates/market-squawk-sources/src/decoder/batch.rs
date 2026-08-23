/// Bounded provider-normalized observation that has not yet mutated live state.
#[derive(Clone, Debug)]
pub struct ProviderNormalizedObservation {
    source_identifier: SourceIdentifier,
    venue: VenueId,
    instrument: InstrumentId,
    timestamp: ProviderTimestampEvidence,
    sequence: ProviderSequenceEvidence,
    snapshot: ProviderSnapshotEvidence,
    checksum: ProviderChecksumEvidence,
    payload: ProviderObservationPayload,
}

impl ProviderNormalizedObservation {
    /// Constructs a relationally checked pre-state observation.
    ///
    /// # Errors
    ///
    /// Rejects excessive numeric fields or snapshot/depth evidence inconsistent with event class.
    #[allow(
        clippy::too_many_arguments,
        reason = "pre-state protocol evidence dimensions are intentionally explicit"
    )]
    pub fn try_new(
        source_identifier: SourceIdentifier,
        venue: VenueId,
        instrument: InstrumentId,
        timestamp: ProviderTimestampEvidence,
        sequence: ProviderSequenceEvidence,
        snapshot: ProviderSnapshotEvidence,
        checksum: ProviderChecksumEvidence,
        payload: ProviderObservationPayload,
    ) -> Result<Self, DecodeError> {
        let event_class = payload.event_class();
        let valid_state_relation = if event_class.requires_book_state() {
            matches!(
                snapshot,
                ProviderSnapshotEvidence::InitializingSnapshot { .. }
                    | ProviderSnapshotEvidence::Delta { .. }
            )
        } else {
            matches!(snapshot, ProviderSnapshotEvidence::NotApplicable(_))
        };
        if !valid_state_relation {
            return Err(DecodeError::InvalidProviderEvidence);
        }
        Ok(Self {
            source_identifier,
            venue,
            instrument,
            timestamp,
            sequence,
            snapshot,
            checksum,
            payload,
        })
    }

    /// Returns the provider object/message identity.
    pub const fn source_identifier(&self) -> &SourceIdentifier {
        &self.source_identifier
    }

    /// Returns the provider venue.
    pub const fn venue(&self) -> &VenueId {
        &self.venue
    }

    /// Returns the resolved internal instrument.
    pub const fn instrument(&self) -> InstrumentId {
        self.instrument
    }

    /// Returns the provider event class.
    pub const fn event_class(&self) -> LiveEventClass {
        self.payload.event_class()
    }

    /// Returns provider market-depth semantics.
    pub const fn depth(&self) -> Option<MarketDepth> {
        self.payload.depth()
    }

    /// Returns unqualified provider timestamp evidence.
    pub const fn timestamp(&self) -> &ProviderTimestampEvidence {
        &self.timestamp
    }

    /// Returns unvalidated provider sequence evidence.
    pub const fn sequence(&self) -> &ProviderSequenceEvidence {
        &self.sequence
    }

    /// Returns pre-state snapshot relationship evidence.
    pub const fn snapshot(&self) -> &ProviderSnapshotEvidence {
        &self.snapshot
    }

    /// Returns unvalidated checksum material.
    pub const fn checksum(&self) -> &ProviderChecksumEvidence {
        &self.checksum
    }

    /// Returns the typed message-atomic provider payload.
    pub const fn payload(&self) -> &ProviderObservationPayload {
        &self.payload
    }

    /// Returns every allocation uniquely owned by this observation, excluding inline storage.
    pub(crate) fn dynamic_retained_bytes(&self) -> Result<usize, DecodeError> {
        let timestamp_rule = match &self.timestamp {
            ProviderTimestampEvidence::AuthoritativelyAbsent(rule) => {
                rule.provider_rule().retained_bytes()
            }
            ProviderTimestampEvidence::Provided { rule, .. } => {
                rule.provider_rule().retained_bytes()
            }
        };
        let snapshot_bytes = match &self.snapshot {
            ProviderSnapshotEvidence::InitializingSnapshot { provider_reference } => {
                provider_reference
                    .as_ref()
                    .map_or(0, SourceIdentifier::retained_bytes)
            }
            ProviderSnapshotEvidence::Delta {
                provider_snapshot_reference,
            } => provider_snapshot_reference
                .as_ref()
                .map_or(0, SourceIdentifier::retained_bytes),
            ProviderSnapshotEvidence::NotApplicable(rule) => rule.provider_rule().retained_bytes(),
        };
        let checksum_bytes = match &self.checksum {
            ProviderChecksumEvidence::Provided { value, rule } => checked_sum([
                value.retained_bytes(),
                rule.provider_rule().retained_bytes(),
            ])?,
            ProviderChecksumEvidence::Unsupported { rule } => rule.provider_rule().retained_bytes(),
        };
        checked_sum([
            self.source_identifier.retained_bytes(),
            self.venue.retained_bytes(),
            timestamp_rule,
            sequence_dynamic_retained_bytes(&self.sequence),
            snapshot_bytes,
            checksum_bytes,
            self.payload.deep_retained_bytes()?,
        ])
    }
}

/// Intrinsically bounded pre-state observations emitted by one synchronous decode call.
///
/// This transient hot-path value deliberately has no Serde implementation.
#[derive(Clone, Debug)]
pub struct DecodedProviderBatch {
    evidence: DecoderEvidence,
    observations: BoundedVec<ProviderNormalizedObservation, MAX_DECODED_EVENTS>,
}

impl DecodedProviderBatch {
    /// Constructs a bounded batch and enforces the aggregate numeric-field ceiling.
    ///
    /// # Errors
    ///
    /// Rejects frame expansion beyond observation or aggregate-field limits.
    pub fn try_new(
        evidence: DecoderEvidence,
        observations: Vec<ProviderNormalizedObservation>,
    ) -> Result<Self, DecodeError> {
        if observations.is_empty() {
            return Err(DecodeError::EmptyBatch);
        }
        let observations = BoundedVec::try_new(observations)
            .map_err(|error| DecodeError::TooManyEvents { max: error.max })?;
        let mut book_item_count = 0_usize;
        for observation in observations.as_slice() {
            book_item_count = book_item_count
                .checked_add(observation.payload.book_item_count())
                .ok_or(DecodeError::TooManyNumericFields {
                    max: MAX_DECODED_BOOK_ITEMS,
                })?;
            if book_item_count > MAX_DECODED_BOOK_ITEMS {
                return Err(DecodeError::TooManyNumericFields {
                    max: MAX_DECODED_BOOK_ITEMS,
                });
            }
        }
        Ok(Self {
            evidence,
            observations,
        })
    }

    /// Returns exact frame and decoder evidence.
    pub const fn evidence(&self) -> &DecoderEvidence {
        &self.evidence
    }

    /// Returns provider observations in wire order.
    pub fn observations(&self) -> &[ProviderNormalizedObservation] {
        self.observations.as_slice()
    }

    pub(crate) fn into_parts(self) -> (DecoderEvidence, Vec<ProviderNormalizedObservation>) {
        (self.evidence, self.observations.into_vec())
    }

    /// Returns checked deep retained bytes for the closed decoded shape.
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError::RetainedSizeOverflow`] on arithmetic overflow.
    pub fn retained_bytes(&self) -> Result<usize, DecodeError> {
        std::mem::size_of::<Self>()
            .checked_add(self.dynamic_retained_bytes()?)
            .ok_or(DecodeError::RetainedSizeOverflow)
    }

    pub(super) fn dynamic_retained_bytes(&self) -> Result<usize, DecodeError> {
        let observations = self
            .observations
            .checked_allocation_bytes()
            .ok_or(DecodeError::RetainedSizeOverflow)?;
        let evidence = self.evidence.dynamic_retained_bytes()?;
        let deep = checked_sum(
            self.observations
                .as_slice()
                .iter()
                .map(ProviderNormalizedObservation::dynamic_retained_bytes)
                .collect::<Result<Vec<_>, _>>()?,
        )?;
        observations
            .checked_add(evidence)
            .and_then(|bytes| bytes.checked_add(deep))
            .ok_or(DecodeError::RetainedSizeOverflow)
    }
}

/// Synchronous object-safe provider decoder.
pub trait MarketDecoder: SourceMetadataProvider {
    /// Decodes one captured bounded frame without I/O or canonical live-state construction.
    ///
    /// # Errors
    ///
    /// Returns a typed failure; partial output is never returned.
    fn decode(
        &mut self,
        frame: &ValidatedRawMarketFrame<'_>,
    ) -> Result<DecodeOutcome, DecodeInternalError>;
}

/// Provider decode or batch-bound failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum DecodeError {
    /// Raw payload is malformed for the selected provider protocol.
    #[error("provider payload is malformed")]
    MalformedPayload,
    /// Decoder emitted no provider observation for an accepted frame.
    #[error("decoded provider batch must not be empty")]
    EmptyBatch,
    /// Provider evidence dimensions contradict event/state semantics.
    #[error("provider evidence is relationally inconsistent")]
    InvalidProviderEvidence,
    /// One raw frame expanded beyond the observation ceiling.
    #[error("decoded batch exceeds maximum observation count {max}")]
    TooManyEvents {
        /// Maximum observations per decoded batch.
        max: usize,
    },
    /// Numeric provider fields exceeded per-observation or aggregate bounds.
    #[error("decoded batch exceeds maximum numeric field count {max}")]
    TooManyNumericFields {
        /// Applicable maximum numeric field count.
        max: usize,
    },
    /// Exact numeric or timestamp conversion failed.
    #[error("provider value cannot be represented exactly")]
    InexactValue,
    /// Decoder state requires a fresh snapshot/resynchronization.
    #[error("decoder requires source resynchronization")]
    ResynchronizationRequired,
    /// Deep retained-size accounting overflowed.
    #[error("decoded retained-size accounting overflow")]
    RetainedSizeOverflow,
}

fn checked_sum(values: impl IntoIterator<Item = usize>) -> Result<usize, DecodeError> {
    values.into_iter().try_fold(0_usize, |total, value| {
        total
            .checked_add(value)
            .ok_or(DecodeError::RetainedSizeOverflow)
    })
}

fn sequence_dynamic_retained_bytes(sequence: &ProviderSequenceEvidence) -> usize {
    match sequence {
        ProviderSequenceEvidence::Provided { rule, .. }
        | ProviderSequenceEvidence::Unsupported { rule } => rule.provider_rule().retained_bytes(),
    }
}

fn is_decimal_lexeme(value: &[u8]) -> bool {
    let mut index = usize::from(value.first() == Some(&b'-'));
    let integer_start = index;
    while value.get(index).is_some_and(u8::is_ascii_digit) {
        index += 1;
    }
    if index == integer_start {
        return false;
    }
    if value.get(index) == Some(&b'.') {
        index += 1;
        let fractional_start = index;
        while value.get(index).is_some_and(u8::is_ascii_digit) {
            index += 1;
        }
        if index == fractional_start {
            return false;
        }
    }
    if matches!(value.get(index), Some(b'e' | b'E')) {
        index += 1;
        if matches!(value.get(index), Some(b'+' | b'-')) {
            index += 1;
        }
        let exponent_start = index;
        while value.get(index).is_some_and(u8::is_ascii_digit) {
            index += 1;
        }
        if index == exponent_start {
            return false;
        }
    }
    index == value.len()
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::mem::size_of;
    use std::num::NonZeroU64;

    use market_squawk_domain::{
        AggressorSide, AuctionPhase, ConnectionGeneration, CorporateActionKind, DigestAlgorithm,
        EvidenceDigest, HaltTransition, IntegrityRule, MarketDepth, MetadataRevision, RuleVersion,
        SourceId, SourceIdentifier, Timestamp, TradingStatus,
    };
    use rust_decimal::Decimal;

    use super::{
        DecoderEvidence, FrameId, FrameSessionBinding, ProviderAggressorEvidence,
        ProviderBookChange, ProviderBookLevel, ProviderBookSide, ProviderDecimalLexeme,
        ProviderObservationPayload, ProviderPrice, ProviderQuantity, ProviderSequenceEvidence,
        ProviderStatusEvidence, sequence_dynamic_retained_bytes,
    };
    use crate::SessionId;
    use crate::authority_time::trusted_test_receipt;

    type TestResult<T = ()> = Result<T, Box<dyn Error>>;

    fn id(value: &str) -> TestResult<SourceIdentifier> {
        Ok(SourceIdentifier::try_from(value)?)
    }

    fn rule(value: &str) -> TestResult<IntegrityRule> {
        Ok(IntegrityRule::new(id(value)?, RuleVersion::new(1)?))
    }

    fn level() -> TestResult<ProviderBookLevel> {
        Ok(ProviderBookLevel::new(
            ProviderPrice::new(ProviderDecimalLexeme::try_new("1")?),
            ProviderQuantity::new(ProviderDecimalLexeme::try_new("2")?),
        ))
    }

    fn levels(count: usize) -> TestResult<Vec<ProviderBookLevel>> {
        (0..count).map(|_| level()).collect()
    }

    fn changes(count: usize) -> TestResult<Vec<ProviderBookChange>> {
        (0..count)
            .map(|index| {
                Ok(ProviderBookChange::new(
                    if index % 2 == 0 {
                        ProviderBookSide::Bid
                    } else {
                        ProviderBookSide::Ask
                    },
                    level()?,
                ))
            })
            .collect()
    }

    #[test]
    fn exact_decimal_grammar_rejects_non_finite_and_partial_values() {
        for valid in ["0", "-0", "-1", "12.3400"] {
            assert!(ProviderDecimalLexeme::try_new(valid).is_ok(), "{valid}");
        }
        for invalid in [
            "",
            "+1",
            ".1",
            "1.",
            "NaN",
            "inf",
            "1_000",
            "1e1",
            "79228162514264337593543950336",
            "0.00000000000000000000000000001",
        ] {
            assert!(
                ProviderDecimalLexeme::try_new(invalid).is_err(),
                "{invalid}"
            );
        }
    }

    #[test]
    fn decimal_lexeme_reports_spare_string_capacity() -> TestResult {
        let mut value = String::with_capacity(128);
        value.push('1');
        let lexeme = ProviderDecimalLexeme {
            lexeme: value,
            decimal: Decimal::ONE,
        };

        assert!(lexeme.retained_bytes() >= 128);
        Ok(())
    }

    #[test]
    fn sequence_evidence_charges_its_capacity_heavy_rule_exactly() -> TestResult {
        let compact = ProviderSequenceEvidence::Unsupported { rule: rule("x")? };
        let mut provider_rule = String::with_capacity(SourceIdentifier::MAX_LENGTH);
        provider_rule.push('x');
        let retained_capacity = provider_rule.capacity();
        let expanded = ProviderSequenceEvidence::Unsupported {
            rule: IntegrityRule::new(
                SourceIdentifier::try_from(provider_rule)?,
                RuleVersion::new(1)?,
            ),
        };

        assert_eq!(
            sequence_dynamic_retained_bytes(&expanded)
                .checked_sub(sequence_dynamic_retained_bytes(&compact)),
            retained_capacity.checked_sub("x".len())
        );
        Ok(())
    }

    #[test]
    fn decoder_evidence_charges_binding_and_rule_allocations_exactly() -> TestResult {
        fn expanded(value: &str, capacity: usize) -> String {
            let mut expanded = String::with_capacity(capacity);
            expanded.push_str(value);
            expanded
        }
        fn evidence(
            source: SourceId,
            revision: SourceIdentifier,
            session: SourceIdentifier,
            decoder: SourceIdentifier,
        ) -> TestResult<DecoderEvidence> {
            let binding = FrameSessionBinding::new(
                source,
                MetadataRevision::new(revision),
                SessionId::new(session),
                ConnectionGeneration::new(1)?,
            );
            let receipt = trusted_test_receipt(Timestamp::from_unix_nanos(1), 1)?;
            Ok(DecoderEvidence {
                currentness: crate::FrameSessionLease::current_for_test(binding.clone(), &receipt),
                binding,
                frame_id: FrameId::new(NonZeroU64::new(1).ok_or("frame fixture must be nonzero")?),
                receipt,
                frame_bytes: 1,
                payload_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [1; 32]),
                decoder_rule: IntegrityRule::new(decoder, RuleVersion::new(1)?),
            })
        }
        let compact = evidence(SourceId::try_from("s")?, id("r")?, id("i")?, id("d")?)?;
        let expanded = evidence(
            SourceId::try_from(expanded("s", SourceId::MAX_LENGTH))?,
            SourceIdentifier::try_from(expanded("r", SourceIdentifier::MAX_LENGTH))?,
            SourceIdentifier::try_from(expanded("i", SourceIdentifier::MAX_LENGTH))?,
            SourceIdentifier::try_from(expanded("d", SourceIdentifier::MAX_LENGTH))?,
        )?;
        let expected_delta = (SourceId::MAX_LENGTH - 1)
            .checked_add((SourceIdentifier::MAX_LENGTH - 1) * 3)
            .ok_or("decoder evidence fixture overflow")?;

        assert_eq!(
            expanded
                .dynamic_retained_bytes()?
                .checked_sub(compact.dynamic_retained_bytes()?),
            Some(expected_delta)
        );
        Ok(())
    }

    #[test]
    fn snapshot_retained_bytes_include_every_nested_level_allocation() -> TestResult {
        for count in [1_usize, 10_000, 20_000] {
            let bid_count = count.min(10_000);
            let ask_count = count.saturating_sub(bid_count);
            let payload = ProviderObservationPayload::book_snapshot(
                MarketDepth::PriceLevel,
                levels(bid_count)?,
                levels(ask_count)?,
            )?;
            let expected = count
                .checked_mul(size_of::<ProviderBookLevel>() + 2)
                .ok_or("snapshot fixture size overflow")?;

            assert_eq!(payload.deep_retained_bytes()?, expected, "count={count}");
        }
        Ok(())
    }

    #[test]
    fn delta_retained_bytes_include_every_nested_change_allocation() -> TestResult {
        for count in [1_usize, 10_000, 20_000] {
            let payload =
                ProviderObservationPayload::book_delta(MarketDepth::PriceLevel, changes(count)?)?;
            let expected = count
                .checked_mul(size_of::<ProviderBookChange>() + 2)
                .ok_or("delta fixture size overflow")?;

            assert_eq!(payload.deep_retained_bytes()?, expected, "count={count}");
        }
        Ok(())
    }

    #[test]
    fn every_payload_variant_has_closed_dynamic_accounting() -> TestResult {
        let status = || -> TestResult<ProviderStatusEvidence> {
            Ok(ProviderStatusEvidence::new(
                id("status")?,
                rule("status-rule")?,
            ))
        };
        let payloads = vec![
            ProviderObservationPayload::Trade {
                trade_id: id("trade")?,
                price: ProviderPrice::new(ProviderDecimalLexeme::try_new("1")?),
                quantity: ProviderQuantity::new(ProviderDecimalLexeme::try_new("2")?),
                aggressor: ProviderAggressorEvidence::new(
                    AggressorSide::Buy,
                    Some(id("buy")?),
                    rule("aggressor-rule")?,
                ),
                taker_order_type: None,
            },
            ProviderObservationPayload::quote(Some(level()?), Some(level()?))?,
            ProviderObservationPayload::book_snapshot(
                MarketDepth::PriceLevel,
                levels(1)?,
                levels(1)?,
            )?,
            ProviderObservationPayload::book_delta(MarketDepth::PriceLevel, changes(1)?)?,
            ProviderObservationPayload::Auction {
                provider_code: id("auction")?,
                rule: rule("auction-rule")?,
                phase: AuctionPhase::Opening,
                price: Some(ProviderPrice::new(ProviderDecimalLexeme::try_new("1")?)),
                paired_quantity: ProviderQuantity::new(ProviderDecimalLexeme::try_new("2")?),
            },
            ProviderObservationPayload::TradingHalt {
                status: status()?,
                transition: HaltTransition::Halted,
                reason: id("reason")?,
            },
            ProviderObservationPayload::InstrumentStatus {
                status: status()?,
                trading_status: TradingStatus::Active,
            },
            ProviderObservationPayload::CorporateAction {
                action_id: id("action")?,
                rule: rule("action-rule")?,
                effective_at: Timestamp::from_unix_nanos(1),
                kind: CorporateActionKind::Delisting,
            },
        ];

        for payload in payloads {
            assert!(payload.deep_retained_bytes()? > 0, "{payload:?}");
        }
        Ok(())
    }
}
