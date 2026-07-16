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

    fn deep_retained_bytes(&self) -> Result<usize, DecodeError> {
        let timestamp_rule = match &self.timestamp {
            ProviderTimestampEvidence::AuthoritativelyAbsent(rule) => {
                rule.provider_rule().as_str().len()
            }
            ProviderTimestampEvidence::Provided { rule, .. } => rule.provider_rule().as_str().len(),
        };
        let snapshot_bytes = match &self.snapshot {
            ProviderSnapshotEvidence::InitializingSnapshot { provider_reference } => {
                provider_reference
                    .as_ref()
                    .map_or(0, |value| value.as_str().len())
            }
            ProviderSnapshotEvidence::Delta {
                provider_snapshot_reference,
            } => provider_snapshot_reference
                .as_ref()
                .map_or(0, |value| value.as_str().len()),
            ProviderSnapshotEvidence::NotApplicable(rule) => rule.provider_rule().as_str().len(),
        };
        let checksum_bytes = match &self.checksum {
            ProviderChecksumEvidence::Provided { value, rule } => {
                checked_sum([value.as_str().len(), rule.provider_rule().as_str().len()])?
            }
            ProviderChecksumEvidence::Unsupported { rule } => rule.provider_rule().as_str().len(),
        };
        checked_sum([
            self.source_identifier.as_str().len(),
            self.venue.as_str().len(),
            timestamp_rule,
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
        let Some(first) = observations.first() else {
            return Err(DecodeError::EmptyBatch);
        };
        if observations.iter().skip(1).any(|observation| {
            observation.venue() != first.venue() || observation.instrument() != first.instrument()
        }) {
            return Err(DecodeError::MixedRoutingScope);
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

    pub(crate) fn into_observations(self) -> Vec<ProviderNormalizedObservation> {
        self.observations.into_vec()
    }

    /// Returns checked deep retained bytes for the closed decoded shape.
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError::RetainedSizeOverflow`] on arithmetic overflow.
    pub fn retained_bytes(&self) -> Result<usize, DecodeError> {
        let shallow = std::mem::size_of::<Self>()
            .checked_add(
                self.observations
                    .len()
                    .checked_mul(std::mem::size_of::<ProviderNormalizedObservation>())
                    .ok_or(DecodeError::RetainedSizeOverflow)?,
            )
            .ok_or(DecodeError::RetainedSizeOverflow)?;
        let deep = checked_sum(
            self.observations
                .as_slice()
                .iter()
                .map(ProviderNormalizedObservation::deep_retained_bytes)
                .collect::<Result<Vec<_>, _>>()?,
        )?;
        shallow
            .checked_add(deep)
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
    ) -> Result<DecodedProviderBatch, DecodeError>;
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
    /// One batch crossed deterministic venue/instrument routing ownership.
    #[error("decoded provider batch must be homogeneous by venue and instrument")]
    MixedRoutingScope,
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
    use super::ProviderDecimalLexeme;

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
}
