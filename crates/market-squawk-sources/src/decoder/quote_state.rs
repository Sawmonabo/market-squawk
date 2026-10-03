//! Original per-field evidence for a provider's documented change-only quote protocol.
use super::*;

/// The source-native size remains unresolved; this state grants no lot/share conversion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderQuoteSizeUnit {
    UnresolvedNative,
}

/// One retained field's original same-generation capture, native clock, and source identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderQuoteFieldOrigin {
    frame: DecoderEvidence,
    instrument: InstrumentId,
    venue: VenueId,
    source_identifier: SourceIdentifier,
    sealed_receipt: EvidenceDigest,
    native_ordinal: std::num::NonZeroU64,
    batch_ordinal: u16,
    content_ordinal: u16,
    native_received_at: Timestamp,
    source_at: Option<Timestamp>,
}
impl ProviderQuoteFieldOrigin {
    #[allow(
        clippy::too_many_arguments,
        reason = "original native and current capture coordinates remain distinct"
    )]
    pub fn try_new(
        frame: DecoderEvidence,
        instrument: InstrumentId,
        venue: VenueId,
        source_identifier: SourceIdentifier,
        sealed_receipt: EvidenceDigest,
        native_ordinal: std::num::NonZeroU64,
        batch_ordinal: u16,
        content_ordinal: u16,
        native_received_at: Timestamp,
        source_at: Option<Timestamp>,
    ) -> Result<Self, DecodeError> {
        if sealed_receipt.algorithm() != DigestAlgorithm::Sha256
            || sealed_receipt.bytes() == [0; 32]
            || native_received_at > frame.received_at()
            || source_at.is_some_and(|value| value > native_received_at)
        {
            return Err(DecodeError::InvalidProviderEvidence);
        }
        Ok(Self {
            frame,
            instrument,
            venue,
            source_identifier,
            sealed_receipt,
            native_ordinal,
            batch_ordinal,
            content_ordinal,
            native_received_at,
            source_at,
        })
    }
    pub const fn frame(&self) -> &DecoderEvidence {
        &self.frame
    }
    pub const fn sealed_receipt(&self) -> EvidenceDigest {
        self.sealed_receipt
    }
    pub const fn native_ordinal(&self) -> std::num::NonZeroU64 {
        self.native_ordinal
    }
    pub const fn batch_ordinal(&self) -> u16 {
        self.batch_ordinal
    }
    pub const fn content_ordinal(&self) -> u16 {
        self.content_ordinal
    }
    pub const fn native_received_at(&self) -> Timestamp {
        self.native_received_at
    }
    pub const fn source_at(&self) -> Option<Timestamp> {
        self.source_at
    }
    pub fn effective_at(&self) -> Timestamp {
        self.source_at.unwrap_or(self.native_received_at)
    }
    fn retained_bytes(&self) -> Result<usize, DecodeError> {
        checked_sum([
            self.frame.dynamic_retained_bytes()?,
            self.venue.retained_bytes(),
            self.source_identifier.retained_bytes(),
        ])
    }
    fn matches(
        &self,
        current: &DecoderEvidence,
        observation: &ProviderNormalizedObservation,
    ) -> bool {
        self.frame.binding() == current.binding()
            && self.frame.frame_id().get() <= current.frame_id().get()
            && self.frame.received_at() <= current.received_at()
            && self.instrument == observation.instrument
            && self.venue == observation.venue
            && self.source_identifier == observation.source_identifier
    }
}
/// Separate original price and size references, never a manufactured atomic snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderAccumulatedQuoteEvidence {
    price: ProviderQuoteFieldOrigin,
    quantity: ProviderQuoteFieldOrigin,
    unit: ProviderQuoteSizeUnit,
}
impl ProviderAccumulatedQuoteEvidence {
    pub fn try_new(
        price: ProviderQuoteFieldOrigin,
        quantity: ProviderQuoteFieldOrigin,
        unit: ProviderQuoteSizeUnit,
    ) -> Result<Self, DecodeError> {
        if price.frame.binding() != quantity.frame.binding()
            || price.instrument != quantity.instrument
            || price.venue != quantity.venue
            || price.source_identifier != quantity.source_identifier
        {
            return Err(DecodeError::InvalidProviderEvidence);
        }
        Ok(Self {
            price,
            quantity,
            unit,
        })
    }
    pub const fn price(&self) -> &ProviderQuoteFieldOrigin {
        &self.price
    }
    pub const fn quantity(&self) -> &ProviderQuoteFieldOrigin {
        &self.quantity
    }
    pub const fn unit(&self) -> ProviderQuoteSizeUnit {
        self.unit
    }
    pub fn oldest_required_at(&self) -> Timestamp {
        self.price.effective_at().min(self.quantity.effective_at())
    }
    pub fn retained_bytes(&self) -> Result<usize, DecodeError> {
        checked_sum([
            std::mem::size_of::<Self>(),
            self.price.retained_bytes()?,
            self.quantity.retained_bytes()?,
        ])
    }
    pub(super) fn validate(
        &self,
        current: &DecoderEvidence,
        observation: &ProviderNormalizedObservation,
    ) -> Result<(), DecodeError> {
        if self.price.matches(current, observation) && self.quantity.matches(current, observation) {
            Ok(())
        } else {
            Err(DecodeError::InvalidProviderEvidence)
        }
    }
}
