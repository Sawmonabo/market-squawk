//! Synchronous decoding into bounded provider-normalized, pre-state observations.
//!
//! Decoders preserve exact provider evidence. They deliberately do not construct canonical
//! [`market_squawk_domain::MarketEvent`] values: sequence/snapshot/checksum qualification and
//! current order-book state belong to the instrument-owned live pipeline.

use market_squawk_domain::{
    AggressorSide, AuctionPhase, CorporateActionKind, DigestAlgorithm, EvidenceDigest,
    HaltTransition, InstrumentId, IntegrityRule, LiveEventClass, MarketDepth, SequenceNumber,
    SourceIdentifier, Timestamp, TradingStatus, VenueId,
};
use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::authority_time::TrustedReceiptObservation;
use crate::bounded::BoundedVec;
use crate::{FrameId, FrameSessionBinding, SourceMetadataProvider, ValidatedRawMarketFrame};

#[path = "decoder/outcome.rs"]
mod outcome;

pub use outcome::{
    ControlFrameKind, DecodeInternalError, DecodeOutcome, DecodedControlFrame, DecodedIgnoredFrame,
    DecodedQuarantineAction, DecodedRecoveryAction, IgnoredFrameReason, QuarantineReason,
    ResynchronizationReason,
};

/// Maximum provider observations emitted by one transport frame.
pub const MAX_DECODED_EVENTS: usize = 1_024;
/// Maximum provider book levels or changes retained across one decoded frame.
///
/// The bound admits a complete current Coinbase Advanced Trade snapshot while remaining below the
/// closed 16 MiB transport-frame ceiling. Downstream live admission applies the stricter configured
/// retained-byte limit before the batch enters a shard mailbox.
pub const MAX_DECODED_BOOK_ITEMS: usize = 131_072;
const MAX_DECIMAL_LEXEME_BYTES: usize = 128;

/// Exact raw-frame and decoder-rule evidence attached to one provider batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecoderEvidence {
    binding: FrameSessionBinding,
    currentness: crate::FrameSessionLease,
    frame_id: FrameId,
    receipt: TrustedReceiptObservation,
    frame_bytes: usize,
    payload_digest: EvidenceDigest,
    decoder_rule: IntegrityRule,
}

impl DecoderEvidence {
    /// Computes SHA-256 over exact raw bytes and binds the result to the shared frame identity.
    pub fn from_validated_frame(
        validated: &ValidatedRawMarketFrame<'_>,
        decoder_rule: IntegrityRule,
    ) -> Self {
        let frame = validated.frame();
        let bytes: [u8; 32] = Sha256::digest(frame.payload()).into();
        Self {
            binding: frame.binding().clone(),
            currentness: validated.currentness_lease().clone(),
            frame_id: frame.frame_id(),
            receipt: validated.trusted_receipt().clone(),
            frame_bytes: frame.payload().len(),
            payload_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, bytes),
            decoder_rule,
        }
    }

    /// Returns the O(1)-clone shared session binding.
    pub const fn binding(&self) -> &FrameSessionBinding {
        &self.binding
    }

    /// Returns the opaque registry lease that must still be current at downstream use.
    pub const fn currentness_lease(&self) -> &crate::FrameSessionLease {
        &self.currentness
    }

    /// Returns exact frame receive time.
    pub const fn received_at(&self) -> Timestamp {
        self.receipt.received_at()
    }

    pub(crate) const fn trusted_receipt(&self) -> &TrustedReceiptObservation {
        &self.receipt
    }

    /// Returns the exact generation-local raw-frame identity.
    pub const fn frame_id(&self) -> FrameId {
        self.frame_id
    }

    /// Returns the exact raw payload byte charge derived from the validated frame.
    pub const fn frame_bytes(&self) -> usize {
        self.frame_bytes
    }

    /// Returns the digest computed from exact raw payload bytes.
    pub const fn payload_digest(&self) -> EvidenceDigest {
        self.payload_digest
    }

    /// Returns the exact metadata-bound decoder rule and revision.
    pub const fn decoder_rule(&self) -> &IntegrityRule {
        &self.decoder_rule
    }

    /// Returns the shared session-identity allocation plus the owned decoder-rule allocation.
    ///
    /// The inline [`Self`] storage is deliberately excluded.
    pub(crate) fn dynamic_retained_bytes(&self) -> Result<usize, DecodeError> {
        self.binding
            .shared_allocation_charge()
            .and_then(|bytes| {
                bytes.checked_add(
                    self.receipt
                        .continuity()
                        .checked_shared_allocation_bytes()
                        .ok()?,
                )
            })
            .and_then(|bytes| bytes.checked_add(self.currentness.shared_allocation_charge()?))
            .and_then(|bytes| bytes.checked_add(self.decoder_rule.provider_rule().retained_bytes()))
            .ok_or(DecodeError::RetainedSizeOverflow)
    }
}

/// Exact validated provider decimal lexeme, retained until tick/lot conversion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderDecimalLexeme {
    lexeme: String,
    decimal: Decimal,
}

impl ProviderDecimalLexeme {
    /// Parses the bounded decimal grammar without a floating-point conversion.
    ///
    /// # Errors
    ///
    /// Rejects oversized, non-ASCII, non-finite, or syntactically invalid decimal text.
    pub fn try_new(value: &str) -> Result<Self, DecodeError> {
        if value.is_empty()
            || value.len() > MAX_DECIMAL_LEXEME_BYTES
            || !value.is_ascii()
            || !is_decimal_lexeme(value.as_bytes())
            || value.contains(['e', 'E'])
        {
            return Err(DecodeError::InexactValue);
        }
        let decimal = Decimal::from_str_exact(value).map_err(|_| DecodeError::InexactValue)?;
        Ok(Self {
            lexeme: value.to_owned(),
            decimal,
        })
    }

    /// Returns the exact provider text.
    pub fn as_str(&self) -> &str {
        &self.lexeme
    }

    /// Returns the checked exact decimal representation retained with the original lexeme.
    pub const fn decimal(&self) -> Decimal {
        self.decimal
    }

    fn retained_bytes(&self) -> usize {
        self.lexeme.capacity()
    }
}

/// Exact provider price retained as checked decimal plus original lexeme.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderPrice(ProviderDecimalLexeme);

impl ProviderPrice {
    /// Constructs an exact provider price.
    pub const fn new(value: ProviderDecimalLexeme) -> Self {
        Self(value)
    }

    /// Returns the checked decimal and exact lexeme.
    pub const fn value(&self) -> &ProviderDecimalLexeme {
        &self.0
    }
}

/// Exact provider quantity retained as checked decimal plus original lexeme.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderQuantity(ProviderDecimalLexeme);

impl ProviderQuantity {
    /// Constructs an exact provider quantity.
    pub const fn new(value: ProviderDecimalLexeme) -> Self {
        Self(value)
    }

    /// Returns the checked decimal and exact lexeme.
    pub const fn value(&self) -> &ProviderDecimalLexeme {
        &self.0
    }
}

/// One typed provider price/quantity level.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderBookLevel {
    price: ProviderPrice,
    quantity: ProviderQuantity,
}

impl ProviderBookLevel {
    /// Constructs an exact provider book level.
    pub const fn new(price: ProviderPrice, quantity: ProviderQuantity) -> Self {
        Self { price, quantity }
    }

    /// Returns exact provider price.
    pub const fn price(&self) -> &ProviderPrice {
        &self.price
    }

    /// Returns exact provider quantity.
    pub const fn quantity(&self) -> &ProviderQuantity {
        &self.quantity
    }
}

/// Provider side for a typed book change.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ProviderBookSide {
    /// Bid-side change.
    Bid,
    /// Ask-side change.
    Ask,
}

/// One typed provider book delta; zero quantity remains exact delete-on-zero evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderBookChange {
    side: ProviderBookSide,
    level: ProviderBookLevel,
}

impl ProviderBookChange {
    /// Constructs a typed provider book change.
    pub const fn new(side: ProviderBookSide, level: ProviderBookLevel) -> Self {
        Self { side, level }
    }

    /// Returns the provider side.
    pub const fn side(&self) -> ProviderBookSide {
        self.side
    }

    /// Returns exact provider level evidence.
    pub const fn level(&self) -> &ProviderBookLevel {
        &self.level
    }
}

/// Provider timestamp evidence before canonical timing qualification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderTimestampEvidence {
    /// Exact timestamp supplied by the provider.
    Provided {
        value: Timestamp,
        rule: IntegrityRule,
    },
    /// Provider metadata authoritatively declares timestamps inapplicable.
    AuthoritativelyAbsent(IntegrityRule),
}

/// Provider sequence evidence before generation-owned progression validation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderSequenceEvidence {
    /// Exact sequence supplied by the provider.
    Provided {
        value: SequenceNumber,
        rule: IntegrityRule,
    },
    /// Selected protocol authoritatively declares no sequence.
    Unsupported { rule: IntegrityRule },
}

/// Snapshot relationship before order-book state applies the observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderSnapshotEvidence {
    /// Observation initializes state from a full provider snapshot.
    InitializingSnapshot {
        /// Reference supplied by the provider, if the protocol transmits one.
        provider_reference: Option<SourceIdentifier>,
    },
    /// Observation is a delta; provider snapshot references remain optional.
    Delta {
        /// Reference supplied by the provider, never synthesized from local state.
        provider_snapshot_reference: Option<SourceIdentifier>,
    },
    /// Observation does not participate in book state.
    NotApplicable(IntegrityRule),
}

/// Provider checksum material before venue-specific canonicalization/state validation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderChecksumEvidence {
    /// Exact checksum text supplied by the provider.
    Provided {
        value: SourceIdentifier,
        rule: IntegrityRule,
    },
    /// Selected protocol authoritatively declares no checksum.
    Unsupported { rule: IntegrityRule },
}

/// Exact provider status code plus the metadata-bound interpretation rule.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderStatusEvidence {
    status: SourceIdentifier,
    rule: IntegrityRule,
}

impl ProviderStatusEvidence {
    /// Constructs provider status evidence without a caller-authored eligibility bit.
    pub const fn new(status: SourceIdentifier, rule: IntegrityRule) -> Self {
        Self { status, rule }
    }

    /// Returns the exact provider status code.
    pub const fn status(&self) -> &SourceIdentifier {
        &self.status
    }

    /// Returns the exact status interpretation rule.
    pub const fn rule(&self) -> &IntegrityRule {
        &self.rule
    }
}

/// Exact aggressor evidence and metadata-bound interpretation rule.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderAggressorEvidence {
    side: AggressorSide,
    provider_code: Option<SourceIdentifier>,
    rule: IntegrityRule,
}

impl ProviderAggressorEvidence {
    /// Constructs typed aggressor evidence.
    pub const fn new(
        side: AggressorSide,
        provider_code: Option<SourceIdentifier>,
        rule: IntegrityRule,
    ) -> Self {
        Self {
            side,
            provider_code,
            rule,
        }
    }

    /// Returns typed aggressor interpretation.
    pub const fn side(&self) -> AggressorSide {
        self.side
    }

    /// Returns exact provider code when transmitted.
    pub const fn provider_code(&self) -> Option<&SourceIdentifier> {
        self.provider_code.as_ref()
    }

    /// Returns exact interpretation rule.
    pub const fn rule(&self) -> &IntegrityRule {
        &self.rule
    }
}

/// Bounded typed provider book snapshot payload.
#[derive(Clone, Debug)]
pub struct ProviderBookSnapshotPayload {
    depth: MarketDepth,
    bids: BoundedVec<ProviderBookLevel, MAX_DECODED_BOOK_ITEMS>,
    asks: BoundedVec<ProviderBookLevel, MAX_DECODED_BOOK_ITEMS>,
}

impl ProviderBookSnapshotPayload {
    /// Returns declared provider depth.
    pub const fn depth(&self) -> MarketDepth {
        self.depth
    }

    /// Returns bid levels in provider order.
    pub fn bids(&self) -> &[ProviderBookLevel] {
        self.bids.as_slice()
    }

    /// Returns ask levels in provider order.
    pub fn asks(&self) -> &[ProviderBookLevel] {
        self.asks.as_slice()
    }
}

/// Bounded typed provider book delta payload.
#[derive(Clone, Debug)]
pub struct ProviderBookDeltaPayload {
    depth: MarketDepth,
    changes: BoundedVec<ProviderBookChange, MAX_DECODED_BOOK_ITEMS>,
}

impl ProviderBookDeltaPayload {
    /// Returns declared provider depth.
    pub const fn depth(&self) -> MarketDepth {
        self.depth
    }

    /// Returns nonempty message-atomic provider changes.
    pub fn changes(&self) -> &[ProviderBookChange] {
        self.changes.as_slice()
    }
}

include!("decoder/payload.rs");
include!("decoder/batch.rs");
