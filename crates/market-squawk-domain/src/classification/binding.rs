//! Immutable identities and validity windows shared by live-plane audit assessments.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};

use super::MarketDepth;
use crate::{ConnectionGeneration, InstrumentId, SourceId, SourceIdentifier, Timestamp, VenueId};

macro_rules! source_identifier_newtype {
    ($name:ident, $description:literal) => {
        #[doc = $description]
        #[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
        #[serde(transparent)]
        pub struct $name(SourceIdentifier);

        impl $name {
            /// Constructs the bounded identity.
            pub const fn new(value: SourceIdentifier) -> Self {
                Self(value)
            }

            /// Returns the underlying bounded identifier.
            pub const fn as_source_identifier(&self) -> &SourceIdentifier {
                &self.0
            }
        }
    };
}

source_identifier_newtype!(
    MetadataRevision,
    "Immutable revision of the authoritative source metadata used by an assessment."
);
source_identifier_newtype!(
    AuthorizationBasis,
    "Auditable policy, agreement, or account basis establishing source authorization."
);
source_identifier_newtype!(
    ProviderProduct,
    "Provider-specific market-data product identity."
);
source_identifier_newtype!(
    ProviderChannel,
    "Provider-specific stream or channel identity."
);

/// Canonical live-event class included in every assessment binding.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveEventClass {
    /// Executed trade.
    Trade,
    /// Bid/ask quote.
    Quote,
    /// Complete order-book image.
    BookSnapshot,
    /// Incremental order-book update.
    BookDelta,
    /// Auction indication or result.
    Auction,
    /// Trading-halt transition.
    TradingHalt,
    /// Instrument-status transition.
    InstrumentStatus,
    /// Corporate-action announcement.
    CorporateAction,
}

impl LiveEventClass {
    /// Returns whether this class requires an initialized order-book state binding.
    pub const fn requires_book_state(self) -> bool {
        matches!(self, Self::BookSnapshot | Self::BookDelta)
    }
}

/// Fixed-width digest of a payload or canonical live-state image.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct EvidenceDigest([u8; 32]);

impl EvidenceDigest {
    /// Constructs a digest already computed by an explicitly selected algorithm.
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the digest bytes.
    pub const fn bytes(self) -> [u8; 32] {
        self.0
    }
}

/// Exact order-book state included in a live evidence binding.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BookStateBinding {
    depth: MarketDepth,
    state_id: SourceIdentifier,
    state_digest: EvidenceDigest,
}

impl BookStateBinding {
    /// Constructs an order-book state binding.
    pub const fn new(
        depth: MarketDepth,
        state_id: SourceIdentifier,
        state_digest: EvidenceDigest,
    ) -> Self {
        Self {
            depth,
            state_id,
            state_digest,
        }
    }

    /// Returns the market-depth class of the state image.
    pub const fn depth(&self) -> MarketDepth {
        self.depth
    }

    /// Returns the state identity.
    pub const fn state_id(&self) -> &SourceIdentifier {
        &self.state_id
    }

    /// Returns the canonical-state digest.
    pub const fn state_digest(&self) -> EvidenceDigest {
        self.state_digest
    }
}

/// Complete immutable key preventing evidence transplant between live observations.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LiveEvidenceBinding {
    source_id: SourceId,
    session_id: SourceIdentifier,
    metadata_revision: MetadataRevision,
    authorization_basis: AuthorizationBasis,
    venue_id: VenueId,
    instrument_id: InstrumentId,
    connection_generation: ConnectionGeneration,
    provider_product: ProviderProduct,
    provider_channel: ProviderChannel,
    event_class: LiveEventClass,
    source_identifier: SourceIdentifier,
    payload_digest: EvidenceDigest,
    canonical_state_digest: EvidenceDigest,
    book_state: Option<BookStateBinding>,
}

impl LiveEvidenceBinding {
    /// Constructs a complete relational key for one canonical live observation.
    ///
    /// # Errors
    ///
    /// Returns [`BindingError::MissingBookState`] when a book snapshot or delta omits the exact
    /// depth/state identity and digest to which its assessments apply.
    #[expect(
        clippy::too_many_arguments,
        reason = "the immutable anti-transplant key must be constructed atomically"
    )]
    pub fn new(
        source_id: SourceId,
        session_id: SourceIdentifier,
        metadata_revision: MetadataRevision,
        authorization_basis: AuthorizationBasis,
        venue_id: VenueId,
        instrument_id: InstrumentId,
        connection_generation: ConnectionGeneration,
        provider_product: ProviderProduct,
        provider_channel: ProviderChannel,
        event_class: LiveEventClass,
        source_identifier: SourceIdentifier,
        payload_digest: EvidenceDigest,
        canonical_state_digest: EvidenceDigest,
        book_state: Option<BookStateBinding>,
    ) -> Result<Self, BindingError> {
        if event_class.requires_book_state() {
            let state = book_state.as_ref().ok_or(BindingError::MissingBookState)?;
            if state.state_digest != canonical_state_digest {
                return Err(BindingError::CanonicalStateDigestMismatch);
            }
        }
        Ok(Self {
            source_id,
            session_id,
            metadata_revision,
            authorization_basis,
            venue_id,
            instrument_id,
            connection_generation,
            provider_product,
            provider_channel,
            event_class,
            source_identifier,
            payload_digest,
            canonical_state_digest,
            book_state,
        })
    }

    /// Returns the source identity.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }
    /// Returns the concrete connection/session identity.
    pub const fn session_id(&self) -> &SourceIdentifier {
        &self.session_id
    }
    /// Returns the source-metadata revision.
    pub const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }
    /// Returns the authorization basis.
    pub const fn authorization_basis(&self) -> &AuthorizationBasis {
        &self.authorization_basis
    }
    /// Returns the venue identity.
    pub const fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }
    /// Returns the instrument identity.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
    /// Returns the connection generation.
    pub const fn connection_generation(&self) -> ConnectionGeneration {
        self.connection_generation
    }
    /// Returns the provider product.
    pub const fn provider_product(&self) -> &ProviderProduct {
        &self.provider_product
    }
    /// Returns the provider channel.
    pub const fn provider_channel(&self) -> &ProviderChannel {
        &self.provider_channel
    }
    /// Returns the event class.
    pub const fn event_class(&self) -> LiveEventClass {
        self.event_class
    }
    /// Returns the provider source identifier for the observation.
    pub const fn source_identifier(&self) -> &SourceIdentifier {
        &self.source_identifier
    }
    /// Returns the exact source-payload digest.
    pub const fn payload_digest(&self) -> EvidenceDigest {
        self.payload_digest
    }
    /// Returns the exact canonical-state digest.
    pub const fn canonical_state_digest(&self) -> EvidenceDigest {
        self.canonical_state_digest
    }
    /// Returns the book-state binding when applicable.
    pub const fn book_state(&self) -> Option<&BookStateBinding> {
        self.book_state.as_ref()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LiveEvidenceBindingWire {
    source_id: SourceId,
    session_id: SourceIdentifier,
    metadata_revision: MetadataRevision,
    authorization_basis: AuthorizationBasis,
    venue_id: VenueId,
    instrument_id: InstrumentId,
    connection_generation: ConnectionGeneration,
    provider_product: ProviderProduct,
    provider_channel: ProviderChannel,
    event_class: LiveEventClass,
    source_identifier: SourceIdentifier,
    payload_digest: EvidenceDigest,
    canonical_state_digest: EvidenceDigest,
    book_state: Option<BookStateBinding>,
}

impl<'de> Deserialize<'de> for LiveEvidenceBinding {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = LiveEvidenceBindingWire::deserialize(deserializer)?;
        Self::new(
            wire.source_id,
            wire.session_id,
            wire.metadata_revision,
            wire.authorization_basis,
            wire.venue_id,
            wire.instrument_id,
            wire.connection_generation,
            wire.provider_product,
            wire.provider_channel,
            wire.event_class,
            wire.source_identifier,
            wire.payload_digest,
            wire.canonical_state_digest,
            wire.book_state,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// One assessment and the exact observation and time interval to which it applies.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BoundAssessment<T> {
    binding: LiveEvidenceBinding,
    evaluated_at: Timestamp,
    valid_until: Timestamp,
    result: T,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, bound(deserialize = "T: Deserialize<'de>"))]
struct BoundAssessmentWire<T> {
    binding: LiveEvidenceBinding,
    evaluated_at: Timestamp,
    valid_until: Timestamp,
    result: T,
}

impl<'de, T> Deserialize<'de> for BoundAssessment<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = BoundAssessmentWire::deserialize(deserializer)?;
        Self::new(
            wire.binding,
            wire.evaluated_at,
            wire.valid_until,
            wire.result,
        )
        .map_err(serde::de::Error::custom)
    }
}

impl<T> BoundAssessment<T> {
    /// Constructs a checked inclusive validity interval.
    ///
    /// # Errors
    ///
    /// Returns [`BindingError::ValidityBeforeEvaluation`] if the expiry precedes evaluation.
    pub fn new(
        binding: LiveEvidenceBinding,
        evaluated_at: Timestamp,
        valid_until: Timestamp,
        result: T,
    ) -> Result<Self, BindingError> {
        if valid_until < evaluated_at {
            return Err(BindingError::ValidityBeforeEvaluation);
        }
        Ok(Self {
            binding,
            evaluated_at,
            valid_until,
            result,
        })
    }

    /// Returns the complete observation binding.
    pub const fn binding(&self) -> &LiveEvidenceBinding {
        &self.binding
    }
    /// Returns when this result was evaluated.
    pub const fn evaluated_at(&self) -> Timestamp {
        self.evaluated_at
    }
    /// Returns the inclusive last instant at which this result may be considered current.
    pub const fn valid_until(&self) -> Timestamp {
        self.valid_until
    }
    /// Returns the retained result.
    pub const fn result(&self) -> &T {
        &self.result
    }
    /// Returns true only from evaluation through the inclusive expiry boundary.
    pub fn is_valid_at(&self, at: Timestamp) -> bool {
        at >= self.evaluated_at && at <= self.valid_until
    }
}

/// Failure to construct a live binding or validity window.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BindingError {
    /// A book event omitted its exact book-state identity.
    MissingBookState,
    /// An assessment expiry precedes its evaluation instant.
    ValidityBeforeEvaluation,
    /// Bound book state and canonical-state digests disagree.
    CanonicalStateDigestMismatch,
}

impl fmt::Display for BindingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingBookState => {
                formatter.write_str("book events require a book-state binding")
            }
            Self::ValidityBeforeEvaluation => {
                formatter.write_str("valid-until must not precede evaluated-at")
            }
            Self::CanonicalStateDigestMismatch => {
                formatter.write_str("book-state digest must match canonical-state digest")
            }
        }
    }
}

impl std::error::Error for BindingError {}
