//! Current source evidence for virtual orders. This module never issues live execution authority.

use market_squawk_domain::{
    AssetClass, BookLevel, CanonicalStateDigest, CanonicalizationRule, CoverageDelay, DataQuality,
    DigestAlgorithm, EvidenceDigest, InstrumentExecutionTerms, LiveEventClass, LiveEvidenceBinding,
    MarketDataInstrumentDefinition, QualificationAssessmentId, RuleVersion, SourceIdentifier,
    Timestamp,
};
use market_squawk_sources::{
    CurrentProviderObservation, ProviderObservationPayload, ProviderTimestampEvidence,
    normalize_positive_quantity, normalize_price,
};
use sha2::{Digest, Sha256};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use thiserror::Error;

/// Single actor-owned source cell. The original observation and registry lease stay intact.
/// Every accepted market/status mutation invalidates outstanding approvals; dropping the owner
/// revokes all reads. There is one retained quote, no unbounded history or background work.
#[derive(Debug, Default)]
pub struct VirtualPaperSourceOwner {
    state: Arc<SourceState>,
    quote: Mutex<Option<Arc<CurrentProviderObservation>>>,
}
#[derive(Debug, Default)]
struct SourceState {
    revision: AtomicU64,
}
impl VirtualPaperSourceOwner {
    /// Called at the real source actor's commit point after its original identity validation.
    /// Passing `false` clears eligibility until a subsequent affirmative actor status decision.
    pub fn commit(
        &self,
        quote: Option<Arc<CurrentProviderObservation>>,
        trading_allowed: bool,
    ) -> Result<(), VirtualPaperError> {
        let mut retained = self
            .quote
            .try_lock()
            .map_err(|_| VirtualPaperError::Unavailable)?;
        self.advance()?;
        if !trading_allowed {
            *retained = None;
        } else if let Some(quote) = quote {
            quote
                .validate_at(now()?)
                .map_err(|_| VirtualPaperError::Revoked)?;
            if !matches!(
                quote.observation().payload(),
                ProviderObservationPayload::Quote { .. }
            ) {
                return Err(VirtualPaperError::Unavailable);
            }
            *retained = Some(quote);
        }
        Ok(())
    }
    fn advance(&self) -> Result<(), VirtualPaperError> {
        self.state
            .revision
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |revision| {
                if revision == u64::MAX {
                    None
                } else {
                    revision.checked_add(1).filter(|next| *next != u64::MAX)
                }
            })
            .map(|_| ())
            .map_err(|_| {
                self.revoke();
                VirtualPaperError::Revoked
            })
    }
    /// Revokes previously captured quotes and all derived approvals permanently.
    pub fn revoke(&self) {
        self.state.revision.store(u64::MAX, Ordering::Release);
    }
    /// Captures the actor's actual retained quote and mutation revision. No flat display value
    /// or caller-authored quote can construct this lease.
    pub fn read(&self) -> Result<VirtualPaperSourceLease, VirtualPaperError> {
        let quote = self
            .quote
            .try_lock()
            .map_err(|_| VirtualPaperError::Unavailable)?;
        let revision = self.state.revision.load(Ordering::Acquire);
        if revision == u64::MAX {
            return Err(VirtualPaperError::Revoked);
        }
        Ok(VirtualPaperSourceLease {
            state: Arc::clone(&self.state),
            revision,
            quote: Arc::clone(quote.as_ref().ok_or(VirtualPaperError::Unavailable)?),
            retention: None,
        })
    }
}
impl Drop for VirtualPaperSourceOwner {
    fn drop(&mut self) {
        self.revoke();
    }
}

/// Opaque current quote captured by the source actor, never serializable or brokerage authority.
#[derive(Debug)]
pub struct VirtualPaperSourceLease {
    state: Arc<SourceState>,
    revision: u64,
    quote: Arc<CurrentProviderObservation>,
    retention: Option<Box<dyn VirtualPaperRetention>>,
}
pub trait VirtualPaperRetention: std::fmt::Debug + Send + Sync {}
impl<T: std::fmt::Debug + Send + Sync> VirtualPaperRetention for T {}
impl VirtualPaperSourceLease {
    /// Carries the original bounded actor read ticket through final dispatch, so retaining an
    /// old source quote never releases its count/byte admission early.
    pub fn retain<T: VirtualPaperRetention + 'static>(mut self, ticket: T) -> Self {
        self.retention = Some(Box::new(ticket));
        self
    }

    /// Original current-source observation for exact metadata/source matching by application owner.
    pub fn current(&self) -> &CurrentProviderObservation {
        &self.quote
    }
    pub fn validate_current(&self) -> Result<(), VirtualPaperError> {
        if self.state.revision.load(Ordering::Acquire) != self.revision {
            return Err(VirtualPaperError::Revoked);
        }
        self.quote
            .validate_at(now()?)
            .map_err(|_| VirtualPaperError::Revoked)
    }
    /// Admits a single virtual order using real reference identity and an explicit simulation
    /// policy. The application supplies endpoints from its reopened source calendar receipt;
    /// these endpoints do not confer provider or brokerage order permission.
    pub fn admit(
        self,
        definition: &MarketDataInstrumentDefinition,
        policy: VirtualPaperPolicy,
    ) -> Result<ConsumedVirtualPaperAuthority, VirtualPaperError> {
        self.validate_current()?;
        let at = now()?;
        let current = self.current();
        let source = current.observation();
        let terms = policy.terms;
        let effective = definition.effective_interval();
        if !matches!(
            definition.asset_class(),
            AssetClass::Equity | AssetClass::Fund
        ) || definition.instrument_id() != terms.instrument_id()
            || definition.quote_currency() != terms.quote_currency()
            || terms.settlement_currency() != Some(definition.quote_currency())
            || terms.contract_multiplier() != rust_decimal::Decimal::ONE
            || terms.lot_size().as_decimal() != rust_decimal::Decimal::ONE
            || source.instrument() != terms.instrument_id()
            || at < effective.starts_at()
            || effective.ends_at().is_some_and(|end| at >= end)
            || policy.calendar_digest == [0; 32]
            || policy.definition_digest == [0; 32]
            || policy.definition_digest
                != current
                    .provider_identity()
                    .evidence()
                    .definition_digest
                    .bytes()
            || at < policy.opens_at
            || at >= policy.closes_at_exclusive
            || policy.calendar_available_at > at
        {
            return Err(VirtualPaperError::Policy);
        }
        let received_at = current.evidence().received_at();
        let source_at = match source.timestamp() {
            ProviderTimestampEvidence::Provided { value, .. } => *value,
            ProviderTimestampEvidence::AuthoritativelyAbsent(_) => {
                return Err(VirtualPaperError::Unavailable);
            }
        };
        let source_policy = current.policy();
        if source_policy.quality_ceiling() != DataQuality::DirectUnverified
            || source.venue().as_str() != "iex"
            || source_policy
                .provider_product()
                .as_source_identifier()
                .as_str()
                != "alpaca-basic-iex-configured-symbols-v1"
            || !source
                .source_identifier()
                .as_str()
                .starts_with("alpaca:iex:quote:")
            || source_policy.coverage().delay() != CoverageDelay::RealTime
            || source_policy.coverage().event_class() != LiveEventClass::Quote
            || source_at < policy.opens_at
            || source_at >= policy.closes_at_exclusive
            || received_at > at
            || source_at > at
        {
            return Err(VirtualPaperError::Unavailable);
        }
        let (bid, ask) = match source.payload() {
            ProviderObservationPayload::Quote {
                bid: Some(bid),
                ask: Some(ask),
            } => (bid, ask),
            _ => return Err(VirtualPaperError::Unavailable),
        };
        // Virtual IEX policy: N positive integral native size units permit at most N whole
        // simulated shares. This is a conservative capacity bound under both documented stock
        // size conventions (shares or integral round lots), not a native lot-size assertion.
        // Do not multiply by100 or reuse this contract for unknown-unit other provider data.
        let bid = BookLevel::new(
            normalize_price(bid.price(), terms.price_tick())
                .map_err(|_| VirtualPaperError::Granularity)?,
            normalize_positive_quantity(bid.quantity(), terms.lot_size())
                .map_err(|_| VirtualPaperError::Granularity)?,
        )
        .map_err(|_| VirtualPaperError::Granularity)?;
        let ask = BookLevel::new(
            normalize_price(ask.price(), terms.price_tick())
                .map_err(|_| VirtualPaperError::Granularity)?,
            normalize_positive_quantity(ask.quantity(), terms.lot_size())
                .map_err(|_| VirtualPaperError::Granularity)?,
        )
        .map_err(|_| VirtualPaperError::Granularity)?;
        if bid.price().get() <= 0 || bid.price() >= ask.price() {
            return Err(VirtualPaperError::Unavailable);
        }
        let freshness = source_policy.freshness();
        let deadline = |timestamp: Timestamp, nanos: u64| {
            timestamp
                .checked_add_nanos(i64::try_from(nanos).map_err(|_| VirtualPaperError::Clock)?)
                .map_err(|_| VirtualPaperError::Clock)
        };
        let mut valid_until = source_policy
            .valid_until()
            .min(current.current_lease().valid_until())
            .min(deadline(received_at, freshness.max_market_age_nanos())?)
            .min(deadline(received_at, freshness.max_transport_age_nanos())?)
            .min(deadline(source_at, freshness.max_source_age_nanos())?)
            .min(
                policy
                    .closes_at_exclusive
                    .checked_sub_nanos(1)
                    .map_err(|_| VirtualPaperError::Clock)?,
            );
        if let Some(end) = effective.ends_at() {
            valid_until = valid_until.min(
                end.checked_sub_nanos(1)
                    .map_err(|_| VirtualPaperError::Clock)?,
            );
        }
        if let Some(end) = source_policy.coverage().effective_until() {
            valid_until = valid_until.min(end);
        }
        if at > valid_until {
            return Err(VirtualPaperError::Expired);
        }
        let nanos = u64::try_from(
            valid_until
                .unix_nanos()
                .checked_sub(at.unix_nanos())
                .ok_or(VirtualPaperError::Clock)?,
        )
        .map_err(|_| VirtualPaperError::Clock)?;
        let monotonic_deadline = Instant::now()
            .checked_add(std::time::Duration::from_nanos(nanos))
            .ok_or(VirtualPaperError::Clock)?;
        let mut quote_hash = Sha256::new();
        quote_hash.update(b"market-squawk/virtual-paper-original-quote/v2\0");
        quote_hash.update(
            current
                .provider_identity()
                .evidence()
                .selection_digest
                .bytes(),
        );
        quote_hash.update(
            u64::try_from(current.row_ordinal())
                .map_err(|_| VirtualPaperError::Policy)?
                .to_be_bytes(),
        );
        quote_hash.update(
            u64::try_from(current.row_count())
                .map_err(|_| VirtualPaperError::Policy)?
                .to_be_bytes(),
        );
        quote_hash.update(current.evidence().payload_digest().bytes());
        for text in [
            current.evidence().binding().source_id().as_str(),
            current
                .evidence()
                .binding()
                .session_id()
                .as_source_identifier()
                .as_str(),
            source.source_identifier().as_str(),
        ] {
            quote_hash.update((text.len() as u64).to_be_bytes());
            quote_hash.update(text.as_bytes());
        }
        quote_hash.update(
            current
                .evidence()
                .binding()
                .connection_generation()
                .get()
                .to_be_bytes(),
        );
        quote_hash.update(
            current
                .evidence()
                .transport_frame()
                .ok_or(VirtualPaperError::Unavailable)?
                .frame_id()
                .get()
                .to_be_bytes(),
        );
        let quote_digest: [u8; 32] = quote_hash.finalize().into();
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/virtual-paper-source/v1\0");
        hash.update(b"alpaca-iex-native-size-whole-share-lower-bound/v1\0");
        hash.update(current.evidence().payload_digest().bytes());
        hash.update(policy.definition_digest);
        hash.update(policy.calendar_digest);
        hash.update(
            definition
                .reference_payload_evidence()
                .content_digest()
                .bytes(),
        );
        hash.update(self.revision.to_be_bytes());
        hash.update(quote_digest);
        hash.update(
            current
                .evidence()
                .binding()
                .metadata_revision()
                .as_source_identifier()
                .as_str()
                .as_bytes(),
        );
        hash.update(source.venue().as_str().as_bytes());
        for text in [
            current.evidence().binding().source_id().as_str(),
            current
                .evidence()
                .binding()
                .session_id()
                .as_source_identifier()
                .as_str(),
            source.source_identifier().as_str(),
            &terms.price_tick().as_decimal().to_string(),
            &terms.lot_size().as_decimal().to_string(),
        ] {
            hash.update((text.len() as u64).to_be_bytes());
            hash.update(text.as_bytes());
        }
        for value in [
            bid.price().get(),
            bid.quantity().get(),
            ask.price().get(),
            ask.quantity().get(),
            received_at.unix_nanos(),
            source_at.unix_nanos(),
            valid_until.unix_nanos(),
        ] {
            hash.update(value.to_be_bytes());
        }
        hash.update(terms.definition_revision().get().to_be_bytes());
        let binding_digest: [u8; 32] = hash.finalize().into();
        let canonical = CanonicalStateDigest::new(
            EvidenceDigest::new(DigestAlgorithm::Sha256, binding_digest),
            CanonicalizationRule::new(
                SourceIdentifier::try_from("market-squawk-virtual-paper-v1")
                    .map_err(|_| VirtualPaperError::Policy)?,
                RuleVersion::new(1).map_err(|_| VirtualPaperError::Policy)?,
            ),
        );
        let original = current.evidence().binding();
        let binding = LiveEvidenceBinding::new(
            original.source_id().clone(),
            original.session_id().as_source_identifier().clone(),
            original.metadata_revision().clone(),
            source_policy.static_authorization().basis().clone(),
            source.venue().clone(),
            terms.instrument_id(),
            original.connection_generation(),
            source_policy.provider_product().clone(),
            source_policy.provider_channel().clone(),
            LiveEventClass::Quote,
            source.source_identifier().clone(),
            current.evidence().payload_digest(),
            canonical,
            None,
        )
        .map_err(|_| VirtualPaperError::Policy)?;
        let mut id = String::from("virtual-paper:");
        use std::fmt::Write as _;
        for byte in binding_digest {
            write!(&mut id, "{byte:02x}").map_err(|_| VirtualPaperError::Policy)?;
        }
        let assessment_id = QualificationAssessmentId::new(
            SourceIdentifier::try_from(id).map_err(|_| VirtualPaperError::Policy)?,
        );
        self.validate_current()?;
        Ok(ConsumedVirtualPaperAuthority {
            source: self,
            evidence: VirtualPaperEvidence {
                assessment_id,
                binding,
                binding_digest,
                valid_until,
            },
            terms,
            quote_digest,
            bid,
            ask,
            received_at,
            source_at,
            monotonic_deadline,
        })
    }
}

/// Explicit simulator increments and actual reference/calendar commitments; never broker terms.
#[derive(Clone, Copy, Debug)]
pub struct VirtualPaperPolicy {
    pub terms: InstrumentExecutionTerms,
    pub definition_digest: [u8; 32],
    pub calendar_digest: [u8; 32],
    pub calendar_available_at: Timestamp,
    pub opens_at: Timestamp,
    pub closes_at_exclusive: Timestamp,
}

/// Noncloneable one-use admission moved through risk and final dispatcher validation.
#[derive(Debug)]
pub struct ConsumedVirtualPaperAuthority {
    source: VirtualPaperSourceLease,
    evidence: VirtualPaperEvidence,
    terms: InstrumentExecutionTerms,
    quote_digest: [u8; 32],
    bid: BookLevel,
    ask: BookLevel,
    received_at: Timestamp,
    source_at: Timestamp,
    monotonic_deadline: Instant,
}
impl ConsumedVirtualPaperAuthority {
    /// Revocable freshness view for the owning source supervisor; this type cannot issue orders.
    pub fn currentness(&self) -> VirtualPaperCurrentness {
        VirtualPaperCurrentness {
            state: Arc::clone(&self.source.state),
            revision: self.source.revision,
            source: self.source.quote.current_lease().clone(),
            provider_identity: self.source.quote.provider_identity().clone(),
            valid_until: self.valid_until(),
            monotonic_deadline: self.monotonic_deadline,
        }
    }

    /// Stable identity of the original source quote, independent of actor revision/admission.
    pub const fn quote_digest(&self) -> [u8; 32] {
        self.quote_digest
    }

    pub fn validate_current(&self) -> Result<(), VirtualPaperError> {
        self.source.validate_current()?;
        if now()? > self.valid_until() || Instant::now() >= self.monotonic_deadline {
            return Err(VirtualPaperError::Expired);
        }
        Ok(())
    }
    pub const fn assessment_id(&self) -> &QualificationAssessmentId {
        self.evidence.assessment_id()
    }
    pub const fn binding(&self) -> &LiveEvidenceBinding {
        self.evidence.binding()
    }
    pub const fn binding_digest(&self) -> [u8; 32] {
        self.evidence.binding_digest()
    }
    pub const fn valid_until(&self) -> Timestamp {
        self.evidence.valid_until()
    }
    pub const fn terms(&self) -> InstrumentExecutionTerms {
        self.terms
    }
    pub const fn bid(&self) -> BookLevel {
        self.bid
    }
    pub const fn ask(&self) -> BookLevel {
        self.ask
    }
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }
    pub const fn source_at(&self) -> Timestamp {
        self.source_at
    }
    pub fn into_evidence(self) -> VirtualPaperEvidence {
        self.evidence
    }
}
/// Inert virtual-only receipt. It cannot mint source or live execution authority.
#[derive(Debug)]
pub struct VirtualPaperEvidence {
    assessment_id: QualificationAssessmentId,
    binding: LiveEvidenceBinding,
    binding_digest: [u8; 32],
    valid_until: Timestamp,
}
impl VirtualPaperEvidence {
    pub const fn assessment_id(&self) -> &QualificationAssessmentId {
        &self.assessment_id
    }
    pub const fn binding(&self) -> &LiveEvidenceBinding {
        &self.binding
    }
    pub const fn binding_digest(&self) -> [u8; 32] {
        self.binding_digest
    }
    pub const fn valid_until(&self) -> Timestamp {
        self.valid_until
    }
}
fn now() -> Result<Timestamp, VirtualPaperError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| VirtualPaperError::Clock)?;
    Ok(Timestamp::from_unix_nanos(
        i64::try_from(duration.as_nanos()).map_err(|_| VirtualPaperError::Clock)?,
    ))
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
pub enum VirtualPaperError {
    #[error("original current quote is unavailable")]
    Unavailable,
    #[error("source or actor revision is no longer current")]
    Revoked,
    #[error("virtual policy or original identity does not match")]
    Policy,
    #[error("quote cannot be represented exactly in declared virtual increments")]
    Granularity,
    #[error("virtual source admission expired")]
    Expired,
    #[error("clock arithmetic failed")]
    Clock,
}

/// Non-authoritative liveness view; no observation payload or order issuance API is retained.
#[derive(Debug)]
pub struct VirtualPaperCurrentness {
    state: Arc<SourceState>,
    revision: u64,
    source: market_squawk_sources::CurrentSourceAuthorityLease,
    provider_identity: market_squawk_sources::CurrentProviderIdentity,
    valid_until: Timestamp,
    monotonic_deadline: Instant,
}
impl VirtualPaperCurrentness {
    pub fn is_current(&self) -> bool {
        let Ok(at) = now() else {
            return false;
        };
        self.state.revision.load(Ordering::Acquire) == self.revision
            && at <= self.valid_until
            && Instant::now() < self.monotonic_deadline
            && self
                .source
                .validate_provider_identity_at(&self.provider_identity, at)
                .is_ok()
    }
}
