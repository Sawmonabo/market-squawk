/// Private deterministic shard-routing key for one homogeneous batch.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CurrentBatchKey {
    venue: VenueId,
    instrument: InstrumentId,
}

/// Exact receipt-validated raw-frame and decoder evidence shared across routed observations.
#[derive(Clone, Debug)]
pub struct CurrentFrameEvidence(Arc<crate::DecoderEvidence>);

impl CurrentFrameEvidence {
    fn new(evidence: crate::DecoderEvidence) -> Self {
        Self(Arc::new(evidence))
    }

    /// Returns the exact current-session binding carried by the decoded frame.
    pub fn binding(&self) -> &FrameSessionBinding {
        self.0.binding()
    }

    /// Returns the nonzero generation-local raw-frame identity.
    pub fn frame_id(&self) -> crate::FrameId {
        self.0.frame_id()
    }

    /// Returns the trusted local raw-frame receive time.
    pub fn received_at(&self) -> Timestamp {
        self.0.received_at()
    }

    /// Returns the SHA-256 digest of the exact raw transport payload.
    pub fn payload_digest(&self) -> market_squawk_domain::EvidenceDigest {
        self.0.payload_digest()
    }

    /// Returns the exact metadata-bound decoder rule.
    pub fn decoder_rule(&self) -> &market_squawk_domain::IntegrityRule {
        self.0.decoder_rule()
    }
}

impl CurrentBatchKey {
    /// Returns exact venue routing identity.
    pub const fn venue(&self) -> &VenueId {
        &self.venue
    }

    /// Returns exact internal instrument routing identity.
    pub const fn instrument(&self) -> InstrumentId {
        self.instrument
    }
}

/// O(1)-clone session/health/capture authority retained by queued observations.
#[derive(Clone, Debug)]
pub struct CurrentSourceAuthorityLease {
    registry_id: u64,
    binding: FrameSessionBinding,
    health_epoch: u64,
    valid_until: Timestamp,
    lease: Arc<SessionLeaseState>,
    capture: crate::CaptureGenerationLease,
}

impl CurrentSourceAuthorityLease {
    /// Revalidates current generation, health epoch, capture, and inclusive deadline in O(1).
    ///
    /// # Errors
    ///
    /// Fails after rollover/revision/health/capture changes or deadline expiry.
    pub fn validate_at(&self, at: Timestamp) -> Result<(), RegistryError> {
        if at <= self.valid_until
            && self.lease.validate_health_epoch(self.health_epoch, at)
            && self.capture.is_healthy()
        {
            Ok(())
        } else {
            Err(RegistryError::HealthNotQualified)
        }
    }

    /// Returns the exact process-local session binding.
    pub const fn binding(&self) -> &FrameSessionBinding {
        &self.binding
    }

    /// Returns the exact registry health epoch bound into this lease.
    pub const fn health_epoch(&self) -> u64 {
        self.health_epoch
    }

    /// Returns the inclusive static/runtime health deadline bound into this lease.
    pub const fn valid_until(&self) -> Timestamp {
        self.valid_until
    }

    /// Returns whether two opaque leases originate from the same process-local registry lineage.
    pub fn shares_registry_lineage_with(&self, other: &Self) -> bool {
        self.registry_id == other.registry_id
    }
}

/// Full mutable stream-state identity inside one instrument-owned shard.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CurrentStreamKey {
    source_id: SourceId,
    venue: VenueId,
    instrument: InstrumentId,
    provider_product: market_squawk_domain::ProviderProduct,
    provider_channel: market_squawk_domain::ProviderChannel,
}

impl CurrentStreamKey {
    /// Returns source identity.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }
    /// Returns venue identity.
    pub const fn venue(&self) -> &VenueId {
        &self.venue
    }
    /// Returns instrument identity.
    pub const fn instrument(&self) -> InstrumentId {
        self.instrument
    }
    /// Returns provider product identity.
    pub const fn provider_product(&self) -> &market_squawk_domain::ProviderProduct {
        &self.provider_product
    }
    /// Returns provider channel identity.
    pub const fn provider_channel(&self) -> &market_squawk_domain::ProviderChannel {
        &self.provider_channel
    }
}

/// Compact exact coverage projection retained with one current provider observation.
///
/// The registry creates this value only after proving exact instrument membership. It therefore
/// retains the selected scope and never the metadata declaration's potentially 4,096-instrument
/// universe.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CurrentCoveragePolicy {
    source_id: SourceId,
    venue: VenueId,
    provider_product: market_squawk_domain::ProviderProduct,
    provider_channel: market_squawk_domain::ProviderChannel,
    event_class: LiveEventClass,
    depth: Option<MarketDepth>,
    delay: CoverageDelay,
    consolidation: CoverageConsolidation,
    delivery: DeliveryEvidence,
    evidence: ExactPayloadEvidence,
    effective_from: Timestamp,
    effective_until: Option<Timestamp>,
    metadata_revision: MetadataRevision,
}

impl CurrentCoveragePolicy {
    /// Returns the registered source identity.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact covered venue.
    pub const fn venue(&self) -> &VenueId {
        &self.venue
    }

    /// Returns the exact provider product.
    pub const fn provider_product(&self) -> &market_squawk_domain::ProviderProduct {
        &self.provider_product
    }

    /// Returns the exact provider channel.
    pub const fn provider_channel(&self) -> &market_squawk_domain::ProviderChannel {
        &self.provider_channel
    }

    /// Returns the exact event class.
    pub const fn event_class(&self) -> LiveEventClass {
        self.event_class
    }

    /// Returns market depth for book events.
    pub const fn depth(&self) -> Option<MarketDepth> {
        self.depth
    }

    /// Returns declared delivery delay semantics.
    pub const fn delay(&self) -> CoverageDelay {
        self.delay
    }

    /// Returns the declared venue-consolidation class.
    pub const fn consolidation(&self) -> CoverageConsolidation {
        self.consolidation
    }

    /// Returns the independently declared direct/indirect delivery relationship.
    pub const fn delivery(&self) -> DeliveryEvidence {
        self.delivery
    }

    /// Returns the exact static coverage declaration evidence.
    pub const fn evidence(&self) -> &ExactPayloadEvidence {
        &self.evidence
    }

    /// Returns the first effective instant.
    pub const fn effective_from(&self) -> Timestamp {
        self.effective_from
    }

    /// Returns the inclusive final effective instant, if bounded.
    pub const fn effective_until(&self) -> Option<Timestamp> {
        self.effective_until
    }

    /// Returns the exact source-metadata revision.
    pub const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }
}

const CURRENT_POLICY_MAX_SOURCE_IDENTIFIERS: usize = 32;
const CURRENT_POLICY_MAX_SOURCE_IDS: usize = 2;
const CURRENT_POLICY_MAX_VENUE_IDS: usize = 2;

fn current_policy_deep_allocation_charge() -> Result<usize, RegistryError> {
    let source_identifiers = market_squawk_domain::SourceIdentifier::MAX_LENGTH
        .checked_mul(CURRENT_POLICY_MAX_SOURCE_IDENTIFIERS)
        .ok_or(RegistryError::RetainedSizeOverflow)?;
    let source_ids = SourceId::MAX_LENGTH
        .checked_mul(CURRENT_POLICY_MAX_SOURCE_IDS)
        .ok_or(RegistryError::RetainedSizeOverflow)?;
    let venues = VenueId::MAX_LENGTH
        .checked_mul(CURRENT_POLICY_MAX_VENUE_IDS)
        .ok_or(RegistryError::RetainedSizeOverflow)?;
    source_identifiers
        .checked_add(source_ids)
        .and_then(|bytes| bytes.checked_add(venues))
        .ok_or(RegistryError::RetainedSizeOverflow)
}

fn current_authority_shared_allocation_charge() -> Result<usize, RegistryError> {
    std::mem::size_of::<SessionLeaseState>()
        .checked_add(SourceId::MAX_LENGTH)
        .and_then(|bytes| {
            market_squawk_domain::SourceIdentifier::MAX_LENGTH
                .checked_mul(2)
                .and_then(|identity_bytes| bytes.checked_add(identity_bytes))
        })
        .and_then(|bytes| bytes.checked_add(256))
        .ok_or(RegistryError::RetainedSizeOverflow)
}

fn current_frame_shared_allocation_charge() -> Result<usize, RegistryError> {
    std::mem::size_of::<crate::DecoderEvidence>()
        .checked_add(SourceId::MAX_LENGTH)
        .and_then(|bytes| {
            market_squawk_domain::SourceIdentifier::MAX_LENGTH
                .checked_mul(3)
                .and_then(|identity_bytes| bytes.checked_add(identity_bytes))
        })
        .ok_or(RegistryError::RetainedSizeOverflow)
}

fn current_routed_batch_retained_bytes(
    observation_count: usize,
    policy_and_provider_allocations: usize,
) -> Result<usize, RegistryError> {
    let authority_allocation = current_authority_shared_allocation_charge()?;
    let frame_allocation = current_frame_shared_allocation_charge()?;
    observation_count
        .checked_mul(std::mem::size_of::<CurrentProviderObservation>())
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<CurrentDecodedProviderBatch>()))
        .and_then(|bytes| bytes.checked_add(policy_and_provider_allocations))
        .and_then(|bytes| bytes.checked_add(authority_allocation))
        .and_then(|bytes| bytes.checked_add(frame_allocation))
        .ok_or(RegistryError::RetainedSizeOverflow)
}

/// Exact static and runtime policy retained with one current observation.
#[derive(Debug)]
pub struct CurrentLivePolicy {
    stream_key: CurrentStreamKey,
    quality_ceiling: market_squawk_domain::DataQuality,
    static_authorization: crate::AuthorizationGrant,
    runtime_authorization: crate::AuthorizationHealth,
    coverage: CurrentCoveragePolicy,
    runtime_coverage: crate::CoverageHealth,
    rule: crate::LiveCoverageRule,
    protocol: crate::LiveProtocolProfile,
    freshness: crate::FreshnessPolicy,
    valid_until: Timestamp,
    universe_evidence: Option<ExactPayloadEvidence>,
}

impl CurrentLivePolicy {
    /// Returns mutable stream-state identity, distinct from shard routing key.
    pub const fn stream_key(&self) -> &CurrentStreamKey {
        &self.stream_key
    }
    pub const fn quality_ceiling(&self) -> market_squawk_domain::DataQuality {
        self.quality_ceiling
    }
    pub const fn static_authorization(&self) -> &crate::AuthorizationGrant {
        &self.static_authorization
    }
    pub const fn runtime_authorization(&self) -> &crate::AuthorizationHealth {
        &self.runtime_authorization
    }
    /// Returns the compact exact-scope coverage projection.
    pub const fn coverage(&self) -> &CurrentCoveragePolicy {
        &self.coverage
    }
    pub const fn runtime_coverage(&self) -> &crate::CoverageHealth {
        &self.runtime_coverage
    }
    pub const fn rule(&self) -> &crate::LiveCoverageRule {
        &self.rule
    }
    pub const fn protocol(&self) -> &crate::LiveProtocolProfile {
        &self.protocol
    }
    pub const fn freshness(&self) -> crate::FreshnessPolicy {
        self.freshness
    }
    pub const fn provider_product(&self) -> &market_squawk_domain::ProviderProduct {
        self.coverage.provider_product()
    }
    pub const fn provider_channel(&self) -> &market_squawk_domain::ProviderChannel {
        self.coverage.provider_channel()
    }
    pub const fn valid_until(&self) -> Timestamp {
        self.valid_until
    }
    pub const fn universe_evidence(&self) -> Option<&ExactPayloadEvidence> {
        self.universe_evidence.as_ref()
    }

    fn deep_allocation_charge(&self) -> Result<usize, RegistryError> {
        current_policy_deep_allocation_charge()
    }
}

/// Intact current provider observation plus exact policy and current-authority lease.
#[derive(Debug)]
pub struct CurrentProviderObservation {
    key: CurrentBatchKey,
    frame_evidence: CurrentFrameEvidence,
    observation: crate::ProviderNormalizedObservation,
    policy: CurrentLivePolicy,
    authority: CurrentSourceAuthorityLease,
}

impl CurrentProviderObservation {
    /// Returns the deterministic venue/instrument routing key.
    pub const fn key(&self) -> &CurrentBatchKey {
        &self.key
    }

    /// Returns exact receipt-validated raw-frame and decoder evidence.
    pub const fn frame_evidence(&self) -> &CurrentFrameEvidence {
        &self.frame_evidence
    }

    pub const fn observation(&self) -> &crate::ProviderNormalizedObservation {
        &self.observation
    }
    pub const fn policy(&self) -> &CurrentLivePolicy {
        &self.policy
    }
    pub const fn authority(&self) -> &CurrentSourceAuthorityLease {
        &self.authority
    }

    /// Returns the current process-local source authority lease.
    pub const fn current_lease(&self) -> &CurrentSourceAuthorityLease {
        &self.authority
    }

    /// Returns full source/channel stream-state identity.
    pub const fn stream_key(&self) -> &CurrentStreamKey {
        self.policy.stream_key()
    }
}

/// Exact-size consuming iterator preserving provider wire order.
#[derive(Debug)]
pub struct CurrentObservationIter(std::vec::IntoIter<CurrentProviderObservation>);

impl Iterator for CurrentObservationIter {
    type Item = CurrentProviderObservation;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl ExactSizeIterator for CurrentObservationIter {}

/// Owned, non-serializable provider batch accepted by bounded shard ingress.
#[derive(Debug)]
pub struct CurrentDecodedProviderBatch {
    key: CurrentBatchKey,
    retained_bytes: usize,
    authority: CurrentSourceAuthorityLease,
    observations: Box<[CurrentProviderObservation]>,
}

impl CurrentDecodedProviderBatch {
    /// Returns the deterministic venue/instrument shard-routing key.
    pub const fn key(&self) -> &CurrentBatchKey {
        &self.key
    }

    /// Returns the conservative retained-memory charge for bounded-queue admission.
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    /// Returns the opaque current source lease required for pre-admission generation binding.
    ///
    /// This validation-only capability is non-Serde and cannot be reconstructed from frame IDs,
    /// session fields, archived batches, or caller-authored health values.
    pub const fn current_lease(&self) -> &CurrentSourceAuthorityLease {
        &self.authority
    }

    /// Revalidates the batch's current source, health, capture, and inclusive deadline authority.
    ///
    /// # Errors
    ///
    /// Fails after source/capture degradation, generation rollover, health revision, or deadline
    /// expiry.
    pub fn validate_at(&self, at: Timestamp) -> Result<(), RegistryError> {
        self.authority.validate_at(at)
    }

    /// Consumes the homogeneous routing batch in original provider wire order.
    pub fn into_observations(self) -> CurrentObservationIter {
        CurrentObservationIter(self.observations.into_vec().into_iter())
    }
}

/// Bounded routed batches produced from one receipt-validated provider frame.
#[derive(Debug)]
pub struct CurrentDecodedProviderBatches {
    batches: Box<[CurrentDecodedProviderBatch]>,
}

impl CurrentDecodedProviderBatches {
    /// Returns the number of distinct venue/instrument routing groups.
    pub fn len(&self) -> usize {
        self.batches.len()
    }

    /// Returns whether the validated frame produced no routing groups.
    ///
    /// Construction rejects empty decoded frames, so production values always return false.
    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }
}

/// Exact-size consuming iterator preserving first-key and per-key wire order.
#[derive(Debug)]
pub struct CurrentBatchIter(std::vec::IntoIter<CurrentDecodedProviderBatch>);

impl Iterator for CurrentBatchIter {
    type Item = CurrentDecodedProviderBatch;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl ExactSizeIterator for CurrentBatchIter {}

impl IntoIterator for CurrentDecodedProviderBatches {
    type Item = CurrentDecodedProviderBatch;
    type IntoIter = CurrentBatchIter;

    fn into_iter(self) -> Self::IntoIter {
        CurrentBatchIter(self.batches.into_vec().into_iter())
    }
}

#[cfg(test)]
mod stream_key_tests {
    use std::mem::size_of;
    use std::collections::HashSet;

    use market_squawk_domain::{
        InstrumentId, ProviderChannel, ProviderProduct, SourceId, SourceIdentifier, VenueId,
    };

    use super::{
        CurrentProviderObservation, CurrentStreamKey, current_routed_batch_retained_bytes,
    };

    fn key(
        source: &str,
        venue: &str,
        instrument: &str,
        product: &str,
        channel: &str,
    ) -> Result<CurrentStreamKey, Box<dyn std::error::Error>> {
        Ok(CurrentStreamKey {
            source_id: SourceId::try_from(source)?,
            venue: VenueId::try_from(venue)?,
            instrument: instrument.parse::<InstrumentId>()?,
            provider_product: ProviderProduct::new(SourceIdentifier::try_from(product)?),
            provider_channel: ProviderChannel::new(SourceIdentifier::try_from(channel)?),
        })
    }

    #[test]
    fn hash_identity_separates_all_five_dimensions() -> Result<(), Box<dyn std::error::Error>> {
        let first_instrument = "018f0000-0000-7000-8000-000000000001";
        let second_instrument = "018f0000-0000-7000-8000-000000000002";
        let keys = HashSet::from([
            key("kraken-primary", "kraken", first_instrument, "BTC/USD", "book")?,
            key("kraken-secondary", "kraken", first_instrument, "BTC/USD", "book")?,
            key("kraken-primary", "coinbase", first_instrument, "BTC/USD", "book")?,
            key("kraken-primary", "kraken", second_instrument, "BTC/USD", "book")?,
            key("kraken-primary", "kraken", first_instrument, "XBT/USD", "book")?,
            key("kraken-primary", "kraken", first_instrument, "BTC/USD", "level3")?,
        ]);

        assert_eq!(keys.len(), 6);
        Ok(())
    }

    #[test]
    fn routed_batch_charges_shared_authority_and_frame_allocations_once()
    -> Result<(), Box<dyn std::error::Error>> {
        const PER_OBSERVATION_DYNAMIC: usize = 137;
        let one = current_routed_batch_retained_bytes(1, PER_OBSERVATION_DYNAMIC)?;
        let two = current_routed_batch_retained_bytes(2, PER_OBSERVATION_DYNAMIC * 2)?;

        assert_eq!(
            two.checked_sub(one),
            Some(size_of::<CurrentProviderObservation>() + PER_OBSERVATION_DYNAMIC)
        );
        Ok(())
    }
}

fn validate_observation_profile(
    protocol: &crate::LiveProtocolProfile,
    observation: &crate::ProviderNormalizedObservation,
) -> Result<(), RegistryError> {
    let sequence_matches = match (protocol.sequence(), observation.sequence()) {
        (
            crate::SequenceValidationProfile::Unsupported { rule: expected },
            crate::ProviderSequenceEvidence::Unsupported { rule: observed },
        )
        | (
            crate::SequenceValidationProfile::Provided { rule: expected, .. },
            crate::ProviderSequenceEvidence::Provided { rule: observed, .. },
        ) => expected == observed,
        _ => false,
    };
    let checksum_matches = match (protocol.checksum(), observation.checksum()) {
        (
            crate::ChecksumValidationProfile::Unsupported { rule: expected },
            crate::ProviderChecksumEvidence::Unsupported { rule: observed },
        )
        | (
            crate::ChecksumValidationProfile::Provided { rule: expected, .. },
            crate::ProviderChecksumEvidence::Provided { rule: observed, .. },
        ) => expected == observed,
        _ => false,
    };
    let timestamp_matches = match observation.timestamp() {
        crate::ProviderTimestampEvidence::Provided { rule, .. } => {
            protocol.source_timestamps() && rule == protocol.timestamp_rule()
        }
        crate::ProviderTimestampEvidence::AuthoritativelyAbsent(rule) => {
            !protocol.source_timestamps() && rule == protocol.timestamp_rule()
        }
    };
    let semantics = protocol.semantic_interpretation();
    let semantic_rule_matches = match observation.payload() {
        crate::ProviderObservationPayload::Trade { aggressor, .. } => {
            aggressor.rule() == semantics.aggressor_rule()
        }
        crate::ProviderObservationPayload::TradingHalt { status, .. }
        | crate::ProviderObservationPayload::InstrumentStatus { status, .. } => {
            status.rule() == semantics.trading_status_rule()
        }
        crate::ProviderObservationPayload::Auction { rule, .. } => rule == semantics.auction_rule(),
        crate::ProviderObservationPayload::CorporateAction { rule, .. } => {
            rule == semantics.corporate_action_rule()
        }
        _ => true,
    };
    if sequence_matches && checksum_matches && timestamp_matches && semantic_rule_matches {
        Ok(())
    } else {
        Err(RegistryError::DecoderProfileMismatch)
    }
}

/// Source registry validation failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum RegistryError {
    /// Process-local registry identity space was exhausted.
    #[error("registry identity space exhausted")]
    RegistryIdentityExhausted,
    /// Source identity is already registered.
    #[error("source identity is already registered")]
    SourceAlreadyRegistered,
    /// Source identity is not present.
    #[error("source identity is not registered")]
    UnknownSource,
    /// Authorization or coverage was not effective at the explicit validation time.
    #[error("source metadata is not effective at validation time")]
    MetadataNotEffective,
    /// A handle came from a different registry or source.
    #[error("source handle was transplanted")]
    HandleTransplanted,
    /// Metadata/session state advanced after the handle was minted.
    #[error("source handle is stale")]
    StaleHandle,
    /// Source was explicitly revoked.
    #[error("source registration is revoked")]
    SourceRevoked,
    /// Metadata replacement did not change revision identity.
    #[error("metadata revision must advance")]
    RevisionNotAdvanced,
    /// A prior metadata incarnation already used the proposed revision identity.
    #[error("metadata revision identity was already used for this source")]
    RevisionAlreadyUsed,
    /// Bounded source metadata revision history is full.
    #[error("source metadata revision history exhausted")]
    RevisionHistoryExhausted,
    /// Session generation did not strictly advance.
    #[error("connection generation must strictly advance")]
    GenerationNotAdvanced,
    /// Session ended or another session became current.
    #[error("source session is not current")]
    SessionNotCurrent,
    /// Registry entry epoch overflowed.
    #[error("source registry epoch exhausted")]
    EpochExhausted,
    /// Shared provider-budget coordinator rejected or could not initialize policy.
    #[error("source provider-budget coordination failed")]
    BudgetCoordinator,
    /// The sole generation-bound raw frame factory was already issued.
    #[error("raw frame factory was already taken for this session")]
    RawFrameFactoryAlreadyTaken,
    /// Persisted authority state violated uniqueness or nonempty-history invariants.
    #[error("registry authority state is invalid")]
    InvalidAuthorityState,
    /// Persisted authority state exceeded a configured source/budget bound.
    #[error("registry authority state capacity exceeded")]
    AuthorityStateCapacity,
    /// Venue, instrument, event, or depth is outside evidenced metadata coverage.
    #[error("live source scope is not covered by current metadata")]
    LiveScopeNotCovered,
    /// Health evidence identity differed from the current session tuple.
    #[error("source health evidence is bound to another session")]
    HealthBindingMismatch,
    /// Snapshot freshness thresholds differ from current metadata.
    #[error("source health freshness policy conflicts with current metadata")]
    HealthPolicyMismatch,
    /// Sole current-health reporter was already moved to supervision.
    #[error("source current-health reporter was already taken")]
    HealthReporterAlreadyTaken,
    /// Current registry-owned health has not established live eligibility.
    #[error("source health is not qualified for live authority")]
    HealthNotQualified,
    /// Health observation did not strictly advance the current generation's health clock.
    #[error("source health observation is stale or replayed")]
    StaleHealthObservation,
    /// Decoder rule or provider evidence conflicts with current metadata-bound profiles.
    #[error("decoded provider batch conflicts with current protocol profile")]
    DecoderProfileMismatch,
    /// Current-generation health authority epoch exhausted; the generation fails closed.
    #[error("source health authority epoch exhausted")]
    HealthEpochExhausted,
    /// Sole admission issuer was already moved out of the registry for this generation.
    #[error("capture admission issuer was already taken")]
    CaptureIssuerAlreadyTaken,
    /// Capture is initializing or terminally incomplete for this generation.
    #[error("capture generation is not healthy")]
    CaptureNotHealthy,
    /// Capture receipt did not match decoded exact frame evidence/current generation.
    #[error("capture admission receipt does not match decoded frame evidence")]
    CaptureReceiptMismatch,
    /// Universe evidence named the wrong product or was not effective.
    #[error("instrument universe attestation conflicts with current source metadata")]
    UniverseAttestationMismatch,
    /// Deep retained-size accounting overflowed.
    #[error("current decoded batch retained-size accounting overflow")]
    RetainedSizeOverflow,
}

fn contains_duplicate_revisions(revisions: &[MetadataRevision]) -> bool {
    revisions
        .iter()
        .enumerate()
        .any(|(index, revision)| revisions[index.saturating_add(1)..].contains(revision))
}
