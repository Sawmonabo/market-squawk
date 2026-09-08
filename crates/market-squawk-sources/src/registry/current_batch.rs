/// Private deterministic shard-routing key for one homogeneous batch.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CurrentBatchKey {
    venue: VenueId,
    instrument: InstrumentId,
}

/// Exact receipt-validated raw-frame and decoder evidence shared across routed observations.
#[derive(Clone, Debug, Eq, PartialEq)]
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

    fn shared_allocation_charge(&self) -> Result<usize, RegistryError> {
        let dynamic = self
            .0
            .dynamic_retained_bytes()
            .map_err(|_| RegistryError::RetainedSizeOverflow)?;
        std::mem::size_of::<crate::DecoderEvidence>()
            .checked_add(crate::conservative_arc_control_block_charge::<
                crate::DecoderEvidence,
            >())
            .and_then(|bytes| bytes.checked_add(dynamic))
            .ok_or(RegistryError::RetainedSizeOverflow)
    }
}

/// Exact bounded HTTP-response receipt and adapter normalization rule shared across observations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CurrentHttpResponseEvidence(Arc<CurrentHttpResponseEvidenceInner>);

#[derive(Debug, Eq, PartialEq)]
struct CurrentHttpResponseEvidenceInner {
    receipt: crate::SegmentedHttpResponseReceipt,
    normalization_rule: market_squawk_domain::IntegrityRule,
}

impl CurrentHttpResponseEvidence {
    fn new(
        receipt: crate::SegmentedHttpResponseReceipt,
        normalization_rule: market_squawk_domain::IntegrityRule,
    ) -> Self {
        Self(Arc::new(CurrentHttpResponseEvidenceInner {
            receipt,
            normalization_rule,
        }))
    }

    /// Returns the complete exact response receipt, including every retained segment coordinate.
    pub fn receipt(&self) -> &crate::SegmentedHttpResponseReceipt {
        &self.0.receipt
    }

    fn shared_allocation_charge(&self) -> Result<usize, RegistryError> {
        let dynamic = self
            .0
            .receipt
            .dynamic_retained_bytes()
            .ok_or(RegistryError::RetainedSizeOverflow)?;
        std::mem::size_of::<CurrentHttpResponseEvidenceInner>()
            .checked_add(crate::conservative_arc_control_block_charge::<
                CurrentHttpResponseEvidenceInner,
            >())
            .and_then(|bytes| bytes.checked_add(dynamic))
            .and_then(|bytes| {
                bytes.checked_add(
                    self.0
                        .normalization_rule
                        .dynamic_retained_bytes()
                        .unwrap_or(usize::MAX),
                )
            })
            .ok_or(RegistryError::RetainedSizeOverflow)
    }
}

/// Closed exact source coordinate for one provider-normalized current observation batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CurrentObservationEvidence {
    /// One exact captured transport frame plus its decoder evidence.
    TransportFrame(CurrentFrameEvidence),
    /// One complete bounded HTTP response plus its adapter normalization rule.
    HttpResponse(CurrentHttpResponseEvidence),
}

impl CurrentObservationEvidence {
    /// Returns the exact process-local source/session/generation binding.
    pub fn binding(&self) -> &FrameSessionBinding {
        match self {
            Self::TransportFrame(evidence) => evidence.binding(),
            Self::HttpResponse(evidence) => evidence.receipt().binding(),
        }
    }

    /// Returns the trusted local receive time of the complete source object.
    pub fn received_at(&self) -> Timestamp {
        match self {
            Self::TransportFrame(evidence) => evidence.received_at(),
            Self::HttpResponse(evidence) => evidence.receipt().received_at(),
        }
    }

    /// Returns the SHA-256 digest of the exact transport payload or complete response body.
    pub fn payload_digest(&self) -> market_squawk_domain::EvidenceDigest {
        match self {
            Self::TransportFrame(evidence) => evidence.payload_digest(),
            Self::HttpResponse(evidence) => evidence.receipt().body_digest(),
        }
    }

    /// Returns the exact metadata-bound adapter rule used to normalize the source object.
    pub fn normalization_rule(&self) -> &market_squawk_domain::IntegrityRule {
        match self {
            Self::TransportFrame(evidence) => evidence.decoder_rule(),
            Self::HttpResponse(evidence) => &evidence.0.normalization_rule,
        }
    }

    /// Returns frame evidence only for transport-frame observations.
    pub const fn transport_frame(&self) -> Option<&CurrentFrameEvidence> {
        match self {
            Self::TransportFrame(evidence) => Some(evidence),
            Self::HttpResponse(_) => None,
        }
    }

    /// Returns complete response evidence only for HTTP-response observations.
    pub const fn http_response(&self) -> Option<&CurrentHttpResponseEvidence> {
        match self {
            Self::TransportFrame(_) => None,
            Self::HttpResponse(evidence) => Some(evidence),
        }
    }

    /// Returns a domain-separated digest of the exact source-object coordinate.
    pub fn coordinate_digest(&self) -> market_squawk_domain::EvidenceDigest {
        use sha2::Digest as _;

        let mut digest = sha2::Sha256::new();
        match self {
            Self::TransportFrame(evidence) => {
                digest.update(b"market-squawk/current-transport-frame-coordinate/v1");
                digest.update(evidence.frame_id().get().to_be_bytes());
            }
            Self::HttpResponse(evidence) => {
                return evidence.receipt().coordinate_digest();
            }
        }
        market_squawk_domain::EvidenceDigest::new(
            market_squawk_domain::DigestAlgorithm::Sha256,
            digest.finalize().into(),
        )
    }

    fn shared_allocation_charge(&self) -> Result<usize, RegistryError> {
        match self {
            Self::TransportFrame(evidence) => evidence.shared_allocation_charge(),
            Self::HttpResponse(evidence) => evidence.shared_allocation_charge(),
        }
    }
}

/// Bounded adapter-normalized observations derived from one exact complete HTTP response.
#[derive(Debug)]
pub struct NormalizedHttpResponseBatch {
    receipt: crate::SegmentedHttpResponseReceipt,
    normalization_rule: market_squawk_domain::IntegrityRule,
    observations: BoundedVec<crate::ProviderNormalizedObservation, { crate::MAX_DECODED_EVENTS }>,
}

impl NormalizedHttpResponseBatch {
    /// Binds normalized observations to one exact complete response without inventing frame evidence.
    pub fn try_new(
        receipt: crate::SegmentedHttpResponseReceipt,
        normalization_rule: market_squawk_domain::IntegrityRule,
        observations: Vec<crate::ProviderNormalizedObservation>,
    ) -> Result<Self, crate::DecodeError> {
        Ok(Self {
            receipt,
            normalization_rule,
            observations: crate::decoder::bounded_provider_observations(observations)?,
        })
    }

    /// Returns the complete response receipt.
    pub const fn receipt(&self) -> &crate::SegmentedHttpResponseReceipt {
        &self.receipt
    }

    /// Returns provider observations in response order.
    pub fn observations(&self) -> &[crate::ProviderNormalizedObservation] {
        self.observations.as_slice()
    }

    fn into_parts(
        self,
    ) -> (
        crate::SegmentedHttpResponseReceipt,
        market_squawk_domain::IntegrityRule,
        Vec<crate::ProviderNormalizedObservation>,
    ) {
        (
            self.receipt,
            self.normalization_rule,
            self.observations.into_vec(),
        )
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

    fn dynamic_retained_bytes(&self) -> usize {
        self.venue.retained_bytes()
    }
}

/// O(1)-clone session/health/capture authority retained by queued observations.
#[derive(Clone, Debug)]
pub struct CurrentSourceAuthorityLease {
    registry_id: u64,
    binding: FrameSessionBinding,
    runtime_health: Arc<crate::SourceHealthSnapshot>,
    health_epoch: u64,
    valid_from: Timestamp,
    valid_until: Timestamp,
    trusted_valid_from: Timestamp,
    trusted_valid_from_monotonic: RegistryMonotonicInstant,
    valid_until_monotonic: RegistryMonotonicInstant,
    lease: Arc<SessionLeaseState>,
    capture: crate::CaptureGenerationLease,
    budget: CurrentBudgetAuthority,
    clock: Arc<SealedRegistryClock>,
}

impl CurrentSourceAuthorityLease {
    /// Revalidates current generation, health epoch, capture, and inclusive deadline in O(1).
    ///
    /// `at` is the processor-owned wall-clock projection for the event being admitted. The
    /// registry also samples its sealed wall/monotonic clock here, so retaining an old in-range
    /// projection cannot extend authority across expiry or a wall-clock discontinuity.
    ///
    /// # Errors
    ///
    /// Fails after rollover, revision or capture changes, degradation, departure from the bounded
    /// healthy-refresh overlap, or deadline expiry.
    pub fn validate_at(&self, at: Timestamp) -> Result<(), RegistryError> {
        let trusted = self.clock.observe()?;
        if trusted.monotonic() < self.trusted_valid_from_monotonic {
            return Err(RegistryError::TrustedClockRegression);
        }
        if trusted.wall() >= self.trusted_valid_from
            && trusted.wall() <= self.valid_until
            && trusted.monotonic() <= self.valid_until_monotonic
            && at >= self.valid_from
            && at <= self.valid_until
            && self.lease.validate_health_epoch(self.health_epoch, at)
            && self.capture.is_healthy()
            && self.budget.is_available()
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

    /// Returns the registry-recorded health snapshot bound to this authority epoch.
    pub fn runtime_health(&self) -> &crate::SourceHealthSnapshot {
        &self.runtime_health
    }

    /// Returns the exact registry health epoch bound into this lease.
    pub const fn health_epoch(&self) -> u64 {
        self.health_epoch
    }

    /// Returns the inclusive accepted-health observation lower bound.
    pub const fn valid_from(&self) -> Timestamp {
        self.valid_from
    }

    /// Returns the inclusive static/runtime health deadline bound into this lease.
    pub const fn valid_until(&self) -> Timestamp {
        self.valid_until
    }

    /// Returns whether two opaque leases originate from the same process-local registry lineage.
    pub fn shares_registry_lineage_with(&self, other: &Self) -> bool {
        self.registry_id == other.registry_id
    }

    fn shared_allocation_charge(&self) -> Result<usize, RegistryError> {
        let health = self
            .runtime_health
            .conservative_arc_allocation_charge()
            .ok_or(RegistryError::RetainedSizeOverflow)?;
        let budget = self.budget.shared_allocation_charge()?;
        let clock = self
            .clock
            .shared_allocation_charge()
            .ok_or(RegistryError::RetainedSizeOverflow)?;
        let session = std::mem::size_of::<SessionLeaseState>()
            .checked_add(crate::conservative_arc_control_block_charge::<
                SessionLeaseState,
            >())
            .ok_or(RegistryError::RetainedSizeOverflow)?;
        let capture = self
            .capture
            .shared_allocation_charge()
            .ok_or(RegistryError::RetainedSizeOverflow)?;
        current_authority_shared_allocation_charge(session, capture, budget, clock)?
            .checked_add(health)
            .ok_or(RegistryError::RetainedSizeOverflow)
    }
}

fn current_authority_shared_allocation_charge(
    session: usize,
    capture: usize,
    budget: usize,
    clock: usize,
) -> Result<usize, RegistryError> {
    session
        .checked_add(capture)
        .and_then(|bytes| bytes.checked_add(budget))
        .and_then(|bytes| bytes.checked_add(clock))
        .ok_or(RegistryError::RetainedSizeOverflow)
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

    fn dynamic_retained_bytes(&self) -> Option<usize> {
        let Self {
            source_id,
            venue,
            instrument: _,
            provider_product,
            provider_channel,
        } = self;
        source_id
            .retained_bytes()
            .checked_add(venue.retained_bytes())?
            .checked_add(provider_product.as_source_identifier().retained_bytes())?
            .checked_add(provider_channel.as_source_identifier().retained_bytes())
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

    fn dynamic_retained_bytes(&self) -> Option<usize> {
        let Self {
            source_id,
            venue,
            provider_product,
            provider_channel,
            event_class: _,
            depth: _,
            delay: _,
            consolidation: _,
            delivery: _,
            evidence,
            effective_from: _,
            effective_until: _,
            metadata_revision,
        } = self;
        source_id
            .retained_bytes()
            .checked_add(venue.retained_bytes())?
            .checked_add(provider_product.as_source_identifier().retained_bytes())?
            .checked_add(provider_channel.as_source_identifier().retained_bytes())?
            .checked_add(evidence.dynamic_retained_bytes()?)?
            .checked_add(metadata_revision.as_source_identifier().retained_bytes())
    }
}

fn current_routed_batch_retained_bytes(
    batch_key_allocation: usize,
    observation_count: usize,
    observation_unique_allocations: usize,
    authority_shared_allocation: usize,
    evidence_shared_allocation: usize,
) -> Result<usize, RegistryError> {
    observation_count
        .checked_mul(std::mem::size_of::<CurrentProviderObservation>())
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<CurrentDecodedProviderBatch>()))
        .and_then(|bytes| bytes.checked_add(batch_key_allocation))
        .and_then(|bytes| bytes.checked_add(observation_unique_allocations))
        .and_then(|bytes| bytes.checked_add(authority_shared_allocation))
        .and_then(|bytes| bytes.checked_add(evidence_shared_allocation))
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
        let Self {
            stream_key,
            quality_ceiling: _,
            static_authorization,
            runtime_authorization,
            coverage,
            runtime_coverage,
            rule,
            protocol,
            freshness: _,
            valid_until: _,
            universe_evidence,
        } = self;
        stream_key
            .dynamic_retained_bytes()
            .and_then(|bytes| bytes.checked_add(static_authorization.dynamic_retained_bytes()?))
            .and_then(|bytes| bytes.checked_add(runtime_authorization.dynamic_retained_bytes()?))
            .and_then(|bytes| bytes.checked_add(coverage.dynamic_retained_bytes()?))
            .and_then(|bytes| bytes.checked_add(runtime_coverage.dynamic_retained_bytes()?))
            .and_then(|bytes| bytes.checked_add(rule.dynamic_retained_bytes()?))
            .and_then(|bytes| bytes.checked_add(protocol.dynamic_retained_bytes()?))
            .and_then(|bytes| {
                bytes.checked_add(
                    universe_evidence
                        .as_ref()
                        .map_or(Some(0), ExactPayloadEvidence::dynamic_retained_bytes)?,
                )
            })
            .ok_or(RegistryError::RetainedSizeOverflow)
    }
}

/// Intact current provider observation plus exact policy and current-authority lease.
#[derive(Debug)]
pub struct CurrentProviderObservation {
    key: CurrentBatchKey,
    evidence: CurrentObservationEvidence,
    observation: crate::ProviderNormalizedObservation,
    policy: CurrentLivePolicy,
    authority: CurrentSourceAuthorityLease,
}

impl CurrentProviderObservation {
    /// Returns the deterministic venue/instrument routing key.
    pub const fn key(&self) -> &CurrentBatchKey {
        &self.key
    }

    /// Returns exact receipt-validated frame or complete-response evidence.
    pub const fn evidence(&self) -> &CurrentObservationEvidence {
        &self.evidence
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

include!("current_batch/tests.rs");
include!("current_batch/validation.rs");
