/// Private deterministic shard-routing key for one homogeneous batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CurrentBatchKey {
    venue: VenueId,
    instrument: InstrumentId,
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
}

/// Full mutable stream-state identity inside one instrument-owned shard.
#[derive(Clone, Debug, Eq, PartialEq)]
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

/// Exact static and runtime policy retained with one current observation.
#[derive(Debug)]
pub struct CurrentLivePolicy {
    stream_key: CurrentStreamKey,
    quality_ceiling: market_squawk_domain::DataQuality,
    static_authorization: crate::AuthorizationGrant,
    runtime_authorization: crate::AuthorizationHealth,
    static_coverage: crate::SourceCoverage,
    runtime_coverage: crate::CoverageHealth,
    rule: crate::LiveCoverageRule,
    protocol: crate::LiveProtocolProfile,
    freshness: crate::FreshnessPolicy,
    provider_product: market_squawk_domain::ProviderProduct,
    provider_channel: market_squawk_domain::ProviderChannel,
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
    pub const fn static_coverage(&self) -> &crate::SourceCoverage {
        &self.static_coverage
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
        &self.provider_product
    }
    pub const fn provider_channel(&self) -> &market_squawk_domain::ProviderChannel {
        &self.provider_channel
    }
    pub const fn valid_until(&self) -> Timestamp {
        self.valid_until
    }
    pub const fn universe_evidence(&self) -> Option<&ExactPayloadEvidence> {
        self.universe_evidence.as_ref()
    }
}

/// Intact current provider observation plus exact policy and current-authority lease.
#[derive(Debug)]
pub struct CurrentProviderObservation {
    observation: crate::ProviderNormalizedObservation,
    policy: CurrentLivePolicy,
    authority: CurrentSourceAuthorityLease,
}

impl CurrentProviderObservation {
    pub const fn observation(&self) -> &crate::ProviderNormalizedObservation {
        &self.observation
    }
    pub const fn policy(&self) -> &CurrentLivePolicy {
        &self.policy
    }
    pub const fn authority(&self) -> &CurrentSourceAuthorityLease {
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
    pub const fn key(&self) -> &CurrentBatchKey {
        &self.key
    }
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
    pub fn validate_at(&self, at: Timestamp) -> Result<(), RegistryError> {
        self.authority.validate_at(at)
    }
    pub fn into_observations(self) -> CurrentObservationIter {
        CurrentObservationIter(self.observations.into_vec().into_iter())
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
