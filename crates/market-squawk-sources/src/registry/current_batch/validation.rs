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
    /// Health evidence fell outside the registry-sealed session/report/validation time chain.
    #[error("source health evidence violates trusted temporal ordering")]
    InvalidHealthTemporalOrder,
    /// The sealed registry clock could not produce a representable observation.
    #[error("trusted registry clock is unavailable")]
    TrustedClockUnavailable,
    /// The sealed registry clock moved backward relative to a prior observation.
    #[error("trusted registry clock regressed")]
    TrustedClockRegression,
    /// A wall deadline could not be converted into the sealed monotonic clock domain.
    #[error("source health monotonic deadline overflowed")]
    HealthDeadlineOverflow,
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
