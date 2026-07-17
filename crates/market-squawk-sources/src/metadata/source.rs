/// Independent connection-idle, transport-age, source-age, and clock-skew thresholds.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FreshnessPolicy {
    max_connection_idle_nanos: u64,
    max_transport_age_nanos: u64,
    max_source_age_nanos: u64,
    max_market_age_nanos: u64,
    max_clock_skew_nanos: u64,
}

impl FreshnessPolicy {
    /// Constructs nonzero connection and market freshness thresholds.
    ///
    /// # Errors
    ///
    /// Rejects a zero connection-idle or market-age bound.
    pub const fn try_new(
        max_connection_idle_nanos: u64,
        max_transport_age_nanos: u64,
        max_source_age_nanos: u64,
        max_market_age_nanos: u64,
        max_clock_skew_nanos: u64,
    ) -> Result<Self, SourceMetadataError> {
        if max_connection_idle_nanos == 0
            || max_transport_age_nanos == 0
            || max_source_age_nanos == 0
            || max_market_age_nanos == 0
            || max_connection_idle_nanos > i64::MAX as u64
            || max_transport_age_nanos > i64::MAX as u64
            || max_source_age_nanos > i64::MAX as u64
            || max_market_age_nanos > i64::MAX as u64
            || max_clock_skew_nanos > i64::MAX as u64
        {
            Err(SourceMetadataError::InvalidFreshnessPolicy)
        } else {
            Ok(Self {
                max_connection_idle_nanos,
                max_transport_age_nanos,
                max_source_age_nanos,
                max_market_age_nanos,
                max_clock_skew_nanos,
            })
        }
    }

    /// Returns the market-data age ceiling; heartbeats do not change its clock.
    pub const fn max_market_age_nanos(self) -> u64 {
        self.max_market_age_nanos
    }

    /// Returns the maximum receive-to-validation transport age.
    pub const fn max_transport_age_nanos(self) -> u64 {
        self.max_transport_age_nanos
    }

    /// Returns the maximum provider-source-to-validation age.
    pub const fn max_source_age_nanos(self) -> u64 {
        self.max_source_age_nanos
    }

    /// Returns the maximum connection-idle interval.
    pub const fn max_connection_idle_nanos(self) -> u64 {
        self.max_connection_idle_nanos
    }

    /// Returns the maximum tolerated provider clock skew.
    pub const fn max_clock_skew_nanos(self) -> u64 {
        self.max_clock_skew_nanos
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FreshnessPolicyWire {
    max_connection_idle_nanos: u64,
    max_transport_age_nanos: u64,
    max_source_age_nanos: u64,
    max_market_age_nanos: u64,
    max_clock_skew_nanos: u64,
}

impl<'de> Deserialize<'de> for FreshnessPolicy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = FreshnessPolicyWire::deserialize(deserializer)?;
        Self::try_new(
            wire.max_connection_idle_nanos,
            wire.max_transport_age_nanos,
            wire.max_source_age_nanos,
            wire.max_market_age_nanos,
            wire.max_clock_skew_nanos,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// All data needed to construct immutable source metadata.
#[derive(Clone, Debug)]
pub struct SourceMetadataInput {
    schema_version: SchemaVersion,
    source_id: SourceId,
    revision_evidence: RevisionBoundPayloadEvidence,
    source_class: SourceClass,
    provider: SourceIdentifier,
    authorization: AuthorizationGrant,
    coverage: SourceCoverage,
    quality_ceiling: DataQuality,
    network: NetworkAccessPolicy,
    freshness: FreshnessPolicy,
    budget: Option<ProviderBudgetPolicy>,
    capabilities: SourceCapabilities,
    protocol: SourceProtocolProfile,
}

impl SourceMetadataInput {
    /// Collects independent source metadata fields for checked construction.
    #[allow(
        clippy::too_many_arguments,
        reason = "metadata evidence dimensions remain explicit"
    )]
    pub const fn new(
        schema_version: SchemaVersion,
        source_id: SourceId,
        revision_evidence: RevisionBoundPayloadEvidence,
        source_class: SourceClass,
        provider: SourceIdentifier,
        authorization: AuthorizationGrant,
        coverage: SourceCoverage,
        quality_ceiling: DataQuality,
        network: NetworkAccessPolicy,
        freshness: FreshnessPolicy,
        budget: Option<ProviderBudgetPolicy>,
        capabilities: SourceCapabilities,
        protocol: SourceProtocolProfile,
    ) -> Self {
        Self {
            schema_version,
            source_id,
            revision_evidence,
            source_class,
            provider,
            authorization,
            coverage,
            quality_ceiling,
            network,
            freshness,
            budget,
            capabilities,
            protocol,
        }
    }
}

/// Immutable, versioned and exact-content-bound source declaration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SourceMetadata {
    schema_version: SchemaVersion,
    source_id: SourceId,
    revision_evidence: RevisionBoundPayloadEvidence,
    source_class: SourceClass,
    provider: SourceIdentifier,
    authorization: AuthorizationGrant,
    coverage: SourceCoverage,
    quality_ceiling: DataQuality,
    network: NetworkAccessPolicy,
    freshness: FreshnessPolicy,
    budget: Option<ProviderBudgetPolicy>,
    capabilities: SourceCapabilities,
    protocol: SourceProtocolProfile,
}

impl SourceMetadata {
    /// Validates and constructs source metadata. This does not register the source or mint runtime
    /// authority.
    ///
    /// # Errors
    ///
    /// Rejects unusable capability declarations and impossible `DirectVerified` ceilings.
    pub fn try_new(input: SourceMetadataInput) -> Result<Self, SourceMetadataError> {
        input
            .schema_version
            .ensure_supported()
            .map_err(|_| SourceMetadataError::UnsupportedSchemaVersion)?;
        if !input.capabilities.live && !input.capabilities.extraction {
            return Err(SourceMetadataError::NoOperationalCapability);
        }
        if input.capabilities.live
            && (input.coverage.domain != CoverageDomain::Instruments
                || input.coverage.live.is_none()
                || input.coverage.topology.is_not_applicable())
        {
            return Err(SourceMetadataError::LiveCoverageWithoutVenue);
        }
        if !input.capabilities.live
            && (input.capabilities.sequence != SequenceCapability::Unsupported
                || input.capabilities.checksum != ChecksumCapability::Unsupported
                || input.capabilities.source_timestamps
                || input.coverage.live.is_some())
        {
            return Err(SourceMetadataError::NonLiveCapabilityConflict);
        }
        let protocol_consistent = match &input.protocol {
            SourceProtocolProfile::NotLive => !input.capabilities.live,
            SourceProtocolProfile::Live(profile) => {
                input.capabilities.live
                    && profile.sequence.capability() == input.capabilities.sequence
                    && profile.checksum.capability() == input.capabilities.checksum
                    && profile.source_timestamps == input.capabilities.source_timestamps
            }
        };
        if !protocol_consistent {
            return Err(SourceMetadataError::ProtocolCapabilityConflict);
        }
        if !input.capabilities.extraction
            && input.capabilities.historical != HistoricalCapability::None
        {
            return Err(SourceMetadataError::HistoricalWithoutExtraction);
        }
        if input.quality_ceiling == DataQuality::DirectVerified
            && (!input.capabilities.live
                || input.capabilities.sequence != SequenceCapability::Provided
                || !input.capabilities.source_timestamps
                || input.coverage.delay != CoverageDelay::RealTime
                || !matches!(
                    (input.source_class, input.coverage.delivery),
                    (SourceClass::Exchange, DeliveryEvidence::DirectVenue)
                        | (SourceClass::Broker, DeliveryEvidence::AuthorizedBroker)
                )
                || (input.source_class == SourceClass::Broker
                    && input.authorization.mode != AuthorizationMode::UserAuthorized))
        {
            return Err(SourceMetadataError::InvalidDirectVerifiedCeiling);
        }
        let local_owned = matches!(
            input.source_class,
            SourceClass::LocalFile | SourceClass::PortfolioExport
        );
        if local_owned
            && (!matches!(input.network, NetworkAccessPolicy::Denied)
                || input.budget.is_some()
                || input.authorization.mode != AuthorizationMode::UserOwnedLocal)
        {
            return Err(SourceMetadataError::InvalidLocalNetworkPolicy);
        }
        let requires_network = matches!(
            input.source_class,
            SourceClass::Exchange
                | SourceClass::Broker
                | SourceClass::OfficialAgency
                | SourceClass::RegulatoryFiling
                | SourceClass::OnChain
        );
        if requires_network
            && (!matches!(input.network, NetworkAccessPolicy::Allowlisted(_))
                || input.budget.is_none())
        {
            return Err(SourceMetadataError::MissingRemoteNetworkPolicy);
        }
        if let Some(budget) = &input.budget {
            let expected = crate::BudgetScope::for_authorization(
                input.provider.clone(),
                &input.authorization,
            )
            .map_err(|_| SourceMetadataError::BudgetAuthorizationMismatch)?;
            if budget.scope() != &expected {
                return Err(if budget.scope().as_source_identifier() != &input.provider {
                    SourceMetadataError::BudgetProviderMismatch
                } else {
                    SourceMetadataError::BudgetAuthorizationMismatch
                });
            }
        }
        Ok(Self {
            schema_version: input.schema_version,
            source_id: input.source_id,
            revision_evidence: input.revision_evidence,
            source_class: input.source_class,
            provider: input.provider,
            authorization: input.authorization,
            coverage: input.coverage,
            quality_ceiling: input.quality_ceiling,
            network: input.network,
            freshness: input.freshness,
            budget: input.budget,
            capabilities: input.capabilities,
            protocol: input.protocol,
        })
    }

    /// Returns the internal source identity.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the metadata revision atomically bound to exact payload evidence.
    pub const fn revision(&self) -> &MetadataRevision {
        self.revision_evidence.metadata_revision()
    }

    /// Returns the exact revision payload evidence, including its content hash.
    pub const fn revision_evidence(&self) -> &RevisionBoundPayloadEvidence {
        &self.revision_evidence
    }

    /// Returns the configured source class.
    pub const fn source_class(&self) -> SourceClass {
        self.source_class
    }

    /// Returns the source metadata schema version.
    pub const fn schema_version(&self) -> SchemaVersion {
        self.schema_version
    }

    /// Returns the bounded provider label retained for metadata and diagnostics.
    pub const fn provider(&self) -> &SourceIdentifier {
        &self.provider
    }

    /// Returns evidence-backed authorization metadata.
    pub const fn authorization(&self) -> &AuthorizationGrant {
        &self.authorization
    }

    /// Returns the declared maximum observation quality; this is never runtime authority.
    pub const fn quality_ceiling(&self) -> DataQuality {
        self.quality_ceiling
    }

    /// Returns coverage evidence and semantics.
    pub const fn coverage(&self) -> &SourceCoverage {
        &self.coverage
    }

    /// Returns the endpoint allowlist and request bounds.
    pub const fn network_policy(&self) -> &NetworkAccessPolicy {
        &self.network
    }

    /// Returns the shared provider budget declaration.
    pub const fn budget_policy(&self) -> Option<&ProviderBudgetPolicy> {
        self.budget.as_ref()
    }

    /// Returns independent connection and market freshness policy.
    pub const fn freshness_policy(&self) -> FreshnessPolicy {
        self.freshness
    }

    /// Returns immutable capabilities.
    pub const fn capabilities(&self) -> SourceCapabilities {
        self.capabilities
    }

    /// Returns the metadata-bound decoder and integrity-validation contract.
    pub const fn protocol_profile(&self) -> &SourceProtocolProfile {
        &self.protocol
    }

    /// Returns whether authorization and coverage are both effective at `at`.
    pub fn is_effective_at(&self, at: Timestamp) -> bool {
        self.authorization.is_effective_at(at) && self.coverage.is_effective_at(at)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceMetadataWire {
    schema_version: SchemaVersion,
    source_id: SourceId,
    revision_evidence: RevisionBoundPayloadEvidence,
    source_class: SourceClass,
    provider: SourceIdentifier,
    authorization: AuthorizationGrant,
    coverage: SourceCoverage,
    quality_ceiling: DataQuality,
    network: NetworkAccessPolicy,
    freshness: FreshnessPolicy,
    budget: Option<ProviderBudgetPolicy>,
    capabilities: SourceCapabilities,
    protocol: SourceProtocolProfile,
}

impl<'de> Deserialize<'de> for SourceMetadata {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = SourceMetadataWire::deserialize(deserializer)?;
        Self::try_new(SourceMetadataInput::new(
            wire.schema_version,
            wire.source_id,
            wire.revision_evidence,
            wire.source_class,
            wire.provider,
            wire.authorization,
            wire.coverage,
            wire.quality_ceiling,
            wire.network,
            wire.freshness,
            wire.budget,
            wire.capabilities,
            wire.protocol,
        ))
        .map_err(serde::de::Error::custom)
    }
}

/// Failure to construct or register source metadata.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum SourceMetadataError {
    /// A required bounded collection was empty.
    #[error("{field} must not be empty")]
    EmptyCollection {
        /// Stable field name.
        field: &'static str,
    },
    /// A bounded collection exceeded its capacity.
    #[error("{field} exceeds maximum capacity {max}")]
    CollectionTooLarge {
        /// Stable field name.
        field: &'static str,
        /// Maximum accepted count.
        max: usize,
    },
    /// A set-like collection contained a duplicate.
    #[error("{field} contains a duplicate value")]
    DuplicateValue {
        /// Stable field name.
        field: &'static str,
    },
    /// Delayed data declared a zero delay.
    #[error("delayed coverage must declare a positive delay")]
    ZeroDelay,
    /// Market depth was declared without a book event.
    #[error("market depth requires book snapshot or delta coverage")]
    DepthWithoutBookCoverage,
    /// A wire value violated topology cardinality.
    #[error("invalid coverage topology")]
    InvalidCoverageTopology,
    /// A wire value violated instrument-coverage cardinality.
    #[error("invalid instrument coverage")]
    InvalidInstrumentCoverage,
    /// Neither live nor extraction behavior was declared.
    #[error("source must declare at least one operational capability")]
    NoOperationalCapability,
    /// A live source omitted a venue scope.
    #[error("live source coverage requires an explicit venue scope")]
    LiveCoverageWithoutVenue,
    /// DirectVerified was declared where direct execution-quality evidence is impossible.
    #[error("DirectVerified ceiling requires direct exchange/broker live delivery")]
    InvalidDirectVerifiedCeiling,
    /// A non-live source advertised live protocol or event behavior.
    #[error("non-live source cannot advertise live sequence/checksum/event/depth capabilities")]
    NonLiveCapabilityConflict,
    /// Capability flags and the authoritative protocol profile disagreed.
    #[error("source capabilities conflict with the protocol validation profile")]
    ProtocolCapabilityConflict,
    /// Historical behavior was declared without an extraction capability.
    #[error("historical capability requires extraction support")]
    HistoricalWithoutExtraction,
    /// A connection or market freshness threshold was zero.
    #[error("connection and market freshness thresholds must be positive")]
    InvalidFreshnessPolicy,
    /// Metadata schema version is not supported by this release.
    #[error("source metadata schema version is unsupported")]
    UnsupportedSchemaVersion,
    /// Live coverage rule contradicted event/depth/snapshot semantics.
    #[error("invalid live event/depth/snapshot coverage rule")]
    InvalidLiveCoverageRule,
    /// Instrument and non-instrument coverage fields were mixed.
    #[error("coverage domain fields are inconsistent")]
    InvalidCoverageDomain,
    /// A local user-owned source attempted to configure network/budget access.
    #[error("local user-owned sources require denied network access and no provider budget")]
    InvalidLocalNetworkPolicy,
    /// A remote source omitted its allowlist or shared provider budget.
    #[error("remote source requires allowlisted network access and a shared provider budget")]
    MissingRemoteNetworkPolicy,
    /// Shared budget scope names a different provider.
    #[error("provider budget scope does not match source provider")]
    BudgetProviderMismatch,
    /// Shared budget account qualification conflicts with authorization mode or evidence basis.
    #[error("provider budget account scope does not match authorization mode and basis")]
    BudgetAuthorizationMismatch,
}

fn interval_contains(interval: EffectiveInterval, at: Timestamp) -> bool {
    at >= interval.starts_at() && interval.ends_at().is_none_or(|end| at < end)
}

fn bounded<T, const MAX: usize>(
    field: &'static str,
    values: Vec<T>,
) -> Result<BoundedVec<T, MAX>, SourceMetadataError> {
    BoundedVec::try_new(values).map_err(|error| SourceMetadataError::CollectionTooLarge {
        field,
        max: error.max,
    })
}

fn reject_duplicates<T>(field: &'static str, values: &[T]) -> Result<(), SourceMetadataError>
where
    T: PartialEq,
{
    if contains_duplicates(values) {
        Err(SourceMetadataError::DuplicateValue { field })
    } else {
        Ok(())
    }
}

fn contains_duplicates<T>(values: &[T]) -> bool
where
    T: PartialEq,
{
    values
        .iter()
        .enumerate()
        .any(|(index, value)| values[index.saturating_add(1)..].contains(value))
}
