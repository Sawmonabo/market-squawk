/// Research/live coverage domain without fabricating instrument asset classes for macro data.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CoverageDomain {
    /// Instrument and venue scoped observations.
    Instruments,
    /// Macroeconomic series and vintages.
    Macroeconomic,
    /// Regulatory filings, XBRL facts, and company submissions.
    RegulatoryFilings,
    /// Portfolio holdings, transactions, and account exports.
    Portfolio,
    /// Corporate action reference datasets.
    CorporateActions,
    /// User-owned or licensed alternative datasets.
    AlternativeData,
}

/// One exact live event/depth snapshot-applicability rule.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LiveCoverageRule {
    event_class: LiveEventClass,
    depth: Option<MarketDepth>,
    snapshot_applicability: SnapshotApplicability,
}

impl LiveCoverageRule {
    /// Constructs a relationally valid event/depth/snapshot rule.
    ///
    /// # Errors
    ///
    /// Book classes require depth and snapshot initialization; non-book classes require no depth
    /// and explicit metadata-backed non-applicability.
    pub fn try_new(
        event_class: LiveEventClass,
        depth: Option<MarketDepth>,
        snapshot_applicability: SnapshotApplicability,
    ) -> Result<Self, SourceMetadataError> {
        let valid = if event_class.requires_book_state() {
            depth.is_some() && matches!(snapshot_applicability, SnapshotApplicability::Required)
        } else {
            depth.is_none()
                && matches!(
                    snapshot_applicability,
                    SnapshotApplicability::NotApplicable { .. }
                )
        };
        if !valid {
            return Err(SourceMetadataError::InvalidLiveCoverageRule);
        }
        Ok(Self {
            event_class,
            depth,
            snapshot_applicability,
        })
    }

    /// Returns the live event class.
    pub const fn event_class(&self) -> LiveEventClass {
        self.event_class
    }

    /// Returns market depth when the event is book-scoped.
    pub const fn depth(&self) -> Option<MarketDepth> {
        self.depth
    }

    /// Returns the metadata-backed snapshot rule.
    pub const fn snapshot_applicability(&self) -> &SnapshotApplicability {
        &self.snapshot_applicability
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LiveCoverageRuleWire {
    event_class: LiveEventClass,
    depth: Option<MarketDepth>,
    snapshot_applicability: SnapshotApplicability,
}

impl<'de> Deserialize<'de> for LiveCoverageRule {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = LiveCoverageRuleWire::deserialize(deserializer)?;
        Self::try_new(wire.event_class, wire.depth, wire.snapshot_applicability)
            .map_err(serde::de::Error::custom)
    }
}

/// Provider product/channel and bounded per-event live coverage rules.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LiveCoverageDeclaration {
    provider_product: ProviderProduct,
    provider_channel: ProviderChannel,
    rules: BoundedVec<LiveCoverageRule, MAX_LIVE_COVERAGE_RULES>,
}

impl LiveCoverageDeclaration {
    /// Constructs nonempty, duplicate-free live coverage rules.
    ///
    /// # Errors
    ///
    /// Rejects empty, duplicate event/depth keys, or excessive rules.
    pub fn try_new(
        provider_product: ProviderProduct,
        provider_channel: ProviderChannel,
        rules: Vec<LiveCoverageRule>,
    ) -> Result<Self, SourceMetadataError> {
        if rules.is_empty() {
            return Err(SourceMetadataError::EmptyCollection {
                field: "live_coverage_rules",
            });
        }
        if rules.iter().enumerate().any(|(index, rule)| {
            rules[index.saturating_add(1)..]
                .iter()
                .any(|other| rule.event_class == other.event_class && rule.depth == other.depth)
        }) {
            return Err(SourceMetadataError::DuplicateValue {
                field: "live_coverage_rules",
            });
        }
        Ok(Self {
            provider_product,
            provider_channel,
            rules: bounded("live_coverage_rules", rules)?,
        })
    }

    /// Returns the exact provider product.
    pub const fn provider_product(&self) -> &ProviderProduct {
        &self.provider_product
    }

    /// Returns the exact provider channel.
    pub const fn provider_channel(&self) -> &ProviderChannel {
        &self.provider_channel
    }

    /// Returns bounded event/depth/snapshot rules.
    pub fn rules(&self) -> &[LiveCoverageRule] {
        self.rules.as_slice()
    }

    /// Returns the exact event/depth rule when declared.
    pub fn rule_for(
        &self,
        event_class: LiveEventClass,
        depth: Option<MarketDepth>,
    ) -> Option<&LiveCoverageRule> {
        self.rules
            .as_slice()
            .iter()
            .find(|rule| rule.event_class == event_class && rule.depth == depth)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LiveCoverageDeclarationWire {
    provider_product: ProviderProduct,
    provider_channel: ProviderChannel,
    rules: BoundedVec<LiveCoverageRule, MAX_LIVE_COVERAGE_RULES>,
}

impl<'de> Deserialize<'de> for LiveCoverageDeclaration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = LiveCoverageDeclarationWire::deserialize(deserializer)?;
        Self::try_new(
            wire.provider_product,
            wire.provider_channel,
            wire.rules.as_slice().to_vec(),
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Declared instrument-universe coverage with an intrinsically bounded enumerated form.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InstrumentCoverage {
    kind: InstrumentCoverageKind,
    instruments: BoundedVec<InstrumentId, MAX_INSTRUMENTS>,
}

impl InstrumentCoverage {
    /// Declares complete coverage of the provider product's evidenced universe.
    pub fn all_declared() -> Self {
        Self::without_instruments(InstrumentCoverageKind::AllDeclared)
    }

    /// Declares incomplete coverage without claiming a complete enumerated list.
    pub fn partial() -> Self {
        Self::without_instruments(InstrumentCoverageKind::Partial)
    }

    /// Declares a bounded exact set of covered internal instruments.
    ///
    /// # Errors
    ///
    /// Rejects an empty, duplicate, or oversized set.
    pub fn enumerated(instruments: Vec<InstrumentId>) -> Result<Self, SourceMetadataError> {
        if instruments.is_empty() {
            return Err(SourceMetadataError::EmptyCollection {
                field: "instruments",
            });
        }
        if contains_duplicates(&instruments) {
            return Err(SourceMetadataError::DuplicateValue {
                field: "instruments",
            });
        }
        let instruments = BoundedVec::try_new(instruments).map_err(|error| {
            SourceMetadataError::CollectionTooLarge {
                field: "instruments",
                max: error.max,
            }
        })?;
        Ok(Self {
            kind: InstrumentCoverageKind::Enumerated,
            instruments,
        })
    }

    fn without_instruments(kind: InstrumentCoverageKind) -> Self {
        Self {
            kind,
            instruments: BoundedVec::empty(),
        }
    }

    /// Returns an exact list when coverage is enumerated.
    pub fn instruments(&self) -> &[InstrumentId] {
        self.instruments.as_slice()
    }

    /// Assesses one instrument without turning partial coverage into positive authority.
    pub fn membership(&self, instrument: InstrumentId) -> InstrumentCoverageMembership {
        match self.kind {
            InstrumentCoverageKind::AllDeclared => {
                InstrumentCoverageMembership::EvidenceBackedUniverse
            }
            InstrumentCoverageKind::Partial => InstrumentCoverageMembership::PartialUnproven,
            InstrumentCoverageKind::Enumerated
                if self.instruments.as_slice().contains(&instrument) =>
            {
                InstrumentCoverageMembership::Enumerated
            }
            InstrumentCoverageKind::Enumerated => InstrumentCoverageMembership::Outside,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InstrumentCoverageWire {
    kind: InstrumentCoverageKind,
    instruments: BoundedVec<InstrumentId, MAX_INSTRUMENTS>,
}

impl<'de> Deserialize<'de> for InstrumentCoverage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = InstrumentCoverageWire::deserialize(deserializer)?;
        let valid = match wire.kind {
            InstrumentCoverageKind::AllDeclared | InstrumentCoverageKind::Partial => {
                wire.instruments.is_empty()
            }
            InstrumentCoverageKind::Enumerated => {
                !wire.instruments.is_empty() && !contains_duplicates(wire.instruments.as_slice())
            }
        };
        if !valid {
            return Err(serde::de::Error::custom(
                SourceMetadataError::InvalidInstrumentCoverage,
            ));
        }
        Ok(Self {
            kind: wire.kind,
            instruments: wire.instruments,
        })
    }
}

/// Evidence-backed declared coverage for one provider product.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SourceCoverage {
    evidence: ExactPayloadEvidence,
    effective: EffectiveInterval,
    domain: CoverageDomain,
    asset_classes: BoundedVec<AssetClass, MAX_ASSET_CLASSES>,
    topology: CoverageTopology,
    instruments: InstrumentCoverage,
    live: Option<LiveCoverageDeclaration>,
    delay: CoverageDelay,
    delivery: DeliveryEvidence,
}

impl SourceCoverage {
    /// Constructs checked coverage without elevating it into runtime execution authority.
    ///
    /// # Errors
    ///
    /// Rejects empty asset coverage, duplicates, oversized sets, a zero delayed-data duration, or
    /// market depth without a book event.
    #[allow(
        clippy::too_many_arguments,
        reason = "coverage dimensions are independent evidence"
    )]
    pub fn try_instrument(
        evidence: ExactPayloadEvidence,
        effective: EffectiveInterval,
        asset_classes: Vec<AssetClass>,
        topology: CoverageTopology,
        instruments: InstrumentCoverage,
        live: Option<LiveCoverageDeclaration>,
        delay: CoverageDelay,
        delivery: DeliveryEvidence,
    ) -> Result<Self, SourceMetadataError> {
        if asset_classes.is_empty() {
            return Err(SourceMetadataError::EmptyCollection {
                field: "asset_classes",
            });
        }
        reject_duplicates("asset_classes", &asset_classes)?;
        if matches!(delay, CoverageDelay::Delayed(0)) {
            return Err(SourceMetadataError::ZeroDelay);
        }
        Ok(Self {
            evidence,
            effective,
            domain: CoverageDomain::Instruments,
            asset_classes: bounded("asset_classes", asset_classes)?,
            topology,
            instruments,
            live,
            delay,
            delivery,
        })
    }

    /// Constructs truthful non-instrument extraction coverage.
    ///
    /// # Errors
    ///
    /// Rejects the instrument domain and a zero delayed-data duration.
    pub fn try_non_instrument(
        evidence: ExactPayloadEvidence,
        effective: EffectiveInterval,
        domain: CoverageDomain,
        delay: CoverageDelay,
        delivery: DeliveryEvidence,
    ) -> Result<Self, SourceMetadataError> {
        if domain == CoverageDomain::Instruments {
            return Err(SourceMetadataError::InvalidCoverageDomain);
        }
        if matches!(delay, CoverageDelay::Delayed(0)) {
            return Err(SourceMetadataError::ZeroDelay);
        }
        Ok(Self {
            evidence,
            effective,
            domain,
            asset_classes: BoundedVec::empty(),
            topology: CoverageTopology::not_applicable(),
            instruments: InstrumentCoverage::partial(),
            live: None,
            delay,
            delivery,
        })
    }

    /// Returns whether coverage is effective at `at`.
    pub fn is_effective_at(&self, at: Timestamp) -> bool {
        interval_contains(self.effective, at)
    }

    /// Returns the exact coverage evidence.
    pub const fn evidence(&self) -> &ExactPayloadEvidence {
        &self.evidence
    }

    /// Returns the venue topology.
    pub const fn topology(&self) -> &CoverageTopology {
        &self.topology
    }

    /// Returns declared delivery delay semantics.
    pub const fn delay(&self) -> CoverageDelay {
        self.delay
    }

    /// Returns the independently declared delivery relationship.
    pub const fn delivery(&self) -> DeliveryEvidence {
        self.delivery
    }

    /// Returns supported live event classes.
    pub const fn live(&self) -> Option<&LiveCoverageDeclaration> {
        self.live.as_ref()
    }

    /// Returns explicitly covered asset classes.
    pub fn asset_classes(&self) -> &[AssetClass] {
        self.asset_classes.as_slice()
    }

    /// Returns supported market depths independently of topology.
    pub const fn domain(&self) -> CoverageDomain {
        self.domain
    }

    /// Returns declared instrument-universe coverage.
    pub const fn instruments(&self) -> &InstrumentCoverage {
        &self.instruments
    }

    /// Returns the coverage effective interval.
    pub const fn effective_interval(&self) -> EffectiveInterval {
        self.effective
    }

    /// Converts the half-open metadata interval end to the inclusive deadline required by the
    /// domain live `CoverageScope` contract.
    ///
    /// Open-ended coverage remains `None`. Checked interval construction guarantees a finite end
    /// has a representable predecessor nanosecond.
    pub fn inclusive_coverage_deadline(&self) -> Option<Timestamp> {
        self.effective
            .ends_at()
            .and_then(|end| end.checked_sub_nanos(1).ok())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceCoverageWire {
    evidence: ExactPayloadEvidence,
    effective: EffectiveInterval,
    domain: CoverageDomain,
    asset_classes: BoundedVec<AssetClass, MAX_ASSET_CLASSES>,
    topology: CoverageTopology,
    instruments: InstrumentCoverage,
    live: Option<LiveCoverageDeclaration>,
    delay: CoverageDelay,
    delivery: DeliveryEvidence,
}

impl<'de> Deserialize<'de> for SourceCoverage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = SourceCoverageWire::deserialize(deserializer)?;
        let result = if wire.domain == CoverageDomain::Instruments {
            Self::try_instrument(
                wire.evidence,
                wire.effective,
                wire.asset_classes.as_slice().to_vec(),
                wire.topology,
                wire.instruments,
                wire.live,
                wire.delay,
                wire.delivery,
            )
        } else {
            if !wire.asset_classes.is_empty()
                || !wire.topology.is_not_applicable()
                || !wire.instruments.instruments().is_empty()
                || wire.live.is_some()
            {
                return Err(serde::de::Error::custom(
                    SourceMetadataError::InvalidCoverageDomain,
                ));
            }
            Self::try_non_instrument(
                wire.evidence,
                wire.effective,
                wire.domain,
                wire.delay,
                wire.delivery,
            )
        };
        result.map_err(serde::de::Error::custom)
    }
}
