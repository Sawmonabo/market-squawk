// A source may carry several exact channels in one original transport frame. This is a
// declaration bound, never a wildcard or permission to pick an arbitrary channel.
const MAX_LIVE_COVERAGE_CHANNELS: usize = 32;
const MAX_LIVE_SOURCE_COHORTS: usize = 32;

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
    /// Source-native market calendar ranges and session hours.
    MarketCalendar,
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
    source_cohorts: BoundedVec<SourceIdentifier, MAX_LIVE_SOURCE_COHORTS>,
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
        if event_class == LiveEventClass::Screener {
            return Err(SourceMetadataError::InvalidLiveCoverageRule);
        }
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
            source_cohorts: BoundedVec::empty(),
        })
    }

    /// Declares bounded exact source cohorts for ranked source observations only.
    ///
    /// # Errors
    /// Rejects missing/duplicate cohorts, excessive keys, or a snapshot requirement.
    pub fn try_source_cohorts(
        mut cohorts: Vec<SourceIdentifier>,
        snapshot_applicability: SnapshotApplicability,
    ) -> Result<Self, SourceMetadataError> {
        if cohorts.is_empty() {
            return Err(SourceMetadataError::EmptyCollection {
                field: "live_source_cohorts",
            });
        }
        if !matches!(
            snapshot_applicability,
            SnapshotApplicability::NotApplicable { .. }
        ) {
            return Err(SourceMetadataError::InvalidLiveCoverageRule);
        }
        if cohorts.len() > MAX_LIVE_SOURCE_COHORTS {
            return Err(SourceMetadataError::CollectionTooLarge {
                field: "live_source_cohorts",
                max: MAX_LIVE_SOURCE_COHORTS,
            });
        }
        reject_duplicates("live_source_cohorts", &cohorts)?;
        cohorts.sort();
        Ok(Self {
            event_class: LiveEventClass::Screener,
            depth: None,
            snapshot_applicability,
            source_cohorts: bounded("live_source_cohorts", cohorts)?,
        })
    }

    /// Returns explicit cohort keys; ordinary instrument rules always have none.
    pub fn source_cohorts(&self) -> &[SourceIdentifier] {
        self.source_cohorts.as_slice()
    }

    /// Requires exact cohort membership under the selected product/channel's Screener rule.
    pub fn permits_source_cohort(&self, cohort: &SourceIdentifier) -> bool {
        self.event_class == LiveEventClass::Screener
            && self.source_cohorts.as_slice().contains(cohort)
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

    pub(crate) fn dynamic_retained_bytes(&self) -> Option<usize> {
        let Self {
            event_class: _,
            depth: _,
            snapshot_applicability,
            source_cohorts,
        } = self;
        let mut bytes = snapshot_applicability
            .dynamic_retained_bytes()?
            .checked_add(source_cohorts.checked_allocation_bytes()?)?;
        for cohort in source_cohorts.as_slice() {
            bytes = bytes.checked_add(cohort.retained_bytes())?;
        }
        Some(bytes)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LiveCoverageRuleWire {
    event_class: LiveEventClass,
    depth: Option<MarketDepth>,
    snapshot_applicability: SnapshotApplicability,
    source_cohorts: BoundedVec<SourceIdentifier, MAX_LIVE_SOURCE_COHORTS>,
}

impl<'de> Deserialize<'de> for LiveCoverageRule {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = LiveCoverageRuleWire::deserialize(deserializer)?;
        let result = if wire.event_class == LiveEventClass::Screener {
            if wire.depth.is_some() {
                return Err(serde::de::Error::custom(
                    SourceMetadataError::InvalidLiveCoverageRule,
                ));
            }
            Self::try_source_cohorts(wire.source_cohorts.into_vec(), wire.snapshot_applicability)
        } else {
            if !wire.source_cohorts.is_empty() {
                return Err(serde::de::Error::custom(
                    SourceMetadataError::InvalidLiveCoverageRule,
                ));
            }
            Self::try_new(wire.event_class, wire.depth, wire.snapshot_applicability)
        };
        result.map_err(serde::de::Error::custom)
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
    live: BoundedVec<LiveCoverageDeclaration, MAX_LIVE_COVERAGE_CHANNELS>,
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
        Self::try_instrument_channels(
            evidence,
            effective,
            asset_classes,
            topology,
            instruments,
            live.into_iter().collect(),
            delay,
            delivery,
        )
    }

    /// Constructs exact product/channel declarations for one shared coverage scope.
    ///
    /// Every channel retains its own event/depth rules. Channels must share the declared
    /// instrument universe, venue topology and delay; different semantics require separate sources.
    /// Empty channels remain valid for instrument reference extraction only.
    ///
    /// # Errors
    /// Rejects duplicate product/channel keys, more than 32 channels or 32 total rules, or invalid
    /// instrument/delay scope. Sorting makes metadata identity independent of declaration order.
    #[allow(
        clippy::too_many_arguments,
        reason = "coverage dimensions are independent evidence"
    )]
    pub fn try_instrument_channels(
        evidence: ExactPayloadEvidence,
        effective: EffectiveInterval,
        asset_classes: Vec<AssetClass>,
        topology: CoverageTopology,
        instruments: InstrumentCoverage,
        mut live: Vec<LiveCoverageDeclaration>,
        delay: CoverageDelay,
        delivery: DeliveryEvidence,
    ) -> Result<Self, SourceMetadataError> {
        if live.len() > MAX_LIVE_COVERAGE_CHANNELS {
            return Err(SourceMetadataError::CollectionTooLarge {
                field: "live_coverage_channels",
                max: MAX_LIVE_COVERAGE_CHANNELS,
            });
        }
        let rule_count = live.iter().try_fold(0usize, |total, channel| {
            total.checked_add(channel.rules().len())
        });
        if rule_count.is_none_or(|count| count > MAX_LIVE_COVERAGE_RULES) {
            return Err(SourceMetadataError::CollectionTooLarge {
                field: "live_coverage_rules",
                max: MAX_LIVE_COVERAGE_RULES,
            });
        }
        live.sort_by(|left, right| {
            (
                left.provider_product().as_source_identifier().as_str(),
                left.provider_channel().as_source_identifier().as_str(),
            )
                .cmp(&(
                    right.provider_product().as_source_identifier().as_str(),
                    right.provider_channel().as_source_identifier().as_str(),
                ))
        });
        if live.windows(2).any(|pair| {
            pair[0].provider_product() == pair[1].provider_product()
                && pair[0].provider_channel() == pair[1].provider_channel()
        }) {
            return Err(SourceMetadataError::DuplicateValue {
                field: "live_coverage_channels",
            });
        }
        if asset_classes.is_empty() {
            return Err(SourceMetadataError::EmptyCollection {
                field: "asset_classes",
            });
        }
        reject_duplicates("asset_classes", &asset_classes)?;
        // Instrument reference metadata has no quote-delivery latency. A live instrument
        // declaration requires market timing semantics; Unknown is not NotApplicable.
        if matches!(delay, CoverageDelay::Delayed(0))
            || (delay == CoverageDelay::NotApplicable && !live.is_empty())
        {
            return Err(SourceMetadataError::ZeroDelay);
        }
        Ok(Self {
            evidence,
            effective,
            domain: CoverageDomain::Instruments,
            asset_classes: bounded("asset_classes", asset_classes)?,
            topology,
            instruments,
            live: bounded("live_coverage_channels", live)?,
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
            live: BoundedVec::empty(),
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

    /// Returns a channel only when this source declares exactly one.
    /// Mixed sources deliberately cannot satisfy callers that omit a product/channel key.
    pub fn live(&self) -> Option<&LiveCoverageDeclaration> {
        match self.live.as_slice() {
            [only] => Some(only),
            _ => None,
        }
    }

    /// Returns the bounded, sorted exact declarations without merging channel rules.
    pub fn live_channels(&self) -> &[LiveCoverageDeclaration] {
        self.live.as_slice()
    }

    /// Selects only the explicitly declared product/channel pair; there is no fallback.
    pub fn live_for(
        &self,
        product: &ProviderProduct,
        channel: &ProviderChannel,
    ) -> Option<&LiveCoverageDeclaration> {
        self.live.as_slice().iter().find(|value| {
            value.provider_product() == product && value.provider_channel() == channel
        })
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
    live: BoundedVec<LiveCoverageDeclaration, MAX_LIVE_COVERAGE_CHANNELS>,
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
            Self::try_instrument_channels(
                wire.evidence,
                wire.effective,
                wire.asset_classes.as_slice().to_vec(),
                wire.topology,
                wire.instruments,
                wire.live.into_vec(),
                wire.delay,
                wire.delivery,
            )
        } else {
            if !wire.asset_classes.is_empty()
                || !wire.topology.is_not_applicable()
                || !wire.instruments.instruments().is_empty()
                || !wire.live.is_empty()
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

#[cfg(test)]
mod cohort_authority_test {
    use super::*;
    use market_squawk_domain::RuleVersion;

    // New source-cohort authority must not deserialize as an ordinary instrument rule.
    #[test]
    fn cohort_authority_cannot_be_transplanted_into_instrument_coverage()
    -> Result<(), Box<dyn std::error::Error>> {
        let snapshot = SnapshotApplicability::NotApplicable {
            metadata_rule: IntegrityRule::new(
                SourceIdentifier::try_from("cohort-no-book-state")?,
                RuleVersion::new(1)?,
            ),
        };
        let key = SourceIdentifier::try_from("EQUITY_ALL_VOLUME_0")?;
        let rule = LiveCoverageRule::try_source_cohorts(vec![key], snapshot)?;
        assert!(!rule.permits_source_cohort(&SourceIdentifier::try_from("OPTION_ALL_VOLUME_0")?));
        let mut wire = serde_json::to_value(&rule)?;
        wire["event_class"] = serde_json::json!("quote");
        assert!(serde_json::from_value::<LiveCoverageRule>(wire).is_err());
        Ok(())
    }
}
