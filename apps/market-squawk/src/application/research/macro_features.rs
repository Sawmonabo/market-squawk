//! Exact provider-neutral Macro feature-vector construction.

use market_squawk_data::{
    ComponentAdjustmentEvidence, ComponentKind, ComponentScope, ComponentSelector, ComponentValue,
    CorporateActionSensitivity, DatasetManifestRef, FeatureLabelComponentInput,
    FeatureLabelComponentSpec, ObservationFamilyKey,
};
use market_squawk_domain::{
    CalendarDate, DigestAlgorithm, EvidenceDigest, ResearchTemporalCoordinate, SourceIdentifier,
    Timestamp, feature_dataset_macro_components_v1,
};
use market_squawk_services::ServiceError;
use rust_decimal::Decimal;
use sha2::{Digest as _, Sha256};

use super::macro_context::{MacroContextReadCapability, MacroContextSnapshot};

const THREE_MONTH_YIELD_INDICATOR: &str = "us-government-yield-3m";
const TWO_YEAR_YIELD_INDICATOR: &str = "us-government-yield-2y";
const TEN_YEAR_YIELD_INDICATOR: &str = "us-government-yield-10y";
const THIRTY_YEAR_YIELD_INDICATOR: &str = "us-government-yield-30y";
const RATE_UNIT: &str = "percent_per_year";

/// Reads and maps one exact provider-neutral Macro feature vector at caller-supplied cutoffs.
///
/// The application supplies both time coordinates. This seam never derives a civil date from an
/// instant and never substitutes wall time for a research cutoff.
pub(crate) async fn read_macro_feature_vector(
    read: &MacroContextReadCapability,
    knowledge_cutoff: Timestamp,
    effective_date_cutoff: CalendarDate,
    deadline: std::time::Instant,
    cancellation: tokio_util::sync::CancellationToken,
) -> Result<MacroFeatureVector, ServiceError> {
    let snapshot = read
        .read_latest_known(
            knowledge_cutoff,
            effective_date_cutoff,
            deadline,
            cancellation,
        )
        .await?;
    MacroFeatureVector::try_from_snapshot(&snapshot)
}

/// Reads the exact curve and valuation-rate context available independently of model completeness.
///
/// The strict V1 forecast vector also requires labor-market and every curve component. This leaf
/// intentionally has a narrower economic contract: it can retain exact curve/regime and
/// valuation-assumption evidence while a model or recommendation honestly abstains because a
/// separate required input is missing.
#[allow(
    dead_code,
    reason = "the shared valuation and decision composition seam consumes this leaf"
)]
pub(crate) async fn read_macro_investment_context(
    read: &MacroContextReadCapability,
    knowledge_cutoff: Timestamp,
    effective_date_cutoff: CalendarDate,
    deadline: std::time::Instant,
    cancellation: tokio_util::sync::CancellationToken,
) -> Result<MacroInvestmentContext, ServiceError> {
    let snapshot = read
        .read_latest_known(
            knowledge_cutoff,
            effective_date_cutoff,
            deadline,
            cancellation,
        )
        .await?;
    MacroInvestmentContext::try_from_snapshot(&snapshot)
}

/// Exact sign-based yield-curve regime derived without an estimated threshold.
///
/// This classification is supporting research evidence. It is not model confidence, a valuation,
/// a recommendation, or execution authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MacroRateRegime {
    UpwardSloping,
    Flat,
    Inverted,
    Mixed,
}

impl MacroRateRegime {
    const fn digest_tag(self) -> u8 {
        match self {
            Self::UpwardSloping => 1,
            Self::Flat => 2,
            Self::Inverted => 3,
            Self::Mixed => 4,
        }
    }
}

/// Provider-neutral, point-in-time yield-curve evidence for one exact effective date.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MacroRateRegimeEvidence {
    effective: ResearchTemporalCoordinate,
    three_month_to_ten_year_spread: Decimal,
    two_year_to_ten_year_spread: Decimal,
    regime: MacroRateRegime,
    evidence_digest: EvidenceDigest,
}

impl MacroRateRegimeEvidence {
    pub(crate) const fn effective(&self) -> &ResearchTemporalCoordinate {
        &self.effective
    }

    pub(crate) const fn three_month_to_ten_year_spread(&self) -> Decimal {
        self.three_month_to_ten_year_spread
    }

    pub(crate) const fn two_year_to_ten_year_spread(&self) -> Decimal {
        self.two_year_to_ten_year_spread
    }

    pub(crate) const fn regime(&self) -> MacroRateRegime {
        self.regime
    }

    pub(crate) const fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest
    }
}

/// Exact government-yield references available to a method-specific valuation producer.
///
/// Constant-maturity par yields are retained as assumption context. This type deliberately does
/// not relabel either value as a zero-coupon discount curve or calculate fair value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MacroValuationRateEvidence {
    effective: ResearchTemporalCoordinate,
    ten_year_government_yield: Decimal,
    thirty_year_government_yield: Decimal,
    unit: SourceIdentifier,
    evidence_digest: EvidenceDigest,
}

impl MacroValuationRateEvidence {
    pub(crate) const fn effective(&self) -> &ResearchTemporalCoordinate {
        &self.effective
    }

    pub(crate) const fn ten_year_government_yield(&self) -> Decimal {
        self.ten_year_government_yield
    }

    pub(crate) const fn thirty_year_government_yield(&self) -> Decimal {
        self.thirty_year_government_yield
    }

    pub(crate) const fn unit(&self) -> &SourceIdentifier {
        &self.unit
    }

    pub(crate) const fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest
    }
}

/// Provider-neutral curve/regime and valuation-rate context over one immutable Macro selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MacroInvestmentContext {
    regime: MacroRateRegimeEvidence,
    valuation_rates: MacroValuationRateEvidence,
    knowledge_cutoff: Timestamp,
    effective_date_cutoff: CalendarDate,
    parent_manifests: Box<[DatasetManifestRef]>,
    evidence_digest: EvidenceDigest,
}

impl MacroInvestmentContext {
    fn try_from_snapshot(snapshot: &MacroContextSnapshot) -> Result<Self, ServiceError> {
        let source_evidence = snapshot.evidence();
        require_sha256(source_evidence.consumed_digest())?;
        if source_evidence.consumed_parent_manifests().is_empty() {
            return Err(ServiceError::InvalidResult);
        }

        let three_month = observed_indicator(snapshot, THREE_MONTH_YIELD_INDICATOR)?;
        let two_year = observed_indicator(snapshot, TWO_YEAR_YIELD_INDICATOR)?;
        let ten_year = observed_indicator(snapshot, TEN_YEAR_YIELD_INDICATOR)?;
        let thirty_year = observed_indicator(snapshot, THIRTY_YEAR_YIELD_INDICATOR)?;
        let effective = three_month.context().time().effective().clone();
        if two_year.context().time().effective() != &effective
            || ten_year.context().time().effective() != &effective
            || thirty_year.context().time().effective() != &effective
        {
            return Err(ServiceError::Unavailable);
        }

        let three_month_value = observed_value(three_month)?;
        let two_year_value = observed_value(two_year)?;
        let ten_year_value = observed_value(ten_year)?;
        let thirty_year_value = observed_value(thirty_year)?;
        let three_month_to_ten_year_spread = ten_year_value
            .checked_sub(three_month_value)
            .ok_or(ServiceError::InvalidResult)?
            .normalize();
        let two_year_to_ten_year_spread = ten_year_value
            .checked_sub(two_year_value)
            .ok_or(ServiceError::InvalidResult)?
            .normalize();
        let regime =
            classify_rate_regime(three_month_to_ten_year_spread, two_year_to_ten_year_spread);
        let unit =
            SourceIdentifier::try_from(RATE_UNIT).map_err(|_| ServiceError::InvalidResult)?;

        let regime_digest = derived_evidence_digest(
            b"market-squawk/macro-rate-regime-evidence/v1\0",
            source_evidence,
            &effective,
            &[
                three_month_value,
                two_year_value,
                ten_year_value,
                three_month_to_ten_year_spread,
                two_year_to_ten_year_spread,
            ],
            &[regime.digest_tag()],
        )?;
        let valuation_digest = derived_evidence_digest(
            b"market-squawk/macro-valuation-rate-evidence/v1\0",
            source_evidence,
            &effective,
            &[ten_year_value, thirty_year_value],
            RATE_UNIT.as_bytes(),
        )?;
        let regime = MacroRateRegimeEvidence {
            effective: effective.clone(),
            three_month_to_ten_year_spread,
            two_year_to_ten_year_spread,
            regime,
            evidence_digest: regime_digest,
        };
        let valuation_rates = MacroValuationRateEvidence {
            effective,
            ten_year_government_yield: ten_year_value,
            thirty_year_government_yield: thirty_year_value,
            unit,
            evidence_digest: valuation_digest,
        };
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/macro-investment-context/v1\0");
        digest.update(source_evidence.consumed_digest().bytes());
        digest.update(regime.evidence_digest().bytes());
        digest.update(valuation_rates.evidence_digest().bytes());
        hash_parent_manifests(&mut digest, source_evidence.consumed_parent_manifests())?;
        let evidence_digest = require_sha256(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            digest.finalize().into(),
        ))?;

        Ok(Self {
            regime,
            valuation_rates,
            knowledge_cutoff: source_evidence.knowledge_cutoff(),
            effective_date_cutoff: source_evidence.effective_date_cutoff(),
            parent_manifests: source_evidence
                .consumed_parent_manifests()
                .to_vec()
                .into_boxed_slice(),
            evidence_digest,
        })
    }

    pub(crate) const fn regime(&self) -> &MacroRateRegimeEvidence {
        &self.regime
    }

    pub(crate) const fn valuation_rates(&self) -> &MacroValuationRateEvidence {
        &self.valuation_rates
    }

    pub(crate) const fn knowledge_cutoff(&self) -> Timestamp {
        self.knowledge_cutoff
    }

    pub(crate) const fn effective_date_cutoff(&self) -> CalendarDate {
        self.effective_date_cutoff
    }

    pub(crate) fn parent_manifests(&self) -> &[DatasetManifestRef] {
        &self.parent_manifests
    }

    pub(crate) const fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest
    }
}

/// One exact V1 Macro vector derived from a single neutral point-in-time snapshot.
///
/// Provider identity remains inside the component selectors and opaque evidence. Consumers use
/// only the code-owned economic order, values, units, cutoffs, and immutable parent set.
#[derive(Clone, Debug)]
pub(crate) struct MacroFeatureVector {
    components: Box<[FeatureLabelComponentInput]>,
    investment_context: Option<MacroInvestmentContext>,
    knowledge_cutoff: Timestamp,
    effective_date_cutoff: CalendarDate,
    parent_manifests: Box<[DatasetManifestRef]>,
    evidence_digest: EvidenceDigest,
    downstream_evidence_digest: EvidenceDigest,
}

impl MacroFeatureVector {
    /// Maps one complete neutral snapshot into the single code-owned V1 economic order.
    pub(crate) fn try_from_snapshot(snapshot: &MacroContextSnapshot) -> Result<Self, ServiceError> {
        let evidence = snapshot.evidence();
        let evidence_digest = evidence.consumed_digest();
        if evidence_digest.algorithm() != DigestAlgorithm::Sha256
            || evidence_digest.bytes() == [0; 32]
        {
            return Err(ServiceError::InvalidResult);
        }
        let investment_context = match MacroInvestmentContext::try_from_snapshot(snapshot) {
            Ok(context) => Some(context),
            Err(ServiceError::Unavailable) => None,
            Err(error) => return Err(error),
        };

        let descriptors = feature_dataset_macro_components_v1();
        if snapshot.selected().len() != descriptors.len() {
            return Err(ServiceError::InvalidResult);
        }
        let mut retained = vec![false; snapshot.selected().len()];
        let mut components = Vec::new();
        components
            .try_reserve_exact(descriptors.len())
            .map_err(|_| ServiceError::ResourceExhausted)?;

        for (position, descriptor) in descriptors.iter().copied().enumerate() {
            if usize::from(descriptor.position()) != position {
                return Err(ServiceError::InvalidResult);
            }
            let mut matches = snapshot
                .selected()
                .iter()
                .enumerate()
                .filter(|(_, selected)| selected.indicator_id() == descriptor.indicator_id());
            let (selected_index, selected) = matches.next().ok_or(ServiceError::InvalidResult)?;
            if matches.next().is_some() || retained[selected_index] {
                return Err(ServiceError::InvalidResult);
            }
            retained[selected_index] = true;

            let observation = selected.observation().ok_or(ServiceError::Unavailable)?;
            let value = match (
                observation.value().observed_value(),
                observation.value().missing_value(),
            ) {
                (Some(value), None) => value,
                (None, Some(_)) => return Err(ServiceError::Unavailable),
                (Some(_), Some(_)) | (None, None) => return Err(ServiceError::InvalidResult),
            };
            let context = observation.context();
            let provenance = context.provenance();
            let effective = context.time().effective().clone();
            if effective.source_period_value().is_some()
                || provenance.instrument_id().is_some()
                || provenance.venue_id().is_some()
                || provenance.received_at() > evidence.knowledge_cutoff()
                || provenance.ingested_at() > evidence.knowledge_cutoff()
                || provenance
                    .availability()
                    .conservative_available_at()
                    .is_none_or(|available| available > evidence.knowledge_cutoff())
            {
                return Err(ServiceError::InvalidResult);
            }
            let specification = FeatureLabelComponentSpec::try_new(
                ComponentKind::Feature,
                ComponentScope::Global,
                CorporateActionSensitivity::NotApplicable,
                descriptor.component_name(),
                std::num::NonZeroU32::MIN,
            )
            .map_err(|_| ServiceError::InvalidResult)?;
            let unit = SourceIdentifier::try_from(descriptor.unit())
                .map_err(|_| ServiceError::InvalidResult)?;
            components.push(
                FeatureLabelComponentInput::try_new(
                    specification,
                    ComponentValue::decimal(value, Some(unit), None)
                        .map_err(|_| ServiceError::InvalidResult)?,
                    vec![ComponentSelector::new(ObservationFamilyKey::Macro {
                        source_id: provenance.source_id().clone(),
                        series: observation.series().clone(),
                        effective: effective.clone(),
                    })],
                    effective,
                    None,
                    ComponentAdjustmentEvidence::NotApplicable,
                )
                .map_err(|_| ServiceError::InvalidResult)?,
            );
        }
        if retained.iter().any(|retained| !retained) {
            return Err(ServiceError::InvalidResult);
        }
        if evidence.consumed_parent_manifests().is_empty() {
            return Err(ServiceError::InvalidResult);
        }

        let mut downstream = Sha256::new();
        downstream.update(b"market-squawk/macro-downstream-evidence/v1\0");
        downstream.update(evidence_digest.bytes());
        match investment_context.as_ref() {
            Some(context) => {
                downstream.update([1]);
                downstream.update(context.evidence_digest().bytes());
            }
            None => downstream.update([0]),
        }
        hash_parent_manifests(&mut downstream, evidence.consumed_parent_manifests())?;
        let downstream_evidence_digest = require_sha256(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            downstream.finalize().into(),
        ))?;

        Ok(Self {
            components: components.into_boxed_slice(),
            investment_context,
            knowledge_cutoff: evidence.knowledge_cutoff(),
            effective_date_cutoff: evidence.effective_date_cutoff(),
            parent_manifests: evidence
                .consumed_parent_manifests()
                .to_vec()
                .into_boxed_slice(),
            evidence_digest,
            downstream_evidence_digest,
        })
    }

    /// Returns label-free global components in the domain-owned economic order.
    pub(crate) fn components(&self) -> &[FeatureLabelComponentInput] {
        &self.components
    }

    /// Returns exact curve/regime and valuation-rate evidence derived from this same selection.
    pub(crate) const fn investment_context(&self) -> Option<&MacroInvestmentContext> {
        self.investment_context.as_ref()
    }

    /// Returns the exact knowledge-time cutoff used by every selection.
    pub(crate) const fn knowledge_cutoff(&self) -> Timestamp {
        self.knowledge_cutoff
    }

    /// Returns the requested provider-neutral effective-date selection boundary.
    pub(crate) const fn effective_date_cutoff(&self) -> CalendarDate {
        self.effective_date_cutoff
    }

    /// Returns the canonical duplicate-free parent generation set.
    pub(crate) fn parent_manifests(&self) -> &[DatasetManifestRef] {
        &self.parent_manifests
    }

    /// Returns the opaque exact identity of the neutral snapshot and its selections.
    pub(crate) const fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest
    }

    /// Returns the exact source selection plus any admitted curve/valuation derivation.
    pub(crate) const fn downstream_evidence_digest(&self) -> EvidenceDigest {
        self.downstream_evidence_digest
    }

    /// Returns exact native source-effective component cutoffs in economic vector order.
    pub(crate) fn component_cutoffs(&self) -> impl Iterator<Item = &ResearchTemporalCoordinate> {
        self.components
            .iter()
            .map(FeatureLabelComponentInput::selection_effective_cutoff)
    }
}

fn observed_indicator<'snapshot>(
    snapshot: &'snapshot MacroContextSnapshot,
    indicator_id: &str,
) -> Result<&'snapshot market_squawk_domain::MacroObservation, ServiceError> {
    let mut matches = snapshot
        .selected()
        .iter()
        .filter(|selected| selected.indicator_id() == indicator_id);
    let selected = matches.next().ok_or(ServiceError::InvalidResult)?;
    if matches.next().is_some() {
        return Err(ServiceError::InvalidResult);
    }
    selected.observation().ok_or(ServiceError::Unavailable)
}

fn observed_value(
    observation: &market_squawk_domain::MacroObservation,
) -> Result<Decimal, ServiceError> {
    match (
        observation.value().observed_value(),
        observation.value().missing_value(),
    ) {
        (Some(value), None) => Ok(value.normalize()),
        (None, Some(_)) => Err(ServiceError::Unavailable),
        (Some(_), Some(_)) | (None, None) => Err(ServiceError::InvalidResult),
    }
}

fn classify_rate_regime(
    three_month_to_ten_year: Decimal,
    two_year_to_ten_year: Decimal,
) -> MacroRateRegime {
    match (
        three_month_to_ten_year.cmp(&Decimal::ZERO),
        two_year_to_ten_year.cmp(&Decimal::ZERO),
    ) {
        (std::cmp::Ordering::Greater, std::cmp::Ordering::Greater) => {
            MacroRateRegime::UpwardSloping
        }
        (std::cmp::Ordering::Equal, std::cmp::Ordering::Equal) => MacroRateRegime::Flat,
        (std::cmp::Ordering::Less, std::cmp::Ordering::Less) => MacroRateRegime::Inverted,
        _ => MacroRateRegime::Mixed,
    }
}

fn derived_evidence_digest(
    domain: &[u8],
    source: &super::macro_context::MacroContextEvidenceReceipt,
    effective: &ResearchTemporalCoordinate,
    values: &[Decimal],
    discriminator: &[u8],
) -> Result<EvidenceDigest, ServiceError> {
    let mut digest = Sha256::new();
    hash_bytes(&mut digest, domain)?;
    digest.update(source.consumed_digest().bytes());
    digest.update(source.knowledge_cutoff().unix_nanos().to_be_bytes());
    digest.update(source.effective_date_cutoff().year().to_be_bytes());
    digest.update([
        source.effective_date_cutoff().month(),
        source.effective_date_cutoff().day(),
    ]);
    hash_temporal_coordinate(&mut digest, effective)?;
    digest.update(
        u64::try_from(values.len())
            .map_err(|_| ServiceError::ResourceExhausted)?
            .to_be_bytes(),
    );
    for value in values {
        let value = value.normalize();
        digest.update(value.mantissa().to_be_bytes());
        digest.update(value.scale().to_be_bytes());
    }
    hash_bytes(&mut digest, discriminator)?;
    hash_parent_manifests(&mut digest, source.consumed_parent_manifests())?;
    require_sha256(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn hash_parent_manifests(
    digest: &mut Sha256,
    parents: &[DatasetManifestRef],
) -> Result<(), ServiceError> {
    digest.update(
        u64::try_from(parents.len())
            .map_err(|_| ServiceError::ResourceExhausted)?
            .to_be_bytes(),
    );
    for parent in parents {
        hash_bytes(digest, parent.dataset_id().as_str().as_bytes())?;
        digest.update(parent.manifest_version().to_be_bytes());
        hash_bytes(digest, parent.schema().name().as_bytes())?;
        digest.update(parent.schema().version().get().to_be_bytes());
        digest.update(parent.schema().fingerprint());
        digest.update(parent.content_hash().bytes());
    }
    Ok(())
}

fn hash_temporal_coordinate(
    digest: &mut Sha256,
    coordinate: &ResearchTemporalCoordinate,
) -> Result<(), ServiceError> {
    if let Some(timestamp) = coordinate.exact_timestamp() {
        digest.update([1]);
        digest.update(timestamp.unix_nanos().to_be_bytes());
    } else if let Some(date) = coordinate.calendar_date_value() {
        digest.update([2]);
        digest.update(date.year().to_be_bytes());
        digest.update([date.month(), date.day()]);
    } else {
        return Err(ServiceError::InvalidResult);
    }
    Ok(())
}

fn hash_bytes(digest: &mut Sha256, value: &[u8]) -> Result<(), ServiceError> {
    digest.update(
        u64::try_from(value.len())
            .map_err(|_| ServiceError::ResourceExhausted)?
            .to_be_bytes(),
    );
    digest.update(value);
    Ok(())
}

fn require_sha256(digest: EvidenceDigest) -> Result<EvidenceDigest, ServiceError> {
    if digest.algorithm() != DigestAlgorithm::Sha256 || digest.bytes() == [0; 32] {
        Err(ServiceError::InvalidResult)
    } else {
        Ok(digest)
    }
}
