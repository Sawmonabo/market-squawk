//! Stable identities for build specifications, selectors, policies, and output rows.

use market_squawk_domain::{
    AvailabilityEvidence, EvidenceDigest, FundamentalPeriod, ResearchTemporalCoordinate,
    SourceIdentifier,
};
use sha2::{Digest as _, Sha256};

use super::model::{
    ComponentAdjustmentEvidence, ComponentValue, DatasetBuildInputs, DatasetBuildLimits,
    DatasetBuildPolicy, DatasetBuildRequest, DatasetExample, DatasetOutputAuthorization,
    DatasetSplit, FeatureLabelComponentInput,
};
use crate::{
    CorporateActionAdjustment, DatasetId, DatasetManifestRef, ObservationFamilyKey, ResearchUse,
    ResearchUseLimits, RightsBasis, Sha256Digest, UniverseMembership,
};

pub(super) fn family_key_digest(family: &ObservationFamilyKey) -> Sha256Digest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/dataset-component-selector/v2");
    encode_family(&mut hash, family);
    Sha256Digest::new(hash.finalize().into())
}

pub(super) fn policy_digest(policy: &DatasetBuildPolicy) -> Sha256Digest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/dataset-build-policy/v1");
    for boundary in policy.split().boundaries() {
        hash.update(boundary.unix_nanos().to_be_bytes());
    }
    hash.update(policy.point_in_time().version().get().to_be_bytes());
    hash.update([match policy.point_in_time().revision_mode() {
        crate::PointInTimeRevisionMode::LatestKnown => 1,
        crate::PointInTimeRevisionMode::AllKnown => 2,
    }]);
    hash.update(policy.corporate_actions().version().get().to_be_bytes());
    hash.update([match policy.corporate_actions().adjustment() {
        CorporateActionAdjustment::Raw => 1,
        CorporateActionAdjustment::SplitAdjusted => 2,
        CorporateActionAdjustment::TotalReturn => 3,
    }]);
    hash.update([policy.missing_values().tag()]);
    put_str(&mut hash, policy.implementation_revision().as_str());
    Sha256Digest::new(hash.finalize().into())
}

pub(super) fn universe_contract_digest(inputs: &DatasetBuildInputs) -> Sha256Digest {
    let mut memberships = inputs
        .universe_memberships()
        .iter()
        .map(membership_digest)
        .collect::<Vec<_>>();
    memberships.sort_unstable();
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/dataset-universe-contract/v1");
    put_str(&mut hash, inputs.universe_id().as_str());
    put_len(&mut hash, memberships.len());
    for membership in memberships {
        hash.update(membership.bytes());
    }
    Sha256Digest::new(hash.finalize().into())
}

#[allow(
    clippy::too_many_arguments,
    reason = "the build identity binds every independent semantic dimension"
)]
pub(super) fn build_spec_digest(
    output_dataset: &DatasetId,
    inputs: &DatasetBuildInputs,
    intended_use: ResearchUse,
    research_use_limits: ResearchUseLimits,
    output_authorization: &DatasetOutputAuthorization,
    limits: DatasetBuildLimits,
    policy_digest: Sha256Digest,
    universe_digest: Sha256Digest,
) -> Sha256Digest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/feature-label-build-spec/v4");
    put_str(&mut hash, output_dataset.as_str());
    hash.update(policy_digest.bytes());
    hash.update(universe_digest.bytes());
    hash.update([research_use_tag(intended_use)]);
    encode_research_use_limits(&mut hash, research_use_limits);
    encode_output_authorization(&mut hash, output_authorization);
    encode_build_limits(&mut hash, limits);
    put_len(&mut hash, inputs.parents().len());
    for parent in inputs.parents() {
        encode_manifest(&mut hash, parent);
    }
    put_len(&mut hash, inputs.component_specs().len());
    for spec in inputs.component_specs() {
        hash.update([spec.kind().tag()]);
        hash.update([spec.scope().tag()]);
        hash.update([spec.corporate_actions().tag()]);
        put_str(&mut hash, spec.name());
        hash.update(spec.version().get().to_be_bytes());
    }
    put_len(&mut hash, inputs.examples().len());
    for example in inputs.examples() {
        put_str(&mut hash, example.example_id());
        hash.update(example.instrument_id().as_uuid().as_bytes());
        hash.update(example.cutoff_at().unix_nanos().to_be_bytes());
        hash.update(example.label_cutoff_at().unix_nanos().to_be_bytes());
        encode_temporal(&mut hash, example.effective_cutoff());
        encode_temporal(&mut hash, example.label_effective_cutoff());
        put_len(&mut hash, example.components().len());
        for component in example.components() {
            encode_component(&mut hash, component);
        }
    }
    Sha256Digest::new(hash.finalize().into())
}

fn encode_research_use_limits(hash: &mut Sha256, limits: ResearchUseLimits) {
    for value in [
        limits.max_roots(),
        limits.max_nodes(),
        limits.max_edges(),
        limits.max_sources(),
        limits.max_retained_bytes(),
    ] {
        put_len(hash, value);
    }
    hash.update(limits.traversal_deadline().as_nanos().to_be_bytes());
    hash.update(limits.permit_lifetime().as_nanos().to_be_bytes());
}

fn encode_output_authorization(hash: &mut Sha256, authorization: &DatasetOutputAuthorization) {
    put_str(hash, authorization.source_id().as_str());
    let basis = authorization.basis();
    hash.update([match basis {
        RightsBasis::ReviewedTerms(_) => 1,
        RightsBasis::UserOwnedLocal(_) => 2,
        RightsBasis::ImportedUserInput(_) => 3,
    }]);
    put_str(hash, basis.reference());
    encode_evidence(hash, basis.digest());
    match basis.root_identity_digest() {
        Some(digest) => {
            hash.update([1]);
            encode_evidence(hash, digest);
        }
        None => hash.update([0]),
    }
    match basis.imported_user_input_evidence() {
        Some(evidence) => {
            hash.update([1]);
            encode_evidence(hash, evidence.admitted_input_set_digest());
            encode_evidence(hash, evidence.generated_manifest_digest());
            encode_evidence(hash, evidence.local_admission_evidence());
            encode_evidence(hash, evidence.workspace_receipt_evidence());
            encode_evidence(hash, evidence.import_receipt_evidence());
            encode_evidence(hash, evidence.binding_digest());
        }
        None => hash.update([0]),
    }
    encode_evidence(hash, authorization.authorization_evidence());
    encode_optional_timestamp(hash, authorization.authorization_expires_at());
}

fn encode_build_limits(hash: &mut Sha256, limits: DatasetBuildLimits) {
    for value in [
        limits.max_input_rows(),
        limits.max_examples(),
        limits.max_components_per_example(),
        limits.max_output_rows(),
        limits.max_retained_bytes(),
    ] {
        put_len(hash, value);
    }
    hash.update(limits.max_duration().as_nanos().to_be_bytes());

    let point_in_time = limits.point_in_time();
    for value in [
        point_in_time.max_candidates(),
        point_in_time.max_families(),
        point_in_time.max_conflicts(),
        point_in_time.max_result_rows(),
        point_in_time.max_retained_bytes(),
    ] {
        put_len(hash, value);
    }

    let universe = limits.universe();
    put_len(hash, universe.max_candidates());
    put_len(hash, universe.max_retained_bytes());

    let corporate_actions = limits.corporate_actions();
    put_len(hash, corporate_actions.max_actions().get());
    put_len(hash, corporate_actions.max_retained_bytes().get());
}

#[allow(
    clippy::too_many_arguments,
    reason = "row lineage binds each independent temporal and policy decision"
)]
pub(super) fn row_lineage_digest(
    request: &DatasetBuildRequest,
    example: &DatasetExample,
    split: DatasetSplit,
    component: &FeatureLabelComponentInput,
    selection_content: Sha256Digest,
    selection_audit: Sha256Digest,
    selected_evidence: &[Sha256Digest],
    universe_content: Sha256Digest,
    universe_audit: Sha256Digest,
    action_content: Sha256Digest,
    action_audit: Sha256Digest,
) -> Sha256Digest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/feature-label-row-lineage/v3");
    hash.update(request.build_spec_digest().digest().bytes());
    hash.update(request.policy_digest().bytes());
    hash.update(request.universe_digest().bytes());
    put_str(&mut hash, example.example_id());
    hash.update(example.instrument_id().as_uuid().as_bytes());
    hash.update(example.cutoff_at().unix_nanos().to_be_bytes());
    hash.update(example.label_cutoff_at().unix_nanos().to_be_bytes());
    encode_temporal(&mut hash, example.effective_cutoff());
    encode_temporal(&mut hash, example.label_effective_cutoff());
    put_str(&mut hash, split.name());
    encode_component(&mut hash, component);
    hash.update(selection_content.bytes());
    hash.update(selection_audit.bytes());
    put_len(&mut hash, selected_evidence.len());
    for evidence in selected_evidence {
        hash.update(evidence.bytes());
    }
    hash.update(universe_content.bytes());
    hash.update(universe_audit.bytes());
    hash.update(action_content.bytes());
    hash.update(action_audit.bytes());
    Sha256Digest::new(hash.finalize().into())
}

fn membership_digest(membership: &UniverseMembership) -> Sha256Digest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/dataset-universe-membership/v1");
    hash.update(membership.instrument_id().as_uuid().as_bytes());
    let interval = membership.effective_interval();
    hash.update(interval.starts_at().unix_nanos().to_be_bytes());
    encode_optional_timestamp(&mut hash, interval.ends_at());
    encode_availability(&mut hash, membership.availability());
    encode_manifest(&mut hash, membership.source_manifest());
    encode_evidence(&mut hash, membership.evidence_digest());
    Sha256Digest::new(hash.finalize().into())
}

fn encode_component(hash: &mut Sha256, component: &FeatureLabelComponentInput) {
    hash.update([component.spec().kind().tag()]);
    hash.update([component.spec().scope().tag()]);
    hash.update([component.spec().corporate_actions().tag()]);
    put_str(hash, component.spec().name());
    hash.update(component.spec().version().get().to_be_bytes());
    match component.value() {
        ComponentValue::Float {
            value,
            unit,
            currency,
        } => {
            hash.update([1]);
            hash.update(value.to_bits().to_be_bytes());
            encode_optional_identifier(hash, unit.as_ref());
            encode_optional_str(hash, currency.as_ref().map(|value| value.as_str()));
        }
        ComponentValue::Decimal {
            value,
            unit,
            currency,
        } => {
            hash.update([2]);
            hash.update(value.mantissa().to_be_bytes());
            hash.update(value.scale().to_be_bytes());
            encode_optional_identifier(hash, unit.as_ref());
            encode_optional_str(hash, currency.as_ref().map(|value| value.as_str()));
        }
        ComponentValue::Missing { reason } => {
            hash.update([3]);
            put_str(hash, reason.as_str());
        }
    }
    put_len(hash, component.selectors().len());
    for selector in component.selectors() {
        hash.update(selector.identity().bytes());
    }
    match component.adjustment() {
        ComponentAdjustmentEvidence::Raw => hash.update([1]),
        ComponentAdjustmentEvidence::NotApplicable => hash.update([2]),
        ComponentAdjustmentEvidence::Applied {
            policy,
            plan_content,
            plan_audit,
            implementation_evidence,
        } => {
            hash.update([3]);
            hash.update(policy.version().get().to_be_bytes());
            hash.update([match policy.adjustment() {
                CorporateActionAdjustment::Raw => 1,
                CorporateActionAdjustment::SplitAdjusted => 2,
                CorporateActionAdjustment::TotalReturn => 3,
            }]);
            hash.update(plan_content.bytes());
            hash.update(plan_audit.bytes());
            encode_evidence(hash, *implementation_evidence);
        }
    }
}

fn encode_family(hash: &mut Sha256, family: &ObservationFamilyKey) {
    match family {
        ObservationFamilyKey::Filing {
            source_id,
            instrument_id,
            accession,
        } => {
            hash.update([1]);
            put_str(hash, source_id.as_str());
            hash.update(instrument_id.as_uuid().as_bytes());
            put_str(hash, accession.as_str());
        }
        ObservationFamilyKey::Fundamental {
            source_id,
            instrument_id,
            concept,
            unit,
            period,
        } => {
            hash.update([2]);
            put_str(hash, source_id.as_str());
            hash.update(instrument_id.as_uuid().as_bytes());
            put_str(hash, concept.as_str());
            put_str(hash, unit.as_str());
            encode_fundamental_period(hash, *period);
        }
        ObservationFamilyKey::Macro {
            source_id,
            series,
            effective,
        } => {
            hash.update([3]);
            put_str(hash, source_id.as_str());
            put_str(hash, series.as_str());
            encode_temporal(hash, effective);
        }
        ObservationFamilyKey::MarketBar {
            source_id,
            instrument_id,
            venue_id,
            provider_instrument_id,
            feed,
            interval,
            adjustment,
            timestamp_basis,
            session,
            effective,
        } => {
            hash.update([9]);
            put_str(hash, source_id.as_str());
            hash.update(instrument_id.as_uuid().as_bytes());
            put_str(hash, venue_id.as_str());
            put_str(hash, provider_instrument_id.as_str());
            put_str(hash, feed.as_str());
            put_str(hash, interval.as_str());
            hash.update([match adjustment {
                market_squawk_domain::MarketBarAdjustment::Raw => 1,
                market_squawk_domain::MarketBarAdjustment::Split => 2,
                market_squawk_domain::MarketBarAdjustment::Dividend => 3,
                market_squawk_domain::MarketBarAdjustment::SpinOff => 4,
                market_squawk_domain::MarketBarAdjustment::All => 5,
            }]);
            hash.update([match timestamp_basis {
                market_squawk_domain::BarTimestampBasis::PeriodStart => 1,
                market_squawk_domain::BarTimestampBasis::PeriodEnd => 2,
            }]);
            hash.update([match session.kind() {
                market_squawk_domain::MarketBarSessionKind::Regular => 1,
                market_squawk_domain::MarketBarSessionKind::Extended => 2,
                market_squawk_domain::MarketBarSessionKind::Continuous => 3,
                market_squawk_domain::MarketBarSessionKind::ProviderDefined => 4,
            }]);
            put_str(hash, session.ruleset().as_str());
            hash.update([match session.evidence().algorithm() {
                market_squawk_domain::DigestAlgorithm::Sha256 => 1,
                market_squawk_domain::DigestAlgorithm::Blake3 => 2,
            }]);
            hash.update(session.evidence().bytes());
            encode_temporal(hash, effective);
        }
        ObservationFamilyKey::FundNav {
            source_id,
            provider_product,
            provider_channel,
            instrument_id,
            provider_instrument_id,
            nav_date,
            valuation_basis,
            currency,
        } => {
            hash.update([10]);
            put_str(hash, source_id.as_str());
            put_str(hash, provider_product.as_source_identifier().as_str());
            put_str(hash, provider_channel.as_source_identifier().as_str());
            hash.update(instrument_id.as_uuid().as_bytes());
            put_str(hash, provider_instrument_id.as_str());
            hash.update(nav_date.year().to_be_bytes());
            hash.update([nav_date.month(), nav_date.day()]);
            hash.update([match valuation_basis {
                market_squawk_domain::FundNavValuationBasis::PerShare => 1,
            }]);
            put_str(hash, currency.as_str());
        }
        ObservationFamilyKey::PortfolioPosition {
            source_id,
            instrument_id,
            account_id,
            effective,
        } => {
            hash.update([4]);
            put_str(hash, source_id.as_str());
            hash.update(instrument_id.as_uuid().as_bytes());
            put_str(hash, account_id.as_str());
            encode_temporal(hash, effective);
        }
        ObservationFamilyKey::Transaction {
            source_id,
            instrument_id,
            account_id,
            source_record_id,
        } => {
            hash.update([5]);
            put_str(hash, source_id.as_str());
            match instrument_id {
                Some(instrument_id) => {
                    hash.update([1]);
                    hash.update(instrument_id.as_uuid().as_bytes());
                }
                None => hash.update([0]),
            }
            put_str(hash, account_id.as_str());
            put_str(hash, source_record_id.as_str());
        }
        ObservationFamilyKey::CorporateAction {
            source_id,
            instrument_id,
            source_record,
        } => {
            hash.update([6]);
            put_str(hash, source_id.as_str());
            hash.update(instrument_id.as_uuid().as_bytes());
            put_str(hash, source_record.as_str());
        }
        ObservationFamilyKey::UniverseMembership {
            source_id,
            instrument_id,
            source_record,
            universe,
        } => {
            hash.update([7]);
            put_str(hash, source_id.as_str());
            hash.update(instrument_id.as_uuid().as_bytes());
            put_str(hash, source_record.as_str());
            put_str(hash, universe.as_str());
        }
        ObservationFamilyKey::AlternativeData {
            source_id,
            instrument_id,
            source_record,
            dataset,
            field,
            effective,
        } => {
            hash.update([8]);
            put_str(hash, source_id.as_str());
            match instrument_id {
                Some(instrument_id) => {
                    hash.update([1]);
                    hash.update(instrument_id.as_uuid().as_bytes());
                }
                None => hash.update([0]),
            }
            put_str(hash, source_record.as_str());
            put_str(hash, dataset.as_str());
            put_str(hash, field.as_str());
            encode_temporal(hash, effective);
        }
    }
}

fn encode_temporal(hash: &mut Sha256, coordinate: &ResearchTemporalCoordinate) {
    if let Some(timestamp) = coordinate.exact_timestamp() {
        hash.update([1]);
        hash.update(timestamp.unix_nanos().to_be_bytes());
    } else if let Some(date) = coordinate.calendar_date_value() {
        hash.update([2]);
        hash.update(date.year().to_be_bytes());
        hash.update([date.month(), date.day()]);
    } else if let Some(period) = coordinate.source_period_value() {
        hash.update([3]);
        put_str(hash, period.scheme().as_str());
        hash.update(period.year().to_be_bytes());
        hash.update(period.ordinal().get().to_be_bytes());
        put_str(hash, period.code().as_str());
    } else {
        hash.update([0]);
    }
}

fn encode_fundamental_period(hash: &mut Sha256, period: FundamentalPeriod) {
    match period {
        FundamentalPeriod::Instant { instant } => {
            hash.update([1]);
            hash.update(instant.year().to_be_bytes());
            hash.update([instant.month(), instant.day()]);
        }
        FundamentalPeriod::Duration { start, end } => {
            hash.update([2]);
            hash.update(start.year().to_be_bytes());
            hash.update([start.month(), start.day()]);
            hash.update(end.year().to_be_bytes());
            hash.update([end.month(), end.day()]);
        }
    }
}

fn encode_manifest(hash: &mut Sha256, manifest: &DatasetManifestRef) {
    put_str(hash, manifest.dataset_id().as_str());
    hash.update(manifest.manifest_version().to_be_bytes());
    put_str(hash, manifest.schema().name());
    hash.update(manifest.schema_version().get().to_be_bytes());
    hash.update(manifest.schema().fingerprint());
    hash.update(manifest.content_hash().bytes());
}

fn encode_availability(hash: &mut Sha256, availability: &AvailabilityEvidence) {
    match availability {
        AvailabilityEvidence::Evidenced {
            available_at,
            evidence,
        } => {
            hash.update([1]);
            hash.update(available_at.unix_nanos().to_be_bytes());
            put_str(hash, evidence.as_str());
        }
        AvailabilityEvidence::LocalFirstObserved { observed_at } => {
            hash.update([2]);
            hash.update(observed_at.unix_nanos().to_be_bytes());
        }
        AvailabilityEvidence::Inferred {
            inferred_at,
            method,
        } => {
            hash.update([3]);
            hash.update(inferred_at.unix_nanos().to_be_bytes());
            put_str(hash, method.as_str());
        }
        AvailabilityEvidence::Unknown => hash.update([4]),
    }
}

fn encode_evidence(hash: &mut Sha256, evidence: EvidenceDigest) {
    hash.update([match evidence.algorithm() {
        market_squawk_domain::DigestAlgorithm::Sha256 => 1,
        market_squawk_domain::DigestAlgorithm::Blake3 => 2,
    }]);
    hash.update(evidence.bytes());
}

fn encode_optional_timestamp(hash: &mut Sha256, value: Option<market_squawk_domain::Timestamp>) {
    match value {
        Some(value) => {
            hash.update([1]);
            hash.update(value.unix_nanos().to_be_bytes());
        }
        None => hash.update([0]),
    }
}

fn encode_optional_identifier(hash: &mut Sha256, value: Option<&SourceIdentifier>) {
    encode_optional_str(hash, value.map(SourceIdentifier::as_str));
}

fn encode_optional_str(hash: &mut Sha256, value: Option<&str>) {
    match value {
        Some(value) => {
            hash.update([1]);
            put_str(hash, value);
        }
        None => hash.update([0]),
    }
}

const fn research_use_tag(value: ResearchUse) -> u8 {
    match value {
        ResearchUse::Display => 1,
        ResearchUse::LocalAnalysis => 2,
        ResearchUse::Train => 3,
    }
}

fn put_str(hash: &mut Sha256, value: &str) {
    put_len(hash, value.len());
    hash.update(value.as_bytes());
}

fn put_len(hash: &mut Sha256, value: usize) {
    hash.update(u64::try_from(value).unwrap_or(u64::MAX).to_be_bytes());
}
