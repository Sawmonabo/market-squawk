//! Selected-stock annual datasets from exact retained native history and listed membership.

use super::*;
use crate::application::{
    analytical_profile::ValidatedAnalyticalProfile,
    research::corporate_actions::SourceAppliedCorporateActionPlanReference,
};
use market_squawk_data::{CurrentListedPopulation, DatasetTargetHorizon};
use std::time::Duration;

#[allow(
    clippy::too_many_arguments,
    reason = "the existing source, profile, population and request fences remain explicit"
)]
pub(in crate::application::research::dataset_preparation) async fn prepare_investment_dataset(
    authority: &DatasetPreparationAuthority,
    instrument: InstrumentId,
    cutoff: Timestamp,
    reference: SourceAppliedCorporateActionPlanReference,
    profile: &ValidatedAnalyticalProfile,
    population: CurrentListedPopulation,
    intended_use: DatasetPreparationUse,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<PreparedFeatureDatasetBuild, DatasetPreparationError> {
    check_control(deadline, cancellation)?;
    if reference.knowledge_cutoff() != cutoff
        || !reference.requested_instruments().contains(&instrument)
        || population.instrument_ids() != [instrument]
        || population.membership_as_of() != cutoff
        || population.financial_profile_digest().bytes()
            != super::super::decode_sha256(&profile.resolution().configuration_digest)
                .ok_or(DatasetPreparationError::InvalidSelection)?
        || !profile
            .recommendation_policy()
            .parameters()
            .allow_retrospective_studies
    {
        return Err(DatasetPreparationError::InvalidSelection);
    }
    let history = authority
        .source_actions
        .read_history_reference(
            &reference,
            instrument,
            deadline,
            cancellation.child_token(),
            None,
        )
        .await
        .map_err(super::super::map_source_action_error)?
        .ok_or(DatasetPreparationError::Unavailable)?;
    let source = native_series(
        authority,
        history,
        instrument,
        cutoff,
        deadline,
        cancellation,
    )
    .await?;
    // These partitions depend only on authentic source coordinates, before any return or macro
    // value is inspected. build_cohort retains only exact annual terminals within each partition.
    let (starts_at, ends_at, split) = annual_partitions(&source)?;
    let history = source
        .nominal_history
        .as_ref()
        .or(source.completed_history.as_ref())
        .ok_or(DatasetPreparationError::InvalidEvidence)?;
    let source_plan = authority
        .source_actions
        .read_price_reference_for_histories(
            &reference,
            &[history],
            deadline,
            cancellation.child_token(),
            None,
        )
        .await
        .map_err(super::super::map_source_action_error)?
        .ok_or(DatasetPreparationError::Unavailable)?
        .into_covered_price_plan()
        .map_err(super::super::map_source_action_error)?;
    if source_plan.retained_bytes() > MAXIMUM_COHORT_SOURCE_BYTES {
        return Err(DatasetPreparationError::Capacity);
    }
    let study = DatasetStudyPolicy::try_new(
        HistoricalStudyBasis::RetrospectiveFrozenSnapshot,
        DatasetBuildPurpose::Training,
        cutoff,
        Some(Duration::ZERO),
        DatasetTargetHorizon::ExactElapsed(Duration::from_nanos(
            RECOMMENDATION_TARGET_HORIZON_NANOS_V1 as u64,
        )),
    )
    .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
    let request = CohortPreparationRequest {
        subject_instrument: instrument,
        subject_manifest: history.selection().pinned().manifest().clone(),
        benchmark: None,
        study,
        population_starts_at: starts_at,
        population_ends_at: ends_at,
        split,
        source_action_reference: Some(reference),
        event: None,
        probability_subject: None,
        costs: None,
    };
    let partitions = population
        .partitions()
        .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
    let [partition] = partitions.as_ref() else {
        return Err(DatasetPreparationError::InvalidEvidence);
    };
    // Native price authority already covers the exact action pool. Historical universe rows and
    // unrelated canonical generations have no role in this declared present-day fixed cohort.
    let support = CanonicalSupport::from_generations(&[])?;
    let cohort = build_cohort(
        authority,
        &request,
        &[source],
        &support,
        Some(partition),
        Some(&Arc::new(source_plan)),
        deadline,
        cancellation,
    )
    .await?;
    let contract = match intended_use {
        DatasetPreparationUse::Train => {
            FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonForwardReturnTrainingV1
        }
        DatasetPreparationUse::LocalAnalysis => {
            FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonForwardReturnAnalysisV1
        }
    };
    authority.finalize_source_cohort(&cohort, intended_use, contract, deadline, cancellation)
}

async fn native_series(
    authority: &DatasetPreparationAuthority,
    history: CompleteMarketBarHistoryCursor,
    instrument: InstrumentId,
    cutoff: Timestamp,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<CohortSeries, DatasetPreparationError> {
    let receipt = history.selection().receipt();
    if history.read_receipt().knowledge_cutoff() != cutoff
        || receipt.instrument_id() != instrument
        || receipt.adjustment() != MarketBarAdjustment::Raw
    {
        return Err(DatasetPreparationError::InvalidEvidence);
    }
    if history.bar_count() < 2 {
        return Err(DatasetPreparationError::Unavailable);
    }
    if history.bar_count() > MAXIMUM_OBSERVATIONS_PER_GENERATION {
        return Err(DatasetPreparationError::Capacity);
    }
    if receipt.date_windows().is_some() {
        return nominal_series_from_history(
            authority,
            history,
            instrument,
            cutoff,
            deadline,
            cancellation,
        )
        .await;
    }
    let manifest = history.selection().pinned().manifest().clone();
    let mut observations = Vec::new();
    observations
        .try_reserve_exact(history.bar_count())
        .map_err(|_| DatasetPreparationError::Capacity)?;
    for bar in history.bars() {
        check_control(deadline, cancellation)?;
        observations.push(ResearchObservation::MarketBar(bar.map_err(|error| {
            super::super::preparation_read_error("stock_history", error)
        })?));
    }
    let mut source = select_series(
        authority,
        &observations,
        &manifest,
        instrument,
        cutoff,
        HistoricalStudyBasis::RetrospectiveFrozenSnapshot,
        deadline,
        cancellation,
    )
    .await?;
    if source.points.len() != history.bar_count() || source.nominal_history.is_some() {
        return Err(DatasetPreparationError::InvalidEvidence);
    }
    source.completed_history = Some(history);
    Ok(source)
}

fn annual_partitions(
    source: &CohortSeries,
) -> Result<(Timestamp, Timestamp, ChronologicalSplitPolicy), DatasetPreparationError> {
    let count = source
        .len()
        .checked_sub(1)
        .ok_or(DatasetPreparationError::Unavailable)?;
    let first = count / 3;
    let second = count * 2 / 3;
    if first < 2 || second <= first || second >= count {
        return Err(DatasetPreparationError::Unavailable);
    }
    let starts_at = source[1].effective;
    let ends_at = source[count].effective;
    let boundaries = [source[first].effective, source[second].effective, ends_at];
    let split = ChronologicalSplitPolicy::try_new(boundaries[0], boundaries[1], boundaries[2])
        .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
    // Verify genuine coverage before expensive macro reads, using coordinates alone. No row is
    // sampled, relabeled or retained based on its price or eventual outcome.
    let mut counts = [0_usize; 3];
    for current in source.iter().skip(1) {
        let target = current
            .effective
            .checked_add_nanos(RECOMMENDATION_TARGET_HORIZON_NANOS_V1)
            .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
        let partition = boundaries
            .iter()
            .position(|end| current.effective <= *end)
            .ok_or(DatasetPreparationError::InvalidEvidence)?;
        if target <= boundaries[partition]
            && source
                .binary_search_by_key(&target, |point| point.effective)
                .is_ok()
        {
            counts[partition] += 1;
        }
    }
    if counts[0] < 2 || counts[1] == 0 || counts[2] == 0 {
        return Err(DatasetPreparationError::Unavailable);
    }
    Ok((starts_at, ends_at, split))
}
