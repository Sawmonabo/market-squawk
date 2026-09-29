//! Compact current input issued only from the original complete native history read.

use super::*;

/// Two original raw observations and their authenticated native-session coordinates.
/// No public constructor or deserializer can issue this source capability.
#[derive(Debug)]
pub struct NominalDailyCurrentSource {
    source: NominalDailyExampleSource,
    prior: MarketBarObservation,
    current: MarketBarObservation,
    source_cutoff: Timestamp,
    retained_bytes: usize,
}

impl NominalDailyCurrentSource {
    pub const fn prior_bar(&self) -> &MarketBarObservation {
        &self.prior
    }
    pub const fn current_bar(&self) -> &MarketBarObservation {
        &self.current
    }
    pub const fn prior_close(&self) -> Timestamp {
        self.source.origin.prior.closes_at_exclusive
    }
    pub const fn current_close(&self) -> Timestamp {
        self.source.origin.current.closes_at_exclusive
    }
    pub const fn source_cutoff(&self) -> Timestamp {
        self.source_cutoff
    }
    pub const fn manifest(&self) -> &DatasetManifestRef {
        &self.source.manifest
    }
    pub const fn named_session_origin(&self) -> &NamedSessionDailyOrigin {
        &self.source.origin
    }
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    /// Creates current label-free inputs at the retained acquisition cutoff. A source-native
    /// civil date remains a date; the separate target origin is its authenticated session close.
    pub fn try_dataset_example(
        &self,
        example_id: &str,
        study: DatasetStudyPolicy,
        components: Vec<FeatureLabelComponentInput>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<DatasetExample, DatasetBuildError> {
        check_control(deadline, cancellation)?;
        if study.basis() != HistoricalStudyBasis::HistoricalAsKnown
            || study.purpose() != DatasetBuildPurpose::StudyInputs
            || study.snapshot_as_of() != self.source_cutoff
            || study.decision_lag().is_some()
            || components.is_empty()
            || components.len() > 1_024
            || components
                .iter()
                .any(|value| value.spec().kind() != ComponentKind::Feature)
        {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        }
        let feature = unique_component(&components, "research.price-return")?;
        let [left, right] = feature.selectors() else {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        };
        let prior = PointInTimeCandidate::new(
            ResearchObservation::MarketBar(self.prior.clone()),
            self.source.manifest.clone(),
        )
        .family_key()
        .map_err(|_| DatasetBuildError::ComponentEvidenceMismatch)?;
        let current = PointInTimeCandidate::new(
            ResearchObservation::MarketBar(self.current.clone()),
            self.source.manifest.clone(),
        )
        .family_key()
        .map_err(|_| DatasetBuildError::ComponentEvidenceMismatch)?;
        if !((left.family() == &prior && right.family() == &current)
            || (left.family() == &current && right.family() == &prior))
        {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        }
        let horizon = study
            .target_horizon()
            .exact_elapsed()
            .ok_or(DatasetBuildError::InvalidRequest)?;
        let target_at = Timestamp::from_unix_nanos(
            self.current_close()
                .unix_nanos()
                .checked_add(
                    i64::try_from(horizon.as_nanos())
                        .map_err(|_| DatasetBuildError::InvalidRequest)?,
                )
                .ok_or(DatasetBuildError::InvalidRequest)?,
        );
        self.source
            .origin
            .validate(&self.current, &self.source.manifest, study, target_at)?;
        check_control(deadline, cancellation)?;
        DatasetExample::from_nominal_daily(
            example_id,
            self.current
                .context()
                .provenance()
                .instrument_id()
                .ok_or(DatasetBuildError::ComponentEvidenceMismatch)?,
            study,
            self.source_cutoff,
            target_at,
            components,
            self.source.clone(),
        )
    }
}

impl CompleteMarketBarHistoryOutput {
    /// Retains only the final two completed native bars, not another full source history.
    /// Local availability remains the actual saved cutoff and never becomes historical knowledge.
    pub fn try_current_nominal_daily_source(
        &self,
        source_cutoff: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<NominalDailyCurrentSource, DatasetBuildError> {
        check_control(deadline, cancellation)?;
        let receipt = self.selection().receipt();
        let sessions = self
            .native_sessions()
            .ok_or(DatasetBuildError::ComponentEvidenceMismatch)?;
        let clocks = receipt.knowledge_clocks();
        if self.read_receipt().knowledge_cutoff() != source_cutoff
            || !receipt.current_research_eligible()
            || receipt.adjustment() != MarketBarAdjustment::Raw
            || receipt.date_windows().is_none()
            || receipt.published_at() > source_cutoff
            || sessions.published_at() > source_cutoff
            || [clocks.0, clocks.1, clocks.2]
                .iter()
                .any(|clock| *clock > source_cutoff)
            || self.bars().len() != sessions.sessions().len()
        {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        }
        let mut selected = [None, None];
        let mut count = 0;
        for (index, bar) in self.bars().iter().enumerate().rev() {
            check_control(deadline, cancellation)?;
            let row = self.nominal_session_row(bar)?;
            let known = bar
                .context()
                .provenance()
                .availability()
                .conservative_available_at()
                .ok_or(DatasetBuildError::ComponentEvidenceMismatch)?;
            if row.closes_at_exclusive > source_cutoff {
                continue;
            }
            if known > source_cutoff
                || bar.context().provenance().received_at() > source_cutoff
                || bar.context().provenance().ingested_at() > source_cutoff
                || bar.adjustment() != MarketBarAdjustment::Raw
                || bar.context().provenance().instrument_id() != Some(receipt.instrument_id())
                || bar.currency() != receipt.currency()
            {
                return Err(DatasetBuildError::ComponentEvidenceMismatch);
            }
            selected[count] = Some((index, bar, row));
            count += 1;
            if count == 2 {
                break;
            }
        }
        let [
            Some((current_index, current, current_row)),
            Some((prior_index, prior, prior_row)),
        ] = selected
        else {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        };
        if prior_index.checked_add(1) != Some(current_index)
            || prior_row.native_date >= current_row.native_date
            || prior_row.closes_at_exclusive > current_row.opens_at
        {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        }
        let source = self.nominal_source(prior_row, current_row, None)?;
        // Use the existing epoch's conservative eight-times serialization accounting. Charge
        // before cloning the two bar payloads; the complete history is never cloned here.
        let mut counter = ByteCounter(0);
        serde_json::to_writer(&mut counter, &(prior, current, &source.origin))
            .map_err(|_| DatasetBuildError::LimitExceeded)?;
        let retained_bytes = counter
            .0
            .checked_mul(8)
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<NominalDailyCurrentSource>()))
            .and_then(|bytes| bytes.checked_add(source.manifest.dataset_id().as_str().len()))
            .and_then(|bytes| bytes.checked_add(source.manifest.schema().name().len()))
            .ok_or(DatasetBuildError::LimitExceeded)?;
        check_control(deadline, cancellation)?;
        Ok(NominalDailyCurrentSource {
            source,
            prior: prior.clone(),
            current: current.clone(),
            source_cutoff,
            retained_bytes,
        })
    }
}

struct ByteCounter(usize);
impl io::Write for ByteCounter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .filter(|bytes| *bytes <= super::super::epoch::MAX_INPUT_EPOCH_BYTES)
            .ok_or_else(|| io::Error::other("current nominal source exceeds epoch bound"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
