//! Original completed timestamp histories retain their own source and native-calendar authority.

use super::{
    ComponentKind, DatasetBuildError, DatasetBuildPurpose, DatasetExample, DatasetStudyPolicy,
    FeatureLabelComponentInput,
};
use crate::{
    CompleteMarketBarHistoryCursor, CompleteMarketBarHistoryOutput,
    CompletedOrdinaryHistoryEvidence, CorporateActionPlan, DatasetManifestRef,
    ObservationFamilyKey, Sha256Digest,
};
use market_squawk_domain::{
    HistoricalStudyBasis, InstrumentId, MarketBarObservation, ResearchTemporalCoordinate, Timestamp,
};
use sha2::{Digest as _, Sha256};
use std::time::Instant;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct TimestampHistoryExampleSource {
    history: CompletedOrdinaryHistoryEvidence,
    study: DatasetStudyPolicy,
    prior: (Timestamp, Sha256Digest),
    current: (Timestamp, Sha256Digest),
    terminal: Option<(Timestamp, Sha256Digest)>,
}
impl TimestampHistoryExampleSource {
    pub(super) const fn study(&self) -> DatasetStudyPolicy {
        self.study
    }
    pub(super) fn manifest(&self) -> &DatasetManifestRef {
        self.history.manifest()
    }
    pub(super) fn validate_price_plan(
        &self,
        instrument: InstrumentId,
        knowledge: Timestamp,
        plan: &CorporateActionPlan,
    ) -> Result<(), DatasetBuildError> {
        let coverage = plan
            .source_split_admission()
            .ok_or(DatasetBuildError::ComponentAdjustmentMismatch)?;
        let terminal = self.terminal.unwrap_or(self.current).0;
        if !coverage.admits_completed_history(&self.history)
            || !coverage.covers_timestamp_history_span(
                instrument,
                self.manifest(),
                knowledge,
                self.prior.0,
                terminal,
            )
            || plan.valuation_cutoff() < terminal
        {
            return Err(DatasetBuildError::ComponentAdjustmentMismatch);
        }
        Ok(())
    }
    pub(super) fn selected_close(
        &self,
        bar: &MarketBarObservation,
        manifest: &DatasetManifestRef,
    ) -> Result<Timestamp, DatasetBuildError> {
        let close = bar
            .completed_at()
            .ok_or(DatasetBuildError::ComponentEvidenceMismatch)?;
        if manifest != self.manifest()
            || ![Some(self.prior), Some(self.current), self.terminal]
                .into_iter()
                .flatten()
                .any(|row| {
                    row.0 == close && original_bar_digest(bar).is_ok_and(|digest| digest == row.1)
                })
        {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        }
        Ok(close)
    }
    pub(super) fn retained_bytes(&self) -> usize {
        [self.history.manifest(), self.history.origin_manifest()]
            .into_iter()
            .map(|manifest| manifest.dataset_id().as_str().len() + manifest.schema().name().len())
            .sum()
    }
    pub(super) fn digest(&self) -> Sha256Digest {
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/timestamp-history-example/v1");
        hash.update(self.history.evidence_digest().bytes());
        for row in [Some(self.prior), Some(self.current), self.terminal] {
            if let Some((at, digest)) = row {
                hash.update([1]);
                hash.update(at.unix_nanos().to_be_bytes());
                hash.update(digest.bytes());
            } else {
                hash.update([0]);
            }
        }
        Sha256Digest::new(hash.finalize().into())
    }
}

use super::nominal_daily::DatasetHistory;

impl CompleteMarketBarHistoryOutput {
    /// Constructs an example using the shared original-history validation.
    #[allow(clippy::too_many_arguments)]
    pub fn try_timestamp_history_dataset_example(
        &self,
        example_id: &str,
        study: DatasetStudyPolicy,
        decision_at: Timestamp,
        target_at: Timestamp,
        components: Vec<FeatureLabelComponentInput>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<DatasetExample, DatasetBuildError> {
        DatasetHistory::Memory(self).try_timestamp_history_dataset_example(
            example_id,
            study,
            decision_at,
            target_at,
            components,
            deadline,
            cancellation,
        )
    }
}
impl CompleteMarketBarHistoryCursor {
    /// Constructs the same example using its exact original indexed rows.
    #[allow(clippy::too_many_arguments)]
    pub fn try_timestamp_history_dataset_example(
        &self,
        example_id: &str,
        study: DatasetStudyPolicy,
        decision_at: Timestamp,
        target_at: Timestamp,
        components: Vec<FeatureLabelComponentInput>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<DatasetExample, DatasetBuildError> {
        DatasetHistory::Indexed(self).try_timestamp_history_dataset_example(
            example_id,
            study,
            decision_at,
            target_at,
            components,
            deadline,
            cancellation,
        )
    }
}
impl DatasetHistory<'_> {
    /// Constructs a frozen retrospective example only from original timestamp bars and native
    /// session replay. Caller cutoffs are checked against those rows and the study horizon.
    #[allow(clippy::too_many_arguments)]
    pub fn try_timestamp_history_dataset_example(
        &self,
        example_id: &str,
        study: DatasetStudyPolicy,
        decision_at: Timestamp,
        target_at: Timestamp,
        components: Vec<FeatureLabelComponentInput>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<DatasetExample, DatasetBuildError> {
        check_control(deadline, cancellation)?;
        if study.basis() != HistoricalStudyBasis::RetrospectiveFrozenSnapshot
            || self.read_receipt().knowledge_cutoff() != study.snapshot_as_of()
            || components.is_empty()
            || components.len() > 1_024
        {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        }
        let history = match self {
            Self::Memory(history) => CompletedOrdinaryHistoryEvidence::try_from_history(history)?,
            Self::Indexed(history) => CompletedOrdinaryHistoryEvidence::try_from_cursor(history)?,
        };
        let feature = unique_component(&components, "research.price-return")?;
        if feature.spec().kind() != ComponentKind::Feature || feature.selectors().len() != 2 {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        }
        let mut rows = [
            self.timestamp_history_row(feature.selectors()[0].family())?,
            self.timestamp_history_row(feature.selectors()[1].family())?,
        ];
        rows.sort_unstable_by_key(|row| row.0);
        if rows[0].0 >= rows[1].0 {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        }
        let terminal = if study.purpose() == DatasetBuildPurpose::Training {
            let label = unique_component(&components, "research.fixed-horizon-forward-return")?;
            let [selector] = label.selectors() else {
                return Err(DatasetBuildError::ComponentEvidenceMismatch);
            };
            if label.spec().kind() != ComponentKind::Label {
                return Err(DatasetBuildError::ComponentEvidenceMismatch);
            }
            let row = self.timestamp_history_row(selector.family())?;
            if row.0 != target_at || row.0 <= rows[1].0 {
                return Err(DatasetBuildError::ComponentEvidenceMismatch);
            }
            Some(row)
        } else {
            None
        };
        let source = TimestampHistoryExampleSource {
            history,
            study,
            prior: rows[0],
            current: rows[1],
            terminal,
        };
        let mut example = DatasetExample::try_new_with_temporal_cutoffs(
            example_id,
            self.selection().receipt().instrument_id(),
            study.snapshot_as_of(),
            (study.purpose() == DatasetBuildPurpose::Training).then_some(study.snapshot_as_of()),
            decision_at,
            ResearchTemporalCoordinate::exact(rows[1].0),
            ResearchTemporalCoordinate::exact(target_at),
            components,
        )?;
        example.attach_timestamp_history(source)?;
        study.validate_example(&example)?;
        check_control(deadline, cancellation)?;
        Ok(example)
    }
    fn timestamp_history_row(
        &self,
        family: &ObservationFamilyKey,
    ) -> Result<(Timestamp, Sha256Digest), DatasetBuildError> {
        let ObservationFamilyKey::MarketBar { effective, .. } = family else {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        };
        effective
            .exact_timestamp()
            .ok_or(DatasetBuildError::ComponentEvidenceMismatch)?;
        let bar = self.selected_bar(family)?;
        Ok((
            bar.completed_at()
                .ok_or(DatasetBuildError::ComponentEvidenceMismatch)?,
            original_bar_digest(&bar)?,
        ))
    }
}
fn unique_component<'a>(
    components: &'a [FeatureLabelComponentInput],
    name: &str,
) -> Result<&'a FeatureLabelComponentInput, DatasetBuildError> {
    let mut values = components
        .iter()
        .filter(|value| value.spec().name() == name);
    let value = values
        .next()
        .ok_or(DatasetBuildError::ComponentEvidenceMismatch)?;
    if values.next().is_some() {
        return Err(DatasetBuildError::ComponentEvidenceMismatch);
    }
    Ok(value)
}
fn original_bar_digest(bar: &MarketBarObservation) -> Result<Sha256Digest, DatasetBuildError> {
    struct Writer {
        hash: Sha256,
        bytes: usize,
    }
    impl std::io::Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.bytes = self
                .bytes
                .checked_add(bytes.len())
                .filter(|count| *count <= super::epoch::MAX_INPUT_EPOCH_BYTES)
                .ok_or_else(|| {
                    std::io::Error::other("original timestamp bar exceeds epoch bound")
                })?;
            self.hash.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Writer {
        hash: Sha256::new(),
        bytes: 0,
    };
    writer
        .hash
        .update(b"market-squawk/original-timestamp-history-bar/v1");
    serde_json::to_writer(&mut writer, bar)
        .map_err(|_| DatasetBuildError::ComponentEvidenceMismatch)?;
    Ok(Sha256Digest::new(writer.hash.finalize().into()))
}

fn check_control(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), DatasetBuildError> {
    if cancellation.is_cancelled() {
        Err(DatasetBuildError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(DatasetBuildError::DeadlineExceeded)
    } else {
        Ok(())
    }
}
