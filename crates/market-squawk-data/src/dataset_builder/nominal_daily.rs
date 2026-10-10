//! Genuine calendar associations for qualified date-only daily source bars.

#[path = "nominal_daily/current.rs"]
mod current;
pub use current::NominalDailyCurrentSource;

use super::{
    ComponentKind, DatasetBuildError, DatasetBuildPurpose, DatasetExample, DatasetStudyPolicy,
    FeatureLabelComponentInput,
};
use crate::{
    CompleteMarketBarHistoryCursor, CompleteMarketBarHistoryOutput, DatasetManifestRef,
    ObservationFamilyKey, PointInTimeCandidate, Sha256Digest,
};
use market_squawk_domain::{
    CalendarDate, EvidenceDigest, HistoricalStudyBasis, MarketBarAdjustment, MarketBarObservation,
    ResearchObservation, Timestamp,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{io, time::Instant};
use tokio_util::sync::CancellationToken;

/// Qualified history reflects the provider mapping retained at its observed revision.
/// It does not assert that the mapping was known at an earlier historical decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketBarHistoryIdentityQualification {
    ProviderAssignedHistoryAtObservedRevision,
}

/// One original daily observation associated with a genuine named session.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionRow {
    native_date: CalendarDate,
    opens_at: Timestamp,
    closes_at_exclusive: Timestamp,
    original_bar_sha256: [u8; 32],
}

/// Original history and calendar evidence retained by one qualified price epoch.
/// Deserialization produces inert evidence; only an authenticated dataset can issue an epoch.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamedSessionDailyOrigin {
    identity_qualification: MarketBarHistoryIdentityQualification,
    current: SessionRow,
    prior: SessionRow,
    #[serde(deserialize_with = "required_option")]
    terminal: Option<SessionRow>,
    history_parent_sha256: [u8; 32],
    history_origin_sha256: [u8; 32],
    history_publication_receipt_sha256: [u8; 32],
    history_content_sha256: [u8; 32],
    history_read_sha256: [u8; 32],
    mapping_digest: EvidenceDigest,
    source_replay_digest: EvidenceDigest,
    capture_receipt_digest: EvidenceDigest,
    calendar_origin_content_digest: EvidenceDigest,
    calendar_capture_binding_digest: EvidenceDigest,
    #[serde(deserialize_with = "required_option")]
    calendar_component_digest: Option<EvidenceDigest>,
    calendar_received_at: Timestamp,
    calendar_published_at: Timestamp,
    history_published_at: Timestamp,
    history_knowledge_cutoff: Timestamp,
}

impl NamedSessionDailyOrigin {
    pub const fn identity_qualification(&self) -> MarketBarHistoryIdentityQualification {
        self.identity_qualification
    }
    pub const fn native_date(&self) -> CalendarDate {
        self.current.native_date
    }
    pub const fn opens_at(&self) -> Timestamp {
        self.current.opens_at
    }
    pub const fn closes_at_exclusive(&self) -> Timestamp {
        self.current.closes_at_exclusive
    }
    pub const fn mapping_digest(&self) -> EvidenceDigest {
        self.mapping_digest
    }
    pub const fn source_replay_digest(&self) -> EvidenceDigest {
        self.source_replay_digest
    }
    pub const fn calendar_origin_content_digest(&self) -> EvidenceDigest {
        self.calendar_origin_content_digest
    }
    pub const fn calendar_capture_binding_digest(&self) -> EvidenceDigest {
        self.calendar_capture_binding_digest
    }
    pub const fn calendar_component_digest(&self) -> Option<EvidenceDigest> {
        self.calendar_component_digest
    }
    pub const fn history_content_digest(&self) -> Sha256Digest {
        Sha256Digest::new(self.history_content_sha256)
    }
    pub const fn history_read_digest(&self) -> Sha256Digest {
        Sha256Digest::new(self.history_read_sha256)
    }
    pub fn target_native_date(&self) -> Option<CalendarDate> {
        self.terminal.as_ref().map(|v| v.native_date)
    }

    pub(super) fn validate(
        &self,
        bar: &MarketBarObservation,
        manifest: &DatasetManifestRef,
        study: DatasetStudyPolicy,
        target_at: Timestamp,
    ) -> Result<(), DatasetBuildError> {
        if (study.basis() != HistoricalStudyBasis::RetrospectiveFrozenSnapshot
            && !(study.basis() == HistoricalStudyBasis::HistoricalAsKnown
                && study.purpose() == DatasetBuildPurpose::StudyInputs))
            || self.history_knowledge_cutoff != study.snapshot_as_of()
            || study.target_horizon().exact_elapsed().is_none()
            || self.identity_qualification
                != MarketBarHistoryIdentityQualification::ProviderAssignedHistoryAtObservedRevision
            || self.history_parent_sha256 != manifest.content_hash().bytes()
            || self.calendar_received_at > self.calendar_published_at
            || self.calendar_published_at > study.snapshot_as_of()
            || self.history_published_at > study.snapshot_as_of()
            || self.current.native_date <= self.prior.native_date
            || self.current.closes_at_exclusive <= self.prior.closes_at_exclusive
            || self.current.closes_at_exclusive >= target_at
            || self.current.closes_at_exclusive > study.snapshot_as_of()
            || (study.purpose() == DatasetBuildPurpose::Training && self.terminal.is_none())
            || (study.purpose() == DatasetBuildPurpose::StudyInputs && self.terminal.is_some())
            || self.terminal.as_ref().is_some_and(|row| {
                row.native_date <= self.current.native_date || row.closes_at_exclusive != target_at
            })
            || [
                self.history_parent_sha256,
                self.history_origin_sha256,
                self.history_publication_receipt_sha256,
                self.history_content_sha256,
                self.history_read_sha256,
                self.mapping_digest.bytes(),
                self.source_replay_digest.bytes(),
                self.capture_receipt_digest.bytes(),
                self.calendar_origin_content_digest.bytes(),
                self.calendar_capture_binding_digest.bytes(),
            ]
            .contains(&[0; 32])
            || self
                .calendar_component_digest
                .is_some_and(|digest| digest.bytes() == [0; 32])
        {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        }
        for row in [&self.prior, &self.current]
            .into_iter()
            .chain(self.terminal.iter())
        {
            if row.opens_at >= row.closes_at_exclusive || row.original_bar_sha256 == [0; 32] {
                return Err(DatasetBuildError::ComponentEvidenceMismatch);
            }
        }
        if self.selected_close(bar, manifest)? != self.closes_at_exclusive() {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        }
        Ok(())
    }

    /// Uses only the already admitted original source pool. Native dates stay native;
    /// the independent original calendar supplies the economic application boundary.
    pub(super) fn validate_price_plan(
        &self,
        manifest: &DatasetManifestRef,
        instrument: market_squawk_domain::InstrumentId,
        knowledge: Timestamp,
        plan: &crate::CorporateActionPlan,
    ) -> Result<(), DatasetBuildError> {
        let coverage = plan
            .source_split_admission()
            .ok_or(DatasetBuildError::ComponentAdjustmentMismatch)?;
        let terminal = self.terminal.as_ref().unwrap_or(&self.current);
        if knowledge != self.history_knowledge_cutoff
            || knowledge != coverage.knowledge_cutoff()
            || !coverage.instruments().contains(&instrument)
            || !coverage.history_input_manifests().contains(manifest)
            || coverage.interval().0 > self.prior.native_date
            || coverage.interval().1 < terminal.native_date
            || coverage
                .application_starts_at(instrument)
                .is_none_or(|start| start > self.prior.closes_at_exclusive)
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
        let date = bar
            .time_semantics()
            .nominal_daily_date()
            .ok_or(DatasetBuildError::ComponentEvidenceMismatch)?
            .date();
        let row = [&self.prior, &self.current]
            .into_iter()
            .chain(self.terminal.iter())
            .find(|row| row.native_date == date)
            .ok_or(DatasetBuildError::ComponentEvidenceMismatch)?;
        if bar.completed_at().is_some()
            || bar.adjustment() != MarketBarAdjustment::Raw
            || bar.context().time().effective().calendar_date_value() != Some(date)
            || manifest.content_hash().bytes() != self.history_parent_sha256
            || bar_digest(bar)?.bytes() != row.original_bar_sha256
        {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        }
        Ok(row.closes_at_exclusive)
    }

    /// Checks inert coordinates; decoding this value never grants source authority.
    pub fn matches_origin_bar(
        &self,
        bar: &MarketBarObservation,
        manifest: &DatasetManifestRef,
        observed_through: Timestamp,
        knowledge_cutoff: Timestamp,
    ) -> bool {
        observed_through == self.current.closes_at_exclusive
            && self.history_knowledge_cutoff == knowledge_cutoff
            && self.history_published_at <= knowledge_cutoff
            && self.calendar_received_at <= self.calendar_published_at
            && self.calendar_published_at <= knowledge_cutoff
            && self.current.closes_at_exclusive <= knowledge_cutoff
            && bar
                .context()
                .provenance()
                .availability()
                .conservative_available_at()
                .is_some_and(|known| known <= knowledge_cutoff)
            && bar.context().provenance().received_at() <= knowledge_cutoff
            && bar.context().provenance().ingested_at() <= knowledge_cutoff
            && self
                .selected_close(bar, manifest)
                .is_ok_and(|close| close == observed_through)
    }

    /// Canonical commitment to inert original history and calendar coordinates.
    /// This digest cannot issue or reopen a source capability.
    pub fn evidence_digest(&self) -> Sha256Digest {
        self.digest()
    }

    pub(super) fn digest(&self) -> Sha256Digest {
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/named-session-nominal-daily-origin/v1");
        hash.update([2]); // ProviderAssignedHistoryAtObservedRevision, closed by validation.
        for row in [
            Some(&self.prior),
            Some(&self.current),
            self.terminal.as_ref(),
        ] {
            if let Some(row) = row {
                hash.update([1]);
                hash.update(row.native_date.days_since_unix_epoch().to_be_bytes());
                hash.update(row.opens_at.unix_nanos().to_be_bytes());
                hash.update(row.closes_at_exclusive.unix_nanos().to_be_bytes());
                hash.update(row.original_bar_sha256);
            } else {
                hash.update([0]);
            }
        }
        for digest in [
            self.history_parent_sha256,
            self.history_origin_sha256,
            self.history_publication_receipt_sha256,
            self.history_content_sha256,
            self.history_read_sha256,
        ] {
            hash.update(digest);
        }
        for digest in [
            Some(self.mapping_digest),
            Some(self.source_replay_digest),
            Some(self.capture_receipt_digest),
            Some(self.calendar_origin_content_digest),
            Some(self.calendar_capture_binding_digest),
            self.calendar_component_digest,
        ] {
            if let Some(digest) = digest {
                hash.update([
                    1,
                    match digest.algorithm() {
                        market_squawk_domain::DigestAlgorithm::Sha256 => 1,
                        market_squawk_domain::DigestAlgorithm::Blake3 => 2,
                    },
                ]);
                hash.update(digest.bytes());
            } else {
                hash.update([0]);
            }
        }
        for clock in [
            self.calendar_received_at,
            self.calendar_published_at,
            self.history_published_at,
            self.history_knowledge_cutoff,
        ] {
            hash.update(clock.unix_nanos().to_be_bytes());
        }
        Sha256Digest::new(hash.finalize().into())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct NominalDailyExampleSource {
    pub(super) origin: NamedSessionDailyOrigin,
    pub(super) manifest: DatasetManifestRef,
}

/// Borrows one of the two sealed history representations for the same dataset validation.
pub(super) enum DatasetHistory<'a> {
    Memory(&'a CompleteMarketBarHistoryOutput),
    Indexed(&'a CompleteMarketBarHistoryCursor),
}
impl DatasetHistory<'_> {
    pub(super) fn selection(&self) -> &crate::CompleteMarketBarHistorySelection {
        match self {
            Self::Memory(history) => history.selection(),
            Self::Indexed(history) => history.selection(),
        }
    }
    pub(super) fn read_receipt(&self) -> &crate::CompleteMarketBarHistoryReadReceipt {
        match self {
            Self::Memory(history) => history.read_receipt(),
            Self::Indexed(history) => history.read_receipt(),
        }
    }
    fn native_sessions(&self) -> Option<&crate::RetainedHistoryNativeSessions> {
        match self {
            Self::Memory(history) => history.native_sessions(),
            Self::Indexed(history) => history.native_sessions(),
        }
    }
    pub(super) fn selected_bar(
        &self,
        family: &ObservationFamilyKey,
    ) -> Result<MarketBarObservation, DatasetBuildError> {
        let ObservationFamilyKey::MarketBar { effective, .. } = family else {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        };
        let bar = match self {
            Self::Memory(history) => {
                let index = if let Some(timestamp) = effective.exact_timestamp() {
                    history
                        .bars()
                        .binary_search_by_key(&Some(timestamp), |bar| {
                            bar.context().time().effective().exact_timestamp()
                        })
                } else if let Some(date) = effective.calendar_date_value() {
                    history.bars().binary_search_by_key(&Some(date), |bar| {
                        bar.time_semantics()
                            .nominal_daily_date()
                            .map(|value| value.date())
                    })
                } else {
                    return Err(DatasetBuildError::ComponentEvidenceMismatch);
                }
                .map_err(|_| DatasetBuildError::ComponentEvidenceMismatch)?;
                history.bars()[index].clone()
            }
            Self::Indexed(history) => history
                .bar_at_coordinate(effective)
                .map_err(map_history_read_error)?
                .ok_or(DatasetBuildError::ComponentEvidenceMismatch)?,
        };
        let candidate = PointInTimeCandidate::new(
            ResearchObservation::MarketBar(bar.clone()),
            self.selection().pinned().manifest().clone(),
        );
        if candidate
            .family_key()
            .map_err(|_| DatasetBuildError::ComponentEvidenceMismatch)?
            != *family
        {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        }
        Ok(bar)
    }
}

impl CompleteMarketBarHistoryOutput {
    /// Derives a retrospective dataset example from this sealed original history.
    pub fn try_nominal_daily_dataset_example(
        &self,
        example_id: &str,
        native_date: CalendarDate,
        study: DatasetStudyPolicy,
        components: Vec<FeatureLabelComponentInput>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<DatasetExample, DatasetBuildError> {
        DatasetHistory::Memory(self).try_nominal_daily_dataset_example(
            example_id,
            native_date,
            study,
            components,
            deadline,
            cancellation,
        )
    }
}
impl CompleteMarketBarHistoryCursor {
    /// Derives the same retrospective example by reading only its original indexed bars.
    pub fn try_nominal_daily_dataset_example(
        &self,
        example_id: &str,
        native_date: CalendarDate,
        study: DatasetStudyPolicy,
        components: Vec<FeatureLabelComponentInput>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<DatasetExample, DatasetBuildError> {
        DatasetHistory::Indexed(self).try_nominal_daily_dataset_example(
            example_id,
            native_date,
            study,
            components,
            deadline,
            cancellation,
        )
    }
}
impl DatasetHistory<'_> {
    /// Derives a retrospective decision from an authentic date/session association. The provider
    /// observation retains its original date, availability and absent completion instant.
    pub fn try_nominal_daily_dataset_example(
        &self,
        example_id: &str,
        native_date: CalendarDate,
        study: DatasetStudyPolicy,
        components: Vec<FeatureLabelComponentInput>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<DatasetExample, DatasetBuildError> {
        check_control(deadline, cancellation)?;
        let receipt = self.selection().receipt();
        let sessions = self
            .native_sessions()
            .ok_or(DatasetBuildError::ComponentEvidenceMismatch)?;
        let horizon = study
            .target_horizon()
            .exact_elapsed()
            .ok_or(DatasetBuildError::InvalidRequest)?;
        if study.basis() != HistoricalStudyBasis::RetrospectiveFrozenSnapshot
            || receipt.requested_dates().is_none()
            || !receipt.current_research_eligible()
            || self.read_receipt().knowledge_cutoff() != study.snapshot_as_of()
            || receipt.adjustment() != MarketBarAdjustment::Raw
            || receipt.knowledge_clocks().0 > study.snapshot_as_of()
            || receipt.knowledge_clocks().1 > study.snapshot_as_of()
            || receipt.knowledge_clocks().2 > study.snapshot_as_of()
            || receipt.published_at() > study.snapshot_as_of()
            || sessions.published_at() > study.snapshot_as_of()
            || components.is_empty()
            || components.len() > 1_024
        {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        }
        let feature = unique_component(&components, "research.price-return")?;
        if feature.spec().kind() != ComponentKind::Feature || feature.selectors().len() != 2 {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        }
        let left = self.selected_nominal_bar(feature.selectors()[0].family())?;
        let right = self.selected_nominal_bar(feature.selectors()[1].family())?;
        let (prior, current) =
            if left.context().time().effective() < right.context().time().effective() {
                (left, right)
            } else {
                (right, left)
            };
        let prior = self.nominal_session_row(&prior)?;
        let current_bar = current;
        let current = self.nominal_session_row(&current_bar)?;
        if current.native_date != native_date {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        }
        let target_at = Timestamp::from_unix_nanos(
            current
                .closes_at_exclusive
                .unix_nanos()
                .checked_add(
                    i64::try_from(horizon.as_nanos())
                        .map_err(|_| DatasetBuildError::InvalidRequest)?,
                )
                .ok_or(DatasetBuildError::InvalidRequest)?,
        );
        let decision_at = Timestamp::from_unix_nanos(
            current
                .closes_at_exclusive
                .unix_nanos()
                .checked_add(
                    i64::try_from(
                        study
                            .decision_lag()
                            .ok_or(DatasetBuildError::InvalidRequest)?
                            .as_nanos(),
                    )
                    .map_err(|_| DatasetBuildError::InvalidRequest)?,
                )
                .ok_or(DatasetBuildError::InvalidRequest)?,
        );
        let terminal = if study.purpose() == DatasetBuildPurpose::Training {
            let label = unique_component(&components, "research.fixed-horizon-forward-return")?;
            let [selector] = label.selectors() else {
                return Err(DatasetBuildError::ComponentEvidenceMismatch);
            };
            Some(self.nominal_session_row(&self.selected_nominal_bar(selector.family())?)?)
        } else {
            None
        };
        let source = self.nominal_source(prior, current, terminal)?;
        source
            .origin
            .validate(&current_bar, &source.manifest, study, target_at)?;
        check_control(deadline, cancellation)?;
        DatasetExample::from_nominal_daily(
            example_id,
            receipt.instrument_id(),
            study,
            decision_at,
            target_at,
            components,
            source,
        )
    }

    fn nominal_source(
        &self,
        prior: SessionRow,
        current: SessionRow,
        terminal: Option<SessionRow>,
    ) -> Result<NominalDailyExampleSource, DatasetBuildError> {
        let receipt = self.selection().receipt();
        let sessions = self
            .native_sessions()
            .ok_or(DatasetBuildError::ComponentEvidenceMismatch)?;
        Ok(NominalDailyExampleSource {
            origin: NamedSessionDailyOrigin {
                identity_qualification:
                    MarketBarHistoryIdentityQualification::ProviderAssignedHistoryAtObservedRevision,
                current,
                prior,
                terminal,
                history_parent_sha256: self.selection().pinned().manifest().content_hash().bytes(),
                history_origin_sha256: self.read_receipt().origin_manifest().content_hash().bytes(),
                history_publication_receipt_sha256: self
                    .read_receipt()
                    .publication_receipt_digest()
                    .bytes(),
                history_content_sha256: self.read_receipt().history_content_digest().bytes(),
                history_read_sha256: self.read_receipt().result_digest().bytes(),
                mapping_digest: sessions.mapping_digest(),
                source_replay_digest: sessions.source_replay_digest(),
                capture_receipt_digest: sessions.capture_receipt_digest(),
                calendar_origin_content_digest: sessions.calendar_origin_content_digest(),
                calendar_capture_binding_digest: sessions.calendar_capture_binding_digest(),
                calendar_component_digest: sessions.calendar_component_digest(),
                calendar_received_at: sessions.received_at(),
                calendar_published_at: sessions.published_at(),
                history_published_at: receipt.published_at(),
                history_knowledge_cutoff: self.read_receipt().knowledge_cutoff(),
            },
            manifest: self.selection().pinned().manifest().clone(),
        })
    }

    fn selected_nominal_bar(
        &self,
        family: &ObservationFamilyKey,
    ) -> Result<MarketBarObservation, DatasetBuildError> {
        let ObservationFamilyKey::MarketBar { effective, .. } = family else {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        };
        effective
            .calendar_date_value()
            .ok_or(DatasetBuildError::ComponentEvidenceMismatch)?;
        self.selected_bar(family)
    }

    fn nominal_session_row(
        &self,
        bar: &MarketBarObservation,
    ) -> Result<SessionRow, DatasetBuildError> {
        let native_date = bar
            .time_semantics()
            .nominal_daily_date()
            .ok_or(DatasetBuildError::ComponentEvidenceMismatch)?
            .date();
        let sessions = self
            .native_sessions()
            .ok_or(DatasetBuildError::ComponentEvidenceMismatch)?
            .sessions();
        let session = sessions
            .find_date(native_date)
            .map_err(map_history_read_error)?
            .ok_or(DatasetBuildError::ComponentEvidenceMismatch)?;
        if session.provider_timestamp().is_some()
            || session.provider_period().is_some()
            || !session.bar_present()
            || bar.completed_at().is_some()
        {
            return Err(DatasetBuildError::ComponentEvidenceMismatch);
        }
        Ok(SessionRow {
            native_date,
            opens_at: session.opens_at(),
            closes_at_exclusive: session.closes_at_exclusive(),
            original_bar_sha256: bar_digest(bar)?.bytes(),
        })
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

fn bar_digest(bar: &MarketBarObservation) -> Result<Sha256Digest, DatasetBuildError> {
    struct Writer {
        hash: Sha256,
        bytes: usize,
    }
    impl io::Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.bytes = self
                .bytes
                .checked_add(bytes.len())
                .filter(|value| *value <= super::epoch::MAX_INPUT_EPOCH_BYTES)
                .ok_or_else(|| io::Error::other("original daily bar exceeds epoch bound"))?;
            self.hash.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Writer {
        hash: Sha256::new(),
        bytes: 0,
    };
    writer
        .hash
        .update(b"market-squawk/original-nominal-daily-bar/v1");
    serde_json::to_writer(&mut writer, bar)
        .map_err(|_| DatasetBuildError::ComponentEvidenceMismatch)?;
    Ok(Sha256Digest::new(writer.hash.finalize().into()))
}

fn required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::deserialize(deserializer)
}

pub(super) fn map_history_read_error(error: crate::AnalyticalReadError) -> DatasetBuildError {
    match error {
        crate::AnalyticalReadError::Query(crate::query::QueryError::Cancelled) => {
            DatasetBuildError::Cancelled
        }
        crate::AnalyticalReadError::Query(crate::query::QueryError::DeadlineExceeded) => {
            DatasetBuildError::DeadlineExceeded
        }
        _ => DatasetBuildError::ComponentEvidenceMismatch,
    }
}
