//! Exact original raw-price selections and source-action replay, isolated from PIT features.

use super::{InputCoreWire, RecipeError, decode_digest, encode_digest, manifest::ManifestWire};
use crate::application::research::corporate_actions::SourceAppliedCorporateActionPlanReference;
use market_squawk_data::{
    CompleteMarketBarHistoryOutput, CompleteMarketBarHistoryRequest, DatasetManifestRef,
    MarketHistoryPriceSurfaceRequirement,
};
use market_squawk_domain::{
    BarTimestampBasis, CalendarDate, InstrumentId, MarketBarAdjustment, MarketBarSessionKind,
    ProviderInstrumentId, SourceIdentifier, Timestamp, VenueId,
};
use serde::{Deserialize, Serialize};

// Matches the existing BacktestDataset complete-history admission ceiling.
const MAXIMUM_DAILY_HISTORY_INSTRUMENTS: usize = 4_096;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(in crate::application::analysis::backtest::input_authority) struct DailyHistoryWire {
    admitted_at_unix_nanos: i64,
    source_action_reference: SourceAppliedCorporateActionPlanReference,
    selections: Vec<DailyHistorySelectionWire>,
}

/// Closed native request coordinates. Session closes qualify financial execution only; they
/// never replace a nominal source date with a fabricated provider timestamp.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "basis", rename_all = "snake_case", deny_unknown_fields)]
enum DailyHistoryCoordinate {
    TimestampedPeriod {
        start: Timestamp,
        end_inclusive: Timestamp,
        timestamp_basis: BarTimestampBasis,
        session_kind: MarketBarSessionKind,
    },
    NominalDailyDate {
        start: CalendarDate,
        end_inclusive: CalendarDate,
        first_session_close: Timestamp,
        last_session_close: Timestamp,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(in crate::application::analysis::backtest::input_authority) struct DailyHistorySelectionWire {
    instrument_id: InstrumentId,
    coordinate: DailyHistoryCoordinate,
    provider_instrument_id: ProviderInstrumentId,
    venue: VenueId,
    feed: SourceIdentifier,
    interval: SourceIdentifier,
    ruleset: SourceIdentifier,
    surface_requirement: MarketHistoryPriceSurfaceRequirement,
    knowledge_cutoff: Timestamp,
    selected_manifest: ManifestWire,
    origin_manifest: ManifestWire,
    selection_digest: String,
    publication_receipt_digest: String,
    result_digest: String,
    bar_count: usize,
}
impl DailyHistoryWire {
    pub(super) fn from_outputs(
        outputs: &[CompleteMarketBarHistoryOutput],
        admitted_at: Timestamp,
        source_action_reference: SourceAppliedCorporateActionPlanReference,
    ) -> Result<Self, RecipeError> {
        if outputs.is_empty() || outputs.len() > MAXIMUM_DAILY_HISTORY_INSTRUMENTS {
            return Err(RecipeError::Invalid);
        }
        let mut selections = outputs
            .iter()
            .map(DailyHistorySelectionWire::from_output)
            .collect::<Result<Vec<_>, _>>()?;
        selections.sort_unstable_by_key(|selection| selection.instrument_id);
        if selections.iter().any(|selection| {
            selection.knowledge_cutoff != source_action_reference.knowledge_cutoff()
                || selection.knowledge_cutoff > admitted_at
        }) {
            return Err(RecipeError::Invalid);
        }
        Ok(Self {
            admitted_at_unix_nanos: admitted_at.unix_nanos(),
            source_action_reference,
            selections,
        })
    }
    pub(super) fn validate(&self, core: &InputCoreWire) -> Result<(), RecipeError> {
        let end = Timestamp::from_unix_nanos(
            core.ends_at_unix_nanos
                .checked_sub(1)
                .ok_or(RecipeError::Invalid)?,
        );
        let start = Timestamp::from_unix_nanos(core.starts_at_unix_nanos);
        if self.admitted_at_unix_nanos < core.ends_at_unix_nanos
            || self.selections.len() != core.instruments.len()
            || self.selections.is_empty()
            || self.selections.len() > MAXIMUM_DAILY_HISTORY_INSTRUMENTS
            || self
                .selections
                .iter()
                .zip(&core.instruments)
                .any(|(selection, instrument)| {
                    selection.instrument_id != *instrument
                        || !selection.covers(start, end)
                        || selection.knowledge_cutoff
                            != self.source_action_reference.knowledge_cutoff()
                        || selection.knowledge_cutoff > self.admitted_at()
                })
        {
            return Err(RecipeError::Invalid);
        }
        for selection in &self.selections {
            selection.validate()?;
        }
        let total = self
            .selections
            .iter()
            .try_fold(0_usize, |total, selection| {
                total
                    .checked_add(selection.bar_count)
                    .ok_or(RecipeError::ResourceExhausted)
            })?;
        if total > core.limits.into_input()?.max_observations {
            return Err(RecipeError::ResourceExhausted);
        }
        let actions = core.corporate_actions()?.ok_or(RecipeError::Invalid)?;
        if actions.valuation_cutoff().unix_nanos() != core.ends_at_unix_nanos
            || actions.knowledge_cutoff() != self.source_action_reference.knowledge_cutoff()
            || actions.knowledge_cutoff() > self.admitted_at()
        {
            return Err(RecipeError::Invalid);
        }
        Ok(())
    }
    pub(in crate::application::analysis::backtest::input_authority) fn admitted_at(
        &self,
    ) -> Timestamp {
        Timestamp::from_unix_nanos(self.admitted_at_unix_nanos)
    }
    pub(in crate::application::analysis::backtest::input_authority) fn source_action_reference(
        &self,
    ) -> &SourceAppliedCorporateActionPlanReference {
        &self.source_action_reference
    }
    pub(in crate::application::analysis::backtest::input_authority) fn selections(
        &self,
    ) -> &[DailyHistorySelectionWire] {
        &self.selections
    }
    pub(in crate::application::analysis::backtest::input_authority) fn manifests(
        &self,
    ) -> Result<Vec<DatasetManifestRef>, RecipeError> {
        let mut result = Vec::new();
        result
            .try_reserve_exact(self.selections.len().saturating_mul(2))
            .map_err(|_| RecipeError::ResourceExhausted)?;
        for selection in &self.selections {
            result.push(selection.selected_manifest.to_manifest()?);
            result.push(selection.origin_manifest.to_manifest()?);
        }
        Ok(result)
    }
}
impl DailyHistorySelectionWire {
    fn from_output(output: &CompleteMarketBarHistoryOutput) -> Result<Self, RecipeError> {
        let receipt = output.selection().receipt();
        if !receipt.realized_outcome_eligible()
            || receipt.adjustment() != MarketBarAdjustment::Raw
            || output.bars().is_empty()
        {
            return Err(RecipeError::Invalid);
        }
        let coordinate = match (receipt.requested_range(), receipt.requested_dates()) {
            (Some((start, end_inclusive)), None) => DailyHistoryCoordinate::TimestampedPeriod {
                start,
                end_inclusive,
                timestamp_basis: receipt.timestamp_basis().ok_or(RecipeError::Invalid)?,
                session_kind: receipt.session_kind().ok_or(RecipeError::Invalid)?,
            },
            (None, Some((start, end_inclusive))) => {
                let native = output.native_sessions().ok_or(RecipeError::Invalid)?;
                DailyHistoryCoordinate::NominalDailyDate {
                    start,
                    end_inclusive,
                    first_session_close: native
                        .sessions()
                        .first()
                        .ok_or(RecipeError::Invalid)?
                        .closes_at_exclusive(),
                    last_session_close: native
                        .sessions()
                        .last()
                        .ok_or(RecipeError::Invalid)?
                        .closes_at_exclusive(),
                }
            }
            _ => return Err(RecipeError::Invalid),
        };
        let result = Self {
            instrument_id: receipt.instrument_id(),
            coordinate,
            provider_instrument_id: receipt.provider_instrument_id().clone(),
            venue: receipt.venue_id().clone(),
            feed: receipt.feed().clone(),
            interval: receipt.interval().clone(),
            ruleset: receipt.session_ruleset().clone(),
            surface_requirement: output.selection().surface_requirement(),
            knowledge_cutoff: output.read_receipt().knowledge_cutoff(),
            selected_manifest: ManifestWire::from_manifest(output.selection().pinned().manifest()),
            origin_manifest: ManifestWire::from_manifest(output.read_receipt().origin_manifest()),
            selection_digest: encode_digest(output.selection().selection_digest().bytes()),
            publication_receipt_digest: encode_digest(receipt.receipt_digest().bytes()),
            result_digest: encode_digest(output.read_receipt().result_digest().bytes()),
            bar_count: output.bars().len(),
        };
        result.validate()?;
        Ok(result)
    }
    fn covers(&self, start: Timestamp, end: Timestamp) -> bool {
        match self.coordinate {
            DailyHistoryCoordinate::TimestampedPeriod {
                start: first,
                end_inclusive: last,
                ..
            } => first <= start && last >= end,
            DailyHistoryCoordinate::NominalDailyDate {
                first_session_close,
                last_session_close,
                ..
            } => first_session_close <= start && last_session_close >= end,
        }
    }
    fn validate(&self) -> Result<(), RecipeError> {
        if self.bar_count == 0
            || self.bar_count > market_squawk_sources::MAX_COMPLETE_MARKET_BAR_HISTORY_TIMESTAMPS
            || [
                &self.selection_digest,
                &self.publication_receipt_digest,
                &self.result_digest,
            ]
            .into_iter()
            .any(|value| decode_digest(value).map_or(true, |bytes| bytes == [0; 32]))
        {
            return Err(RecipeError::Invalid);
        }
        if let DailyHistoryCoordinate::NominalDailyDate {
            start,
            end_inclusive,
            first_session_close,
            last_session_close,
        } = self.coordinate
        {
            if start > end_inclusive
                || first_session_close > last_session_close
                || last_session_close > self.knowledge_cutoff
            {
                return Err(RecipeError::Invalid);
            }
        }
        self.origin_manifest.to_manifest()?;
        self.exact_request().map(|_| ())
    }
    pub(in crate::application::analysis::backtest::input_authority) fn exact_request(
        &self,
    ) -> Result<CompleteMarketBarHistoryRequest, RecipeError> {
        let manifest = self.selected_manifest.to_manifest()?;
        let result = match self.coordinate {
            DailyHistoryCoordinate::TimestampedPeriod {
                start,
                end_inclusive,
                timestamp_basis,
                session_kind,
            } => CompleteMarketBarHistoryRequest::try_exact(
                self.instrument_id,
                start,
                end_inclusive,
                self.provider_instrument_id.clone(),
                self.venue.clone(),
                self.feed.clone(),
                self.interval.clone(),
                MarketBarAdjustment::Raw,
                timestamp_basis,
                session_kind,
                self.ruleset.clone(),
                self.knowledge_cutoff,
                manifest,
            ),
            DailyHistoryCoordinate::NominalDailyDate {
                start,
                end_inclusive,
                ..
            } => CompleteMarketBarHistoryRequest::try_exact_nominal(
                self.instrument_id,
                start,
                end_inclusive,
                self.provider_instrument_id.clone(),
                self.venue.clone(),
                self.feed.clone(),
                self.interval.clone(),
                MarketBarAdjustment::Raw,
                self.ruleset.clone(),
                self.knowledge_cutoff,
                manifest,
            ),
        }
        .map_err(|_| RecipeError::Invalid)?;
        if self.surface_requirement == MarketHistoryPriceSurfaceRequirement::SelectedOnly {
            Ok(result)
        } else {
            result
                .try_with_surface_requirement(self.surface_requirement)
                .map_err(|_| RecipeError::Invalid)
        }
    }
    pub(in crate::application::analysis::backtest::input_authority) fn matches(
        &self,
        output: &CompleteMarketBarHistoryOutput,
    ) -> Result<bool, RecipeError> {
        Ok(&Self::from_output(output)? == self)
    }
}
