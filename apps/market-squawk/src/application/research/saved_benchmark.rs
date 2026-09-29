//! Original comparison selection and history recipe, retained inside decision evidence.
//! No read from this module acquires provider data or upgrades an originally missing source.

use super::{
    benchmark::{BenchmarkHistoryEvaluation, BenchmarkHistoryReference},
    benchmark_selection::{
        RecommendationBenchmarkSelectionReadCapability, SavedBenchmarkSelectionReference,
    },
    market_history::{MarketHistoryReadCapability, MarketHistoryUnavailableReason},
};
use crate::{ResearchService, application::market_calendar::CompletedMarketSessionReadCapability};
use market_squawk_decisions::SavedBenchmarkComparisonEvidence;
use market_squawk_domain::{Currency, InstrumentId, Timestamp};
use market_squawk_services::{RequestContext, ServiceError};
use serde::{Deserialize, Serialize};

/// Original request coordinates, independent of whether an event probability was produced.
pub(crate) struct SavedBenchmarkRequest {
    pub(crate) subject: InstrumentId,
    pub(crate) requested: Option<InstrumentId>,
    pub(crate) currency: Currency,
    pub(crate) source_cutoff: Timestamp,
    pub(crate) observed_through: Timestamp,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SavedBenchmarkComparison {
    version: u16,
    #[serde(deserialize_with = "Option::deserialize")]
    requested: Option<InstrumentId>,
    #[serde(deserialize_with = "Option::deserialize")]
    selected: Option<SavedBenchmarkSelectionReference>,
    #[serde(deserialize_with = "Option::deserialize")]
    accompanying: Option<SavedBenchmarkSelectionReference>,
    history: SavedHistory,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum SavedHistory {
    SelectionUnavailable,
    StorageUnavailable,
    Captured {
        reference: BenchmarkHistoryReference,
    },
}

pub(crate) enum SavedBenchmarkReplay {
    Evaluated(BenchmarkHistoryEvaluation),
    Unavailable(SavedBenchmarkUnavailable),
}

#[derive(Clone, Copy)]
pub(crate) enum SavedBenchmarkUnavailable {
    Selection,
    Storage,
    Integrity,
}

impl SavedBenchmarkComparison {
    /// Schema and canonical-byte validation only; this grants no source-use authority.
    pub(crate) fn decode(
        evidence: &SavedBenchmarkComparisonEvidence,
    ) -> Result<Self, ServiceError> {
        let value: Self = serde_json::from_slice(evidence.canonical_record())
            .map_err(|_| ServiceError::InvalidResult)?;
        value.validate(evidence)?;
        if serde_json::to_vec(&value).map_err(|_| ServiceError::InvalidResult)?
            != evidence.canonical_record()
        {
            return Err(ServiceError::InvalidResult);
        }
        Ok(value)
    }

    fn validate(&self, evidence: &SavedBenchmarkComparisonEvidence) -> Result<(), ServiceError> {
        if self.version != 1 {
            return Err(ServiceError::InvalidResult);
        }
        if let Some(selected) = &self.selected {
            selected.validate_at(evidence.source_cutoff())?;
            if selected.explicit() != self.requested {
                return Err(ServiceError::InvalidResult);
            }
        }
        if let Some(accompanying) = &self.accompanying {
            accompanying.validate_at(evidence.source_cutoff())?;
            if self.selected.is_none()
                || accompanying.explicit() != Some(accompanying.instrument_id())
                || accompanying.instrument_id() == evidence.instrument_id()
                || self.selected.as_ref().is_some_and(|selected| {
                    accompanying.instrument_id() == selected.instrument_id()
                })
            {
                return Err(ServiceError::InvalidResult);
            }
        }
        match &self.history {
            SavedHistory::SelectionUnavailable
                if self.selected.is_none() && self.accompanying.is_none() => {}
            SavedHistory::StorageUnavailable if self.selected.is_some() => {}
            SavedHistory::Captured { reference } if self.selected.is_some() => {
                let selected = self.selected.as_ref().ok_or(ServiceError::InvalidResult)?;
                let expected = [evidence.instrument_id(), selected.instrument_id()]
                    .into_iter()
                    .chain(
                        self.accompanying
                            .as_ref()
                            .map(SavedBenchmarkSelectionReference::instrument_id),
                    );
                if reference.currency() != evidence.currency()
                    || reference.source_cutoff() != evidence.source_cutoff()
                    || reference.observed_through() != evidence.observed_through()
                    || !reference.instruments().eq(expected)
                {
                    return Err(ServiceError::InvalidResult);
                }
            }
            _ => return Err(ServiceError::InvalidResult),
        }
        Ok(())
    }

    pub(crate) const fn requested(&self) -> Option<InstrumentId> {
        self.requested
    }
    pub(crate) const fn selected(&self) -> Option<&SavedBenchmarkSelectionReference> {
        self.selected.as_ref()
    }
    pub(crate) const fn accompanying(&self) -> Option<&SavedBenchmarkSelectionReference> {
        self.accompanying.as_ref()
    }

    /// Fresh authorization over the original catalog and exact immutable history sources.
    pub(crate) async fn replay(
        &self,
        research: &ResearchService,
        history: &MarketHistoryReadCapability,
        calendars: &CompletedMarketSessionReadCapability,
        context: &RequestContext,
    ) -> Result<SavedBenchmarkReplay, ServiceError> {
        let reference = match &self.history {
            SavedHistory::SelectionUnavailable => {
                return Ok(SavedBenchmarkReplay::Unavailable(
                    SavedBenchmarkUnavailable::Selection,
                ));
            }
            SavedHistory::StorageUnavailable => {
                return Ok(SavedBenchmarkReplay::Unavailable(
                    SavedBenchmarkUnavailable::Storage,
                ));
            }
            SavedHistory::Captured { reference } => reference,
        };
        let catalog =
            RecommendationBenchmarkSelectionReadCapability::new(research.market_data_instruments());
        for saved in self.selected.iter().chain(self.accompanying.iter()) {
            match catalog.read_saved_comparison(saved, context.deadline(), context.cancellation()) {
                Ok(true) => {}
                Ok(false)
                | Err(
                    ServiceError::Unavailable | ServiceError::NotFound | ServiceError::Unauthorized,
                ) => {
                    return Ok(SavedBenchmarkReplay::Unavailable(
                        SavedBenchmarkUnavailable::Selection,
                    ));
                }
                Err(ServiceError::InvalidResult) => {
                    return Ok(SavedBenchmarkReplay::Unavailable(
                        SavedBenchmarkUnavailable::Integrity,
                    ));
                }
                Err(error) => return Err(error),
            }
        }
        match history
            .read_saved_benchmark_history(
                research,
                calendars,
                reference,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
        {
            Ok(value) => Ok(SavedBenchmarkReplay::Evaluated(value)),
            Err(MarketHistoryUnavailableReason::StorageUnavailable) => Ok(
                SavedBenchmarkReplay::Unavailable(SavedBenchmarkUnavailable::Storage),
            ),
            Err(MarketHistoryUnavailableReason::IntegrityUnproven) => Ok(
                SavedBenchmarkReplay::Unavailable(SavedBenchmarkUnavailable::Integrity),
            ),
            Err(error) => Err(history_error(error)),
        }
    }
}

/// Captures even an unresolved explicit choice; it is never replaced by SPY, VTI or the subject.
pub(crate) async fn prepare(
    research: &ResearchService,
    history: &MarketHistoryReadCapability,
    calendars: &CompletedMarketSessionReadCapability,
    input: SavedBenchmarkRequest,
    context: &RequestContext,
) -> Result<SavedBenchmarkComparisonEvidence, ServiceError> {
    let catalog =
        RecommendationBenchmarkSelectionReadCapability::new(research.market_data_instruments());
    let selected = catalog.select_saved_comparison(
        input.requested,
        input.source_cutoff,
        context.deadline(),
        context.cancellation(),
    )?;
    let accompanying = if let Some(selected) = &selected {
        catalog
            .select_saved_accompanying(
                input.source_cutoff,
                context.deadline(),
                context.cancellation(),
            )?
            .filter(|member| {
                member.instrument_id() != input.subject
                    && member.instrument_id() != selected.instrument_id()
            })
    } else {
        None
    };
    let saved_history = if let Some(selected) = &selected {
        match history
            .read_benchmark_history(
                research,
                calendars,
                [input.subject, selected.instrument_id()],
                accompanying
                    .as_ref()
                    .map(SavedBenchmarkSelectionReference::instrument_id),
                input.currency,
                input.source_cutoff,
                input.observed_through,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
        {
            Ok(value) => SavedHistory::Captured {
                reference: value.reference().clone(),
            },
            Err(MarketHistoryUnavailableReason::StorageUnavailable) => {
                SavedHistory::StorageUnavailable
            }
            Err(error) => return Err(history_error(error)),
        }
    } else {
        SavedHistory::SelectionUnavailable
    };
    let value = SavedBenchmarkComparison {
        version: 1,
        requested: input.requested,
        selected,
        accompanying,
        history: saved_history,
    };
    let bytes = serde_json::to_vec(&value).map_err(|_| ServiceError::InvalidResult)?;
    let evidence = SavedBenchmarkComparisonEvidence::try_new(
        input.subject,
        input.currency,
        input.source_cutoff,
        input.observed_through,
        bytes.into_boxed_slice(),
    )
    .map_err(|_| ServiceError::InvalidResult)?;
    value.validate(&evidence)?;
    Ok(evidence)
}

fn history_error(error: MarketHistoryUnavailableReason) -> ServiceError {
    match error {
        MarketHistoryUnavailableReason::Cancelled => ServiceError::Cancelled,
        MarketHistoryUnavailableReason::DeadlineExceeded => ServiceError::DeadlineExceeded,
        MarketHistoryUnavailableReason::CapacityExceeded => ServiceError::ResourceExhausted,
        MarketHistoryUnavailableReason::IntegrityUnproven => ServiceError::InvalidResult,
        MarketHistoryUnavailableReason::StorageUnavailable => ServiceError::Unavailable,
    }
}
