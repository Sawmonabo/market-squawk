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
    subject: InstrumentId,
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
        projection: market_squawk_data::ChartProjectionReference,
    },
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
        if self.version != 1 || self.subject != evidence.instrument_id() {
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
            SavedHistory::Captured { reference, .. } if self.selected.is_some() => {
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
            Ok(value) => {
                let projection = publish_projection(research, &value, context)?;
                SavedHistory::Captured {
                    reference: value.reference().clone(),
                    projection,
                }
            }
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
        subject: input.subject,
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

impl SavedBenchmarkComparison {
    /// Opens the immutable original financial projection without replaying the entire source.
    pub(crate) async fn read_projection(
        &self,
        research: &ResearchService,
        start: Option<i64>,
        end: Option<i64>,
        point_limit: usize,
        context: &RequestContext,
    ) -> Result<serde_json::Value, ServiceError> {
        use crate::application::model::forecast::{
            authorize_projection_parents, chart_storage_error, read_chart_display,
        };
        use serde_json::json;
        let mut members = vec![json!({"role":"subject","instrumentId":match &self.history {
            SavedHistory::Captured{reference,..}=>reference.instruments().next().ok_or(ServiceError::InvalidResult)?,
            _=>self.subject,
        },"label":"Investment"})];
        if let Some(selected) = &self.selected {
            members.push(json!({"role":"selected","instrumentId":selected.instrument_id(),"label":selected.label()}));
        } else if let Some(requested) = self.requested {
            members.push(
                json!({"role":"selected","instrumentId":requested,"label":"Selected comparison"}),
            );
        }
        if let Some(accompanying) = &self.accompanying {
            members.push(json!({"role":"accompanying","instrumentId":accompanying.instrument_id(),"label":accompanying.label()}));
        }
        let unavailable = |reason: &str, summary: &str| json!({"state":"unavailable","basis":"split_adjusted_price_index","members":members,"reason":reason,"summary":summary});
        let (reference, projection) = match &self.history {
            SavedHistory::Captured {
                reference,
                projection,
            } => (reference, projection),
            SavedHistory::SelectionUnavailable => {
                return Ok(unavailable(
                    "selection_unavailable",
                    "The original comparison identity is unavailable. The saved choice has not been replaced.",
                ));
            }
            SavedHistory::StorageUnavailable => {
                return Ok(unavailable(
                    "storage_unavailable",
                    "The original comparison history is unavailable. Newer history has not been substituted.",
                ));
            }
        };
        let catalog =
            RecommendationBenchmarkSelectionReadCapability::new(research.market_data_instruments());
        for selected in self.selected.iter().chain(self.accompanying.iter()) {
            match catalog.read_saved_comparison(
                selected,
                context.deadline(),
                context.cancellation(),
            ) {
                Ok(true) => {}
                Ok(false)
                | Err(
                    ServiceError::Unavailable | ServiceError::NotFound | ServiceError::Unauthorized,
                ) => {
                    return Ok(unavailable(
                        "selection_unavailable",
                        "The original comparison identity is unavailable. The saved choice has not been replaced.",
                    ));
                }
                Err(error) => return Err(error),
            }
        }
        if projection.source_sha256 != projection_source(reference)? {
            return Err(ServiceError::InvalidResult);
        }
        let parents = reference.parent_manifests()?;
        let permit = if parents.is_empty() {
            None
        } else {
            Some(
                authorize_projection_parents(
                    research,
                    &parents,
                    reference.source_cutoff(),
                    context,
                )
                .await?,
            )
        };
        let metadata = research
            .chart_projections()
            .metadata(projection, context.deadline(), context.cancellation())
            .map_err(chart_storage_error)?;
        let mut value: serde_json::Value =
            serde_json::from_slice(&metadata).map_err(|_| ServiceError::InvalidResult)?;
        let object = value.as_object_mut().ok_or(ServiceError::InvalidResult)?;
        object.insert("members".into(), json!(members));
        if object.get("state").and_then(serde_json::Value::as_str) == Some("available") {
            if permit.is_none() {
                return Err(ServiceError::InvalidResult);
            }
            let (points, display) =
                read_chart_display(research, projection, start, end, point_limit, context)?;
            object.insert("points".into(), json!(points));
            object.insert("display".into(), display);
        }
        if context.cancellation().is_cancelled() {
            return Err(ServiceError::Cancelled);
        }
        if std::time::Instant::now() >= context.deadline() {
            return Err(ServiceError::DeadlineExceeded);
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|v| i64::try_from(v.as_nanos()).ok())
            .map(Timestamp::from_unix_nanos)
            .ok_or(ServiceError::Internal)?;
        if permit.is_some_and(|permit| now >= permit.expires_at()) {
            return Err(ServiceError::Unauthorized);
        }
        Ok(value)
    }
}

fn projection_source(reference: &BenchmarkHistoryReference) -> Result<[u8; 32], ServiceError> {
    use sha2::{Digest as _, Sha256};
    Ok(
        Sha256::digest(serde_json::to_vec(reference).map_err(|_| ServiceError::InvalidResult)?)
            .into(),
    )
}

fn publish_projection(
    research: &ResearchService,
    actual: &BenchmarkHistoryEvaluation,
    context: &RequestContext,
) -> Result<market_squawk_data::ChartProjectionReference, ServiceError> {
    use crate::application::{
        benchmark::BenchmarkHistoryDisposition,
        model::forecast::{chart_quality, chart_storage_error},
    };
    use market_squawk_data::ChartProjectionRow;
    use serde_json::json;
    let (reason, summary) = match actual.disposition() {
        BenchmarkHistoryDisposition::Available => (
            None,
            "Split-adjusted closing prices start at 100 on the same original trading session. Cash distributions are excluded; gaps indicate missing observations.",
        ),
        BenchmarkHistoryDisposition::MissingSubject => (
            Some("missing_subject"),
            "No comparable price history was retained for this investment at the original cutoff.",
        ),
        BenchmarkHistoryDisposition::MissingSelectedComparison => (
            Some("missing_selected_comparison"),
            "No comparable price history was retained for the selected comparison at the original cutoff.",
        ),
        BenchmarkHistoryDisposition::NoCommonObservation => (
            Some("no_common_observation"),
            "The retained histories have no shared eligible observation on the same trading session.",
        ),
    };
    let metadata = if let Some(reason) = reason {
        json!({"state":"unavailable","basis":"split_adjusted_price_index","reason":reason,"summary":summary})
    } else {
        let baseline = actual.baseline().ok_or(ServiceError::InvalidResult)?;
        json!({"state":"available","basis":"split_adjusted_price_index","summary":summary,
            "baseline":{"date":baseline.date.to_string(),"sessionCloseUnixNanos":baseline.session_close.unix_nanos().to_string()}})
    };
    let metadata = serde_json::to_vec(&metadata).map_err(|_| ServiceError::InvalidResult)?;
    let rows=actual.points().map(|point| {
        let point=point.map_err(|error|match error {
            MarketHistoryUnavailableReason::Cancelled=>market_squawk_data::ChartProjectionError::Cancelled,
            MarketHistoryUnavailableReason::DeadlineExceeded=>market_squawk_data::ChartProjectionError::DeadlineExceeded,
            MarketHistoryUnavailableReason::StorageUnavailable=>market_squawk_data::ChartProjectionError::Unavailable,
            _=>market_squawk_data::ChartProjectionError::Invalid,
        })?;
        let observations=point.observations.iter().map(|observation|observation.as_ref().map(|value|json!({
            "close":value.close.normalize().to_string(),"priceIndex":value.price_index.normalize().to_string(),
            "availableAtUnixNanos":value.available_at.unix_nanos().to_string(),
            "providerCompletedAtUnixNanos":value.provider_completed_at.map(|v|v.unix_nanos().to_string()),"quality":chart_quality(value.quality),
        }))).collect::<Vec<_>>();
        Ok(ChartProjectionRow{time_nanos:point.coordinate.session_close.unix_nanos(),
            values:point.observations.iter().map(|observation|observation.as_ref().map(|value|value.price_index.into())).collect(),
            point:json!({"coordinate":{"date":point.coordinate.date.to_string(),"sessionCloseUnixNanos":point.coordinate.session_close.unix_nanos().to_string()},"observations":observations})})
    });
    let projection = research
        .chart_projections()
        .publish(
            projection_source(actual.reference())?,
            &metadata,
            actual.reference().instruments().count(),
            rows,
            context.deadline(),
            context.cancellation(),
        )
        .map_err(chart_storage_error)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|value| i64::try_from(value.as_nanos()).ok())
        .map(Timestamp::from_unix_nanos)
        .ok_or(ServiceError::Internal)?;
    if actual
        .rights()
        .iter()
        .any(|rights| now >= rights.expires_at)
    {
        return Err(ServiceError::Unauthorized);
    };
    Ok(projection)
}
