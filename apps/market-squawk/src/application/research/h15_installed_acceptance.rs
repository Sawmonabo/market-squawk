//! Closed installed H.15 acceptance access to existing producer and neutral consumer authorities.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use market_squawk_adapter_federal_reserve::BoardDatasetProfile;
use market_squawk_data::{BoardFullHistoryPublicationReference, DatasetManifestRef};
use market_squawk_domain::{CalendarDate, EvidenceDigest, Timestamp};
use market_squawk_services::{
    JsonStructureLimits, RequestContext, RequestId, ServiceError, ServiceLimits,
};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use super::{
    ProductionResearchIngestCoordinator, macro_context::MacroContextReadCapability,
    macro_features::MacroInvestmentContext,
};

/// A fixture-only handle to the registered installed producer and ordinary neutral read.
#[derive(Clone)]
pub struct H15InstalledAcceptance {
    ingest: Arc<ProductionResearchIngestCoordinator>,
    read: MacroContextReadCapability,
}

/// Immutable internal evidence from the same neutral selection used by `Macro.GetContext`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct H15InstalledAcceptanceRead {
    /// Exact cutoffs used by both public and internal reads.
    pub knowledge_cutoff: Timestamp,
    pub effective_date_cutoff: CalendarDate,
    /// The selector's exact consumed digest, including its native full-history binding.
    pub consumed_digest: EvidenceDigest,
    /// Native full-history binding from the rate receipt actually selected by this snapshot.
    pub selected_native_binding: Option<EvidenceDigest>,
    /// Only exact canonical generations actually selected by the neutral consumer.
    pub consumed_parents: Vec<DatasetManifestRef>,
    /// Exact selected canonical rate rows and missingness, in code-owned economic order.
    pub selected_observations: Vec<(String, Option<Value>)>,
    /// Identity of the derived valuation and regime context, if all required rates exist.
    pub investment_digest: Option<EvidenceDigest>,
    /// Exact generations bound into that derived context.
    pub investment_parents: Vec<DatasetManifestRef>,
    /// Derived ten-year and thirty-year annual par-yield assumptions, when available.
    pub valuation_yields: Option<(String, String)>,
}

/// One fixture-only failure; no authority or mutable catalog escapes through it.
#[derive(Debug, thiserror::Error)]
#[error("installed H.15 acceptance access failed: {message}")]
pub struct H15InstalledAcceptanceError {
    message: String,
}

impl H15InstalledAcceptance {
    pub(crate) fn new(
        ingest: Arc<ProductionResearchIngestCoordinator>,
        read: MacroContextReadCapability,
    ) -> Self {
        Self { ingest, read }
    }

    /// Calls the registered Board full-history producer under its existing extraction, rate,
    /// rights, lifecycle, raw-seal, partition, and publication authorities.
    pub async fn publish(
        &self,
    ) -> Result<BoardFullHistoryPublicationReference, H15InstalledAcceptanceError> {
        let structure =
            JsonStructureLimits::try_new(16, 4096, 64, 64).map_err(|error| failure(error))?;
        let limits = ServiceLimits::try_new(4096, 16, 4096, 16, structure)
            .map_err(|error| failure(error))?;
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(60 * 60))
            .ok_or_else(|| failure("deadline overflow"))?;
        let context = RequestContext::new(
            RequestId::try_string("installed-h15-full-history-acceptance")
                .map_err(|error| failure(error))?,
            CancellationToken::new(),
            deadline,
            limits,
        );
        self.ingest
            .prepare_default_h15_full_history(&context)
            .await
            .map_err(|error| failure(format!("{error:?}")))
    }

    /// Reads the actual provider-neutral selector and derived investment context at explicit
    /// point-in-time coordinates, returning immutable evidence only.
    pub async fn read(
        &self,
        knowledge_cutoff: Timestamp,
        effective_date_cutoff: CalendarDate,
    ) -> Result<H15InstalledAcceptanceRead, H15InstalledAcceptanceError> {
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(30))
            .ok_or_else(|| failure("deadline overflow"))?;
        let snapshot = self
            .read
            .read_latest_known(
                knowledge_cutoff,
                effective_date_cutoff,
                deadline,
                CancellationToken::new(),
            )
            .await
            .map_err(failure)?;
        let selected_observations = snapshot
            .selected()
            .iter()
            .map(|selection| {
                Ok((
                    selection.indicator_id().to_owned(),
                    selection
                        .observation()
                        .map(serde_json::to_value)
                        .transpose()
                        .map_err(failure)?,
                ))
            })
            .collect::<Result<Vec<_>, H15InstalledAcceptanceError>>()?;
        let evidence = snapshot.evidence();
        let investment = match MacroInvestmentContext::try_from_snapshot(&snapshot) {
            Ok(value) => Some(value),
            Err(ServiceError::Unavailable) => None,
            Err(error) => return Err(failure(error)),
        };
        let valuation_yields = investment.as_ref().map(|value| {
            let rates = value.valuation_rates();
            (
                rates.ten_year_government_yield().to_string(),
                rates.thirty_year_government_yield().to_string(),
            )
        });
        Ok(H15InstalledAcceptanceRead {
            knowledge_cutoff,
            effective_date_cutoff,
            consumed_digest: evidence.consumed_digest(),
            selected_native_binding: evidence.selected_board_native_binding().map_err(failure)?,
            consumed_parents: evidence.consumed_parent_manifests().to_vec(),
            selected_observations,
            investment_digest: investment
                .as_ref()
                .map(MacroInvestmentContext::evidence_digest),
            investment_parents: investment
                .as_ref()
                .map_or_else(Vec::new, |value| value.parent_manifests().to_vec()),
            valuation_yields,
        })
    }

    /// Returns the exact analytical identity of the code-owned full-history profile.
    pub fn full_history_dataset(&self) -> Result<String, H15InstalledAcceptanceError> {
        BoardDatasetProfile::h15_treasury_constant_maturities_full_history()
            .map(|profile| profile.analytical_dataset().as_str().to_owned())
            .map_err(failure)
    }
}

fn failure(error: impl std::fmt::Display) -> H15InstalledAcceptanceError {
    H15InstalledAcceptanceError {
        message: error.to_string(),
    }
}
