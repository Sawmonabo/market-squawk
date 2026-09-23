//! Closed analytical-capability limits for selected Board datasets.

use serde::Serialize;

use crate::{
    BOARD_H15_TREASURY_CONSTANT_MATURITIES_ROLLING_DASHBOARD_DATE_COUNT, BoardDatasetProfile,
};

/// Exact scope of the bounded H.15 dataset installed for the current dashboard.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BoardH15DatasetScope {
    /// The provider's current definitions over its rolling 100-date response.
    RollingCurrentDefinition,
}

/// Point-in-time basis supplied by the selected rolling H.15 route.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BoardH15PointInTimeBasis {
    /// The common data plane can prove only when this installation first observed the response.
    LocalFirstObservedOnly,
}

/// Provider-authored vintage chronology available from the selected rolling H.15 route.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BoardH15ProviderVintageHistory {
    /// The route does not supply a provider-authored historical vintage chronology.
    Unavailable,
}

/// Historical analytical use supported by the selected rolling H.15 route.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BoardH15HistoricalUse {
    /// Only common-store generations published before the tested cutoff may enter a historical
    /// information set. The route cannot backfill periods before local observation.
    LocallyPublishedGenerationsOnly,
}

/// Investment-workflow role supported by the selected rolling H.15 route.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BoardH15InvestmentUse {
    /// H.15 may contribute only supplemental macro evidence; it is not a complete decision input.
    SupplementalMacroEvidenceOnly,
}

/// Execution role supported by the selected rolling H.15 route.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BoardH15ExecutionUse {
    /// The delayed macro route supplies no order or execution capability.
    None,
}

/// Shared authority that must admit any concrete analytical use.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BoardH15AdmissionBoundary {
    /// Raw sealing, canonical publication, PIT selection, currentness, and research-use admission
    /// remain owned by the common manifest and research-use authorities.
    CommonManifestPitAndResearchUseRequired,
}

/// Descriptive analytical limits of the exact rolling H.15 dashboard profile.
///
/// This value is not provider permission, persistence evidence, a manifest, a PIT selection, or a
/// research-use decision. It cannot authorize analysis by itself. It only describes the maximum
/// role that the common durable/PIT/research-use path may admit for this exact code-owned profile.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BoardH15AnalyticalCapability {
    dataset_scope: BoardH15DatasetScope,
    rolling_date_count: usize,
    point_in_time_basis: BoardH15PointInTimeBasis,
    provider_vintage_history: BoardH15ProviderVintageHistory,
    historical_use: BoardH15HistoricalUse,
    investment_use: BoardH15InvestmentUse,
    execution_use: BoardH15ExecutionUse,
    admission_boundary: BoardH15AdmissionBoundary,
}

impl BoardH15AnalyticalCapability {
    /// Describes limits only for the exact code-owned rolling profile.
    #[must_use]
    pub fn for_profile(profile: &BoardDatasetProfile) -> Option<Self> {
        profile
            .contract()
            .is_h15_treasury_constant_maturities_rolling_dashboard()
            .then_some(Self {
                dataset_scope: BoardH15DatasetScope::RollingCurrentDefinition,
                rolling_date_count:
                    BOARD_H15_TREASURY_CONSTANT_MATURITIES_ROLLING_DASHBOARD_DATE_COUNT,
                point_in_time_basis: BoardH15PointInTimeBasis::LocalFirstObservedOnly,
                provider_vintage_history: BoardH15ProviderVintageHistory::Unavailable,
                historical_use: BoardH15HistoricalUse::LocallyPublishedGenerationsOnly,
                investment_use: BoardH15InvestmentUse::SupplementalMacroEvidenceOnly,
                execution_use: BoardH15ExecutionUse::None,
                admission_boundary:
                    BoardH15AdmissionBoundary::CommonManifestPitAndResearchUseRequired,
            })
    }

    /// Returns the exact bounded provider-date count.
    #[must_use]
    pub const fn rolling_date_count(self) -> usize {
        self.rolling_date_count
    }

    /// Returns the dataset-scope limit.
    #[must_use]
    pub const fn dataset_scope(self) -> BoardH15DatasetScope {
        self.dataset_scope
    }

    /// Returns the available PIT basis.
    #[must_use]
    pub const fn point_in_time_basis(self) -> BoardH15PointInTimeBasis {
        self.point_in_time_basis
    }

    /// Returns the provider-vintage-history limit.
    #[must_use]
    pub const fn provider_vintage_history(self) -> BoardH15ProviderVintageHistory {
        self.provider_vintage_history
    }

    /// Returns the historical-use limit.
    #[must_use]
    pub const fn historical_use(self) -> BoardH15HistoricalUse {
        self.historical_use
    }

    /// Returns the investment-workflow limit.
    #[must_use]
    pub const fn investment_use(self) -> BoardH15InvestmentUse {
        self.investment_use
    }

    /// Returns the execution-use limit.
    #[must_use]
    pub const fn execution_use(self) -> BoardH15ExecutionUse {
        self.execution_use
    }

    /// Returns the common authority boundary required for concrete use.
    #[must_use]
    pub const fn admission_boundary(self) -> BoardH15AdmissionBoundary {
        self.admission_boundary
    }
}
