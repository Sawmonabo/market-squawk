//! Versioned transport-neutral operation contracts.

use serde::Serialize;
use serde_json::Value;

/// Canonical typed request field for explicit mutation confirmation.
pub const TOOL_CONFIRMATION_FIELD: &str = "confirm";
/// Canonical typed request field for bounded instrument identity or universe.
pub const TOOL_INSTRUMENT_IDS_FIELD: &str = "instrumentIds";
/// Canonical typed request field for bounded result-count and result-byte limits.
pub const TOOL_RESULT_LIMITS_FIELD: &str = "resultLimits";
/// Canonical typed request field for requested providers and coverage.
pub const TOOL_SOURCE_COVERAGE_FIELD: &str = "sourceCoverage";
/// Canonical typed request field for effective, publication, availability, or observation time.
pub const TOOL_TIME_RANGE_FIELD: &str = "timeRange";

/// Stable business domain for one registered operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceDomain {
    /// Durable background-work admission, progress, recovery, and control.
    Job,
    /// Screens, dossiers, investment targets, reviews, and invalidation state.
    Decision,
    /// Backup, restore, updates, workspaces, settings, logs, and local lifecycle authority.
    Operations,
    /// Source registration, status, coverage, and health.
    Source,
    /// Live and historical market observations.
    Market,
    /// Research datasets, manifests, history, and alternative data.
    Research,
    /// Filings, facts, statements, and ratios.
    Fundamental,
    /// Macroeconomic series, observations, vintages, and revisions.
    Macro,
    /// Holdings, transactions, performance, exposure, and risk.
    Portfolio,
    /// Returns, factors, valuation, scenarios, features, and backtests.
    Analysis,
    /// Model metadata, bundles, evaluation, and prediction.
    Model,
    /// Fair-value measurements, classification, evidence, and approvals.
    FairValue,
    /// Controlled paper-bot status and lifecycle.
    Bot,
    /// Risk-mediated paper orders, fills, cancellation, and reconciliation.
    Execution,
}

impl ServiceDomain {
    /// Stable serialized domain name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Job => "job",
            Self::Decision => "decision",
            Self::Operations => "operations",
            Self::Source => "source",
            Self::Market => "market",
            Self::Research => "research",
            Self::Fundamental => "fundamental",
            Self::Macro => "macro",
            Self::Portfolio => "portfolio",
            Self::Analysis => "analysis",
            Self::Model => "model",
            Self::FairValue => "fair_value",
            Self::Bot => "bot",
            Self::Execution => "execution",
        }
    }
}

/// Whether an operation's typed input admits a bounded scope dimension.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScopeRequirement {
    /// The typed request must provide this scope.
    Required,
    /// The typed request may provide this scope and the service applies a bounded default.
    Optional,
    /// This scope does not apply to the operation.
    NotApplicable,
}

/// Scope dimensions owned by a descriptor's typed request contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolScope {
    instruments: ScopeRequirement,
    time_range: ScopeRequirement,
    result_limits: ScopeRequirement,
    source_coverage: ScopeRequirement,
}

impl ToolScope {
    /// Creates the exact scope requirements for one operation.
    #[must_use]
    pub const fn new(
        instruments: ScopeRequirement,
        time_range: ScopeRequirement,
        result_limits: ScopeRequirement,
        source_coverage: ScopeRequirement,
    ) -> Self {
        Self {
            instruments,
            time_range,
            result_limits,
            source_coverage,
        }
    }

    /// Instrument-universe or instrument-identity requirement.
    #[must_use]
    pub const fn instruments(self) -> ScopeRequirement {
        self.instruments
    }

    /// Effective, publication, availability, or observation-time requirement.
    #[must_use]
    pub const fn time_range(self) -> ScopeRequirement {
        self.time_range
    }

    /// Caller result-count and result-byte requirement.
    #[must_use]
    pub const fn result_limits(self) -> ScopeRequirement {
        self.result_limits
    }

    /// Requested source and coverage requirement.
    #[must_use]
    pub const fn source_coverage(self) -> ScopeRequirement {
        self.source_coverage
    }
}

/// Authority path required before a service may execute an admitted request.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolAuthorization {
    /// Read-only local operation with no mutation authority.
    ReadOnly,
    /// Local mutation requiring an explicit typed confirmation.
    LocalConfirmation,
    /// Mutation that must traverse the central risk and one-use dispatch authority.
    RiskMediated,
}

impl ToolAuthorization {
    /// Stable serialized authorization name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnly => "read_only",
            Self::LocalConfirmation => "local_confirmation",
            Self::RiskMediated => "risk_mediated",
        }
    }
}

/// Controlled disposition for a valid result that exceeds its inline ceiling.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolArtifactPolicy {
    /// Reject oversize output instead of persisting it.
    InlineOnly,
    /// Publish the complete result through a capability-confined opaque artifact repository.
    OpaqueOnOverflow,
}

/// Source coverage and data-quality evidence required in every result.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceEvidencePolicy {
    /// The service must return bounded source-coverage and data-quality evidence.
    Required,
    /// The operation has no source-derived result and must state that evidence is not applicable.
    NotApplicable,
}

/// Output policy enforced independently of domain business logic.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolResultPolicy {
    source_evidence: SourceEvidencePolicy,
    artifact: ToolArtifactPolicy,
}

impl ToolResultPolicy {
    /// Creates an explicit result policy.
    #[must_use]
    pub const fn new(source_evidence: SourceEvidencePolicy, artifact: ToolArtifactPolicy) -> Self {
        Self {
            source_evidence,
            artifact,
        }
    }

    /// Required source-evidence disposition.
    #[must_use]
    pub const fn source_evidence(self) -> SourceEvidencePolicy {
        self.source_evidence
    }

    /// Oversize-result disposition.
    #[must_use]
    pub const fn artifact(self) -> ToolArtifactPolicy {
        self.artifact
    }
}

/// Complete business-independent contract carried by a descriptor and admitted request.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolContract {
    domain: ServiceDomain,
    authorization: ToolAuthorization,
    scope: ToolScope,
    result: ToolResultPolicy,
}

impl ToolContract {
    /// Creates a complete operation contract.
    #[must_use]
    pub const fn new(
        domain: ServiceDomain,
        authorization: ToolAuthorization,
        scope: ToolScope,
        result: ToolResultPolicy,
    ) -> Self {
        Self {
            domain,
            authorization,
            scope,
            result,
        }
    }

    /// Business domain.
    #[must_use]
    pub const fn domain(self) -> ServiceDomain {
        self.domain
    }

    /// Required authority path.
    #[must_use]
    pub const fn authorization(self) -> ToolAuthorization {
        self.authorization
    }

    /// Typed input-scope requirements.
    #[must_use]
    pub const fn scope(self) -> ToolScope {
        self.scope
    }

    /// Result and artifact policy.
    #[must_use]
    pub const fn result(self) -> ToolResultPolicy {
        self.result
    }

    pub(crate) const fn is_compatible_with_effects(self, read_only: bool) -> bool {
        match (read_only, self.authorization) {
            (true, ToolAuthorization::ReadOnly) => true,
            (false, ToolAuthorization::LocalConfirmation | ToolAuthorization::RiskMediated) => {
                !matches!(self.authorization, ToolAuthorization::RiskMediated)
                    || matches!(self.domain, ServiceDomain::Bot | ServiceDomain::Execution)
            }
            _ => false,
        }
    }

    pub(crate) fn metadata_value(self) -> Result<Value, serde_json::Error> {
        let mut value = serde_json::to_value(self)?;
        let Value::Object(ref mut object) = value else {
            return Ok(Value::Null);
        };
        object.insert("schemaVersion".to_owned(), Value::from(1));
        Ok(value)
    }
}
