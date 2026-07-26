//! Bounded, secret-free, query-only local readiness reporting.

use std::collections::{BTreeMap, BTreeSet};
use std::io;

use market_squawk_data::{Catalog, CatalogDiagnosticSnapshot, CatalogLimit};
use market_squawk_domain::DataQuality;
use market_squawk_mcp::{McpLimitSpec, McpLimits, validate_service_capabilities};
use market_squawk_platform::{AppConfig, EffectiveConfigView, LocalPaths, PathError};
use market_squawk_services::ServiceCapabilities;
use market_squawk_sources::{
    OnboardingState, ProfileReleaseState, Requirement, RightsAdmissionState,
    built_in_provider_profiles,
};
use serde::Serialize;
use thiserror::Error;

use crate::application::application_capabilities;
use crate::local_product::local_catalog_config;

const PROVIDER_SESSION_LIMIT: usize = 32;
const REQUIRED_APPLICATION_DOMAINS: [&str; 11] = [
    "source",
    "market",
    "research",
    "fundamental",
    "macro",
    "portfolio",
    "analysis",
    "model",
    "fair_value",
    "bot",
    "execution",
];

/// Complete bounded local-readiness report.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DoctorReport<'config> {
    schema_version: &'static str,
    status: DoctorStatus,
    configuration: EffectiveConfigView<'config>,
    local_storage: LocalStorageReport,
    tracing: LocalTracingReport,
    endpoints: EndpointReport,
    artifacts: ArtifactReport,
    application: ApplicationReport,
    mcp: McpReport,
    providers: ProviderReport,
    release_blockers: Vec<ReleaseBlocker>,
}

impl DoctorReport<'_> {
    /// Returns whether every locally observable readiness predicate passed.
    ///
    /// Exact-head release, authorized external-provider, hosted-OS, fuzz, and performance evidence
    /// remain separate release-ledger concerns and are not inferred by this local inspection.
    #[must_use]
    pub const fn is_ready(&self) -> bool {
        matches!(self.status, DoctorStatus::Ready)
    }
}

/// Readiness-report construction failed before a bounded report could be produced.
#[derive(Debug, Error)]
pub enum DoctorError {
    /// The configured storage root could not be inspected.
    #[error("doctor could not inspect the configured local storage root")]
    StorageInspection(#[source] io::Error),
    /// A code-owned result limit is invalid.
    #[error("doctor provider-session limit is invalid")]
    InvalidProviderLimit,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum DoctorStatus {
    Blocked,
    Ready,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct LocalStorageReport {
    configured: bool,
    existed_before_inspection: bool,
    modified_by_inspection: bool,
    layout: LocalLayoutReport,
    catalog: CatalogReport,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct LocalLayoutReport {
    state: ObservationState,
    error: Option<&'static str>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CatalogReport {
    state: ObservationState,
    journal_mode: Option<String>,
    applied_migrations: Option<u32>,
    error: Option<&'static str>,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum ObservationState {
    Available,
    Missing,
    NotObserved,
    Unavailable,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct LocalTracingReport {
    local_only: bool,
    human_readable: bool,
    json: bool,
    remote_exporter: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct EndpointReport {
    deny_by_default: bool,
    coinbase: ConfiguredEndpointReport,
    kraken: ConfiguredEndpointReport,
    live_execution_endpoint_configured: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ConfiguredEndpointReport {
    configured: bool,
    allowlisted: bool,
    endpoint: Option<&'static str>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ArtifactReport {
    layout_available: bool,
    capability_confined: bool,
    arbitrary_path_access: bool,
    mcp_opaque_references_only: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ApplicationReport {
    descriptor_contract_valid: bool,
    runtime_state: ObservationState,
    required_domains_complete: bool,
    domains: Vec<&'static str>,
    tool_count: usize,
    error: Option<&'static str>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct McpReport {
    descriptor_contract_valid: bool,
    runtime_state: ObservationState,
    transport: &'static str,
    peer_identity: &'static str,
    durable_audit_configured: bool,
    controlled_artifacts_configured: bool,
    error: Option<&'static str>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProviderReport {
    bounded: bool,
    runtime_observation: ObservationState,
    surfaces: Vec<ProviderSurfaceReport>,
    error: Option<&'static str>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProviderSurfaceReport {
    surface_id: &'static str,
    capability_revision: u64,
    release_state: ProfileReleaseState,
    rights_state: RightsAdmissionState,
    account_requirement: Requirement,
    credential_requirement: Requirement,
    coverage: &'static str,
    quality_ceiling: DataQuality,
    session_state: Option<OnboardingState>,
    runtime_status: ObservationState,
    runtime_coverage: ObservationState,
    runtime_health: ObservationState,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ReleaseBlocker {
    code: &'static str,
    surfaces: Vec<&'static str>,
}

/// Inspects code-owned contracts and an existing local layout without acquiring write authority.
///
/// The command does not initialize directories, migrate or recover SQLite, open secret stores,
/// acquire application or MCP audit ownership, start adapters, or call remote endpoints. Catalog
/// facts are read through a bounded SQLite read-only/query-only snapshot that participates in
/// normal WAL locking.
///
/// # Errors
///
/// Returns [`DoctorError`] only when the configured root cannot be inspected or a code-owned bound
/// cannot be represented. Missing or unavailable product authorities are reported as stable,
/// secret-free blockers.
pub async fn inspect(config: &AppConfig) -> Result<DoctorReport<'_>, DoctorError> {
    let existed_before_inspection = config
        .data_dir()
        .try_exists()
        .map_err(DoctorError::StorageInspection)?;
    let provider_limit =
        CatalogLimit::new(PROVIDER_SESSION_LIMIT).map_err(|_| DoctorError::InvalidProviderLimit)?;
    let endpoints = endpoint_report(config);
    let mut blockers = Vec::new();

    let (paths, layout) = inspect_layout(config, existed_before_inspection, &mut blockers);
    let (catalog, catalog_snapshot) =
        inspect_catalog(paths.as_ref(), provider_limit, &mut blockers);
    let (application, capabilities) = application_report(&mut blockers);
    let mcp = mcp_report(capabilities.as_ref(), paths.is_some(), &mut blockers);
    let providers = provider_report(catalog_snapshot.as_ref());
    append_provider_blockers(&providers, &mut blockers);
    let artifacts = ArtifactReport {
        layout_available: paths.is_some(),
        capability_confined: paths.is_some(),
        arbitrary_path_access: false,
        mcp_opaque_references_only: true,
    };

    blockers.sort_by(|left, right| left.code.cmp(right.code));
    blockers.dedup_by(|left, right| left.code == right.code && left.surfaces == right.surfaces);
    let status = if blockers.is_empty() {
        DoctorStatus::Ready
    } else {
        DoctorStatus::Blocked
    };
    Ok(DoctorReport {
        schema_version: "market-squawk-doctor-v1",
        status,
        configuration: config.redacted_view(),
        local_storage: LocalStorageReport {
            configured: !config.data_dir().as_os_str().is_empty(),
            existed_before_inspection,
            modified_by_inspection: false,
            layout,
            catalog,
        },
        tracing: LocalTracingReport {
            local_only: true,
            human_readable: true,
            json: true,
            remote_exporter: false,
        },
        endpoints,
        artifacts,
        application,
        mcp,
        providers,
        release_blockers: blockers,
    })
}

impl ReleaseBlocker {
    const fn new(code: &'static str, surfaces: Vec<&'static str>) -> Self {
        Self { code, surfaces }
    }
}

fn inspect_layout(
    config: &AppConfig,
    existed: bool,
    blockers: &mut Vec<ReleaseBlocker>,
) -> (Option<LocalPaths>, LocalLayoutReport) {
    if !existed {
        blockers.push(ReleaseBlocker::new("storage.root_missing", Vec::new()));
        return (
            None,
            LocalLayoutReport {
                state: ObservationState::Missing,
                error: Some("storage.root_missing"),
            },
        );
    }
    match LocalPaths::open_existing(config.data_dir()) {
        Ok(paths) => (
            Some(paths),
            LocalLayoutReport {
                state: ObservationState::Available,
                error: None,
            },
        ),
        Err(error) => {
            let code = classify_path_error(&error);
            blockers.push(ReleaseBlocker::new(code, Vec::new()));
            (
                None,
                LocalLayoutReport {
                    state: ObservationState::Unavailable,
                    error: Some(code),
                },
            )
        }
    }
}

fn inspect_catalog(
    paths: Option<&LocalPaths>,
    provider_limit: CatalogLimit,
    blockers: &mut Vec<ReleaseBlocker>,
) -> (CatalogReport, Option<CatalogDiagnosticSnapshot>) {
    let Some(paths) = paths else {
        return (
            CatalogReport {
                state: ObservationState::NotObserved,
                journal_mode: None,
                applied_migrations: None,
                error: Some("catalog.layout_unavailable"),
            },
            None,
        );
    };
    let config = match local_catalog_config(paths) {
        Ok(config) => config,
        Err(_error) => {
            blockers.push(ReleaseBlocker::new(
                "catalog.configuration_invalid",
                Vec::new(),
            ));
            return (
                CatalogReport {
                    state: ObservationState::Unavailable,
                    journal_mode: None,
                    applied_migrations: None,
                    error: Some("catalog.configuration_invalid"),
                },
                None,
            );
        }
    };
    match Catalog::diagnostics(config, provider_limit) {
        Ok(snapshot) => (
            CatalogReport {
                state: ObservationState::Available,
                journal_mode: Some(snapshot.journal_mode().to_owned()),
                applied_migrations: Some(snapshot.applied_migrations()),
                error: None,
            },
            Some(snapshot),
        ),
        Err(error) => {
            let code = classify_catalog_error(&error);
            blockers.push(ReleaseBlocker::new(code, Vec::new()));
            (
                CatalogReport {
                    state: ObservationState::Unavailable,
                    journal_mode: None,
                    applied_migrations: None,
                    error: Some(code),
                },
                None,
            )
        }
    }
}

fn application_report(
    blockers: &mut Vec<ReleaseBlocker>,
) -> (ApplicationReport, Option<ServiceCapabilities>) {
    match application_capabilities() {
        Ok(capabilities) => {
            let domains = capabilities
                .tools()
                .iter()
                .map(|tool| tool.contract().domain().as_str())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            let required_domains_complete = REQUIRED_APPLICATION_DOMAINS
                .into_iter()
                .all(|required| domains.contains(&required));
            if !required_domains_complete {
                blockers.push(ReleaseBlocker::new(
                    "application.required_domains",
                    Vec::new(),
                ));
            }
            (
                ApplicationReport {
                    descriptor_contract_valid: true,
                    runtime_state: ObservationState::NotObserved,
                    required_domains_complete,
                    domains,
                    tool_count: capabilities.tools().len(),
                    error: None,
                },
                Some(capabilities),
            )
        }
        Err(_error) => {
            blockers.push(ReleaseBlocker::new(
                "application.descriptor_contract_invalid",
                Vec::new(),
            ));
            (
                ApplicationReport {
                    descriptor_contract_valid: false,
                    runtime_state: ObservationState::NotObserved,
                    required_domains_complete: false,
                    domains: Vec::new(),
                    tool_count: 0,
                    error: Some("application.descriptor_contract_invalid"),
                },
                None,
            )
        }
    }
}

fn mcp_report(
    capabilities: Option<&ServiceCapabilities>,
    layout_available: bool,
    blockers: &mut Vec<ReleaseBlocker>,
) -> McpReport {
    let error = match capabilities {
        None => Some("mcp.application_contract_invalid"),
        Some(capabilities) => match McpLimits::try_from(McpLimitSpec::default()) {
            Ok(limits) => validate_service_capabilities(capabilities, limits)
                .err()
                .map(|_error| "mcp.descriptor_contract_invalid"),
            Err(_error) => Some("mcp.limit_contract_invalid"),
        },
    };
    if let Some(code) = error {
        blockers.push(ReleaseBlocker::new(code, Vec::new()));
    }
    McpReport {
        descriptor_contract_valid: error.is_none(),
        runtime_state: ObservationState::NotObserved,
        transport: "stdio",
        peer_identity: "unverified",
        durable_audit_configured: layout_available,
        controlled_artifacts_configured: layout_available,
        error,
    }
}

fn provider_report(snapshot: Option<&CatalogDiagnosticSnapshot>) -> ProviderReport {
    let profiles = match built_in_provider_profiles() {
        Ok(profiles) => profiles,
        Err(_error) => {
            return ProviderReport {
                bounded: false,
                runtime_observation: ObservationState::NotObserved,
                surfaces: Vec::new(),
                error: Some("providers.profile_contract_invalid"),
            };
        }
    };
    let sessions = snapshot
        .map(CatalogDiagnosticSnapshot::current_provider_sessions)
        .unwrap_or_default()
        .iter()
        .map(|session| (session.surface_id(), session.state()))
        .collect::<BTreeMap<_, _>>();
    ProviderReport {
        bounded: snapshot.is_some(),
        runtime_observation: ObservationState::NotObserved,
        surfaces: profiles
            .iter()
            .map(|profile| {
                let capability = profile.capability();
                let (account_requirement, credential_requirement, _contact_requirement) =
                    profile.requirements();
                let (coverage, quality_ceiling) = profile.coverage();
                ProviderSurfaceReport {
                    surface_id: profile.id(),
                    capability_revision: capability.revision().get(),
                    release_state: profile.release_state(),
                    rights_state: capability.rights_state(),
                    account_requirement,
                    credential_requirement,
                    coverage,
                    quality_ceiling,
                    session_state: sessions.get(profile.id()).copied(),
                    runtime_status: ObservationState::NotObserved,
                    runtime_coverage: ObservationState::NotObserved,
                    runtime_health: ObservationState::NotObserved,
                }
            })
            .collect(),
        error: snapshot
            .is_none()
            .then_some("providers.session_status_unavailable"),
    }
}

fn append_provider_blockers(providers: &ProviderReport, blockers: &mut Vec<ReleaseBlocker>) {
    if providers.surfaces.is_empty() {
        blockers.push(ReleaseBlocker::new(
            "providers.profile_contract_invalid",
            Vec::new(),
        ));
        return;
    }
    if !providers.bounded {
        blockers.push(ReleaseBlocker::new(
            "providers.session_status_unavailable",
            Vec::new(),
        ));
    }
    let onboarding_incomplete = providers
        .surfaces
        .iter()
        .filter(|surface| surface.session_state != Some(OnboardingState::ActiveScoped))
        .map(|surface| surface.surface_id)
        .collect::<Vec<_>>();
    if !onboarding_incomplete.is_empty() {
        blockers.push(ReleaseBlocker::new(
            "providers.onboarding_incomplete",
            onboarding_incomplete,
        ));
    }
    blockers.push(ReleaseBlocker::new(
        "providers.runtime_health_not_observed",
        providers
            .surfaces
            .iter()
            .map(|surface| surface.surface_id)
            .collect(),
    ));

    let refresh_required = providers
        .surfaces
        .iter()
        .filter(|surface| surface.release_state == ProfileReleaseState::RefreshRequired)
        .map(|surface| surface.surface_id)
        .collect::<Vec<_>>();
    if !refresh_required.is_empty() {
        blockers.push(ReleaseBlocker::new(
            "providers.evidence_refresh",
            refresh_required,
        ));
    }
    let rights_blocked = providers
        .surfaces
        .iter()
        .filter(|surface| surface.release_state == ProfileReleaseState::RightsBlocked)
        .map(|surface| surface.surface_id)
        .collect::<Vec<_>>();
    if !rights_blocked.is_empty() {
        blockers.push(ReleaseBlocker::new(
            "providers.rights_blocked",
            rights_blocked,
        ));
    }
    let treasury_rights_limited = providers
        .surfaces
        .iter()
        .filter(|surface| {
            surface.surface_id == "treasury.daily-rates-xml"
                && surface.release_state == ProfileReleaseState::RightsLimited
        })
        .map(|surface| surface.surface_id)
        .collect::<Vec<_>>();
    if !treasury_rights_limited.is_empty() {
        blockers.push(ReleaseBlocker::new(
            "providers.treasury_xml_durable_rights",
            treasury_rights_limited,
        ));
    }
}

fn endpoint_report(config: &AppConfig) -> EndpointReport {
    EndpointReport {
        deny_by_default: true,
        coinbase: ConfiguredEndpointReport {
            configured: config.coinbase().is_some(),
            allowlisted: config.coinbase().is_some(),
            endpoint: config.coinbase().map(|profile| profile.endpoint()),
        },
        kraken: ConfiguredEndpointReport {
            configured: config.kraken().is_some(),
            allowlisted: config.kraken().is_some(),
            endpoint: config.kraken().map(|profile| profile.endpoint()),
        },
        live_execution_endpoint_configured: false,
    }
}

fn classify_path_error(error: &PathError) -> &'static str {
    match error {
        PathError::Io { source, .. } if source.kind() == io::ErrorKind::NotFound => {
            "storage.layout_incomplete"
        }
        PathError::Io { source, .. } if source.kind() == io::ErrorKind::PermissionDenied => {
            "storage.permission_denied"
        }
        PathError::ReadOnly => "storage.read_only",
        PathError::PreparedRootChanged => "storage.identity_changed",
        PathError::CatalogAlreadyLocked => "storage.catalog_writer_active",
        PathError::ArtifactRootUnavailable
        | PathError::ControlRootUnavailable
        | PathError::CatalogLocationUnavailable => "storage.layout_incomplete",
        PathError::CatalogRestoreConflict | PathError::CatalogRestoreIndeterminate => {
            "storage.catalog_restore_incomplete"
        }
        PathError::Io { .. } => "storage.io_unavailable",
    }
}

fn classify_catalog_error(error: &market_squawk_data::CatalogError) -> &'static str {
    use market_squawk_data::CatalogError;

    match error {
        CatalogError::UnsafePath => "catalog.unsafe_path",
        CatalogError::UnsafeJournalMode => "catalog.unsafe_journal_mode",
        CatalogError::ForeignCatalog => "catalog.foreign_database",
        CatalogError::MigrationRegistryMismatch | CatalogError::MigrationDigestMismatch { .. } => {
            "catalog.migration_mismatch"
        }
        CatalogError::CorruptCatalog
        | CatalogError::Serialization(_)
        | CatalogError::ProviderCapability(_)
        | CatalogError::PublicConfiguration(_)
        | CatalogError::OnboardingState(_) => "catalog.invalid_state",
        CatalogError::Sqlite(error) if error.sqlite_error_code().is_some() => {
            "catalog.sqlite_unavailable"
        }
        _ => "catalog.read_unavailable",
    }
}
