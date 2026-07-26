//! Authorized, exact-head provider acceptance evidence.

use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    path::{Component, Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::{Context as _, Result, anyhow, bail};
use chrono::{SecondsFormat, Utc};
use market_squawk_data::CatalogLimit;
use market_squawk_domain::{EvidenceDigest, SourceIdentifier};
use market_squawk_services::{
    JsonStructureLimits, RequestContext, RequestId, ServiceError, ServiceLimits,
};
use market_squawk_sources::{DataUseOperation, OnboardingState};
use serde::Serialize;
use serde_json::{Map, Value, json};
use tokio_util::sync::CancellationToken;

use super::{
    identity::RepositoryIdentity,
    io::{
        PublishedReport, StableFileIdentity, hash_stable_file, publish_report_with_identity_barrier,
    },
};
use crate::{
    AppConfig, LocalProduct, OnboardingSessionView, ProviderActivationLease, ProviderProfileView,
    application::{Application, ResearchProviderRuntimeGeneration},
    cli::ReleaseProviderArguments,
};

const REPORT_KIND: &str = "market_squawk.release.providers";
const EXTERNAL_NETWORK_GATE: &str = "MARKET_SQUAWK_EXTERNAL_NETWORK";
const PROVIDER_TERMS_GATE: &str = "MARKET_SQUAWK_PROVIDER_TERMS_ACCEPTED";
const MAXIMUM_EXECUTABLE_BYTES: u64 = 1024 * 1024 * 1024;
const MAXIMUM_PROVIDER_SESSIONS: usize = 32;
const REQUEST_MAXIMUM_BYTES: usize = 1024 * 1024;
const REQUEST_MAXIMUM_ITEMS: usize = 1024;
const APPLICATION_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const LIVE_START_TIMEOUT: Duration = Duration::from_secs(90);
const LIVE_QUALIFICATION_TIMEOUT: Duration = Duration::from_secs(120);
const LIVE_POLL_INTERVAL: Duration = Duration::from_millis(250);
const SHUTDOWN_BARRIER_MARGIN: Duration = Duration::from_secs(5);

const COINBASE_PUBLIC: &str = "coinbase.public-market-data";
const COINBASE_DIRECT: &str = "coinbase.exchange-direct-market-data";
const KRAKEN_PUBLIC: &str = "kraken.spot-public-market-data";
const SEC_EDGAR: &str = "sec.edgar-public";
const FRED_ALFRED: &str = "fred-alfred.api-v1-v2";
const BLS_PUBLIC: &str = "bls.v1-unregistered";
const BLS_REGISTERED: &str = "bls.v2-registered";
const TREASURY_XML: &str = "treasury.daily-rates-xml";
const TREASURY_FISCAL: &str = "treasury.fiscal-data";

const ADMITTED_SURFACES: [&str; 9] = [
    COINBASE_PUBLIC,
    COINBASE_DIRECT,
    KRAKEN_PUBLIC,
    SEC_EDGAR,
    FRED_ALFRED,
    BLS_PUBLIC,
    BLS_REGISTERED,
    TREASURY_XML,
    TREASURY_FISCAL,
];

const DATA_USE_OPERATIONS: [DataUseOperation; 6] = [
    DataUseOperation::Retrieve,
    DataUseOperation::Display,
    DataUseOperation::Persist,
    DataUseOperation::ModelTraining,
    DataUseOperation::Export,
    DataUseOperation::Redistribute,
];

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct ProviderEvidence {
    schema_version: u32,
    repository: RepositoryIdentity,
    executable: StableFileIdentity,
    collected_at: String,
    requirements: EvidenceRequirements,
    selected_surfaces: Vec<String>,
    surfaces: Vec<SurfaceEvidence>,
    restart_recovery: RestartRecoveryEvidence,
    direct_verified_action: DirectActionEvidence,
    fred_alfred_rights: FredAlfredRightsEvidence,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct EvidenceRequirements {
    external_network_authorized: bool,
    provider_terms_accepted: bool,
    direct_verified_action_required: bool,
    fred_alfred_rights_required: bool,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct SurfaceEvidence {
    surface_id: String,
    profile: ProviderProfileView,
    session: SessionEvidence,
    activation: ActivationEvidence,
    research_runtime: Option<ResearchRuntimeEvidence>,
    live_runtime: Option<LiveRuntimeEvidence>,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct SessionEvidence {
    session_id: String,
    surface_id: String,
    state: OnboardingState,
    credential_configured: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ActivationEvidence {
    session_id: String,
    capability_revision: u64,
    capability_digest: EvidenceDigest,
    rights_decision_digest: EvidenceDigest,
    public_configuration_digest: EvidenceDigest,
    account_binding_digest: Option<EvidenceDigest>,
    verification_response_digest: Option<EvidenceDigest>,
    runtime_response_digest: EvidenceDigest,
    authority_effective_at_unix_nanos: i64,
    verification_expires_at_unix_nanos: Option<i64>,
    data_use_admission: BTreeMap<&'static str, bool>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ResearchRuntimeEvidence {
    source_id: String,
    session_id: String,
    capability_revision: u64,
    capability_digest: EvidenceDigest,
    rights_authorization_digest: EvidenceDigest,
    runtime_generation_digest: EvidenceDigest,
    authority_effective_at_unix_nanos: i64,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct LiveRuntimeEvidence {
    expected_quality: &'static str,
    start: Value,
    bot_status: Value,
    source_status: Value,
    source_coverage: Value,
    source_health: Value,
    orders: Value,
    fills: Value,
    reconciliation: Value,
    stop: Value,
    action_completed: bool,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct RestartRecoveryEvidence {
    completed: bool,
    recovered_surfaces: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct DirectActionEvidence {
    required: bool,
    selected: bool,
    completed: bool,
    order_count: usize,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct FredAlfredRightsEvidence {
    required: bool,
    selected: bool,
    persistence_admitted: bool,
    model_training_admitted: bool,
    admitted: bool,
}

#[derive(Clone, Debug)]
struct RecoveryExpectation {
    surface_id: SourceIdentifier,
    activation: ActivationEvidence,
    research_runtime: Option<ResearchRuntimeEvidence>,
}

pub(super) async fn run(config: AppConfig, arguments: ReleaseProviderArguments) -> Result<Value> {
    require_exact_repository_arguments(&arguments)?;
    require_gate(EXTERNAL_NETWORK_GATE)?;
    require_gate(PROVIDER_TERMS_GATE)?;
    let selected = admit_selected_surfaces(&arguments)?;
    let output_directory = admit_new_output_directory(&arguments.output)?;
    let repository = RepositoryIdentity::admit(&arguments.repository)?;
    if !repository.clean {
        bail!("provider release evidence requires a clean repository");
    }
    let executable_path = env::current_exe().context("running executable path is unavailable")?;
    let executable = hash_stable_file(&executable_path, MAXIMUM_EXECUTABLE_BYTES)?;
    let shutdown_timeout = config
        .source_shutdown()
        .checked_add(SHUTDOWN_BARRIER_MARGIN)
        .ok_or_else(|| anyhow!("provider shutdown deadline overflow"))?;

    let product = LocalProduct::try_new(config.clone())
        .context("provider evidence could not initialize the local product")?;
    let collection =
        collect_provider_evidence(&product, &selected, &arguments, shutdown_timeout).await;
    let shutdown = shutdown_product(&product, shutdown_timeout).await;
    let (mut surfaces, expectations, direct_action, fred_rights) = match (collection, shutdown) {
        (Ok(collection), Ok(())) => collection,
        (Err(collection_error), Ok(())) => return Err(collection_error),
        (Ok(_), Err(shutdown_error)) => return Err(shutdown_error),
        (Err(collection_error), Err(shutdown_error)) => {
            bail!(
                "provider evidence failed and shutdown was incomplete: collection={collection_error:#}; shutdown={shutdown_error:#}"
            )
        }
    };
    drop(product);

    let recovered = LocalProduct::try_new(config)
        .context("provider evidence could not reconstruct the local product")?;
    let recovery = verify_restart_recovery(&recovered, &expectations);
    let recovered_shutdown = shutdown_product(&recovered, shutdown_timeout).await;
    match (recovery, recovered_shutdown) {
        (Ok(()), Ok(())) => {}
        (Err(recovery_error), Ok(())) => return Err(recovery_error),
        (Ok(()), Err(shutdown_error)) => return Err(shutdown_error),
        (Err(recovery_error), Err(shutdown_error)) => {
            bail!(
                "provider restart recovery failed and shutdown was incomplete: recovery={recovery_error:#}; shutdown={shutdown_error:#}"
            )
        }
    }
    drop(recovered);

    let final_executable = hash_stable_file(&executable_path, MAXIMUM_EXECUTABLE_BYTES)?;
    if final_executable != executable {
        bail!("provider evidence executable changed while acceptance was collected");
    }
    surfaces.sort_by(|left, right| left.surface_id.cmp(&right.surface_id));
    repository.verify_unchanged()?;
    let payload = ProviderEvidence {
        schema_version: 1,
        repository: repository.clone(),
        executable,
        collected_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
        requirements: EvidenceRequirements {
            external_network_authorized: true,
            provider_terms_accepted: true,
            direct_verified_action_required: arguments.require_direct_verified_action,
            fred_alfred_rights_required: arguments.require_fred_alfred_rights,
        },
        selected_surfaces: selected.iter().map(|value| (*value).to_owned()).collect(),
        surfaces,
        restart_recovery: RestartRecoveryEvidence {
            completed: true,
            recovered_surfaces: selected.iter().map(|value| (*value).to_owned()).collect(),
        },
        direct_verified_action: direct_action,
        fred_alfred_rights: fred_rights,
    };
    let output = create_output_directory(&output_directory)?;
    let report_path = output.join("provider-evidence.json");
    let published =
        match publish_report_with_identity_barrier(&report_path, REPORT_KIND, &payload, || {
            repository.verify_unchanged()
        }) {
            Ok(published) => published,
            Err(error) => {
                remove_empty_output_directory(&output);
                return Err(error);
            }
        };
    Ok(publication_value(&published))
}

async fn collect_provider_evidence(
    product: &LocalProduct,
    selected: &[&'static str],
    arguments: &ReleaseProviderArguments,
    shutdown_timeout: Duration,
) -> Result<(
    Vec<SurfaceEvidence>,
    Vec<RecoveryExpectation>,
    DirectActionEvidence,
    FredAlfredRightsEvidence,
)> {
    let onboarding = product.provider_onboarding();
    let activation = product.provider_activation();
    let profiles = onboarding
        .profiles()
        .into_iter()
        .map(|profile| (profile.id().to_owned(), profile))
        .collect::<BTreeMap<_, _>>();
    let mut sessions = onboarding
        .current_sessions(CatalogLimit::new(MAXIMUM_PROVIDER_SESSIONS)?)?
        .into_iter()
        .map(|session| (session.surface_id().to_owned(), session))
        .collect::<BTreeMap<_, _>>();
    preflight_selected_authority(
        selected,
        &sessions,
        onboarding.as_ref(),
        activation.as_ref(),
    )?;
    let mut surfaces = Vec::new();
    let mut expectations = Vec::new();
    let mut direct_order_count = 0_usize;
    let mut fred_persistence = false;
    let mut fred_training = false;

    for surface_id in selected {
        let profile = profiles
            .get(*surface_id)
            .cloned()
            .ok_or_else(|| anyhow!("selected provider surface is not built in: {surface_id}"))?;
        let session = ensure_active_session(product, surface_id, &mut sessions).await?;
        let lease = onboarding
            .activation_lease(session.session_id())
            .with_context(|| {
                format!("active provider lease could not be recovered: {surface_id}")
            })?;
        require_matching_lease(surface_id, &session, &lease)?;
        let activation_evidence = activation_evidence(&lease);
        let research_runtime = if requires_research_runtime(surface_id) {
            let profile_id = SourceIdentifier::try_from(*surface_id)?;
            let runtime = activation
                .research_runtime_generation(&profile_id)?
                .ok_or_else(|| anyhow!("provider research runtime is not active: {surface_id}"))?;
            Some(research_runtime_evidence(&runtime)?)
        } else {
            None
        };
        if *surface_id == FRED_ALFRED {
            fred_persistence = lease.admits(DataUseOperation::Persist);
            fred_training = lease.admits(DataUseOperation::ModelTraining);
        }
        let live_runtime = if is_live_surface(surface_id) {
            let evidence = exercise_live_surface(
                product.application().as_ref(),
                surface_id,
                session.session_id().to_string(),
                arguments.require_direct_verified_action && *surface_id == COINBASE_DIRECT,
                shutdown_timeout,
            )
            .await?;
            if *surface_id == COINBASE_DIRECT {
                direct_order_count = evidence.orders.as_array().map_or(0, std::vec::Vec::len);
            }
            Some(evidence)
        } else {
            None
        };
        expectations.push(RecoveryExpectation {
            surface_id: lease.surface_id().clone(),
            activation: activation_evidence.clone(),
            research_runtime: research_runtime.clone(),
        });
        surfaces.push(SurfaceEvidence {
            surface_id: (*surface_id).to_owned(),
            profile,
            session: SessionEvidence {
                session_id: session.session_id().to_string(),
                surface_id: session.surface_id().to_owned(),
                state: session.state(),
                credential_configured: session.credential_stored(),
            },
            activation: activation_evidence,
            research_runtime,
            live_runtime,
        });
    }

    let direct_selected = selected.contains(&COINBASE_DIRECT);
    let direct_completed = direct_order_count > 0;
    if arguments.require_direct_verified_action && !direct_completed {
        bail!("required DirectVerified risk-approved paper action was not observed");
    }
    let fred_selected = selected.contains(&FRED_ALFRED);
    let fred_admitted = fred_persistence && fred_training;
    if arguments.require_fred_alfred_rights && !fred_admitted {
        bail!("required FRED and ALFRED persistence and model-training rights are not admitted");
    }
    Ok((
        surfaces,
        expectations,
        DirectActionEvidence {
            required: arguments.require_direct_verified_action,
            selected: direct_selected,
            completed: direct_completed,
            order_count: direct_order_count,
        },
        FredAlfredRightsEvidence {
            required: arguments.require_fred_alfred_rights,
            selected: fred_selected,
            persistence_admitted: fred_persistence,
            model_training_admitted: fred_training,
            admitted: fred_admitted,
        },
    ))
}

async fn ensure_active_session(
    product: &LocalProduct,
    surface_id: &'static str,
    sessions: &mut BTreeMap<String, OnboardingSessionView>,
) -> Result<OnboardingSessionView> {
    if let Some(session) = sessions.get(surface_id) {
        if session.state() != OnboardingState::ActiveScoped {
            bail!(
                "provider session is not active for {surface_id}: {:?}",
                session.state()
            );
        }
        return Ok(session.clone());
    }
    if !matches!(surface_id, COINBASE_PUBLIC | KRAKEN_PUBLIC | TREASURY_XML) {
        bail!(
            "provider surface requires a portal-prepared active session before release evidence: {surface_id}"
        );
    }
    let session = product
        .provider_onboarding()
        .start(
            crate::StartOnboardingRequest::try_new(surface_id, None, None)?,
            CancellationToken::new(),
        )
        .await
        .with_context(|| format!("provider onboarding probe failed: {surface_id}"))?;
    if session.state() != OnboardingState::ActiveScoped {
        bail!(
            "provider onboarding did not establish active authority for {surface_id}: {:?}",
            session.state()
        );
    }
    sessions.insert(surface_id.to_owned(), session.clone());
    Ok(session)
}

fn preflight_selected_authority(
    selected: &[&str],
    sessions: &BTreeMap<String, OnboardingSessionView>,
    onboarding: &crate::ProviderOnboardingService,
    activation: &crate::ProviderAdapterActivation,
) -> Result<()> {
    for surface_id in selected {
        let Some(session) = sessions.get(*surface_id) else {
            if matches!(*surface_id, COINBASE_PUBLIC | KRAKEN_PUBLIC | TREASURY_XML) {
                continue;
            }
            bail!(
                "provider surface requires a portal-prepared active session before release evidence: {surface_id}"
            );
        };
        if session.state() != OnboardingState::ActiveScoped {
            bail!(
                "provider session is not active for {surface_id}: {:?}",
                session.state()
            );
        }
        let lease = onboarding.activation_lease(session.session_id())?;
        require_matching_lease(surface_id, session, &lease)?;
        if requires_research_runtime(surface_id) {
            let profile_id = SourceIdentifier::try_from(*surface_id)?;
            if activation
                .research_runtime_generation(&profile_id)?
                .is_none()
            {
                bail!("provider research runtime is not active: {surface_id}");
            }
        }
    }
    Ok(())
}

fn require_matching_lease(
    surface_id: &str,
    session: &OnboardingSessionView,
    lease: &ProviderActivationLease,
) -> Result<()> {
    if lease.surface_id().as_str() != surface_id || lease.session_id() != session.session_id() {
        bail!("provider activation lease does not match its durable session");
    }
    Ok(())
}

fn activation_evidence(lease: &ProviderActivationLease) -> ActivationEvidence {
    let data_use_admission = DATA_USE_OPERATIONS
        .into_iter()
        .map(|operation| (operation.evidence_name(), lease.admits(operation)))
        .collect();
    ActivationEvidence {
        session_id: lease.session_id().to_string(),
        capability_revision: lease.capability_revision().get(),
        capability_digest: lease.capability_digest(),
        rights_decision_digest: lease.rights_decision_digest(),
        public_configuration_digest: lease.public_configuration_digest(),
        account_binding_digest: lease.account_digest(),
        verification_response_digest: lease.verification_evidence_digest(),
        runtime_response_digest: lease.runtime_evidence_digest(),
        authority_effective_at_unix_nanos: lease.authority_effective_at().unix_nanos(),
        verification_expires_at_unix_nanos: lease
            .verification_expires_at()
            .map(market_squawk_domain::Timestamp::unix_nanos),
        data_use_admission,
    }
}

fn research_runtime_evidence(
    runtime: &ResearchProviderRuntimeGeneration,
) -> Result<ResearchRuntimeEvidence> {
    Ok(ResearchRuntimeEvidence {
        source_id: runtime.metadata().source_id().as_str().to_owned(),
        session_id: runtime.session_id().to_string(),
        capability_revision: runtime.capability_revision().get(),
        capability_digest: runtime.capability_digest(),
        rights_authorization_digest: runtime.rights_authorization_evidence(),
        runtime_generation_digest: runtime.generation_digest()?,
        authority_effective_at_unix_nanos: runtime.authority_effective_at().unix_nanos(),
    })
}

async fn exercise_live_surface(
    application: &Application,
    surface_id: &'static str,
    session_id: String,
    require_action: bool,
    shutdown_timeout: Duration,
) -> Result<LiveRuntimeEvidence> {
    let (provider, expected_quality) = match surface_id {
        COINBASE_PUBLIC => ("coinbase", "direct_unverified"),
        COINBASE_DIRECT => ("coinbase-direct", "direct_verified"),
        KRAKEN_PUBLIC => ("kraken", "direct_unverified"),
        _ => bail!("selected provider surface is not a live runtime"),
    };
    let mut start_arguments = json_object(json!({
        "provider": provider,
        "initialCash": "100000",
        "feeBasisPoints": 100,
    }))?;
    if surface_id == COINBASE_DIRECT {
        start_arguments.insert("providerSessionId".to_owned(), Value::String(session_id));
    }
    let run = async {
        let start = invoke(
            application,
            "Bot.Start",
            start_arguments,
            LIVE_START_TIMEOUT,
        )
        .await?;
        let source_status =
            wait_for_source_quality(application, surface_id, expected_quality).await?;
        let bot_status = invoke(
            application,
            "Bot.GetStatus",
            Map::new(),
            APPLICATION_REQUEST_TIMEOUT,
        )
        .await?;
        let source_coverage = invoke(
            application,
            "Source.GetCoverage",
            source_arguments(surface_id),
            APPLICATION_REQUEST_TIMEOUT,
        )
        .await?;
        let source_health = invoke(
            application,
            "Source.GetHealth",
            source_arguments(surface_id),
            APPLICATION_REQUEST_TIMEOUT,
        )
        .await?;
        require_live_source_evidence(
            surface_id,
            expected_quality,
            &source_status,
            &source_coverage,
            &source_health,
        )?;
        let orders = if require_action {
            wait_for_orders(application).await?
        } else {
            invoke(
                application,
                "Execution.GetOrders",
                Map::new(),
                APPLICATION_REQUEST_TIMEOUT,
            )
            .await?
        };
        if surface_id != COINBASE_DIRECT
            && orders.as_array().is_some_and(|orders| !orders.is_empty())
        {
            bail!("non-execution-quality provider produced an automated paper order");
        }
        let fills = invoke(
            application,
            "Execution.GetFills",
            Map::new(),
            APPLICATION_REQUEST_TIMEOUT,
        )
        .await?;
        let reconciliation = invoke(
            application,
            "Execution.Reconcile",
            Map::new(),
            APPLICATION_REQUEST_TIMEOUT,
        )
        .await?;
        let action_completed = orders.as_array().is_some_and(|orders| !orders.is_empty());
        Ok((
            expected_quality,
            start,
            bot_status,
            source_status,
            source_coverage,
            source_health,
            orders,
            fills,
            reconciliation,
            action_completed,
        ))
    }
    .await;
    let stop = invoke(
        application,
        "Bot.Stop",
        json_object(json!({"reason": "provider release evidence completed"}))?,
        shutdown_timeout,
    )
    .await;
    let (
        expected_quality,
        start,
        bot_status,
        source_status,
        source_coverage,
        source_health,
        orders,
        fills,
        reconciliation,
        action_completed,
    ) = match (run, stop.as_ref()) {
        (Ok(evidence), Ok(_)) => evidence,
        (Err(run_error), Ok(_)) => return Err(run_error),
        (Ok(_), Err(stop_error)) => {
            return Err(anyhow!("provider runtime stop failed: {stop_error:#}"));
        }
        (Err(run_error), Err(stop_error)) => {
            bail!(
                "provider runtime evidence and stop both failed: evidence={run_error:#}; stop={stop_error:#}"
            )
        }
    };
    Ok(LiveRuntimeEvidence {
        expected_quality,
        start,
        bot_status,
        source_status,
        source_coverage,
        source_health,
        orders,
        fills,
        reconciliation,
        stop: stop?,
        action_completed,
    })
}

async fn wait_for_source_quality(
    application: &Application,
    surface_id: &str,
    expected_quality: &str,
) -> Result<Value> {
    let deadline = Instant::now()
        .checked_add(LIVE_QUALIFICATION_TIMEOUT)
        .ok_or_else(|| anyhow!("provider qualification deadline overflow"))?;
    loop {
        let observation = match invoke(
            application,
            "Source.GetStatus",
            source_arguments(surface_id),
            APPLICATION_REQUEST_TIMEOUT,
        )
        .await
        {
            Ok(status) => {
                if status_has_quality(&status, surface_id, expected_quality) {
                    return Ok(status);
                }
                "source status did not contain the required quality".to_owned()
            }
            Err(error) => format!("{error:#}"),
        };
        if Instant::now() >= deadline {
            bail!(
                "provider runtime did not reach {expected_quality} before its deadline: {observation}"
            );
        }
        tokio::time::sleep(LIVE_POLL_INTERVAL).await;
    }
}

fn status_has_quality(status: &Value, surface_id: &str, expected_quality: &str) -> bool {
    status.as_array().is_some_and(|rows| {
        let matching = rows
            .iter()
            .filter(|row| row.pointer("/profile/id").and_then(Value::as_str) == Some(surface_id))
            .collect::<Vec<_>>();
        !matching.is_empty()
            && matching.iter().all(|row| {
                row.pointer("/profile/id").and_then(Value::as_str) == Some(surface_id)
                    && row.pointer("/runtime/state").and_then(Value::as_str) == Some("active")
                    && row.pointer("/runtime/quality").and_then(Value::as_str)
                        == Some(expected_quality)
            })
    })
}

fn require_live_source_evidence(
    surface_id: &str,
    expected_quality: &str,
    status: &Value,
    coverage: &Value,
    health: &Value,
) -> Result<()> {
    if !status_has_quality(status, surface_id, expected_quality)
        || !rows_match_surface_state(
            coverage,
            surface_id,
            "/surfaceId",
            "/runtimeCoverage/state",
            "established",
        )
        || !rows_match_surface_state(
            health,
            surface_id,
            "/surfaceId",
            "/runtimeHealth/state",
            "active",
        )
        || !health.as_array().is_some_and(|rows| {
            rows.iter().all(|row| {
                row.pointer("/runtimeHealth/quality")
                    .and_then(Value::as_str)
                    == Some(expected_quality)
            })
        })
    {
        bail!("provider runtime status, coverage, or health evidence is incomplete");
    }
    Ok(())
}

fn rows_match_surface_state(
    value: &Value,
    surface_id: &str,
    identity_pointer: &str,
    state_pointer: &str,
    expected_state: &str,
) -> bool {
    value.as_array().is_some_and(|rows| {
        !rows.is_empty()
            && rows.iter().all(|row| {
                row.pointer(identity_pointer).and_then(Value::as_str) == Some(surface_id)
                    && row.pointer(state_pointer).and_then(Value::as_str) == Some(expected_state)
            })
    })
}

async fn wait_for_orders(application: &Application) -> Result<Value> {
    let deadline = Instant::now()
        .checked_add(LIVE_QUALIFICATION_TIMEOUT)
        .ok_or_else(|| anyhow!("provider action deadline overflow"))?;
    loop {
        let orders = invoke(
            application,
            "Execution.GetOrders",
            Map::new(),
            APPLICATION_REQUEST_TIMEOUT,
        )
        .await?;
        if orders.as_array().is_some_and(|orders| !orders.is_empty()) {
            return Ok(orders);
        }
        if Instant::now() >= deadline {
            bail!("DirectVerified runtime did not produce a paper order before its deadline");
        }
        tokio::time::sleep(LIVE_POLL_INTERVAL).await;
    }
}

async fn invoke(
    application: &Application,
    operation: &str,
    mut arguments: Map<String, Value>,
    timeout: Duration,
) -> Result<Value> {
    arguments.insert(
        "resultLimits".to_owned(),
        json!({
            "maximumItems": REQUEST_MAXIMUM_ITEMS,
            "maximumBytes": REQUEST_MAXIMUM_BYTES,
        }),
    );
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| anyhow!("provider request deadline overflow"))?;
    let limits = ServiceLimits::try_new(
        REQUEST_MAXIMUM_BYTES,
        REQUEST_MAXIMUM_ITEMS,
        REQUEST_MAXIMUM_BYTES,
        REQUEST_MAXIMUM_ITEMS,
        JsonStructureLimits::try_new(32, REQUEST_MAXIMUM_BYTES, 4096, 4096)?,
    )?;
    let request_id = RequestId::try_string(format!(
        "release-provider-{}",
        uuid::Uuid::new_v4().simple()
    ))?;
    application
        .invoke(
            operation,
            arguments,
            RequestContext::new(request_id, CancellationToken::new(), deadline, limits),
        )
        .await
        .map(|result| result.structured_content().clone())
        .map_err(|error| application_error(operation, error))
}

fn application_error(operation: &str, error: ServiceError) -> anyhow::Error {
    anyhow!("provider application operation {operation} failed: {error}")
}

fn source_arguments(surface_id: &str) -> Map<String, Value> {
    let mut arguments = Map::new();
    arguments.insert("sourceCoverage".to_owned(), json!([surface_id]));
    arguments
}

fn json_object(value: Value) -> Result<Map<String, Value>> {
    value
        .as_object()
        .cloned()
        .ok_or_else(|| anyhow!("code-owned provider arguments are not an object"))
}

fn verify_restart_recovery(
    product: &LocalProduct,
    expectations: &[RecoveryExpectation],
) -> Result<()> {
    let onboarding = product.provider_onboarding();
    let activation = product.provider_activation();
    let sessions = onboarding
        .current_sessions(CatalogLimit::new(MAXIMUM_PROVIDER_SESSIONS)?)?
        .into_iter()
        .map(|session| (session.surface_id().to_owned(), session))
        .collect::<BTreeMap<_, _>>();
    for expected in expectations {
        let surface_id = expected.surface_id.as_str();
        let session = sessions.get(surface_id).ok_or_else(|| {
            anyhow!("provider session was not recovered after restart: {surface_id}")
        })?;
        if session.state() != OnboardingState::ActiveScoped
            || session.session_id().to_string() != expected.activation.session_id
        {
            bail!("provider session authority changed during restart: {surface_id}");
        }
        let lease = onboarding.activation_lease(session.session_id())?;
        if activation_evidence(&lease) != expected.activation {
            bail!("provider activation evidence changed during restart: {surface_id}");
        }
        match &expected.research_runtime {
            Some(expected_runtime) => {
                let runtime = activation
                    .research_runtime_generation(&expected.surface_id)?
                    .ok_or_else(|| {
                        anyhow!("provider research runtime was not recovered: {surface_id}")
                    })?;
                if research_runtime_evidence(&runtime)? != *expected_runtime {
                    bail!("provider research runtime changed during restart: {surface_id}");
                }
            }
            None => {
                if activation
                    .research_runtime_generation(&expected.surface_id)?
                    .is_some()
                {
                    bail!(
                        "unexpected provider research runtime appeared after restart: {surface_id}"
                    );
                }
            }
        }
    }
    Ok(())
}

async fn shutdown_product(product: &LocalProduct, timeout: Duration) -> Result<()> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| anyhow!("provider shutdown deadline overflow"))?;
    if !product.application().shutdown(deadline).await.is_complete() {
        bail!("provider evidence application shutdown was incomplete");
    }
    Ok(())
}

fn require_exact_repository_arguments(arguments: &ReleaseProviderArguments) -> Result<()> {
    if arguments.repository.head.is_none() || arguments.repository.tree.is_none() {
        bail!("provider release evidence requires exact --head and --tree");
    }
    Ok(())
}

fn require_gate(name: &str) -> Result<()> {
    if env::var(name).ok().as_deref() != Some("1") {
        bail!("provider release evidence requires {name}=1");
    }
    Ok(())
}

fn admit_selected_surfaces(arguments: &ReleaseProviderArguments) -> Result<Vec<&'static str>> {
    let requested = arguments
        .providers
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if requested.len() != arguments.providers.len() || requested.is_empty() {
        bail!("provider release evidence requires a nonempty unique surface set");
    }
    if requested
        .iter()
        .any(|surface| !ADMITTED_SURFACES.contains(surface))
    {
        bail!("provider release evidence accepts exact built-in surface identifiers only");
    }
    if arguments.require_direct_verified_action && !requested.contains(COINBASE_DIRECT) {
        bail!("DirectVerified action evidence requires the Coinbase Direct surface");
    }
    if arguments.require_fred_alfred_rights && !requested.contains(FRED_ALFRED) {
        bail!("FRED and ALFRED rights evidence requires the exact FRED/ALFRED surface");
    }
    Ok(ADMITTED_SURFACES
        .into_iter()
        .filter(|surface| requested.contains(surface))
        .collect())
}

fn is_live_surface(surface_id: &str) -> bool {
    matches!(
        surface_id,
        COINBASE_PUBLIC | COINBASE_DIRECT | KRAKEN_PUBLIC
    )
}

fn requires_research_runtime(surface_id: &str) -> bool {
    matches!(
        surface_id,
        SEC_EDGAR | FRED_ALFRED | BLS_PUBLIC | BLS_REGISTERED | TREASURY_FISCAL
    )
}

fn admit_new_output_directory(requested: &Path) -> Result<PathBuf> {
    if requested
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        bail!("provider evidence output contains parent traversal");
    }
    match fs::symlink_metadata(requested) {
        Ok(_) => bail!("provider evidence output directory already exists"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).context("provider evidence output path could not be inspected");
        }
    }
    let name = requested
        .file_name()
        .filter(|name| !name.is_empty())
        .ok_or_else(|| anyhow!("provider evidence output has no directory name"))?;
    let parent = requested
        .parent()
        .ok_or_else(|| anyhow!("provider evidence output has no parent"))?;
    let parent = if parent.as_os_str().is_empty() {
        env::current_dir().context("current directory is unavailable")?
    } else {
        parent.to_path_buf()
    };
    let metadata =
        fs::symlink_metadata(&parent).context("provider evidence output parent is unavailable")?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        bail!("provider evidence output parent is not a real directory");
    }
    Ok(parent
        .canonicalize()
        .context("provider evidence output parent cannot be canonicalized")?
        .join(name))
}

fn create_output_directory(path: &Path) -> Result<PathBuf> {
    fs::create_dir(path).context("provider evidence output directory could not be created")?;
    let result = (|| {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;

            fs::set_permissions(path, fs::Permissions::from_mode(0o700))
                .context("provider evidence output permissions could not be restricted")?;
        }
        let metadata = fs::symlink_metadata(path)
            .context("provider evidence output directory metadata is unavailable")?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            bail!("provider evidence output is not a real directory");
        }
        path.canonicalize()
            .context("provider evidence output directory cannot be canonicalized")
    })();
    if result.is_err() {
        remove_empty_output_directory(path);
    }
    result
}

fn remove_empty_output_directory(path: &Path) {
    let empty = fs::read_dir(path)
        .ok()
        .is_some_and(|mut entries| entries.next().is_none());
    if empty {
        let _ignored = fs::remove_dir(path);
    }
}

fn publication_value(published: &PublishedReport) -> Value {
    json!({
        "path": published.path,
        "sha256": published.sha256,
        "byte_count": published.byte_count,
    })
}
