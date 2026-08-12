//! Authorized, exact-head provider acceptance evidence.

use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    num::NonZeroU16,
    path::{Component, Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::{Context as _, Result, anyhow, bail};
use chrono::{SecondsFormat, Utc};
use market_squawk_adapter_bls::BlsSource;
use market_squawk_adapter_fred::FredSource;
use market_squawk_adapter_treasury::{
    TreasuryDailyRateFamily, TreasuryDailyRateQuery, TreasuryFiscalQuery,
};
use market_squawk_data::{
    CatalogLimit, DatasetId, FeatureLabelDataset, GenerationParentRelation, SourceOperation,
};
use market_squawk_domain::{
    AvailabilityEvidence, CalendarDate, DataQuality, DigestAlgorithm, EvidenceDigest,
    FundamentalConsolidation, FundamentalRestatementStatus, PayloadReference, ResearchObservation,
    SourceIdentifier,
};
use market_squawk_services::{
    JsonStructureLimits, RequestContext, RequestId, ServiceError, ServiceLimits,
};
use market_squawk_sources::{DataUseOperation, OnboardingState};
use serde::Serialize;
use serde_json::{Map, Value, json};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use super::{
    identity::RepositoryIdentity,
    io::{
        PublishedReport, StableFileIdentity, hash_stable_file, publish_report_with_identity_barrier,
    },
};
use crate::{
    AppConfig, LocalProduct, OnboardingSessionView, ProviderActivationLease, ProviderProfileView,
    application::{Application, ResearchProviderRuntimeGeneration, ResearchSourceDiscovery},
    cli::ReleaseProviderArguments,
};

const REPORT_KIND: &str = "market_squawk.release.providers";
const EXTERNAL_NETWORK_GATE: &str = "MARKET_SQUAWK_EXTERNAL_NETWORK";
const PROVIDER_TERMS_GATE: &str = "MARKET_SQUAWK_PROVIDER_TERMS_ACCEPTED";
const MAXIMUM_EXECUTABLE_BYTES: u64 = 1024 * 1024 * 1024;
const MAXIMUM_TRAINING_REQUEST_BYTES: u64 = 8 * 1024 * 1024;
const MAXIMUM_PROVIDER_SESSIONS: usize = 32;
const REQUEST_MAXIMUM_BYTES: usize = 1024 * 1024;
const REQUEST_MAXIMUM_ITEMS: usize = 1024;
const APPLICATION_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const LIVE_START_TIMEOUT: Duration = Duration::from_secs(90);
const LIVE_QUALIFICATION_TIMEOUT: Duration = Duration::from_secs(120);
const RESEARCH_ACCEPTANCE_TIMEOUT: Duration = Duration::from_secs(90);
const LIVE_POLL_INTERVAL: Duration = Duration::from_millis(250);

const COINBASE_PUBLIC: &str = "coinbase.public-market-data";
const COINBASE_DIRECT: &str = "coinbase.exchange-direct-market-data";
const KRAKEN_PUBLIC: &str = "kraken.spot-public-market-data";
const SEC_EDGAR: &str = "sec.edgar-public";
const SEC_SOURCE_ID: &str = "sec-sec.edgar-public";
const SEC_SUBMISSIONS_FAMILY: &str = "sec_submissions_filings";
const SEC_COMPANY_FACTS_FAMILY: &str = "sec_company_facts";
const SEC_SUBMISSIONS_OPERATION: &str = "Fundamental.GetFilings";
const SEC_COMPANY_FACTS_OPERATION: &str = "Fundamental.GetFacts";
const FRED_ALFRED: &str = "fred-alfred.api-v1-v2";
const BLS_PUBLIC: &str = "bls.v1-unregistered";
const BLS_REGISTERED: &str = "bls.v2-registered";
const TREASURY_XML: &str = "treasury.daily-rates-xml";
const TREASURY_FISCAL: &str = "treasury.fiscal-data";
const BLS_UNEMPLOYMENT_SERIES: &str = "LNS14000000";
const BLS_PUBLIC_MAXIMUM_ACCEPTANCE_ROWS: u64 = 10 * 13;
const BLS_REGISTERED_MAXIMUM_ACCEPTANCE_ROWS: u64 = 20 * 13;
const MAXIMUM_TREASURY_FISCAL_RELEASE_PAGES: usize = REQUEST_MAXIMUM_ITEMS - 1;

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
    parent_rights_authorization_digest: EvidenceDigest,
    rights_authorization_digest: EvidenceDigest,
    rights_authorization_expires_at_unix_nanos: Option<i64>,
    rights_subjects: Vec<String>,
    rights_operations: Vec<&'static str>,
    runtime_generation_digest: EvidenceDigest,
    authority_effective_at_unix_nanos: i64,
    publications: Vec<ResearchPublicationEvidence>,
    python_training: Option<PythonTrainingEvidence>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ResearchPublicationEvidence {
    surface_id: String,
    family: String,
    provider_dataset: String,
    source_object_id: String,
    source_payload_digest: EvidenceDigest,
    analytical_dataset_id: String,
    manifest_version: u64,
    manifest_content_hash: String,
    row_count: u64,
    total_bytes: u64,
    object_count: u64,
    lineage_digest: String,
    python_export_sha256: Option<String>,
    observation_query_row_count: u64,
    vintage_query_row_count: Option<u64>,
    series_ids: Vec<String>,
    temporal_semantics: ResearchPublicationTemporalSemantics,
    sec: Option<SecPublicationEvidence>,
    fred: Option<FredPublicationEvidence>,
    treasury_fiscal: Option<TreasuryFiscalPublicationEvidence>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct SecPublicationEvidence {
    cik: String,
    instrument_id: String,
    observation_kind: String,
    quality: String,
    query_operation: String,
    provenance_verified_rows: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct FredPublicationEvidence {
    series_id: String,
    realtime_start: String,
    realtime_end: String,
    provider_row_count: u64,
    pages: Vec<FredPageEvidence>,
    observation_query: QueryRowEvidence,
    vintage_query: QueryRowEvidence,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct FredPageEvidence {
    source_object_id: String,
    source_payload_digest: EvidenceDigest,
    offset: u64,
    limit: u64,
    returned_rows: u64,
    provider_row_count: u64,
    terminal: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct TreasuryFiscalPublicationEvidence {
    first_record_date: CalendarDate,
    last_record_date: CalendarDate,
    page_size: u16,
    query_digest: String,
    provider_row_count: u64,
    pages: Vec<TreasuryFiscalPageEvidence>,
    observation_query: QueryRowEvidence,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct TreasuryFiscalPageEvidence {
    source_object_id: String,
    source_payload_digest: EvidenceDigest,
    page_number: u64,
    request_digest: String,
    returned_rows: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct QueryRowEvidence {
    row_count: u64,
    content_sha256: String,
    rows: Vec<Value>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ResearchPublicationTemporalSemantics {
    EffectiveObservations,
    ProviderReportedVintages,
    LocallyObservedCurrentSnapshot,
    LocallyObservedSecDisclosure,
    TreasuryFiscalEffectiveObservations,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PythonTrainingEvidence {
    request_sha256: String,
    request_byte_count: u64,
    dataset_id: String,
    manifest_version: u64,
    manifest_content_hash: String,
    source_surface_id: String,
    source_parent_dataset_id: String,
    source_parent_manifest_version: u64,
    source_parent_content_hash: String,
    parents: Vec<PythonTrainingParentEvidence>,
    build_spec_digest: String,
    policy_digest: String,
    universe_digest: String,
    python_export_sha256: String,
    train_examples: usize,
    validation_examples: usize,
    test_examples: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PythonTrainingParentEvidence {
    dataset_id: String,
    manifest_version: u64,
    manifest_content_hash: String,
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
    parent_authorization_digest: Option<EvidenceDigest>,
    authorization_digest: Option<EvidenceDigest>,
    authorization_expires_at_unix_nanos: Option<i64>,
    exact_series: Vec<String>,
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
    let product = LocalProduct::try_new(config.clone())
        .context("provider evidence could not initialize the local product")?;
    let shutdown_timeout = product.application().shutdown_timeout();
    let collection =
        collect_provider_evidence(&product, &selected, &arguments, shutdown_timeout).await;
    let shutdown = shutdown_product(&product).await;
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
    let recovery = verify_restart_recovery(&recovered, &expectations).await;
    let recovered_shutdown = shutdown_product(&recovered).await;
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
        schema_version: 5,
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
    let mut fred_parent_authorization = None;
    let mut fred_authorization = None;
    let mut fred_authorization_expires_at = None;
    let mut fred_exact_series = Vec::new();

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
            let (publications, python_training) = if *surface_id == TREASURY_XML {
                let acceptance_year = product
                    .treasury_daily_rate_release_year()
                    .context("Treasury daily-rate activation does not cover all five families")?;
                (
                    exercise_treasury_research(product.application().as_ref(), acceptance_year)
                        .await?,
                    None,
                )
            } else if *surface_id == TREASURY_FISCAL {
                let query = product
                    .treasury_fiscal_release_query()
                    .context("Treasury Fiscal Data activation has no exact admitted query")?;
                (
                    exercise_treasury_fiscal_research(product.application().as_ref(), &query)
                        .await?,
                    None,
                )
            } else if *surface_id == SEC_EDGAR {
                let cik = admit_sec_release_cik(arguments)?;
                (
                    exercise_sec_research(product.application().as_ref(), cik).await?,
                    None,
                )
            } else if *surface_id == FRED_ALFRED {
                let (dataset, training_request) = admit_fred_release_inputs(arguments)?;
                let publications =
                    exercise_fred_research(product.application().as_ref(), &dataset).await?;
                let training = exercise_python_training(
                    product,
                    FRED_ALFRED,
                    "FRED/ALFRED",
                    training_request,
                    &publications,
                )
                .await?;
                (publications, Some(training))
            } else if matches!(*surface_id, BLS_PUBLIC | BLS_REGISTERED) {
                let (dataset, training_request) = admit_bls_release_inputs(arguments, surface_id)?;
                let publications =
                    exercise_bls_research(product.application().as_ref(), surface_id, &dataset)
                        .await?;
                let training = exercise_python_training(
                    product,
                    surface_id,
                    "BLS",
                    training_request,
                    &publications,
                )
                .await?;
                (publications, Some(training))
            } else {
                (Vec::new(), None)
            };
            Some(research_runtime_evidence(
                &runtime,
                publications,
                python_training,
            )?)
        } else {
            None
        };
        if *surface_id == FRED_ALFRED {
            let runtime = research_runtime
                .as_ref()
                .ok_or_else(|| anyhow!("FRED/ALFRED research runtime evidence is absent"))?;
            fred_persistence = runtime.rights_operations.contains(&"persist");
            fred_training = runtime.rights_operations.contains(&"train");
            fred_parent_authorization = Some(runtime.parent_rights_authorization_digest);
            fred_authorization = Some(runtime.rights_authorization_digest);
            fred_authorization_expires_at = runtime.rights_authorization_expires_at_unix_nanos;
            fred_exact_series.clone_from(&runtime.rights_subjects);
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
    let collected_at_unix_nanos = Utc::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("provider collection time is outside nanosecond range"))?;
    let fred_admitted = fred_selected
        && fred_persistence
        && fred_training
        && fred_parent_authorization.is_some_and(|digest| digest.bytes() != [0; 32])
        && fred_authorization.is_some_and(|digest| {
            digest.bytes() != [0; 32] && Some(digest) != fred_parent_authorization
        })
        && fred_authorization_expires_at.is_some_and(|expiry| expiry > collected_at_unix_nanos)
        && fred_exact_series.len() == 1;
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
            parent_authorization_digest: fred_parent_authorization,
            authorization_digest: fred_authorization,
            authorization_expires_at_unix_nanos: fred_authorization_expires_at,
            exact_series: fred_exact_series,
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
    if !matches!(surface_id, COINBASE_PUBLIC | KRAKEN_PUBLIC) {
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
    product
        .provider_onboarding()
        .activate(session.session_id(), CancellationToken::new())
        .await
        .with_context(|| format!("provider onboarding activation failed: {surface_id}"))?;
    let session = product
        .provider_onboarding()
        .resume(session.session_id())
        .with_context(|| {
            format!("provider onboarding state could not be reloaded: {surface_id}")
        })?;
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
            if matches!(*surface_id, COINBASE_PUBLIC | KRAKEN_PUBLIC) {
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
    publications: Vec<ResearchPublicationEvidence>,
    python_training: Option<PythonTrainingEvidence>,
) -> Result<ResearchRuntimeEvidence> {
    let rights_operations = [
        SourceOperation::Retrieve,
        SourceOperation::Display,
        SourceOperation::Persist,
        SourceOperation::Cache,
        SourceOperation::Redistribute,
        SourceOperation::Train,
    ]
    .into_iter()
    .filter(|operation| runtime.rights_admits(*operation))
    .map(source_operation_name)
    .collect();
    Ok(ResearchRuntimeEvidence {
        source_id: runtime.metadata().source_id().as_str().to_owned(),
        session_id: runtime.session_id().to_string(),
        capability_revision: runtime.capability_revision().get(),
        capability_digest: runtime.capability_digest(),
        parent_rights_authorization_digest: runtime.parent_rights_authorization_evidence(),
        rights_authorization_digest: runtime.rights_authorization_evidence(),
        rights_authorization_expires_at_unix_nanos: runtime
            .rights_authorization_expires_at()
            .map(market_squawk_domain::Timestamp::unix_nanos),
        rights_subjects: runtime
            .rights_exact_subjects()
            .map_or_else(Vec::new, |subjects| {
                subjects
                    .iter()
                    .map(|subject| subject.as_str().to_owned())
                    .collect()
            }),
        rights_operations,
        runtime_generation_digest: runtime.generation_digest()?,
        authority_effective_at_unix_nanos: runtime.authority_effective_at().unix_nanos(),
        publications,
        python_training,
    })
}

const fn source_operation_name(operation: SourceOperation) -> &'static str {
    match operation {
        SourceOperation::Retrieve => "retrieve",
        SourceOperation::Display => "display",
        SourceOperation::Persist => "persist",
        SourceOperation::Cache => "cache",
        SourceOperation::Redistribute => "redistribute",
        SourceOperation::Train => "train",
    }
}

async fn exercise_treasury_research(
    application: &Application,
    acceptance_year: u16,
) -> Result<Vec<ResearchPublicationEvidence>> {
    let mut evidence = Vec::new();
    for (family, dataset) in treasury_acceptance_datasets(acceptance_year)? {
        let dataset_text = dataset.as_str();
        let discovery = invoke(
            application,
            "Source.Discover",
            json_object(json!({
                "provider": TREASURY_XML,
                "dataset": dataset_text,
                "confirm": true,
                "sourceCoverage": [TREASURY_XML],
            }))?,
            RESEARCH_ACCEPTANCE_TIMEOUT,
        )
        .await?;
        let discovery = ResearchSourceDiscovery::from_publication(discovery)?;
        if discovery.profile().as_str() != TREASURY_XML
            || discovery.request().dataset() != &dataset
            || discovery.objects().len() != 1
            || !discovery.rights().persistence_operation_admitted()
        {
            bail!("Treasury discovery did not produce one persistence-authorized exact object");
        }
        let object = discovery
            .objects()
            .first()
            .ok_or_else(|| anyhow!("Treasury discovery object is absent"))?;
        let source_object = object.source_object();
        let source_object_id = source_object.object_id().as_str().to_owned();
        let source_payload_digest = source_object.evidence().content_digest();
        let ingestion = invoke(
            application,
            "Research.IngestSource",
            json_object(json!({
                "provider": TREASURY_XML,
                "object": source_object_id,
                "dataset": dataset_text,
                "discoveryReceipt": object.discovery_receipt(),
                "confirm": true,
                "sourceCoverage": [TREASURY_XML],
            }))?,
            RESEARCH_ACCEPTANCE_TIMEOUT,
        )
        .await?;
        let mut publication = parse_research_publication(
            TREASURY_XML,
            family,
            dataset_text,
            source_object_id,
            source_payload_digest,
            &ingestion,
        )?;
        publication.observation_query_row_count =
            verify_queryable_publication(application, &publication).await?;
        evidence.push(publication);
    }
    Ok(evidence)
}

async fn exercise_treasury_fiscal_research(
    application: &Application,
    query: &TreasuryFiscalQuery,
) -> Result<Vec<ResearchPublicationEvidence>> {
    let dataset = query
        .dataset()
        .context("Treasury Fiscal Data provider dataset is invalid")?;
    let analytical_dataset = query
        .analytical_dataset()
        .context("Treasury Fiscal Data analytical dataset is invalid")?;
    DatasetId::try_from(analytical_dataset.as_str())
        .context("Treasury Fiscal Data analytical dataset identity is invalid")?;
    let discovery = invoke(
        application,
        "Source.Discover",
        json_object(json!({
            "provider": TREASURY_FISCAL,
            "dataset": dataset.as_str(),
            "confirm": true,
            "sourceCoverage": [TREASURY_FISCAL],
        }))?,
        RESEARCH_ACCEPTANCE_TIMEOUT,
    )
    .await?;
    let discovery = ResearchSourceDiscovery::from_publication(discovery)?;
    if discovery.profile().as_str() != TREASURY_FISCAL
        || discovery.request().dataset() != &dataset
        || discovery.objects().is_empty()
        || discovery.objects().len() > MAXIMUM_TREASURY_FISCAL_RELEASE_PAGES
        || !discovery.rights().persistence_operation_admitted()
    {
        bail!(
            "Treasury Fiscal Data discovery did not produce a complete bounded \
             persistence-authorized page chain"
        );
    }

    let mut pages = Vec::new();
    pages.try_reserve_exact(discovery.objects().len())?;
    let mut final_publication = None;
    for (index, object) in discovery.objects().iter().enumerate() {
        let source_object = object.source_object();
        let page_number = index
            .checked_add(1)
            .ok_or_else(|| anyhow!("Treasury Fiscal Data page number overflow"))?;
        let (object_page, request_digest, payload_digest) =
            parse_treasury_fiscal_object_id(source_object.object_id().as_str())?;
        let expected_request = query
            .page(page_number)
            .context("Treasury Fiscal Data page request is invalid")?;
        if object_page != page_number
            || request_digest != expected_request.request_digest()
            || source_object.evidence().content_digest().algorithm() != DigestAlgorithm::Sha256
            || source_object.evidence().content_digest().bytes() != payload_digest
        {
            bail!("Treasury Fiscal Data discovery returned an invalid exact page identity");
        }
        let source_object_id = source_object.object_id().as_str().to_owned();
        let source_payload_digest = source_object.evidence().content_digest();
        let ingestion = invoke(
            application,
            "Research.IngestSource",
            json_object(json!({
                "provider": TREASURY_FISCAL,
                "object": source_object_id,
                "dataset": dataset.as_str(),
                "discoveryReceipt": object.discovery_receipt(),
                "confirm": true,
                "sourceCoverage": [TREASURY_FISCAL],
            }))?,
            RESEARCH_ACCEPTANCE_TIMEOUT,
        )
        .await?;
        let publication = parse_research_publication(
            TREASURY_FISCAL,
            "average_interest_rates_v2",
            dataset.as_str(),
            source_object_id.clone(),
            source_payload_digest,
            &ingestion,
        )?;
        if publication.analytical_dataset_id != analytical_dataset.as_str()
            || publication.object_count
                > u64::try_from(discovery.objects().len())
                    .context("Treasury Fiscal Data object count overflow")?
        {
            bail!("Treasury Fiscal Data publication is not bound to its exact provider query");
        }
        pages.push(TreasuryFiscalPageEvidence {
            source_object_id,
            source_payload_digest,
            page_number: u64::try_from(page_number)
                .context("Treasury Fiscal Data page number overflow")?,
            request_digest: lower_hex(request_digest),
            returned_rows: 0,
        });
        final_publication = Some(publication);
    }

    let mut publication =
        final_publication.ok_or_else(|| anyhow!("Treasury Fiscal Data publication is absent"))?;
    if publication.object_count
        != u64::try_from(pages.len()).context("Treasury Fiscal Data object count overflow")?
        || pages.last().is_none_or(|page| {
            page.source_object_id != publication.source_object_id
                || page.source_payload_digest != publication.source_payload_digest
        })
    {
        bail!("Treasury Fiscal Data final manifest does not cover the discovered page chain");
    }
    let observations = query_row_evidence(&query_publication(application, &publication).await?)?;
    if observations.row_count != publication.row_count {
        bail!("Treasury Fiscal Data query did not return the complete published row set");
    }
    let TreasuryFiscalQueryValidation { page_rows, series } =
        validate_treasury_fiscal_query_rows(&observations.rows, query, &pages)?;
    for page in &mut pages {
        let payload_digest = page.source_payload_digest.bytes();
        page.returned_rows = page_rows
            .get(&payload_digest)
            .copied()
            .filter(|rows| *rows > 0)
            .ok_or_else(|| anyhow!("Treasury Fiscal Data page has no published observations"))?;
    }
    let accounted_rows = pages.iter().try_fold(0_u64, |total, page| {
        total
            .checked_add(page.returned_rows)
            .ok_or_else(|| anyhow!("Treasury Fiscal Data row count overflow"))
    })?;
    if accounted_rows != observations.row_count {
        bail!("Treasury Fiscal Data page evidence does not account for every published row");
    }
    publication.observation_query_row_count = observations.row_count;
    publication.series_ids = series;
    publication.temporal_semantics =
        ResearchPublicationTemporalSemantics::TreasuryFiscalEffectiveObservations;
    publication.treasury_fiscal = Some(TreasuryFiscalPublicationEvidence {
        first_record_date: query.first_record_date(),
        last_record_date: query.last_record_date(),
        page_size: query.page_size().get(),
        query_digest: lower_hex(query.query_digest()),
        provider_row_count: observations.row_count,
        pages,
        observation_query: observations,
    });
    Ok(vec![publication])
}

async fn exercise_sec_research(
    application: &Application,
    cik: &str,
) -> Result<Vec<ResearchPublicationEvidence>> {
    let specifications = [
        (
            SEC_SUBMISSIONS_FAMILY,
            format!("sec.submissions.cik.{cik}"),
            format!("sec.submissions.composite.CIK{cik}"),
            SEC_SUBMISSIONS_OPERATION,
            "filing",
        ),
        (
            SEC_COMPANY_FACTS_FAMILY,
            format!("sec.company-facts.cik.{cik}"),
            format!("https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"),
            SEC_COMPANY_FACTS_OPERATION,
            "fundamental",
        ),
    ];
    let mut evidence = Vec::with_capacity(specifications.len());
    for (family, dataset_text, expected_object, operation, observation_kind) in specifications {
        let dataset = SourceIdentifier::try_from(dataset_text.as_str())
            .context("SEC release dataset identity is invalid")?;
        DatasetId::try_from(dataset.as_str())
            .context("SEC release analytical dataset identity is invalid")?;
        let discovery = invoke(
            application,
            "Source.Discover",
            json_object(json!({
                "provider": SEC_EDGAR,
                "dataset": dataset.as_str(),
                "confirm": true,
                "sourceCoverage": [SEC_EDGAR],
            }))?,
            RESEARCH_ACCEPTANCE_TIMEOUT,
        )
        .await?;
        let discovery = ResearchSourceDiscovery::from_publication(discovery)?;
        if discovery.profile().as_str() != SEC_EDGAR
            || discovery.request().dataset() != &dataset
            || discovery.objects().len() != 1
            || !discovery.rights().persistence_operation_admitted()
        {
            bail!("SEC discovery did not produce one persistence-authorized exact object");
        }
        let object = discovery
            .objects()
            .first()
            .ok_or_else(|| anyhow!("SEC discovery object is absent"))?;
        let source_object = object.source_object();
        let source_object_id = source_object.object_id().as_str().to_owned();
        if source_object_id != expected_object {
            bail!("SEC discovery returned an object outside the exact requested CIK dataset");
        }
        let source_payload_digest = source_object.evidence().content_digest();
        if source_payload_digest.algorithm() != DigestAlgorithm::Sha256
            || source_payload_digest.bytes() == [0; 32]
        {
            bail!("SEC discovery omitted exact SHA-256 source payload evidence");
        }
        let ingestion = invoke(
            application,
            "Research.IngestSource",
            json_object(json!({
                "provider": SEC_EDGAR,
                "object": source_object_id,
                "dataset": dataset.as_str(),
                "discoveryReceipt": object.discovery_receipt(),
                "confirm": true,
                "sourceCoverage": [SEC_EDGAR],
            }))?,
            RESEARCH_ACCEPTANCE_TIMEOUT,
        )
        .await?;
        let mut publication = parse_research_publication(
            SEC_EDGAR,
            family,
            dataset.as_str(),
            source_object_id,
            source_payload_digest,
            &ingestion,
        )?;
        if publication.analytical_dataset_id != dataset.as_str() {
            bail!("SEC analytical publication is not bound to its exact provider dataset");
        }
        let sec =
            verify_sec_publication(application, &publication, cik, operation, observation_kind)
                .await?;
        publication.observation_query_row_count = sec.provenance_verified_rows;
        publication.temporal_semantics =
            ResearchPublicationTemporalSemantics::LocallyObservedSecDisclosure;
        publication.sec = Some(sec);
        evidence.push(publication);
    }
    if evidence.len() != 2
        || evidence[0].provider_dataset == evidence[1].provider_dataset
        || evidence[0].source_object_id == evidence[1].source_object_id
        || evidence[0].analytical_dataset_id == evidence[1].analytical_dataset_id
        || evidence[0].manifest_content_hash == evidence[1].manifest_content_hash
    {
        bail!("SEC acceptance did not produce two distinct filings and Company Facts publications");
    }
    Ok(evidence)
}

async fn exercise_fred_research(
    application: &Application,
    dataset: &SourceIdentifier,
) -> Result<Vec<ResearchPublicationEvidence>> {
    let dataset_text = dataset.as_str();
    let series = FredSource::rights_subject_identifier(dataset)
        .context("FRED/ALFRED dataset has no exact rights subject")?;
    let (realtime_start, realtime_end) = FredSource::dataset_realtime_interval(dataset)
        .context("FRED/ALFRED dataset has no exact real-time interval")?;
    let discovery = invoke(
        application,
        "Source.Discover",
        json_object(json!({
            "provider": FRED_ALFRED,
            "dataset": dataset_text,
            "confirm": true,
            "sourceCoverage": [FRED_ALFRED],
        }))?,
        RESEARCH_ACCEPTANCE_TIMEOUT,
    )
    .await?;
    let discovery = ResearchSourceDiscovery::from_publication(discovery)?;
    if discovery.profile().as_str() != FRED_ALFRED
        || discovery.request().dataset() != dataset
        || discovery.objects().is_empty()
        || !discovery.rights().persistence_operation_admitted()
        || !discovery.rights().model_training_operation_admitted()
        || discovery.rights().source_wide()
        || discovery.rights().exact_subjects() != std::slice::from_ref(&series)
    {
        bail!(
            "FRED/ALFRED discovery did not retain exact-series persistence and training authority"
        );
    }

    let mut pages = Vec::new();
    pages.try_reserve_exact(discovery.objects().len())?;
    let mut final_publication = None;
    let mut expected_offset = 0_usize;
    let mut provider_row_count = None;
    for object in discovery.objects() {
        let source_object = object.source_object();
        let identity = FredSource::page_object_identity(source_object.object_id())
            .context("FRED/ALFRED discovery returned an invalid page identity")?;
        if identity.offset() != expected_offset
            || identity.page_digest() != source_object.evidence().content_digest().bytes()
            || provider_row_count.is_some_and(|count| count != identity.total())
            || identity.terminal() != (pages.len() + 1 == discovery.objects().len())
        {
            bail!("FRED/ALFRED discovery returned an incomplete or inconsistent page chain");
        }
        provider_row_count = Some(identity.total());
        expected_offset = expected_offset
            .checked_add(identity.returned())
            .ok_or_else(|| anyhow!("FRED/ALFRED page offset overflow"))?;
        let source_object_id = source_object.object_id().as_str().to_owned();
        let source_payload_digest = source_object.evidence().content_digest();
        let ingestion = invoke(
            application,
            "Research.IngestSource",
            json_object(json!({
                "provider": FRED_ALFRED,
                "object": source_object_id,
                "dataset": dataset_text,
                "discoveryReceipt": object.discovery_receipt(),
                "confirm": true,
                "sourceCoverage": [FRED_ALFRED],
            }))?,
            RESEARCH_ACCEPTANCE_TIMEOUT,
        )
        .await?;
        let publication = parse_research_publication(
            FRED_ALFRED,
            "fred_alfred_vintages",
            dataset_text,
            source_object_id,
            source_payload_digest,
            &ingestion,
        )?;
        pages.push(FredPageEvidence {
            source_object_id: source_object.object_id().as_str().to_owned(),
            source_payload_digest,
            offset: u64::try_from(identity.offset()).context("FRED/ALFRED page offset overflow")?,
            limit: u64::try_from(identity.limit()).context("FRED/ALFRED page limit overflow")?,
            returned_rows: u64::try_from(identity.returned())
                .context("FRED/ALFRED page row count overflow")?,
            provider_row_count: u64::try_from(identity.total())
                .context("FRED/ALFRED provider row count overflow")?,
            terminal: identity.terminal(),
        });
        final_publication = Some(publication);
    }
    let provider_row_count = provider_row_count
        .and_then(|count| u64::try_from(count).ok())
        .ok_or_else(|| anyhow!("FRED/ALFRED provider row count is unavailable"))?;
    if u64::try_from(expected_offset).ok() != Some(provider_row_count) {
        bail!("FRED/ALFRED page chain did not consume the provider-declared result");
    }
    let mut publication =
        final_publication.ok_or_else(|| anyhow!("FRED/ALFRED publication is absent"))?;
    if publication.row_count != provider_row_count
        || publication.object_count != u64::try_from(pages.len())?
        || pages.last().is_none_or(|page| {
            page.source_object_id != publication.source_object_id
                || page.source_payload_digest != publication.source_payload_digest
                || !page.terminal
        })
    {
        bail!("FRED/ALFRED final publication is not the complete discovered dataset");
    }
    let observations = query_row_evidence(&query_publication(application, &publication).await?)?;
    let vintages = verify_fred_vintage_publication(application, &publication).await?;
    if observations.row_count != provider_row_count || vintages.row_count != provider_row_count {
        bail!("FRED/ALFRED analytical queries did not return the complete published row set");
    }
    validate_fred_query_rows(&observations.rows, dataset, &series, &pages)?;
    validate_fred_query_rows(&vintages.rows, dataset, &series, &pages)?;
    if observations != vintages {
        bail!("FRED/ALFRED observation and vintage queries did not return one exact row set");
    }
    publication.observation_query_row_count = observations.row_count;
    publication.vintage_query_row_count = Some(vintages.row_count);
    publication.series_ids = vec![series.as_str().to_owned()];
    publication.temporal_semantics = ResearchPublicationTemporalSemantics::ProviderReportedVintages;
    publication.fred = Some(FredPublicationEvidence {
        series_id: series.as_str().to_owned(),
        realtime_start: realtime_start.to_string(),
        realtime_end: realtime_end.to_string(),
        provider_row_count,
        pages,
        observation_query: observations,
        vintage_query: vintages,
    });
    Ok(vec![publication])
}

async fn exercise_bls_research(
    application: &Application,
    surface_id: &'static str,
    dataset: &SourceIdentifier,
) -> Result<Vec<ResearchPublicationEvidence>> {
    let dataset_text = dataset.as_str();
    let discovery = invoke(
        application,
        "Source.Discover",
        json_object(json!({
            "provider": surface_id,
            "dataset": dataset_text,
            "confirm": true,
            "sourceCoverage": [surface_id],
        }))?,
        RESEARCH_ACCEPTANCE_TIMEOUT,
    )
    .await?;
    let discovery = ResearchSourceDiscovery::from_publication(discovery)?;
    if discovery.profile().as_str() != surface_id
        || discovery.request().dataset() != dataset
        || discovery.objects().len() != 1
        || !discovery.rights().persistence_operation_admitted()
    {
        bail!("BLS discovery did not produce one persistence-authorized exact object");
    }
    let object = discovery
        .objects()
        .first()
        .ok_or_else(|| anyhow!("BLS discovery object is absent"))?;
    let source_object = object.source_object();
    let source_object_id = source_object.object_id().as_str().to_owned();
    let source_payload_digest = source_object.evidence().content_digest();
    let ingestion = invoke(
        application,
        "Research.IngestSource",
        json_object(json!({
            "provider": surface_id,
            "object": source_object_id,
            "dataset": dataset_text,
            "discoveryReceipt": object.discovery_receipt(),
            "confirm": true,
            "sourceCoverage": [surface_id],
        }))?,
        RESEARCH_ACCEPTANCE_TIMEOUT,
    )
    .await?;
    let mut publication = parse_research_publication(
        surface_id,
        "bls_unemployment_rate_current_snapshot",
        dataset_text,
        source_object_id,
        source_payload_digest,
        &ingestion,
    )?;
    publication.observation_query_row_count =
        verify_bls_current_snapshot_publication(application, &publication).await?;
    publication.series_ids = vec![BLS_UNEMPLOYMENT_SERIES.to_owned()];
    publication.temporal_semantics =
        ResearchPublicationTemporalSemantics::LocallyObservedCurrentSnapshot;
    Ok(vec![publication])
}

async fn exercise_python_training(
    product: &LocalProduct,
    source_surface_id: &'static str,
    source_label: &'static str,
    request: &Path,
    publications: &[ResearchPublicationEvidence],
) -> Result<PythonTrainingEvidence> {
    if publications.is_empty()
        || publications.iter().any(|publication| {
            publication.surface_id != source_surface_id
                || publication.python_export_sha256.is_some()
        })
    {
        bail!("{source_label} publication evidence is invalid for a derived training handoff");
    }
    let request_before =
        hash_stable_file(request, MAXIMUM_TRAINING_REQUEST_BYTES).with_context(|| {
            format!("{source_label} training request is not a stable bounded regular file")
        })?;
    let built =
        crate::local_product::cli_dataset::build_point_in_time_dataset_from_file(product, request)
            .await
            .with_context(|| {
                format!("{source_label} point-in-time training dataset could not be built")
            })?;
    let request_after = hash_stable_file(request, MAXIMUM_TRAINING_REQUEST_BYTES)
        .with_context(|| format!("{source_label} training request could not be revalidated"))?;
    if request_before != request_after {
        bail!("{source_label} training request changed while the dataset was built");
    }
    python_training_evidence(
        &built,
        source_surface_id,
        source_label,
        publications,
        request_before,
    )
}

fn python_training_evidence(
    built: &FeatureLabelDataset,
    source_surface_id: &'static str,
    source_label: &'static str,
    publications: &[ResearchPublicationEvidence],
    request: StableFileIdentity,
) -> Result<PythonTrainingEvidence> {
    let mut matching_parents = built.pinned().parents().iter().filter(|parent| {
        parent.relation() == GenerationParentRelation::DerivedInput
            && publications.iter().any(|publication| {
                parent.manifest().dataset_id().as_str() == publication.analytical_dataset_id
                    && parent.manifest().manifest_version() == publication.manifest_version
                    && lower_hex(parent.manifest().content_hash().bytes())
                        == publication.manifest_content_hash
            })
    });
    let source_parent = matching_parents.next().ok_or_else(|| {
        anyhow!("training dataset omitted the exact published {source_label} parent")
    })?;
    if matching_parents.next().is_some() {
        bail!("training dataset contains ambiguous {source_label} parent generations");
    }
    let splits = built.split_counts();
    if splits.train_examples() == 0
        || splits.validation_examples() == 0
        || splits.test_examples() == 0
    {
        bail!("training dataset must contain nonempty train, validation, and test splits");
    }
    let manifest = built.manifest();
    let parents = built
        .pinned()
        .parents()
        .iter()
        .map(|parent| PythonTrainingParentEvidence {
            dataset_id: parent.manifest().dataset_id().as_str().to_owned(),
            manifest_version: parent.manifest().manifest_version(),
            manifest_content_hash: lower_hex(parent.manifest().content_hash().bytes()),
        })
        .collect();
    let python_export_sha256 = built
        .python_export()
        .context("canonical Python training descriptor could not be reproduced")?
        .content_hash();
    Ok(PythonTrainingEvidence {
        request_sha256: request.sha256,
        request_byte_count: request.byte_count,
        dataset_id: manifest.dataset_id().as_str().to_owned(),
        manifest_version: manifest.manifest_version(),
        manifest_content_hash: lower_hex(manifest.content_hash().bytes()),
        source_surface_id: source_surface_id.to_owned(),
        source_parent_dataset_id: source_parent.manifest().dataset_id().as_str().to_owned(),
        source_parent_manifest_version: source_parent.manifest().manifest_version(),
        source_parent_content_hash: lower_hex(source_parent.manifest().content_hash().bytes()),
        parents,
        build_spec_digest: lower_hex(built.build_spec_digest().digest().bytes()),
        policy_digest: lower_hex(built.policy_digest().bytes()),
        universe_digest: lower_hex(built.universe_digest().bytes()),
        python_export_sha256: lower_hex(python_export_sha256.bytes()),
        train_examples: splits.train_examples(),
        validation_examples: splits.validation_examples(),
        test_examples: splits.test_examples(),
    })
}

fn parse_research_publication(
    surface_id: &str,
    family: &str,
    provider_dataset: &str,
    source_object_id: String,
    source_payload_digest: EvidenceDigest,
    ingestion: &Value,
) -> Result<ResearchPublicationEvidence> {
    let manifest = ingestion
        .get("manifest")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("research ingestion manifest is absent"))?;
    let analytical_dataset_id = required_text(manifest.get("datasetId"), "dataset id")?;
    let manifest_content_hash = required_sha256(
        manifest.get("contentHash"),
        "research manifest content hash",
    )?;
    let manifest_version =
        required_nonzero_u64(manifest.get("manifestVersion"), "research manifest version")?;
    let row_count =
        required_nonzero_u64(ingestion.get("rowCount"), "research publication row count")?;
    let total_bytes = required_nonzero_u64(
        ingestion.get("totalBytes"),
        "research publication byte count",
    )?;
    let object_count = required_nonzero_u64(
        ingestion.get("objectCount"),
        "research publication object count",
    )?;
    let lineage_digest =
        required_sha256(ingestion.get("lineageDigest"), "research lineage digest")?;
    let python_export_sha256 = optional_sha256(
        ingestion.get("pythonExportSha256"),
        "research Python export digest",
    )?;
    Ok(ResearchPublicationEvidence {
        surface_id: surface_id.to_owned(),
        family: family.to_owned(),
        provider_dataset: provider_dataset.to_owned(),
        source_object_id,
        source_payload_digest,
        analytical_dataset_id,
        manifest_version,
        manifest_content_hash,
        row_count,
        total_bytes,
        object_count,
        lineage_digest,
        python_export_sha256,
        observation_query_row_count: 0,
        vintage_query_row_count: None,
        series_ids: Vec::new(),
        temporal_semantics: ResearchPublicationTemporalSemantics::EffectiveObservations,
        sec: None,
        fred: None,
        treasury_fiscal: None,
    })
}

async fn verify_queryable_publication(
    application: &Application,
    publication: &ResearchPublicationEvidence,
) -> Result<u64> {
    let observations = query_publication(application, publication).await?;
    query_result_row_count(&observations)
        .ok_or_else(|| anyhow!("research publication is not queryable"))
}

async fn query_publication(
    application: &Application,
    publication: &ResearchPublicationEvidence,
) -> Result<Value> {
    query_publication_with_operation(application, publication, "Macro.GetObservations").await
}

async fn query_publication_with_operation(
    application: &Application,
    publication: &ResearchPublicationEvidence,
    operation: &str,
) -> Result<Value> {
    let observations = invoke(
        application,
        operation,
        json_object(json!({
            "dataset": publication.analytical_dataset_id,
            "sourceCoverage": [publication.surface_id],
        }))?,
        RESEARCH_ACCEPTANCE_TIMEOUT,
    )
    .await?;
    let manifest = observations
        .get("manifest")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("research query manifest is absent"))?;
    if manifest.get("datasetId").and_then(Value::as_str)
        != Some(publication.analytical_dataset_id.as_str())
        || manifest.get("manifestVersion").and_then(Value::as_u64)
            != Some(publication.manifest_version)
        || manifest.get("contentHash").and_then(Value::as_str)
            != Some(publication.manifest_content_hash.as_str())
    {
        bail!("research query did not use the exact published manifest");
    }
    Ok(observations)
}

async fn verify_sec_publication(
    application: &Application,
    publication: &ResearchPublicationEvidence,
    cik: &str,
    operation: &str,
    observation_kind: &str,
) -> Result<SecPublicationEvidence> {
    let observations =
        query_publication_with_operation(application, publication, operation).await?;
    let rows = observations
        .get("rows")
        .and_then(Value::as_array)
        .filter(|rows| !rows.is_empty())
        .ok_or_else(|| anyhow!("SEC query was not returned as a nonempty inline row set"))?;
    let row_count = u64::try_from(rows.len()).context("SEC observation row count overflow")?;
    if row_count != publication.row_count {
        bail!("SEC query did not return the exact published row set");
    }
    let mut instruments = BTreeSet::new();
    for row in rows {
        let row = row
            .as_object()
            .ok_or_else(|| anyhow!("SEC query row is invalid"))?;
        let payload = required_lower_hex_bytes(row.get("payload_json"), "SEC canonical payload")?;
        let payload_digest =
            required_lower_hex_bytes(row.get("payload_sha256"), "SEC canonical payload digest")?;
        let expected_payload_digest: [u8; 32] = Sha256::digest(&payload).into();
        if payload_digest.as_slice() != expected_payload_digest {
            bail!("SEC canonical row payload digest is invalid");
        }
        let observation: ResearchObservation = serde_json::from_slice(&payload)
            .context("SEC canonical row payload could not be decoded")?;
        let (context, source_specific_valid) = match (&observation, observation_kind) {
            (ResearchObservation::Filing(value), "filing") => {
                let context = value.context();
                let provenance = context.provenance();
                let published = context.time().published();
                let published_matches = match provenance.source_timestamp() {
                    Some(timestamp) => {
                        published.and_then(|value| value.exact_timestamp()) == Some(timestamp)
                    }
                    None => published.is_some_and(|value| value.calendar_date_value().is_some()),
                };
                (
                    context,
                    value.accession() == provenance.source_identifier()
                        && !value.form_type().as_str().is_empty()
                        && context.time().effective().calendar_date_value().is_some()
                        && published_matches,
                )
            }
            (ResearchObservation::Fundamental(value), "fundamental") => {
                let context = value.context();
                let provenance = context.provenance();
                let fact_context = value.fact_context();
                (
                    context,
                    provenance.source_timestamp().is_none()
                        && fact_context.filing_form().is_some()
                        && fact_context.period().end()
                            == context
                                .time()
                                .effective()
                                .calendar_date_value()
                                .ok_or_else(|| anyhow!("SEC fact effective date is not exact"))?
                        && fact_context.filed_on().is_some()
                        && fact_context.filed_on()
                            == context
                                .time()
                                .published()
                                .and_then(|published| published.calendar_date_value())
                        && fact_context.revision_order().ordinal() == context.time().revision()
                        && value.xbrl_evidence().is_none()
                        && fact_context.xbrl_context_id().is_none()
                        && fact_context.dimensions().dimensions().is_none()
                        && fact_context.consolidation() == FundamentalConsolidation::Unavailable
                        && matches!(
                            fact_context.restatement_status(),
                            FundamentalRestatementStatus::Unavailable
                        ),
                )
            }
            _ => bail!("SEC query returned the wrong canonical observation family"),
        };
        let provenance = context.provenance();
        let instrument_id = provenance
            .instrument_id()
            .ok_or_else(|| anyhow!("SEC observation omitted stable instrument identity"))?
            .to_string();
        let payload_matches = matches!(
            provenance.payload_reference(),
            PayloadReference::ContentHash(hash)
                if hash.algorithm() == publication.source_payload_digest.algorithm()
                    && hash.digest() == publication.source_payload_digest.bytes()
        );
        let availability_matches = matches!(
            provenance.availability(),
            AvailabilityEvidence::LocalFirstObserved { observed_at }
                if *observed_at == provenance.received_at()
        );
        if !source_specific_valid
            || provenance.source_id().as_str() != SEC_SOURCE_ID
            || provenance.venue_id().is_some()
            || provenance.quality() != DataQuality::OfficialDelayed
            || provenance.ingested_at() < provenance.received_at()
            || !payload_matches
            || !availability_matches
            || row.get("observation_kind").and_then(Value::as_str) != Some(observation_kind)
            || row.get("source_id").and_then(Value::as_str) != Some(SEC_SOURCE_ID)
            || row.get("instrument_id").and_then(Value::as_str) != Some(instrument_id.as_str())
            || row.get("venue_id").is_some_and(|value| !value.is_null())
            || row.get("source_identifier").and_then(Value::as_str)
                != Some(provenance.source_identifier().as_str())
            || row.get("received_at") != row.get("available_at")
            || row.get("availability_kind").and_then(Value::as_str) != Some("local_first_observed")
            || row.get("quality").and_then(Value::as_str) != Some("official_delayed")
            || row.get("revision").and_then(Value::as_u64)
                != Some(u64::from(context.time().revision().get()))
            || row.get("effective_precision").and_then(Value::as_str) != Some("calendar_date")
        {
            bail!("SEC canonical row lost direct source, time, quality, or payload provenance");
        }
        instruments.insert(instrument_id);
    }
    if instruments.len() != 1 {
        bail!("SEC publication does not bind one stable instrument identity");
    }
    let instrument_id = instruments
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("SEC publication does not bind one stable instrument identity"))?;
    Ok(SecPublicationEvidence {
        cik: cik.to_owned(),
        instrument_id,
        observation_kind: observation_kind.to_owned(),
        quality: "official_delayed".to_owned(),
        query_operation: operation.to_owned(),
        provenance_verified_rows: row_count,
    })
}

fn required_lower_hex_bytes(value: Option<&Value>, field: &str) -> Result<Vec<u8>> {
    let value = value
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty() && value.len().is_multiple_of(2))
        .ok_or_else(|| anyhow!("{field} is absent or has an invalid length"))?;
    let mut decoded = Vec::new();
    decoded
        .try_reserve_exact(value.len() / 2)
        .with_context(|| format!("{field} allocation failed"))?;
    for pair in value.as_bytes().chunks_exact(2) {
        let high = lower_hex_nibble(pair[0]).ok_or_else(|| anyhow!("{field} is not lower hex"))?;
        let low = lower_hex_nibble(pair[1]).ok_or_else(|| anyhow!("{field} is not lower hex"))?;
        decoded.push((high << 4) | low);
    }
    Ok(decoded)
}

const fn lower_hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

async fn verify_bls_current_snapshot_publication(
    application: &Application,
    publication: &ResearchPublicationEvidence,
) -> Result<u64> {
    let maximum_rows = if publication
        .provider_dataset
        .starts_with("bls:timeseries:public-v1:")
    {
        BLS_PUBLIC_MAXIMUM_ACCEPTANCE_ROWS
    } else if publication
        .provider_dataset
        .starts_with("bls:timeseries:registered-v2:")
    {
        BLS_REGISTERED_MAXIMUM_ACCEPTANCE_ROWS
    } else {
        bail!("BLS current-snapshot publication has an unknown access tier");
    };
    if publication.row_count > maximum_rows {
        bail!("BLS unemployment acceptance exceeds one bounded provider year window");
    }
    let observations = query_publication(application, publication).await?;
    let rows = observations
        .get("rows")
        .and_then(Value::as_array)
        .filter(|rows| !rows.is_empty())
        .ok_or_else(|| anyhow!("BLS current-snapshot query was not returned inline"))?;
    if u64::try_from(rows.len()).context("BLS observation row count overflow")?
        != publication.row_count
    {
        bail!("BLS current-snapshot query did not return the exact published row set");
    }
    let payload_digest = lower_hex(publication.source_payload_digest.bytes());
    for row in rows {
        let row = row
            .as_object()
            .ok_or_else(|| anyhow!("BLS current-snapshot query row is invalid"))?;
        let source_identifier = row
            .get("source_identifier")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("BLS current-snapshot provenance is absent"))?;
        if !bls_observation_identity_matches(
            source_identifier,
            BLS_UNEMPLOYMENT_SERIES,
            &payload_digest,
        ) || row.get("observation_kind").and_then(Value::as_str) != Some("macro")
            || row.get("availability_kind").and_then(Value::as_str) != Some("local_first_observed")
            || row.get("available_at").is_none_or(Value::is_null)
            || row.get("received_at") != row.get("available_at")
            || row
                .get("source_timestamp")
                .is_some_and(|value| !value.is_null())
            || row
                .get("published_precision")
                .is_some_and(|value| !value.is_null())
            || row.get("revision").and_then(Value::as_u64) == Some(0)
            || row.get("revision").and_then(Value::as_u64).is_none()
            || row.get("effective_period_scheme").and_then(Value::as_str) != Some("bls-monthly")
            || row.get("unit").and_then(Value::as_str) != Some("percent")
            || row.get("quality").and_then(Value::as_str) != Some("official_delayed")
        {
            bail!("BLS query mislabeled current revised observations or lost direct provenance");
        }
    }
    u64::try_from(rows.len()).context("BLS observation row count overflow")
}

fn bls_observation_identity_matches(identity: &str, series: &str, payload_digest: &str) -> bool {
    let mut fields = identity.split(':');
    fields.next() == Some("bls")
        && fields.next() == Some(series)
        && fields
            .next()
            .and_then(|year| year.parse::<u16>().ok())
            .is_some_and(|year| (1900..=9999).contains(&year))
        && fields.next().is_some_and(|period| {
            period.len() == 3
                && period.starts_with('M')
                && period[1..]
                    .parse::<u8>()
                    .is_ok_and(|month| (1..=13).contains(&month))
        })
        && fields.next() == Some(payload_digest)
        && fields.next().is_none()
}

async fn verify_fred_vintage_publication(
    application: &Application,
    publication: &ResearchPublicationEvidence,
) -> Result<QueryRowEvidence> {
    let vintages = invoke(
        application,
        "Macro.GetVintages",
        json_object(json!({
            "dataset": publication.analytical_dataset_id,
            "sourceCoverage": [FRED_ALFRED],
        }))?,
        RESEARCH_ACCEPTANCE_TIMEOUT,
    )
    .await?;
    let manifest = vintages
        .get("manifest")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("FRED/ALFRED vintage query manifest is absent"))?;
    if manifest.get("datasetId").and_then(Value::as_str)
        != Some(publication.analytical_dataset_id.as_str())
        || manifest.get("manifestVersion").and_then(Value::as_u64)
            != Some(publication.manifest_version)
        || manifest.get("contentHash").and_then(Value::as_str)
            != Some(publication.manifest_content_hash.as_str())
    {
        bail!("FRED/ALFRED vintage query did not use the exact published manifest");
    }
    query_row_evidence(&vintages)
}

fn validate_fred_query_rows(
    rows: &[Value],
    provider_dataset: &SourceIdentifier,
    series: &SourceIdentifier,
    pages: &[FredPageEvidence],
) -> Result<()> {
    const FRED_SOURCE_ID: &str = "fred-fred-alfred.api-v1-v2";
    const DAYS_FROM_YEAR_ONE_TO_UNIX_EPOCH: i32 = 719_163;

    let namespace = provider_dataset
        .as_str()
        .split(':')
        .next()
        .filter(|value| matches!(*value, "fred" | "alfred"))
        .ok_or_else(|| anyhow!("FRED/ALFRED dataset namespace is invalid"))?;
    let mut expected_page_rows = BTreeMap::new();
    for page in pages {
        if page.source_payload_digest.algorithm() != DigestAlgorithm::Sha256
            || page.source_payload_digest.bytes() == [0; 32]
            || expected_page_rows
                .insert(page.source_payload_digest.bytes(), page.returned_rows)
                .is_some()
        {
            bail!("FRED/ALFRED page payload evidence is invalid or duplicated");
        }
    }

    let mut observed_page_rows = BTreeMap::<[u8; 32], u64>::new();
    let mut identities = BTreeSet::new();
    for row in rows {
        let row = row
            .as_object()
            .ok_or_else(|| anyhow!("FRED/ALFRED query row is invalid"))?;
        let payload =
            required_lower_hex_bytes(row.get("payload_json"), "FRED/ALFRED canonical payload")?;
        let payload_digest = required_lower_hex_bytes(
            row.get("payload_sha256"),
            "FRED/ALFRED canonical payload digest",
        )?;
        let expected_payload_digest: [u8; 32] = Sha256::digest(&payload).into();
        if payload_digest.as_slice() != expected_payload_digest {
            bail!("FRED/ALFRED canonical row payload digest is invalid");
        }
        let observation: ResearchObservation = serde_json::from_slice(&payload)
            .context("FRED/ALFRED canonical row payload could not be decoded")?;
        let ResearchObservation::Macro(observation) = observation else {
            bail!("FRED/ALFRED query returned a non-macro observation");
        };
        let context = observation.context();
        let provenance = context.provenance();
        let effective = context
            .time()
            .effective()
            .calendar_date_value()
            .ok_or_else(|| anyhow!("FRED/ALFRED effective date precision was lost"))?;
        let published = context
            .time()
            .published()
            .and_then(|value| value.calendar_date_value())
            .ok_or_else(|| anyhow!("FRED/ALFRED vintage date precision was lost"))?;
        if context
            .time()
            .superseded()
            .is_some_and(|value| value.calendar_date_value().is_none())
        {
            bail!("FRED/ALFRED supersession date precision was lost");
        }
        let expected_revision = published
            .days_since_unix_epoch()
            .checked_add(DAYS_FROM_YEAR_ONE_TO_UNIX_EPOCH)
            .and_then(|value| u32::try_from(value).ok())
            .ok_or_else(|| anyhow!("FRED/ALFRED vintage revision is invalid"))?;
        let source_identifier = format!("{namespace}:{series}:{effective}:{published}");
        let page_digest = match provenance.payload_reference() {
            PayloadReference::ContentHash(hash) if hash.algorithm() == DigestAlgorithm::Sha256 => {
                hash.digest()
            }
            _ => bail!("FRED/ALFRED row omitted exact provider-page evidence"),
        };
        if !expected_page_rows.contains_key(&page_digest) {
            bail!("FRED/ALFRED row references a payload outside the discovered page chain");
        }
        let observed = observed_page_rows.entry(page_digest).or_default();
        *observed = observed
            .checked_add(1)
            .ok_or_else(|| anyhow!("FRED/ALFRED page row count overflow"))?;
        if observation.series() != series
            || provenance.source_id().as_str() != FRED_SOURCE_ID
            || provenance.instrument_id().is_some()
            || provenance.venue_id().is_some()
            || provenance.source_identifier().as_str() != source_identifier
            || provenance.source_timestamp().is_some()
            || provenance.quality() != DataQuality::OfficialDelayed
            || provenance.ingested_at() < provenance.received_at()
            || !matches!(
                provenance.availability(),
                AvailabilityEvidence::LocalFirstObserved { observed_at }
                    if *observed_at == provenance.received_at()
            )
            || context.time().revision().get() != expected_revision
            || row.get("observation_kind").and_then(Value::as_str) != Some("macro")
            || row.get("source_id").and_then(Value::as_str) != Some(FRED_SOURCE_ID)
            || row.get("source_identifier").and_then(Value::as_str)
                != Some(source_identifier.as_str())
            || row
                .get("instrument_id")
                .is_some_and(|value| !value.is_null())
            || row.get("venue_id").is_some_and(|value| !value.is_null())
            || row
                .get("source_timestamp")
                .is_some_and(|value| !value.is_null())
            || row.get("received_at") != row.get("available_at")
            || row.get("availability_kind").and_then(Value::as_str) != Some("local_first_observed")
            || row.get("effective_precision").and_then(Value::as_str) != Some("calendar_date")
            || row.get("published_precision").and_then(Value::as_str) != Some("calendar_date")
            || row.get("revision").and_then(Value::as_u64) != Some(u64::from(expected_revision))
            || row.get("quality").and_then(Value::as_str) != Some("official_delayed")
            || row.get("unit").and_then(Value::as_str) != Some(observation.unit().as_str())
        {
            bail!("FRED/ALFRED canonical row lost exact series, time, quality, or provenance");
        }
        if !identities.insert((source_identifier, expected_payload_digest)) {
            bail!("FRED/ALFRED query repeats a canonical observation identity");
        }
    }
    if observed_page_rows != expected_page_rows {
        bail!("FRED/ALFRED query rows do not exactly cover the discovered page chain");
    }
    Ok(())
}

fn parse_treasury_fiscal_object_id(value: &str) -> Result<(usize, [u8; 32], [u8; 32])> {
    let mut fields = value.split(':');
    if fields.next() != Some("treasury-page") || fields.next() != Some("fiscal") {
        bail!("Treasury Fiscal Data source object identity is invalid");
    }
    let page_number = fields
        .next()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .ok_or_else(|| anyhow!("Treasury Fiscal Data page number is invalid"))?;
    let request_digest = fields
        .next()
        .ok_or_else(|| anyhow!("Treasury Fiscal Data request digest is absent"))
        .and_then(|value| decode_lower_sha256(value, "Treasury Fiscal Data request digest"))?;
    let payload_digest = fields
        .next()
        .ok_or_else(|| anyhow!("Treasury Fiscal Data payload digest is absent"))
        .and_then(|value| decode_lower_sha256(value, "Treasury Fiscal Data payload digest"))?;
    if fields.next().is_some() {
        bail!("Treasury Fiscal Data source object identity has trailing fields");
    }
    Ok((page_number, request_digest, payload_digest))
}

struct TreasuryFiscalQueryValidation {
    page_rows: BTreeMap<[u8; 32], u64>,
    series: Vec<String>,
}

fn validate_treasury_fiscal_query_rows(
    rows: &[Value],
    query: &TreasuryFiscalQuery,
    pages: &[TreasuryFiscalPageEvidence],
) -> Result<TreasuryFiscalQueryValidation> {
    const TREASURY_FISCAL_SOURCE_ID: &str = "treasury-treasury.fiscal-data";

    let mut expected_pages = BTreeMap::new();
    for page in pages {
        let request_digest = decode_lower_sha256(
            &page.request_digest,
            "Treasury Fiscal Data page request digest",
        )?;
        if page.source_payload_digest.algorithm() != DigestAlgorithm::Sha256
            || page.source_payload_digest.bytes() == [0; 32]
            || expected_pages
                .insert(page.source_payload_digest.bytes(), request_digest)
                .is_some()
        {
            bail!("Treasury Fiscal Data page payload evidence is invalid or duplicated");
        }
    }
    let mut observed_pages = BTreeMap::<[u8; 32], u64>::new();
    let mut identities = BTreeSet::new();
    let mut series = BTreeSet::new();
    for row in rows {
        let row = row
            .as_object()
            .ok_or_else(|| anyhow!("Treasury Fiscal Data query row is invalid"))?;
        let payload = required_lower_hex_bytes(
            row.get("payload_json"),
            "Treasury Fiscal Data canonical payload",
        )?;
        let declared_payload_digest = required_lower_hex_bytes(
            row.get("payload_sha256"),
            "Treasury Fiscal Data canonical payload digest",
        )?;
        let payload_digest: [u8; 32] = Sha256::digest(&payload).into();
        if declared_payload_digest.as_slice() != payload_digest {
            bail!("Treasury Fiscal Data canonical payload digest is invalid");
        }
        let observation: ResearchObservation = serde_json::from_slice(&payload)
            .context("Treasury Fiscal Data canonical payload could not be decoded")?;
        let ResearchObservation::Macro(observation) = observation else {
            bail!("Treasury Fiscal Data query returned a non-macro observation");
        };
        let request_digest = required_lower_hex_bytes(
            row.get("request_sha256"),
            "Treasury Fiscal Data request digest",
        )?;
        let lineage = required_lower_hex_bytes(
            row.get("extraction_lineage_json"),
            "Treasury Fiscal Data extraction lineage",
        )?;
        let context = observation.context();
        let provenance = context.provenance();
        let effective = context
            .time()
            .effective()
            .calendar_date_value()
            .ok_or_else(|| anyhow!("Treasury Fiscal Data effective date precision was lost"))?;
        let source_identifier = provenance.source_identifier().as_str();
        let expected_prefix = format!("treasury-fiscal-rate:{effective}:");
        let page_digest = match provenance.payload_reference() {
            PayloadReference::ContentHash(hash) if hash.algorithm() == DigestAlgorithm::Sha256 => {
                hash.digest()
            }
            _ => bail!("Treasury Fiscal Data row omitted exact provider-page evidence"),
        };
        let expected_request_digest = expected_pages.get(&page_digest).ok_or_else(|| {
            anyhow!(
                "Treasury Fiscal Data row references a payload outside the discovered page chain"
            )
        })?;
        let observed = observed_pages.entry(page_digest).or_default();
        *observed = observed
            .checked_add(1)
            .ok_or_else(|| anyhow!("Treasury Fiscal Data page row count overflow"))?;
        if request_digest.as_slice() != expected_request_digest.as_slice()
            || serde_json::from_slice::<Value>(&lineage)
                .ok()
                .is_none_or(|value| value.is_null())
            || row.keys().any(|field| !research_row_field_allowed(field))
            || effective < query.first_record_date()
            || effective > query.last_record_date()
            || !source_identifier.starts_with(&expected_prefix)
            || !treasury_fiscal_revision_matches(source_identifier, effective)
            || provenance.source_id().as_str() != TREASURY_FISCAL_SOURCE_ID
            || provenance.instrument_id().is_some()
            || provenance.venue_id().is_some()
            || provenance.source_timestamp().is_some()
            || provenance.quality() != DataQuality::OfficialDelayed
            || provenance.ingested_at() < provenance.received_at()
            || !matches!(
                provenance.availability(),
                AvailabilityEvidence::LocalFirstObserved { observed_at }
                    if *observed_at == provenance.received_at()
            )
            || context.time().published().is_some()
            || context.time().superseded().is_some()
            || context.time().revision().get() != 1
            || !treasury_fiscal_series_valid(observation.series().as_str())
            || observation.unit().as_str() != "percent"
            || observation.value().observed_value().is_none()
            || observation.value().missing_value().is_some()
            || row.get("schema_version").and_then(Value::as_u64) != Some(3)
            || row.get("observation_kind").and_then(Value::as_str) != Some("macro")
            || row.get("source_id").and_then(Value::as_str) != Some(TREASURY_FISCAL_SOURCE_ID)
            || row.get("source_identifier").and_then(Value::as_str) != Some(source_identifier)
            || row.get("received_at") != row.get("available_at")
            || row.get("availability_kind").and_then(Value::as_str) != Some("local_first_observed")
            || row.get("effective_precision").and_then(Value::as_str) != Some("calendar_date")
            || row.get("effective_date").and_then(Value::as_str)
                != Some(effective.to_string().as_str())
            || row.get("revision").and_then(Value::as_u64) != Some(1)
            || row.get("quality").and_then(Value::as_str) != Some("official_delayed")
            || row.get("value_state").and_then(Value::as_str) != Some("observed")
            || row.get("unit").and_then(Value::as_str) != Some("percent")
            || row
                .get("instrument_id")
                .is_some_and(|value| !value.is_null())
            || row.get("venue_id").is_some_and(|value| !value.is_null())
            || row
                .get("source_timestamp")
                .is_some_and(|value| !value.is_null())
            || row
                .get("published_precision")
                .is_some_and(|value| !value.is_null())
            || row
                .get("superseded_precision")
                .is_some_and(|value| !value.is_null())
            || !identities.insert((source_identifier.to_owned(), payload_digest))
        {
            bail!(
                "Treasury Fiscal Data row lost exact source, time, quality, or payload authority"
            );
        }
        series.insert(observation.series().as_str().to_owned());
    }
    if observed_pages.len() != expected_pages.len()
        || expected_pages
            .keys()
            .any(|digest| !observed_pages.contains_key(digest))
        || series.is_empty()
    {
        bail!("Treasury Fiscal Data rows do not exactly cover every discovered provider page");
    }
    Ok(TreasuryFiscalQueryValidation {
        page_rows: observed_pages,
        series: series.into_iter().collect(),
    })
}

fn treasury_fiscal_revision_matches(identity: &str, effective: CalendarDate) -> bool {
    let mut fields = identity.split(':');
    fields.next() == Some("treasury-fiscal-rate")
        && fields.next() == Some(effective.to_string().as_str())
        && fields
            .next()
            .and_then(|value| value.parse::<u64>().ok())
            .is_some_and(|line| line > 0)
        && fields
            .next()
            .is_some_and(|digest| decode_lower_sha256(digest, "row identity").is_ok())
        && fields.next().is_none()
}

fn treasury_fiscal_series_valid(series: &str) -> bool {
    let mut fields = series.split(':');
    fields.next() == Some("treasury")
        && fields.next() == Some("average-interest-rate")
        && fields.next() == Some("v2")
        && fields.next().is_some_and(|value| !value.is_empty())
        && fields.next().is_some_and(|value| !value.is_empty())
        && fields.next().is_none()
}

fn research_row_field_allowed(field: &str) -> bool {
    matches!(
        field,
        "schema_version"
            | "request_sha256"
            | "extraction_lineage_json"
            | "observation_kind"
            | "source_id"
            | "instrument_id"
            | "venue_id"
            | "source_identifier"
            | "source_timestamp"
            | "received_at"
            | "available_at"
            | "availability_reported_or_inferred_at"
            | "availability_kind"
            | "availability_evidence"
            | "availability_method"
            | "ingested_at"
            | "effective_precision"
            | "effective_at"
            | "effective_date"
            | "effective_period_scheme"
            | "effective_period_year"
            | "effective_period_ordinal"
            | "effective_period_code"
            | "published_precision"
            | "published_at"
            | "published_date"
            | "published_period_scheme"
            | "published_period_year"
            | "published_period_ordinal"
            | "published_period_code"
            | "revision"
            | "superseded_precision"
            | "superseded_at"
            | "superseded_date"
            | "superseded_period_scheme"
            | "superseded_period_year"
            | "superseded_period_ordinal"
            | "superseded_period_code"
            | "quality"
            | "value_state"
            | "missing_marker"
            | "missing_reason"
            | "value_mantissa"
            | "value_scale"
            | "unit"
            | "currency"
            | "payload_sha256"
            | "payload_json"
    )
}

fn decode_lower_sha256(value: &str, field: &str) -> Result<[u8; 32]> {
    let decoded = required_lower_hex_bytes(Some(&Value::String(value.to_owned())), field)?;
    decoded
        .try_into()
        .map_err(|_bytes: Vec<u8>| anyhow!("{field} is not a SHA-256 digest"))
}

fn query_result_row_count(result: &Value) -> Option<u64> {
    let inline_rows = result
        .get("rows")
        .and_then(Value::as_array)
        .and_then(|rows| u64::try_from(rows.len()).ok())
        .filter(|rows| *rows > 0);
    let artifact_rows = result
        .pointer("/artifact/rowCount")
        .and_then(Value::as_u64)
        .filter(|rows| *rows > 0);
    inline_rows.or(artifact_rows)
}

fn query_row_evidence(result: &Value) -> Result<QueryRowEvidence> {
    let rows = result
        .get("rows")
        .and_then(Value::as_array)
        .filter(|rows| !rows.is_empty())
        .ok_or_else(|| anyhow!("release query must return one complete bounded inline row set"))?;
    if result
        .get("artifact")
        .is_some_and(|artifact| !artifact.is_null())
    {
        bail!("release query returned an opaque artifact instead of independently verifiable rows");
    }
    let row_count = u64::try_from(rows.len()).context("query result row count overflow")?;
    let canonical = serde_json::to_vec(rows).context("query rows could not be canonicalized")?;
    Ok(QueryRowEvidence {
        row_count,
        content_sha256: lower_hex(Sha256::digest(canonical).into()),
        rows: rows.clone(),
    })
}

fn treasury_acceptance_datasets(
    acceptance_year: u16,
) -> Result<Vec<(&'static str, SourceIdentifier)>> {
    TreasuryDailyRateFamily::ALL
        .into_iter()
        .map(|family| {
            let query = TreasuryDailyRateQuery::year(family, acceptance_year)?;
            Ok((
                treasury_family_evidence_name(family),
                query.dataset().clone(),
            ))
        })
        .collect()
}

const fn treasury_family_evidence_name(family: TreasuryDailyRateFamily) -> &'static str {
    match family {
        TreasuryDailyRateFamily::NominalParYieldCurve => "nominal_par_yield_curve",
        TreasuryDailyRateFamily::BillRates => "bill_rates",
        TreasuryDailyRateFamily::LongTermRates => "long_term_rates",
        TreasuryDailyRateFamily::RealParYieldCurve => "real_par_yield_curve",
        TreasuryDailyRateFamily::RealLongTermRates => "real_long_term_rates",
    }
}

fn required_text(value: Option<&Value>, field: &str) -> Result<String> {
    value
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!("{field} is absent"))
}

fn required_nonzero_u64(value: Option<&Value>, field: &str) -> Result<u64> {
    value
        .and_then(Value::as_u64)
        .filter(|value| *value > 0)
        .ok_or_else(|| anyhow!("{field} is absent or zero"))
}

fn required_sha256(value: Option<&Value>, field: &str) -> Result<String> {
    value
        .and_then(Value::as_str)
        .filter(|value| {
            value.len() == 64
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!("{field} is not a lowercase SHA-256 digest"))
}

fn optional_sha256(value: Option<&Value>, field: &str) -> Result<Option<String>> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(value) => required_sha256(Some(value), field).map(Some),
    }
}

fn lower_hex(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
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

async fn verify_restart_recovery(
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
                let mut expected_identity = expected_runtime.clone();
                expected_identity.publications.clear();
                expected_identity.python_training = None;
                if research_runtime_evidence(&runtime, Vec::new(), None)? != expected_identity {
                    bail!("provider research runtime changed during restart: {surface_id}");
                }
                for publication in &expected_runtime.publications {
                    let observation_count = match publication.temporal_semantics {
                        ResearchPublicationTemporalSemantics::LocallyObservedCurrentSnapshot => {
                            verify_bls_current_snapshot_publication(
                                product.application().as_ref(),
                                publication,
                            )
                            .await?
                        }
                        ResearchPublicationTemporalSemantics::LocallyObservedSecDisclosure => {
                            let expected_sec = publication.sec.as_ref().ok_or_else(|| {
                                anyhow!("SEC publication recovery evidence is absent")
                            })?;
                            let recovered_sec = verify_sec_publication(
                                product.application().as_ref(),
                                publication,
                                &expected_sec.cik,
                                &expected_sec.query_operation,
                                &expected_sec.observation_kind,
                            )
                            .await?;
                            if recovered_sec != *expected_sec {
                                bail!(
                                    "SEC publication provenance changed during restart: \
                                     {surface_id}"
                                );
                            }
                            recovered_sec.provenance_verified_rows
                        }
                        ResearchPublicationTemporalSemantics::EffectiveObservations => {
                            verify_queryable_publication(
                                product.application().as_ref(),
                                publication,
                            )
                            .await?
                        }
                        ResearchPublicationTemporalSemantics::ProviderReportedVintages => {
                            let expected_fred = publication.fred.as_ref().ok_or_else(|| {
                                anyhow!("FRED/ALFRED recovery evidence is absent")
                            })?;
                            let observations = query_row_evidence(
                                &query_publication(product.application().as_ref(), publication)
                                    .await?,
                            )?;
                            let vintages = verify_fred_vintage_publication(
                                product.application().as_ref(),
                                publication,
                            )
                            .await?;
                            let provider_dataset =
                                SourceIdentifier::try_from(publication.provider_dataset.as_str())
                                    .context("recovered FRED/ALFRED provider dataset is invalid")?;
                            let series =
                                SourceIdentifier::try_from(expected_fred.series_id.as_str())
                                    .context("recovered FRED/ALFRED series is invalid")?;
                            validate_fred_query_rows(
                                &observations.rows,
                                &provider_dataset,
                                &series,
                                &expected_fred.pages,
                            )?;
                            validate_fred_query_rows(
                                &vintages.rows,
                                &provider_dataset,
                                &series,
                                &expected_fred.pages,
                            )?;
                            if observations != expected_fred.observation_query
                                || vintages != expected_fred.vintage_query
                            {
                                bail!(
                                    "FRED/ALFRED query evidence changed during restart: \
                                     {surface_id}"
                                );
                            }
                            observations.row_count
                        }
                        ResearchPublicationTemporalSemantics::TreasuryFiscalEffectiveObservations => {
                            verify_treasury_fiscal_recovery(
                                product.application().as_ref(),
                                publication,
                            )
                            .await?
                        }
                    };
                    if observation_count != publication.observation_query_row_count {
                        bail!(
                            "provider publication row count changed during restart: {surface_id}"
                        );
                    }
                }
                if let Some(training) = &expected_runtime.python_training {
                    verify_python_training_recovery(product, training)?;
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

async fn verify_treasury_fiscal_recovery(
    application: &Application,
    publication: &ResearchPublicationEvidence,
) -> Result<u64> {
    let expected = publication
        .treasury_fiscal
        .as_ref()
        .ok_or_else(|| anyhow!("Treasury Fiscal Data recovery evidence is absent"))?;
    let page_size = NonZeroU16::new(expected.page_size)
        .ok_or_else(|| anyhow!("Treasury Fiscal Data recovery page size is invalid"))?;
    let query = TreasuryFiscalQuery::average_interest_rates_v2(
        expected.first_record_date,
        expected.last_record_date,
        page_size,
    )
    .context("Treasury Fiscal Data recovery query is invalid")?;
    let provider_dataset = query
        .dataset()
        .context("Treasury Fiscal Data recovery provider dataset is invalid")?;
    let analytical_dataset = query
        .analytical_dataset()
        .context("Treasury Fiscal Data recovery analytical dataset is invalid")?;
    if lower_hex(query.query_digest()) != expected.query_digest
        || provider_dataset.as_str() != publication.provider_dataset
        || analytical_dataset.as_str() != publication.analytical_dataset_id
    {
        bail!("Treasury Fiscal Data recovery selector does not match the published generation");
    }
    let observations = query_row_evidence(&query_publication(application, publication).await?)?;
    let TreasuryFiscalQueryValidation { page_rows, series } =
        validate_treasury_fiscal_query_rows(&observations.rows, &query, &expected.pages)?;
    if observations != expected.observation_query
        || observations.row_count != expected.provider_row_count
        || series != publication.series_ids
        || expected.pages.iter().any(|page| {
            page_rows.get(&page.source_payload_digest.bytes()).copied() != Some(page.returned_rows)
        })
    {
        bail!("Treasury Fiscal Data query evidence changed during restart");
    }
    Ok(observations.row_count)
}

fn verify_python_training_recovery(
    product: &LocalProduct,
    expected: &PythonTrainingEvidence,
) -> Result<()> {
    let dataset_id = DatasetId::try_from(expected.dataset_id.as_str())
        .context("recorded Python training dataset identity is invalid")?;
    let deadline = Instant::now()
        .checked_add(APPLICATION_REQUEST_TIMEOUT)
        .ok_or_else(|| anyhow!("Python training recovery deadline overflow"))?;
    let cancellation = CancellationToken::new();
    let recovered = product
        .research()
        .analytical_reader()
        .feature_dataset(&dataset_id, deadline, &cancellation)
        .context("Python training generation could not be reopened after restart")?
        .ok_or_else(|| anyhow!("Python training generation is absent after restart"))?;
    let generation = recovered.generation();
    let manifest = generation.manifest();
    let recovered_parents = generation.parents();
    if manifest.manifest_version() != expected.manifest_version
        || lower_hex(manifest.content_hash().bytes()) != expected.manifest_content_hash
        || generation
            .build_spec_digest()
            .is_none_or(|digest| lower_hex(digest.digest().bytes()) != expected.build_spec_digest)
        || lower_hex(recovered.policy_digest().bytes()) != expected.policy_digest
        || lower_hex(recovered.universe_digest().bytes()) != expected.universe_digest
        || lower_hex(recovered.python_export_sha256().bytes()) != expected.python_export_sha256
        || recovered_parents.len() != expected.parents.len()
        || recovered_parents
            .iter()
            .zip(&expected.parents)
            .any(|(actual, recorded)| {
                actual.relation() != GenerationParentRelation::DerivedInput
                    || actual.manifest().dataset_id().as_str() != recorded.dataset_id
                    || actual.manifest().manifest_version() != recorded.manifest_version
                    || lower_hex(actual.manifest().content_hash().bytes())
                        != recorded.manifest_content_hash
            })
    {
        bail!("Python training generation changed during restart");
    }
    let splits = recovered.split_counts();
    if splits.train_examples() != expected.train_examples
        || splits.validation_examples() != expected.validation_examples
        || splits.test_examples() != expected.test_examples
    {
        bail!("Python training split evidence changed during restart");
    }
    Ok(())
}

async fn shutdown_product(product: &LocalProduct) -> Result<()> {
    let timeout = product.application().shutdown_timeout();
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
    if requested.contains(FRED_ALFRED) {
        admit_fred_release_inputs(arguments)?;
    } else if arguments.fred_dataset.is_some() || arguments.fred_training_request.is_some() {
        bail!("FRED/ALFRED release inputs require the selected FRED/ALFRED surface");
    }
    if requested.contains(SEC_EDGAR) {
        admit_sec_release_cik(arguments)?;
    } else if arguments.sec_cik.is_some() {
        bail!("SEC release inputs require the selected SEC surface");
    }
    let selected_bls = [BLS_PUBLIC, BLS_REGISTERED]
        .into_iter()
        .filter(|surface| requested.contains(*surface))
        .collect::<Vec<_>>();
    match selected_bls.as_slice() {
        [] => {
            if arguments.bls_dataset.is_some() || arguments.bls_training_request.is_some() {
                bail!("BLS release inputs require one selected BLS surface");
            }
        }
        [surface] => {
            admit_bls_release_inputs(arguments, surface)?;
        }
        _ => bail!("provider release evidence accepts one BLS access tier per run"),
    }
    Ok(ADMITTED_SURFACES
        .into_iter()
        .filter(|surface| requested.contains(surface))
        .collect())
}

fn admit_sec_release_cik(arguments: &ReleaseProviderArguments) -> Result<&str> {
    arguments
        .sec_cik
        .as_deref()
        .filter(|cik| {
            cik.len() == 10
                && cik.bytes().all(|byte| byte.is_ascii_digit())
                && cik.bytes().any(|byte| byte != b'0')
        })
        .ok_or_else(|| {
            anyhow!("selected SEC surface requires one exact nonzero 10-digit --sec-cik")
        })
}

fn admit_fred_release_inputs(
    arguments: &ReleaseProviderArguments,
) -> Result<(SourceIdentifier, &Path)> {
    let provider_dataset = arguments
        .fred_dataset
        .as_deref()
        .ok_or_else(|| anyhow!("selected FRED/ALFRED surface requires --fred-dataset"))
        .and_then(|value| {
            SourceIdentifier::try_from(value).context("FRED/ALFRED provider dataset is invalid")
        })?;
    let analytical_dataset = FredSource::analytical_dataset_identifier(&provider_dataset)
        .context("FRED/ALFRED provider dataset is not an exact bounded observations request")?;
    DatasetId::try_from(analytical_dataset.as_str())
        .context("FRED/ALFRED analytical dataset identity is invalid")?;
    let training = arguments
        .fred_training_request
        .as_deref()
        .ok_or_else(|| anyhow!("selected FRED/ALFRED surface requires --fred-training-request"))?;
    Ok((provider_dataset, training))
}

fn admit_bls_release_inputs<'a>(
    arguments: &'a ReleaseProviderArguments,
    surface_id: &str,
) -> Result<(SourceIdentifier, &'a Path)> {
    let provider_dataset = arguments
        .bls_dataset
        .as_deref()
        .ok_or_else(|| anyhow!("selected BLS surface requires --bls-dataset"))
        .and_then(|value| {
            SourceIdentifier::try_from(value).context("BLS provider dataset is invalid")
        })?;
    let expected_tier = match surface_id {
        BLS_PUBLIC => "bls:timeseries:public-v1:",
        BLS_REGISTERED => "bls:timeseries:registered-v2:",
        _ => bail!("BLS release inputs require one exact BLS surface"),
    };
    if !provider_dataset.as_str().starts_with(expected_tier) {
        bail!("BLS provider dataset does not match the selected access tier");
    }
    let analytical_dataset = BlsSource::analytical_dataset_identifier(&provider_dataset)
        .context("BLS provider dataset is not an exact bounded timeseries request")?;
    DatasetId::try_from(analytical_dataset.as_str())
        .context("BLS analytical dataset identity is invalid")?;
    let training = arguments
        .bls_training_request
        .as_deref()
        .ok_or_else(|| anyhow!("selected BLS surface requires --bls-training-request"))?;
    Ok((provider_dataset, training))
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
        SEC_EDGAR | FRED_ALFRED | BLS_PUBLIC | BLS_REGISTERED | TREASURY_XML | TREASURY_FISCAL
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
