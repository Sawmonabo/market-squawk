use std::error::Error;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use clap::Parser as _;
use market_squawk::application::analysis::{
    AnalysisCatalog, AnalysisDatasetScope, AnalysisDomainService, FeatureDatasetRegistration,
    GovernedBacktestAuthority, GovernedBacktestCommand, GovernedBacktestInputRegistrar,
    GovernedBacktestInputRegistrationInput, GovernedBacktestInputRegistrationReceipt,
    GovernedBacktestRecord,
};
use market_squawk::application::{ApplicationDomainService, application_capabilities};
use market_squawk::cli::{Cli, Command, FeatureCommand};
use market_squawk::{
    AppPaths, PinnedBacktestInput, ProductionBacktestService, ProductionBacktestServiceError,
    ResearchIngestRequest, ResearchService,
};
use market_squawk_analytics::{
    BatchFeatureCatalog, BatchFeatureCatalogConfig, BatchFeaturePolicies,
    MissingValuePolicy as AnalyticsMissingValuePolicy, REQUIRED_BATCH_FEATURE_COUNT,
    ShockComposition, VarianceConvention, WeightPolicy,
};
use market_squawk_backtesting::{
    AVAILABLE_AT_COMPONENT, BacktestContext, BacktestDataset, BacktestEngine, BacktestError,
    BacktestLimits, BacktestLimitsInput, BacktestOutcome, BacktestRequest, BacktestStrategy,
    BacktestStrategyRegistry, DEPTH_COMPONENT, EVENT_AT_COMPONENT, ExperimentLimits,
    ExperimentLimitsInput, MID_PRICE_COMPONENT, PortfolioSeed, RESEARCH_EXECUTION_POLICY_VERSION,
    ResearchExecutionAssumptions, ResearchExecutionAssumptionsInput, ResearchLiquidityPriority,
    SPREAD_COMPONENT, STALE_AT_COMPONENT, UNIVERSE_COMPONENT,
};
use market_squawk_data::{
    CatalogAuthority, CatalogConfig, CatalogLimit, CatalogResultLimits, ChronologicalSplitPolicy,
    ComponentAdjustmentEvidence, ComponentKind, ComponentScope, ComponentSelector, ComponentValue,
    CorporateActionAdjustment, CorporateActionLimits, CorporateActionPolicy,
    CorporateActionSensitivity, DatasetBuildInputs, DatasetBuildLimits, DatasetBuildPolicy,
    DatasetBuildRequest, DatasetId, DatasetManifestRef, DatasetOutputAuthorization,
    FeatureLabelComponentInput, FeatureLabelComponentSpec, FeatureLabelDataset, ObjectStoreConfig,
    ObservationFamilyKey, PinnedInstrumentDefinitions, PinnedQueryOutput, PointInTimeLimits,
    PointInTimePolicy, PointInTimeRevisionMode, QueryLimits, QueryRequest, ResearchQueryEngine,
    ResearchUse, ResearchUseGrantInput, ResearchUseLimits, ResearchUseSet, RightsBasis,
    RightsDecisionInput, SourceOperation, UniverseId, UniverseLimits, UniverseMembership,
    extraction_batch_digest,
};
use market_squawk_domain::{
    AccountId, AuthorizationBasis, AvailabilityEvidence, BasisPoints, ChecksumCapability,
    CoverageDelay, Currency, DataQuality, DeliveryEvidence, DigestAlgorithm, EffectiveInterval,
    EvidenceDigest, ExactPayloadEvidence, InstrumentDefinition, InstrumentId, MacroObservation,
    MetadataRevision, Money, PayloadReference, ResearchContext, ResearchObservation,
    ResearchProvenance, ResearchProvenanceInput, ResearchTemporalCoordinate, ResearchTime,
    RevisionBoundPayloadEvidence, RevisionNumber, SchemaVersion, SequenceCapability, SourceId,
    SourceIdentifier, Timestamp, UniverseMembershipObservation,
};
use market_squawk_execution::{BoundedOrderIntents, StrategyError};
use market_squawk_portfolio::{PortfolioLimitInput, PortfolioLimits};
use market_squawk_services::{
    JsonStructureLimits, RequestContext, RequestId, ResultCompleteness, ServiceError,
    ServiceLimits, TypedToolRequest,
};
use market_squawk_sources::{
    AuthorizationGrant, AuthorizationMode, AvailabilityEvidence as SourceAvailabilityEvidence,
    CanonicalObservationPayload, CoverageDomain, DiscoveryRequest, ExtractionBatch,
    ExtractionRecord, ExtractionRequest, FreshnessPolicy, HistoricalCapability,
    NetworkAccessPolicy, SourceCapabilities, SourceClass, SourceCoverage, SourceMetadata,
    SourceMetadataInput, SourceObject, SourceProtocolProfile,
};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[derive(Debug)]
struct UnusedBacktestInputRegistrar;

#[async_trait]
impl GovernedBacktestInputRegistrar for UnusedBacktestInputRegistrar {
    async fn register_input(
        &self,
        _input: GovernedBacktestInputRegistrationInput,
        _cancellation: CancellationToken,
        _deadline: Instant,
    ) -> Result<GovernedBacktestInputRegistrationReceipt, ServiceError> {
        Err(ServiceError::Unavailable)
    }
}

#[derive(Debug)]
struct UnusedBacktestAuthority;

#[async_trait]
impl GovernedBacktestAuthority for UnusedBacktestAuthority {
    async fn run(
        &self,
        _command: GovernedBacktestCommand,
        _cancellation: CancellationToken,
        _deadline: Instant,
    ) -> Result<GovernedBacktestRecord, ServiceError> {
        Err(ServiceError::Unavailable)
    }

    async fn get(
        &self,
        _run_id: &str,
        _cancellation: CancellationToken,
        _deadline: Instant,
    ) -> Result<Option<GovernedBacktestRecord>, ServiceError> {
        Err(ServiceError::Unavailable)
    }

    fn begin_shutdown(&self) {}

    async fn finish_shutdown(&self, _deadline: Instant) -> Result<(), ServiceError> {
        Ok(())
    }
}

#[test]
fn production_backtest_inventory_is_confined_to_the_controlled_artifact_root()
-> Result<(), Box<dyn Error>> {
    let temporary = tempfile::tempdir()?;
    let paths = AppPaths::prepare(temporary.path().join("market-squawk"))?;
    let _service = ProductionBacktestService::initialize(
        &paths,
        ExperimentLimits::try_new(ExperimentLimitsInput {
            max_trials: 8,
            max_record_bytes: 64 * 1024,
            max_artifact_bytes: 64 * 1024,
            max_metrics: 8,
        })?,
        BacktestStrategyRegistry::try_new(Vec::new())?,
    )?;

    let run_boundary: fn(
        &ProductionBacktestService,
        PinnedBacktestInput,
        &SourceIdentifier,
        &CancellationToken,
    ) -> Result<BacktestOutcome, ProductionBacktestServiceError> = ProductionBacktestService::run;
    let _ = run_boundary;
    let input_contract: fn(PinnedBacktestInput) -> PinnedInstrumentDefinitions =
        |input| input.instrument_definitions;
    let _ = input_contract;
    assert!(paths.artifacts()?.root().join("backtesting/v1").is_dir());
    Ok(())
}

#[derive(Debug, Default)]
struct RecordingStrategy {
    definition_revisions: Vec<u64>,
}

impl BacktestStrategy for RecordingStrategy {
    fn on_observation(
        &mut self,
        context: &BacktestContext<'_>,
    ) -> Result<BoundedOrderIntents, StrategyError> {
        self.definition_revisions
            .push(context.execution_terms().definition_revision().get());
        Ok(BoundedOrderIntents::new())
    }
}

#[tokio::test]
async fn pinned_dataset_resolves_historical_instrument_definitions_per_decision() -> TestResult {
    let directory = tempfile::tempdir()?;
    let paths = AppPaths::prepare(directory.path().join("market-squawk"))?;
    let catalog_config = fixture_catalog_config(&paths)?;
    let object_config = ObjectStoreConfig::try_new(8 * 1024 * 1024, 128, Duration::from_secs(60))?;
    let instrument_id: InstrumentId = "00000000-0000-0000-0000-000000000020".parse()?;
    let source = fixture_source("backtest-fixture")?;
    let (batch, membership_evidence) = fixture_extraction_batch(instrument_id)?;
    let rights = RightsDecisionInput {
        source_id: source.source_id().clone(),
        payload_digest: extraction_batch_digest(&batch)?,
        retrieved_at: Timestamp::from_unix_nanos(15),
        basis: RightsBasis::reviewed_terms("https://example.test/backtest-fixture/v1", digest(31))?,
        authorization_evidence: digest(32),
        authorization_expires_at: Some(Timestamp::from_unix_nanos(i64::MAX)),
        permitted_operations: vec![SourceOperation::Persist],
    };

    {
        let catalog = CatalogAuthority::open(catalog_config.clone())?;
        catalog.register_source(&source, rights.retrieved_at)?;
        catalog.register_source(
            &fixture_source("market-squawk.derived")?,
            Timestamp::from_unix_nanos(10),
        )?;
        let registered_rights = catalog.admit_source_rights(rights.clone())?;
        catalog.admit_research_use_grant(ResearchUseGrantInput::try_new(
            registered_rights.rights_id(),
            ResearchUseSet::try_new(vec![ResearchUse::LocalAnalysis])?,
            digest(33),
            Some(Timestamp::from_unix_nanos(i64::MAX)),
        )?)?;
    }

    let service = ResearchService::initialize(&paths, catalog_config.clone(), 8, object_config)?;
    let source_dataset = service
        .ingest(
            ResearchIngestRequest::locally_observed(
                source,
                rights,
                "backtest-reference-authority-v1",
                batch,
            )?,
            CancellationToken::new(),
        )
        .await?;
    let built = service
        .build_dataset(
            fixture_dataset_request(
                "derived.backtest.reference-authority",
                source_dataset.manifest().clone(),
                instrument_id,
                membership_evidence,
            )?,
            CancellationToken::new(),
        )
        .await?;
    let successor = service
        .build_dataset(
            fixture_dataset_request(
                "derived.backtest.reference-authority-successor",
                source_dataset.manifest().clone(),
                instrument_id,
                membership_evidence,
            )?,
            CancellationToken::new(),
        )
        .await?;
    let terminal = service
        .build_dataset(
            fixture_dataset_request(
                "derived.backtest.reference-authority-terminal",
                source_dataset.manifest().clone(),
                instrument_id,
                membership_evidence,
            )?,
            CancellationToken::new(),
        )
        .await?;
    let legacy_paths = AppPaths::prepare(directory.path().join("legacy-feature-catalog"))?;
    let legacy = fixture_feature_dataset(
        &legacy_paths,
        "derived.backtest.reference-authority-secondary",
        instrument_id,
    )
    .await?;
    let legacy_scope = AnalysisDatasetScope::try_new(
        vec![instrument_id],
        Timestamp::from_unix_nanos(1),
        Timestamp::from_unix_nanos(200),
        vec![SourceId::try_from("backtest-fixture")?],
        vec![DataQuality::DirectVerified],
    )?;
    let analysis = AnalysisDomainService::new_with_feature_reader(
        Arc::new(fixture_analysis_catalog(vec![
            FeatureDatasetRegistration::new(legacy, legacy_scope.clone()),
            FeatureDatasetRegistration::new(successor.clone(), legacy_scope),
        ])?),
        service.analytical_reader(),
        Arc::new(UnusedBacktestInputRegistrar),
        Arc::new(UnusedBacktestAuthority),
    );
    let undersized = analysis
        .call(
            feature_dataset_request(json!({
                "resultLimits": {"maximumItems": 2, "maximumBytes": 65536}
            }))?,
            feature_dataset_context(101)?,
        )
        .await;
    assert!(matches!(undersized, Err(ServiceError::ResourceExhausted)));

    let first_page_evidence = analysis
        .call(
            feature_dataset_request(json!({
                "resultLimits": {
                    "maximumItems": REQUIRED_BATCH_FEATURE_COUNT + 1,
                    "maximumBytes": 65536
                }
            }))?,
            feature_dataset_context(102)?,
        )
        .await?;
    let first_page_byte_ceiling = first_page_evidence.encoded_bytes();
    assert_eq!(
        first_page_evidence.item_count(),
        REQUIRED_BATCH_FEATURE_COUNT + 1
    );
    assert_eq!(
        first_page_evidence.structured_content()["nextAfterDataset"],
        "derived.backtest.reference-authority"
    );
    assert_eq!(first_page_evidence.structured_content()["hasMore"], true);

    let first = analysis
        .call(
            feature_dataset_request(json!({
                "resultLimits": {
                    "maximumItems": REQUIRED_BATCH_FEATURE_COUNT + 3,
                    "maximumBytes": first_page_byte_ceiling
                }
            }))?,
            feature_dataset_context(103)?,
        )
        .await
        .map_err(|error| {
            std::io::Error::other(format!("tight-byte first feature page failed: {error:?}"))
        })?;
    let first_content = first.structured_content();
    let first_items = first_content["items"]
        .as_array()
        .ok_or("first feature page has no items")?;
    let cursor = first_content["nextAfterDataset"]
        .as_str()
        .ok_or("first feature page has no durable cursor")?
        .to_owned();
    assert_eq!(first_items.len(), REQUIRED_BATCH_FEATURE_COUNT + 1);
    assert!(
        first_items[..REQUIRED_BATCH_FEATURE_COUNT]
            .iter()
            .all(|item| item["kind"] == "feature_contract")
    );
    assert_eq!(
        first_items[REQUIRED_BATCH_FEATURE_COUNT]["manifest"]["dataset"],
        "derived.backtest.reference-authority"
    );
    assert_eq!(cursor, "derived.backtest.reference-authority");
    assert_eq!(first_content["hasMore"], true);
    assert_eq!(first.encoded_bytes(), first_page_byte_ceiling);
    assert_eq!(
        first.metadata().available_items(),
        Some(REQUIRED_BATCH_FEATURE_COUNT + 4)
    );
    assert_eq!(first.metadata().source_coverage()["datasetCount"], 1);

    let cli = Cli::try_parse_from([
        "market-squawk",
        "feature",
        "list",
        "--after-dataset",
        cursor.as_str(),
    ])
    .map_err(|error| {
        std::io::Error::other(format!("feature continuation CLI parsing failed: {error}"))
    })?;
    assert!(matches!(
        cli.command,
        Command::Feature {
            command: FeatureCommand::List {
                after_dataset: Some(ref value)
            }
        } if value == &cursor
    ));

    let continuation_evidence = analysis
        .call(
            feature_dataset_request(json!({
                "afterDataset": cursor.clone(),
                "resultLimits": {"maximumItems": 1, "maximumBytes": 65536}
            }))?,
            feature_dataset_context(104)?,
        )
        .await?;
    let continuation_byte_ceiling = continuation_evidence.encoded_bytes();
    assert_eq!(continuation_evidence.item_count(), 1);
    assert_eq!(
        continuation_evidence.structured_content()["items"][0]["manifest"]["dataset"],
        "derived.backtest.reference-authority-secondary"
    );
    assert_eq!(
        continuation_evidence.structured_content()["nextAfterDataset"],
        "derived.backtest.reference-authority-secondary"
    );
    assert_eq!(continuation_evidence.structured_content()["hasMore"], true);

    let continued_request = feature_dataset_request(json!({
        "afterDataset": cursor,
        "resultLimits": {
            "maximumItems": 2,
            "maximumBytes": continuation_byte_ceiling
        }
    }))
    .map_err(|error| {
        std::io::Error::other(format!("feature continuation admission failed: {error}"))
    })?;
    let continued = analysis
        .call(continued_request, feature_dataset_context(105)?)
        .await
        .map_err(|error| {
            std::io::Error::other(format!("tight-byte feature continuation failed: {error:?}"))
        })?;
    let continued_content = continued.structured_content();
    let continued_items = continued_content["items"]
        .as_array()
        .ok_or("continued feature page has no items")?;
    let continued_cursor = continued_content["nextAfterDataset"]
        .as_str()
        .ok_or("continued feature page has no durable cursor")?
        .to_owned();
    assert_eq!(continued_items.len(), 1);
    assert_eq!(continued_items[0]["kind"], "feature_dataset");
    assert_eq!(
        continued_items[0]["manifest"]["dataset"],
        "derived.backtest.reference-authority-secondary"
    );
    assert_eq!(
        continued_cursor,
        "derived.backtest.reference-authority-secondary"
    );
    assert_eq!(continued_content["hasMore"], true);
    assert_eq!(continued.encoded_bytes(), continuation_byte_ceiling);
    assert_eq!(continued.metadata().available_items(), Some(3));
    assert_eq!(continued.metadata().source_coverage()["datasetCount"], 1);

    let final_page = analysis
        .call(
            feature_dataset_request(json!({
                "afterDataset": continued_cursor,
                "resultLimits": {"maximumItems": 2, "maximumBytes": 65536}
            }))?,
            feature_dataset_context(106)?,
        )
        .await?;
    let final_content = final_page.structured_content();
    let final_items = final_content["items"]
        .as_array()
        .ok_or("final feature page has no items")?;
    assert_eq!(final_items.len(), 2);
    assert_eq!(final_items[0]["kind"], "feature_dataset");
    assert_eq!(
        final_items[0]["manifest"]["dataset"],
        "derived.backtest.reference-authority-successor"
    );
    assert!(
        final_items[0]["pythonExportSha256"].is_string(),
        "the durable generation must win an overlapping legacy identity"
    );
    assert_eq!(final_items[1]["kind"], "feature_dataset");
    assert_eq!(
        final_items[1]["manifest"]["dataset"],
        "derived.backtest.reference-authority-terminal"
    );
    assert_eq!(final_content["hasMore"], false);
    assert!(final_content["nextAfterDataset"].is_null());
    assert_eq!(
        final_page.metadata().completeness(),
        ResultCompleteness::Complete
    );
    assert_eq!(final_page.metadata().available_items(), None);

    let exact_overlap = analysis
        .call(
            feature_dataset_request(json!({
                "dataset": "derived.backtest.reference-authority-successor",
                "resultLimits": {
                    "maximumItems": REQUIRED_BATCH_FEATURE_COUNT + 1,
                    "maximumBytes": 65536
                }
            }))?,
            feature_dataset_context(107)?,
        )
        .await?;
    let exact_overlap_items = exact_overlap.structured_content()["items"]
        .as_array()
        .ok_or("exact overlap result has no items")?;
    let exact_overlap_datasets = exact_overlap_items
        .iter()
        .filter(|item| item["kind"] == "feature_dataset")
        .collect::<Vec<_>>();
    assert_eq!(exact_overlap_datasets.len(), 1);
    assert_eq!(
        exact_overlap_datasets[0]["manifest"]["dataset"],
        "derived.backtest.reference-authority-successor"
    );
    assert!(exact_overlap_datasets[0]["pythonExportSha256"].is_string());

    let exhausted = analysis
        .call(
            feature_dataset_request(json!({
                "afterDataset": "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
                "resultLimits": {"maximumItems": 2, "maximumBytes": 65536}
            }))?,
            feature_dataset_context(108)?,
        )
        .await?;
    assert!(exhausted.structured_content().is_null());
    assert_eq!(exhausted.item_count(), 0);
    assert_eq!(
        exhausted.metadata().completeness(),
        ResultCompleteness::Complete
    );
    assert_eq!(exhausted.metadata().available_items(), None);

    let conflicting = analysis
        .call(
            feature_dataset_request(json!({
                "dataset": "derived.backtest.reference-authority-successor",
                "afterDataset": "derived.backtest.reference-authority",
                "resultLimits": {"maximumItems": 2, "maximumBytes": 65536}
            }))?,
            feature_dataset_context(109)?,
        )
        .await;
    assert!(matches!(conflicting, Err(ServiceError::InvalidRequest)));

    let query = ResearchQueryEngine::from_pinned_dataset(
        built.pinned().clone(),
        "components",
        service.analytical().object_store(),
        CancellationToken::new(),
    )
    .await?;
    let baseline_output = query_backtest_rows(&query, built.manifest()).await?;
    let changed_identity_output = query_backtest_rows(&query, built.manifest()).await?;
    let coverage_mismatch_output = query_backtest_rows(&query, built.manifest()).await?;
    drop(query);
    drop(analysis);
    drop(terminal);
    drop(successor);
    drop(built);
    drop(source_dataset);
    drop(service);

    let definition_v1 = instrument_definition(instrument_id, 1, "0.01")?;
    let definition_v2 = instrument_definition(instrument_id, 2, "0.05")?;
    let extra_id: InstrumentId = "00000000-0000-0000-0000-000000000021".parse()?;
    let extra_definition = instrument_definition(extra_id, 1, "0.01")?;
    let catalog = CatalogAuthority::open(catalog_config)?;
    catalog.put_instrument(&definition_v1, Timestamp::from_unix_nanos(10))?;
    catalog.put_instrument(&definition_v2, Timestamp::from_unix_nanos(20))?;
    catalog.put_instrument(&extra_definition, Timestamp::from_unix_nanos(10))?;

    let baseline_definitions = catalog.pin_instrument_definitions(
        &[instrument_id],
        Timestamp::from_unix_nanos(30),
        CatalogLimit::new(2)?,
    )?;
    let changed_identity_definitions = catalog.pin_instrument_definitions(
        &[instrument_id],
        Timestamp::from_unix_nanos(31),
        CatalogLimit::new(2)?,
    )?;
    assert_eq!(
        baseline_definitions.content_identity(),
        changed_identity_definitions.content_identity()
    );
    assert_ne!(
        baseline_definitions.audit_identity(),
        changed_identity_definitions.audit_identity()
    );

    let limits = fixture_backtest_limits()?;
    let dataset =
        BacktestDataset::try_from_pinned_query(baseline_output, baseline_definitions, limits)?;
    let changed_identity_dataset = BacktestDataset::try_from_pinned_query(
        changed_identity_output,
        changed_identity_definitions,
        limits,
    )?;
    assert_ne!(dataset.identity(), changed_identity_dataset.identity());

    let coverage_mismatch = catalog.pin_instrument_definitions(
        &[instrument_id, extra_id],
        Timestamp::from_unix_nanos(30),
        CatalogLimit::new(3)?,
    )?;
    assert!(matches!(
        BacktestDataset::try_from_pinned_query(coverage_mismatch_output, coverage_mismatch, limits),
        Err(BacktestError::InvalidDataset)
    ));

    let mut strategy = RecordingStrategy::default();
    BacktestEngine::run(
        &fixture_backtest_request(dataset)?,
        &mut strategy,
        &CancellationToken::new(),
    )?;
    assert_eq!(strategy.definition_revisions, vec![1, 2]);
    Ok(())
}

fn fixture_catalog_config(paths: &AppPaths) -> TestResult<CatalogConfig> {
    Ok(CatalogConfig::try_new(
        paths.catalog()?.clone(),
        Duration::from_millis(750),
        CatalogLimit::new(64)?,
        CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
    )?)
}

fn fixture_extraction_batch(
    instrument_id: InstrumentId,
) -> TestResult<(ExtractionBatch, EvidenceDigest)> {
    let membership = fixture_membership_observation(instrument_id)?;
    let membership_evidence =
        CanonicalObservationPayload::try_from_observation(&membership)?.identity();
    let observations = [
        (membership, 1, 1, "membership-v1"),
        (
            fixture_macro_observation("backtest-feature", 1, 1)?,
            1,
            1,
            "feature-v1",
        ),
        (
            fixture_macro_observation("backtest-label", 40, 40)?,
            40,
            40,
            "label-v1",
        ),
    ];
    let discovery = DiscoveryRequest::try_new(
        SourceIdentifier::try_from("backtest-research")?,
        Some(Timestamp::from_unix_nanos(50)),
        NonZeroU16::MIN,
        Timestamp::from_unix_nanos(1_000),
    )?;
    let object = SourceObject::try_new(
        SourceId::try_from("backtest-fixture")?,
        MetadataRevision::new(SourceIdentifier::try_from("revision-1")?),
        &discovery,
        SourceIdentifier::try_from("backtest-observations")?,
        SourceIdentifier::try_from("application-json")?,
        ExactPayloadEvidence::from_content_digest(digest(4)),
        EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?,
        Some(Timestamp::from_unix_nanos(1)),
        Some(16 * 1024),
    )?;
    let request = ExtractionRequest::try_new(
        object,
        NonZeroU32::new(3).ok_or("nonzero extraction record limit")?,
        NonZeroU64::new(1024 * 1024).ok_or("nonzero extraction byte limit")?,
        Timestamp::from_unix_nanos(1_000),
    )?;
    let records = observations
        .into_iter()
        .map(|(observation, effective_at, available_at, revision)| {
            let payload = serde_json::to_vec(&observation)?;
            let evidence =
                EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(&payload).into());
            Ok(ExtractionRecord::try_new(
                &request,
                SourceIdentifier::try_from("market-squawk-research-v3")?,
                ExactPayloadEvidence::from_content_digest(evidence),
                Timestamp::from_unix_nanos(effective_at),
                Some(Timestamp::from_unix_nanos(available_at)),
                SourceAvailabilityEvidence::Observed {
                    available_at: Timestamp::from_unix_nanos(available_at),
                    evidence: SourceIdentifier::try_from("fixture-publication")?,
                },
                SourceIdentifier::try_from(revision)?,
                None,
                payload.into(),
            )?)
        })
        .collect::<TestResult<Vec<_>>>()?;
    Ok((
        ExtractionBatch::try_new(&request, records)?,
        membership_evidence,
    ))
}

fn fixture_membership_observation(instrument_id: InstrumentId) -> TestResult<ResearchObservation> {
    Ok(ResearchObservation::UniverseMembership(
        UniverseMembershipObservation::new(
            fixture_context(Some(instrument_id), "universe-membership-1", 1, 1)?,
            SourceIdentifier::try_from("backtest.historical")?,
            EffectiveInterval::new(Timestamp::from_unix_nanos(1), None)?,
        )?,
    ))
}

fn fixture_macro_observation(
    series: &str,
    effective_at: i64,
    available_at: i64,
) -> TestResult<ResearchObservation> {
    Ok(ResearchObservation::Macro(MacroObservation::new(
        fixture_context(None, series, effective_at, available_at)?,
        SourceIdentifier::try_from(series)?,
        Decimal::ONE,
        SourceIdentifier::try_from("USD")?,
    )))
}

fn fixture_context(
    instrument_id: Option<InstrumentId>,
    source_identifier: &str,
    effective_at: i64,
    available_at: i64,
) -> TestResult<ResearchContext> {
    let source_identifier = SourceIdentifier::try_from(source_identifier)?;
    let available_at = Timestamp::from_unix_nanos(available_at);
    Ok(ResearchContext::new(
        ResearchProvenance::try_new(ResearchProvenanceInput {
            source_id: SourceId::try_from("backtest-fixture")?,
            instrument_id,
            venue_id: None,
            source_identifier: source_identifier.clone(),
            source_timestamp: Some(Timestamp::from_unix_nanos(effective_at)),
            received_at: available_at,
            ingested_at: available_at,
            quality: DataQuality::OfficialDelayed,
            payload_reference: PayloadReference::SourceReference(source_identifier.clone()),
            availability: AvailabilityEvidence::evidenced(
                available_at,
                SourceIdentifier::try_from("fixture-publication")?,
            ),
        })?,
        ResearchTime::new(
            Timestamp::from_unix_nanos(effective_at),
            Some(available_at),
            RevisionNumber::new(1)?,
            None,
        )?,
    )?)
}

fn fixture_dataset_request(
    dataset_id: &str,
    source_manifest: DatasetManifestRef,
    instrument_id: InstrumentId,
    membership_evidence: EvidenceDigest,
) -> TestResult<DatasetBuildRequest> {
    let specs = fixture_component_specs()?;
    let inputs = DatasetBuildInputs::try_new(
        vec![source_manifest.clone()],
        UniverseId::try_from("backtest.historical")?,
        vec![UniverseMembership::new(
            instrument_id,
            EffectiveInterval::new(Timestamp::from_unix_nanos(1), None)?,
            AvailabilityEvidence::evidenced(
                Timestamp::from_unix_nanos(1),
                SourceIdentifier::try_from("fixture-publication")?,
            ),
            source_manifest,
            membership_evidence,
        )],
        specs.clone(),
        vec![
            fixture_dataset_example("backtest-before-boundary", instrument_id, 19, &specs)?,
            fixture_dataset_example("backtest-after-boundary", instrument_id, 21, &specs)?,
        ],
    )?;
    Ok(DatasetBuildRequest::try_new(
        DatasetId::try_from(dataset_id)?,
        inputs,
        DatasetBuildPolicy::new(
            ChronologicalSplitPolicy::try_new(
                Timestamp::from_unix_nanos(60),
                Timestamp::from_unix_nanos(70),
                Timestamp::from_unix_nanos(80),
            )?,
            PointInTimePolicy::try_new(NonZeroU32::MIN, PointInTimeRevisionMode::LatestKnown)?,
            CorporateActionPolicy::new(CorporateActionAdjustment::Raw, NonZeroU32::MIN),
            market_squawk_data::MissingValuePolicy::Preserve,
            SourceIdentifier::try_from("backtest-fixture-builder-v1")?,
        ),
        ResearchUse::LocalAnalysis,
        ResearchUseLimits::try_new(
            8,
            32,
            32,
            8,
            2 * 1024 * 1024,
            Duration::from_secs(2),
            Duration::from_secs(30),
        )?,
        DatasetOutputAuthorization::try_new(
            SourceId::try_from("market-squawk.derived")?,
            RightsBasis::reviewed_terms("https://example.test/local-derived/v1", digest(62))?,
            digest(63),
            None,
        )?,
        DatasetBuildLimits::try_new(
            16,
            2,
            8,
            16,
            4 * 1024 * 1024,
            Duration::from_secs(5),
            PointInTimeLimits::try_new(16, 16, 4, 16, 1024 * 1024)?,
            UniverseLimits::try_new(4, 1024 * 1024)?,
            CorporateActionLimits::try_new(
                NonZeroUsize::new(4).ok_or("nonzero action limit")?,
                NonZeroUsize::new(1024 * 1024).ok_or("nonzero action byte limit")?,
            )?,
        )?,
    )?)
}

async fn fixture_feature_dataset(
    paths: &AppPaths,
    dataset_id: &str,
    instrument_id: InstrumentId,
) -> TestResult<FeatureLabelDataset> {
    let catalog_config = fixture_catalog_config(paths)?;
    let object_config = ObjectStoreConfig::try_new(8 * 1024 * 1024, 128, Duration::from_secs(60))?;
    let source = fixture_source("backtest-fixture")?;
    let (batch, membership_evidence) = fixture_extraction_batch(instrument_id)?;
    let rights = RightsDecisionInput {
        source_id: source.source_id().clone(),
        payload_digest: extraction_batch_digest(&batch)?,
        retrieved_at: Timestamp::from_unix_nanos(15),
        basis: RightsBasis::reviewed_terms("https://example.test/backtest-fixture/v1", digest(31))?,
        authorization_evidence: digest(32),
        authorization_expires_at: Some(Timestamp::from_unix_nanos(i64::MAX)),
        permitted_operations: vec![SourceOperation::Persist],
    };
    {
        let catalog = CatalogAuthority::open(catalog_config.clone())?;
        catalog.register_source(&source, rights.retrieved_at)?;
        catalog.register_source(
            &fixture_source("market-squawk.derived")?,
            Timestamp::from_unix_nanos(10),
        )?;
        let registered_rights = catalog.admit_source_rights(rights.clone())?;
        catalog.admit_research_use_grant(ResearchUseGrantInput::try_new(
            registered_rights.rights_id(),
            ResearchUseSet::try_new(vec![ResearchUse::LocalAnalysis])?,
            digest(33),
            Some(Timestamp::from_unix_nanos(i64::MAX)),
        )?)?;
    }
    let service = ResearchService::initialize(paths, catalog_config, 8, object_config)?;
    let source_dataset = service
        .ingest(
            ResearchIngestRequest::locally_observed(
                source,
                rights,
                "legacy-feature-pagination-v1",
                batch,
            )?,
            CancellationToken::new(),
        )
        .await?;
    service
        .build_dataset(
            fixture_dataset_request(
                dataset_id,
                source_dataset.manifest().clone(),
                instrument_id,
                membership_evidence,
            )?,
            CancellationToken::new(),
        )
        .await
        .map_err(Into::into)
}

fn fixture_analysis_catalog(
    feature_datasets: Vec<FeatureDatasetRegistration>,
) -> TestResult<AnalysisCatalog> {
    let config = BatchFeatureCatalogConfig::try_new(
        NonZeroU32::new(252).ok_or("nonzero periods per year")?,
        NonZeroU32::new(950_000).ok_or("nonzero confidence level")?,
        6,
        BatchFeaturePolicies::new(
            VarianceConvention::Sample,
            AnalyticsMissingValuePolicy::Reject,
            WeightPolicy::PositiveNormalized,
            market_squawk_domain::RoundingPolicy::NearestEven,
            ShockComposition::Compounded,
        ),
    )?;
    Ok(AnalysisCatalog::try_new(
        Vec::new(),
        BatchFeatureCatalog::try_new(config, "feature-pagination-test-v1")?,
        feature_datasets,
    )?)
}

fn feature_dataset_request(arguments: Value) -> TestResult<TypedToolRequest> {
    let arguments = arguments
        .as_object()
        .cloned()
        .ok_or("feature dataset arguments must be an object")?;
    Ok(application_capabilities()?
        .find("Analysis.GetFeatureDatasets")
        .ok_or("feature dataset operation is not registered")?
        .admit(arguments)?)
}

fn feature_dataset_context(id: i64) -> TestResult<RequestContext> {
    Ok(RequestContext::new(
        RequestId::Integer(id),
        CancellationToken::new(),
        Instant::now() + Duration::from_secs(5),
        ServiceLimits::try_new(
            64 * 1024,
            64,
            64 * 1024,
            64,
            JsonStructureLimits::try_new(16, 16 * 1024, 256, 256)?,
        )?,
    ))
}

fn fixture_component_specs() -> TestResult<Vec<FeatureLabelComponentSpec>> {
    [
        (ComponentKind::Feature, EVENT_AT_COMPONENT),
        (ComponentKind::Feature, AVAILABLE_AT_COMPONENT),
        (ComponentKind::Feature, STALE_AT_COMPONENT),
        (ComponentKind::Feature, MID_PRICE_COMPONENT),
        (ComponentKind::Feature, SPREAD_COMPONENT),
        (ComponentKind::Feature, DEPTH_COMPONENT),
        (ComponentKind::Feature, UNIVERSE_COMPONENT),
        (ComponentKind::Label, "market_squawk.backtest.future_label"),
    ]
    .into_iter()
    .map(|(kind, name)| {
        FeatureLabelComponentSpec::try_new(
            kind,
            ComponentScope::Global,
            CorporateActionSensitivity::NotApplicable,
            name,
            NonZeroU32::MIN,
        )
        .map_err(Into::into)
    })
    .collect()
}

fn fixture_dataset_example(
    example_id: &str,
    instrument_id: InstrumentId,
    cutoff_at: i64,
    specs: &[FeatureLabelComponentSpec],
) -> TestResult<market_squawk_data::DatasetExample> {
    let feature_selector = ObservationFamilyKey::Macro {
        source_id: SourceId::try_from("backtest-fixture")?,
        series: SourceIdentifier::try_from("backtest-feature")?,
        effective: ResearchTemporalCoordinate::exact(Timestamp::from_unix_nanos(1)),
    };
    let label_selector = ObservationFamilyKey::Macro {
        source_id: SourceId::try_from("backtest-fixture")?,
        series: SourceIdentifier::try_from("backtest-label")?,
        effective: ResearchTemporalCoordinate::exact(Timestamp::from_unix_nanos(40)),
    };
    let components = specs
        .iter()
        .cloned()
        .map(|spec| {
            let value = match spec.name() {
                EVENT_AT_COMPONENT => cutoff_at - 2,
                AVAILABLE_AT_COMPONENT => cutoff_at - 1,
                STALE_AT_COMPONENT => cutoff_at + 5,
                MID_PRICE_COMPONENT => 100 + cutoff_at,
                SPREAD_COMPONENT => 20,
                DEPTH_COMPONENT => 10,
                UNIVERSE_COMPONENT | "market_squawk.backtest.future_label" => 1,
                _ => return Err("unexpected backtest fixture component".into()),
            };
            let selector = if spec.kind() == ComponentKind::Label {
                label_selector.clone()
            } else {
                feature_selector.clone()
            };
            Ok(FeatureLabelComponentInput::try_new(
                spec,
                ComponentValue::decimal(Decimal::from(value), None, None)?,
                vec![ComponentSelector::new(selector)],
                ComponentAdjustmentEvidence::NotApplicable,
            )?)
        })
        .collect::<TestResult<Vec<_>>>()?;
    Ok(market_squawk_data::DatasetExample::try_new(
        example_id,
        instrument_id,
        Timestamp::from_unix_nanos(cutoff_at),
        Timestamp::from_unix_nanos(50),
        components,
    )?)
}

async fn query_backtest_rows(
    query: &ResearchQueryEngine,
    manifest: &DatasetManifestRef,
) -> TestResult<PinnedQueryOutput> {
    Ok(query
        .query_pinned(
            QueryRequest::try_new(manifest.clone(), "SELECT * FROM components")?,
            QueryLimits::try_new(
                32,
                256 * 1024,
                4 * 1024 * 1024,
                1,
                64,
                64,
                Duration::from_secs(2),
            )?,
            CancellationToken::new(),
        )
        .await?)
}

fn fixture_source(source_id: &str) -> TestResult<SourceMetadata> {
    let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
    Ok(SourceMetadata::try_new(SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        SourceId::try_from(source_id)?,
        RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(SourceIdentifier::try_from("revision-1")?),
            ExactPayloadEvidence::from_content_digest(digest(1)),
        ),
        SourceClass::LocalFile,
        SourceIdentifier::try_from("local")?,
        AuthorizationGrant::new(
            AuthorizationMode::UserOwnedLocal,
            AuthorizationBasis::new(SourceIdentifier::try_from("user-owned-file")?),
            ExactPayloadEvidence::from_content_digest(digest(2)),
            effective,
        ),
        SourceCoverage::try_non_instrument(
            ExactPayloadEvidence::from_content_digest(digest(3)),
            effective,
            CoverageDomain::Macroeconomic,
            CoverageDelay::Delayed(1),
            DeliveryEvidence::Unknown,
        )?,
        DataQuality::OfficialDelayed,
        NetworkAccessPolicy::Denied,
        FreshnessPolicy::try_new(1, 1, 1, 1, 0)?,
        None,
        SourceCapabilities::new(
            false,
            true,
            SequenceCapability::Unsupported,
            ChecksumCapability::Unsupported,
            HistoricalCapability::RevisionPreserving,
            false,
        ),
        SourceProtocolProfile::NotLive,
    ))?)
}

fn instrument_definition(
    instrument_id: InstrumentId,
    revision: u64,
    tick_size: &str,
) -> TestResult<InstrumentDefinition> {
    Ok(serde_json::from_value(serde_json::json!({
        "instrument_id": instrument_id.to_string(),
        "definition_revision": revision,
        "asset_class": "equity",
        "primary_denomination": { "kind": "currency", "value": "USD" },
        "quote_currency": "USD",
        "tick_size": tick_size,
        "lot_size": "1",
        "contract_multiplier": "1",
        "venue_mappings": [],
        "identifiers": [],
        "trading_status": "active"
    }))?)
}

fn fixture_backtest_limits() -> TestResult<BacktestLimits> {
    Ok(BacktestLimits::try_new(BacktestLimitsInput {
        max_observations: 8,
        max_pending_intents: 4,
        max_fills: 4,
        max_retained_bytes: 1024 * 1024,
    })?)
}

fn fixture_backtest_request(dataset: BacktestDataset) -> TestResult<BacktestRequest> {
    let account_id: AccountId = "00000000-0000-0000-0000-000000000030".parse()?;
    Ok(BacktestRequest::try_new(
        dataset,
        ResearchExecutionAssumptions::try_new(ResearchExecutionAssumptionsInput {
            version: RESEARCH_EXECUTION_POLICY_VERSION,
            fee_basis_points: BasisPoints::new(0),
            slippage_basis_points: BasisPoints::new(0),
            maximum_random_slippage_basis_points: BasisPoints::new(0),
            maximum_participation_basis_points: BasisPoints::new(10_000),
            liquidity_priority: ResearchLiquidityPriority::SignalTimeThenOrderId,
            latency_nanos: 1,
            allow_partial_fills: true,
            fee_decimal_scale: 4,
        })?,
        PortfolioSeed::try_new(
            account_id,
            Money::new(Decimal::from(1_000), Currency::try_from("USD")?),
            PortfolioLimits::try_new(PortfolioLimitInput {
                max_accounts: 1,
                max_instruments: 4,
                max_lots: 4,
                max_transactions: 4,
                max_factors: 4,
                max_scenarios: 4,
                max_history: 4,
                max_results: 16,
                max_retained_bytes: 1024 * 1024,
            })?,
        )?,
        None,
        vec![SourceIdentifier::try_from("backtest-feature-labels")?],
        7,
        fixture_backtest_limits()?,
    )?)
}

fn digest(byte: u8) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, [byte; 32])
}
