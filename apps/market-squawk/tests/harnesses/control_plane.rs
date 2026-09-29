// Rust #159105: this macOS-only test-link diagnostic is caused by the measured
// `__eh_frame` exceeding arm64 compact-unwind's 24-bit offset range.
#![allow(linker_messages)]

#[path = "../backtest_vertical.rs"]
mod backtest_vertical;
#[path = "../decision_persistence.rs"]
mod decision_persistence;
#[path = "../journal.rs"]
mod journal;
#[path = "../journal_path_integration.rs"]
mod journal_path_integration;
#[path = "../production_mcp_composition.rs"]
mod production_mcp_composition;
#[cfg(feature = "release-evidence")]
#[path = "../release_demonstration.rs"]
mod release_demonstration;
#[path = "../replay.rs"]
mod replay;
#[path = "../research_vertical.rs"]
mod research_vertical;

#[cfg(test)]
mod product_doctor {
    use std::collections::BTreeMap;

    use market_squawk::doctor;
    use market_squawk_platform::{AppConfig, ConfigOverrides, ConfigSources};

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[tokio::test]
    async fn reports_provenance_without_mutating_storage() -> TestResult {
        let temporary = tempfile::tempdir()?;
        let environment = BTreeMap::new();
        let config = AppConfig::load(ConfigSources::new(
            None,
            &environment,
            ConfigOverrides {
                data_dir: Some(temporary.path().to_path_buf()),
                source_shutdown_ms: Some(60_000),
                ..ConfigOverrides::default()
            },
        ))?;
        let entries_before = std::fs::read_dir(temporary.path())?.count();
        let report = serde_json::to_value(doctor::inspect(&config).await?)?;
        let entries_after = std::fs::read_dir(temporary.path())?.count();

        assert_eq!(report["status"], "blocked");
        assert_eq!(report["configuration"]["sourceShutdownMs"]["value"], 60_000);
        assert_eq!(report["configuration"]["sourceShutdownMs"]["origin"], "cli");
        assert_eq!(entries_before, entries_after);
        assert_eq!(report["localStorage"]["modifiedByInspection"], false);
        assert_eq!(report["localStorage"]["layout"]["state"], "unavailable");
        assert_eq!(report["application"]["descriptorContractValid"], true);
        assert_eq!(report["application"]["requiredDomainsComplete"], true);
        assert_eq!(report["mcp"]["descriptorContractValid"], true);
        assert_eq!(report["artifacts"]["capabilityConfined"], false);
        assert_eq!(report["artifacts"]["arbitraryPathAccess"], false);
        assert!(
            report["providers"]["surfaces"]
                .as_array()
                .is_some_and(|surfaces| !surfaces.is_empty())
        );
        assert_eq!(report["providers"]["runtimeObservation"], "not_observed");
        assert!(
            report["releaseBlockers"]
                .as_array()
                .is_some_and(|blockers| !blockers.is_empty())
        );
        Ok(())
    }
}

#[cfg(test)]
mod portfolio_application {
    use std::error::Error;
    use std::io::Write as _;
    use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use market_squawk::application::{ApplicationDomainService, application_capabilities};
    use market_squawk::{AppPaths, PortfolioApplicationLimits, PortfolioApplicationService};
    use market_squawk_domain::{
        DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, MetadataRevision,
        SourceId, SourceIdentifier, Timestamp,
    };
    use market_squawk_services::{JsonStructureLimits, RequestContext, RequestId, ServiceLimits};
    use market_squawk_sources::{
        AvailabilityEvidence, DiscoveryRequest, ExtractionBatch, ExtractionRecord,
        ExtractionRequest, SourceObject,
    };
    use serde_json::{Value, json};
    use sha2::{Digest as _, Sha256};
    use tokio_util::sync::CancellationToken;

    type TestResult = Result<(), Box<dyn Error>>;

    #[tokio::test]
    async fn portfolio_import_atomically_publishes_the_queried_revision() -> TestResult {
        let temporary = tempfile::tempdir()?;
        let paths = AppPaths::prepare(temporary.path())?;
        let batch = account_and_holding_batch(1, 100, "50")?;
        let mut artifact = paths
            .artifacts()?
            .resolve("portfolio/import.json")?
            .create_new()?;
        serde_json::to_writer(&mut artifact, &batch)?;
        artifact.flush()?;

        let service =
            PortfolioApplicationService::try_new(&paths, PortfolioApplicationLimits::standard())?;
        let imported = service
            .call(
                admitted(
                    "Portfolio.Import",
                    json!({
                        "accountId": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                        "artifactId": "portfolio/import.json",
                        "resultLimits": {"maximumItems": 16, "maximumBytes": 65536},
                        "confirm": true
                    }),
                )?,
                context(1)?,
            )
            .await
            .map_err(|error| format!("first import: {error}"))?;
        let holdings = service
            .call(
                admitted(
                    "Portfolio.GetHoldings",
                    json!({
                        "accountId": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                        "resultLimits": {"maximumItems": 16, "maximumBytes": 65536}
                    }),
                )?,
                context(2)?,
            )
            .await
            .map_err(|error| format!("holdings: {error}"))?;

        assert_eq!(imported.structured_content()["state"], "saved");
        let holding = holdings
            .structured_content()
            .as_array()
            .and_then(|rows| rows.first())
            .ok_or("published holding is missing")?;
        let snapshot = holding["snapshotToken"]
            .as_str()
            .ok_or("snapshot is missing")?;
        uuid::Uuid::parse_str(snapshot)?;
        let accounts = service
            .call(
                admitted(
                    "Portfolio.ListAccounts",
                    json!({
                        "resultLimits": {"maximumItems": 16, "maximumBytes": 65536}
                    }),
                )?,
                context(3)?,
            )
            .await
            .map_err(|error| format!("accounts: {error}"))?;
        let token = accounts.structured_content()["accounts"][0]["accountToken"]
            .as_str()
            .ok_or("account token is missing")?;
        let arguments = json!({
            "accountToken": token,
            "instrumentIds": ["11111111-1111-4111-8111-111111111111"],
            "resultLimits": {"maximumItems": 16, "maximumBytes": 65536}
        });
        let performance = service
            .call(
                admitted("Portfolio.GetPerformance", arguments.clone())?,
                context(4)?,
            )
            .await
            .map_err(|error| format!("initial performance: {error}"))?;
        let capabilities = application_capabilities()?;
        performance.validate_for(
            capabilities
                .find("Portfolio.GetPerformance")
                .ok_or("performance descriptor missing")?,
        )?;
        let report = performance.structured_content();
        assert_eq!(report["snapshotToken"], snapshot);
        assert_eq!(
            report["currentValue"],
            json!({"amount": "1050", "currency": "USD"})
        );
        assert_eq!(
            report["accountingEvidence"]["cash"]["amount"],
            json!({"amount": "1000", "currency": "USD"})
        );
        assert_eq!(
            report["accountingEvidence"]["reportedMarketValue"]["amount"],
            "50"
        );
        assert_eq!(
            report["accountingEvidence"]["unrealizedGain"]["amount"]["amount"],
            "10"
        );
        assert_eq!(
            report["accountingEvidence"]["realizedGain"]["status"],
            "not_available"
        );
        assert_eq!(report["historyStatus"], "insufficient_history");
        assert!(report.get("timeWeightedReturn").is_none());
        assert!(
            admitted(
                "Portfolio.GetPerformance",
                json!({
                    "accountId": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                    "resultLimits": {"maximumItems": 16, "maximumBytes": 65536}
                })
            )
            .is_err()
        );
        let mut unknown_account = arguments.clone();
        unknown_account["accountToken"] = json!("portfolio_00000000000000000000000000000000");
        assert!(
            service
                .call(
                    admitted("Portfolio.GetPerformance", unknown_account)?,
                    context(5)?
                )
                .await
                .is_err()
        );
        let second_batch = account_and_holding_batch(2, 200, "100")?;
        let mut second_artifact = paths
            .artifacts()?
            .resolve("portfolio/update.json")?
            .create_new()?;
        serde_json::to_writer(&mut second_artifact, &second_batch)?;
        second_artifact.flush()?;
        service
            .call(
                admitted(
                    "Portfolio.Import",
                    json!({
                        "accountId": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                        "artifactId": "portfolio/update.json", "confirm": true,
                        "resultLimits": {"maximumItems": 16, "maximumBytes": 65536}
                    }),
                )?,
                context(6)?,
            )
            .await
            .map_err(|error| format!("second import: {error}"))?;
        let updated = service
            .call(
                admitted("Portfolio.GetPerformance", arguments.clone())?,
                context(7)?,
            )
            .await
            .map_err(|error| format!("updated performance: {error}"))?;
        updated.validate_for(
            capabilities
                .find("Portfolio.GetPerformance")
                .ok_or("performance descriptor missing")?,
        )?;
        let expected = updated.structured_content().clone();
        assert_eq!(expected["currentValue"]["amount"], "1100");
        assert_eq!(expected["periods"], 1);
        assert!(
            expected["timeWeightedReturn"]
                .as_str()
                .is_some_and(|rate| rate != "0")
        );
        assert_ne!(expected["snapshotToken"], snapshot);
        let mut closing_period = arguments.clone();
        closing_period["timeRange"] = json!({"start": "1970-01-01T00:00:00.000000150Z", "end": "1970-01-01T00:00:00.000000250Z"});
        let closing = service
            .call(
                admitted("Portfolio.GetPerformance", closing_period)?,
                context(10)?,
            )
            .await
            .map_err(|error| format!("closing period: {error}"))?;
        assert_eq!(closing.structured_content()["periods"], 1);
        assert_eq!(
            closing.structured_content()["timeWeightedReturn"],
            expected["timeWeightedReturn"]
        );
        let mut empty_period = arguments.clone();
        empty_period["timeRange"] = json!({"start": "1970-01-01T00:00:00.000000300Z", "end": "1970-01-01T00:00:00.000000400Z"});
        let outside = service
            .call(
                admitted("Portfolio.GetPerformance", empty_period)?,
                context(8)?,
            )
            .await
            .map_err(|error| format!("empty period: {error}"))?;
        assert_eq!(
            outside.structured_content()["historyStatus"],
            "insufficient_history"
        );
        assert!(
            outside
                .structured_content()
                .get("timeWeightedReturn")
                .is_none()
        );
        drop(service);
        let reopened =
            PortfolioApplicationService::try_new(&paths, PortfolioApplicationLimits::standard())?;
        let retained = reopened
            .call(
                admitted("Portfolio.GetPerformance", arguments)?,
                context(9)?,
            )
            .await
            .map_err(|error| format!("reopened performance: {error}"))?;
        retained.validate_for(
            capabilities
                .find("Portfolio.GetPerformance")
                .ok_or("performance descriptor missing")?,
        )?;
        assert_eq!(retained.structured_content(), &expected);
        Ok(())
    }

    fn admitted(
        operation: &str,
        arguments: Value,
    ) -> Result<market_squawk_services::TypedToolRequest, Box<dyn Error>> {
        let arguments = arguments
            .as_object()
            .cloned()
            .ok_or("arguments must be an object")?;
        Ok(application_capabilities()?
            .find(operation)
            .ok_or("operation is not registered")?
            .admit(arguments)?)
    }

    fn context(id: i64) -> Result<RequestContext, Box<dyn Error>> {
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

    fn account_and_holding_batch(
        revision: u64,
        at: i64,
        market_value: &str,
    ) -> Result<ExtractionBatch, Box<dyn Error>> {
        let source_id = SourceId::try_from("portfolio-control-test")?;
        let metadata_revision =
            MetadataRevision::new(SourceIdentifier::try_from("portfolio-control-v1")?);
        let dataset = SourceIdentifier::try_from("portfolio-records")?;
        let discovery = DiscoveryRequest::try_new(
            dataset,
            None,
            NonZeroU16::MIN,
            Timestamp::from_unix_nanos(1_000),
        )?;
        let record = |record_id: &str, value: Value| {
            serde_json::to_vec(&json!({
                "record_id": record_id, "revision_number": revision,
                "supersedes_revision": revision.checked_sub(1).filter(|previous| *previous > 0).map(|previous| format!("statement-{previous}")),
                "received_at_unix_nanos": (at + 3).to_string(),
                "ingested_at_unix_nanos": (at + 4).to_string(), "record": value,
            }))
        };
        let payloads = [
            record(
                "account",
                json!({"kind": "account", "account_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "currency": "USD", "cash_balance": "1000", "as_of_unix_nanos": at.to_string()}),
            )?,
            record(
                "holding",
                json!({"kind": "holding", "account_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "instrument_id": "11111111-1111-4111-8111-111111111111", "currency": "USD", "quantity": "2", "lot_size": "1", "market_value": market_value, "as_of_unix_nanos": at.to_string(), "cost_basis": {"status": "resolved", "amount": "40", "lot_method": "fifo"}}),
            )?,
        ];
        let object_bytes = payloads.concat();
        let object_evidence = exact_evidence(&object_bytes);
        let object = SourceObject::try_new_with_availability(
            source_id,
            metadata_revision,
            &discovery,
            SourceIdentifier::try_from(format!("portfolio-control-object-{revision}"))?,
            SourceIdentifier::try_from("application-market-squawk-portfolio-records-json")?,
            object_evidence,
            EffectiveInterval::new(Timestamp::from_unix_nanos(at), None)?,
            Some(Timestamp::from_unix_nanos(at + 1)),
            AvailabilityEvidence::Observed {
                available_at: Timestamp::from_unix_nanos(at + 2),
                evidence: SourceIdentifier::try_from("local-file-first-observed")?,
            },
            Some(u64::try_from(object_bytes.len())?),
        )?;
        let request = ExtractionRequest::try_new(
            object,
            NonZeroU32::new(16).ok_or("record bound is zero")?,
            NonZeroU64::new(64 * 1024).ok_or("byte bound is zero")?,
            Timestamp::from_unix_nanos(1_000),
        )?;
        let records = payloads
            .into_iter()
            .map(|payload| {
                let payload = Bytes::from(payload);
                ExtractionRecord::try_new(
                    &request,
                    SourceIdentifier::try_from("market-squawk-portfolio-raw-v1")?,
                    exact_evidence(&payload),
                    Timestamp::from_unix_nanos(at),
                    Some(Timestamp::from_unix_nanos(at + 1)),
                    AvailabilityEvidence::Observed {
                        available_at: Timestamp::from_unix_nanos(at + 2),
                        evidence: SourceIdentifier::try_from("local-file-first-observed")?,
                    },
                    SourceIdentifier::try_from(format!("statement-{revision}"))?,
                    None,
                    payload,
                )
                .map_err(Into::into)
            })
            .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
        Ok(ExtractionBatch::try_new(&request, records)?)
    }

    fn exact_evidence(bytes: &[u8]) -> ExactPayloadEvidence {
        ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            Sha256::digest(bytes).into(),
        ))
    }
}
