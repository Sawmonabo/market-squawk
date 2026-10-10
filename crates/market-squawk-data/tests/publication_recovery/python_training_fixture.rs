//! Verification-only export through the same sealed producer exercised by the parent test.
//! This is compiled only into the existing integration-test executable, never the product.

use super::*;
use std::{fs::OpenOptions, io::Write, path::Path};

pub(super) const EXAMPLES: usize = 18;

pub(super) fn decision_nanos(index: usize) -> Result<i64, Box<dyn Error>> {
    if index >= EXAMPLES {
        return Err("fixture example limit".into());
    }
    // Preserve the first example's PIT boundaries and finish before the existing receipt.
    Ok(if index == 0 {
        100
    } else {
        175 + 25 * i64::try_from(index)?
    })
}

pub(super) fn prices(index: usize) -> Result<[i64; 3], Box<dyn Error>> {
    decision_nanos(index)?;
    let index = i64::try_from(index)?;
    let forward = 5 + index * index;
    let previous = 1_000_000_i64;
    let current = previous + index * 10_000;
    let terminal = current
        .checked_mul(1_000 + forward)
        .ok_or("fixture price overflow")?
        / 1_000;
    Ok([previous, current, terminal])
}

pub(super) fn macro_input(index: usize, position: u8) -> Result<(i64, Decimal), Box<dyn Error>> {
    let pulse_index = usize::from(position) + 1;
    let effective = if index < pulse_index {
        90
    } else {
        decision_nanos(if index == pulse_index {
            pulse_index
        } else {
            pulse_index + 1
        })? - 1
    };
    // Independent observed pulses avoid zero scales and a singular thirteen-feature fit.
    let value =
        Decimal::new(i64::from(position) + 1, 2) + Decimal::new(i64::from(index == pulse_index), 3);
    Ok((effective, value))
}

pub(super) fn macro_batch_with_membership() -> Result<ExtractionBatch, Box<dyn Error>> {
    let template = extraction_batch_with_membership_until(true, Timestamp::from_unix_nanos(700))?;
    let request = ExtractionRequest::try_new(
        template.request().object().clone(),
        NonZeroU32::new(u32::try_from(
            feature_dataset_macro_components_v1().len() * 3 + 1,
        )?)
        .ok_or("nonzero macro fixture record limit")?,
        NonZeroU64::new(1024 * 1024).ok_or("nonzero macro fixture byte limit")?,
        template.request().deadline(),
    )?;
    let mut records = Vec::with_capacity(feature_dataset_macro_components_v1().len() * 3 + 1);
    for descriptor in feature_dataset_macro_components_v1() {
        let pulse_index = usize::from(descriptor.position()) + 1;
        for index in [0, pulse_index, pulse_index + 1] {
            let (effective, value) = macro_input(index, descriptor.position())?;
            let effective = Timestamp::from_unix_nanos(effective);
            let source_identifier =
                format!("{}:{}", descriptor.indicator_id(), effective.unix_nanos());
            let context = ResearchContext::new(
                ResearchProvenance::try_new(ResearchProvenanceInput {
                    source_id: SourceId::try_from("fred-local-fixture")?,
                    instrument_id: None,
                    venue_id: None,
                    source_identifier: SourceIdentifier::try_from(source_identifier.as_str())?,
                    source_timestamp: None,
                    received_at: Timestamp::from_unix_nanos(610),
                    ingested_at: Timestamp::from_unix_nanos(610),
                    quality: DataQuality::OfficialDelayed,
                    payload_reference: PayloadReference::SourceReference(
                        SourceIdentifier::try_from("fixture-original-macro-publication")?,
                    ),
                    availability: DomainAvailabilityEvidence::evidenced(
                        effective,
                        SourceIdentifier::try_from("fixture-original-macro-publication")?,
                    ),
                })?,
                ResearchTime::new(effective, Some(effective), RevisionNumber::new(1)?, None)?,
            )?;
            let observation = ResearchObservation::Macro(MacroObservation::new(
                context,
                SourceIdentifier::try_from(descriptor.indicator_id())?,
                value,
                SourceIdentifier::try_from(descriptor.unit())?,
            ));
            records.push(source_record(
                &request,
                &observation,
                effective,
                "fixture-original-macro-publication",
            )?);
        }
    }
    records.push(source_record(
        &request,
        &universe_membership_observation()?,
        Timestamp::from_unix_nanos(1),
        "constituent-publication",
    )?);
    Ok(ExtractionBatch::try_new(&request, records)?)
}

fn source_record(
    request: &ExtractionRequest,
    observation: &ResearchObservation,
    effective: Timestamp,
    availability_evidence: &str,
) -> Result<ExtractionRecord, Box<dyn Error>> {
    let payload = serde_json::to_vec(observation)?;
    Ok(ExtractionRecord::try_new(
        request,
        SourceIdentifier::try_from("market-squawk-research-v3")?,
        ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            Sha256::digest(&payload).into(),
        )),
        effective,
        Some(effective),
        SourceAvailabilityEvidence::Observed {
            available_at: effective,
            evidence: SourceIdentifier::try_from(availability_evidence)?,
        },
        SourceIdentifier::try_from("revision-1")?,
        None,
        payload.into(),
    )?)
}

pub(super) async fn publish(root: &Path) -> TestResult {
    // An explicitly requested empty test root is the entire write capability. Existing user or
    // release state is never opened for fixture mutation, restored over, or copied by SQL.
    if !root.is_absolute()
        || !std::fs::symlink_metadata(root)?.is_dir()
        || std::fs::read_dir(root)?.next().is_some()
    {
        return Err("dataset fixture requires an empty absolute directory".into());
    }
    let paths = LocalPaths::prepare(root)?;
    let mut values = Vec::with_capacity(EXAMPLES * 3);
    for index in 0..EXAMPLES {
        let at = decision_nanos(index)?;
        let [previous, current, terminal] = prices(index)?;
        values.extend([(at - 20, previous), (at - 10, current), (at, terminal)]);
    }
    let market = closed_price_return_market_bar_fixture_for_values(
        &values,
        Timestamp::from_unix_nanos(610),
    )?;
    let (service, publisher, membership, market_bars) = initialized_service_with_universe_fixture(
        &paths,
        test_catalog_config(paths.catalog()?.clone())?,
        ObjectStoreConfig::try_new(8 * 1024 * 1024, 1024, Duration::from_secs(60))?,
        market,
        true,
        None,
    )
    .await?;
    let research_limits = ResearchUseLimits::try_new(
        8,
        32,
        32,
        8,
        1024 * 1024,
        Duration::from_secs(2),
        Duration::from_secs(30),
    )?;
    let request = closed_price_return_request_for_fixture(
        membership.manifest().clone(),
        market_bars.manifest().clone(),
        dataset_membership_instrument()?,
        research_limits,
        false,
        false,
        true,
    )?;
    let cancellation = CancellationToken::new();
    let dataset = service
        .dataset_builder()
        .build(request.clone(), cancellation.clone())
        .await
        .inspect_err(|_| eprintln!("dataset fixture stage=build"))?;
    let contract =
        FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonForwardReturnTrainingV1;
    let wall = Timestamp::from_unix_nanos(i64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
    )?);
    let publication = publisher
        .publish(
            &service,
            contract,
            &request,
            &dataset,
            closed_price_return_proof(
                &request,
                membership.manifest().clone(),
                wall.checked_sub_nanos(1_000_000_000)?,
                wall.checked_add_nanos(120_000_000_000)?,
                96,
            )?,
            &cancellation,
        )
        .inspect_err(|_| eprintln!("dataset fixture stage=publish"))?;
    assert_eq!(
        publication.disposition(),
        FeatureDatasetProductionPublicationDisposition::Published
    );
    assert_eq!(dataset.split_counts().train_examples(), 14);
    assert_eq!(dataset.split_counts().validation_examples(), 2);
    assert_eq!(dataset.split_counts().test_examples(), 2);
    let export = dataset.python_export()?;
    // Read through the exact same production admission/receipt/row verifier used by Python.
    // The descriptor alone is not evidence that training admission succeeded.
    market_squawk_data::verify_python_dataset(
        root,
        export.content_hash(),
        contract,
        Timestamp::from_unix_nanos(700),
        market_squawk_data::PythonDatasetVerificationLimits::try_new(256, 64 * 1024 * 1024)?,
        Instant::now() + Duration::from_secs(30),
        &cancellation,
    )
    .inspect_err(|_| eprintln!("dataset fixture stage=verify"))?;
    write_new(&root.join("fixture-export.json"), export.bytes())?;
    write_new(
        &root.join("fixture-receipt.json"),
        publication.receipt().canonical_json(),
    )?;
    Ok(())
}

fn write_new(path: &Path, bytes: &[u8]) -> TestResult {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}
