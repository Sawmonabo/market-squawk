use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    ArrayRef, Decimal128Array, FixedSizeBinaryArray, Float64Array, TimestampNanosecondArray,
    UInt8Array, UInt32Array, builder::FixedSizeBinaryBuilder,
};
use arrow::record_batch::RecordBatch;
use market_squawk_data::{
    AnalyticalManifestCatalog, CatalogAuthority, CatalogConfig, CatalogLimit, CatalogResultLimits,
    DatasetArrowBatch, DatasetId, DatasetManifestRef, DatasetSchemaRef, DatasetSchemaRegistry,
    FeatureLabelBatchBindings, ManifestCatalogError, ManifestObject, ManifestPlan, QueryLimits,
    QueryRequest, ResearchArrowBatch, Sha256Digest,
};
use market_squawk_domain::{
    AvailabilityEvidence, DataQuality, DigestAlgorithm, EvidenceDigest, MacroObservation,
    PayloadReference, ResearchContext, ResearchObservation, ResearchProvenance,
    ResearchProvenanceInput, ResearchTime, RevisionNumber, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_platform::LocalPaths;
use rusqlite::{Connection, params};
use rust_decimal::Decimal;

type TestResult = Result<(), Box<dyn Error>>;

fn fixed_text(value: Option<&str>, width: i32) -> Result<FixedSizeBinaryArray, Box<dyn Error>> {
    let mut builder = FixedSizeBinaryBuilder::new(width);
    match value {
        Some(value) => {
            let mut padded = vec![0_u8; usize::try_from(width)?];
            padded[..value.len()].copy_from_slice(value.as_bytes());
            builder.append_value(padded)?;
        }
        None => builder.append_null(),
    }
    Ok(builder.finish())
}

#[test]
fn schema_identity_is_causal_across_publication_pinning_and_restart() -> TestResult {
    let registry = DatasetSchemaRegistry::local();
    let research = registry.canonical_research_observations()?;
    let feature_labels = registry.canonical_feature_labels()?;
    assert_ne!(research, feature_labels);
    let feature_schema = registry.bind_feature_labels(
        &feature_labels,
        &FeatureLabelBatchBindings::new("feature-examples".try_into()?, [3; 32], [4; 32], [5; 32]),
    )?;
    assert!(feature_schema.field_with_name("component_name").is_ok());
    assert!(
        feature_schema
            .field_with_name("value_decimal_mantissa")
            .is_ok()
    );
    assert!(feature_schema.field_with_name("payload_json").is_err());

    let decimal = Decimal128Array::from(vec![None::<i128>]).with_precision_and_scale(38, 0)?;
    let feature_batch = RecordBatch::try_new(
        Arc::clone(&feature_schema),
        vec![
            Arc::new(fixed_text(Some("example-1"), 256)?) as ArrayRef,
            Arc::new(FixedSizeBinaryArray::try_from_iter(
                [
                    uuid::Uuid::parse_str("018fb5b0-6da1-7d66-9c7a-0f57c5f94ca1")?
                        .into_bytes()
                        .to_vec(),
                ]
                .into_iter(),
            )?) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(vec![1_i64]).with_timezone_utc()) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(vec![None::<i64>]).with_timezone_utc())
                as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(vec![None::<i64>]).with_timezone_utc())
                as ArrayRef,
            Arc::new(UInt8Array::from(vec![2_u8])) as ArrayRef,
            Arc::new(UInt8Array::from(vec![1_u8])) as ArrayRef,
            Arc::new(UInt8Array::from(vec![1_u8])) as ArrayRef,
            Arc::new(fixed_text(Some("return-1d"), 256)?) as ArrayRef,
            Arc::new(UInt32Array::from(vec![1_u32])) as ArrayRef,
            Arc::new(Float64Array::from(vec![Some(0.25)])) as ArrayRef,
            Arc::new(decimal) as ArrayRef,
            Arc::new(UInt8Array::from(vec![None::<u8>])) as ArrayRef,
            Arc::new(fixed_text(Some("return"), 32)?) as ArrayRef,
            Arc::new(fixed_text(None, 3)?) as ArrayRef,
            Arc::new(fixed_text(None, 256)?) as ArrayRef,
            Arc::new(FixedSizeBinaryArray::try_from_iter(
                [[9_u8; 32].as_slice()].into_iter(),
            )?) as ArrayRef,
        ],
    )?;
    let feature_batch = DatasetArrowBatch::try_new(feature_labels.clone(), feature_batch)?;
    assert_eq!(feature_batch.schema_ref(), &feature_labels);
    assert!(
        ResearchArrowBatch::try_from_record_batch(feature_batch.record_batch().clone()).is_err()
    );
    let mut extra_metadata = feature_schema.metadata().clone();
    extra_metadata.insert("unbound.semantic".to_owned(), "hostile".to_owned());
    let hostile_schema = feature_schema
        .as_ref()
        .clone()
        .with_metadata(extra_metadata)
        .into();
    let hostile = RecordBatch::try_new(
        hostile_schema,
        feature_batch.record_batch().columns().to_vec(),
    )?;
    assert!(DatasetArrowBatch::try_new(feature_labels.clone(), hostile).is_err());

    let research_context = ResearchContext::new(
        ResearchProvenance::try_new(ResearchProvenanceInput {
            source_id: SourceId::try_from("schema-contract-fixture")?,
            instrument_id: None,
            venue_id: None,
            source_identifier: SourceIdentifier::try_from("GDP:2026Q1:v1")?,
            source_timestamp: None,
            received_at: Timestamp::from_unix_nanos(110),
            ingested_at: Timestamp::from_unix_nanos(120),
            quality: DataQuality::OfficialDelayed,
            payload_reference: PayloadReference::SourceReference(SourceIdentifier::try_from(
                "schema-contract:gdp:2026q1",
            )?),
            availability: AvailabilityEvidence::evidenced(
                Timestamp::from_unix_nanos(100),
                SourceIdentifier::try_from("schema-contract-release")?,
            ),
        })?,
        ResearchTime::new(
            Timestamp::from_unix_nanos(90),
            Some(Timestamp::from_unix_nanos(100)),
            RevisionNumber::new(1)?,
            None,
        )?,
    )?;
    let current_research = ResearchArrowBatch::try_from_observations(
        SourceIdentifier::try_from("schema-contract-dataset")?,
        EvidenceDigest::new(DigestAlgorithm::Sha256, [11; 32]),
        vec![ResearchObservation::Macro(MacroObservation::new(
            research_context,
            SourceIdentifier::try_from("GDP")?,
            Decimal::new(25, 1),
            SourceIdentifier::try_from("USD")?,
        ))],
    )?;
    let current_contract_is_accepted =
        DatasetArrowBatch::try_new(research.clone(), current_research.record_batch().clone())
            .is_ok();
    let mut mismatched_contracts_are_rejected = true;
    for replacement in [
        None,
        Some("0000000000000000000000000000000000000000000000000000000000000000"),
    ] {
        let mut metadata = current_research.record_batch().schema().metadata().clone();
        match replacement {
            Some(identity) => {
                metadata.insert(
                    "market_squawk.research_payload_contract_sha256".to_owned(),
                    identity.to_owned(),
                );
            }
            None => {
                metadata.remove("market_squawk.research_payload_contract_sha256");
            }
        }
        let altered_schema = Arc::new(
            current_research
                .record_batch()
                .schema()
                .as_ref()
                .clone()
                .with_metadata(metadata),
        );
        let altered_batch = RecordBatch::try_new(
            altered_schema,
            current_research.record_batch().columns().to_vec(),
        )?;
        mismatched_contracts_are_rejected &=
            DatasetArrowBatch::try_new(research.clone(), altered_batch).is_err();
    }
    assert!(current_contract_is_accepted && mismatched_contracts_are_rejected);

    let mut tampered_fingerprint = research.fingerprint();
    tampered_fingerprint[0] ^= 0xff;
    let tampered =
        DatasetSchemaRef::try_new(research.name(), research.version(), tampered_fingerprint)?;
    assert!(registry.resolve(&tampered).is_err());

    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("market-squawk"))?;
    let location = paths.catalog()?.clone();
    drop(CatalogAuthority::open(CatalogConfig::try_new(
        location.clone(),
        Duration::from_millis(750),
        CatalogLimit::new(32)?,
        CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
    )?)?);
    let dataset = DatasetId::try_from("schema-bound")?;
    let object =
        ManifestObject::try_new(Sha256Digest::new([1; 32]), 1, 1, Sha256Digest::new([2; 32]))?;
    let plan = ManifestPlan::append(dataset.clone(), None, vec![object.clone()], 8)?;
    let artifact_id = uuid::Uuid::new_v4();
    let connection = Connection::open(location.path())?;
    connection.pragma_update(None, "foreign_keys", false)?;
    connection.execute(
        "INSERT INTO analytical_generations
         (dataset_id, manifest_version, content_hash, lineage_hash, row_count, total_bytes,
          schema_name, schema_version, schema_fingerprint, anchor_manifest_id, generation_kind,
          parent_count, build_spec_digest, created_at_ns)
         VALUES (?1, 1, ?2, ?3, 1, 1, ?4, ?5, ?6, ?7, 'ingest', 0, NULL, 1)",
        params![
            dataset.as_str(),
            plan.content_hash().bytes().as_slice(),
            plan.lineage_digest().bytes().as_slice(),
            research.name(),
            i64::from(research.version().get()),
            research.fingerprint().as_slice(),
            uuid::Uuid::new_v4().to_string(),
        ],
    )?;
    connection.execute(
        "INSERT INTO analytical_generation_objects
         (dataset_id, manifest_version, ordinal, artifact_id, content_hash, row_count,
          size_bytes, lineage_hash)
         VALUES (?1, 1, 0, ?2, ?3, 1, 1, ?4)",
        params![
            dataset.as_str(),
            artifact_id.to_string(),
            object.content_hash().bytes().as_slice(),
            object.lineage_digest().bytes().as_slice(),
        ],
    )?;
    assert!(
        connection
            .execute(
                "INSERT INTO analytical_generations
                 (dataset_id, manifest_version, content_hash, lineage_hash, row_count, total_bytes,
                  schema_version, anchor_manifest_id, generation_kind, parent_count,
                  build_spec_digest, created_at_ns)
                 VALUES ('unknown-schema', 1, zeroblob(32), zeroblob(32), 1, 1, 2, ?1,
                         'ingest', 0, NULL, 1)",
                [uuid::Uuid::new_v4().to_string()],
            )
            .is_err()
    );
    drop(connection);

    let expected =
        DatasetManifestRef::try_new_with_schema(dataset, 1, research.clone(), plan.content_hash())?;
    let catalog = AnalyticalManifestCatalog::open(&location, 8)?;
    assert_eq!(
        catalog.latest(expected.dataset_id())?,
        Some(expected.clone())
    );
    let tampered_manifest = DatasetManifestRef::try_new_with_schema(
        expected.dataset_id().clone(),
        expected.manifest_version(),
        tampered.clone(),
        expected.content_hash(),
    )?;
    assert!(matches!(
        catalog.pinned(&tampered_manifest),
        Err(ManifestCatalogError::SchemaMismatch)
    ));
    let limits = QueryLimits::try_new(1, 4096, 8 * 1024 * 1024, 1, 64, 64, Duration::from_secs(1))?;
    let feature_manifest = DatasetManifestRef::try_new_with_schema(
        expected.dataset_id().clone(),
        expected.manifest_version(),
        feature_labels,
        expected.content_hash(),
    )?;
    assert_ne!(
        QueryRequest::try_new(expected.clone(), "SELECT 1")?.artifact_identity(&limits),
        QueryRequest::try_new(feature_manifest, "SELECT 1")?.artifact_identity(&limits)
    );
    assert!(QueryRequest::try_new(tampered_manifest, "SELECT 1").is_err());
    drop(catalog);
    let restarted = AnalyticalManifestCatalog::open(&location, 8)?;
    assert_eq!(restarted.latest(expected.dataset_id())?, Some(expected));
    Ok(())
}
