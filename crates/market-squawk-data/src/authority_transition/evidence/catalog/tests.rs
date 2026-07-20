use market_squawk_domain::{SourceIdentifier, Timestamp};
use uuid::Uuid;

use super::{
    ArtifactEvidenceRow, CatalogEvidenceSnapshot, EvidenceError, EvidenceLimits,
    EvidenceSnapshotRequest, GenerationEvidenceRow, GenerationObjectEvidenceRow,
    ManifestEvidenceRow, QueryArtifactEvidenceRow,
};
use crate::{DatasetId, GenerationKind, ManifestObject, ManifestPlan, Sha256Digest};

#[test]
fn snapshot_rejects_generation_whose_lineage_differs_from_ordered_objects()
-> Result<(), Box<dyn std::error::Error>> {
    let dataset = DatasetId::try_from("prices.daily")?;
    let artifact_id = Uuid::new_v4();
    let manifest_id = Uuid::new_v4();
    let object = ManifestObject::try_new(
        Sha256Digest::new([3; 32]),
        7,
        512,
        Sha256Digest::new([5; 32]),
    )?;
    let plan = ManifestPlan::append(dataset.clone(), None, object.clone(), 8)?;
    let artifact = ArtifactEvidenceRow::try_new(
        artifact_id,
        Uuid::new_v4(),
        "objects/sha256/03/0303030303030303030303030303030303030303030303030303030303030303.parquet",
        object.content_hash(),
        object.size_bytes(),
    )?;
    let manifest = ManifestEvidenceRow::try_new(
        manifest_id,
        dataset.clone(),
        1,
        artifact_id,
        plan.content_hash(),
    )?;
    let generation_object = GenerationObjectEvidenceRow::try_new(
        artifact_id,
        object.content_hash(),
        object.row_count(),
        object.size_bytes(),
        object.lineage_digest(),
    )?;
    let generation = GenerationEvidenceRow::try_new(
        dataset,
        1,
        plan.content_hash(),
        Sha256Digest::new([99; 32]),
        plan.row_count(),
        plan.total_bytes(),
        1,
        manifest_id,
        None,
        GenerationKind::Ingest,
        vec![generation_object],
    )?;
    let limits = EvidenceLimits::try_new(16, 64, 1 << 20, 1 << 20, 64 << 10)?;
    let request = EvidenceSnapshotRequest::new(Timestamp::from_unix_nanos(123), limits);

    let result = CatalogEvidenceSnapshot::try_new(
        request,
        vec![artifact],
        vec![manifest],
        vec![generation],
        Vec::new(),
    );

    assert!(matches!(
        result,
        Err(EvidenceError::GenerationSemanticMismatch)
    ));
    Ok(())
}

#[test]
fn snapshot_accepts_query_result_as_an_independent_physical_artifact()
-> Result<(), Box<dyn std::error::Error>> {
    let query = QueryArtifactEvidenceRow::try_new(
        Uuid::new_v4(),
        SourceIdentifier::try_from("local-research")?,
        Sha256Digest::new([21; 32]),
        Uuid::new_v4(),
        "objects/sha256/17/1717171717171717171717171717171717171717171717171717171717171717.parquet",
        Sha256Digest::new([23; 32]),
        4_096,
        Timestamp::from_unix_nanos(2_000),
    )?;
    let limits = EvidenceLimits::try_new(16, 64, 1 << 20, 1 << 20, 64 << 10)?;
    let request = EvidenceSnapshotRequest::new(Timestamp::from_unix_nanos(1_000), limits);

    let snapshot =
        CatalogEvidenceSnapshot::try_new(request, Vec::new(), Vec::new(), Vec::new(), vec![query])?;

    assert_eq!(snapshot.physical_artifact_count(), 1);
    Ok(())
}
