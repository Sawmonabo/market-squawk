use std::error::Error;

use market_squawk_data::{
    DatasetId, ManifestObject, ManifestPlan, ManifestPlanError, Sha256Digest,
};

type TestResult = Result<(), Box<dyn Error>>;

#[test]
fn compaction_replaces_objects_without_changing_rows_or_lineage() -> TestResult {
    let dataset = DatasetId::try_from("fred-gdp")?;
    let first = ManifestObject::try_new(
        Sha256Digest::new([1; 32]),
        2,
        100,
        Sha256Digest::new([9; 32]),
    )?;
    let second = ManifestObject::try_new(
        Sha256Digest::new([2; 32]),
        3,
        120,
        Sha256Digest::new([8; 32]),
    )?;
    let appended = ManifestPlan::append(dataset.clone(), None, vec![first], 2)?;
    let appended = ManifestPlan::append(dataset, Some(&appended), vec![second], 2)?;
    let compacted_object = ManifestObject::try_new(
        Sha256Digest::new([3; 32]),
        5,
        150,
        appended.lineage_digest(),
    )?;
    let compacted = ManifestPlan::compact(&appended, compacted_object)?;
    assert_eq!(compacted.row_count(), appended.row_count());
    assert_eq!(compacted.lineage_digest(), appended.lineage_digest());
    assert_eq!(compacted.objects().len(), 1);
    assert_eq!(appended.objects().len(), 2);
    Ok(())
}

#[test]
fn appending_beyond_the_small_file_ceiling_requires_compaction() -> TestResult {
    let dataset = DatasetId::try_from("fred-gdp")?;
    let first = ManifestObject::try_new(
        Sha256Digest::new([1; 32]),
        1,
        10,
        Sha256Digest::new([9; 32]),
    )?;
    let second = ManifestObject::try_new(
        Sha256Digest::new([2; 32]),
        1,
        10,
        Sha256Digest::new([8; 32]),
    )?;
    let plan = ManifestPlan::append(dataset, None, vec![first], 1)?;
    assert!(matches!(
        ManifestPlan::append(plan.dataset_id().clone(), Some(&plan), vec![second], 1),
        Err(ManifestPlanError::SmallFileCeiling { max_objects: 1 })
    ));
    Ok(())
}
