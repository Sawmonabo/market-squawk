//! Exact successful partition outputs in the existing immutable decision journal.
use super::*;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CurrentFindCompletionReference {
    pub(crate) ordinal: usize,
    pub(crate) completion_sha256: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CurrentFindCompletionRecord {
    pub(super) preparation_id: Uuid,
    pub(super) preparation_sha256: [u8; 32],
    pub(super) ordinal: usize,
    pub(super) partition_sha256: [u8; 32],
    pub(super) previous_sha256: Option<[u8; 32]>,
    pub(crate) dataset_job: Option<CurrentFindDatasetJob>,
    pub(crate) dataset_content_sha256: Option<[u8; 32]>,
}
impl CurrentFindCompletionRecord {
    pub(super) fn validate(&self) -> Result<(), DecisionApplicationError> {
        if self.preparation_id.is_nil() || self.preparation_sha256 == [0; 32]
            || self.partition_sha256 == [0; 32] || self.ordinal >= MAXIMUM_CURRENT_FIND_PARTITIONS
            || self.previous_sha256.is_none() != (self.ordinal == 0)
            || self.previous_sha256 == Some([0; 32])
            || self.dataset_job.is_some() != self.dataset_content_sha256.is_some()
            || self.dataset_content_sha256 == Some([0; 32])
            || self.dataset_job.as_ref().is_some_and(|job| job.ordinal != self.ordinal || job.job_id.is_nil() || job.generation == 0)
        { return Err(invalid()); }
        Ok(())
    }
    pub(super) fn digest(&self) -> Result<[u8; 32], DecisionApplicationError> { digest(self) }
    pub(crate) fn reference(&self) -> Result<CurrentFindCompletionReference, DecisionApplicationError> {
        Ok(CurrentFindCompletionReference { ordinal: self.ordinal,
            completion_sha256: self.digest()?.iter().map(|byte| format!("{byte:02x}")).collect() })
    }
}
impl DecisionApplication {
    /// Read exactly one committed output. Journal replay authenticated its preceding prefix.
    pub(crate) fn current_find_completion(
        &self, parent: &CurrentFindPreparationRecord, ordinal: usize,
    ) -> Result<Option<CurrentFindCompletionRecord>, DecisionApplicationError> {
        if ordinal >= parent.partition_count { return Err(invalid()); }
        let state = self.reader()?;
        match state.journal.current_find_record(&key(parent.preparation_id, ordinal))? {
            None => Ok(None),
            Some(CurrentFindCustodyRecord::Completion(value)) => {
                let Some(CurrentFindCustodyRecord::Partition(partition)) = state.journal.current_find_record(&partition_key(parent.preparation_id, ordinal))?
                    else { return Err(invalid()); };
                validate_partition_parent(&partition, parent, parent.digest()?)?;
                validate_join(&value, &partition)?;
                Ok(Some(*value))
            }
            Some(_) => Err(invalid()),
        }
    }

    /// Caller has reopened the actual unique build output and completed job against the original
    /// partition. Retention checks every structural join and commits its original output digest.
    pub(crate) fn retain_current_find_completion(
        &self, parent: &CurrentFindPreparationRecord, partition: &RetainedCurrentFindPartition,
        dataset_job: Option<CurrentFindDatasetJob>, dataset_content_sha256: Option<[u8; 32]>,
        context: &RequestContext,
    ) -> Result<CurrentFindCompletionRecord, DecisionApplicationError> {
        let ordinal = partition.ordinal();
        let parent_digest = parent.digest()?;
        validate_partition_parent(&partition.record, parent, parent_digest)?;
        let state = self.writer()?;
        let Some(CurrentFindCustodyRecord::Partition(actual)) = state.journal.current_find_record(&partition_key(parent.preparation_id, ordinal))?
            else { return Err(invalid()); };
        if *actual != partition.record { return Err(invalid()); }
        let previous_sha256 = if ordinal == 0 { None } else {
            let Some(CurrentFindCustodyRecord::Completion(previous)) = state.journal.current_find_record(&key(parent.preparation_id, ordinal - 1))?
                else { return Err(invalid()); };
            if previous.preparation_sha256 != parent_digest { return Err(invalid()); }
            Some(previous.digest()?)
        };
        let record = CurrentFindCompletionRecord { preparation_id: parent.preparation_id,
            preparation_sha256: parent_digest, ordinal, partition_sha256: digest(&partition.record)?,
            previous_sha256, dataset_job, dataset_content_sha256 };
        validate_join(&record, &partition.record)?;
        if let Some(existing) = state.journal.current_find_record(&key(parent.preparation_id, ordinal))? {
            return match existing {
                CurrentFindCustodyRecord::Completion(existing) if *existing == record => Ok(record),
                _ => Err(invalid()),
            };
        }
        let custody = CurrentFindCustodyRecord::Completion(Box::new(record.clone()));
        custody.validate()?;
        let encoded = super::super::codec::current_find_custody(&custody)?;
        check_control(context)?;
        state.journal.append(&encoded)?;
        Ok(record)
    }
}
pub(super) fn validate_join(value: &CurrentFindCompletionRecord, partition: &CurrentFindPartitionRecord) -> Result<(), DecisionApplicationError> {
    value.validate()?;
    if value.preparation_id != partition.preparation_id
        || value.preparation_sha256 != partition.preparation_sha256 || value.ordinal != partition.ordinal
        || value.partition_sha256 != digest(partition)?
        || value.dataset_job.is_some() != partition.evidence.expected_build_spec().is_some()
    { return Err(invalid()); }
    Ok(())
}
pub(super) fn key(id: Uuid, ordinal: usize) -> String { format!("current-find:completion:{id}:{ordinal}") }
