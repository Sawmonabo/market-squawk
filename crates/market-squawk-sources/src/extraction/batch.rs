//! Deep-retained bounded extraction batches and hostile-wire validation.

use serde::de::{IgnoredAny, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;
use std::mem::size_of;

use super::contracts::{
    ExtractionError, ExtractionRecord, ExtractionRequest, MAX_EXTRACTION_RECORDS,
    MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES,
};

const ALLOCATOR_CAPACITY_ALLOWANCE_FACTOR: usize = 2;

/// Intrinsically bounded normalized extraction output retaining its exact request.
#[derive(Clone, Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExtractionBatch {
    request: ExtractionRequest,
    #[serde(rename = "total_retained_bytes")]
    logical_retained_bytes: u64,
    records: AggregateBoundedRecords,
}

/// Incremental construction boundary for one request-bound extraction batch.
///
/// Every pushed record is lineage-checked and charged against the request and process-global
/// deep-retained ceilings before the next record is accepted.
#[derive(Debug)]
pub struct ExtractionBatchAccumulator {
    request: ExtractionRequest,
    retained_record_bytes: u64,
    records: Vec<ExtractionRecord>,
}

impl ExtractionBatchAccumulator {
    /// Starts one incrementally bounded batch.
    ///
    /// # Errors
    ///
    /// Rejects a request whose retained lineage already exceeds its byte ceiling.
    pub fn try_new(request: &ExtractionRequest) -> Result<Self, ExtractionError> {
        enforce_byte_limit(request, batch_base_bytes(request)?)?;
        Ok(Self {
            request: request.clone(),
            retained_record_bytes: 0,
            records: Vec::new(),
        })
    }

    /// Adds one record after enforcing count, lineage, allocation, and deep-byte bounds.
    ///
    /// # Errors
    ///
    /// Rejects record-count overflow, lineage transplants, allocation failure, and byte overflow.
    pub fn push(&mut self, record: ExtractionRecord) -> Result<(), ExtractionError> {
        if self.records.len() >= self.request.max_records() as usize {
            return Err(ExtractionError::RecordLimitExceeded {
                requested: self.request.max_records(),
            });
        }
        if !record.matches_request(&self.request) {
            return Err(ExtractionError::ObjectBindingMismatch);
        }
        let record_bytes = record.retained_bytes()?;
        let retained_record_bytes = self
            .retained_record_bytes
            .checked_add(record_bytes)
            .ok_or(ExtractionError::ByteCountOverflow)?;
        let fixed_retained_bytes = batch_base_bytes(&self.request)?
            .checked_add(retained_record_bytes)
            .ok_or(ExtractionError::ByteCountOverflow)?;
        let prospective_len = self
            .records
            .len()
            .checked_add(1)
            .ok_or(ExtractionError::ByteCountOverflow)?;
        reserve_record_capacity(
            &mut self.records,
            prospective_len,
            fixed_retained_bytes,
            request_byte_limit(&self.request),
        )?;
        self.records.push(record);
        self.retained_record_bytes = retained_record_bytes;
        Ok(())
    }

    /// Finalizes the batch by moving the already-bounded record vector without copying it.
    ///
    /// # Errors
    ///
    /// Returns a byte-overflow error if the final checked retained-byte sum cannot be represented.
    pub fn finish(self) -> Result<ExtractionBatch, ExtractionError> {
        let records = AggregateBoundedRecords::try_new(self.records)?;
        let logical_retained_bytes = batch_base_bytes(&self.request)?
            .checked_add(records.logical_retained_bytes()?)
            .ok_or(ExtractionError::ByteCountOverflow)?;
        let total_retained_bytes = logical_retained_bytes
            .checked_add(records.allocator_slack_bytes()?)
            .ok_or(ExtractionError::ByteCountOverflow)?;
        enforce_byte_limit(&self.request, total_retained_bytes)?;
        Ok(ExtractionBatch {
            request: self.request,
            logical_retained_bytes,
            records,
        })
    }
}

impl ExtractionBatch {
    /// Constructs a batch bounded by request and global deep-retained ceilings.
    ///
    /// # Errors
    ///
    /// Rejects lineage transplants, count overflow, or deep-retained byte violations.
    pub fn try_new(
        request: &ExtractionRequest,
        records: Vec<ExtractionRecord>,
    ) -> Result<Self, ExtractionError> {
        if records.len() > request.max_records() as usize {
            return Err(ExtractionError::RecordLimitExceeded {
                requested: request.max_records(),
            });
        }
        if records
            .iter()
            .any(|record| !record.matches_request(request))
        {
            return Err(ExtractionError::ObjectBindingMismatch);
        }
        let records = AggregateBoundedRecords::try_new(records)?;
        let logical_retained_bytes = batch_base_bytes(request)?
            .checked_add(records.logical_retained_bytes()?)
            .ok_or(ExtractionError::ByteCountOverflow)?;
        let total_retained_bytes = logical_retained_bytes
            .checked_add(records.allocator_slack_bytes()?)
            .ok_or(ExtractionError::ByteCountOverflow)?;
        enforce_byte_limit(request, total_retained_bytes)?;
        Ok(Self {
            request: request.clone(),
            logical_retained_bytes,
            records,
        })
    }

    /// Returns exact request lineage.
    pub const fn request(&self) -> &ExtractionRequest {
        &self.request
    }

    /// Returns normalized records.
    pub fn records(&self) -> &[ExtractionRecord] {
        self.records.as_slice()
    }

    /// Returns checked deep-retained bytes, including current allocator slack.
    ///
    /// # Errors
    ///
    /// Returns [`ExtractionError::ByteCountOverflow`] if the platform allocation size cannot be
    /// represented by the canonical byte-count type.
    pub fn total_bytes(&self) -> Result<u64, ExtractionError> {
        self.logical_retained_bytes
            .checked_add(self.records.allocator_slack_bytes()?)
            .ok_or(ExtractionError::ByteCountOverflow)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExtractionBatchWire {
    request: ExtractionRequest,
    #[serde(rename = "total_retained_bytes")]
    logical_retained_bytes: u64,
    records: AggregateBoundedRecords,
}

impl<'de> Deserialize<'de> for ExtractionBatch {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let ExtractionBatchWire {
            request,
            logical_retained_bytes,
            records,
        } = ExtractionBatchWire::deserialize(deserializer)?;
        let rebuilt =
            Self::try_new(&request, records.into_vec()).map_err(serde::de::Error::custom)?;
        if rebuilt.logical_retained_bytes != logical_retained_bytes {
            return Err(serde::de::Error::custom(ExtractionError::ByteCountOverflow));
        }
        Ok(rebuilt)
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(transparent)]
struct AggregateBoundedRecords(Vec<ExtractionRecord>);

impl AggregateBoundedRecords {
    fn try_new(records: Vec<ExtractionRecord>) -> Result<Self, ExtractionError> {
        if records.len() > MAX_EXTRACTION_RECORDS {
            return Err(ExtractionError::LimitTooLarge {
                field: "records",
                max: MAX_EXTRACTION_RECORDS as u64,
            });
        }
        let normalized = Self(records);
        if normalized.runtime_retained_bytes()? > MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES {
            return Err(ExtractionError::ByteLimitExceeded {
                requested: MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES,
            });
        }
        Ok(normalized)
    }

    fn logical_retained_bytes(&self) -> Result<u64, ExtractionError> {
        self.0.iter().try_fold(0_u64, |total, record| {
            total
                .checked_add(record.retained_bytes()?)
                .ok_or(ExtractionError::ByteCountOverflow)
        })
    }

    fn allocator_slack_bytes(&self) -> Result<u64, ExtractionError> {
        unused_record_capacity_bytes(self.0.capacity(), self.0.len())
    }

    fn runtime_retained_bytes(&self) -> Result<u64, ExtractionError> {
        self.logical_retained_bytes()?
            .checked_add(self.allocator_slack_bytes()?)
            .ok_or(ExtractionError::ByteCountOverflow)
    }

    fn as_slice(&self) -> &[ExtractionRecord] {
        &self.0
    }

    fn into_vec(self) -> Vec<ExtractionRecord> {
        self.0
    }
}

struct AggregateBoundedRecordsVisitor;

impl<'de> Visitor<'de> for AggregateBoundedRecordsVisitor {
    type Value = AggregateBoundedRecords;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a globally count- and deep-byte-bounded record sequence")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        if sequence
            .size_hint()
            .is_some_and(|hint| hint > MAX_EXTRACTION_RECORDS)
        {
            return Err(serde::de::Error::custom(ExtractionError::LimitTooLarge {
                field: "records",
                max: MAX_EXTRACTION_RECORDS as u64,
            }));
        }
        // A hostile hint cannot authorize any structural allocation. Each growth is admitted from
        // the records actually decoded below.
        let mut records = Vec::new();
        let mut retained = 0_u64;
        while records.len() < MAX_EXTRACTION_RECORDS {
            let Some(record) = sequence.next_element::<ExtractionRecord>()? else {
                return AggregateBoundedRecords::try_new(records).map_err(serde::de::Error::custom);
            };
            let retained_after_push = retained
                .checked_add(record.retained_bytes().map_err(serde::de::Error::custom)?)
                .ok_or_else(|| serde::de::Error::custom(ExtractionError::ByteCountOverflow))?;
            let prospective_len = records
                .len()
                .checked_add(1)
                .ok_or_else(|| serde::de::Error::custom(ExtractionError::ByteCountOverflow))?;
            reserve_record_capacity(
                &mut records,
                prospective_len,
                retained_after_push,
                MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES,
            )
            .map_err(serde::de::Error::custom)?;
            records.push(record);
            retained = retained_after_push;
        }
        if sequence.next_element::<IgnoredAny>()?.is_some() {
            Err(serde::de::Error::custom(ExtractionError::LimitTooLarge {
                field: "records",
                max: MAX_EXTRACTION_RECORDS as u64,
            }))
        } else {
            AggregateBoundedRecords::try_new(records).map_err(serde::de::Error::custom)
        }
    }
}

fn batch_base_bytes(request: &ExtractionRequest) -> Result<u64, ExtractionError> {
    u64::try_from(size_of::<ExtractionBatch>())
        .map_err(|_| ExtractionError::ByteCountOverflow)?
        .checked_add(request.dynamic_retained_bytes()?)
        .ok_or(ExtractionError::ByteCountOverflow)
}

fn unused_record_capacity_bytes(capacity: usize, length: usize) -> Result<u64, ExtractionError> {
    let unused = capacity
        .checked_sub(length)
        .and_then(|count| count.checked_mul(size_of::<ExtractionRecord>()))
        .ok_or(ExtractionError::ByteCountOverflow)?;
    u64::try_from(unused).map_err(|_| ExtractionError::ByteCountOverflow)
}

fn enforce_byte_limit(
    request: &ExtractionRequest,
    total_retained_bytes: u64,
) -> Result<(), ExtractionError> {
    enforce_maximum(request_byte_limit(request), total_retained_bytes)
}

fn request_byte_limit(request: &ExtractionRequest) -> u64 {
    request
        .max_bytes()
        .min(MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES)
}

fn enforce_maximum(maximum: u64, total_retained_bytes: u64) -> Result<(), ExtractionError> {
    if total_retained_bytes > maximum {
        Err(ExtractionError::ByteLimitExceeded { requested: maximum })
    } else {
        Ok(())
    }
}

fn reserve_record_capacity(
    records: &mut Vec<ExtractionRecord>,
    prospective_len: usize,
    fixed_retained_bytes: u64,
    maximum_bytes: u64,
) -> Result<(), ExtractionError> {
    let admission = prospective_capacity_admission(records.capacity(), prospective_len)?;
    let admitted_capacity = admission
        .map(|(_, admitted_capacity)| admitted_capacity)
        .unwrap_or(records.capacity());
    let admitted_unused = unused_record_capacity_bytes(admitted_capacity, prospective_len)?;
    let admitted_total = fixed_retained_bytes
        .checked_add(admitted_unused)
        .ok_or(ExtractionError::ByteCountOverflow)?;
    enforce_maximum(maximum_bytes, admitted_total)?;

    let Some((minimum_capacity, admitted_capacity)) = admission else {
        return Ok(());
    };
    let additional = minimum_capacity
        .checked_sub(records.len())
        .ok_or(ExtractionError::ByteCountOverflow)?;
    records
        .try_reserve_exact(additional)
        .map_err(|_| ExtractionError::AllocationFailed)?;
    if records.capacity() < minimum_capacity || records.capacity() > admitted_capacity {
        return Err(ExtractionError::ByteLimitExceeded {
            requested: maximum_bytes,
        });
    }
    let actual_unused = unused_record_capacity_bytes(records.capacity(), prospective_len)?;
    let actual_total = fixed_retained_bytes
        .checked_add(actual_unused)
        .ok_or(ExtractionError::ByteCountOverflow)?;
    enforce_maximum(maximum_bytes, actual_total)
}

fn prospective_capacity_admission(
    current_capacity: usize,
    prospective_len: usize,
) -> Result<Option<(usize, usize)>, ExtractionError> {
    if prospective_len <= current_capacity {
        return Ok(None);
    }
    let geometric_capacity = if current_capacity == 0 {
        1
    } else {
        current_capacity
            .checked_mul(2)
            .ok_or(ExtractionError::ByteCountOverflow)?
    };
    let minimum_capacity = geometric_capacity.max(prospective_len);
    let admitted_capacity = minimum_capacity
        .checked_mul(ALLOCATOR_CAPACITY_ALLOWANCE_FACTOR)
        .ok_or(ExtractionError::ByteCountOverflow)?;
    Ok(Some((minimum_capacity, admitted_capacity)))
}

impl<'de> Deserialize<'de> for AggregateBoundedRecords {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(AggregateBoundedRecordsVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejected_capacity_growth_does_not_reserve() {
        let mut records = Vec::new();
        let initial_capacity = records.capacity();
        assert!(matches!(
            reserve_record_capacity(
                &mut records,
                1,
                MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES,
                MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES,
            ),
            Err(ExtractionError::ByteLimitExceeded {
                requested: MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES
            })
        ));
        assert_eq!(records.capacity(), initial_capacity);
    }
}
