//! Deep-retained bounded extraction batches and hostile-wire validation.

use serde::de::{IgnoredAny, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;
use std::mem::size_of;

use super::contracts::{
    ExtractionError, ExtractionRecord, ExtractionRequest, MAX_EXTRACTION_RECORDS,
    MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES,
};

/// Intrinsically bounded normalized extraction output retaining its exact request.
#[derive(Clone, Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExtractionBatch {
    request: ExtractionRequest,
    total_retained_bytes: u64,
    records: AggregateBoundedRecords,
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
        let record_bytes = records.retained_bytes()?;
        let total_retained_bytes = u64::try_from(size_of::<Self>())
            .map_err(|_| ExtractionError::ByteCountOverflow)?
            .checked_add(request.dynamic_retained_bytes()?)
            .and_then(|total| total.checked_add(record_bytes))
            .ok_or(ExtractionError::ByteCountOverflow)?;
        if total_retained_bytes > request.max_bytes()
            || total_retained_bytes > MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES
        {
            return Err(ExtractionError::ByteLimitExceeded {
                requested: request
                    .max_bytes()
                    .min(MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES),
            });
        }
        Ok(Self {
            request: request.clone(),
            total_retained_bytes,
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

    /// Returns checked deep-retained bytes, including structural overhead.
    pub const fn total_bytes(&self) -> u64 {
        self.total_retained_bytes
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExtractionBatchWire {
    request: ExtractionRequest,
    total_retained_bytes: u64,
    records: AggregateBoundedRecords,
}

impl<'de> Deserialize<'de> for ExtractionBatch {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ExtractionBatchWire::deserialize(deserializer)?;
        let rebuilt = Self::try_new(&wire.request, wire.records.as_slice().to_vec())
            .map_err(serde::de::Error::custom)?;
        if rebuilt.total_retained_bytes != wire.total_retained_bytes {
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
        let normalized = Self(records.into_boxed_slice().into_vec());
        if normalized.retained_bytes()? > MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES {
            return Err(ExtractionError::ByteLimitExceeded {
                requested: MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES,
            });
        }
        Ok(normalized)
    }

    fn retained_bytes(&self) -> Result<u64, ExtractionError> {
        self.0.iter().try_fold(0_u64, |total, record| {
            total
                .checked_add(record.retained_bytes()?)
                .ok_or(ExtractionError::ByteCountOverflow)
        })
    }

    fn as_slice(&self) -> &[ExtractionRecord] {
        &self.0
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
        // Never trust a hostile size hint for a large up-front allocation.
        let mut records = Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(1_024));
        let mut retained = 0_u64;
        while records.len() < MAX_EXTRACTION_RECORDS {
            let Some(record) = sequence.next_element::<ExtractionRecord>()? else {
                return Ok(AggregateBoundedRecords(
                    records.into_boxed_slice().into_vec(),
                ));
            };
            retained = retained
                .checked_add(record.retained_bytes().map_err(serde::de::Error::custom)?)
                .ok_or_else(|| serde::de::Error::custom(ExtractionError::ByteCountOverflow))?;
            if retained > MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES {
                return Err(serde::de::Error::custom(
                    ExtractionError::ByteLimitExceeded {
                        requested: MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES,
                    },
                ));
            }
            records.push(record);
        }
        if sequence.next_element::<IgnoredAny>()?.is_some() {
            Err(serde::de::Error::custom(ExtractionError::LimitTooLarge {
                field: "records",
                max: MAX_EXTRACTION_RECORDS as u64,
            }))
        } else {
            Ok(AggregateBoundedRecords(
                records.into_boxed_slice().into_vec(),
            ))
        }
    }
}

impl<'de> Deserialize<'de> for AggregateBoundedRecords {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(AggregateBoundedRecordsVisitor)
    }
}
