//! Exact, bounded contracts for assigning durable research-observation revisions.

use std::time::Instant;

use futures_util::future::BoxFuture;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

mod batch;
mod canonical;
mod evidence;
mod semantic;

pub use batch::{
    ExtractionRevisionEvidence, ExtractionRevisionPlan, ObservedRevisionAssignments,
    ObservedRevisionBatch, ObservedRevisionRecord,
};
pub use canonical::CanonicalObservationFamily;
pub use evidence::{
    MAX_OBSERVED_SEMANTIC_PAYLOAD_BYTES, MAX_OBSERVED_VERSION_EVIDENCE_BYTES,
    ObservedProviderOrder, ObservedSemanticPayload, ObservedVersionEvidence, ObservedVersionKind,
};
pub use semantic::CanonicalObservationPayload;
#[doc(hidden)]
pub use semantic::serializer::{PitV1CanonicalEncoder, PitV1EncodingControl, PitV1EncodingError};

/// Maximum input records admitted by one atomic observed-revision assignment.
pub const MAX_OBSERVED_REVISION_BATCH_RECORDS: usize = 100_000;
/// Maximum deep bytes admitted by one in-memory observed-revision assignment.
pub const MAX_OBSERVED_REVISION_BATCH_BYTES: usize = 64 * 1024 * 1024;

const _: () = assert!(MAX_OBSERVED_REVISION_BATCH_BYTES > MAX_OBSERVED_SEMANTIC_PAYLOAD_BYTES);

/// Failure to construct, validate, or durably assign observed research revisions.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ObservedRevisionError {
    /// A required exact evidence value was empty.
    #[error("observed revision {field} evidence must not be empty")]
    EmptyEvidence {
        /// Stable evidence field name.
        field: &'static str,
    },
    /// One exact evidence value exceeded its field-specific byte ceiling.
    #[error("observed revision {field} evidence exceeds {max} bytes")]
    EvidenceLimitExceeded {
        /// Stable evidence field name.
        field: &'static str,
        /// Inclusive byte ceiling.
        max: usize,
    },
    /// The input count exceeded the atomic batch ceiling.
    #[error("observed revision batch exceeds {max} input records")]
    RecordLimitExceeded {
        /// Inclusive record-count ceiling.
        max: usize,
    },
    /// The checked deep-retained batch size exceeded its ceiling.
    #[error("observed revision batch exceeds {max} retained bytes")]
    BatchByteLimitExceeded {
        /// Inclusive deep-retained byte ceiling.
        max: usize,
    },
    /// Checked byte or allocation arithmetic overflowed.
    #[error("observed revision byte accounting overflowed")]
    ByteCountOverflow,
    /// A canonical PIT-v1 family or payload cannot be encoded exactly.
    #[error("observed revision canonical encoding failed")]
    CanonicalEncoding,
    /// A fallible bounded allocation could not be reserved.
    #[error("observed revision bounded allocation failed")]
    AllocationFailure,
    /// A family from another source was transplanted into a single-source batch.
    #[error("observed revision family does not match the batch source")]
    SourceMismatch,
    /// One exact family/version claimed divergent payload or ordering evidence.
    #[error("observed revision evidence conflicts with the same exact family and version")]
    Conflict,
    /// Provider ordering was missing, ambiguous, or attached to locally observed content.
    #[error("provider-supplied versions require one unambiguous provider order")]
    AmbiguousProviderOrder,
    /// Unique-record assignments did not match the validated batch cardinality.
    #[error("observed revision assignments expected {expected} rows but received {observed}")]
    AssignmentCountMismatch {
        /// Required unique-record assignment count.
        expected: usize,
        /// Supplied assignment count.
        observed: usize,
    },
    /// The durable one-based revision sequence could not advance.
    #[error("observed revision sequence overflowed")]
    RevisionOverflow,
    /// A newly observed provider version precedes the immutable retained frontier.
    #[error("observed revision is an unsupported predecessor of retained history")]
    BackfillRejected,
    /// Durable revision state was internally inconsistent or digest evidence mismatched.
    #[error("observed revision authority state is corrupt")]
    CorruptAuthorityState,
    /// Durable revision state could not be read or committed.
    #[error("observed revision persistence is unavailable")]
    PersistenceUnavailable,
    /// The operation's monotonic deadline elapsed before commit.
    #[error("observed revision assignment deadline elapsed")]
    DeadlineExceeded,
    /// Cancellation was observed before commit.
    #[error("observed revision assignment was cancelled")]
    Cancelled,
}

/// Object-safe asynchronous authority for one atomic observed-revision assignment.
///
/// Implementations must exact-compare retained family, version, and payload evidence on every
/// digest hit, apply cancellation/deadline checks before commit, and either commit the entire batch
/// or leave durable state unchanged.
pub trait ObservedRevisionAuthority: Send + Sync {
    /// Assigns replay-stable one-based revisions and returns them in original input order.
    fn assign(
        &self,
        batch: ObservedRevisionBatch,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<ObservedRevisionAssignments, ObservedRevisionError>>;
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU16;

    use market_squawk_domain::{
        CalendarDate, DigestAlgorithm, ResearchPeriod, ResearchTemporalCoordinate, RevisionNumber,
        SourceId, SourceIdentifier,
    };
    use static_assertions::assert_obj_safe;

    use super::{
        CanonicalObservationFamily, MAX_OBSERVED_REVISION_BATCH_BYTES,
        MAX_OBSERVED_SEMANTIC_PAYLOAD_BYTES, ObservedProviderOrder, ObservedRevisionAuthority,
        ObservedRevisionBatch, ObservedRevisionError, ObservedRevisionRecord,
        ObservedSemanticPayload, ObservedVersionEvidence, ObservedVersionKind,
    };

    assert_obj_safe!(ObservedRevisionAuthority);

    fn source(value: &str) -> Result<SourceId, Box<dyn std::error::Error>> {
        Ok(SourceId::try_from(value)?)
    }

    fn identifier(value: &str) -> Result<SourceIdentifier, Box<dyn std::error::Error>> {
        Ok(SourceIdentifier::try_from(value)?)
    }

    fn record(
        family: &CanonicalObservationFamily,
        token: &[u8],
        payload: &[u8],
        order_date: Option<CalendarDate>,
    ) -> Result<ObservedRevisionRecord, ObservedRevisionError> {
        let version = ObservedVersionEvidence::provider_supplied(token)?;
        let payload = ObservedSemanticPayload::try_from_bytes(payload)?;
        let order = order_date
            .map(|date| {
                ObservedProviderOrder::try_new(
                    ResearchTemporalCoordinate::calendar_date(date),
                    token,
                )
            })
            .transpose()?;
        ObservedRevisionRecord::try_new(family.clone(), version, payload, order)
    }

    #[test]
    fn observed_revision_contract_is_exact_bounded_aligned_and_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = source("fred")?;
        let effective = ResearchTemporalCoordinate::source_period(ResearchPeriod::try_new(
            identifier("fred-quarter")?,
            2024,
            NonZeroU16::new(1).ok_or("nonzero fixture")?,
            identifier("Q1")?,
        )?);
        let family = CanonicalObservationFamily::macro_observation(
            &source,
            &identifier("GDP")?,
            &effective,
        )?;

        let mut exact_family = b"MSQPIT".to_vec();
        exact_family.extend_from_slice(&1_u16.to_le_bytes());
        exact_family.extend_from_slice(&24_u64.to_le_bytes());
        exact_family.extend_from_slice(b"market-squawk/pit/family");
        exact_family.push(3);
        exact_family.extend_from_slice(&4_u64.to_le_bytes());
        exact_family.extend_from_slice(b"fred");
        exact_family.extend_from_slice(&3_u64.to_le_bytes());
        exact_family.extend_from_slice(b"GDP");
        exact_family.push(3);
        exact_family.extend_from_slice(&12_u64.to_le_bytes());
        exact_family.extend_from_slice(b"fred-quarter");
        exact_family.extend_from_slice(&2024_u16.to_le_bytes());
        exact_family.extend_from_slice(&1_u16.to_le_bytes());
        exact_family.extend_from_slice(&2_u64.to_le_bytes());
        exact_family.extend_from_slice(b"Q1");
        assert_eq!(family.exact_bytes(), exact_family);
        assert_eq!(family.identity().algorithm(), DigestAlgorithm::Sha256);
        assert_eq!(
            family.identity().bytes(),
            [
                0x3f, 0x05, 0x1c, 0x37, 0x3d, 0x3f, 0x62, 0x50, 0x07, 0x4e, 0xab, 0x6c, 0xa0, 0x7f,
                0x36, 0xa9, 0x66, 0x91, 0x30, 0x7f, 0x45, 0xc6, 0x82, 0xed, 0x0a, 0x99, 0xa8, 0x60,
                0xf5, 0x1d, 0x38, 0x1a,
            ]
        );

        let provider_version = ObservedVersionEvidence::provider_supplied(b"vintage-2024-01")?;
        let same_provider_version = ObservedVersionEvidence::provider_supplied(b"vintage-2024-01")?;
        let local_payload = ObservedSemanticPayload::try_from_bytes(b"canonical-row-v1")?;
        let local_version = ObservedVersionEvidence::locally_observed_content(&local_payload)?;
        assert_eq!(
            provider_version.kind(),
            ObservedVersionKind::ProviderSupplied
        );
        assert_eq!(
            local_version.kind(),
            ObservedVersionKind::LocallyObservedContent
        );
        assert_eq!(
            provider_version.identity(),
            same_provider_version.identity()
        );
        assert_eq!(provider_version.exact_evidence(), b"vintage-2024-01");
        assert_eq!(
            local_version.exact_evidence(),
            local_payload.exact_evidence()
        );
        let wrong_local_payload = ObservedSemanticPayload::try_from_bytes(b"different-row")?;
        assert_eq!(
            ObservedRevisionRecord::try_new(
                family.clone(),
                local_version,
                wrong_local_payload,
                None,
            ),
            Err(ObservedRevisionError::Conflict)
        );

        let provider_without_order = ObservedRevisionRecord::try_new(
            family.clone(),
            provider_version,
            local_payload.clone(),
            None,
        );
        assert_eq!(
            provider_without_order,
            Err(ObservedRevisionError::AmbiguousProviderOrder)
        );
        assert_eq!(
            ObservedRevisionRecord::try_new(
                family.clone(),
                ObservedVersionEvidence::locally_observed_content(&local_payload)?,
                local_payload,
                Some(ObservedProviderOrder::try_new(
                    ResearchTemporalCoordinate::calendar_date(CalendarDate::new(2024, 1, 1)?),
                    b"not-local-order",
                )?),
            ),
            Err(ObservedRevisionError::AmbiguousProviderOrder)
        );

        let first_date = CalendarDate::new(2024, 1, 1)?;
        let second_date = CalendarDate::new(2024, 2, 1)?;
        let records = vec![
            record(&family, b"v2", b"101.0", Some(second_date))?,
            record(&family, b"v1", b"100.0", Some(first_date))?,
            record(&family, b"v1", b"100.0", Some(first_date))?,
        ];
        let batch = ObservedRevisionBatch::try_new(source.clone(), records)?;
        assert_eq!(batch.input_len(), 3);
        assert_eq!(batch.unique_records().len(), 2);
        let assignments =
            batch.align_assignments(vec![RevisionNumber::new(7)?, RevisionNumber::new(8)?])?;
        assert_eq!(
            assignments.as_slice(),
            [
                RevisionNumber::new(8)?,
                RevisionNumber::new(7)?,
                RevisionNumber::new(7)?,
            ]
        );

        let divergent_payload = ObservedRevisionBatch::try_new(
            source.clone(),
            vec![
                record(&family, b"same-token", b"100.0", Some(first_date))?,
                record(&family, b"same-token", b"changed-body", Some(first_date))?,
            ],
        );
        assert_eq!(divergent_payload, Err(ObservedRevisionError::Conflict));

        let bounded_record = record(&family, b"bounded", b"100.0", Some(first_date))?;
        assert_eq!(
            ObservedRevisionBatch::try_new_with_test_limits(
                source.clone(),
                vec![bounded_record.clone()],
                0,
                MAX_OBSERVED_REVISION_BATCH_BYTES,
            ),
            Err(ObservedRevisionError::RecordLimitExceeded { max: 0 })
        );
        assert_eq!(
            ObservedRevisionBatch::try_new_with_test_limits(
                source.clone(),
                vec![bounded_record],
                1,
                0,
            ),
            Err(ObservedRevisionError::BatchByteLimitExceeded { max: 0 })
        );

        let missing_provider_order = ObservedRevisionRecord::try_new(
            family,
            ObservedVersionEvidence::provider_supplied(b"v2")?,
            ObservedSemanticPayload::try_from_bytes(b"101.0")?,
            None,
        );
        assert_eq!(
            missing_provider_order,
            Err(ObservedRevisionError::AmbiguousProviderOrder)
        );

        let oversized_payload = vec![0_u8; MAX_OBSERVED_SEMANTIC_PAYLOAD_BYTES + 1];
        assert_eq!(
            ObservedSemanticPayload::try_from_bytes(&oversized_payload),
            Err(ObservedRevisionError::EvidenceLimitExceeded {
                field: "semantic_payload",
                max: MAX_OBSERVED_SEMANTIC_PAYLOAD_BYTES,
            })
        );
        Ok(())
    }
}
