//! Fixed writer-start storage and pinned standard-library runtime proof.

use std::sync::Arc;

use market_squawk_domain::{RawCaptureFrameView, checked_arc_value_allocation_bytes};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

use super::super::accounting::{
    AccountingComponent, CaptureAccountingError, CaptureMemoryAccounting, CaptureMemoryReservation,
};

pub(super) const WRITER_RUNTIME_FORMULA_REVISION: u32 = 1;
pub(super) const EXPECTED_ARTIFACT_SHA256: [u8; 32] = [
    0x4b, 0x2a, 0xaf, 0xb3, 0x49, 0x91, 0xac, 0x31, 0xef, 0x29, 0x6c, 0x42, 0xb7, 0x29, 0xb9, 0x4e,
    0xb1, 0x02, 0x0d, 0xe6, 0xb4, 0x00, 0x6b, 0x28, 0xe9, 0x73, 0x04, 0xf0, 0xe1, 0x5c, 0xa5, 0x0f,
];
pub(super) const MAX_EVENT_NAME_BYTES: usize = 16 + std::mem::size_of::<u64>();
pub(super) const MAX_GENERATION_NAME_BYTES: usize = 4 * std::mem::size_of::<u64>()
    + market_squawk_domain::SourceId::MAX_LENGTH
    + 2 * market_squawk_domain::SourceIdentifier::MAX_LENGTH;
pub(super) const PRIVATE_RUNTIME_UPPER_BOUND_BYTES: usize = 16 * 1024;
pub(super) const WRITER_THREAD_NAME: &str = "market-squawk-capture";
pub(super) const MAX_WRITER_THREAD_NAME_BYTES: usize = 64;

const WRITER_RUNTIME_PROOF_ARTIFACT: &[u8] = include_bytes!(
    "../../../../../docs/reports/performance/2026-07-17-q2-a4-writer-runtime-proof.md"
);

/// Failure to validate the pinned Rust writer-runtime proof.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum WriterRuntimeProofError {
    /// The binary target is not covered by the persisted proof artifact.
    #[error("capture writer runtime proof does not cover the compiled target")]
    CompiledTargetMismatch,
    /// The compiled formula revision differs from the persisted contract.
    #[error(
        "capture writer runtime formula revision mismatch: expected {expected}, actual {actual}"
    )]
    FormulaRevisionMismatch {
        /// Revision required by this binary.
        expected: u32,
        /// Revision presented for validation.
        actual: u32,
    },
    /// The embedded proof input differs from the reviewed artifact.
    #[error("capture writer runtime proof artifact hash mismatch")]
    ArtifactHashMismatch {
        /// Reviewed artifact digest.
        expected: [u8; 32],
        /// Digest computed from the embedded artifact.
        actual: [u8; 32],
    },
}

#[cfg(all(target_arch = "aarch64", target_os = "macos"))]
pub(super) const fn compiled_target() -> &'static str {
    "aarch64-apple-darwin"
}

#[cfg(all(target_arch = "x86_64", target_os = "macos"))]
pub(super) const fn compiled_target() -> &'static str {
    "x86_64-apple-darwin"
}

#[cfg(all(target_arch = "aarch64", target_os = "linux"))]
pub(super) const fn compiled_target() -> &'static str {
    "aarch64-unknown-linux-gnu"
}

#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
pub(super) const fn compiled_target() -> &'static str {
    "x86_64-unknown-linux-gnu"
}

#[cfg(all(target_arch = "aarch64", target_os = "windows"))]
pub(super) const fn compiled_target() -> &'static str {
    "aarch64-pc-windows-msvc"
}

#[cfg(all(target_arch = "x86_64", target_os = "windows"))]
pub(super) const fn compiled_target() -> &'static str {
    "x86_64-pc-windows-msvc"
}

#[cfg(not(any(
    all(target_arch = "aarch64", target_os = "macos"),
    all(target_arch = "x86_64", target_os = "macos"),
    all(target_arch = "aarch64", target_os = "linux"),
    all(target_arch = "x86_64", target_os = "linux"),
    all(target_arch = "aarch64", target_os = "windows"),
    all(target_arch = "x86_64", target_os = "windows"),
)))]
pub(super) const fn compiled_target() -> &'static str {
    "unsupported"
}

pub(super) fn artifact_sha256() -> [u8; 32] {
    Sha256::digest(WRITER_RUNTIME_PROOF_ARTIFACT).into()
}

pub(super) fn validate_runtime_proof(
    target: &str,
    formula_revision: u32,
    artifact_hash: [u8; 32],
) -> Result<(), WriterRuntimeProofError> {
    if !matches!(
        target,
        "aarch64-apple-darwin"
            | "x86_64-apple-darwin"
            | "aarch64-unknown-linux-gnu"
            | "x86_64-unknown-linux-gnu"
            | "aarch64-pc-windows-msvc"
            | "x86_64-pc-windows-msvc"
    ) {
        return Err(WriterRuntimeProofError::CompiledTargetMismatch);
    }
    if formula_revision != WRITER_RUNTIME_FORMULA_REVISION {
        return Err(WriterRuntimeProofError::FormulaRevisionMismatch {
            expected: WRITER_RUNTIME_FORMULA_REVISION,
            actual: formula_revision,
        });
    }
    if artifact_hash != EXPECTED_ARTIFACT_SHA256 {
        return Err(WriterRuntimeProofError::ArtifactHashMismatch {
            expected: EXPECTED_ARTIFACT_SHA256,
            actual: artifact_hash,
        });
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(super) enum WriterScratchError {
    #[error("capture writer scratch allocation of {requested_bytes} bytes failed")]
    AllocationFailed { requested_bytes: usize },
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(super) enum WriterScratchInvariantError {
    #[error("capture writer scratch capacity invariant failed")]
    Capacity,
    #[error("capture writer scratch length arithmetic overflowed")]
    ArithmeticOverflow,
    #[error("capture writer source scratch was not valid UTF-8")]
    InvalidUtf8,
}

#[derive(Debug)]
pub(super) struct WriterScratch {
    source: Vec<u8>,
    generation: Vec<u8>,
    event: Vec<u8>,
}

impl WriterScratch {
    pub(super) fn try_prepare() -> Result<Self, WriterScratchError> {
        Self::try_prepare_with_requests(
            market_squawk_domain::SourceId::MAX_LENGTH,
            MAX_GENERATION_NAME_BYTES,
            MAX_EVENT_NAME_BYTES,
        )
    }

    fn try_prepare_with_requests(
        source_bytes: usize,
        generation_bytes: usize,
        event_bytes: usize,
    ) -> Result<Self, WriterScratchError> {
        Ok(Self {
            source: try_fixed_bytes(source_bytes)?,
            generation: try_fixed_bytes(generation_bytes)?,
            event: try_fixed_bytes(event_bytes)?,
        })
    }

    pub(super) fn source_capacity(&self) -> usize {
        self.source.capacity()
    }

    pub(super) fn generation_capacity(&self) -> usize {
        self.generation.capacity()
    }

    pub(super) fn event_capacity(&self) -> usize {
        self.event.capacity()
    }

    pub(super) fn diagnostic_uuid_inputs<F: RawCaptureFrameView>(
        &mut self,
        frame: &F,
    ) -> Result<(Uuid, Uuid), WriterScratchInvariantError> {
        self.generation.clear();
        for field in [
            frame.source_id().as_str().as_bytes(),
            frame
                .metadata_revision()
                .as_source_identifier()
                .as_str()
                .as_bytes(),
            frame.session_identifier().as_str().as_bytes(),
        ] {
            let length = u64::try_from(field.len())
                .map_err(|_error| WriterScratchInvariantError::ArithmeticOverflow)?;
            extend_without_growth(&mut self.generation, &length.to_be_bytes())?;
            extend_without_growth(&mut self.generation, field)?;
        }
        extend_without_growth(
            &mut self.generation,
            &frame.connection_generation().get().to_be_bytes(),
        )?;
        let connection_id = Uuid::new_v5(&Uuid::NAMESPACE_OID, &self.generation);

        self.event.clear();
        extend_without_growth(&mut self.event, connection_id.as_bytes())?;
        extend_without_growth(&mut self.event, &frame.frame_ordinal().get().to_be_bytes())?;
        Ok((
            connection_id,
            Uuid::new_v5(&Uuid::NAMESPACE_OID, &self.event),
        ))
    }

    pub(super) fn source_arc(
        &mut self,
        source: &str,
    ) -> Result<Arc<str>, WriterScratchInvariantError> {
        self.source.clear();
        extend_without_growth(&mut self.source, source.as_bytes())?;
        let source = std::str::from_utf8(&self.source)
            .map_err(|_error| WriterScratchInvariantError::InvalidUtf8)?;
        Ok(Arc::from(source))
    }
}

fn extend_without_growth(
    destination: &mut Vec<u8>,
    bytes: &[u8],
) -> Result<(), WriterScratchInvariantError> {
    let next = destination
        .len()
        .checked_add(bytes.len())
        .ok_or(WriterScratchInvariantError::ArithmeticOverflow)?;
    if next > destination.capacity() {
        return Err(WriterScratchInvariantError::Capacity);
    }
    destination.extend_from_slice(bytes);
    Ok(())
}

fn try_fixed_bytes(requested_bytes: usize) -> Result<Vec<u8>, WriterScratchError> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(requested_bytes)
        .map_err(|_error| WriterScratchError::AllocationFailed { requested_bytes })?;
    Ok(bytes)
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(super) enum WriterFixedStorageError {
    #[error("capture writer fixed-storage formula overflowed")]
    ArithmeticOverflow,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct WriterFixedStorageTerms {
    pub(super) source_scratch_bytes: usize,
    pub(super) generation_scratch_bytes: usize,
    pub(super) event_scratch_bytes: usize,
    pub(super) destination_lease_bytes: usize,
    pub(super) owner_allocation_bytes: usize,
    pub(super) thread_name_bytes: usize,
    pub(super) spawn_packet_bytes: usize,
    pub(super) private_runtime_bytes: usize,
}

impl WriterFixedStorageTerms {
    pub(super) fn checked_total(self) -> Result<usize, WriterFixedStorageError> {
        [
            self.source_scratch_bytes,
            self.generation_scratch_bytes,
            self.event_scratch_bytes,
            self.destination_lease_bytes,
            self.owner_allocation_bytes,
            self.thread_name_bytes,
            self.spawn_packet_bytes,
            self.private_runtime_bytes,
        ]
        .into_iter()
        .try_fold(0usize, usize::checked_add)
        .ok_or(WriterFixedStorageError::ArithmeticOverflow)
    }
}

/// Auditable fixed-storage quote retained for the complete writer lifecycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriterFixedStorageReceipt {
    terms: WriterFixedStorageTerms,
    total_bytes: usize,
    compiled_target: &'static str,
    formula_revision: u32,
    artifact_sha256: [u8; 32],
}

impl WriterFixedStorageReceipt {
    /// Returns the allocator-observed source scratch capacity.
    pub const fn source_scratch_bytes(self) -> usize {
        self.terms.source_scratch_bytes
    }

    /// Returns the allocator-observed authority-name scratch capacity.
    pub const fn generation_scratch_bytes(self) -> usize {
        self.terms.generation_scratch_bytes
    }

    /// Returns the allocator-observed event-name scratch capacity.
    pub const fn event_scratch_bytes(self) -> usize {
        self.terms.event_scratch_bytes
    }

    /// Returns the complete per-writer destination-lease allocation charge.
    pub const fn destination_lease_bytes(self) -> usize {
        self.terms.destination_lease_bytes
    }

    /// Returns the complete allocation charge for the shared fixed-storage owner.
    pub const fn owner_allocation_bytes(self) -> usize {
        self.terms.owner_allocation_bytes
    }

    /// Returns the bounded builder and C-string thread-name charge.
    pub const fn thread_name_bytes(self) -> usize {
        self.terms.thread_name_bytes
    }

    /// Returns the concrete Market Squawk spawn-packet inline charge.
    pub const fn spawn_packet_bytes(self) -> usize {
        self.terms.spawn_packet_bytes
    }

    /// Returns the pinned private standard-library runtime upper bound.
    pub const fn private_runtime_bytes(self) -> usize {
        self.terms.private_runtime_bytes
    }

    /// Returns the checked total reserved against the channel ledger.
    pub const fn total_bytes(self) -> usize {
        self.total_bytes
    }

    /// Returns the exact compiled target covered by the proof artifact.
    pub const fn compiled_target(self) -> &'static str {
        self.compiled_target
    }

    /// Returns the fixed-storage formula revision.
    pub const fn formula_revision(self) -> u32 {
        self.formula_revision
    }

    /// Returns the SHA-256 binding to the persisted proof artifact.
    pub const fn artifact_sha256(self) -> [u8; 32] {
        self.artifact_sha256
    }
}

#[derive(Debug)]
pub(super) struct WriterFixedStorageOwner {
    receipt: WriterFixedStorageReceipt,
    _reservation: CaptureMemoryReservation,
}

impl WriterFixedStorageOwner {
    pub(super) const fn receipt(&self) -> &WriterFixedStorageReceipt {
        &self.receipt
    }
}

#[derive(Debug)]
pub(super) struct PreparedWriterRuntime {
    pub(super) scratch: WriterScratch,
    pub(super) thread_name: String,
    pub(super) fixed_storage: Arc<WriterFixedStorageOwner>,
}

#[derive(Debug, Error)]
pub(super) enum WriterRuntimePreparationError {
    #[error(transparent)]
    Scratch(#[from] WriterScratchError),
    #[error(transparent)]
    Proof(#[from] WriterRuntimeProofError),
    #[error("capture writer thread name is {actual} bytes; maximum is {limit}")]
    ThreadNameLimitExceeded { actual: usize, limit: usize },
    #[error("capture writer thread-name allocation of {requested_bytes} bytes failed")]
    ThreadNameAllocationFailed { requested_bytes: usize },
    #[error(transparent)]
    FixedStorage(#[from] WriterFixedStorageError),
    #[error("capture writer fixed-storage layout overflowed")]
    Layout,
    #[error(transparent)]
    Accounting(#[from] CaptureAccountingError),
}

pub(super) fn prepare_writer_runtime(
    accounting: &Arc<CaptureMemoryAccounting>,
    destination_lease_bytes: usize,
    spawn_packet_bytes: usize,
) -> Result<PreparedWriterRuntime, WriterRuntimePreparationError> {
    let scratch = WriterScratch::try_prepare()?;
    let mut thread_name = String::new();
    if WRITER_THREAD_NAME.len() > MAX_WRITER_THREAD_NAME_BYTES {
        return Err(WriterRuntimePreparationError::ThreadNameLimitExceeded {
            actual: WRITER_THREAD_NAME.len(),
            limit: MAX_WRITER_THREAD_NAME_BYTES,
        });
    }
    thread_name
        .try_reserve_exact(WRITER_THREAD_NAME.len())
        .map_err(
            |_error| WriterRuntimePreparationError::ThreadNameAllocationFailed {
                requested_bytes: WRITER_THREAD_NAME.len(),
            },
        )?;
    thread_name.push_str(WRITER_THREAD_NAME);

    let observed_artifact_sha256 = artifact_sha256();
    validate_runtime_proof(
        compiled_target(),
        WRITER_RUNTIME_FORMULA_REVISION,
        observed_artifact_sha256,
    )?;
    let owner_allocation_bytes = checked_arc_value_allocation_bytes::<WriterFixedStorageOwner>(0)
        .map_err(|_error| WriterRuntimePreparationError::Layout)?;
    let c_string_name_bytes = thread_name
        .len()
        .checked_add(1)
        .ok_or(WriterFixedStorageError::ArithmeticOverflow)?;
    let thread_name_bytes = thread_name
        .capacity()
        .checked_add(c_string_name_bytes)
        .ok_or(WriterFixedStorageError::ArithmeticOverflow)?;
    let terms = WriterFixedStorageTerms {
        source_scratch_bytes: scratch.source_capacity(),
        generation_scratch_bytes: scratch.generation_capacity(),
        event_scratch_bytes: scratch.event_capacity(),
        destination_lease_bytes,
        owner_allocation_bytes,
        thread_name_bytes,
        spawn_packet_bytes,
        private_runtime_bytes: PRIVATE_RUNTIME_UPPER_BOUND_BYTES,
    };
    let total_bytes = terms.checked_total()?;
    let receipt = WriterFixedStorageReceipt {
        terms,
        total_bytes,
        compiled_target: compiled_target(),
        formula_revision: WRITER_RUNTIME_FORMULA_REVISION,
        artifact_sha256: observed_artifact_sha256,
    };
    let reservation = accounting.try_reserve(AccountingComponent::Fixed, total_bytes)?;
    Ok(PreparedWriterRuntime {
        scratch,
        thread_name,
        fixed_storage: Arc::new(WriterFixedStorageOwner {
            receipt,
            _reservation: reservation,
        }),
    })
}

#[cfg(all(test, market_squawk_loom))]
mod loom_model;

#[cfg(test)]
mod tests {
    use super::{
        EXPECTED_ARTIFACT_SHA256, WRITER_RUNTIME_FORMULA_REVISION, WriterFixedStorageTerms,
        WriterRuntimeProofError, WriterScratch, validate_runtime_proof,
    };

    #[test]
    fn runtime_proof_rejects_each_mismatched_binding() {
        assert_eq!(
            validate_runtime_proof(
                "unsupported-target",
                WRITER_RUNTIME_FORMULA_REVISION,
                EXPECTED_ARTIFACT_SHA256,
            ),
            Err(WriterRuntimeProofError::CompiledTargetMismatch)
        );
        assert_eq!(
            validate_runtime_proof(
                super::compiled_target(),
                WRITER_RUNTIME_FORMULA_REVISION + 1,
                EXPECTED_ARTIFACT_SHA256,
            ),
            Err(WriterRuntimeProofError::FormulaRevisionMismatch {
                expected: WRITER_RUNTIME_FORMULA_REVISION,
                actual: WRITER_RUNTIME_FORMULA_REVISION + 1,
            })
        );
        assert_eq!(
            validate_runtime_proof(
                super::compiled_target(),
                WRITER_RUNTIME_FORMULA_REVISION,
                [0; 32],
            ),
            Err(WriterRuntimeProofError::ArtifactHashMismatch {
                expected: EXPECTED_ARTIFACT_SHA256,
                actual: [0; 32],
            })
        );
    }

    #[test]
    fn runtime_proof_accepts_the_embedded_artifact_and_compiled_target() {
        let actual = super::artifact_sha256();
        assert_eq!(actual, EXPECTED_ARTIFACT_SHA256);
        assert_eq!(
            validate_runtime_proof(
                super::compiled_target(),
                WRITER_RUNTIME_FORMULA_REVISION,
                actual,
            ),
            Ok(())
        );
    }

    #[test]
    fn scratch_preparation_reports_observed_capacities_and_refuses_impossible_allocation()
    -> Result<(), Box<dyn std::error::Error>> {
        let scratch = WriterScratch::try_prepare()?;
        assert!(scratch.source_capacity() >= market_squawk_domain::SourceId::MAX_LENGTH);
        assert!(scratch.generation_capacity() >= super::MAX_GENERATION_NAME_BYTES);
        assert!(scratch.event_capacity() >= super::MAX_EVENT_NAME_BYTES);
        assert!(matches!(
            WriterScratch::try_prepare_with_requests(usize::MAX, 1, 1),
            Err(super::WriterScratchError::AllocationFailed {
                requested_bytes: usize::MAX
            })
        ));
        Ok(())
    }

    #[test]
    fn fixed_storage_terms_accept_exact_sum_and_reject_overflow() {
        let terms = WriterFixedStorageTerms {
            source_scratch_bytes: 1,
            generation_scratch_bytes: 2,
            event_scratch_bytes: 3,
            destination_lease_bytes: 4,
            owner_allocation_bytes: 5,
            thread_name_bytes: 6,
            spawn_packet_bytes: 7,
            private_runtime_bytes: 8,
        };
        assert_eq!(terms.checked_total(), Ok(36));
        assert!(matches!(
            WriterFixedStorageTerms {
                private_runtime_bytes: usize::MAX,
                ..terms
            }
            .checked_total(),
            Err(super::WriterFixedStorageError::ArithmeticOverflow)
        ));
    }
}
