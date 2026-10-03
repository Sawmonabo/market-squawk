//! Bounded packing of independently sealed captures, preserving each original receipt.
use super::super::capture::{ProviderWholeCaptureToken, SealedProviderCaptureSetReceipt};
use super::*;
use market_squawk_platform::ResearchObjectControlPoint;
use std::io::{Seek as _, SeekFrom};
use std::sync::Arc;

const DOMAIN: &[u8] = b"market-squawk/provider-capture-pack/v1";

/// Value-only ordered identity accumulator; it does not grant publication authority.
#[derive(Clone, Debug)]
pub struct ProviderCapturePackAccumulator {
    hash: Sha256,
    captures: u64,
    bodies: u64,
    bytes: u64,
}
impl Default for ProviderCapturePackAccumulator {
    fn default() -> Self {
        Self::new()
    }
}
impl ProviderCapturePackAccumulator {
    /// Starts the exact original-capture sequence.
    pub fn new() -> Self {
        let mut hash = Sha256::new();
        hash.update(DOMAIN);
        Self {
            hash,
            captures: 0,
            bodies: 0,
            bytes: 0,
        }
    }
    /// Adds one complete original receipt in declared source order.
    pub fn push(
        &mut self,
        receipt: &SealedProviderCaptureSetReceipt,
    ) -> Result<(), ProviderLogicalPublicationError> {
        self.push_evidence(
            receipt.receipt_digest(),
            receipt.capture().pages().len() as u64,
            receipt.capture().total_body_bytes(),
        )
    }
    /// Accumulates already validated persisted value evidence; this cannot mint a pack seal.
    pub fn push_evidence(
        &mut self,
        receipt_digest: EvidenceDigest,
        body_count: u64,
        body_bytes: u64,
    ) -> Result<(), ProviderLogicalPublicationError> {
        if body_count == 0 || receipt_digest.algorithm() != DigestAlgorithm::Sha256 {
            return Err(ProviderLogicalPublicationError::CaptureObjectMismatch);
        }
        self.hash.update(self.captures.to_le_bytes());
        self.hash.update(self.bytes.to_le_bytes());
        self.hash.update(receipt_digest.bytes());
        self.hash.update(body_count.to_le_bytes());
        self.hash.update(body_bytes.to_le_bytes());
        self.captures = self
            .captures
            .checked_add(1)
            .ok_or(ProviderLogicalPublicationError::CountOverflow)?;
        self.bodies = self
            .bodies
            .checked_add(body_count)
            .ok_or(ProviderLogicalPublicationError::CountOverflow)?;
        self.bytes = self
            .bytes
            .checked_add(body_bytes)
            .ok_or(ProviderLogicalPublicationError::CountOverflow)?;
        Ok(())
    }
    /// Returns the next packed body byte offset without retaining earlier captures.
    pub const fn size_bytes(&self) -> u64 {
        self.bytes
    }
    /// Returns the exact number of independent captures.
    pub const fn capture_count(&self) -> u64 {
        self.captures
    }
    /// Closes the value identity including exact counts and byte length.
    pub fn finish(mut self) -> EvidenceDigest {
        self.hash.update(self.captures.to_le_bytes());
        self.hash.update(self.bodies.to_le_bytes());
        self.hash.update(self.bytes.to_le_bytes());
        EvidenceDigest::new(DigestAlgorithm::Sha256, self.hash.finalize().into())
    }
}

/// One bounded original response's immutable byte coordinate in the shared pack.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PackedProviderCaptureBody {
    /// Original page ordinal within this independent capture.
    pub page_ordinal: u16,
    /// Exact byte offset in the complete packed payload.
    pub offset: u64,
    /// Exact original response length.
    pub size_bytes: u64,
    /// Original response digest retained unchanged.
    pub content_digest: EvidenceDigest,
}

/// Issued only after consuming an original capture token and copying its verified bytes.
#[derive(Debug, Serialize)]
pub struct PackedProviderCapture {
    ordinal: u64,
    receipt: SealedProviderCaptureSetReceipt,
    bodies: Box<[PackedProviderCaptureBody]>,
}
impl PackedProviderCapture {
    /// Returns its exact position among independent original captures.
    pub const fn ordinal(&self) -> u64 {
        self.ordinal
    }
    /// Returns original receipt evidence, never newly manufactured capture authority.
    pub const fn receipt(&self) -> &SealedProviderCaptureSetReceipt {
        &self.receipt
    }
    /// Returns one bounded capture's exact body coordinates.
    pub fn bodies(&self) -> &[PackedProviderCaptureBody] {
        &self.bodies
    }
}

/// One bounded-memory writer for a disk-sized sequence of independently sealed bodies.
#[derive(Debug)]
pub struct PendingProviderCapturePack {
    store: Arc<SealedResearchJournalStore>,
    pending: Option<PendingResearchObject>,
    identity: ProviderCapturePackAccumulator,
    source: Option<SourceId>,
    first_capture: Option<SealedProviderCaptureSetReceipt>,
    poisoned: bool,
}
impl PendingProviderCapturePack {
    /// Starts controlled raw staging under the caller's independent disk admission.
    pub fn begin(
        store: Arc<SealedResearchJournalStore>,
        admission: ResearchObjectAdmission,
    ) -> Result<Self, ProviderLogicalPublicationError> {
        Ok(Self {
            pending: Some(store.begin_logical_object(admission)?),
            store,
            identity: ProviderCapturePackAccumulator::new(),
            source: None,
            first_capture: None,
            poisoned: false,
        })
    }
    /// Consumes exactly one original seal and appends its complete verified response bodies.
    pub fn append(
        &mut self,
        token: ProviderWholeCaptureToken,
        objects: Vec<VerifiedResearchObject>,
        control: &dyn ResearchObjectControl,
    ) -> Result<PackedProviderCapture, ProviderLogicalPublicationError> {
        let result = self.append_inner(token, objects, control);
        if result.is_err() {
            if let Some(pending) = self.pending.take() {
                self.store.abort_logical_object(pending)?;
            }
        }
        result
    }

    fn append_inner(
        &mut self,
        token: ProviderWholeCaptureToken,
        objects: Vec<VerifiedResearchObject>,
        control: &dyn ResearchObjectControl,
    ) -> Result<PackedProviderCapture, ProviderLogicalPublicationError> {
        if self.poisoned {
            return Err(ProviderLogicalPublicationError::Poisoned);
        }
        self.poisoned = true;
        let receipt = token.persisted_receipt().clone();
        if objects.len() != receipt.capture().pages().len()
            || objects.is_empty()
            || self
                .source
                .as_ref()
                .is_some_and(|source| source != receipt.capture().source_id())
        {
            return Err(ProviderLogicalPublicationError::CaptureObjectMismatch);
        }
        let ordinal = self.identity.capture_count();
        let mut offset = self.identity.size_bytes();
        let mut bodies = Vec::with_capacity(objects.len());
        let mut buffer = [0u8; 64 * 1024];
        for (page_ordinal, mut object) in objects.into_iter().enumerate() {
            let page_ordinal = u16::try_from(page_ordinal)
                .map_err(|_| ProviderLogicalPublicationError::OrdinalOverflow)?;
            let frame = receipt
                .row_frame(0, page_ordinal)
                .map_err(|_| ProviderLogicalPublicationError::CaptureObjectMismatch)?;
            let page = &receipt.capture().pages()[usize::from(page_ordinal)];
            if object.content_digest() != frame.page_body_digest()
                || object.size_bytes() != page.body_bytes()
            {
                return Err(ProviderLogicalPublicationError::CaptureObjectMismatch);
            }
            object.seek(SeekFrom::Start(0))?;
            let mut hash = Sha256::new();
            let mut copied = 0u64;
            loop {
                control
                    .checkpoint(ResearchObjectControlPoint::BeforeVerificationChunk {
                        offset_bytes: copied,
                    })
                    .map_err(SealedResearchJournalStoreError::ObjectControl)?;
                let count = object.read(&mut buffer)?;
                if count == 0 {
                    break;
                }
                self.pending
                    .as_mut()
                    .ok_or(ProviderLogicalPublicationError::Poisoned)?
                    .write_all(&buffer[..count])?;
                hash.update(&buffer[..count]);
                copied = copied
                    .checked_add(count as u64)
                    .ok_or(ProviderLogicalPublicationError::CountOverflow)?;
            }
            let digest = EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into());
            if copied != page.body_bytes() || digest != page.body_digest() {
                return Err(ProviderLogicalPublicationError::CaptureObjectMismatch);
            }
            object.reverify_for_commit(control)?;
            bodies.push(PackedProviderCaptureBody {
                page_ordinal,
                offset,
                size_bytes: copied,
                content_digest: digest,
            });
            offset = offset
                .checked_add(copied)
                .ok_or(ProviderLogicalPublicationError::CountOverflow)?;
        }
        self.identity.push(&receipt)?;
        if offset != self.identity.size_bytes() {
            return Err(ProviderLogicalPublicationError::CaptureObjectMismatch);
        }
        if self.first_capture.is_none() {
            self.first_capture = Some(receipt.clone());
        }
        self.source = Some(receipt.capture().source_id().clone());
        self.poisoned = false;
        Ok(PackedProviderCapture {
            ordinal,
            receipt,
            bodies: bodies.into_boxed_slice(),
        })
    }
    /// Seals one immutable payload regardless of the number of original HTTP responses.
    pub fn finish(
        mut self,
        store: &SealedResearchJournalStore,
        control: &dyn ResearchObjectControl,
        logical_ordinal: u32,
    ) -> Result<SealedProviderCapturePack, ProviderLogicalPublicationError> {
        if self.poisoned || self.identity.capture_count() == 0 {
            return Err(ProviderLogicalPublicationError::Poisoned);
        }
        let capture_count = self.identity.capture_count();
        let body_count = self.identity.bodies;
        let bytes = self.identity.size_bytes();
        let identity = std::mem::take(&mut self.identity).finish();
        let object = store.finish_logical_object(
            self.pending
                .take()
                .ok_or(ProviderLogicalPublicationError::Poisoned)?,
            control,
        )?;
        if object.size_bytes() != bytes {
            return Err(ProviderLogicalPublicationError::CaptureObjectMismatch);
        }
        let input = SealedLogicalObjectInput::try_from_verified(
            LogicalObjectRole::ProviderPayload,
            logical_ordinal,
            identity,
            object,
            control,
        )?;
        let seal = ProviderCapturePackSeal {
            first_capture: self
                .first_capture
                .take()
                .ok_or(ProviderLogicalPublicationError::CaptureObjectMismatch)?,
            captures_digest: identity,
            capture_count,
            body_count,
            source_id: self
                .source
                .take()
                .ok_or(ProviderLogicalPublicationError::CaptureObjectMismatch)?,
            object: input.object().clone(),
            logical_ordinal,
        };
        Ok(SealedProviderCapturePack { input, seal })
    }
    /// Discards an incomplete writable pack through the existing raw-store cleanup boundary.
    pub fn abort(
        mut self,
        store: &SealedResearchJournalStore,
    ) -> Result<(), ProviderLogicalPublicationError> {
        if let Some(pending) = self.pending.take() {
            store.abort_logical_object(pending)?;
        }
        Ok(())
    }
}

impl Drop for PendingProviderCapturePack {
    fn drop(&mut self) {
        if let Some(pending) = self.pending.take() {
            // The store validates retained owner and exact file identity before removal.
            let _cleanup = self.store.abort_logical_object(pending);
        }
    }
}

/// Complete packed-object authority; value claims cannot construct this seal.
#[derive(Debug)]
pub struct ProviderCapturePackSeal {
    first_capture: SealedProviderCaptureSetReceipt,
    captures_digest: EvidenceDigest,
    capture_count: u64,
    body_count: u64,
    source_id: SourceId,
    object: ResearchObjectReceipt,
    logical_ordinal: u32,
}
impl ProviderCapturePackSeal {
    /// Returns the original first capture for indexed custody-session lookup.
    pub const fn first_capture(&self) -> &SealedProviderCaptureSetReceipt {
        &self.first_capture
    }
    /// Returns exact complete original capture-sequence identity.
    pub const fn captures_digest(&self) -> EvidenceDigest {
        self.captures_digest
    }
    /// Returns exact independently sealed capture cardinality.
    pub const fn capture_count(&self) -> u64 {
        self.capture_count
    }
    /// Returns exact response-body cardinality.
    pub const fn body_count(&self) -> u64 {
        self.body_count
    }
    /// Returns the common original source.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }
    /// Returns the complete immutable pack receipt.
    pub const fn object(&self) -> &ResearchObjectReceipt {
        &self.object
    }
    /// Returns its position in the complete logical object graph.
    pub const fn logical_ordinal(&self) -> u32 {
        self.logical_ordinal
    }
}
/// One sealed raw input and the independent original-token closure proof consumed at publication.
#[derive(Debug)]
pub struct SealedProviderCapturePack {
    input: SealedLogicalObjectInput,
    seal: ProviderCapturePackSeal,
}
impl SealedProviderCapturePack {
    /// Transfers the logical raw input and its nonforgeable whole-pack seal.
    pub fn into_parts(self) -> (SealedLogicalObjectInput, ProviderCapturePackSeal) {
        (self.input, self.seal)
    }
}
