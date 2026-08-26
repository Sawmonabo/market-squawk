//! Seal-first discovery admissions for exact BLS response components.

use std::sync::Arc;

use market_squawk_domain::{EvidenceDigest, Timestamp};
use market_squawk_sources::{
    DiscoveryBatch, ExtractionBatch, ExtractionRequest, ProviderCaptureComponentToken,
    ProviderCaptureMaterial, ProviderCaptureSealExpectation, ProviderCaptureSealRequest,
    ProviderCaptureTerminalDisposition, ProviderNativeLineageBatch, ProviderWholeCaptureToken,
    SealedProviderCaptureBinding, SealedProviderCaptureMaterial, SealedProviderCaptureSetReceipt,
    SourceObject, SourceObjectCaptureIdentity, payload_matches_exact_evidence,
};

use crate::client::RetrievedBlsPage;
use crate::contract::BlsRuntimeInstanceCapability;
use crate::{BlsActivationCandidate, BlsSourceError};

/// Discovery batch paired with every exact response needed to construct it.
///
/// Root must consume this value with [`Self::into_sealing_parts`], physically seal the raw
/// material, then return the pending half through `BlsSource::admit_sealed_discovery`. No source
/// object can enter extraction before that exact seal is joined.
#[derive(Debug)]
pub struct BlsDiscoveryOutput {
    batch: DiscoveryBatch,
    capture_material: ProviderCaptureMaterial,
    retained_pages: Box<[RetrievedBlsPage]>,
    source_generation_digest: EvidenceDigest,
}

impl BlsDiscoveryOutput {
    pub(crate) fn new(
        batch: DiscoveryBatch,
        capture_material: ProviderCaptureMaterial,
        retained_pages: Vec<RetrievedBlsPage>,
        source_generation_digest: EvidenceDigest,
    ) -> Self {
        Self {
            batch,
            capture_material,
            retained_pages: retained_pages.into_boxed_slice(),
            source_generation_digest,
        }
    }

    /// Returns the exact source-object discovery batch before its required physical seal.
    pub const fn batch(&self) -> &DiscoveryBatch {
        &self.batch
    }

    /// Returns every bounded official response ready for application-owned physical sealing.
    pub const fn capture_material(&self) -> &ProviderCaptureMaterial {
        &self.capture_material
    }

    pub(crate) const fn source_generation_digest(&self) -> EvidenceDigest {
        self.source_generation_digest
    }

    pub(crate) fn retained_pages(&self) -> &[RetrievedBlsPage] {
        &self.retained_pages
    }

    /// Separates root-owned sealing work from the private pending admission coordinate.
    pub fn into_sealing_parts(
        self,
    ) -> Result<(BlsPendingDiscovery, ProviderCaptureSealRequest), BlsSourceError> {
        let component_scoped = self.batch.objects().len() > 1;
        let (capture_expectation, seal_request) = if component_scoped {
            self.capture_material
                .into_component_seal_parts()
                .map_err(|_| BlsSourceError::InvalidPublication)?
        } else {
            self.capture_material.into_whole_seal_parts()
        };
        Ok((
            BlsPendingDiscovery {
                batch: self.batch,
                capture_expectation,
                component_scoped,
                retained_pages: self.retained_pages,
                source_generation_digest: self.source_generation_digest,
            },
            seal_request,
        ))
    }
}

/// Non-cloneable discovery state awaiting the exact physical seal of its raw response graph.
#[derive(Debug)]
pub struct BlsPendingDiscovery {
    batch: DiscoveryBatch,
    capture_expectation: ProviderCaptureSealExpectation,
    component_scoped: bool,
    retained_pages: Box<[RetrievedBlsPage]>,
    source_generation_digest: EvidenceDigest,
}

#[derive(Debug)]
pub(crate) enum BlsDiscoveryCaptureToken {
    Whole(ProviderWholeCaptureToken),
    Component(ProviderCaptureComponentToken),
}

impl BlsDiscoveryCaptureToken {
    fn persisted_receipt(&self) -> &SealedProviderCaptureSetReceipt {
        match self {
            Self::Whole(token) => token.persisted_receipt(),
            Self::Component(token) => token.persisted_receipt(),
        }
    }

    fn component_ordinal(&self) -> Option<u16> {
        match self {
            Self::Whole(_) => None,
            Self::Component(token) => Some(token.ordinal()),
        }
    }

    pub(crate) fn try_bind(
        self,
        batch: ExtractionBatch,
        native_lineage: ProviderNativeLineageBatch,
        row_capture_page_ordinals: Vec<u16>,
    ) -> Result<SealedProviderCaptureBinding, BlsSourceError> {
        match self {
            Self::Whole(token) => SealedProviderCaptureBinding::try_whole(
                token,
                batch,
                native_lineage,
                row_capture_page_ordinals,
            ),
            Self::Component(token) => SealedProviderCaptureBinding::try_component(
                token,
                batch,
                native_lineage,
                row_capture_page_ordinals,
            ),
        }
        .map_err(|_| BlsSourceError::InvalidPublication)
    }
}

#[derive(Debug)]
struct RetainedDiscoverySelections {
    admissions: Vec<BlsDiscoveryObjectAdmission>,
}

impl RetainedDiscoverySelections {
    fn try_new(
        admissions: Vec<BlsDiscoveryObjectAdmission>,
        expected: usize,
    ) -> Result<Self, BlsSourceError> {
        if admissions.len() != expected
            || admissions
                .iter()
                .enumerate()
                .any(|(ordinal, admission)| admission.chunk_index() != ordinal)
        {
            return Err(BlsSourceError::InvalidPublication);
        }
        Ok(Self { admissions })
    }

    fn into_admissions(self) -> Box<[BlsDiscoveryObjectAdmission]> {
        self.admissions.into_boxed_slice()
    }
}

impl BlsPendingDiscovery {
    /// Returns the unpublishable discovery batch only for root scheduling inspection.
    pub const fn batch(&self) -> &DiscoveryBatch {
        &self.batch
    }
}

/// Physically sealed discovery split into one non-reusable extraction admission per object.
#[derive(Debug)]
pub struct BlsDiscoveryAdmission {
    objects: Box<[BlsDiscoveryObjectAdmission]>,
}

impl BlsDiscoveryAdmission {
    pub(crate) fn try_new(
        pending: BlsPendingDiscovery,
        sealed_capture: SealedProviderCaptureMaterial,
        expected_runtime_instance: &Arc<BlsRuntimeInstanceCapability>,
        activation: &BlsActivationCandidate,
    ) -> Result<Self, BlsSourceError> {
        let BlsPendingDiscovery {
            batch,
            capture_expectation,
            component_scoped,
            retained_pages,
            source_generation_digest,
        } = pending;
        let rejoined = capture_expectation
            .try_rejoin(sealed_capture)
            .map_err(|_| BlsSourceError::InvalidPublication)?;
        let capture_tokens = if component_scoped {
            let tokens = rejoined
                .try_into_components()
                .map_err(|_| BlsSourceError::InvalidPublication)?
                .into_tokens();
            let mut capture_tokens = Vec::new();
            capture_tokens
                .try_reserve_exact(tokens.len())
                .map_err(|_| BlsSourceError::InvalidPublication)?;
            capture_tokens.extend(
                tokens
                    .into_vec()
                    .into_iter()
                    .map(BlsDiscoveryCaptureToken::Component),
            );
            capture_tokens
        } else {
            let mut capture_tokens = Vec::new();
            capture_tokens
                .try_reserve_exact(1)
                .map_err(|_| BlsSourceError::InvalidPublication)?;
            capture_tokens.push(BlsDiscoveryCaptureToken::Whole(
                rejoined
                    .try_into_whole()
                    .map_err(|_| BlsSourceError::InvalidPublication)?,
            ));
            capture_tokens
        };
        if !Arc::ptr_eq(activation.runtime_instance(), expected_runtime_instance)
            || source_generation_digest != activation.plan().plan_digest()
        {
            return Err(BlsSourceError::InvalidPublication);
        }

        let chunk_count = batch.objects().len();
        let root_receipt = capture_tokens
            .first()
            .map(BlsDiscoveryCaptureToken::persisted_receipt)
            .ok_or(BlsSourceError::InvalidPublication)?;
        let root_receipt_digest = root_receipt.receipt_digest();
        let capture = root_receipt.capture();
        if chunk_count == 0
            || retained_pages.len() != chunk_count
            || capture_tokens.len() != chunk_count
            || capture.pages().len() != chunk_count
            || (chunk_count == 1
                && (capture.terminal() != ProviderCaptureTerminalDisposition::StandaloneResponse
                    || !capture.request_graph_components().is_empty()))
            || (chunk_count > 1
                && (capture.terminal() != ProviderCaptureTerminalDisposition::CompleteRequestGraph
                    || capture.request_graph_components().len() != chunk_count))
        {
            return Err(BlsSourceError::InvalidPublication);
        }

        let mut selections = Vec::new();
        selections
            .try_reserve_exact(chunk_count)
            .map_err(|_| BlsSourceError::InvalidPublication)?;
        for (expected_index, ((object, retained_page), capture_token)) in batch
            .objects()
            .iter()
            .zip(retained_pages.into_vec())
            .zip(capture_tokens)
            .enumerate()
        {
            let sealed_capture = capture_token.persisted_receipt();
            let capture = sealed_capture.capture();
            let (object_index, object_digest) = crate::source::parse_object_id(object.object_id())
                .map_err(|_| BlsSourceError::InvalidPublication)?;
            let page = capture
                .pages()
                .get(expected_index)
                .ok_or(BlsSourceError::InvalidPublication)?;
            let (
                component_request_identity,
                component_content_digest,
                component_observation_digest,
                component_terminal,
            ) = if chunk_count == 1 {
                (
                    capture.request_set_identity(),
                    capture.content_digest(),
                    capture.observation_digest(),
                    capture.terminal(),
                )
            } else {
                let component = capture
                    .request_graph_components()
                    .get(expected_index)
                    .ok_or(BlsSourceError::InvalidPublication)?;
                if usize::from(component.ordinal()) != expected_index
                    || usize::from(component.first_page_ordinal()) != expected_index
                    || component.page_count().get() != 1
                    || component.total_body_bytes() != page.body_bytes()
                {
                    return Err(BlsSourceError::InvalidPublication);
                }
                (
                    component.request_set_identity(),
                    component.content_digest(),
                    component.observation_digest(),
                    component.terminal(),
                )
            };
            let expected_capture_identity = SourceObjectCaptureIdentity::Paged {
                content_digest: component_content_digest,
                page_count: std::num::NonZeroU16::new(1)
                    .ok_or(BlsSourceError::InvalidPublication)?,
                terminal: component_terminal,
            };
            if object_index != expected_index
                || capture_token.component_ordinal()
                    != component_scoped.then_some(
                        u16::try_from(expected_index)
                            .map_err(|_| BlsSourceError::InvalidPublication)?,
                    )
                || sealed_capture.receipt_digest() != root_receipt_digest
                || object.discovery_request_id() != batch.request().request_id()
                || object.capture_identity() != expected_capture_identity
                || object.evidence().content_digest() != page.body_digest()
                || object.expected_bytes() != Some(page.body_bytes())
                || object.effective_interval().starts_at() != page.received_at()
                || component_request_identity != page.request_identity()
                || retained_page.received_at != page.received_at()
                || retained_page.sha256_hex != object_digest
                || u64::try_from(retained_page.bytes.len()).ok() != Some(page.body_bytes())
                || !payload_matches_exact_evidence(&retained_page.bytes, object.evidence())
            {
                return Err(BlsSourceError::InvalidPublication);
            }
            selections.push(BlsDiscoveryObjectAdmission {
                object: object.clone(),
                chunk_index: u16::try_from(expected_index)
                    .map_err(|_| BlsSourceError::InvalidPublication)?,
                component_request_identity,
                component_content_digest,
                component_observation_digest,
                response_received_at: page.received_at(),
                capture_token,
                retained_page,
                runtime_instance: Arc::clone(expected_runtime_instance),
                activation_candidate_digest: activation.candidate_digest(),
                source_generation_digest,
            });
        }
        let retained = RetainedDiscoverySelections::try_new(selections, chunk_count)?;
        Ok(Self {
            objects: retained.into_admissions(),
        })
    }

    /// Returns the exact number of one-shot object admissions.
    pub fn len(&self) -> usize {
        self.objects.len()
    }

    /// Consumes the graph admission into one non-reusable capability per captured object.
    pub fn into_objects(self) -> Box<[BlsDiscoveryObjectAdmission]> {
        self.objects
    }
}

/// One non-cloneable, physically sealed BLS object admitted for exactly one extraction attempt.
#[derive(Debug)]
pub struct BlsDiscoveryObjectAdmission {
    object: SourceObject,
    chunk_index: u16,
    component_request_identity: EvidenceDigest,
    component_content_digest: EvidenceDigest,
    component_observation_digest: EvidenceDigest,
    response_received_at: Timestamp,
    capture_token: BlsDiscoveryCaptureToken,
    retained_page: RetrievedBlsPage,
    runtime_instance: Arc<BlsRuntimeInstanceCapability>,
    activation_candidate_digest: EvidenceDigest,
    source_generation_digest: EvidenceDigest,
}

impl BlsDiscoveryObjectAdmission {
    /// Returns the exact object root must use to construct the bounded extraction request.
    pub const fn object(&self) -> &SourceObject {
        &self.object
    }

    pub(crate) fn validate_for_extraction(
        &self,
        request: &ExtractionRequest,
        expected_runtime_instance: &Arc<BlsRuntimeInstanceCapability>,
        activation: &BlsActivationCandidate,
    ) -> Result<(), BlsSourceError> {
        if request.object() != &self.object
            || !Arc::ptr_eq(&self.runtime_instance, expected_runtime_instance)
            || !Arc::ptr_eq(activation.runtime_instance(), expected_runtime_instance)
            || self.activation_candidate_digest != activation.candidate_digest()
            || self.source_generation_digest != activation.plan().plan_digest()
            || request.deadline() >= activation.expires_at()
            || self
                .capture_token
                .persisted_receipt()
                .receipt_digest()
                .bytes()
                == [0; 32]
        {
            return Err(BlsSourceError::InvalidPublication);
        }
        validate_object_component(self)
    }

    pub(crate) const fn chunk_index(&self) -> usize {
        self.chunk_index as usize
    }

    pub(crate) const fn response_received_at(&self) -> Timestamp {
        self.response_received_at
    }

    pub(crate) fn sealed_discovery_capture(&self) -> &SealedProviderCaptureSetReceipt {
        self.capture_token.persisted_receipt()
    }

    pub(crate) const fn retained_page(&self) -> &RetrievedBlsPage {
        &self.retained_page
    }

    pub(crate) const fn component_request_identity(&self) -> EvidenceDigest {
        self.component_request_identity
    }

    pub(crate) const fn component_content_digest(&self) -> EvidenceDigest {
        self.component_content_digest
    }

    pub(crate) const fn component_observation_digest(&self) -> EvidenceDigest {
        self.component_observation_digest
    }

    pub(crate) const fn source_generation_digest(&self) -> EvidenceDigest {
        self.source_generation_digest
    }

    pub(crate) const fn activation_candidate_digest(&self) -> EvidenceDigest {
        self.activation_candidate_digest
    }

    pub(crate) fn runtime_instance(&self) -> &Arc<BlsRuntimeInstanceCapability> {
        &self.runtime_instance
    }

    pub(crate) fn into_capture_token(self) -> BlsDiscoveryCaptureToken {
        self.capture_token
    }
}

fn validate_object_component(
    admission: &BlsDiscoveryObjectAdmission,
) -> Result<(), BlsSourceError> {
    let capture = admission.capture_token.persisted_receipt().capture();
    let index = admission.chunk_index();
    let page = capture
        .pages()
        .get(index)
        .ok_or(BlsSourceError::InvalidPublication)?;
    let (request, content, observation) =
        if capture.terminal() == ProviderCaptureTerminalDisposition::CompleteRequestGraph {
            let component = capture
                .request_graph_components()
                .get(index)
                .ok_or(BlsSourceError::InvalidPublication)?;
            (
                component.request_set_identity(),
                component.content_digest(),
                component.observation_digest(),
            )
        } else {
            (
                capture.request_set_identity(),
                capture.content_digest(),
                capture.observation_digest(),
            )
        };
    if request != admission.component_request_identity
        || content != admission.component_content_digest
        || observation != admission.component_observation_digest
        || page.request_identity() != admission.component_request_identity
        || page.body_digest() != admission.object.evidence().content_digest()
        || page.received_at() != admission.response_received_at
        || admission.object.effective_interval().starts_at() != admission.response_received_at
        || admission.retained_page.received_at != admission.response_received_at
        || admission.retained_page.sha256_hex
            != admission
                .object
                .object_id()
                .as_str()
                .rsplit(':')
                .next()
                .ok_or(BlsSourceError::InvalidPublication)?
        || u64::try_from(admission.retained_page.bytes.len()).ok()
            != admission.object.expected_bytes()
        || !payload_matches_exact_evidence(
            &admission.retained_page.bytes,
            admission.object.evidence(),
        )
    {
        return Err(BlsSourceError::InvalidPublication);
    }
    Ok(())
}
