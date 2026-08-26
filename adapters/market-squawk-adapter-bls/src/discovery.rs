//! Seal-first discovery admissions for exact BLS response components.

use std::sync::Arc;

use market_squawk_domain::{EvidenceDigest, Timestamp};
use market_squawk_sources::{
    DiscoveryBatch, ExtractionRequest, ProviderCaptureMaterial,
    ProviderCaptureSetReceipt, ProviderCaptureTerminalDisposition,
    SealedProviderCaptureSetReceipt, SourceObject, SourceObjectCaptureIdentity,
};

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
    runtime_instance: Arc<BlsRuntimeInstanceCapability>,
    activation_candidate_digest: EvidenceDigest,
}

impl BlsDiscoveryOutput {
    pub(crate) fn new(
        batch: DiscoveryBatch,
        capture_material: ProviderCaptureMaterial,
        runtime_instance: Arc<BlsRuntimeInstanceCapability>,
        activation_candidate_digest: EvidenceDigest,
    ) -> Self {
        Self {
            batch,
            capture_material,
            runtime_instance,
            activation_candidate_digest,
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

    /// Separates root-owned sealing work from the private pending admission coordinate.
    pub fn into_sealing_parts(self) -> (BlsPendingDiscovery, ProviderCaptureMaterial) {
        let capture_receipt = self.capture_material.receipt().clone();
        (
            BlsPendingDiscovery {
                batch: self.batch,
                capture_receipt,
                runtime_instance: self.runtime_instance,
                activation_candidate_digest: self.activation_candidate_digest,
            },
            self.capture_material,
        )
    }
}

/// Non-cloneable discovery state awaiting the exact physical seal of its raw response graph.
#[derive(Debug)]
pub struct BlsPendingDiscovery {
    batch: DiscoveryBatch,
    capture_receipt: ProviderCaptureSetReceipt,
    runtime_instance: Arc<BlsRuntimeInstanceCapability>,
    activation_candidate_digest: EvidenceDigest,
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
        sealed_capture: SealedProviderCaptureSetReceipt,
        expected_runtime_instance: &Arc<BlsRuntimeInstanceCapability>,
        activation: &BlsActivationCandidate,
    ) -> Result<Self, BlsSourceError> {
        if !Arc::ptr_eq(&pending.runtime_instance, expected_runtime_instance)
            || !Arc::ptr_eq(activation.runtime_instance(), expected_runtime_instance)
            || pending.activation_candidate_digest != activation.candidate_digest()
            || sealed_capture.capture() != &pending.capture_receipt
            || sealed_capture.receipt_digest().bytes() == [0; 32]
        {
            return Err(BlsSourceError::InvalidPublication);
        }

        let capture = sealed_capture.capture();
        let chunk_count = pending.batch.objects().len();
        if chunk_count == 0
            || capture.pages().len() != chunk_count
            || (chunk_count == 1
                && (capture.terminal()
                    != ProviderCaptureTerminalDisposition::StandaloneResponse
                    || !capture.request_graph_components().is_empty()))
            || (chunk_count > 1
                && (capture.terminal()
                    != ProviderCaptureTerminalDisposition::CompleteRequestGraph
                    || capture.request_graph_components().len() != chunk_count))
        {
            return Err(BlsSourceError::InvalidPublication);
        }

        let mut objects = Vec::new();
        objects
            .try_reserve_exact(chunk_count)
            .map_err(|_| BlsSourceError::InvalidPublication)?;
        for (expected_index, object) in pending.batch.objects().iter().enumerate() {
            let (object_index, _) = crate::source::parse_object_id(object.object_id())
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
                || object.discovery_request_id() != pending.batch.request().request_id()
                || object.capture_identity() != expected_capture_identity
                || object.evidence().content_digest() != page.body_digest()
                || object.expected_bytes() != Some(page.body_bytes())
                || object.effective_interval().starts_at() != page.received_at()
                || component_request_identity != page.request_identity()
            {
                return Err(BlsSourceError::InvalidPublication);
            }
            objects.push(BlsDiscoveryObjectAdmission {
                object: object.clone(),
                chunk_index: u16::try_from(expected_index)
                    .map_err(|_| BlsSourceError::InvalidPublication)?,
                component_request_identity,
                component_content_digest,
                component_observation_digest,
                response_received_at: page.received_at(),
                sealed_discovery_capture: sealed_capture.clone(),
                runtime_instance: Arc::clone(expected_runtime_instance),
                activation_candidate_digest: activation.candidate_digest(),
            });
        }
        Ok(Self {
            objects: objects.into_boxed_slice(),
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
    sealed_discovery_capture: SealedProviderCaptureSetReceipt,
    runtime_instance: Arc<BlsRuntimeInstanceCapability>,
    activation_candidate_digest: EvidenceDigest,
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
            || request.deadline() >= activation.expires_at()
            || self.sealed_discovery_capture.receipt_digest().bytes() == [0; 32]
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

    pub(crate) const fn sealed_discovery_capture(
        &self,
    ) -> &SealedProviderCaptureSetReceipt {
        &self.sealed_discovery_capture
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

    pub(crate) const fn activation_candidate_digest(&self) -> EvidenceDigest {
        self.activation_candidate_digest
    }

    pub(crate) fn runtime_instance(&self) -> &Arc<BlsRuntimeInstanceCapability> {
        &self.runtime_instance
    }
}

fn validate_object_component(admission: &BlsDiscoveryObjectAdmission) -> Result<(), BlsSourceError> {
    let capture = admission.sealed_discovery_capture.capture();
    let index = admission.chunk_index();
    let page = capture
        .pages()
        .get(index)
        .ok_or(BlsSourceError::InvalidPublication)?;
    let (request, content, observation) = if capture.terminal()
        == ProviderCaptureTerminalDisposition::CompleteRequestGraph
    {
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
    {
        return Err(BlsSourceError::InvalidPublication);
    }
    Ok(())
}
