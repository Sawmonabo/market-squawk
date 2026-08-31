//! Exact provider-neutral Macro feature-vector construction.

use market_squawk_data::{
    ComponentAdjustmentEvidence, ComponentKind, ComponentScope, ComponentSelector, ComponentValue,
    CorporateActionSensitivity, DatasetManifestRef, FeatureLabelComponentInput,
    FeatureLabelComponentSpec, ObservationFamilyKey,
};
use market_squawk_domain::{
    CalendarDate, DigestAlgorithm, EvidenceDigest, ResearchTemporalCoordinate, SourceIdentifier,
    Timestamp, feature_dataset_macro_components_v1,
};
use market_squawk_services::ServiceError;

use super::macro_context::{MacroContextReadCapability, MacroContextSnapshot};

/// Reads and maps one exact provider-neutral Macro feature vector at caller-supplied cutoffs.
///
/// The application supplies both time coordinates. This seam never derives a civil date from an
/// instant and never substitutes wall time for a research cutoff.
pub(crate) async fn read_macro_feature_vector(
    read: &MacroContextReadCapability,
    knowledge_cutoff: Timestamp,
    effective_date_cutoff: CalendarDate,
    deadline: std::time::Instant,
    cancellation: tokio_util::sync::CancellationToken,
) -> Result<MacroFeatureVector, ServiceError> {
    let snapshot = read
        .read_latest_known(
            knowledge_cutoff,
            effective_date_cutoff,
            deadline,
            cancellation,
        )
        .await?;
    MacroFeatureVector::try_from_snapshot(&snapshot)
}

/// One exact V1 Macro vector derived from a single neutral point-in-time snapshot.
///
/// Provider identity remains inside the component selectors and opaque evidence. Consumers use
/// only the code-owned economic order, values, units, cutoffs, and immutable parent set.
#[derive(Clone, Debug)]
pub(crate) struct MacroFeatureVector {
    components: Box<[FeatureLabelComponentInput]>,
    knowledge_cutoff: Timestamp,
    effective_date_cutoff: CalendarDate,
    parent_manifests: Box<[DatasetManifestRef]>,
    evidence_digest: EvidenceDigest,
}

impl MacroFeatureVector {
    /// Maps one complete neutral snapshot into the single code-owned V1 economic order.
    pub(crate) fn try_from_snapshot(snapshot: &MacroContextSnapshot) -> Result<Self, ServiceError> {
        let evidence = snapshot.evidence();
        let evidence_digest = evidence.consumed_digest();
        if evidence_digest.algorithm() != DigestAlgorithm::Sha256
            || evidence_digest.bytes() == [0; 32]
        {
            return Err(ServiceError::InvalidResult);
        }

        let descriptors = feature_dataset_macro_components_v1();
        if snapshot.selected().len() != descriptors.len() {
            return Err(ServiceError::InvalidResult);
        }
        let mut retained = vec![false; snapshot.selected().len()];
        let mut components = Vec::new();
        components
            .try_reserve_exact(descriptors.len())
            .map_err(|_| ServiceError::ResourceExhausted)?;

        for (position, descriptor) in descriptors.iter().copied().enumerate() {
            if usize::from(descriptor.position()) != position {
                return Err(ServiceError::InvalidResult);
            }
            let mut matches = snapshot
                .selected()
                .iter()
                .enumerate()
                .filter(|(_, selected)| selected.indicator_id() == descriptor.indicator_id());
            let (selected_index, selected) = matches.next().ok_or(ServiceError::InvalidResult)?;
            if matches.next().is_some() || retained[selected_index] {
                return Err(ServiceError::InvalidResult);
            }
            retained[selected_index] = true;

            let observation = selected.observation().ok_or(ServiceError::Unavailable)?;
            let value = match (
                observation.value().observed_value(),
                observation.value().missing_value(),
            ) {
                (Some(value), None) => value,
                (None, Some(_)) => return Err(ServiceError::Unavailable),
                (Some(_), Some(_)) | (None, None) => return Err(ServiceError::InvalidResult),
            };
            let context = observation.context();
            let provenance = context.provenance();
            let effective = context.time().effective().clone();
            if effective.source_period_value().is_some()
                || provenance.instrument_id().is_some()
                || provenance.venue_id().is_some()
                || provenance.received_at() > evidence.knowledge_cutoff()
                || provenance.ingested_at() > evidence.knowledge_cutoff()
                || provenance
                    .availability()
                    .conservative_available_at()
                    .is_none_or(|available| available > evidence.knowledge_cutoff())
            {
                return Err(ServiceError::InvalidResult);
            }
            let specification = FeatureLabelComponentSpec::try_new(
                ComponentKind::Feature,
                ComponentScope::Global,
                CorporateActionSensitivity::NotApplicable,
                descriptor.component_name(),
                std::num::NonZeroU32::MIN,
            )
            .map_err(|_| ServiceError::InvalidResult)?;
            let unit = SourceIdentifier::try_from(descriptor.unit())
                .map_err(|_| ServiceError::InvalidResult)?;
            components.push(
                FeatureLabelComponentInput::try_new(
                    specification,
                    ComponentValue::decimal(value, Some(unit), None)
                        .map_err(|_| ServiceError::InvalidResult)?,
                    vec![ComponentSelector::new(ObservationFamilyKey::Macro {
                        source_id: provenance.source_id().clone(),
                        series: observation.series().clone(),
                        effective: effective.clone(),
                    })],
                    effective,
                    None,
                    ComponentAdjustmentEvidence::NotApplicable,
                )
                .map_err(|_| ServiceError::InvalidResult)?,
            );
        }
        if retained.iter().any(|retained| !retained) {
            return Err(ServiceError::InvalidResult);
        }
        if evidence.consumed_parent_manifests().is_empty() {
            return Err(ServiceError::InvalidResult);
        }

        Ok(Self {
            components: components.into_boxed_slice(),
            knowledge_cutoff: evidence.knowledge_cutoff(),
            effective_date_cutoff: evidence.effective_date_cutoff(),
            parent_manifests: evidence
                .consumed_parent_manifests()
                .to_vec()
                .into_boxed_slice(),
            evidence_digest,
        })
    }

    /// Returns label-free global components in the domain-owned economic order.
    pub(crate) fn components(&self) -> &[FeatureLabelComponentInput] {
        &self.components
    }

    /// Returns the exact knowledge-time cutoff used by every selection.
    pub(crate) const fn knowledge_cutoff(&self) -> Timestamp {
        self.knowledge_cutoff
    }

    /// Returns the requested provider-neutral effective-date selection boundary.
    pub(crate) const fn effective_date_cutoff(&self) -> CalendarDate {
        self.effective_date_cutoff
    }

    /// Returns the canonical duplicate-free parent generation set.
    pub(crate) fn parent_manifests(&self) -> &[DatasetManifestRef] {
        &self.parent_manifests
    }

    /// Returns the opaque exact identity of the neutral snapshot and its selections.
    pub(crate) const fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest
    }

    /// Returns exact native source-effective component cutoffs in economic vector order.
    pub(crate) fn component_cutoffs(&self) -> impl Iterator<Item = &ResearchTemporalCoordinate> {
        self.components
            .iter()
            .map(FeatureLabelComponentInput::selection_effective_cutoff)
    }
}
