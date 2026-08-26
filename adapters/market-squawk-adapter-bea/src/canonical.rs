//! Closed BEA mapping into canonical point-in-time macro observations.

use std::collections::BTreeMap;
use std::num::{NonZeroU16, NonZeroU32};

use bytes::Bytes;
use market_squawk_domain::{
    AvailabilityEvidence, DataQuality, DigestAlgorithm, EvidenceDigest, MacroMissingValue,
    MacroObservation, PayloadHash, PayloadReference, ResearchContext, ResearchObservation,
    ResearchPeriod, ResearchProvenance, ResearchProvenanceInput, ResearchTemporalCoordinate,
    ResearchTime, RevisionNumber, SourceId, SourceIdentifier, Timestamp,
};
use rust_decimal::Decimal;
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::{
    BeaCompleteness, BeaDataEvidencePage, BeaDoctorAdmissionEvidence, BeaFrequency,
    BeaMissingValue, BeaObservation, BeaObservationValue,
    BeaSealedAcquisitionReceipt, BeaSourceBinding,
};

/// Canonical BEA mapping or evidence invariant failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum BeaCanonicalError {
    /// Source/config/doctor/rights/raw-seal evidence did not match.
    #[error("invalid BEA canonicalization authority")]
    InvalidAuthority,
    /// Provider and local clocks cannot establish conservative point-in-time ordering.
    #[error("invalid BEA canonicalization clock")]
    InvalidClock,
    /// The selected row, period, identity, or completeness state is invalid.
    #[error("invalid BEA canonical observation")]
    InvalidObservation,
    /// `value × 10^UNIT_MULT` cannot be represented exactly as a canonical decimal.
    #[error("BEA unit multiplier cannot be represented exactly")]
    InvalidScale,
    /// Canonical encoding or bounded allocation failed.
    #[error("BEA canonical payload could not be encoded")]
    Encoding,
}

/// Candidate-only coordinates supplied after doctor and acquisition raw graphs are sealed.
///
/// This context cannot mint durable revision, publication, restart, or query authority. Canonical
/// rows carry revision one only as the shared revision authority's required input shape; root
/// composition must replace it with its durable assignment before immutable publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeaCanonicalContext {
    source_id: SourceId,
    provider_dataset_id: SourceIdentifier,
    analytical_dataset_id: SourceIdentifier,
    source_binding_digest: EvidenceDigest,
    doctor_admission_digest: EvidenceDigest,
    doctor_sealed_graph_digest: EvidenceDigest,
    rights_policy_digest: EvidenceDigest,
    root_rights_decision_digest: EvidenceDigest,
    rights_rejoin_digest: EvidenceDigest,
    raw_seal_digest: EvidenceDigest,
    ingested_at: Timestamp,
}

impl BeaCanonicalContext {
    /// Constructs candidate mapping context from exact doctor admission, use, and acquisition seal.
    pub(crate) fn try_new(
        binding: &BeaSourceBinding,
        doctor: &BeaDoctorAdmissionEvidence,
        sealed_acquisition: &BeaSealedAcquisitionReceipt,
        ingested_at: Timestamp,
    ) -> Result<Self, BeaCanonicalError> {
        doctor
            .validate_current(
                binding,
                sealed_acquisition.dataset_id(),
                doctor.analytical_dataset_id(),
                ingested_at,
            )
            .map_err(|_| BeaCanonicalError::InvalidAuthority)?;
        if sealed_acquisition.source_id() != binding.source_id()
            || sealed_acquisition.metadata_revision() != binding.metadata_revision()
            || doctor.dataset_id() != sealed_acquisition.dataset_id()
            || doctor.source_binding_digest() != binding.binding_digest()
            || doctor.rights_policy_digest() != binding.rights_policy_digest()
            || doctor.root_rights_decision_digest() != binding.root_rights_decision_digest()
            || doctor.rights_rejoin_digest() != binding.rights_rejoin_digest()
        {
            return Err(BeaCanonicalError::InvalidAuthority);
        }
        Ok(Self {
            source_id: binding.source_id().clone(),
            provider_dataset_id: sealed_acquisition.dataset_id().clone(),
            analytical_dataset_id: doctor.analytical_dataset_id().clone(),
            source_binding_digest: binding.binding_digest(),
            doctor_admission_digest: doctor.admission_digest(),
            doctor_sealed_graph_digest: doctor.doctor_sealed_graph_digest(),
            rights_policy_digest: binding.rights_policy_digest(),
            root_rights_decision_digest: binding.root_rights_decision_digest(),
            rights_rejoin_digest: binding.rights_rejoin_digest(),
            raw_seal_digest: sealed_acquisition.sealed_graph_digest(),
            ingested_at,
        })
    }

    /// Returns the configured provider-query identity.
    pub const fn provider_dataset_id(&self) -> &SourceIdentifier {
        &self.provider_dataset_id
    }
    /// Returns the canonical analytical dataset identity.
    pub const fn analytical_dataset_id(&self) -> &SourceIdentifier {
        &self.analytical_dataset_id
    }
    /// Returns the exact sealed raw-record commitment.
    pub const fn raw_seal_digest(&self) -> EvidenceDigest {
        self.raw_seal_digest
    }
    /// Returns the candidate's local canonicalization instant.
    pub const fn ingested_at(&self) -> Timestamp {
        self.ingested_at
    }
}

/// Canonical macro observation plus all BEA-native and authority lineage required at publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeaCanonicalObservation {
    observation: MacroObservation,
    canonical_payload: Bytes,
    canonical_payload_digest: EvidenceDigest,
    native_row_digest: EvidenceDigest,
    native_series_digest: EvidenceDigest,
    metadata_generation: EvidenceDigest,
    raw_page_digest: EvidenceDigest,
    raw_seal_digest: EvidenceDigest,
    source_binding_digest: EvidenceDigest,
    doctor_admission_digest: EvidenceDigest,
    doctor_sealed_graph_digest: EvidenceDigest,
    rights_policy_digest: EvidenceDigest,
    root_rights_decision_digest: EvidenceDigest,
    rights_rejoin_digest: EvidenceDigest,
}

impl BeaCanonicalObservation {
    /// Maps one exact provider row without inventing a release publication clock.
    fn try_from_captured(
        captured: &BeaDataEvidencePage,
        observation_index: usize,
        context: &BeaCanonicalContext,
    ) -> Result<Self, BeaCanonicalError> {
        if captured.page().receipt().completeness() == BeaCompleteness::Partial {
            return Err(BeaCanonicalError::InvalidObservation);
        }
        let native = captured
            .page()
            .observations()
            .get(observation_index)
            .ok_or(BeaCanonicalError::InvalidObservation)?;
        let capture = captured.capture();
        let capture_page = capture
            .pages()
            .first()
            .filter(|_| capture.pages().len() == 1)
            .ok_or(BeaCanonicalError::InvalidObservation)?;
        let received_at = capture_page.received_at();
        if received_at > context.ingested_at
            || captured
                .page()
                .production_time()
                .is_some_and(|production| production.timestamp() > received_at)
        {
            return Err(BeaCanonicalError::InvalidClock);
        }
        let raw_page_digest = capture_page.body_digest();
        if raw_page_digest.algorithm() != DigestAlgorithm::Sha256
            || raw_page_digest.bytes() != captured.page().receipt().response_digest()
        {
            return Err(BeaCanonicalError::InvalidObservation);
        }
        let native_series_digest = canonical_series_digest(native)?;
        let series = identifier_from_digest("bea-series", native_series_digest)?;
        let unit_digest = unit_digest(native);
        let unit = identifier_from_digest("bea-unit", unit_digest)?;
        let source_identifier = SourceIdentifier::try_from(format!(
            "bea-row:{}:{}",
            lower_hex(native_series_digest.bytes()),
            lower_hex(native.digest())
        ))
        .map_err(|_| BeaCanonicalError::Encoding)?;
        let provenance = ResearchProvenance::try_new(ResearchProvenanceInput {
            source_id: context.source_id.clone(),
            instrument_id: None,
            venue_id: None,
            source_identifier,
            // `UTCProductionTime` is response production, not an official release timestamp.
            source_timestamp: captured
                .page()
                .production_time()
                .map(|production| production.timestamp()),
            received_at,
            ingested_at: context.ingested_at,
            quality: DataQuality::OfficialDelayed,
            payload_reference: PayloadReference::ContentHash(PayloadHash::new(
                DigestAlgorithm::Sha256,
                raw_page_digest.bytes(),
            )),
            availability: AvailabilityEvidence::local_first_observed(received_at),
        })
        .map_err(|_| BeaCanonicalError::InvalidClock)?;
        let time = ResearchTime::try_new_with_coordinates(
            effective_coordinate(native)?,
            None,
            RevisionNumber::new(1).map_err(|_| BeaCanonicalError::InvalidObservation)?,
            None,
        )
        .map_err(|_| BeaCanonicalError::InvalidClock)?;
        let research_context =
            ResearchContext::new(provenance, time).map_err(|_| BeaCanonicalError::InvalidClock)?;
        let observation = match native.value() {
            BeaObservationValue::Observed { value, .. } => MacroObservation::new(
                research_context,
                series,
                scale_value_exact(*value, native.unit().unit_multiplier())?,
                unit,
            ),
            BeaObservationValue::Missing(missing) => MacroObservation::missing(
                research_context,
                series,
                MacroMissingValue::new(
                    SourceIdentifier::try_from(match missing {
                        BeaMissingValue::Absent => "bea-absent",
                        BeaMissingValue::Blank => "bea-blank",
                        BeaMissingValue::SuppressedRegional => "bea-regional-suppression-l",
                    })
                    .map_err(|_| BeaCanonicalError::Encoding)?,
                    Some(
                        SourceIdentifier::try_from(match missing {
                            BeaMissingValue::Absent => "value-dimension-absent-or-null",
                            BeaMissingValue::Blank => "provider-empty-lexical-value",
                            BeaMissingValue::SuppressedRegional => {
                                crate::BEA_REGIONAL_SUPPRESSION_REASON
                            }
                        })
                        .map_err(|_| BeaCanonicalError::Encoding)?,
                    ),
                ),
                unit,
            ),
        };
        let canonical_payload = serde_json::to_vec(&ResearchObservation::Macro(observation.clone()))
            .map(Bytes::from)
            .map_err(|_| BeaCanonicalError::Encoding)?;
        let canonical_payload_digest = digest_bytes(&canonical_payload);
        Ok(Self {
            observation,
            canonical_payload,
            canonical_payload_digest,
            native_row_digest: digest(native.digest()),
            native_series_digest,
            metadata_generation: digest(captured.page().metadata_generation().digest()),
            raw_page_digest,
            raw_seal_digest: context.raw_seal_digest,
            source_binding_digest: context.source_binding_digest,
            doctor_admission_digest: context.doctor_admission_digest,
            doctor_sealed_graph_digest: context.doctor_sealed_graph_digest,
            rights_policy_digest: context.rights_policy_digest,
            root_rights_decision_digest: context.root_rights_decision_digest,
            rights_rejoin_digest: context.rights_rejoin_digest,
        })
    }

    /// Returns the canonical point-in-time macro observation.
    pub const fn observation(&self) -> &MacroObservation {
        &self.observation
    }
    /// Returns exact canonical JSON bytes for shared immutable publication.
    pub const fn canonical_payload(&self) -> &Bytes {
        &self.canonical_payload
    }
    /// Returns the canonical-payload commitment.
    pub const fn canonical_payload_digest(&self) -> EvidenceDigest {
        self.canonical_payload_digest
    }
    /// Returns the complete provider-native row commitment.
    pub const fn native_row_digest(&self) -> EvidenceDigest {
        self.native_row_digest
    }
    /// Returns the period-independent provider-native series commitment.
    pub const fn native_series_digest(&self) -> EvidenceDigest {
        self.native_series_digest
    }
    /// Returns the exact metadata generation used to build `GetData`.
    pub const fn metadata_generation(&self) -> EvidenceDigest {
        self.metadata_generation
    }
    /// Returns the exact raw response-body commitment.
    pub const fn raw_page_digest(&self) -> EvidenceDigest {
        self.raw_page_digest
    }
    /// Returns the exact shared raw-seal commitment.
    pub const fn raw_seal_digest(&self) -> EvidenceDigest {
        self.raw_seal_digest
    }
    /// Returns the exact source/config/credential/rights/quota binding.
    pub const fn source_binding_digest(&self) -> EvidenceDigest {
        self.source_binding_digest
    }
    /// Returns the actual-seal-bound in-process doctor admission.
    pub const fn doctor_admission_digest(&self) -> EvidenceDigest {
        self.doctor_admission_digest
    }
    /// Returns the actual shared raw graph that admitted doctor.
    pub const fn doctor_sealed_graph_digest(&self) -> EvidenceDigest {
        self.doctor_sealed_graph_digest
    }
    /// Returns the fixed private-use/no-sale policy commitment.
    pub const fn rights_policy_digest(&self) -> EvidenceDigest {
        self.rights_policy_digest
    }
    /// Returns the non-authoritative root decision coordinate.
    pub const fn root_rights_decision_digest(&self) -> EvidenceDigest {
        self.root_rights_decision_digest
    }
    /// Returns the policy/root-decision rejoin commitment.
    pub const fn rights_rejoin_digest(&self) -> EvidenceDigest {
        self.rights_rejoin_digest
    }
}

/// One bounded canonical BEA dataset batch ready for a shared immutable publisher.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeaCanonicalBatch {
    source_id: SourceId,
    provider_dataset_id: SourceIdentifier,
    analytical_dataset_id: SourceIdentifier,
    observations: Vec<BeaCanonicalObservation>,
    batch_digest: EvidenceDigest,
}

impl BeaCanonicalBatch {
    /// Canonicalizes every row from the exact acquisition physically bound into `context`.
    pub(crate) fn try_from_sealed(
        sealed_acquisition: &BeaSealedAcquisitionReceipt,
        context: &BeaCanonicalContext,
        maximum_records: NonZeroU32,
    ) -> Result<Self, BeaCanonicalError> {
        if sealed_acquisition.sealed_graph_digest() != context.raw_seal_digest
            || sealed_acquisition.source_id() != &context.source_id
            || sealed_acquisition.dataset_id() != &context.provider_dataset_id
        {
            return Err(BeaCanonicalError::InvalidAuthority);
        }
        let captured = sealed_acquisition.evidence().data();
        if captured.page().observations().is_empty()
            || captured.page().observations().len() > maximum_records.get() as usize
        {
            return Err(BeaCanonicalError::InvalidObservation);
        }
        let mut observations = Vec::new();
        observations
            .try_reserve_exact(captured.page().observations().len())
            .map_err(|_| BeaCanonicalError::Encoding)?;
        for index in 0..captured.page().observations().len() {
            observations.push(BeaCanonicalObservation::try_from_captured(
                captured, index, context,
            )?);
        }
        let mut hasher = Sha256::new();
        hasher.update(b"market-squawk/bea-canonical-batch/v1");
        hash_text(&mut hasher, context.source_id.as_str())?;
        hash_text(&mut hasher, context.provider_dataset_id.as_str())?;
        hash_text(&mut hasher, context.analytical_dataset_id.as_str())?;
        hasher.update(context.source_binding_digest.bytes());
        hasher.update(context.doctor_admission_digest.bytes());
        hasher.update(context.doctor_sealed_graph_digest.bytes());
        hasher.update(context.rights_policy_digest.bytes());
        hasher.update(context.root_rights_decision_digest.bytes());
        hasher.update(context.rights_rejoin_digest.bytes());
        hasher.update(context.raw_seal_digest.bytes());
        hasher.update(
            u64::try_from(observations.len())
                .map_err(|_| BeaCanonicalError::Encoding)?
                .to_be_bytes(),
        );
        for observation in &observations {
            hasher.update(observation.canonical_payload_digest.bytes());
            hasher.update(observation.native_row_digest.bytes());
        }
        let batch_digest =
            EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into());
        Ok(Self {
            source_id: context.source_id.clone(),
            provider_dataset_id: context.provider_dataset_id.clone(),
            analytical_dataset_id: context.analytical_dataset_id.clone(),
            observations,
            batch_digest,
        })
    }

    /// Returns the configured provider-query identity.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }
    /// Returns the configured provider-query identity.
    pub const fn provider_dataset_id(&self) -> &SourceIdentifier {
        &self.provider_dataset_id
    }
    /// Returns the immutable analytical dataset target.
    pub const fn analytical_dataset_id(&self) -> &SourceIdentifier {
        &self.analytical_dataset_id
    }
    /// Returns canonical rows in provider response order.
    pub fn observations(&self) -> &[BeaCanonicalObservation] {
        &self.observations
    }
    /// Returns the full provider/native/canonical/authority graph commitment.
    pub const fn batch_digest(&self) -> EvidenceDigest {
        self.batch_digest
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        SourceId,
        SourceIdentifier,
        SourceIdentifier,
        Vec<BeaCanonicalObservation>,
        EvidenceDigest,
    ) {
        (
            self.source_id,
            self.provider_dataset_id,
            self.analytical_dataset_id,
            self.observations,
            self.batch_digest,
        )
    }
}

fn effective_coordinate(
    observation: &BeaObservation,
) -> Result<ResearchTemporalCoordinate, BeaCanonicalError> {
    let scheme = match observation.period().frequency() {
        BeaFrequency::Annual => "bea-annual",
        BeaFrequency::Quarterly => "bea-quarterly",
        BeaFrequency::Monthly => "bea-monthly",
    };
    let period = ResearchPeriod::try_new(
        SourceIdentifier::try_from(scheme).map_err(|_| BeaCanonicalError::Encoding)?,
        observation.period().year(),
        NonZeroU16::new(u16::from(observation.period().ordinal()))
            .ok_or(BeaCanonicalError::InvalidObservation)?,
        SourceIdentifier::try_from(observation.period().raw())
            .map_err(|_| BeaCanonicalError::Encoding)?,
    )
    .map_err(|_| BeaCanonicalError::InvalidObservation)?;
    Ok(ResearchTemporalCoordinate::source_period(period))
}

fn canonical_series_digest(
    observation: &BeaObservation,
) -> Result<EvidenceDigest, BeaCanonicalError> {
    let mut dimensions = BTreeMap::new();
    for (name, value) in observation.identity().dimensions() {
        if !matches_semantic(name, &["TimePeriod", "CL_UNIT", "UNIT_MULT"]) {
            dimensions.insert(name, value);
        }
    }
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk/bea-canonical-series/v1");
    hash_text(&mut hasher, observation.identity().dataset().as_str())?;
    hash_optional_text(&mut hasher, observation.identity().table())?;
    hash_optional_text(&mut hasher, observation.identity().line())?;
    hasher.update([match observation.period().frequency() {
        BeaFrequency::Annual => 1,
        BeaFrequency::Quarterly => 2,
        BeaFrequency::Monthly => 3,
    }]);
    hasher.update(
        u64::try_from(dimensions.len())
            .map_err(|_| BeaCanonicalError::Encoding)?
            .to_be_bytes(),
    );
    for (name, value) in dimensions {
        hash_text(&mut hasher, name)?;
        hash_text(&mut hasher, value)?;
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hasher.finalize().into(),
    ))
}

fn unit_digest(observation: &BeaObservation) -> EvidenceDigest {
    let mut hasher = Sha256::new();
    // The canonical value is already scaled to the base `CL_UNIT`. `UNIT_MULT` remains in the
    // native row/capture lineage and must not be encoded into the post-scaling unit identity.
    hasher.update(b"market-squawk/bea-canonical-unit/v2");
    hasher.update(
        u64::try_from(observation.unit().cl_unit().len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    hasher.update(observation.unit().cl_unit().as_bytes());
    EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into())
}

fn scale_value_exact(value: Decimal, exponent: i16) -> Result<Decimal, BeaCanonicalError> {
    let magnitude = exponent.unsigned_abs();
    if magnitude > 28 {
        return Err(BeaCanonicalError::InvalidScale);
    }
    let mut factor = Decimal::ONE;
    for _ in 0..magnitude {
        factor = factor
            .checked_mul(Decimal::TEN)
            .ok_or(BeaCanonicalError::InvalidScale)?;
    }
    let scaled = if exponent >= 0 {
        value.checked_mul(factor)
    } else {
        value.checked_div(factor)
    }
    .ok_or(BeaCanonicalError::InvalidScale)?;
    let reversible = if exponent >= 0 {
        scaled.checked_div(factor)
    } else {
        scaled.checked_mul(factor)
    }
    .is_some_and(|candidate| candidate.normalize() == value.normalize());
    if !reversible {
        return Err(BeaCanonicalError::InvalidScale);
    }
    Ok(scaled.normalize())
}

fn identifier_from_digest(
    prefix: &str,
    digest: EvidenceDigest,
) -> Result<SourceIdentifier, BeaCanonicalError> {
    SourceIdentifier::try_from(format!("{prefix}:{}", lower_hex(digest.bytes())))
        .map_err(|_| BeaCanonicalError::Encoding)
}

fn digest(value: [u8; 32]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, value)
}

fn digest_bytes(value: &[u8]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(value).into())
}

fn matches_semantic(value: &str, candidates: &[&str]) -> bool {
    candidates
        .iter()
        .any(|candidate| value.eq_ignore_ascii_case(candidate))
}

fn hash_text(hasher: &mut Sha256, value: &str) -> Result<(), BeaCanonicalError> {
    hasher.update(
        u64::try_from(value.len())
            .map_err(|_| BeaCanonicalError::Encoding)?
            .to_be_bytes(),
    );
    hasher.update(value.as_bytes());
    Ok(())
}

fn hash_optional_text(
    hasher: &mut Sha256,
    value: Option<&str>,
) -> Result<(), BeaCanonicalError> {
    match value {
        Some(value) => {
            hasher.update([1]);
            hash_text(hasher, value)
        }
        None => {
            hasher.update([0]);
            Ok(())
        }
    }
}

fn lower_hex(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}
