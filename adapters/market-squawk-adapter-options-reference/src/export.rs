//! Storage-neutral provider-reference export and ambiguity reconciliation.
//!
//! The adapter emits exact OCC/Cboe rows and deterministic alias assertions. Callers own any
//! external sort or staging needed for the production-sized files; this module owns only the
//! provider semantics used to classify one sorted assertion stream. It creates no canonical
//! instrument identity, durable generation, current pointer, or point-in-time authority.

use std::cmp::Ordering;

use market_squawk_domain::{OccOptionIdentity, ProviderInstrumentId, SourceIdentifier};
use serde::Serialize;
use thiserror::Error;

use crate::{
    CboeSeriesReference, CboeSymbolId, CboeVenue, OccDlpProductReference, OccProductType,
    PublicationRequest, ReferenceObjectContext,
};

/// Why an exported row cannot itself establish a canonical security identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OptionsReferenceIdentityDisposition {
    /// The row remains exact provider-native reference evidence for the shared resolver.
    ProviderNativeReferenceOnly,
}

/// What one exact current reference object establishes about a row's validity interval.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OptionsReferenceValidityDisposition {
    /// Presence is established only in the exact source snapshot; start and end are not inferred.
    ExactSourceSnapshotOnly,
}

/// Meaning of provider symbols carried by one exported row.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OptionsReferenceAliasDisposition {
    /// Symbols are resolver candidates and never silently become canonical identities.
    ProviderAliasCandidateOnly,
}

/// Provider-local currentness meaning after exact capture and strict parsing.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OptionsReferenceCurrentnessDisposition {
    /// Integrity is established; shared application policy must classify freshness.
    RequiresApplicationFreshnessClassification,
}

/// One exact provider-native row ready for caller-owned staged export.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "family", content = "record")]
pub enum ReferenceExportRecord {
    /// One Cboe venue-series observation with exact OSI, symbol, status, and raw lineage.
    CboeSeries(CboeSeriesReference),
    /// One OCC DLP option-product/root observation with exact aliases and raw lineage.
    OccProduct(OccDlpProductReference),
}

impl From<CboeSeriesReference> for ReferenceExportRecord {
    fn from(value: CboeSeriesReference) -> Self {
        Self::CboeSeries(value)
    }
}

impl From<OccDlpProductReference> for ReferenceExportRecord {
    fn from(value: OccDlpProductReference) -> Self {
        Self::OccProduct(value)
    }
}

impl ReferenceExportRecord {
    /// Returns the exact Cboe series when this is a Cboe export.
    pub const fn as_cboe_series(&self) -> Option<&CboeSeriesReference> {
        match self {
            Self::CboeSeries(record) => Some(record),
            Self::OccProduct(_) => None,
        }
    }

    /// Returns the exact OCC product when this is an OCC export.
    pub const fn as_occ_product(&self) -> Option<&OccDlpProductReference> {
        match self {
            Self::CboeSeries(_) => None,
            Self::OccProduct(record) => Some(record),
        }
    }

    /// Returns the exact object and clock lineage carried by this row.
    pub const fn object_context(&self) -> &ReferenceObjectContext {
        match self {
            Self::CboeSeries(record) => record.object_context(),
            Self::OccProduct(record) => record.object_context(),
        }
    }

    /// Returns the explicit provider-native-only identity meaning.
    pub const fn identity(&self) -> OptionsReferenceIdentityDisposition {
        OptionsReferenceIdentityDisposition::ProviderNativeReferenceOnly
    }

    /// Returns the exact-snapshot-only validity meaning.
    pub const fn validity(&self) -> OptionsReferenceValidityDisposition {
        OptionsReferenceValidityDisposition::ExactSourceSnapshotOnly
    }

    /// Returns the explicit unresolved-provider-alias meaning.
    pub const fn alias(&self) -> OptionsReferenceAliasDisposition {
        OptionsReferenceAliasDisposition::ProviderAliasCandidateOnly
    }

    /// Visits every deterministic alias assertion needed for provider-local conflict detection.
    ///
    /// A Cboe row emits symbol, OSI, and venue/symbol assertions. An OCC row emits its exact
    /// product-natural-key assertion. The caller may write these to an external sorter without
    /// retaining the publication in memory.
    ///
    /// # Errors
    ///
    /// Rejects a caller-owned sink failure.
    pub fn visit_alias_assertions<F>(&self, mut sink: F) -> Result<(), ReferenceExportError>
    where
        F: FnMut(ReferenceAliasAssertion) -> Result<(), ReferenceExportError>,
    {
        let context = self.object_context();
        let request_id = context.transport_evidence().request().request_id().clone();
        match self {
            Self::CboeSeries(record) => {
                let symbol = record.cboe_symbol_id().clone();
                let osi = record.contract().osi().clone();
                let underlying = record.underlying().clone();
                let evidence = record.record_id().clone();
                sink(ReferenceAliasAssertion::new(
                    request_id.clone(),
                    ReferenceAliasKey::CboeSymbol {
                        symbol: symbol.clone(),
                    },
                    ReferenceAliasTarget::CboeContract {
                        osi: osi.clone(),
                        underlying,
                    },
                    evidence.clone(),
                ))?;
                sink(ReferenceAliasAssertion::new(
                    request_id.clone(),
                    ReferenceAliasKey::CboeOsi { osi },
                    ReferenceAliasTarget::CboeSymbol {
                        symbol: symbol.clone(),
                    },
                    evidence.clone(),
                ))?;
                sink(ReferenceAliasAssertion::new(
                    request_id,
                    ReferenceAliasKey::CboeVenueSymbol {
                        venue: record.venue(),
                        symbol,
                    },
                    ReferenceAliasTarget::ProviderRecord,
                    evidence,
                ))
            }
            Self::OccProduct(record) => sink(ReferenceAliasAssertion::new(
                request_id,
                ReferenceAliasKey::OccProduct {
                    options_symbol: record.options_symbol().clone(),
                    product_type: record.product_type(),
                },
                ReferenceAliasTarget::ProviderRecord,
                record.record_id().clone(),
            )),
        }
    }
}

/// Deterministic provider alias or natural key within one exact acquisition request.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ReferenceAliasKey {
    /// A Cboe compressed symbol candidate.
    CboeSymbol {
        /// Exact case-sensitive Cboe symbol.
        symbol: CboeSymbolId,
    },
    /// An OCC/OSI contract candidate observed by Cboe.
    CboeOsi {
        /// Exact 21-character OSI identity.
        osi: OccOptionIdentity,
    },
    /// A supposedly unique Cboe venue/symbol row key.
    CboeVenueSymbol {
        /// Exact Cboe venue publication.
        venue: CboeVenue,
        /// Exact case-sensitive Cboe symbol.
        symbol: CboeSymbolId,
    },
    /// A supposedly unique OCC DLP product key.
    OccProduct {
        /// Exact OCC options/product symbol.
        options_symbol: ProviderInstrumentId,
        /// Exact OCC product type.
        product_type: OccProductType,
    },
}

/// Exact candidate value asserted for one provider alias key.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ReferenceAliasTarget {
    /// OSI contract and independently reported underlying alias for one Cboe symbol.
    CboeContract {
        /// Exact 21-character OSI identity.
        osi: OccOptionIdentity,
        /// Exact provider-reported underlying alias.
        underlying: ProviderInstrumentId,
    },
    /// One Cboe symbol reported for an OSI contract.
    CboeSymbol {
        /// Exact case-sensitive Cboe symbol.
        symbol: CboeSymbolId,
    },
    /// Presence of a supposedly unique provider natural-key row.
    ProviderRecord,
}

impl Ord for ReferenceAliasKey {
    fn cmp(&self, other: &Self) -> Ordering {
        let family = alias_key_family(self).cmp(&alias_key_family(other));
        if family != Ordering::Equal {
            return family;
        }
        match (self, other) {
            (Self::CboeSymbol { symbol: left }, Self::CboeSymbol { symbol: right }) => {
                left.cmp(right)
            }
            (Self::CboeOsi { osi: left }, Self::CboeOsi { osi: right }) => {
                left.as_str().cmp(right.as_str())
            }
            (
                Self::CboeVenueSymbol {
                    venue: left_venue,
                    symbol: left_symbol,
                },
                Self::CboeVenueSymbol {
                    venue: right_venue,
                    symbol: right_symbol,
                },
            ) => (left_venue, left_symbol).cmp(&(right_venue, right_symbol)),
            (
                Self::OccProduct {
                    options_symbol: left_symbol,
                    product_type: left_type,
                },
                Self::OccProduct {
                    options_symbol: right_symbol,
                    product_type: right_type,
                },
            ) => (left_symbol, left_type).cmp(&(right_symbol, right_type)),
            _ => Ordering::Equal,
        }
    }
}

impl PartialOrd for ReferenceAliasKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

const fn alias_key_family(key: &ReferenceAliasKey) -> u8 {
    match key {
        ReferenceAliasKey::CboeSymbol { .. } => 0,
        ReferenceAliasKey::CboeOsi { .. } => 1,
        ReferenceAliasKey::CboeVenueSymbol { .. } => 2,
        ReferenceAliasKey::OccProduct { .. } => 3,
    }
}

impl Ord for ReferenceAliasTarget {
    fn cmp(&self, other: &Self) -> Ordering {
        let family = alias_target_family(self).cmp(&alias_target_family(other));
        if family != Ordering::Equal {
            return family;
        }
        match (self, other) {
            (
                Self::CboeContract {
                    osi: left_osi,
                    underlying: left_underlying,
                },
                Self::CboeContract {
                    osi: right_osi,
                    underlying: right_underlying,
                },
            ) => (left_osi.as_str(), left_underlying).cmp(&(right_osi.as_str(), right_underlying)),
            (Self::CboeSymbol { symbol: left }, Self::CboeSymbol { symbol: right }) => {
                left.cmp(right)
            }
            (Self::ProviderRecord, Self::ProviderRecord) => Ordering::Equal,
            _ => Ordering::Equal,
        }
    }
}

impl PartialOrd for ReferenceAliasTarget {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

const fn alias_target_family(target: &ReferenceAliasTarget) -> u8 {
    match target {
        ReferenceAliasTarget::CboeContract { .. } => 0,
        ReferenceAliasTarget::CboeSymbol { .. } => 1,
        ReferenceAliasTarget::ProviderRecord => 2,
    }
}

/// Deterministic external-sort coordinate for one alias assertion.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceAliasSortKey {
    request_id: SourceIdentifier,
    key: ReferenceAliasKey,
    target: ReferenceAliasTarget,
    evidence: SourceIdentifier,
}

/// One exact storage-neutral alias assertion derived from a provider row.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceAliasAssertion {
    request_id: SourceIdentifier,
    key: ReferenceAliasKey,
    target: ReferenceAliasTarget,
    evidence: SourceIdentifier,
}

impl ReferenceAliasAssertion {
    fn new(
        request_id: SourceIdentifier,
        key: ReferenceAliasKey,
        target: ReferenceAliasTarget,
        evidence: SourceIdentifier,
    ) -> Self {
        Self {
            request_id,
            key,
            target,
            evidence,
        }
    }

    /// Reconstructs one typed assertion read from caller-owned external staging.
    ///
    /// This value remains provider evidence rather than publication authority. The shared caller
    /// must still bind it to exact typed rows and raw receipts before composition.
    ///
    /// # Errors
    ///
    /// Rejects a key and target from incompatible provider families.
    pub fn try_from_parts(
        request_id: SourceIdentifier,
        key: ReferenceAliasKey,
        target: ReferenceAliasTarget,
        evidence: SourceIdentifier,
    ) -> Result<Self, ReferenceExportError> {
        validate_assertion_shape(&key, &target)?;
        Ok(Self {
            request_id,
            key,
            target,
            evidence,
        })
    }

    /// Returns the exact acquisition request containing this assertion.
    pub const fn request_id(&self) -> &SourceIdentifier {
        &self.request_id
    }

    /// Returns the exact provider alias or natural key.
    pub const fn key(&self) -> &ReferenceAliasKey {
        &self.key
    }

    /// Returns the exact candidate value asserted for the key.
    pub const fn target(&self) -> &ReferenceAliasTarget {
        &self.target
    }

    /// Returns the exact provider row supplying this assertion.
    pub const fn evidence(&self) -> &SourceIdentifier {
        &self.evidence
    }

    /// Consumes the assertion into fields suitable for caller-owned external staging.
    pub fn into_parts(
        self,
    ) -> (
        SourceIdentifier,
        ReferenceAliasKey,
        ReferenceAliasTarget,
        SourceIdentifier,
    ) {
        (self.request_id, self.key, self.target, self.evidence)
    }

    /// Returns an owned deterministic key suitable for caller-owned external sorting.
    pub fn sort_key(&self) -> ReferenceAliasSortKey {
        ReferenceAliasSortKey {
            request_id: self.request_id.clone(),
            key: self.key.clone(),
            target: self.target.clone(),
            evidence: self.evidence.clone(),
        }
    }
}

/// Provider-local ambiguity class; no candidate is selected as a winner.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReferenceConflictKind {
    /// One Cboe symbol mapped to distinct OSI contracts.
    CboeSymbolMapsMultipleOsi,
    /// One OSI contract mapped to distinct Cboe symbols.
    CboeOsiMapsMultipleSymbols,
    /// One Cboe symbol mapped to distinct independent underlying aliases.
    CboeSymbolMapsMultipleUnderlying,
    /// One provider natural key appeared more than once in the same acquisition.
    DuplicateProviderRecord,
}

/// Exact conflicting provider evidence retained without selecting a winner.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceConflict {
    request_id: SourceIdentifier,
    kind: ReferenceConflictKind,
    key: ReferenceAliasKey,
    first_evidence: SourceIdentifier,
    second_evidence: SourceIdentifier,
}

impl ReferenceConflict {
    /// Returns the exact acquisition request containing the conflict.
    pub const fn request_id(&self) -> &SourceIdentifier {
        &self.request_id
    }

    /// Returns the provider-local conflict class.
    pub const fn kind(&self) -> ReferenceConflictKind {
        self.kind
    }

    /// Returns the exact ambiguous alias or provider natural key.
    pub const fn key(&self) -> &ReferenceAliasKey {
        &self.key
    }

    /// Returns the first exact provider-row evidence identity.
    pub const fn first_evidence(&self) -> &SourceIdentifier {
        &self.first_evidence
    }

    /// Returns the second exact provider-row evidence identity.
    pub const fn second_evidence(&self) -> &SourceIdentifier {
        &self.second_evidence
    }
}

/// Resolution state for all assertions sharing one request-scoped alias key.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReferenceAliasResolutionState {
    /// Every observation made the same compatible assertion.
    Exact,
    /// At least one incompatible assertion was retained as a conflict.
    Ambiguous,
}

/// Bounded terminal outcome for one request-scoped provider alias key.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceAliasResolution {
    request_id: SourceIdentifier,
    key: ReferenceAliasKey,
    state: ReferenceAliasResolutionState,
    observations: u64,
    conflicts: u32,
}

impl ReferenceAliasResolution {
    /// Returns the exact acquisition request containing these observations.
    pub const fn request_id(&self) -> &SourceIdentifier {
        &self.request_id
    }

    /// Returns the exact provider alias or natural key.
    pub const fn key(&self) -> &ReferenceAliasKey {
        &self.key
    }

    /// Returns whether the request-scoped key was exact or ambiguous.
    pub const fn state(&self) -> ReferenceAliasResolutionState {
        self.state
    }

    /// Returns every compatible or conflicting assertion observed for this key.
    pub const fn observations(&self) -> u64 {
        self.observations
    }

    /// Returns the exact number of emitted conflicts for this key.
    pub const fn conflicts(&self) -> u32 {
        self.conflicts
    }
}

/// Streaming reconciler for a caller-owned externally sorted assertion stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReferenceConflictReconciler {
    request_id: SourceIdentifier,
    max_conflicts: u32,
}

/// Terminal counts from one complete deterministic conflict-reconciliation stream.
///
/// This is request-scoped provider semantics, not retained rows, a publication catalog, or a
/// canonical identity decision.
#[derive(Debug, Eq, PartialEq)]
pub struct ReferenceConflictReconciliationReceipt {
    request_id: SourceIdentifier,
    conflicts: usize,
}

impl ReferenceConflictReconciliationReceipt {
    /// Returns the exact publication request whose assertions were reconciled.
    pub const fn request_id(&self) -> &SourceIdentifier {
        &self.request_id
    }

    /// Returns the exact number of emitted ambiguity conflicts.
    pub const fn conflicts(&self) -> usize {
        self.conflicts
    }
}

impl ReferenceConflictReconciler {
    /// Binds reconciliation to one exact publication request and its admitted conflict ceiling.
    ///
    /// # Errors
    ///
    /// Rejects a publication limit that cannot be represented by the streaming reconciler.
    pub fn try_for_publication(request: &PublicationRequest) -> Result<Self, ReferenceExportError> {
        let max_conflicts = u32::try_from(request.limits().max_conflicts())
            .map_err(|_| ReferenceExportError::InvalidLimit)?;
        Ok(Self {
            request_id: request.request_id().clone(),
            max_conflicts,
        })
    }

    /// Reconciles one complete deterministic assertion stream without retaining it in memory.
    ///
    /// Input must be ordered by [`ReferenceAliasAssertion::sort_key`]. Outcomes and conflicts are
    /// emitted incrementally to caller-owned staging. The input callback may read an external
    /// sort run fallibly without materializing it. The function returns success only after the
    /// callback reports terminal EOF and the final group is emitted. Caller-owned partial output
    /// must be discarded unless a terminal receipt is returned.
    ///
    /// # Errors
    ///
    /// Rejects unsorted or structurally incompatible assertions, counter/limit overflow, or a
    /// caller-owned output failure.
    pub fn reconcile<N, O, C>(
        self,
        mut next_assertion: N,
        mut outcome_sink: O,
        mut conflict_sink: C,
    ) -> Result<ReferenceConflictReconciliationReceipt, ReferenceExportError>
    where
        N: FnMut() -> Result<Option<ReferenceAliasAssertion>, ReferenceExportError>,
        O: FnMut(ReferenceAliasResolution) -> Result<(), ReferenceExportError>,
        C: FnMut(ReferenceConflict) -> Result<(), ReferenceExportError>,
    {
        let mut previous_sort_key = None;
        let mut active = None;
        let mut total_conflicts = 0_u32;
        while let Some(assertion) = next_assertion()? {
            if assertion.request_id != self.request_id {
                return Err(ReferenceExportError::RequestMismatch);
            }
            let sort_key = assertion.sort_key();
            if previous_sort_key
                .as_ref()
                .is_some_and(|previous| previous > &sort_key)
            {
                return Err(ReferenceExportError::UnsortedAssertions);
            }
            previous_sort_key = Some(sort_key);
            match active.take() {
                None => active = Some(ActiveAliasGroup::new(assertion)),
                Some(mut group) if group.matches(&assertion) => {
                    let conflicts = group.conflicts_with(&assertion)?;
                    group.observations = group
                        .observations
                        .checked_add(1)
                        .ok_or(ReferenceExportError::CountOverflow)?;
                    for kind in conflicts.into_iter().flatten() {
                        total_conflicts = total_conflicts
                            .checked_add(1)
                            .ok_or(ReferenceExportError::CountOverflow)?;
                        if total_conflicts > self.max_conflicts {
                            return Err(ReferenceExportError::ConflictLimitExceeded);
                        }
                        group.conflicts = group
                            .conflicts
                            .checked_add(1)
                            .ok_or(ReferenceExportError::CountOverflow)?;
                        conflict_sink(ReferenceConflict {
                            request_id: group.first.request_id.clone(),
                            kind,
                            key: group.first.key.clone(),
                            first_evidence: group.first.evidence.clone(),
                            second_evidence: assertion.evidence.clone(),
                        })?;
                    }
                    active = Some(group);
                }
                Some(group) => {
                    outcome_sink(group.into_resolution())?;
                    active = Some(ActiveAliasGroup::new(assertion));
                }
            }
        }
        if let Some(group) = active {
            outcome_sink(group.into_resolution())?;
        }
        Ok(ReferenceConflictReconciliationReceipt {
            request_id: self.request_id,
            conflicts: usize::try_from(total_conflicts)
                .map_err(|_| ReferenceExportError::CountOverflow)?,
        })
    }
}

struct ActiveAliasGroup {
    first: ReferenceAliasAssertion,
    observations: u64,
    conflicts: u32,
}

impl ActiveAliasGroup {
    const fn new(first: ReferenceAliasAssertion) -> Self {
        Self {
            first,
            observations: 1,
            conflicts: 0,
        }
    }

    fn matches(&self, assertion: &ReferenceAliasAssertion) -> bool {
        self.first.request_id == assertion.request_id && self.first.key == assertion.key
    }

    fn conflicts_with(
        &self,
        assertion: &ReferenceAliasAssertion,
    ) -> Result<[Option<ReferenceConflictKind>; 2], ReferenceExportError> {
        validate_assertion_shape(&self.first.key, &self.first.target)?;
        validate_assertion_shape(&assertion.key, &assertion.target)?;
        match (&self.first.key, &self.first.target, &assertion.target) {
            (
                ReferenceAliasKey::CboeSymbol { .. },
                ReferenceAliasTarget::CboeContract {
                    osi: first_osi,
                    underlying: first_underlying,
                },
                ReferenceAliasTarget::CboeContract {
                    osi: second_osi,
                    underlying: second_underlying,
                },
            ) => Ok([
                (first_osi != second_osi)
                    .then_some(ReferenceConflictKind::CboeSymbolMapsMultipleOsi),
                (first_underlying != second_underlying)
                    .then_some(ReferenceConflictKind::CboeSymbolMapsMultipleUnderlying),
            ]),
            (
                ReferenceAliasKey::CboeOsi { .. },
                ReferenceAliasTarget::CboeSymbol {
                    symbol: first_symbol,
                },
                ReferenceAliasTarget::CboeSymbol {
                    symbol: second_symbol,
                },
            ) => Ok([
                (first_symbol != second_symbol)
                    .then_some(ReferenceConflictKind::CboeOsiMapsMultipleSymbols),
                None,
            ]),
            (
                ReferenceAliasKey::CboeVenueSymbol { .. } | ReferenceAliasKey::OccProduct { .. },
                ReferenceAliasTarget::ProviderRecord,
                ReferenceAliasTarget::ProviderRecord,
            ) => Ok([Some(ReferenceConflictKind::DuplicateProviderRecord), None]),
            _ => Err(ReferenceExportError::InvalidAssertion),
        }
    }

    fn into_resolution(self) -> ReferenceAliasResolution {
        ReferenceAliasResolution {
            request_id: self.first.request_id,
            key: self.first.key,
            state: if self.conflicts == 0 {
                ReferenceAliasResolutionState::Exact
            } else {
                ReferenceAliasResolutionState::Ambiguous
            },
            observations: self.observations,
            conflicts: self.conflicts,
        }
    }
}

fn validate_assertion_shape(
    key: &ReferenceAliasKey,
    target: &ReferenceAliasTarget,
) -> Result<(), ReferenceExportError> {
    if matches!(
        (key, target),
        (
            ReferenceAliasKey::CboeSymbol { .. },
            ReferenceAliasTarget::CboeContract { .. }
        ) | (
            ReferenceAliasKey::CboeOsi { .. },
            ReferenceAliasTarget::CboeSymbol { .. }
        ) | (
            ReferenceAliasKey::CboeVenueSymbol { .. } | ReferenceAliasKey::OccProduct { .. },
            ReferenceAliasTarget::ProviderRecord
        )
    ) {
        Ok(())
    } else {
        Err(ReferenceExportError::InvalidAssertion)
    }
}

/// Storage-neutral export or reconciliation failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ReferenceExportError {
    /// A caller-selected conflict limit was zero or excessive.
    #[error("invalid option-reference conflict limit")]
    InvalidLimit,
    /// An assertion belonged to a different acquisition request.
    #[error("option-reference alias assertion request mismatch")]
    RequestMismatch,
    /// Alias assertions were not supplied in their exact deterministic order.
    #[error("option-reference alias assertions are not sorted")]
    UnsortedAssertions,
    /// An alias key and target belonged to incompatible provider families.
    #[error("invalid option-reference alias assertion")]
    InvalidAssertion,
    /// Retained conflicts exceeded the explicit caller ceiling.
    #[error("option-reference conflict limit exceeded")]
    ConflictLimitExceeded,
    /// A bounded observation or conflict counter overflowed.
    #[error("option-reference export counter overflowed")]
    CountOverflow,
    /// Caller-owned staging rejected an emitted record, assertion, outcome, or conflict.
    #[error("option-reference export sink rejected output")]
    SinkRejected,
}
