//! Seal-first Schwab REST option-chain and expiration publication.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU16;

use bytes::Bytes;
use market_squawk_domain::{
    CalendarDate, Currency, DigestAlgorithm, EvidenceDigest, InstrumentId, Money,
    OccOptionIdentity, OptionComponent, OptionComponentState, OptionContractTerms,
    OptionContractTermsInput, OptionExpirationClass, OptionExpirationObservation,
    OptionExpirationObservationInput, OptionKind, OptionSettlementKind, OptionSnapshotObservation,
    OptionSnapshotObservationInput, OptionUnderlyingObservation, ProviderChannel, ProviderProduct,
    QuantityLots, SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_sources::{
    ExtractionRevisionPlan, OptionExpirationRange, OptionMarketBatchDisposition,
    OptionMarketCompleteness, OptionMarketCompletenessInput, OptionMarketCursorState,
    OptionMarketRequestFilter, OptionMarketRequestScope, OptionMarketRequestScopeInput,
    OptionStrikeRange, ProviderCaptureError, ProviderNativeLineageImplementation,
    ProviderOptionMarketBatch, ProviderOptionMarketNativeLineageBatch, SchwabMarketDataFamily,
    SealedProviderOptionMarketBinding,
};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive as _;
use serde::Serialize;
use thiserror::Error;

use crate::rest::ExpirationEntry;
use crate::transport::SchwabSealedRestResponseParts;
use crate::{
    NativeField, NativeNumber, NativeScalar, OptionContract, OptionContractField, OptionSide,
    ProviderIdentifier, ReadOnlyRoute, SchwabMarketDataDelay, SchwabMarketDataDepth,
    SchwabMarketDataQualification, SchwabResolvedProviderIdentity, SchwabRestPayload,
    SchwabSealedRestResponse,
};

/// Exact feed/venue/depth/delay and authorization evidence for one REST option response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchwabRestOptionMarketDataEvidence {
    reference_venue_id: Option<VenueId>,
    qualification: SchwabMarketDataQualification,
    currency: Currency,
}

impl SchwabRestOptionMarketDataEvidence {
    /// Constructs caller-qualified option-market semantics without inventing a venue or delay.
    pub fn try_new(
        reference_venue_id: Option<VenueId>,
        qualification: SchwabMarketDataQualification,
        currency: Currency,
    ) -> Result<Self, SchwabRestOptionPublicationError> {
        if !matches!(
            qualification.family(),
            SchwabMarketDataFamily::OptionChains | SchwabMarketDataFamily::ExpirationChains
        ) {
            return Err(SchwabRestOptionPublicationError::InvalidEvidence);
        }
        Ok(Self {
            reference_venue_id,
            qualification,
            currency,
        })
    }

    pub const fn feed(&self) -> &SourceIdentifier {
        self.qualification.feed()
    }

    pub const fn venue_id(&self) -> Option<&VenueId> {
        self.reference_venue_id.as_ref()
    }

    pub const fn depth(&self) -> SchwabMarketDataDepth {
        self.qualification.depth()
    }

    pub const fn delay(&self) -> SchwabMarketDataDelay {
        self.qualification.delay()
    }

    pub const fn provider_product(&self) -> &ProviderProduct {
        self.qualification.provider_product()
    }

    pub const fn provider_channel(&self) -> &ProviderChannel {
        self.qualification.provider_channel()
    }

    pub const fn qualification_evidence(&self) -> EvidenceDigest {
        self.qualification.observation_evidence()
    }

    pub const fn entitlement_evidence(&self) -> EvidenceDigest {
        self.qualification.entitlement_evidence()
    }

    pub const fn capability_evidence(&self) -> EvidenceDigest {
        self.qualification.capability_evidence()
    }

    pub const fn currency(&self) -> Currency {
        self.currency
    }
}

/// Resolved underlying identity required by both option response families.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchwabRestOptionUnderlyingRequest {
    identity: SchwabResolvedProviderIdentity,
    instrument_id: InstrumentId,
    definition_revision: EvidenceDigest,
}

impl SchwabRestOptionUnderlyingRequest {
    pub fn new(
        identity: SchwabResolvedProviderIdentity,
        instrument_id: InstrumentId,
        definition_revision: EvidenceDigest,
    ) -> Self {
        Self {
            identity,
            instrument_id,
            definition_revision,
        }
    }
}

/// Exact resolved identity input for one returned Schwab option contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchwabRestOptionContractRequest {
    identity: SchwabResolvedProviderIdentity,
    instrument_id: InstrumentId,
    definition_revision: EvidenceDigest,
    occ_identity: Option<OccOptionIdentity>,
}

impl SchwabRestOptionContractRequest {
    pub fn new(
        identity: SchwabResolvedProviderIdentity,
        instrument_id: InstrumentId,
        definition_revision: EvidenceDigest,
        occ_identity: Option<OccOptionIdentity>,
    ) -> Self {
        Self {
            identity,
            instrument_id,
            definition_revision,
            occ_identity,
        }
    }
}

/// Complete mapping input for one already sealed Schwab option or expiration response.
#[derive(Debug)]
pub struct SchwabRestOptionPublicationRequest {
    underlying: SchwabRestOptionUnderlyingRequest,
    contracts: Vec<SchwabRestOptionContractRequest>,
    market_data: SchwabRestOptionMarketDataEvidence,
    ingested_at: Timestamp,
}

impl SchwabRestOptionPublicationRequest {
    pub fn new(
        underlying: SchwabRestOptionUnderlyingRequest,
        contracts: Vec<SchwabRestOptionContractRequest>,
        market_data: SchwabRestOptionMarketDataEvidence,
        ingested_at: Timestamp,
    ) -> Self {
        Self {
            underlying,
            contracts,
            market_data,
            ingested_at,
        }
    }
}

/// Why one provider option record remained raw instead of becoming a canonical row.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SchwabRestOptionDispositionReason {
    MissingContractSymbol,
    MissingMappingInput,
    InvalidRequiredTerms,
    CanonicalMappingRejected,
    DuplicateCanonicalIdentity,
    InvalidComponentClock,
}

/// Exact provider record omitted from the canonical option batch.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SchwabRestOptionDisposition {
    provider_record_ordinal: u32,
    provider_symbol: Option<Box<str>>,
    expiration_group: Box<str>,
    strike_group: Option<Box<str>>,
    reason: SchwabRestOptionDispositionReason,
}

impl SchwabRestOptionDisposition {
    pub const fn provider_record_ordinal(&self) -> u32 {
        self.provider_record_ordinal
    }

    pub fn provider_symbol(&self) -> Option<&str> {
        self.provider_symbol.as_deref()
    }

    pub const fn reason(&self) -> SchwabRestOptionDispositionReason {
        self.reason
    }
}

/// Typed option publication or recoverable sealed raw evidence when every provider row abstained.
#[derive(Debug)]
pub enum SchwabRestOptionPublicationOutcome {
    Published(Box<SchwabSealedRestOptionPublication>),
    SealedRaw(Box<SchwabSealedRawRestOptionPublication>),
}

/// Non-cloneable sealed option response retained when it has no canonical row.
#[derive(Debug)]
pub struct SchwabSealedRawRestOptionPublication {
    response: SchwabSealedRestResponse,
    dispositions: Box<[SchwabRestOptionDisposition]>,
}

impl SchwabSealedRawRestOptionPublication {
    pub const fn dispositions(&self) -> &[SchwabRestOptionDisposition] {
        &self.dispositions
    }

    pub fn into_response(self) -> SchwabSealedRestResponse {
        self.response
    }
}

/// One-shot canonical/native/physical option publication handoff.
#[derive(Debug)]
pub struct SchwabSealedRestOptionPublication {
    market_data: SchwabRestOptionMarketDataEvidence,
    revision_plan: ExtractionRevisionPlan,
    binding: SealedProviderOptionMarketBinding,
    dispositions: Box<[SchwabRestOptionDisposition]>,
}

impl SchwabSealedRestOptionPublication {
    pub const fn market_data(&self) -> &SchwabRestOptionMarketDataEvidence {
        &self.market_data
    }

    pub const fn revision_plan(&self) -> &ExtractionRevisionPlan {
        &self.revision_plan
    }

    pub const fn binding(&self) -> &SealedProviderOptionMarketBinding {
        &self.binding
    }

    pub const fn dispositions(&self) -> &[SchwabRestOptionDisposition] {
        &self.dispositions
    }

    pub fn into_parts(self) -> (ExtractionRevisionPlan, SealedProviderOptionMarketBinding) {
        (self.revision_plan, self.binding)
    }
}

impl SchwabSealedRestResponse {
    /// Maps an option chain or expiration catalog only after exact raw response sealing.
    pub fn into_option_publication(
        self,
        request: SchwabRestOptionPublicationRequest,
    ) -> Result<SchwabRestOptionPublicationOutcome, SchwabRestOptionPublicationError> {
        if !matches!(
            self.route(),
            ReadOnlyRoute::Chains | ReadOnlyRoute::ExpirationChain
        ) {
            return Err(SchwabRestOptionPublicationError::FamilyMismatch);
        }
        validate_underlying(&self, &request.underlying)?;
        let parts = self.parts();
        let qualification_family = match parts.receipt.route() {
            ReadOnlyRoute::Chains => SchwabMarketDataFamily::OptionChains,
            ReadOnlyRoute::ExpirationChain => SchwabMarketDataFamily::ExpirationChains,
            _ => return Err(SchwabRestOptionPublicationError::FamilyMismatch),
        };
        if !request
            .market_data
            .qualification
            .validates_rest_receipt(qualification_family, &parts.receipt)
        {
            return Err(SchwabRestOptionPublicationError::InvalidEvidence);
        }
        let received_at = timestamp_from_millis(parts.receipt.received_at_unix_millis())?;
        if request.ingested_at < received_at {
            return Err(SchwabRestOptionPublicationError::InvalidEvidence);
        }
        let provider_records = parts.accounting.provider_records;
        let (rows, native_rows, dispositions, reported_records, filter) = match &parts.payload {
            SchwabRestPayload::OptionChain(parsed) if self.route() == ReadOnlyRoute::Chains => {
                let reported_records = native_u64(parsed.value().number_of_contracts())
                    .ok_or(SchwabRestOptionPublicationError::InvalidEvidence)?;
                let mapped =
                    map_snapshots(parts, parsed.value().contracts(), &request, received_at)?;
                (
                    OptionRows::Snapshots(mapped.rows),
                    mapped.native_rows,
                    mapped.dispositions,
                    Some(reported_records),
                    option_chain_filter(parts.receipt.request_url(), request.market_data.currency)?,
                )
            }
            SchwabRestPayload::Expirations(parsed)
                if self.route() == ReadOnlyRoute::ExpirationChain =>
            {
                if !request.contracts.is_empty() {
                    return Err(SchwabRestOptionPublicationError::MappingMismatch);
                }
                require_unfiltered_expiration_request(parts.receipt.request_url())?;
                let mapped = map_expirations(parsed.value().expirations(), &request)?;
                (
                    OptionRows::Expirations(mapped.rows),
                    mapped.native_rows,
                    mapped.dispositions,
                    None,
                    OptionMarketRequestFilter::try_new(None, None, None, Vec::new())?,
                )
            }
            _ => return Err(SchwabRestOptionPublicationError::FamilyMismatch),
        };
        let row_count = rows.len();
        let disposition_count = u64::try_from(dispositions.len())
            .map_err(|_| SchwabRestOptionPublicationError::InvalidEvidence)?;
        if u64::try_from(row_count)
            .ok()
            .and_then(|mapped| mapped.checked_add(disposition_count))
            != Some(provider_records)
        {
            return Err(SchwabRestOptionPublicationError::InvalidEvidence);
        }
        // Canonical option publication is atomic for the exact sealed response. If any returned
        // provider row cannot be mapped, keep the complete response as sealed raw evidence rather
        // than emitting a canonical subset that could be mistaken for the requested chain.
        if !dispositions.is_empty() {
            return Ok(SchwabRestOptionPublicationOutcome::SealedRaw(Box::new(
                SchwabSealedRawRestOptionPublication {
                    response: self,
                    dispositions: dispositions.into_boxed_slice(),
                },
            )));
        }

        let capture = parts.token.persisted_receipt().capture();
        if capture.request_set_identity()
            != EvidenceDigest::new(DigestAlgorithm::Sha256, parts.receipt.request_sha256())
        {
            return Err(SchwabRestOptionPublicationError::InvalidEvidence);
        }
        let scope = OptionMarketRequestScope::try_new(OptionMarketRequestScopeInput {
            source_id: parts.coordinates.source_id().clone(),
            metadata_revision: parts.coordinates.metadata_revision().clone(),
            dataset: parts.coordinates.dataset().clone(),
            provider_product: request.market_data.provider_product().clone(),
            provider_channel: request.market_data.provider_channel().clone(),
            venue_id: request.market_data.reference_venue_id.clone(),
            underlying_instrument_id: request.underlying.instrument_id,
            underlying_definition_revision: request.underlying.definition_revision,
            provider_instrument_id: request.underlying.identity.provider_instrument_id().clone(),
            request_identity: capture.request_set_identity(),
            observation_identity: capture.observation_digest(),
            entitlement_evidence: request.market_data.entitlement_evidence(),
            capability_evidence: request.market_data.capability_evidence(),
            available_at: received_at,
            received_at,
            ingested_at: request.ingested_at,
            filter,
        })?;
        let missing_records = disposition_count;
        let completeness = OptionMarketCompleteness::try_new(OptionMarketCompletenessInput {
            expected_records: Some(provider_records),
            returned_records: u64::try_from(row_count)
                .map_err(|_| SchwabRestOptionPublicationError::InvalidEvidence)?,
            missing_records,
            unexpected_records: 0,
            provider_reported_records: reported_records,
            page_count: NonZeroU16::MIN,
            cursor: OptionMarketCursorState::NotApplicable,
            disposition: if dispositions.is_empty() {
                OptionMarketBatchDisposition::Complete
            } else {
                OptionMarketBatchDisposition::Unavailable
            },
        })?;
        let sidecar = encode_sidecar(parts, &request.market_data, &dispositions)?;
        let batch = match rows {
            OptionRows::Snapshots(rows) => {
                ProviderOptionMarketBatch::try_snapshots(scope, completeness, rows)?
            }
            OptionRows::Expirations(rows) => {
                ProviderOptionMarketBatch::try_expirations(scope, completeness, rows)?
            }
        };
        let native_lineage = ProviderOptionMarketNativeLineageBatch::try_new(
            ProviderNativeLineageImplementation::SchwabRestMarketDataV1,
            &batch,
            native_rows,
            sidecar,
        )?;
        let revision_plan = ExtractionRevisionPlan::locally_observed_with_native_lineage(row_count)
            .map_err(|_| SchwabRestOptionPublicationError::InvalidEvidence)?;
        let market_data = request.market_data;
        let SchwabSealedRestResponseParts { token, .. } = self.into_parts();
        let binding = SealedProviderOptionMarketBinding::try_new(
            token,
            batch,
            native_lineage,
            vec![0; row_count],
        )?;
        binding.validate()?;
        Ok(SchwabRestOptionPublicationOutcome::Published(Box::new(
            SchwabSealedRestOptionPublication {
                market_data,
                revision_plan,
                binding,
                dispositions: dispositions.into_boxed_slice(),
            },
        )))
    }
}

enum OptionRows {
    Snapshots(Vec<OptionSnapshotObservation>),
    Expirations(Vec<OptionExpirationObservation>),
}

impl OptionRows {
    fn len(&self) -> usize {
        match self {
            Self::Snapshots(rows) => rows.len(),
            Self::Expirations(rows) => rows.len(),
        }
    }
}

struct MappedRows<T> {
    rows: Vec<T>,
    native_rows: Vec<Bytes>,
    dispositions: Vec<SchwabRestOptionDisposition>,
}

fn map_snapshots(
    parts: &SchwabSealedRestResponseParts,
    contracts: &[OptionContract],
    request: &SchwabRestOptionPublicationRequest,
    received_at: Timestamp,
) -> Result<MappedRows<OptionSnapshotObservation>, SchwabRestOptionPublicationError> {
    let mut inputs = BTreeMap::new();
    for input in &request.contracts {
        validate_resolved_identity(&input.identity, input.definition_revision)?;
        if inputs
            .insert(input.identity.provider_symbol().clone(), input)
            .is_some()
        {
            return Err(SchwabRestOptionPublicationError::MappingMismatch);
        }
    }
    let mut used = BTreeSet::new();
    let mut canonical_ids = BTreeSet::new();
    let mut rows = Vec::new();
    let mut native_rows = Vec::new();
    let mut dispositions = Vec::new();
    let underlying = underlying_observation(parts, request.market_data.currency)?;

    for (index, contract) in contracts.iter().enumerate() {
        let ordinal =
            u32::try_from(index).map_err(|_| SchwabRestOptionPublicationError::InvalidEvidence)?;
        let Some(symbol) = option_text(contract, OptionContractField::Symbol)? else {
            dispositions.push(contract_disposition(
                ordinal,
                contract,
                None,
                SchwabRestOptionDispositionReason::MissingContractSymbol,
            ));
            continue;
        };
        let provider_symbol = match ProviderIdentifier::try_new(symbol.to_owned()) {
            Ok(value) => value,
            Err(_) => {
                dispositions.push(contract_disposition(
                    ordinal,
                    contract,
                    Some(symbol),
                    SchwabRestOptionDispositionReason::InvalidRequiredTerms,
                ));
                continue;
            }
        };
        let Some(input) = inputs.get(&provider_symbol) else {
            dispositions.push(contract_disposition(
                ordinal,
                contract,
                Some(provider_symbol.as_str()),
                SchwabRestOptionDispositionReason::MissingMappingInput,
            ));
            continue;
        };
        used.insert(provider_symbol.clone());
        if !canonical_ids.insert(input.instrument_id) {
            dispositions.push(contract_disposition(
                ordinal,
                contract,
                Some(provider_symbol.as_str()),
                SchwabRestOptionDispositionReason::DuplicateCanonicalIdentity,
            ));
            continue;
        }
        let snapshot =
            match option_snapshot(contract, input, request, underlying.clone(), received_at) {
                Ok(value) => value,
                Err(reason) => {
                    dispositions.push(contract_disposition(
                        ordinal,
                        contract,
                        Some(provider_symbol.as_str()),
                        reason,
                    ));
                    continue;
                }
            };
        native_rows.push(encode_snapshot_native_row(
            ordinal, contract, input, request,
        )?);
        rows.push(snapshot);
    }
    if used.len() != inputs.len() {
        return Err(SchwabRestOptionPublicationError::MappingMismatch);
    }
    Ok(MappedRows {
        rows,
        native_rows,
        dispositions,
    })
}

fn map_expirations(
    expirations: &[ExpirationEntry],
    request: &SchwabRestOptionPublicationRequest,
) -> Result<MappedRows<OptionExpirationObservation>, SchwabRestOptionPublicationError> {
    let mut dates = BTreeSet::new();
    let mut rows = Vec::new();
    let mut native_rows = Vec::new();
    let mut dispositions = Vec::new();
    for (index, expiration) in expirations.iter().enumerate() {
        let ordinal =
            u32::try_from(index).map_err(|_| SchwabRestOptionPublicationError::InvalidEvidence)?;
        let date = match calendar_date(&expiration.expiration_date) {
            Ok(date) if dates.insert(date) => date,
            Ok(_) => {
                dispositions.push(expiration_disposition(
                    ordinal,
                    expiration,
                    request,
                    SchwabRestOptionDispositionReason::DuplicateCanonicalIdentity,
                ));
                continue;
            }
            Err(_) => {
                dispositions.push(expiration_disposition(
                    ordinal,
                    expiration,
                    request,
                    SchwabRestOptionDispositionReason::InvalidRequiredTerms,
                ));
                continue;
            }
        };
        let observation = OptionExpirationObservation::try_new(OptionExpirationObservationInput {
            underlying_instrument_id: request.underlying.instrument_id,
            underlying_definition_revision: request.underlying.definition_revision,
            provider_instrument_id: request.underlying.identity.provider_instrument_id().clone(),
            expiration: date,
            class: expiration_class(&expiration.expiration_type),
            standard: native_bool_component(&expiration.standard),
        })
        .map_err(|_| SchwabRestOptionPublicationError::InvalidEvidence)?;
        native_rows.push(encode_expiration_native_row(ordinal, expiration, request)?);
        rows.push(observation);
    }
    Ok(MappedRows {
        rows,
        native_rows,
        dispositions,
    })
}

fn option_snapshot(
    contract: &OptionContract,
    input: &SchwabRestOptionContractRequest,
    request: &SchwabRestOptionPublicationRequest,
    underlying: OptionUnderlyingObservation,
    received_at: Timestamp,
) -> Result<OptionSnapshotObservation, SchwabRestOptionDispositionReason> {
    let expiration = contract_expiration(contract)?;
    let strike = contract_strike(contract)?;
    let kind = contract_kind(contract)?;
    let multiplier = required_positive_decimal(contract, OptionContractField::Multiplier)?;
    validate_optional_contract_terms(contract, expiration, strike, kind)?;
    let terms = OptionContractTerms::try_new(OptionContractTermsInput {
        option_instrument_id: input.instrument_id,
        underlying_instrument_id: request.underlying.instrument_id,
        option_definition_revision: input.definition_revision,
        underlying_definition_revision: request.underlying.definition_revision,
        provider_instrument_id: input.identity.provider_instrument_id().clone(),
        occ_identity: input.occ_identity.clone(),
        expiration,
        strike: Money::new(strike, request.market_data.currency),
        kind,
        multiplier,
        exercise_style: OptionComponent::unavailable(OptionComponentState::ProviderAbsent, None),
        settlement: settlement_component(contract),
    })
    .map_err(|_| SchwabRestOptionDispositionReason::InvalidRequiredTerms)?;
    let quote_at = option_timestamp(contract, OptionContractField::QuoteTimeInLong, received_at)?;
    let trade_at = option_timestamp(contract, OptionContractField::TradeTimeInLong, received_at)?;
    OptionSnapshotObservation::try_new(OptionSnapshotObservationInput {
        terms,
        bid_price: money_component(
            contract,
            OptionContractField::Bid,
            request.market_data.currency,
            quote_at,
        ),
        bid_size: quantity_component(contract, OptionContractField::BidSize, quote_at),
        ask_price: money_component(
            contract,
            OptionContractField::Ask,
            request.market_data.currency,
            quote_at,
        ),
        ask_size: quantity_component(contract, OptionContractField::AskSize, quote_at),
        last_price: money_component(
            contract,
            OptionContractField::Last,
            request.market_data.currency,
            trade_at,
        ),
        last_size: quantity_component(contract, OptionContractField::LastSize, trade_at),
        mark_price: money_component(
            contract,
            OptionContractField::Mark,
            request.market_data.currency,
            quote_at,
        ),
        trade_conditions: OptionComponent::unavailable(OptionComponentState::ProviderAbsent, None),
        volume: unsigned_component(contract, OptionContractField::TotalVolume, None),
        open_interest: unsigned_component(contract, OptionContractField::OpenInterest, None),
        implied_volatility: decimal_component(contract, OptionContractField::Volatility, None),
        delta: decimal_component(contract, OptionContractField::Delta, None),
        gamma: decimal_component(contract, OptionContractField::Gamma, None),
        theta: decimal_component(contract, OptionContractField::Theta, None),
        vega: decimal_component(contract, OptionContractField::Vega, None),
        rho: decimal_component(contract, OptionContractField::Rho, None),
        underlying,
    })
    .map_err(|_| SchwabRestOptionDispositionReason::CanonicalMappingRejected)
}

fn underlying_observation(
    parts: &SchwabSealedRestResponseParts,
    currency: Currency,
) -> Result<OptionUnderlyingObservation, SchwabRestOptionPublicationError> {
    let SchwabRestPayload::OptionChain(parsed) = &parts.payload else {
        return OptionUnderlyingObservation::try_new(
            OptionComponent::unavailable(OptionComponentState::NotApplicable, None),
            EvidenceDigest::new(DigestAlgorithm::Sha256, parts.receipt.body_sha256()),
        )
        .map_err(|_| SchwabRestOptionPublicationError::InvalidEvidence);
    };
    let price = match parsed.value().underlying_price() {
        NativeField::Absent => {
            OptionComponent::unavailable(OptionComponentState::ProviderAbsent, None)
        }
        NativeField::Null => OptionComponent::unavailable(OptionComponentState::ProviderNull, None),
        NativeField::Value(value) => match parse_decimal(value) {
            Some(value) if value > Decimal::ZERO => {
                OptionComponent::observed(Money::new(value, currency), None)
            }
            _ => OptionComponent::unavailable(OptionComponentState::Invalid, None),
        },
    };
    OptionUnderlyingObservation::try_new(
        price,
        EvidenceDigest::new(DigestAlgorithm::Sha256, parts.receipt.body_sha256()),
    )
    .map_err(|_| SchwabRestOptionPublicationError::InvalidEvidence)
}

fn validate_underlying(
    response: &SchwabSealedRestResponse,
    underlying: &SchwabRestOptionUnderlyingRequest,
) -> Result<(), SchwabRestOptionPublicationError> {
    validate_resolved_identity(&underlying.identity, underlying.definition_revision)?;
    let requested = request_symbol(response.receipt().request_url())?;
    if requested != underlying.identity.provider_symbol().as_str() {
        return Err(SchwabRestOptionPublicationError::MappingMismatch);
    }
    if let SchwabRestPayload::OptionChain(parsed) = &response.parts().payload
        && parsed.value().symbol() != underlying.identity.provider_symbol()
    {
        return Err(SchwabRestOptionPublicationError::MappingMismatch);
    }
    Ok(())
}

fn validate_resolved_identity(
    identity: &SchwabResolvedProviderIdentity,
    definition_revision: EvidenceDigest,
) -> Result<(), SchwabRestOptionPublicationError> {
    require_evidence(identity.resolution_evidence())?;
    require_evidence(definition_revision)
}

fn require_evidence(evidence: EvidenceDigest) -> Result<(), SchwabRestOptionPublicationError> {
    if evidence.algorithm() != DigestAlgorithm::Sha256 || evidence.bytes() == [0; 32] {
        Err(SchwabRestOptionPublicationError::InvalidEvidence)
    } else {
        Ok(())
    }
}

fn option_text(
    contract: &OptionContract,
    name: OptionContractField,
) -> Result<Option<&str>, SchwabRestOptionPublicationError> {
    match option_scalar(contract, name) {
        None | Some(NativeScalar::Null) => Ok(None),
        Some(NativeScalar::Text(value)) => Ok(Some(value)),
        Some(NativeScalar::Bool(_) | NativeScalar::Number(_)) => {
            Err(SchwabRestOptionPublicationError::InvalidEvidence)
        }
    }
}

fn option_scalar(contract: &OptionContract, name: OptionContractField) -> Option<&NativeScalar> {
    contract
        .fields()
        .iter()
        .find(|field| field.name() == &name)
        .map(|field| field.value())
}

fn contract_expiration(
    contract: &OptionContract,
) -> Result<CalendarDate, SchwabRestOptionDispositionReason> {
    let date = contract
        .expiration_group()
        .split_once(':')
        .map_or(contract.expiration_group(), |(date, _)| date);
    calendar_date(date).map_err(|_| SchwabRestOptionDispositionReason::InvalidRequiredTerms)
}

fn contract_strike(
    contract: &OptionContract,
) -> Result<Decimal, SchwabRestOptionDispositionReason> {
    contract
        .strike_group()
        .parse::<Decimal>()
        .ok()
        .filter(|value| !value.is_sign_negative())
        .map(|value| value.normalize())
        .ok_or(SchwabRestOptionDispositionReason::InvalidRequiredTerms)
}

fn contract_kind(
    contract: &OptionContract,
) -> Result<OptionKind, SchwabRestOptionDispositionReason> {
    Ok(match contract.side() {
        OptionSide::Call => OptionKind::Call,
        OptionSide::Put => OptionKind::Put,
    })
}

fn validate_optional_contract_terms(
    contract: &OptionContract,
    expiration: CalendarDate,
    strike: Decimal,
    kind: OptionKind,
) -> Result<(), SchwabRestOptionDispositionReason> {
    if let Some(scalar) = option_scalar(contract, OptionContractField::PutCall) {
        let expected = match kind {
            OptionKind::Call => "CALL",
            OptionKind::Put => "PUT",
        };
        if !matches!(scalar, NativeScalar::Text(value) if value.eq_ignore_ascii_case(expected)) {
            return Err(SchwabRestOptionDispositionReason::InvalidRequiredTerms);
        }
    }
    if let Some(scalar) = option_scalar(contract, OptionContractField::StrikePrice) {
        if !matches!(scalar, NativeScalar::Number(value) if parse_decimal(value) == Some(strike)) {
            return Err(SchwabRestOptionDispositionReason::InvalidRequiredTerms);
        }
    }
    if let Some(scalar) = option_scalar(contract, OptionContractField::ExpirationDate) {
        let NativeScalar::Text(value) = scalar else {
            return Err(SchwabRestOptionDispositionReason::InvalidRequiredTerms);
        };
        let provider_date = value.get(..10).and_then(|value| calendar_date(value).ok());
        if provider_date != Some(expiration) {
            return Err(SchwabRestOptionDispositionReason::InvalidRequiredTerms);
        }
    }
    Ok(())
}

fn required_positive_decimal(
    contract: &OptionContract,
    name: OptionContractField,
) -> Result<Decimal, SchwabRestOptionDispositionReason> {
    match option_scalar(contract, name) {
        Some(NativeScalar::Number(value)) => parse_decimal(value)
            .filter(|value| *value > Decimal::ZERO)
            .ok_or(SchwabRestOptionDispositionReason::InvalidRequiredTerms),
        _ => Err(SchwabRestOptionDispositionReason::InvalidRequiredTerms),
    }
}

fn decimal_component(
    contract: &OptionContract,
    name: OptionContractField,
    source_at: Option<Timestamp>,
) -> OptionComponent<Decimal> {
    match option_scalar(contract, name) {
        None => OptionComponent::unavailable(OptionComponentState::ProviderAbsent, source_at),
        Some(NativeScalar::Null) => {
            OptionComponent::unavailable(OptionComponentState::ProviderNull, source_at)
        }
        Some(NativeScalar::Number(value)) => match parse_decimal(value) {
            Some(value) => OptionComponent::observed(value, source_at),
            None => OptionComponent::unavailable(OptionComponentState::Invalid, source_at),
        },
        Some(NativeScalar::Bool(_) | NativeScalar::Text(_)) => {
            OptionComponent::unavailable(OptionComponentState::Invalid, source_at)
        }
    }
}

fn money_component(
    contract: &OptionContract,
    name: OptionContractField,
    currency: Currency,
    source_at: Option<Timestamp>,
) -> OptionComponent<Money> {
    match decimal_component(contract, name, source_at) {
        OptionComponent::Observed { value, source_at } if !value.is_sign_negative() => {
            OptionComponent::observed(Money::new(value, currency), source_at)
        }
        OptionComponent::Observed { source_at, .. } => {
            OptionComponent::unavailable(OptionComponentState::Invalid, source_at)
        }
        OptionComponent::Unavailable { reason, source_at } => {
            OptionComponent::unavailable(reason, source_at)
        }
    }
}

fn quantity_component(
    contract: &OptionContract,
    name: OptionContractField,
    source_at: Option<Timestamp>,
) -> OptionComponent<QuantityLots> {
    match decimal_component(contract, name, source_at) {
        OptionComponent::Observed { value, source_at } => value
            .to_i64()
            .filter(|lots| Decimal::from(*lots) == value)
            .and_then(|lots| QuantityLots::new(lots).ok())
            .map_or_else(
                || OptionComponent::unavailable(OptionComponentState::Invalid, source_at),
                |value| OptionComponent::observed(value, source_at),
            ),
        OptionComponent::Unavailable { reason, source_at } => {
            OptionComponent::unavailable(reason, source_at)
        }
    }
}

fn unsigned_component(
    contract: &OptionContract,
    name: OptionContractField,
    source_at: Option<Timestamp>,
) -> OptionComponent<u64> {
    match decimal_component(contract, name, source_at) {
        OptionComponent::Observed { value, source_at } => value
            .to_u64()
            .filter(|number| Decimal::from(*number) == value)
            .map_or_else(
                || OptionComponent::unavailable(OptionComponentState::Invalid, source_at),
                |value| OptionComponent::observed(value, source_at),
            ),
        OptionComponent::Unavailable { reason, source_at } => {
            OptionComponent::unavailable(reason, source_at)
        }
    }
}

fn settlement_component(contract: &OptionContract) -> OptionComponent<OptionSettlementKind> {
    match option_scalar(contract, OptionContractField::SettlementType) {
        None => OptionComponent::unavailable(OptionComponentState::ProviderAbsent, None),
        Some(NativeScalar::Null) => {
            OptionComponent::unavailable(OptionComponentState::ProviderNull, None)
        }
        Some(NativeScalar::Text(value)) => SourceIdentifier::try_from(value.as_ref())
            .map(OptionSettlementKind::Other)
            .map_or_else(
                |_| OptionComponent::unavailable(OptionComponentState::Invalid, None),
                |value| OptionComponent::observed(value, None),
            ),
        Some(NativeScalar::Bool(_) | NativeScalar::Number(_)) => {
            OptionComponent::unavailable(OptionComponentState::Invalid, None)
        }
    }
}

fn option_timestamp(
    contract: &OptionContract,
    name: OptionContractField,
    received_at: Timestamp,
) -> Result<Option<Timestamp>, SchwabRestOptionDispositionReason> {
    let value = match option_scalar(contract, name) {
        None | Some(NativeScalar::Null) => return Ok(None),
        Some(NativeScalar::Number(value)) => value,
        Some(NativeScalar::Bool(_) | NativeScalar::Text(_)) => {
            return Err(SchwabRestOptionDispositionReason::InvalidComponentClock);
        }
    };
    let timestamp = value
        .as_str()
        .parse::<u64>()
        .ok()
        .filter(|value| *value != 0)
        .and_then(|value| i64::try_from(value).ok())
        .and_then(|value| value.checked_mul(1_000_000))
        .map(Timestamp::from_unix_nanos)
        .filter(|timestamp| *timestamp <= received_at)
        .ok_or(SchwabRestOptionDispositionReason::InvalidComponentClock)?;
    Ok(Some(timestamp))
}

fn parse_decimal(value: &NativeNumber) -> Option<Decimal> {
    value
        .as_str()
        .parse::<Decimal>()
        .ok()
        .map(|value| value.normalize())
}

fn native_u64(value: &NativeField<u64>) -> Option<u64> {
    match value {
        NativeField::Value(value) => Some(*value),
        NativeField::Absent | NativeField::Null => None,
    }
}

fn native_bool_component(value: &NativeField<bool>) -> OptionComponent<bool> {
    match value {
        NativeField::Absent => {
            OptionComponent::unavailable(OptionComponentState::ProviderAbsent, None)
        }
        NativeField::Null => OptionComponent::unavailable(OptionComponentState::ProviderNull, None),
        NativeField::Value(value) => OptionComponent::observed(*value, None),
    }
}

fn expiration_class(value: &NativeField<Box<str>>) -> OptionComponent<OptionExpirationClass> {
    match value {
        NativeField::Absent => {
            OptionComponent::unavailable(OptionComponentState::ProviderAbsent, None)
        }
        NativeField::Null => OptionComponent::unavailable(OptionComponentState::ProviderNull, None),
        NativeField::Value(value) => SourceIdentifier::try_from(value.as_ref())
            .map(OptionExpirationClass::Other)
            .map_or_else(
                |_| OptionComponent::unavailable(OptionComponentState::Invalid, None),
                |value| OptionComponent::observed(value, None),
            ),
    }
}

fn calendar_date(value: &str) -> Result<CalendarDate, SchwabRestOptionPublicationError> {
    let mut parts = value.split('-');
    let year = parts
        .next()
        .and_then(|value| value.parse::<u16>().ok())
        .ok_or(SchwabRestOptionPublicationError::InvalidEvidence)?;
    let month = parts
        .next()
        .and_then(|value| value.parse::<u8>().ok())
        .ok_or(SchwabRestOptionPublicationError::InvalidEvidence)?;
    let day = parts
        .next()
        .and_then(|value| value.parse::<u8>().ok())
        .ok_or(SchwabRestOptionPublicationError::InvalidEvidence)?;
    if parts.next().is_some() {
        return Err(SchwabRestOptionPublicationError::InvalidEvidence);
    }
    CalendarDate::new(year, month, day)
        .map_err(|_| SchwabRestOptionPublicationError::InvalidEvidence)
}

fn request_symbol(url: &str) -> Result<String, SchwabRestOptionPublicationError> {
    let url =
        url::Url::parse(url).map_err(|_| SchwabRestOptionPublicationError::InvalidEvidence)?;
    let mut symbol = None;
    for (key, value) in url.query_pairs() {
        if key == "symbol" {
            if symbol.replace(value.into_owned()).is_some() {
                return Err(SchwabRestOptionPublicationError::InvalidEvidence);
            }
        }
    }
    symbol.ok_or(SchwabRestOptionPublicationError::InvalidEvidence)
}

fn option_chain_filter(
    url: &str,
    currency: Currency,
) -> Result<OptionMarketRequestFilter, SchwabRestOptionPublicationError> {
    let url =
        url::Url::parse(url).map_err(|_| SchwabRestOptionPublicationError::InvalidEvidence)?;
    let mut from = None;
    let mut to = None;
    let mut strike = None;
    let mut kind = None;
    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "symbol" => {}
            "fromDate" if from.is_none() => from = Some(calendar_date(&value)?),
            "toDate" if to.is_none() => to = Some(calendar_date(&value)?),
            "strike" if strike.is_none() => {
                strike = Some(
                    value
                        .parse::<Decimal>()
                        .map_err(|_| SchwabRestOptionPublicationError::InvalidEvidence)?
                        .normalize(),
                )
            }
            "contractType" if kind.is_none() => {
                kind = match value.as_ref() {
                    "CALL" => Some(Some(OptionKind::Call)),
                    "PUT" => Some(Some(OptionKind::Put)),
                    "ALL" => Some(None),
                    _ => return Err(SchwabRestOptionPublicationError::InvalidEvidence),
                }
            }
            "fromDate" | "toDate" | "strike" | "contractType" => {
                return Err(SchwabRestOptionPublicationError::InvalidEvidence);
            }
            _ => return Err(SchwabRestOptionPublicationError::InvalidEvidence),
        }
    }
    let expiration_range = match (from, to) {
        (None, None) => None,
        (Some(start), Some(end)) => Some(OptionExpirationRange::try_new(start, end)?),
        _ => return Err(SchwabRestOptionPublicationError::InvalidEvidence),
    };
    let strike_range = strike
        .map(|value| {
            let money = Money::new(value, currency);
            OptionStrikeRange::try_new(money, money)
        })
        .transpose()?;
    OptionMarketRequestFilter::try_new(expiration_range, strike_range, kind.flatten(), Vec::new())
        .map_err(Into::into)
}

fn require_unfiltered_expiration_request(
    url: &str,
) -> Result<(), SchwabRestOptionPublicationError> {
    let url =
        url::Url::parse(url).map_err(|_| SchwabRestOptionPublicationError::InvalidEvidence)?;
    for (key, _) in url.query_pairs() {
        if key != "symbol" {
            return Err(SchwabRestOptionPublicationError::InvalidEvidence);
        }
    }
    Ok(())
}

fn timestamp_from_millis(value: u64) -> Result<Timestamp, SchwabRestOptionPublicationError> {
    i64::try_from(value)
        .ok()
        .and_then(|value| value.checked_mul(1_000_000))
        .map(Timestamp::from_unix_nanos)
        .ok_or(SchwabRestOptionPublicationError::InvalidEvidence)
}

fn contract_disposition(
    ordinal: u32,
    contract: &OptionContract,
    symbol: Option<&str>,
    reason: SchwabRestOptionDispositionReason,
) -> SchwabRestOptionDisposition {
    SchwabRestOptionDisposition {
        provider_record_ordinal: ordinal,
        provider_symbol: symbol.map(Into::into),
        expiration_group: contract.expiration_group().into(),
        strike_group: Some(contract.strike_group().into()),
        reason,
    }
}

fn expiration_disposition(
    ordinal: u32,
    expiration: &ExpirationEntry,
    request: &SchwabRestOptionPublicationRequest,
    reason: SchwabRestOptionDispositionReason,
) -> SchwabRestOptionDisposition {
    SchwabRestOptionDisposition {
        provider_record_ordinal: ordinal,
        provider_symbol: Some(
            request
                .underlying
                .identity
                .provider_symbol()
                .as_str()
                .into(),
        ),
        expiration_group: expiration.expiration_date.clone(),
        strike_group: None,
        reason,
    }
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct SchwabOptionNativeRowV1<'a> {
    version: u16,
    family: &'static str,
    provider_record_ordinal: u32,
    side: &'static str,
    expiration_group: &'a str,
    strike_group: &'a str,
    fields: Vec<SchwabOptionNativeFieldV1<'a>>,
    option_instrument_id: InstrumentId,
    option_definition_revision: EvidenceDigest,
    provider_instrument_id: &'a str,
    resolution_evidence: EvidenceDigest,
    underlying_instrument_id: InstrumentId,
    underlying_definition_revision: EvidenceDigest,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct SchwabOptionNativeFieldV1<'a> {
    name: &'static str,
    value: SchwabNativeValueV1<'a>,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
enum SchwabNativeValueV1<'a> {
    Absent,
    Null,
    Bool(bool),
    Unsigned(u64),
    Number(&'a str),
    Text(&'a str),
}

fn encode_snapshot_native_row(
    ordinal: u32,
    contract: &OptionContract,
    input: &SchwabRestOptionContractRequest,
    request: &SchwabRestOptionPublicationRequest,
) -> Result<Bytes, SchwabRestOptionPublicationError> {
    let fields = contract
        .fields()
        .iter()
        .map(|field| SchwabOptionNativeFieldV1 {
            name: option_field_name(*field.name()),
            value: native_scalar(field.value()),
        })
        .collect();
    serde_json::to_vec(&SchwabOptionNativeRowV1 {
        version: 1,
        family: "schwab.rest.option-snapshot",
        provider_record_ordinal: ordinal,
        side: match contract.side() {
            OptionSide::Call => "call",
            OptionSide::Put => "put",
        },
        expiration_group: contract.expiration_group(),
        strike_group: contract.strike_group(),
        fields,
        option_instrument_id: input.instrument_id,
        option_definition_revision: input.definition_revision,
        provider_instrument_id: input.identity.provider_instrument_id().as_str(),
        resolution_evidence: input.identity.resolution_evidence(),
        underlying_instrument_id: request.underlying.instrument_id,
        underlying_definition_revision: request.underlying.definition_revision,
    })
    .map(Bytes::from)
    .map_err(|_| SchwabRestOptionPublicationError::NativeEncoding)
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct SchwabExpirationNativeRowV1<'a> {
    version: u16,
    family: &'static str,
    provider_record_ordinal: u32,
    expiration_date: &'a str,
    days_to_expiration: u64,
    expiration_type: SchwabNativeValueV1<'a>,
    standard: SchwabNativeValueV1<'a>,
    underlying_instrument_id: InstrumentId,
    underlying_definition_revision: EvidenceDigest,
    provider_instrument_id: &'a str,
    resolution_evidence: EvidenceDigest,
}

fn encode_expiration_native_row(
    ordinal: u32,
    expiration: &ExpirationEntry,
    request: &SchwabRestOptionPublicationRequest,
) -> Result<Bytes, SchwabRestOptionPublicationError> {
    serde_json::to_vec(&SchwabExpirationNativeRowV1 {
        version: 1,
        family: "schwab.rest.option-expiration",
        provider_record_ordinal: ordinal,
        expiration_date: &expiration.expiration_date,
        days_to_expiration: expiration.days_to_expiration,
        expiration_type: native_text_field(&expiration.expiration_type),
        standard: native_bool_field(&expiration.standard),
        underlying_instrument_id: request.underlying.instrument_id,
        underlying_definition_revision: request.underlying.definition_revision,
        provider_instrument_id: request
            .underlying
            .identity
            .provider_instrument_id()
            .as_str(),
        resolution_evidence: request.underlying.identity.resolution_evidence(),
    })
    .map(Bytes::from)
    .map_err(|_| SchwabRestOptionPublicationError::NativeEncoding)
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct SchwabOptionNativeSidecarV1<'a> {
    version: u16,
    family: &'static str,
    service: &'static str,
    route: &'static str,
    provider_schema: &'a str,
    provider_schema_version: u16,
    request_url: &'a str,
    request_sha256: [u8; 32],
    response_sha256: [u8; 32],
    response_status: u16,
    response_bytes: u64,
    declared_response_bytes: Option<u64>,
    received_at_unix_millis: u64,
    latency_millis: u64,
    token_generation: u64,
    response_headers: Vec<SchwabOptionHeaderV1<'a>>,
    requested_items: u64,
    returned_items: u64,
    missing_items: u64,
    unexpected_items: u64,
    provider_records: u64,
    feed: &'a str,
    reference_venue: Option<&'a str>,
    provider_reported_venue: Option<&'a str>,
    depth: SchwabMarketDataDepth,
    delay: SchwabMarketDataDelay,
    provider_product: &'a str,
    provider_channel: &'a str,
    currency: &'a str,
    qualification_evidence: EvidenceDigest,
    qualification_receipt_evidence: EvidenceDigest,
    qualification_family: SchwabMarketDataFamily,
    qualification_observed_at: Timestamp,
    qualification_response_observed_at: Timestamp,
    entitlement_evidence: EvidenceDigest,
    capability_evidence: EvidenceDigest,
    chain: Option<SchwabOptionChainSemanticsV1<'a>>,
    unknown_field_count: usize,
    unknown_field_bytes: usize,
    unknown_field_paths: &'a [Box<str>],
    unknown_field_digest: [u8; 32],
    dispositions: &'a [SchwabRestOptionDisposition],
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct SchwabOptionHeaderV1<'a> {
    name: &'a str,
    value: &'a [u8],
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct SchwabOptionChainSemanticsV1<'a> {
    underlying_symbol: &'a str,
    status: SchwabNativeValueV1<'a>,
    strategy: SchwabNativeValueV1<'a>,
    underlying_price: SchwabNativeValueV1<'a>,
    volatility: SchwabNativeValueV1<'a>,
    interest_rate: SchwabNativeValueV1<'a>,
    days_to_expiration: SchwabNativeValueV1<'a>,
    number_of_contracts: SchwabNativeValueV1<'a>,
}

fn encode_sidecar(
    parts: &SchwabSealedRestResponseParts,
    market: &SchwabRestOptionMarketDataEvidence,
    dispositions: &[SchwabRestOptionDisposition],
) -> Result<Bytes, SchwabRestOptionPublicationError> {
    let (family, provider_schema, provider_schema_version, unknown, chain) = match &parts.payload {
        SchwabRestPayload::OptionChain(parsed) => (
            "schwab.rest.option-chain",
            parsed.schema_name(),
            parsed.schema_version(),
            parsed.unknown_fields(),
            Some(SchwabOptionChainSemanticsV1 {
                underlying_symbol: parsed.value().symbol().as_str(),
                status: native_text_field(parsed.value().status()),
                strategy: native_text_field(parsed.value().strategy()),
                underlying_price: native_number_field(parsed.value().underlying_price()),
                volatility: native_number_field(parsed.value().volatility()),
                interest_rate: native_number_field(parsed.value().interest_rate()),
                days_to_expiration: native_u64_field(parsed.value().days_to_expiration()),
                number_of_contracts: native_u64_field(parsed.value().number_of_contracts()),
            }),
        ),
        SchwabRestPayload::Expirations(parsed) => (
            "schwab.rest.expiration-chain",
            parsed.schema_name(),
            parsed.schema_version(),
            parsed.unknown_fields(),
            None,
        ),
        _ => return Err(SchwabRestOptionPublicationError::FamilyMismatch),
    };
    let response_headers = parts
        .receipt
        .headers()
        .iter()
        .map(|header| SchwabOptionHeaderV1 {
            name: header.name(),
            value: header.value(),
        })
        .collect();
    serde_json::to_vec(&SchwabOptionNativeSidecarV1 {
        version: 1,
        family,
        service: "schwab-market-data-rest",
        route: match parts.receipt.route() {
            ReadOnlyRoute::Chains => "chains",
            ReadOnlyRoute::ExpirationChain => "expiration-chain",
            _ => return Err(SchwabRestOptionPublicationError::FamilyMismatch),
        },
        provider_schema,
        provider_schema_version,
        request_url: parts.receipt.request_url(),
        request_sha256: parts.receipt.request_sha256(),
        response_sha256: parts.receipt.body_sha256(),
        response_status: parts.receipt.status(),
        response_bytes: parts.receipt.body_bytes(),
        declared_response_bytes: parts.receipt.declared_body_bytes(),
        received_at_unix_millis: parts.receipt.received_at_unix_millis(),
        latency_millis: parts.receipt.latency_ms(),
        token_generation: parts.receipt.token_generation().get(),
        response_headers,
        requested_items: parts.accounting.requested,
        returned_items: parts.accounting.returned,
        missing_items: parts.accounting.missing,
        unexpected_items: parts.accounting.unexpected,
        provider_records: parts.accounting.provider_records,
        feed: market.feed().as_str(),
        reference_venue: market.reference_venue_id.as_ref().map(VenueId::as_str),
        provider_reported_venue: None,
        depth: market.depth(),
        delay: market.delay(),
        provider_product: market.provider_product().as_source_identifier().as_str(),
        provider_channel: market.provider_channel().as_source_identifier().as_str(),
        currency: market.currency.as_str(),
        qualification_evidence: market.qualification_evidence(),
        qualification_receipt_evidence: market.qualification.receipt_evidence(),
        qualification_family: market.qualification.family(),
        qualification_observed_at: market.qualification.family_observed_at(),
        qualification_response_observed_at: market.qualification.response_observed_at(),
        entitlement_evidence: market.entitlement_evidence(),
        capability_evidence: market.capability_evidence(),
        chain,
        unknown_field_count: unknown.field_count(),
        unknown_field_bytes: unknown.encoded_bytes(),
        unknown_field_paths: unknown.paths(),
        unknown_field_digest: unknown.digest(),
        dispositions,
    })
    .map(Bytes::from)
    .map_err(|_| SchwabRestOptionPublicationError::NativeEncoding)
}

fn native_scalar(value: &NativeScalar) -> SchwabNativeValueV1<'_> {
    match value {
        NativeScalar::Null => SchwabNativeValueV1::Null,
        NativeScalar::Bool(value) => SchwabNativeValueV1::Bool(*value),
        NativeScalar::Number(value) => SchwabNativeValueV1::Number(value.as_str()),
        NativeScalar::Text(value) => SchwabNativeValueV1::Text(value),
    }
}

fn native_text_field(value: &NativeField<Box<str>>) -> SchwabNativeValueV1<'_> {
    match value {
        NativeField::Absent => SchwabNativeValueV1::Absent,
        NativeField::Null => SchwabNativeValueV1::Null,
        NativeField::Value(value) => SchwabNativeValueV1::Text(value),
    }
}

fn native_number_field(value: &NativeField<NativeNumber>) -> SchwabNativeValueV1<'_> {
    match value {
        NativeField::Absent => SchwabNativeValueV1::Absent,
        NativeField::Null => SchwabNativeValueV1::Null,
        NativeField::Value(value) => SchwabNativeValueV1::Number(value.as_str()),
    }
}

fn native_u64_field(value: &NativeField<u64>) -> SchwabNativeValueV1<'_> {
    match value {
        NativeField::Absent => SchwabNativeValueV1::Absent,
        NativeField::Null => SchwabNativeValueV1::Null,
        NativeField::Value(value) => SchwabNativeValueV1::Unsigned(*value),
    }
}

fn native_bool_field(value: &NativeField<bool>) -> SchwabNativeValueV1<'_> {
    match value {
        NativeField::Absent => SchwabNativeValueV1::Absent,
        NativeField::Null => SchwabNativeValueV1::Null,
        NativeField::Value(value) => SchwabNativeValueV1::Bool(*value),
    }
}

const fn option_field_name(field: OptionContractField) -> &'static str {
    match field {
        OptionContractField::PutCall => "putCall",
        OptionContractField::Symbol => "symbol",
        OptionContractField::Description => "description",
        OptionContractField::ExchangeName => "exchangeName",
        OptionContractField::Bid => "bid",
        OptionContractField::Ask => "ask",
        OptionContractField::Last => "last",
        OptionContractField::Mark => "mark",
        OptionContractField::BidSize => "bidSize",
        OptionContractField::AskSize => "askSize",
        OptionContractField::LastSize => "lastSize",
        OptionContractField::HighPrice => "highPrice",
        OptionContractField::LowPrice => "lowPrice",
        OptionContractField::OpenPrice => "openPrice",
        OptionContractField::ClosePrice => "closePrice",
        OptionContractField::TotalVolume => "totalVolume",
        OptionContractField::TradeDate => "tradeDate",
        OptionContractField::QuoteTimeInLong => "quoteTimeInLong",
        OptionContractField::TradeTimeInLong => "tradeTimeInLong",
        OptionContractField::NetChange => "netChange",
        OptionContractField::Volatility => "volatility",
        OptionContractField::Delta => "delta",
        OptionContractField::Gamma => "gamma",
        OptionContractField::Theta => "theta",
        OptionContractField::Vega => "vega",
        OptionContractField::Rho => "rho",
        OptionContractField::OpenInterest => "openInterest",
        OptionContractField::TimeValue => "timeValue",
        OptionContractField::TheoreticalOptionValue => "theoreticalOptionValue",
        OptionContractField::TheoreticalVolatility => "theoreticalVolatility",
        OptionContractField::StrikePrice => "strikePrice",
        OptionContractField::ExpirationDate => "expirationDate",
        OptionContractField::DaysToExpiration => "daysToExpiration",
        OptionContractField::ExpirationType => "expirationType",
        OptionContractField::LastTradingDay => "lastTradingDay",
        OptionContractField::Multiplier => "multiplier",
        OptionContractField::SettlementType => "settlementType",
        OptionContractField::DeliverableNote => "deliverableNote",
        OptionContractField::PercentChange => "percentChange",
        OptionContractField::MarkChange => "markChange",
        OptionContractField::MarkPercentChange => "markPercentChange",
        OptionContractField::InTheMoney => "inTheMoney",
        OptionContractField::Mini => "mini",
        OptionContractField::NonStandard => "nonStandard",
    }
}

/// Secret-free Schwab option response publication failure.
#[derive(Debug, Error)]
pub enum SchwabRestOptionPublicationError {
    #[error("sealed Schwab REST response is not an option-chain or expiration response")]
    FamilyMismatch,
    #[error("Schwab REST option publication evidence is invalid")]
    InvalidEvidence,
    #[error("Schwab REST option mapping inputs do not match the sealed response")]
    MappingMismatch,
    #[error("Schwab REST option provider-native evidence could not be encoded")]
    NativeEncoding,
    #[error(transparent)]
    Capture(#[from] ProviderCaptureError),
}
