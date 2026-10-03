//! Canonical action normalization and native coverage bound to the common sealed extraction.

use std::num::NonZeroU32;

use bytes::Bytes;
use market_squawk_domain::{
    AvailabilityEvidence as ResearchAvailability, CalendarDate,
    CorporateActionEventInstrumentIdentity, CorporateActionKind, CorporateActionObservation,
    CorporateActionQueryInstrumentIdentity, CorporateActionSourceCategory,
    CorporateActionSourceDates, CorporateActionSourceDisposition, CorporateActionSourceObservation,
    CorporateActionSourceObservationInput, CorporateActionSourcePayload,
    CorporateActionSourceQueryContract, CorporateActionSourceScope, Currency, EffectiveInterval,
    EvidenceDigest, ExactPayloadEvidence, InstrumentId, MergerConsideration, Money, PayloadHash,
    PayloadReference, ResearchContext, ResearchObservation, ResearchProvenance,
    ResearchProvenanceInput, ResearchTemporalCoordinate, ResearchTime, RevisionNumber,
    SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    AvailabilityEvidence, CURRENT_RESEARCH_RECORD_SCHEMA, DiscoveryRequest, ExtractionBatch,
    ExtractionBatchAccumulator, ExtractionRecord, ExtractionRequest, ExtractionRevisionPlan,
    ProviderNativeLineageBatchBuilder, ProviderNativeLineageImplementation,
    ProviderWholeCaptureToken, SealedProviderCaptureBinding, SourceMetadata, SourceObject,
    SourceObjectCaptureIdentity,
};
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::Number;
use uuid::Uuid;

use super::AlpacaCorporateActionInstrument;
use super::native::{CashSubtype, NativeAction};
use super::{
    AlpacaCorporateActionCategory as Category, AlpacaCorporateActionsRequest, Page, lower_hex,
    sha256,
};
use crate::AlpacaError;

const MEDIA_TYPE: &str = "application/vnd.market-squawk.alpaca-corporate-actions+json";

/// Canonical catalog resolution bound to one action ID and its source effective date.
#[derive(Clone, Debug)]
pub struct AlpacaCorporateActionIdentity {
    action_id: Uuid,
    effective_date: CalendarDate,
    subject: AlpacaCorporateActionInstrument,
    related: Option<AlpacaCorporateActionInstrument>,
}

impl AlpacaCorporateActionIdentity {
    /// Retains exact mappings from the existing source-qualified catalog resolver.
    /// The application must resolve historical mappings for the action's date and knowledge
    /// boundary; current ticker lookup is not an admissible substitute.
    pub fn try_new(
        action_id: Uuid,
        effective_date: CalendarDate,
        subject: AlpacaCorporateActionInstrument,
        related: Option<AlpacaCorporateActionInstrument>,
    ) -> Result<Self, AlpacaError> {
        if action_id.is_nil()
            || related
                .as_ref()
                .is_some_and(|value| value.instrument() == subject.instrument())
        {
            return Err(AlpacaError::InvalidCoverage);
        }
        Ok(Self {
            action_id,
            effective_date,
            subject,
            related,
        })
    }
}

/// Explicit source/normalization disposition for every returned native action.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AlpacaCorporateActionDisposition {
    /// Existing canonical action semantics and exact source terms were admitted.
    Normalized,
    /// Exact historical source-qualified instrument resolution is absent.
    MissingIdentity,
    /// No ex/effective date exists; processing or payment date cannot substitute.
    MissingEffectiveDate,
    /// The provider supplied no monetary currency; no implicit USD assumption is made.
    MissingCurrency,
    /// Required economics are absent or cannot be represented exactly.
    MissingOrInvalidTerms,
    /// The existing canonical action taxonomy cannot yet represent the complete economics.
    UnsupportedCanonicalEconomics,
    /// A source due-bill interval requires an entitlement convention beyond the existing action.
    DueBillEntitlementRequired,
}

#[derive(Clone, Debug, Serialize)]
struct ActionDisposition {
    action_id: Uuid,
    category: Category,
    instrument_id: Option<InstrumentId>,
    disposition: AlpacaCorporateActionDisposition,
    effective_date: Option<CalendarDate>,
    received_at: Timestamp,
}

/// Source-minted evidence for an exhausted processing-date query and every returned disposition.
/// This never asserts that the source contains every economic action in an ex-date interval.
#[derive(Clone, Debug, Serialize)]
pub struct AlpacaCorporateActionsCoverage {
    request: AlpacaCorporateActionsRequest,
    query_instruments: Vec<CorporateActionQueryInstrumentIdentity>,
    capture_observation_digest: EvidenceDigest,
    sealed_capture_receipt_digest: EvidenceDigest,
    category_counts: [u32; 16],
    actions: Vec<ActionDisposition>,
    page_count: usize,
    received_at: Timestamp,
}

impl AlpacaCorporateActionsCoverage {
    /// Returns the all-category, all-quality query scope; its dates mean provider processing.
    pub const fn request(&self) -> &AlpacaCorporateActionsRequest {
        &self.request
    }
    /// Returns the sealed exact page graph identity.
    pub const fn capture_observation_digest(&self) -> EvidenceDigest {
        self.capture_observation_digest
    }
    /// Returns complete captured request cardinality, including zero.
    pub fn returned_actions(&self) -> usize {
        self.actions.len()
    }
    /// Returns whether unresolved returned actions prevent economic completeness.
    pub fn has_unresolved_actions(&self) -> bool {
        self.actions
            .iter()
            .any(|action| action.disposition != AlpacaCorporateActionDisposition::Normalized)
    }
    /// Returns the actual final acquisition clock, distinct from all historical dates.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }
    /// Returns exact per-action dispositions without provider-native payloads.
    pub fn dispositions(
        &self,
    ) -> impl Iterator<
        Item = (
            Uuid,
            Category,
            Option<InstrumentId>,
            AlpacaCorporateActionDisposition,
            Option<CalendarDate>,
        ),
    > {
        self.actions.iter().map(|a| {
            (
                a.action_id,
                a.category,
                a.instrument_id,
                a.disposition,
                a.effective_date,
            )
        })
    }
    /// Returns every reviewed category and its exact observed count, including zero counts.
    pub fn category_counts(&self) -> impl Iterator<Item = (Category, u32)> + '_ {
        Category::ALL
            .into_iter()
            .map(|category| (category, self.category_counts[category.index()]))
    }
}

/// Physically sealed source response awaiting exact catalog resolution and research publication.
#[derive(Debug)]
pub struct AlpacaPreparedCorporateActionsPublication {
    authority: ProviderWholeCaptureToken,
    metadata: SourceMetadata,
    request: AlpacaCorporateActionsRequest,
    dataset: SourceIdentifier,
    pages: Vec<Page>,
}

impl AlpacaPreparedCorporateActionsPublication {
    pub(super) fn new(
        authority: ProviderWholeCaptureToken,
        metadata: SourceMetadata,
        request: AlpacaCorporateActionsRequest,
        dataset: SourceIdentifier,
        pages: Vec<Page>,
    ) -> Self {
        Self {
            authority,
            metadata,
            request,
            dataset,
            pages,
        }
    }

    /// Returns source metadata for the existing application ingestion authority.
    pub const fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }
    /// Returns the source-owned request dataset identity.
    pub const fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }

    /// Creates the exact discovery object only after physical capture sealing succeeded.
    pub fn source_object(&self, request: &DiscoveryRequest) -> Result<SourceObject, AlpacaError> {
        if request.dataset() != &self.dataset || request.effective_at().is_some() {
            return Err(AlpacaError::InvalidCoverage);
        }
        let capture = self.authority.persisted_receipt().capture();
        let received_at = self
            .pages
            .last()
            .ok_or(AlpacaError::CaptureMaterial)?
            .received_at;
        SourceObject::try_new_with_capture_identity(
            self.metadata.source_id().clone(),
            self.metadata.revision().clone(),
            request,
            SourceIdentifier::try_from(format!(
                "alpaca:corporate-actions:object:{}",
                lower_hex(capture.content_digest().bytes())
            ))?,
            SourceIdentifier::try_from(MEDIA_TYPE)?,
            ExactPayloadEvidence::from_content_digest(capture.content_digest()),
            SourceObjectCaptureIdentity::try_from_capture(capture)
                .map_err(|_| AlpacaError::CaptureMaterial)?,
            EffectiveInterval::new(received_at, None).map_err(|_| AlpacaError::Protocol)?,
            None,
            AvailabilityEvidence::LocalFirstObserved {
                observed_at: received_at,
            },
            Some(capture.total_body_bytes()),
        )
        .map_err(|_| AlpacaError::CaptureMaterial)
    }

    /// Normalizes supported source economics and binds every native disposition to one immutable
    /// extraction, including a genuinely empty terminal result. Missing economics are retained in
    /// the sidecar and must block complete-economic-coverage admission by downstream selectors.
    pub fn try_into_binding(
        self,
        request: &ExtractionRequest,
        identities: &[AlpacaCorporateActionIdentity],
        query_instruments: &[CorporateActionQueryInstrumentIdentity],
        ingested_at: Timestamp,
    ) -> Result<(SealedProviderCaptureBinding, AlpacaCorporateActionsCoverage), AlpacaError> {
        let capture = self.authority.persisted_receipt().capture();
        let received_at = self
            .pages
            .last()
            .ok_or(AlpacaError::CaptureMaterial)?
            .received_at;
        let object = request.object();
        if query_instruments.len() != self.request.symbols().len()
            || query_instruments
                .iter()
                .zip(self.request.symbols())
                .any(|(identity, symbol)| {
                    identity.symbol.as_str() != symbol
                        || !identity.valid_for_capture(self.pages[0].received_at)
                })
            || ingested_at < received_at
            || object.source_id() != self.metadata.source_id()
            || object.metadata_revision() != self.metadata.revision()
            || object.dataset() != &self.dataset
            || object.object_id().as_str()
                != format!(
                    "alpaca:corporate-actions:object:{}",
                    lower_hex(capture.content_digest().bytes())
                )
            || object.media_type().as_str() != MEDIA_TYPE
            || object.evidence().content_digest() != capture.content_digest()
            || object.capture_identity()
                != SourceObjectCaptureIdentity::try_from_capture(capture)
                    .map_err(|_| AlpacaError::CaptureMaterial)?
            || object.expected_bytes() != Some(capture.total_body_bytes())
            || object.availability()
                != &(AvailabilityEvidence::LocalFirstObserved {
                    observed_at: received_at,
                })
            || object.effective_interval()
                != EffectiveInterval::new(received_at, None).map_err(|_| AlpacaError::Protocol)?
            || object.published_at().is_some()
        {
            return Err(AlpacaError::CaptureMaterial);
        }
        let total = self
            .pages
            .iter()
            .map(|page| page.actions.len())
            .sum::<usize>();
        if identities.len() > total {
            return Err(AlpacaError::InvalidCoverage);
        }
        for (index, identity) in identities.iter().enumerate() {
            if !identity.subject.retained().valid_for_event(ingested_at)
                || identity.subject.retained().source_id != *self.metadata.source_id()
                || identity.related.as_ref().is_some_and(|related| {
                    !related.retained().valid_for_event(ingested_at)
                        || related.retained().source_id != *self.metadata.source_id()
                })
                || identities[..index]
                    .iter()
                    .any(|prior| prior.action_id == identity.action_id)
                || !self.pages.iter().any(|page| {
                    page.actions.iter().any(|action| {
                        action.fields.id == identity.action_id
                            && action.effective_date() == Some(identity.effective_date)
                            && action.subject_symbol() == Some(identity.subject.symbol())
                            && identity.related.as_ref().is_none_or(|related| {
                                action.related_symbol() == Some(related.symbol())
                            })
                    })
                })
            {
                return Err(AlpacaError::InvalidCoverage);
            }
        }
        let mut coverage = AlpacaCorporateActionsCoverage {
            request: self.request.clone(),
            query_instruments: query_instruments.to_vec(),
            capture_observation_digest: capture.observation_digest(),
            sealed_capture_receipt_digest: self.authority.persisted_receipt().receipt_digest(),
            category_counts: [0; 16],
            actions: Vec::new(),
            page_count: self.pages.len(),
            received_at,
        };
        coverage
            .actions
            .try_reserve_exact(total)
            .map_err(|_| AlpacaError::Allocation)?;
        let mut accumulator = ExtractionBatchAccumulator::try_new(request)
            .map_err(|_| AlpacaError::CaptureMaterial)?;
        let mut normalized = Vec::new();
        normalized
            .try_reserve_exact(total)
            .map_err(|_| AlpacaError::Allocation)?;
        for page in &self.pages {
            for action in &page.actions {
                let identity = identities
                    .iter()
                    .find(|identity| identity.action_id == action.fields.id)
                    .filter(|identity| {
                        action.effective_date() == Some(identity.effective_date)
                            && action.subject_symbol() == Some(identity.subject.symbol())
                    });
                let result = normalize(action, identity, &self.metadata, page, ingested_at);
                let disposition = match result {
                    Ok(observation) => {
                        let effective = observation.context().time().effective().clone();
                        let payload =
                            serde_json::to_vec(&ResearchObservation::CorporateAction(observation))
                                .map(Bytes::from)
                                .map_err(|_| AlpacaError::Serialization)?;
                        let evidence = ExactPayloadEvidence::from_content_digest(sha256(&payload));
                        // Provider has no revision sequence. This content identity is admitted only
                        // through the common locally-observed revision planner below.
                        let revision = SourceIdentifier::try_from(format!(
                            "observed:{}",
                            lower_hex(evidence.content_digest().bytes())
                        ))?;
                        accumulator
                            .push(
                                ExtractionRecord::try_new_with_time(
                                    request,
                                    SourceIdentifier::try_from(CURRENT_RESEARCH_RECORD_SCHEMA)?,
                                    evidence,
                                    effective,
                                    None,
                                    AvailabilityEvidence::LocalFirstObserved {
                                        observed_at: page.received_at,
                                    },
                                    revision,
                                    None,
                                    payload,
                                )
                                .map_err(|_| AlpacaError::CaptureMaterial)?,
                            )
                            .map_err(|_| AlpacaError::CaptureMaterial)?;
                        normalized.push((
                            page,
                            action,
                            Some(identity.ok_or(AlpacaError::InvalidCoverage)?),
                            "economic",
                        ));
                        AlpacaCorporateActionDisposition::Normalized
                    }
                    Err(disposition) => disposition,
                };
                coverage.category_counts[action.category.index()] = coverage.category_counts
                    [action.category.index()]
                .checked_add(1)
                .ok_or(AlpacaError::Protocol)?;
                coverage.actions.push(ActionDisposition {
                    action_id: action.fields.id,
                    category: action.category,
                    instrument_id: identity.map(|value| value.subject.instrument()),
                    disposition,
                    effective_date: action.effective_date(),
                    received_at: page.received_at,
                });
                let source_row = source_action_observation(
                    action,
                    identity,
                    disposition,
                    &self.metadata,
                    page,
                    ingested_at,
                )?;
                push_source_record(&mut accumulator, request, source_row)?;
                normalized.push((page, action, identity, "source_disposition"));
            }
        }
        let summary = source_summary_observation(
            &coverage,
            &self.dataset,
            &self.metadata,
            self.pages.last().ok_or(AlpacaError::CaptureMaterial)?,
            ingested_at,
        )?;
        push_source_record(&mut accumulator, request, summary)?;
        let batch = accumulator
            .finish()
            .map_err(|_| AlpacaError::CaptureMaterial)?;
        let mut native = ProviderNativeLineageBatchBuilder::try_new(
            ProviderNativeLineageImplementation::AlpacaCorporateActionsV1,
            &batch,
        )
        .map_err(|_| AlpacaError::CaptureMaterial)?;
        native
            .try_set_batch_sidecar(&Sidecar {
                version: 1,
                scope: "all_types_all_quality_us_processing_dates",
                announcement_time_supplied: false,
                lifecycle_coverage_guaranteed: false,
                coverage: &coverage,
                pages: self
                    .pages
                    .iter()
                    .map(|page| NativePage {
                        ordinal: page.ordinal,
                        request_url: page.request_url.as_str(),
                        request_page_token: page.request_token.as_deref(),
                        response_next_page_token: page.next_token.as_deref(),
                        body_digest: page.body_digest,
                        body_bytes: page.body.len(),
                        received_at: page.received_at,
                        rate: page.rate,
                        actions: &page.actions,
                    })
                    .collect(),
            })
            .map_err(|_| AlpacaError::CaptureMaterial)?;
        let mut row_pages = Vec::new();
        row_pages
            .try_reserve_exact(normalized.len())
            .map_err(|_| AlpacaError::Allocation)?;
        for (page, action, identity, row_kind) in normalized {
            native
                .try_push(&NativeRow {
                    row_kind,
                    page_ordinal: page.ordinal,
                    action,
                    subject: identity.map(|value| value.subject.instrument()),
                    subject_identity: identity.map(|value| value.subject.retained()),
                    related: identity
                        .and_then(|value| value.related.as_ref())
                        .map(AlpacaCorporateActionInstrument::instrument),
                    related_identity: identity
                        .and_then(|value| value.related.as_ref())
                        .map(AlpacaCorporateActionInstrument::retained),
                })
                .map_err(|_| AlpacaError::CaptureMaterial)?;
            row_pages.push(page.ordinal);
        }
        native
            .try_push(&NativeSummaryRow {
                row_kind: "query_summary",
                capture_observation_digest: coverage.capture_observation_digest,
            })
            .map_err(|_| AlpacaError::CaptureMaterial)?;
        row_pages.push(
            self.pages
                .last()
                .ok_or(AlpacaError::CaptureMaterial)?
                .ordinal,
        );
        let native = native.finish().map_err(|_| AlpacaError::CaptureMaterial)?;
        let binding =
            SealedProviderCaptureBinding::try_whole(self.authority, batch, native, row_pages)
                .map_err(|_| AlpacaError::CaptureMaterial)?;
        Ok((binding, coverage))
    }

    /// Assigns source revisions using existing observed-content authority, never event dates.
    pub fn revision_plan(batch: &ExtractionBatch) -> Result<ExtractionRevisionPlan, AlpacaError> {
        if batch.request().object().media_type().as_str() != MEDIA_TYPE {
            return Err(AlpacaError::InvalidCoverage);
        }
        ExtractionRevisionPlan::locally_observed(batch.records().len())
            .map_err(|_| AlpacaError::Protocol)
    }
}

fn normalize(
    action: &NativeAction,
    identity: Option<&AlpacaCorporateActionIdentity>,
    metadata: &SourceMetadata,
    page: &Page,
    ingested_at: Timestamp,
) -> Result<CorporateActionObservation, AlpacaCorporateActionDisposition> {
    use AlpacaCorporateActionDisposition as D;
    let date = action.effective_date().ok_or(D::MissingEffectiveDate)?;
    let identity = identity.ok_or(D::MissingIdentity)?;
    if identity.effective_date != date || action.subject_symbol() != Some(identity.subject.symbol())
    {
        return Err(D::MissingIdentity);
    }
    let fields = &action.fields;
    if fields.due_bill_on_date.is_some()
        || fields.due_bill_off_date.is_some()
        || fields.due_bill_redemption_date.is_some()
    {
        return Err(D::DueBillEntitlementRequired);
    }
    let kind = match action.category {
        Category::ForwardSplit | Category::ReverseSplit => {
            if fields
                .new_symbol
                .as_deref()
                .is_some_and(|symbol| !symbol.is_empty() && symbol != identity.subject.symbol())
            {
                return Err(D::UnsupportedCanonicalEconomics);
            }
            let (numerator, denominator) =
                ratio(fields.new_rate.as_ref(), fields.old_rate.as_ref())?;
            CorporateActionKind::Split {
                numerator,
                denominator,
            }
        }
        Category::CashDividend => {
            let amount = money(fields.rate.as_ref(), fields.currency.as_deref())?;
            match fields.sub_type {
                None => CorporateActionKind::CashDividend { amount },
                Some(CashSubtype::ReturnOfCapital) => {
                    CorporateActionKind::ReturnOfCapital { amount }
                }
                Some(CashSubtype::Interest) => return Err(D::UnsupportedCanonicalEconomics),
            }
        }
        Category::SpinOff => {
            let related = related(identity, fields.new_symbol.as_deref())?;
            let (numerator, denominator) =
                ratio(fields.new_rate.as_ref(), fields.source_rate.as_ref())?;
            CorporateActionKind::Spinoff {
                distributed_instrument: related,
                numerator,
                denominator,
            }
        }
        Category::CashMerger | Category::StockMerger | Category::StockAndCashMerger => {
            let successor = related(identity, fields.acquirer_symbol.as_deref())?;
            let consideration = match action.category {
                Category::CashMerger => MergerConsideration::Cash {
                    amount: money(fields.rate.as_ref(), fields.currency.as_deref())?,
                },
                Category::StockMerger => {
                    let (numerator, denominator) =
                        ratio(fields.acquirer_rate.as_ref(), fields.acquiree_rate.as_ref())?;
                    MergerConsideration::Stock {
                        numerator,
                        denominator,
                    }
                }
                _ => {
                    let (numerator, denominator) =
                        ratio(fields.acquirer_rate.as_ref(), fields.acquiree_rate.as_ref())?;
                    MergerConsideration::Mixed {
                        numerator,
                        denominator,
                        cash: money(fields.cash_rate.as_ref(), fields.currency.as_deref())?,
                    }
                }
            };
            CorporateActionKind::Merger {
                successor,
                consideration,
            }
        }
        _ => return Err(D::UnsupportedCanonicalEconomics),
    };
    let provenance = ResearchProvenance::try_new(ResearchProvenanceInput {
        source_id: metadata.source_id().clone(),
        instrument_id: Some(identity.subject.instrument()),
        venue_id: None,
        source_identifier: SourceIdentifier::try_from(format!(
            "alpaca:corporate-action:{}",
            fields.id
        ))
        .map_err(|_| D::MissingOrInvalidTerms)?,
        source_timestamp: None,
        received_at: page.received_at,
        ingested_at,
        quality: metadata.quality_ceiling(),
        payload_reference: PayloadReference::ContentHash(PayloadHash::new(
            page.body_digest.algorithm(),
            page.body_digest.bytes(),
        )),
        availability: ResearchAvailability::local_first_observed(page.received_at),
    })
    .map_err(|_| D::MissingOrInvalidTerms)?;
    let time = ResearchTime::try_new_with_coordinates(
        ResearchTemporalCoordinate::calendar_date(date),
        None,
        RevisionNumber::new(1).map_err(|_| D::MissingOrInvalidTerms)?,
        None,
    )
    .map_err(|_| D::MissingOrInvalidTerms)?;
    CorporateActionObservation::new(
        ResearchContext::new(provenance, time).map_err(|_| D::MissingOrInvalidTerms)?,
        kind,
    )
    .map_err(|_| D::MissingOrInvalidTerms)
}

fn related(
    identity: &AlpacaCorporateActionIdentity,
    symbol: Option<&str>,
) -> Result<InstrumentId, AlpacaCorporateActionDisposition> {
    let related = identity
        .related
        .as_ref()
        .ok_or(AlpacaCorporateActionDisposition::MissingIdentity)?;
    if symbol != Some(related.symbol()) {
        return Err(AlpacaCorporateActionDisposition::MissingIdentity);
    }
    Ok(related.instrument())
}

fn positive(value: Option<&Number>) -> Result<Decimal, AlpacaCorporateActionDisposition> {
    let text = value
        .ok_or(AlpacaCorporateActionDisposition::MissingOrInvalidTerms)?
        .as_str();
    Decimal::from_str_exact(text)
        .or_else(|error| {
            let Some((base, _)) = text.split_once(['e', 'E']) else {
                return Err(error);
            };
            // The scientific parser otherwise permits rounding its significand. Reject that
            // loss before exponent handling so source cash and ratios remain exact.
            Decimal::from_str_exact(base)?;
            Decimal::from_scientific(text)
        })
        .ok()
        .filter(|value| *value > Decimal::ZERO)
        .ok_or(AlpacaCorporateActionDisposition::MissingOrInvalidTerms)
}

fn money(
    value: Option<&Number>,
    currency: Option<&str>,
) -> Result<Money, AlpacaCorporateActionDisposition> {
    let currency = currency.ok_or(AlpacaCorporateActionDisposition::MissingCurrency)?;
    let currency = Currency::try_from(currency)
        .map_err(|_| AlpacaCorporateActionDisposition::MissingCurrency)?;
    Ok(Money::new(positive(value)?, currency))
}

fn ratio(
    numerator: Option<&Number>,
    denominator: Option<&Number>,
) -> Result<(NonZeroU32, NonZeroU32), AlpacaCorporateActionDisposition> {
    use AlpacaCorporateActionDisposition::MissingOrInvalidTerms as Invalid;
    let numerator = positive(numerator)?;
    let denominator = positive(denominator)?;
    let scale = numerator.scale().max(denominator.scale());
    let numerator = u128::try_from(numerator.mantissa())
        .ok()
        .and_then(|value| value.checked_mul(10u128.checked_pow(scale - numerator.scale())?))
        .ok_or(Invalid)?;
    let denominator = u128::try_from(denominator.mantissa())
        .ok()
        .and_then(|value| value.checked_mul(10u128.checked_pow(scale - denominator.scale())?))
        .ok_or(Invalid)?;
    let (mut a, mut b) = (numerator, denominator);
    while b != 0 {
        (a, b) = (b, a % b);
    }
    let numerator = u32::try_from(numerator / a)
        .ok()
        .and_then(NonZeroU32::new)
        .ok_or(Invalid)?;
    let denominator = u32::try_from(denominator / a)
        .ok()
        .and_then(NonZeroU32::new)
        .ok_or(Invalid)?;
    Ok((numerator, denominator))
}

#[derive(Serialize)]
struct Sidecar<'a> {
    version: u16,
    scope: &'static str,
    announcement_time_supplied: bool,
    lifecycle_coverage_guaranteed: bool,
    coverage: &'a AlpacaCorporateActionsCoverage,
    pages: Vec<NativePage<'a>>,
}

#[derive(Serialize)]
struct NativePage<'a> {
    ordinal: u16,
    request_url: &'a str,
    request_page_token: Option<&'a str>,
    response_next_page_token: Option<&'a str>,
    body_digest: EvidenceDigest,
    body_bytes: usize,
    received_at: Timestamp,
    rate: super::RateEvidence,
    actions: &'a [NativeAction],
}

#[derive(Serialize)]
struct NativeRow<'a> {
    row_kind: &'static str,
    page_ordinal: u16,
    action: &'a NativeAction,
    subject: Option<InstrumentId>,
    subject_identity: Option<&'a CorporateActionEventInstrumentIdentity>,
    related: Option<InstrumentId>,
    related_identity: Option<&'a CorporateActionEventInstrumentIdentity>,
}

#[derive(Serialize)]
struct NativeSummaryRow {
    row_kind: &'static str,
    capture_observation_digest: EvidenceDigest,
}

fn source_context(
    metadata: &SourceMetadata,
    page: &Page,
    ingested_at: Timestamp,
    source_identifier: SourceIdentifier,
    instrument: Option<InstrumentId>,
    date: CalendarDate,
) -> Result<ResearchContext, AlpacaError> {
    let provenance = ResearchProvenance::try_new(ResearchProvenanceInput {
        source_id: metadata.source_id().clone(),
        instrument_id: instrument,
        venue_id: None,
        source_identifier,
        source_timestamp: None,
        received_at: page.received_at,
        ingested_at,
        quality: metadata.quality_ceiling(),
        payload_reference: PayloadReference::ContentHash(PayloadHash::new(
            page.body_digest.algorithm(),
            page.body_digest.bytes(),
        )),
        availability: ResearchAvailability::local_first_observed(page.received_at),
    })
    .map_err(|_| AlpacaError::Protocol)?;
    let time = ResearchTime::try_new_with_coordinates(
        ResearchTemporalCoordinate::calendar_date(date),
        None,
        RevisionNumber::new(1).map_err(|_| AlpacaError::Protocol)?,
        None,
    )
    .map_err(|_| AlpacaError::Protocol)?;
    ResearchContext::new(provenance, time).map_err(|_| AlpacaError::Protocol)
}

fn source_action_observation(
    action: &NativeAction,
    identity: Option<&AlpacaCorporateActionIdentity>,
    disposition: AlpacaCorporateActionDisposition,
    metadata: &SourceMetadata,
    page: &Page,
    ingested_at: Timestamp,
) -> Result<CorporateActionSourceObservation, AlpacaError> {
    let action_id =
        SourceIdentifier::try_from(format!("alpaca:corporate-action:{}", action.fields.id))?;
    let dates = action.dates();
    let disposition = match disposition {
        AlpacaCorporateActionDisposition::Normalized => {
            CorporateActionSourceDisposition::Normalized
        }
        AlpacaCorporateActionDisposition::MissingIdentity => {
            CorporateActionSourceDisposition::MissingIdentity
        }
        AlpacaCorporateActionDisposition::MissingEffectiveDate => {
            CorporateActionSourceDisposition::MissingEffectiveDate
        }
        AlpacaCorporateActionDisposition::MissingCurrency => {
            CorporateActionSourceDisposition::MissingCurrency
        }
        AlpacaCorporateActionDisposition::MissingOrInvalidTerms => {
            CorporateActionSourceDisposition::MissingOrInvalidTerms
        }
        AlpacaCorporateActionDisposition::UnsupportedCanonicalEconomics => {
            CorporateActionSourceDisposition::UnsupportedCanonicalEconomics
        }
        AlpacaCorporateActionDisposition::DueBillEntitlementRequired => {
            CorporateActionSourceDisposition::DueBillEntitlementRequired
        }
    };
    let context = source_context(
        metadata,
        page,
        ingested_at,
        action_id.clone(),
        identity.map(|v| v.subject.instrument()),
        dates.process,
    )?;
    CorporateActionSourceObservation::try_new(CorporateActionSourceObservationInput {
        context,
        payload: CorporateActionSourcePayload::ReturnedAction {
            action_id,
            category: CorporateActionSourceCategory::ALL[action.category.index()],
            subject_symbol: action
                .subject_symbol()
                .map(SourceIdentifier::try_from)
                .transpose()?,
            dates: CorporateActionSourceDates {
                process_date: dates.process,
                ex_date: dates.ex,
                effective_date: dates.effective,
                record_date: dates.record,
                payable_date: dates.payable,
                due_bill_on_date: dates.due_bill_on,
                due_bill_off_date: dates.due_bill_off,
                due_bill_redemption_date: dates.due_bill_redemption,
                expiration_date: dates.expiration,
            },
            currency: action
                .fields
                .currency
                .as_deref()
                // Unusable currency remains explicit in the native source terms. Its absence
                // from canonical monetary evidence must not discard the returned disposition.
                .and_then(|currency| Currency::try_from(currency).ok()),
            disposition,
        },
    })
    .map_err(|_| AlpacaError::Protocol)
}

fn source_summary_observation(
    coverage: &AlpacaCorporateActionsCoverage,
    dataset: &SourceIdentifier,
    metadata: &SourceMetadata,
    page: &Page,
    ingested_at: Timestamp,
) -> Result<CorporateActionSourceObservation, AlpacaError> {
    let id = SourceIdentifier::try_from(format!(
        "alpaca:corporate-actions:coverage:{}",
        lower_hex(coverage.capture_observation_digest.bytes())
    ))?;
    let context = source_context(
        metadata,
        page,
        ingested_at,
        id,
        None,
        coverage.request.process_start(),
    )?;
    CorporateActionSourceObservation::try_new(CorporateActionSourceObservationInput {
        context,
        payload: CorporateActionSourcePayload::QuerySummary {
            scope: CorporateActionSourceScope {
                dataset: dataset.clone(),
                process_start: coverage.request.process_start(),
                process_end: coverage.request.process_end(),
                symbols: coverage
                    .request
                    .symbols()
                    .iter()
                    .map(|v| SourceIdentifier::try_from(v.as_str()))
                    .collect::<Result<Vec<_>, _>>()?,
                query_instruments: coverage.query_instruments.clone(),
                query_contract:
                    CorporateActionSourceQueryContract::AlpacaAllTypesAllQualityUsProcessDatesV1,
                capture_observation_digest: coverage.capture_observation_digest,
                sealed_capture_receipt_digest: coverage.sealed_capture_receipt_digest,
            },
            category_counts: coverage.category_counts,
            normalized_count: u32::try_from(
                coverage
                    .actions
                    .iter()
                    .filter(|a| a.disposition == AlpacaCorporateActionDisposition::Normalized)
                    .count(),
            )
            .map_err(|_| AlpacaError::Protocol)?,
            page_count: u16::try_from(coverage.page_count).map_err(|_| AlpacaError::Protocol)?,
        },
    })
    .map_err(|_| AlpacaError::Protocol)
}

fn push_source_record(
    accumulator: &mut ExtractionBatchAccumulator,
    request: &ExtractionRequest,
    observation: CorporateActionSourceObservation,
) -> Result<(), AlpacaError> {
    let effective = observation.context().time().effective().clone();
    let received = observation.context().provenance().received_at();
    let payload = Bytes::from(
        serde_json::to_vec(&ResearchObservation::CorporateActionSource(observation))
            .map_err(|_| AlpacaError::Serialization)?,
    );
    let evidence = ExactPayloadEvidence::from_content_digest(sha256(&payload));
    let revision = SourceIdentifier::try_from(format!(
        "observed:{}",
        lower_hex(evidence.content_digest().bytes())
    ))?;
    accumulator
        .push(
            ExtractionRecord::try_new_with_time(
                request,
                SourceIdentifier::try_from(CURRENT_RESEARCH_RECORD_SCHEMA)?,
                evidence,
                effective,
                None,
                AvailabilityEvidence::LocalFirstObserved {
                    observed_at: received,
                },
                revision,
                None,
                payload,
            )
            .map_err(|_| AlpacaError::CaptureMaterial)?,
        )
        .map_err(|_| AlpacaError::CaptureMaterial)
}
