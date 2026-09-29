//! Fresh original-coordinate Raw and Split reads under Alpaca's actual bar adjustment contract.
//!
//! Each side uses the existing complete-history publisher and exact origin read. They remain two
//! independently acquired snapshots; neither a synthetic dual capture nor a provider as-of clock
//! is claimed. Alpaca `asof` controls symbol mapping, not adjustment knowledge time.

use super::*;
use crate::application::research::market_history::NativeSessionHistory;
use market_squawk_data::{
    CompleteMarketBarHistoryCursor, CompleteMarketBarHistoryRequest, Sha256Digest,
};
use market_squawk_domain::{
    BarTimestampBasis, MarketBarAdjustment, MarketBarObservation, MarketBarSessionKind,
    ProviderInstrumentId, VenueId,
};

pub(super) struct AlpacaOriginAdjustmentAnchor {
    pub(super) raw: CompleteMarketBarHistoryCursor,
    pub(super) split: CompleteMarketBarHistoryCursor,
    pub(super) raw_page_received_at: Box<[Timestamp]>,
    pub(super) split_page_received_at: Box<[Timestamp]>,
}
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AlpacaOriginAdjustmentAnchorReference {
    raw: AlpacaHistoryReference,
    split: AlpacaHistoryReference,
    raw_page_received_at: Box<[Timestamp]>,
    split_page_received_at: Box<[Timestamp]>,
}
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AlpacaHistoryReference {
    version: u16,
    instrument: InstrumentId,
    requested: (Timestamp, Timestamp),
    provider_instrument: ProviderInstrumentId,
    venue: VenueId,
    feed: SourceIdentifier,
    interval: SourceIdentifier,
    adjustment: MarketBarAdjustment,
    timestamp_basis: BarTimestampBasis,
    session_kind: MarketBarSessionKind,
    session_ruleset: SourceIdentifier,
    cutoff: Timestamp,
    origin: EvidenceDigest,
    binding: EvidenceDigest,
    publication: EvidenceDigest,
    read: EvidenceDigest,
}
impl AlpacaHistoryReference {
    pub(super) const fn instrument_id(&self) -> InstrumentId {
        self.instrument
    }
}
impl AlpacaOriginAdjustmentAnchorReference {
    pub(super) fn bounded_page_clocks(&self) -> bool {
        [&self.raw_page_received_at, &self.split_page_received_at]
            .iter()
            .all(|pages| {
                !pages.is_empty()
                    && pages.len() <= market_squawk_sources::MAX_PROVIDER_CAPTURE_PAGES
            })
    }
}
impl AlpacaOriginAdjustmentAnchor {
    fn try_from_reads(
        raw: CompleteMarketBarHistoryCursor,
        split: CompleteMarketBarHistoryCursor,
        cutoff: Timestamp,
        raw_page_received_at: Box<[Timestamp]>,
        split_page_received_at: Box<[Timestamp]>,
    ) -> Result<Self, ApplicableActionPlanError> {
        let a = raw.selection().receipt();
        let b = split.selection().receipt();
        let raw_native = raw
            .native_sessions()
            .ok_or(ApplicableActionPlanError::InvalidEvidence)?;
        let split_native = split
            .native_sessions()
            .ok_or(ApplicableActionPlanError::InvalidEvidence)?;
        if !raw_native.sessions().same_rows(split_native.sessions()).map_err(|_| ApplicableActionPlanError::InvalidEvidence)?
            || raw.bar_count() == 0
            || raw.bar_count() > 64
            || raw.bar_count() != split.bar_count()
            || raw.read_receipt().knowledge_cutoff() != cutoff
            || split.read_receipt().knowledge_cutoff() != cutoff
            || raw.selection().pinned().manifest() != a.origin_manifest()
            || split.selection().pinned().manifest() != b.origin_manifest()
            || a.graph_purpose().as_str() != "alpaca-iex-historical-bars-and-calendar/v1"
            || b.graph_purpose() != a.graph_purpose()
            // Exact timestamp receipts revalidate their original instrument revision and
            // provider mapping across the full requested interval at publication and reopen.
            // Nominal date-window history has a different, weaker identity qualification.
            || a.date_windows().is_some()
            || b.date_windows().is_some()
            || a.requested_range().is_none()
            || a.timestamp_basis().is_none()
            || a.session_kind().is_none()
            || a.adjustment() != MarketBarAdjustment::Raw
            || b.adjustment() != MarketBarAdjustment::Split
            || a.source_id() != b.source_id()
            || a.instrument_id() != b.instrument_id()
            || a.instrument_revision_digest() != b.instrument_revision_digest()
            || a.provider_instrument_id() != b.provider_instrument_id()
            || a.venue_id() != b.venue_id()
            || a.feed() != b.feed()
            || a.interval() != b.interval()
            || a.requested_range() != b.requested_range()
            || a.expected_provider_timestamps() != b.expected_provider_timestamps()
            || a.currency() != b.currency()
            || a.timestamp_basis() != b.timestamp_basis()
            || a.session_kind() != b.session_kind()
            || a.session_ruleset() != b.session_ruleset()
            || a.published_at() > cutoff
            || b.published_at() > cutoff
        {
            return Err(ApplicableActionPlanError::InvalidEvidence);
        }
        for (raw, split) in raw.bars().zip(split.bars()) {
            let raw = raw.map_err(|_| ApplicableActionPlanError::InvalidEvidence)?;
            let split = split.map_err(|_| ApplicableActionPlanError::InvalidEvidence)?;
            if !same_coordinate(&raw, &split) {
                return Err(ApplicableActionPlanError::InvalidEvidence);
            }
        }
        Ok(Self {
            raw,
            split,
            raw_page_received_at,
            split_page_received_at,
        })
    }
    pub(super) fn reference(
        &self,
    ) -> Result<AlpacaOriginAdjustmentAnchorReference, ApplicableActionPlanError> {
        Ok(AlpacaOriginAdjustmentAnchorReference {
            raw: timestamped_history_reference(&self.raw)?,
            split: timestamped_history_reference(&self.split)?,
            raw_page_received_at: self.raw_page_received_at.clone(),
            split_page_received_at: self.split_page_received_at.clone(),
        })
    }
}
impl SourceAppliedCorporateActionReadCapability {
    /// Joins two actual source publications. Acquisition must use the existing plan directory,
    /// Raw/Split request selection, raw sealing and canonical publisher. This method performs no
    /// provider request and cannot turn caller bars or hashes into an anchor.
    pub(crate) async fn with_fresh_alpaca_split_anchor<H: NativeSessionHistory>(
        &self,
        plan: SourceAppliedCorporateActionPlan,
        raw: H,
        split: H,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<SourceAppliedCorporateActionPlan, ApplicableActionPlanError> {
        self.with_fresh_alpaca_split_anchor_with_job_context(
            plan,
            raw,
            split,
            deadline,
            cancellation,
            None,
        )
        .await
    }

    pub(crate) async fn with_fresh_alpaca_split_anchor_with_job_context<H: NativeSessionHistory>(
        &self,
        mut plan: SourceAppliedCorporateActionPlan,
        raw: H,
        split: H,
        deadline: Instant,
        cancellation: &CancellationToken,
        job: Option<&market_squawk_jobs::JobRunContext>,
    ) -> Result<SourceAppliedCorporateActionPlan, ApplicableActionPlanError> {
        check(deadline, cancellation)?;
        if plan.anchor.is_some() {
            return Err(ApplicableActionPlanError::InvalidEvidence);
        }
        let raw_pages = self
            .anchor_page_clocks(&raw, deadline, cancellation, job)
            .await?;
        let split_pages = self
            .anchor_page_clocks(&split, deadline, cancellation, job)
            .await?;
        let raw = raw
            .into_native_cursor(self.research.analytical(), deadline, cancellation.clone())
            .map_err(|_| ApplicableActionPlanError::InvalidEvidence)?;
        let split = split
            .into_native_cursor(self.research.analytical(), deadline, cancellation.clone())
            .map_err(|_| ApplicableActionPlanError::InvalidEvidence)?;
        let anchor = AlpacaOriginAdjustmentAnchor::try_from_reads(
            raw,
            split,
            plan.source.knowledge_cutoff(),
            raw_pages,
            split_pages,
        )?;
        if !plan
            .requested_instruments
            .contains(&anchor.raw.selection().receipt().instrument_id())
        {
            return Err(ApplicableActionPlanError::InvalidEvidence);
        }
        plan.anchor = Some(anchor);
        check(deadline, cancellation)?;
        Ok(plan)
    }

    /// Uses the existing controlled generation worker to reopen every physical source page.
    /// Empty response pages remain part of the source snapshot and its freshness constraint.
    async fn anchor_page_clocks<H: NativeSessionHistory>(
        &self,
        history: &H,
        deadline: Instant,
        cancellation: &CancellationToken,
        job: Option<&market_squawk_jobs::JobRunContext>,
    ) -> Result<Box<[Timestamp]>, ApplicableActionPlanError> {
        let receipt = history.selection().receipt().clone();
        self.research
            .read_provider_capture_generation_with_job_context(
                job,
                receipt.origin_manifest().clone(),
                deadline,
                cancellation,
                move |generation, _, _, _, _| {
                    let invalid = || crate::ResearchServiceError::IngestAuthorityMismatch;
                    if generation.pinned().manifest() != receipt.origin_manifest()
                        || generation.source_id() != receipt.source_id()
                        || generation.published_at() != receipt.published_at()
                    {
                        return Err(invalid());
                    }
                    let object = generation
                        .objects()
                        .iter()
                        .find(|object| {
                            object.generation_object_ordinal()
                                == usize::from(receipt.origin_object_ordinal())
                        })
                        .ok_or_else(invalid)?;
                    if object.object().artifact_id() != receipt.origin_artifact_id()
                        || object.inputs().len() != 1
                    {
                        return Err(invalid());
                    }
                    let binding = object.inputs()[0].binding();
                    if binding.binding_digest().bytes() != receipt.binding_digest().bytes()
                        || binding.sealed_capture_receipt_digest().bytes()
                            != receipt.capture_receipt_digest().bytes()
                        || binding.capture().content_digest().bytes()
                            != receipt.capture_graph_digests().0.bytes()
                        || binding.capture().observation_digest().bytes()
                            != receipt.capture_graph_digests().1.bytes()
                    {
                        return Err(invalid());
                    }
                    // Capture construction already bounds this original ordered list to 64 pages.
                    Ok(binding
                        .capture()
                        .pages()
                        .iter()
                        .map(|page| page.received_at())
                        .collect())
                },
            )
            .await
            .map_err(|_| ApplicableActionPlanError::InvalidEvidence)
    }
}
impl SourceAppliedCorporateActionReadCapability {
    pub(super) async fn reopen_anchor(
        &self,
        reference: &AlpacaOriginAdjustmentAnchorReference,
        cutoff: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
        job: Option<&market_squawk_jobs::JobRunContext>,
    ) -> Result<
        (
            CompleteMarketBarHistoryCursor,
            CompleteMarketBarHistoryCursor,
        ),
        ApplicableActionPlanError,
    > {
        let raw = self
            .reopen_timestamped_history(&reference.raw, cutoff, deadline, cancellation, job)
            .await?;
        let split = self
            .reopen_timestamped_history(&reference.split, cutoff, deadline, cancellation, job)
            .await?;
        Ok((raw, split))
    }
    pub(super) async fn reopen_timestamped_history(
        &self,
        reference: &AlpacaHistoryReference,
        cutoff: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
        job: Option<&market_squawk_jobs::JobRunContext>,
    ) -> Result<CompleteMarketBarHistoryCursor, ApplicableActionPlanError> {
        check(deadline, cancellation)?;
        if reference.version != 1
            || reference.cutoff != cutoff
            || !matches!(
                reference.adjustment,
                MarketBarAdjustment::Raw | MarketBarAdjustment::Split
            )
            || [
                reference.origin,
                reference.binding,
                reference.publication,
                reference.read,
            ]
            .iter()
            .any(|digest| digest.bytes() == [0; 32])
        {
            return Err(ApplicableActionPlanError::InvalidEvidence);
        }
        let manifest = self
            .research
            .analytical_reader()
            .provider_capture_origin(
                reference.binding,
                Sha256Digest::new(reference.origin.bytes()),
                cutoff,
                deadline,
                cancellation,
            )
            .map_err(|_| ApplicableActionPlanError::InvalidEvidence)?
            .ok_or(ApplicableActionPlanError::InvalidEvidence)?;
        let request = CompleteMarketBarHistoryRequest::try_exact(
            reference.instrument,
            reference.requested.0,
            reference.requested.1,
            reference.provider_instrument.clone(),
            reference.venue.clone(),
            reference.feed.clone(),
            reference.interval.clone(),
            reference.adjustment,
            reference.timestamp_basis,
            reference.session_kind,
            reference.session_ruleset.clone(),
            cutoff,
            manifest,
        )
        .map_err(|_| ApplicableActionPlanError::InvalidEvidence)?;
        let read = self
            .research
            .analytical_reader()
            .read_complete_market_bar_history_cursor(request, deadline, cancellation.clone())
            .await
            .map_err(|_| ApplicableActionPlanError::InvalidEvidence)?
            .ok_or(ApplicableActionPlanError::InvalidEvidence)?;
        let read = self
            .research
            .rejoin_market_history_native_sessions_with_job_context(
                read,
                deadline,
                cancellation,
                job,
            )
            .await
            .map_err(|_| ApplicableActionPlanError::InvalidEvidence)?;
        if timestamped_history_reference(&read)? != *reference {
            return Err(ApplicableActionPlanError::InvalidEvidence);
        }
        Ok(read)
    }
}
pub(super) fn timestamped_history_reference<H: NativeSessionHistory>(
    read: &H,
) -> Result<AlpacaHistoryReference, ApplicableActionPlanError> {
    let receipt = read.selection().receipt();
    let digest = |bytes| EvidenceDigest::new(market_squawk_domain::DigestAlgorithm::Sha256, bytes);
    Ok(AlpacaHistoryReference {
        version: 1,
        instrument: receipt.instrument_id(),
        requested: receipt
            .requested_range()
            .ok_or(ApplicableActionPlanError::InvalidEvidence)?,
        provider_instrument: receipt.provider_instrument_id().clone(),
        venue: receipt.venue_id().clone(),
        feed: receipt.feed().clone(),
        interval: receipt.interval().clone(),
        adjustment: receipt.adjustment(),
        timestamp_basis: receipt
            .timestamp_basis()
            .ok_or(ApplicableActionPlanError::InvalidEvidence)?,
        session_kind: receipt
            .session_kind()
            .ok_or(ApplicableActionPlanError::InvalidEvidence)?,
        session_ruleset: receipt.session_ruleset().clone(),
        cutoff: read.read_receipt().knowledge_cutoff(),
        origin: digest(receipt.origin_manifest().content_hash().bytes()),
        binding: digest(receipt.binding_digest().bytes()),
        publication: digest(receipt.receipt_digest().bytes()),
        read: digest(read.read_receipt().result_digest().bytes()),
    })
}
/// Evidence digests inside independently fetched calendar receipts may differ. Exact native
/// period boundaries and the same source ruleset/venue remain mandatory; no date is reconstructed.
pub(super) fn same_coordinate(a: &MarketBarObservation, b: &MarketBarObservation) -> bool {
    let (Some(a_time), Some(b_time)) = (
        a.time_semantics().timestamped_period(),
        b.time_semantics().timestamped_period(),
    ) else {
        return false;
    };
    a.context().provenance().instrument_id() == b.context().provenance().instrument_id()
        && a.context().provenance().source_id() == b.context().provenance().source_id()
        && a.context().provenance().venue_id() == b.context().provenance().venue_id()
        && a.provider_instrument_id() == b.provider_instrument_id()
        && a.feed() == b.feed()
        && a.interval() == b.interval()
        && a.currency() == b.currency()
        && a_time.period_start() == b_time.period_start()
        && a_time.period_end_exclusive() == b_time.period_end_exclusive()
        && a_time.timestamp_basis() == b_time.timestamp_basis()
        && a_time.session().kind() == b_time.session().kind()
        && a_time.session().ruleset() == b_time.session().ruleset()
}
pub(super) fn same_reported_values(a: &MarketBarObservation, b: &MarketBarObservation) -> bool {
    a.open() == b.open()
        && a.high() == b.high()
        && a.low() == b.low()
        && a.close() == b.close()
        && a.volume() == b.volume()
        && a.trade_count() == b.trade_count()
        && a.vwap() == b.vwap()
}
/// Exact source-reported reciprocal price/volume relation; no division, rounding or factor is
/// applied to the forecast. Missing/zero volume cannot establish this independent share anchor.
pub(super) fn reciprocal_price_volume(
    raw: &MarketBarObservation,
    split: &MarketBarObservation,
) -> bool {
    if raw.volume() <= rust_decimal::Decimal::ZERO
        || split.volume() <= rust_decimal::Decimal::ZERO
        || raw.trade_count() != split.trade_count()
    {
        return false;
    }
    for (raw_price, split_price) in [
        (raw.open(), split.open()),
        (raw.high(), split.high()),
        (raw.low(), split.low()),
        (raw.close(), split.close()),
    ] {
        let left = raw_price.amount().checked_mul(raw.volume());
        let right = split_price.amount().checked_mul(split.volume());
        if left.is_none() || left != right {
            return false;
        }
    }
    match (raw.vwap(), split.vwap()) {
        (None, None) => true,
        (Some(a), Some(b)) => a
            .amount()
            .checked_mul(raw.volume())
            .is_some_and(|left| b.amount().checked_mul(split.volume()) == Some(left)),
        _ => false,
    }
}

impl SourceAppliedCorporateActionReadCapability {
    /// Derives both inert request plans from the authenticated original forecast and an actual
    /// catalog read. Existing runtime admission and publication revalidate this exact definition;
    /// this method grants neither source authority nor permission to substitute a new alias.
    pub(crate) fn prepare_original_coordinate_anchor_plans(
        &self,
        forecast: &crate::application::model::forecast::LatestValidForecast,
        instrument: &market_squawk_data::MarketDataInstrumentRecord,
        analysis_at: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<
        [market_squawk_adapter_alpaca::AlpacaHistoricalEquityPreflightPlan; 2],
        ApplicableActionPlanError,
    > {
        use market_squawk_adapter_alpaca::{
            AlpacaAdjustment, AlpacaHistoricalEquityPreflightPlan, AlpacaInstrumentMapping,
        };
        check(deadline, cancellation)?;
        let serving = forecast
            .selected_distribution()
            .ok_or(ApplicableActionPlanError::InvalidEvidence)?
            .serving_binding();
        let original = serving
            .origin_bar()
            .ok_or(ApplicableActionPlanError::InvalidEvidence)?;
        let definition = instrument.definition();
        let exact = original
            .time_semantics()
            .timestamped_period()
            .ok_or(ApplicableActionPlanError::InvalidEvidence)?;
        if instrument.published_at() > analysis_at
            || serving.knowledge_cutoff() > analysis_at
            || definition.instrument_id()
                != original
                    .context()
                    .provenance()
                    .instrument_id()
                    .ok_or(ApplicableActionPlanError::InvalidEvidence)?
            || definition.quote_currency() != original.currency()
            || definition
                .provider_identity_at(
                    serving.source_id(),
                    original.provider_instrument_id(),
                    exact.provider_timestamp(),
                )
                .is_none()
        {
            return Err(ApplicableActionPlanError::InvalidEvidence);
        }
        let mapping = AlpacaInstrumentMapping::try_new(
            original.provider_instrument_id().as_str().to_owned(),
            definition.instrument_id(),
            definition.asset_class(),
        )
        .map_err(|_| ApplicableActionPlanError::InvalidEvidence)?;
        let raw = AlpacaHistoricalEquityPreflightPlan::try_for_original_price_coordinate(
            mapping.clone(),
            original,
            AlpacaAdjustment::Raw,
            analysis_at,
        )
        .map_err(|_| ApplicableActionPlanError::InvalidEvidence)?;
        let split = AlpacaHistoricalEquityPreflightPlan::try_for_original_price_coordinate(
            mapping,
            original,
            AlpacaAdjustment::Split,
            analysis_at,
        )
        .map_err(|_| ApplicableActionPlanError::InvalidEvidence)?;
        check(deadline, cancellation)?;
        Ok([raw, split])
    }
}
