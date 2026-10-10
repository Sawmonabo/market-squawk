//! Original historical calendar reconstruction, with no live runtime or publication authority.

use super::*;
use market_squawk_adapter_alpaca::{AlpacaRetainedCalendarSessions, AlpacaTradingApiEnvironment};
use market_squawk_domain::{
    BarTimeSemantics, BarTimestampBasis, ExactPayloadEvidence, MarketBarSessionEvidence,
    MarketBarSessionKind, RevisionBoundPayloadEvidence, SourceIdentifier, VenueId,
};

/// A physically reopened original calendar. This type cannot issue currentness receipts,
/// completed-session authority, publication guards or execution-session evidence.
#[derive(Clone)]
pub(crate) struct RetainedMarketSessionRead {
    reference: CompletedMarketSessionReference,
    action_calendar: Arc<market_squawk_data::RetainedCorporateActionCalendar>,
    replay: Arc<AlpacaRetainedCalendarSessions>,
    calendar_id: SourceIdentifier,
    calendar_revision: RevisionBoundPayloadEvidence,
}

impl RetainedMarketSessionRead {
    pub(crate) const fn reference(&self) -> &CompletedMarketSessionReference {
        &self.reference
    }
    pub(crate) const fn source_action_calendar(
        &self,
    ) -> &Arc<market_squawk_data::RetainedCorporateActionCalendar> {
        &self.action_calendar
    }
    pub(crate) const fn native_session_replay(&self) -> &Arc<AlpacaRetainedCalendarSessions> {
        &self.replay
    }
    pub(crate) fn venue_id(&self) -> &VenueId {
        self.action_calendar.venue_id()
    }
    pub(crate) fn requested_dates(&self) -> (CalendarDate, CalendarDate) {
        self.replay.requested_dates()
    }
    pub(crate) fn available_at(&self) -> Timestamp {
        self.action_calendar.available_at()
    }
    pub(crate) const fn calendar_id(&self) -> &SourceIdentifier {
        &self.calendar_id
    }
    pub(crate) const fn calendar_revision(&self) -> &RevisionBoundPayloadEvidence {
        &self.calendar_revision
    }

    /// Exact source-reported historical date at the original cutoff, never live permission.
    pub(crate) fn date_session_on(
        &self,
        date: CalendarDate,
        knowledge_cutoff: Timestamp,
        evaluated_at: Timestamp,
    ) -> Option<RetainedMarketSessionDateReceipt> {
        let values = self
            .action_calendar
            .date_session_on(date, knowledge_cutoff, evaluated_at)?;
        let index = self
            .replay
            .sessions()
            .binary_search_by_key(&date, |day| day.date())
            .ok()?;
        let day = &self.replay.sessions()[index];
        let provider_period = match (day.period_start(), day.period_end_exclusive()) {
            (Some(start), Some(end)) if self.replay.market() == AlpacaCalendarMarket::Iex => {
                let session = MarketBarSessionEvidence::try_new(
                    MarketBarSessionKind::ProviderDefined,
                    SourceIdentifier::try_from("alpaca-v3-iex-utc-completed-daily-v1").ok()?,
                    values.receipt_digest,
                )
                .ok()?;
                Some(
                    BarTimeSemantics::try_new(start, end, BarTimestampBasis::PeriodStart, session)
                        .ok()?,
                )
            }
            (None, None) if self.replay.market() != AlpacaCalendarMarket::Iex => None,
            _ => return None,
        };
        Some(RetainedMarketSessionDateReceipt {
            reference: self.reference.clone(),
            date,
            opens_at: values.opens_at,
            closes_at_exclusive: values.closes_at_exclusive,
            available_at: values.available_at,
            evidence_digest: values.receipt_digest,
            provider_period,
        })
    }
}

/// Historical native coordinates only; deliberately distinct from the live date receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RetainedMarketSessionDateReceipt {
    reference: CompletedMarketSessionReference,
    date: CalendarDate,
    opens_at: Timestamp,
    closes_at_exclusive: Timestamp,
    available_at: Timestamp,
    evidence_digest: EvidenceDigest,
    provider_period: Option<BarTimeSemantics>,
}
impl RetainedMarketSessionDateReceipt {
    pub(crate) const fn reference(&self) -> &CompletedMarketSessionReference {
        &self.reference
    }
    pub(crate) const fn date(&self) -> CalendarDate {
        self.date
    }
    pub(crate) const fn opens_at(&self) -> Timestamp {
        self.opens_at
    }
    pub(crate) const fn closes_at_exclusive(&self) -> Timestamp {
        self.closes_at_exclusive
    }
    pub(crate) const fn available_at(&self) -> Timestamp {
        self.available_at
    }
    pub(crate) const fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest
    }
    pub(crate) const fn provider_period(&self) -> Option<&BarTimeSemantics> {
        self.provider_period.as_ref()
    }
}

/// Uses only the existing research/capture owners. No active account or network is consulted.
#[derive(Clone)]
pub(crate) struct RetainedMarketSessionReadCapability {
    research: Arc<ResearchService>,
}
impl RetainedMarketSessionReadCapability {
    pub(crate) fn new(research: Arc<ResearchService>) -> Self {
        Self { research }
    }

    pub(crate) async fn read_reference_with_job_context(
        &self,
        original: &CompletedMarketSessionReference,
        as_of: Timestamp,
        deadline: Instant,
        cancellation: CancellationToken,
        job: Option<&market_squawk_jobs::JobRunContext>,
    ) -> Result<Option<RetainedMarketSessionRead>, CompletedMarketSessionError> {
        check(deadline, &cancellation)?;
        let manifest = self
            .research
            .analytical_reader()
            .provider_capture_origin(
                original.capture_binding_digest,
                Sha256Digest::new(original.origin_content_digest.bytes()),
                as_of,
                deadline,
                &cancellation,
            )
            .map_err(|_| controlled_error(deadline, &cancellation))?;
        let Some(manifest) = manifest else {
            return Ok(None);
        };
        let Some(origin) = read_calendar_origin(
            &self.research,
            manifest.clone(),
            Some(original.capture_binding_digest),
            None,
            as_of,
            deadline,
            &cancellation,
            job,
        )
        .await?
        else {
            return Ok(None);
        };
        let coverage = origin
            .calendar_rows
            .first()
            .ok_or(CompletedMarketSessionError::InvalidEvidence)?;
        let scope = coverage.scope();
        let market = match scope.native_product.as_str() {
            "IEX" => AlpacaCalendarMarket::Iex,
            "XNYS" => AlpacaCalendarMarket::Nyse,
            "XNAS" => AlpacaCalendarMarket::Nasdaq,
            _ => return Err(CompletedMarketSessionError::InvalidEvidence),
        };
        // Recover the sole original request by its retained commitment, never today's account.
        let mut request = None;
        for environment in [
            AlpacaTradingApiEnvironment::Live,
            AlpacaTradingApiEnvironment::Paper,
        ] {
            let candidate = AlpacaAuthenticatedCalendarRequest::try_for_market(
                environment,
                market,
                scope.date_scope.start_date(),
                scope.date_scope.end_date(),
            )
            .map_err(invalid)?;
            if candidate.capture_request_identity().map_err(invalid)?
                == origin.binding.capture().request_set_identity()
            {
                if request.replace(candidate).is_some() {
                    return Err(CompletedMarketSessionError::InvalidEvidence);
                }
            }
        }
        let request = request.ok_or(CompletedMarketSessionError::InvalidEvidence)?;
        let binding_digest = origin.binding.binding_digest();
        let calendar_revision = RevisionBoundPayloadEvidence::new(
            origin.retained_metadata.revision().clone(),
            ExactPayloadEvidence::from_content_digest(
                origin.binding.capture().pages()[0].body_digest(),
            ),
        );
        let replay = super::super::alpaca::read_alpaca_retained_calendar_with_job_context(
            &self.research,
            manifest.clone(),
            request,
            binding_digest,
            origin.calendar_rows.into_boxed_slice(),
            as_of,
            deadline,
            &cancellation,
            job,
        )
        .await?;
        let retained_replay = Arc::clone(&replay);
        let action_calendar = self
            .research
            .read_provider_capture_generation_with_job_context(
                job,
                manifest.clone(),
                deadline,
                &cancellation,
                move |generation, _, _, analytical, read_cancel| {
                    analytical
                        .rejoin_corporate_action_calendar(
                            &generation,
                            retained_replay,
                            as_of,
                            deadline,
                            read_cancel,
                        )
                        .map(Arc::new)
                        .map_err(ResearchServiceError::from)
                },
            )
            .await
            .map_err(|_| controlled_error(deadline, &cancellation))?;
        let reference = reference(&manifest, binding_digest);
        if &reference != original
            || action_calendar.available_at() != origin.published_at.max(replay.received_at())
        {
            return Err(CompletedMarketSessionError::InvalidEvidence);
        }
        let calendar_id = SourceIdentifier::try_from(if market == AlpacaCalendarMarket::Iex {
            "alpaca-v3-calendar-iex-utc"
        } else {
            "alpaca-v3-listed-market-native-calendar"
        })
        .map_err(invalid)?;
        check(deadline, &cancellation)?;
        Ok(Some(RetainedMarketSessionRead {
            reference,
            action_calendar,
            replay,
            calendar_id,
            calendar_revision,
        }))
    }
}
