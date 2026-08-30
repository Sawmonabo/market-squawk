use std::collections::BTreeSet;
use std::num::{NonZeroU16, NonZeroU32};
use std::time::Duration;

use market_squawk_domain::{CalendarDate, ExactPayloadEvidence, SourceIdentifier, Timestamp};
use market_squawk_sources::{
    ApiEndpointRule, ExtractionAuthority, ExtractionSourceError, InFlightExtractionRequest,
    MAX_PROVIDER_CAPTURE_BYTES, MAX_PROVIDER_CAPTURE_PAGES, NetworkPolicyError, PathScope,
    ProviderCaptureMaterial, QueryParameterRule, QuerySensitivity, SourceError,
};
use tokio_util::sync::CancellationToken;

use crate::{
    FredOperation, FredParseLimits, FredReleaseCursor, FredReleaseObservationPage,
    FredRightsDisposition, FredVintagePage, MAX_FRED_V2_RELEASE_PAGE_OBSERVATIONS,
};

use super::{
    FredDataset, FredHttpAuthorization, FredHttpRequest, FredSource, acquire_request_permit,
    evidence_for_payload, map_adapter_error, standalone_capture_material, system_timestamp,
};

const VINTAGE_DATES_ENDPOINT: &str = "https://api.stlouisfed.org/fred/series/vintagedates";
const RELEASE_OBSERVATIONS_V2_ENDPOINT: &str =
    "https://api.stlouisfed.org/fred/v2/release/observations";
const MAX_VINTAGE_PAGE_RECORDS: u16 = 10_000;
const MAX_RELEASE_SCOPE_SERIES: usize = 256;
const MAX_RELEASE_STRING_BYTES: usize = 64 * 1024;

/// Builds the exact endpoint-policy rule for v1 observation pages.
pub fn fred_observations_endpoint_rule() -> Result<ApiEndpointRule, NetworkPolicyError> {
    let mut rules = fred_page_query_rules(&[
        ("series_id", 120, QuerySensitivity::Public),
        ("realtime_start", 10, QuerySensitivity::Public),
        ("realtime_end", 10, QuerySensitivity::Public),
        ("limit", 6, QuerySensitivity::Public),
        ("offset", 20, QuerySensitivity::Public),
        ("sort_order", 4, QuerySensitivity::Public),
        ("output_type", 1, QuerySensitivity::Public),
        ("file_type", 4, QuerySensitivity::Public),
        ("api_key", 32, QuerySensitivity::Secret),
    ])?;
    rules.push(QueryParameterRule::try_new_exact_public(
        SourceIdentifier::try_from("units")
            .map_err(|_| NetworkPolicyError::InvalidRequestBounds)?,
        SourceIdentifier::try_from("lin").map_err(|_| NetworkPolicyError::InvalidRequestBounds)?,
    )?);
    ApiEndpointRule::try_new(
        "https://api.stlouisfed.org/fred/series/observations",
        PathScope::Exact,
        rules,
        10,
        2_048,
    )
}

/// Builds the exact endpoint-policy rule for v1 ALFRED vintage-date pages.
pub fn fred_vintage_dates_endpoint_rule() -> Result<ApiEndpointRule, NetworkPolicyError> {
    fred_page_endpoint_rule(
        VINTAGE_DATES_ENDPOINT,
        &[
            ("series_id", 120, QuerySensitivity::Public),
            ("realtime_start", 10, QuerySensitivity::Public),
            ("realtime_end", 10, QuerySensitivity::Public),
            ("limit", 5, QuerySensitivity::Public),
            ("offset", 20, QuerySensitivity::Public),
            ("sort_order", 4, QuerySensitivity::Public),
            ("file_type", 4, QuerySensitivity::Public),
            ("api_key", 32, QuerySensitivity::Secret),
        ],
    )
}

/// Builds the exact endpoint-policy rule for bearer-authenticated v2 release pages.
pub fn fred_release_observations_v2_endpoint_rule() -> Result<ApiEndpointRule, NetworkPolicyError> {
    fred_page_endpoint_rule(
        RELEASE_OBSERVATIONS_V2_ENDPOINT,
        &[
            ("release_id", 10, QuerySensitivity::Public),
            ("format", 4, QuerySensitivity::Public),
            ("limit", 6, QuerySensitivity::Public),
            ("next_cursor", 256, QuerySensitivity::Public),
        ],
    )
}

fn fred_page_endpoint_rule(
    endpoint: &str,
    declarations: &[(&str, u16, QuerySensitivity)],
) -> Result<ApiEndpointRule, NetworkPolicyError> {
    let rules = fred_page_query_rules(declarations)?;
    ApiEndpointRule::try_new(
        endpoint,
        PathScope::Exact,
        rules,
        u8::try_from(declarations.len()).map_err(|_| NetworkPolicyError::InvalidRequestBounds)?,
        2_048,
    )
}

fn fred_page_query_rules(
    declarations: &[(&str, u16, QuerySensitivity)],
) -> Result<Vec<QueryParameterRule>, NetworkPolicyError> {
    declarations
        .iter()
        .map(|(key, max_value_bytes, sensitivity)| {
            QueryParameterRule::try_new(
                SourceIdentifier::try_from(*key)
                    .map_err(|_| NetworkPolicyError::InvalidRequestBounds)?,
                *max_value_bytes,
                false,
                *sensitivity,
            )
        })
        .collect::<Result<Vec<_>, _>>()
}

/// Exact v1 vintage-date response and raw material required for immutable sealing.
#[derive(Debug)]
pub struct FredVintageExtractionPage {
    page: FredVintagePage,
    page_evidence: ExactPayloadEvidence,
    received_at: Timestamp,
    capture: ProviderCaptureMaterial,
}

/// One complete, strictly ordered v1 vintage-date chain and all exact raw response material.
#[derive(Debug)]
pub struct FredVintageExtraction {
    pages: Box<[FredVintagePage]>,
    vintage_dates: Box<[CalendarDate]>,
    raw_body_bytes: u64,
    captures: Box<[ProviderCaptureMaterial]>,
}

impl FredVintageExtraction {
    /// Returns every validated provider page in ascending offset order.
    pub fn pages(&self) -> &[FredVintagePage] {
        &self.pages
    }

    /// Returns the complete provider-declared vintage set in strict ascending order.
    pub fn vintage_dates(&self) -> &[CalendarDate] {
        &self.vintage_dates
    }

    /// Returns one exact raw response material per page in the same order.
    pub fn captures(&self) -> &[ProviderCaptureMaterial] {
        &self.captures
    }

    /// Returns the checked aggregate provider-body bytes across the complete chain.
    pub const fn raw_body_bytes(&self) -> u64 {
        self.raw_body_bytes
    }

    /// Consumes the complete chain into its pages, dates, and raw materials.
    pub fn into_parts(
        self,
    ) -> (
        Box<[FredVintagePage]>,
        Box<[CalendarDate]>,
        Box<[ProviderCaptureMaterial]>,
    ) {
        (self.pages, self.vintage_dates, self.captures)
    }
}

impl FredVintageExtractionPage {
    /// Returns the strict offset-bearing vintage-date page.
    pub const fn page(&self) -> &FredVintagePage {
        &self.page
    }

    /// Returns exact evidence for the provider response bytes.
    pub const fn page_evidence(&self) -> &ExactPayloadEvidence {
        &self.page_evidence
    }

    /// Returns the local first-complete receipt instant.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Consumes the response into its strict page and raw capture material.
    pub fn into_parts(self) -> (FredVintagePage, ProviderCaptureMaterial) {
        (self.page, self.capture)
    }
}

/// Exact v2 release response and raw material required for immutable sealing.
#[derive(Debug)]
pub struct FredReleaseExtractionPage {
    page: FredReleaseObservationPage,
    page_evidence: ExactPayloadEvidence,
    received_at: Timestamp,
    capture: ProviderCaptureMaterial,
}

/// One complete, cursor-contiguous v2 release chain and all exact raw response material.
#[derive(Debug)]
pub struct FredReleaseExtraction {
    pages: Box<[FredReleaseObservationPage]>,
    observation_count: usize,
    raw_body_bytes: u64,
    captures: Box<[ProviderCaptureMaterial]>,
}

impl FredReleaseExtraction {
    /// Returns every validated response page in cursor order.
    pub fn pages(&self) -> &[FredReleaseObservationPage] {
        &self.pages
    }

    /// Returns the checked number of observations across the complete release chain.
    pub const fn observation_count(&self) -> usize {
        self.observation_count
    }

    /// Returns one exact raw response material per page in cursor order.
    pub fn captures(&self) -> &[ProviderCaptureMaterial] {
        &self.captures
    }

    /// Returns the checked aggregate provider-body bytes across the complete chain.
    pub const fn raw_body_bytes(&self) -> u64 {
        self.raw_body_bytes
    }

    /// Consumes the complete chain into its pages and raw materials.
    pub fn into_parts(
        self,
    ) -> (
        Box<[FredReleaseObservationPage]>,
        Box<[ProviderCaptureMaterial]>,
    ) {
        (self.pages, self.captures)
    }
}

impl FredReleaseExtractionPage {
    /// Returns the strict cursor-bearing v2 page.
    pub const fn page(&self) -> &FredReleaseObservationPage {
        &self.page
    }

    /// Returns exact evidence for the provider response bytes.
    pub const fn page_evidence(&self) -> &ExactPayloadEvidence {
        &self.page_evidence
    }

    /// Returns the local first-complete receipt instant.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Consumes the response into its strict page and raw capture material.
    pub fn into_parts(self) -> (FredReleaseObservationPage, ProviderCaptureMaterial) {
        (self.page, self.capture)
    }
}

impl FredSource {
    /// Acquires every v1 vintage-date page under one shared account budget.
    ///
    /// The caller supplies a finite page ceiling no greater than the raw-capture graph ceiling.
    /// The result is returned only after offsets, totals, real-time bounds, global date ordering,
    /// and terminal exhaustion all agree; a truncated prefix is never returned as complete.
    #[allow(
        clippy::too_many_arguments,
        reason = "provider dataset, page size/count, authority, deadline, operation, and cancellation are independent"
    )]
    pub async fn acquire_all_vintage_dates(
        &self,
        authority: &ExtractionAuthority,
        provider_dataset: &SourceIdentifier,
        page_limit: NonZeroU16,
        maximum_pages: NonZeroU16,
        deadline: Timestamp,
        operation: FredOperation,
        cancellation: CancellationToken,
    ) -> Result<FredVintageExtraction, ExtractionSourceError> {
        let maximum_pages = usize::from(maximum_pages.get());
        if page_limit.get() > MAX_VINTAGE_PAGE_RECORDS || maximum_pages > MAX_PROVIDER_CAPTURE_PAGES
        {
            return Err(ExtractionSourceError::Source(
                SourceError::InvalidProtocolState,
            ));
        }
        let mut pages = Vec::new();
        let mut dates = Vec::new();
        let mut captures = Vec::new();
        let mut raw_body_bytes = 0_u64;
        let mut offset = 0_usize;
        let mut expected_count = None;
        let mut expected_interval = None;
        loop {
            if pages.len() >= maximum_pages {
                return Err(ExtractionSourceError::Source(
                    SourceError::InvalidProtocolState,
                ));
            }
            let extracted = self
                .acquire_vintage_page(
                    authority,
                    provider_dataset,
                    offset,
                    page_limit,
                    deadline,
                    operation,
                    cancellation.clone(),
                )
                .await?;
            let (page, capture) = extracted.into_parts();
            raw_body_bytes = add_capture_bytes(raw_body_bytes, &capture)?;
            let interval = (page.realtime_start(), page.realtime_end());
            if page.offset() != offset
                || expected_count.is_some_and(|count| count != page.count())
                || expected_interval.is_some_and(|expected| expected != interval)
                || dates
                    .last()
                    .zip(page.vintage_dates().first())
                    .is_some_and(|(previous, next)| previous >= next)
            {
                return Err(ExtractionSourceError::Source(
                    SourceError::InvalidProtocolState,
                ));
            }
            expected_count = Some(page.count());
            expected_interval = Some(interval);
            dates.extend_from_slice(page.vintage_dates());
            let next = page.next_offset();
            pages.push(page);
            captures.push(capture);
            let Some(next) = next else {
                if expected_count != Some(dates.len()) {
                    return Err(ExtractionSourceError::Source(
                        SourceError::InvalidProtocolState,
                    ));
                }
                break;
            };
            if next <= offset || next != dates.len() {
                return Err(ExtractionSourceError::Source(
                    SourceError::InvalidProtocolState,
                ));
            }
            offset = next;
        }
        Ok(FredVintageExtraction {
            pages: pages.into_boxed_slice(),
            vintage_dates: dates.into_boxed_slice(),
            raw_body_bytes,
            captures: captures.into_boxed_slice(),
        })
    }

    /// Acquires one exact v1 vintage-date page using offset/count pagination.
    ///
    /// The same account-scoped extraction authority used by v1 observations and v2 releases
    /// governs this request. Production onboarding supplies one shared one-request-per-second
    /// budget, so this method cannot form an independent provider throttle.
    #[allow(
        clippy::too_many_arguments,
        reason = "provider dataset, offset, limit, authority, deadline, operation, and cancellation are independent"
    )]
    pub async fn acquire_vintage_page(
        &self,
        authority: &ExtractionAuthority,
        provider_dataset: &SourceIdentifier,
        offset: usize,
        limit: NonZeroU16,
        deadline: Timestamp,
        operation: FredOperation,
        cancellation: CancellationToken,
    ) -> Result<FredVintageExtractionPage, ExtractionSourceError> {
        self.validate_authority(authority)?;
        if limit.get() > MAX_VINTAGE_PAGE_RECORDS
            || !matches!(
                operation,
                FredOperation::RetrieveEphemeral | FredOperation::Persist
            )
        {
            return Err(ExtractionSourceError::Source(
                SourceError::InvalidProtocolState,
            ));
        }
        let dataset = FredDataset::parse(provider_dataset)
            .map_err(|_| ExtractionSourceError::Source(SourceError::InvalidProtocolState))?;
        self.authorize_series_operation(&dataset, operation)?;
        let mut public_url = url::Url::parse(VINTAGE_DATES_ENDPOINT)
            .map_err(|_| ExtractionSourceError::Source(SourceError::InvalidProtocolState))?;
        public_url
            .query_pairs_mut()
            .append_pair("series_id", dataset.series_id())
            .append_pair("realtime_start", &dataset.realtime_start().to_string())
            .append_pair("realtime_end", &dataset.realtime_end().to_string())
            .append_pair("limit", &limit.to_string())
            .append_pair("offset", &offset.to_string())
            .append_pair("sort_order", "asc")
            .append_pair("file_type", "json");
        let mut authorization_target = public_url.clone();
        authorization_target
            .query_pairs_mut()
            .append_pair("api_key", self.api_key.expose());
        let (response, in_flight) = self
            .execute_macro_request(
                authority,
                &authorization_target,
                public_url.clone(),
                FredHttpAuthorization::QueryParameter,
                deadline,
                cancellation,
            )
            .await?;
        let page = FredVintagePage::parse(
            &response.body,
            FredParseLimits::try_new(
                usize::from(limit.get()),
                self.response_limit,
                self.response_limit.min(MAX_RELEASE_STRING_BYTES),
            )
            .map_err(|_| ExtractionSourceError::Source(SourceError::InvalidProtocolState))?,
        )
        .map_err(|_| ExtractionSourceError::Source(SourceError::InvalidProtocolState))?;
        if page.offset() != offset
            || page.limit() != usize::from(limit.get())
            || page.realtime_start() != dataset.realtime_start()
            || page.realtime_end() != dataset.realtime_end()
        {
            return Err(ExtractionSourceError::Source(
                SourceError::InvalidProtocolState,
            ));
        }
        let capture = standalone_capture_material(
            &self.metadata,
            SourceIdentifier::try_from(format!(
                "alfred:series-vintage-dates:{}:{}:{}",
                dataset.series_id(),
                dataset.realtime_start(),
                dataset.realtime_end()
            ))
            .map_err(|_| ExtractionSourceError::Source(SourceError::InvalidProtocolState))?,
            &public_url,
            &response,
        )?;
        let extracted = FredVintageExtractionPage {
            page_evidence: evidence_for_payload(&response.body, &public_url)
                .map_err(map_adapter_error)?,
            received_at: response.received_at,
            page,
            capture,
        };
        in_flight.record_success()?;
        Ok(extracted)
    }

    /// Acquires one exact v2 release page using bearer-header authentication and cursor paging.
    ///
    /// `authorized_series` is a closed, deduplicated page scope. Every series is authorized before
    /// the request and every returned series must remain inside it. This prevents the bulk release
    /// endpoint from silently widening an exact owner-authorized series scope.
    #[allow(
        clippy::too_many_arguments,
        reason = "release identity, cursor, series scope, limit, authority, deadline, operation, and cancellation are independent"
    )]
    pub async fn acquire_release_page(
        &self,
        authority: &ExtractionAuthority,
        release_id: NonZeroU32,
        requested_cursor: Option<&FredReleaseCursor>,
        authorized_series: &[SourceIdentifier],
        limit: NonZeroU32,
        deadline: Timestamp,
        operation: FredOperation,
        cancellation: CancellationToken,
    ) -> Result<FredReleaseExtractionPage, ExtractionSourceError> {
        self.validate_authority(authority)?;
        let limit = usize::try_from(limit.get())
            .map_err(|_| ExtractionSourceError::Source(SourceError::InvalidProtocolState))?;
        if limit > MAX_FRED_V2_RELEASE_PAGE_OBSERVATIONS
            || authorized_series.is_empty()
            || authorized_series.len() > MAX_RELEASE_SCOPE_SERIES
            || !matches!(
                operation,
                FredOperation::RetrieveEphemeral | FredOperation::Persist
            )
        {
            return Err(ExtractionSourceError::Source(
                SourceError::InvalidProtocolState,
            ));
        }
        let exact_scope = authorized_series.iter().cloned().collect::<BTreeSet<_>>();
        if exact_scope.len() != authorized_series.len() {
            return Err(ExtractionSourceError::Source(
                SourceError::InvalidProtocolState,
            ));
        }
        for series in &exact_scope {
            let decision = self
                .rights
                .assess(
                    series,
                    &[operation],
                    system_timestamp().map_err(map_adapter_error)?,
                )
                .map_err(|_| ExtractionSourceError::Source(SourceError::Unauthorized))?;
            if decision.disposition() != FredRightsDisposition::Permitted {
                return Err(ExtractionSourceError::Source(SourceError::Unauthorized));
            }
        }
        let mut public_url = url::Url::parse(RELEASE_OBSERVATIONS_V2_ENDPOINT)
            .map_err(|_| ExtractionSourceError::Source(SourceError::InvalidProtocolState))?;
        public_url
            .query_pairs_mut()
            .append_pair("release_id", &release_id.to_string())
            .append_pair("format", "json")
            .append_pair("limit", &limit.to_string());
        if let Some(cursor) = requested_cursor {
            public_url
                .query_pairs_mut()
                .append_pair("next_cursor", cursor.encoded());
        }
        let (response, in_flight) = self
            .execute_macro_request(
                authority,
                &public_url,
                public_url.clone(),
                FredHttpAuthorization::BearerHeader,
                deadline,
                cancellation,
            )
            .await?;
        let page = FredReleaseObservationPage::parse_for_request(
            &response.body,
            FredParseLimits::try_new(
                limit,
                self.response_limit,
                self.response_limit.min(MAX_RELEASE_STRING_BYTES),
            )
            .map_err(|_| ExtractionSourceError::Source(SourceError::InvalidProtocolState))?,
            release_id.get(),
            requested_cursor,
        )
        .map_err(|_| ExtractionSourceError::Source(SourceError::InvalidProtocolState))?;
        if page
            .series()
            .iter()
            .any(|series| !exact_scope.contains(series.series_id()))
        {
            return Err(ExtractionSourceError::Source(SourceError::Unauthorized));
        }
        let capture = standalone_capture_material(
            &self.metadata,
            SourceIdentifier::try_from(format!(
                "fred:v2-release-observations:{}",
                release_id.get()
            ))
            .map_err(|_| ExtractionSourceError::Source(SourceError::InvalidProtocolState))?,
            &public_url,
            &response,
        )?;
        let extracted = FredReleaseExtractionPage {
            page_evidence: evidence_for_payload(&response.body, &public_url)
                .map_err(map_adapter_error)?,
            received_at: response.received_at,
            page,
            capture,
        };
        in_flight.record_success()?;
        Ok(extracted)
    }

    /// Acquires one complete v2 release chain under the same v1/v2 account budget.
    ///
    /// The exact authorized series set is treated as the complete expected release membership.
    /// Every member is rights-checked before every request; every returned member is checked again,
    /// and terminal publication requires equality with the expected set. Cursor continuity,
    /// release/source attribution, split-series metadata, observation order, and total bounds are
    /// validated across pages. This method is therefore appropriate only for a release whose
    /// complete membership has already been admitted by the owner authority.
    #[allow(
        clippy::too_many_arguments,
        reason = "release/scope, page size/count, authority, deadline, operation, and cancellation are independent"
    )]
    pub async fn acquire_complete_release(
        &self,
        authority: &ExtractionAuthority,
        release_id: NonZeroU32,
        authorized_series: &[SourceIdentifier],
        page_limit: NonZeroU32,
        maximum_pages: NonZeroU16,
        deadline: Timestamp,
        operation: FredOperation,
        cancellation: CancellationToken,
    ) -> Result<FredReleaseExtraction, ExtractionSourceError> {
        let maximum_pages = usize::from(maximum_pages.get());
        if maximum_pages > MAX_PROVIDER_CAPTURE_PAGES {
            return Err(ExtractionSourceError::Source(
                SourceError::InvalidProtocolState,
            ));
        }
        let expected_series = authorized_series.iter().cloned().collect::<BTreeSet<_>>();
        if expected_series.len() != authorized_series.len() || expected_series.is_empty() {
            return Err(ExtractionSourceError::Source(
                SourceError::InvalidProtocolState,
            ));
        }
        let mut pages = Vec::new();
        let mut captures = Vec::new();
        let mut cursor = None;
        let mut release = None;
        let mut observed_series = BTreeSet::new();
        let mut last_series: Option<crate::FredReleaseSeries> = None;
        let mut observation_count = 0_usize;
        let mut raw_body_bytes = 0_u64;
        loop {
            if pages.len() >= maximum_pages {
                return Err(ExtractionSourceError::Source(
                    SourceError::InvalidProtocolState,
                ));
            }
            let extracted = self
                .acquire_release_page(
                    authority,
                    release_id,
                    cursor.as_ref(),
                    authorized_series,
                    page_limit,
                    deadline,
                    operation,
                    cancellation.clone(),
                )
                .await?;
            let (page, capture) = extracted.into_parts();
            raw_body_bytes = add_capture_bytes(raw_body_bytes, &capture)?;
            if release
                .as_ref()
                .is_some_and(|expected| expected != page.release())
            {
                return Err(ExtractionSourceError::Source(
                    SourceError::GenerationResynchronizationRequired,
                ));
            }
            release.get_or_insert_with(|| page.release().clone());
            for series in page.series() {
                if let Some(previous) = last_series.as_ref() {
                    match previous.series_id().cmp(series.series_id()) {
                        std::cmp::Ordering::Greater => {
                            return Err(ExtractionSourceError::Source(
                                SourceError::InvalidProtocolState,
                            ));
                        }
                        std::cmp::Ordering::Equal => {
                            if !release_series_metadata_matches(previous, series)
                                || previous
                                    .observations()
                                    .last()
                                    .zip(series.observations().first())
                                    .is_some_and(|(left, right)| {
                                        left.observation_date() >= right.observation_date()
                                    })
                            {
                                return Err(ExtractionSourceError::Source(
                                    SourceError::GenerationResynchronizationRequired,
                                ));
                            }
                        }
                        std::cmp::Ordering::Less => {}
                    }
                }
                observed_series.insert(series.series_id().clone());
                last_series = Some(series.clone());
            }
            observation_count = observation_count
                .checked_add(page.observation_count())
                .ok_or_else(|| ExtractionSourceError::Source(SourceError::InvalidProtocolState))?;
            let has_more = page.has_more();
            let next = page.next_cursor().cloned();
            pages.push(page);
            captures.push(capture);
            if !has_more {
                if next.is_some() || observed_series != expected_series {
                    return Err(ExtractionSourceError::Source(
                        SourceError::GenerationResynchronizationRequired,
                    ));
                }
                break;
            }
            let next = next
                .ok_or_else(|| ExtractionSourceError::Source(SourceError::InvalidProtocolState))?;
            if cursor.as_ref().is_some_and(|previous: &FredReleaseCursor| {
                previous.series_id() > next.series_id()
                    || (previous.series_id() == next.series_id()
                        && previous.observation_date() >= next.observation_date())
            }) {
                return Err(ExtractionSourceError::Source(
                    SourceError::InvalidProtocolState,
                ));
            }
            cursor = Some(next);
        }
        Ok(FredReleaseExtraction {
            pages: pages.into_boxed_slice(),
            observation_count,
            raw_body_bytes,
            captures: captures.into_boxed_slice(),
        })
    }

    fn authorize_series_operation(
        &self,
        dataset: &FredDataset,
        operation: FredOperation,
    ) -> Result<(), ExtractionSourceError> {
        let series = SourceIdentifier::try_from(dataset.series_id())
            .map_err(|_| ExtractionSourceError::Source(SourceError::InvalidProtocolState))?;
        let now = system_timestamp().map_err(map_adapter_error)?;
        let decision = self
            .rights
            .assess(&series, &[operation], now)
            .map_err(|_| ExtractionSourceError::Source(SourceError::Unauthorized))?;
        if decision.disposition() != FredRightsDisposition::Permitted {
            return Err(ExtractionSourceError::Source(SourceError::Unauthorized));
        }
        Ok(())
    }

    async fn execute_macro_request(
        &self,
        authority: &ExtractionAuthority,
        authorization_target: &url::Url,
        public_url: url::Url,
        authorization: FredHttpAuthorization,
        deadline: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<(super::FredHttpResponse, InFlightExtractionRequest), ExtractionSourceError> {
        if cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled);
        }
        let now = system_timestamp().map_err(map_adapter_error)?;
        if deadline <= now {
            return Err(ExtractionSourceError::DeadlineExceeded);
        }
        let permit = acquire_request_permit(
            authority,
            authorization_target.as_str(),
            deadline,
            cancellation.clone(),
        )
        .await?;
        let in_flight = permit.authorize_send(authorization_target.as_str())?;
        let wall_remaining = deadline
            .unix_nanos()
            .checked_sub(now.unix_nanos())
            .and_then(|nanos| u64::try_from(nanos).ok())
            .map(Duration::from_nanos)
            .ok_or(ExtractionSourceError::DeadlineExceeded)?;
        let response = self
            .transport
            .execute(
                FredHttpRequest {
                    public_url,
                    api_key: self.api_key.clone(),
                    authorization,
                },
                self.response_limit,
                self.request_timeout.min(wall_remaining),
                cancellation,
            )
            .await
            .map_err(map_adapter_error)?;
        in_flight.validate_response_size(
            u64::try_from(response.body.len())
                .map_err(|_| ExtractionSourceError::Source(SourceError::InvalidProtocolState))?,
        )?;
        if response
            .content_encoding
            .as_deref()
            .is_some_and(|value| !value.eq_ignore_ascii_case(b"identity"))
        {
            return Err(ExtractionSourceError::Source(
                SourceError::InvalidProtocolState,
            ));
        }
        match response.status {
            200 => Ok((response, in_flight)),
            401 | 403 => Err(ExtractionSourceError::Source(SourceError::Unauthorized)),
            429 | 503 => {
                let retry_deadline =
                    in_flight.apply_retry_after_header(response.retry_after.as_deref(), 0)?;
                Err(ExtractionSourceError::Source(
                    SourceError::BudgetWaitUntil {
                        deadline: retry_deadline,
                    },
                ))
            }
            _ => Err(ExtractionSourceError::Source(SourceError::Network)),
        }
    }
}

fn add_capture_bytes(
    current: u64,
    capture: &ProviderCaptureMaterial,
) -> Result<u64, ExtractionSourceError> {
    current
        .checked_add(capture.receipt().total_body_bytes())
        .filter(|total| *total <= MAX_PROVIDER_CAPTURE_BYTES)
        .ok_or_else(|| ExtractionSourceError::Source(SourceError::InvalidProtocolState))
}

fn release_series_metadata_matches(
    left: &crate::FredReleaseSeries,
    right: &crate::FredReleaseSeries,
) -> bool {
    left.series_id() == right.series_id()
        && left.title() == right.title()
        && left.frequency() == right.frequency()
        && left.units() == right.units()
        && left.seasonal_adjustment() == right.seasonal_adjustment()
        && left.last_updated() == right.last_updated()
        && left.copyright_id() == right.copyright_id()
        && left.notes() == right.notes()
}
