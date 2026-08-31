//! Provider-local activation, acquisition, accounting, and dashboard-read contracts.
//!
//! These types deliberately stop at the provider boundary. The application owns durable catalog
//! reads and publication transactions; this module supplies the exact dataset inventory and
//! technical evidence predicates needed to validate Treasury acquisition and normalization.
//!
//! `FiscalData` denotes the selected Average Interest Rates V2 family only. Broader auction,
//! debt, and fiscal datasets require their own closed dictionaries and canonical variants and are
//! never inferred complete from this family.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use market_squawk_domain::{
    CalendarDate, DigestAlgorithm, EvidenceDigest, MetadataRevision, SourceId, SourceIdentifier,
    Timestamp,
};
use market_squawk_sources::{
    BudgetWindowSemantics, DiscoveryBatch, ExtractionBatch, ProviderBudgetPolicy,
    ProviderCaptureMaterial, SealedProviderCaptureSetReceipt, SourceMetadata,
};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::{
    FiscalDataParseLimits, TreasuryDailyRateFamily, TreasuryDailyRatePage,
    TreasuryDailyRatePageRequest, TreasuryDailyRatePeriodKind, TreasuryDailyRateQuery,
    TreasuryPageRequest, TreasuryProtocolError, TreasurySourceConfig,
};

/// Code-owned activation surface for one exact Treasury adapter generation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TreasurySurface {
    /// Selected Fiscal Data Average Interest Rates V2 REST API.
    FiscalData,
    /// Daily interest-rate Atom/XML feed.
    DailyRatesXml,
}

impl TreasurySurface {
    /// Returns the exact built-in onboarding/runtime profile identity.
    pub const fn profile_id(self) -> &'static str {
        match self {
            Self::FiscalData => "treasury.fiscal-data",
            Self::DailyRatesXml => "treasury.daily-rates-xml",
        }
    }

    /// Returns whether this public GET surface needs a credential.
    pub const fn requires_credential(self) -> bool {
        false
    }
}

/// Closed provider family represented by one configured Treasury dataset.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "family")]
pub enum TreasuryDatasetFamily {
    /// Fiscal Data Average Interest Rates v2.
    AverageInterestRatesV2,
    /// One of the five daily-rate XML families.
    DailyRate(TreasuryDailyRateFamily),
}

impl TreasuryDatasetFamily {
    /// Returns a stable provider-local family name suitable for receipts and UI routing.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AverageInterestRatesV2 => "average_interest_rates_v2",
            Self::DailyRate(TreasuryDailyRateFamily::NominalParYieldCurve) => {
                "nominal_par_yield_curve"
            }
            Self::DailyRate(TreasuryDailyRateFamily::BillRates) => "bill_rates",
            Self::DailyRate(TreasuryDailyRateFamily::LongTermRates) => "long_term_rates",
            Self::DailyRate(TreasuryDailyRateFamily::RealParYieldCurve) => "real_par_yield_curve",
            Self::DailyRate(TreasuryDailyRateFamily::RealLongTermRates) => "real_long_term_rates",
        }
    }
}

/// Exact temporal scope bound into one provider query.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum TreasuryDatasetPeriod {
    /// Inclusive Fiscal Data civil-date range.
    FiscalDateRange {
        /// First admitted record date.
        first: CalendarDate,
        /// Final admitted record date.
        last: CalendarDate,
        /// Exact provider page size.
        page_size: u16,
    },
    /// One complete calendar year.
    CalendarYear {
        /// Selected year.
        year: u16,
    },
    /// One complete calendar month.
    CalendarMonth {
        /// Selected year.
        year: u16,
        /// Selected month.
        month: u8,
    },
    /// Treasury's zero-based, empty-terminal all-history response chain.
    AllHistory,
}

/// Provider acquisition behavior required for one exact query shape.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TreasuryPublicationMode {
    /// The complete dataset is carried by one bounded provider response.
    AtomicResponse,
    /// Every one-based page must be captured before the page chain can be marked complete.
    CompletePageChain,
    /// A separately checkpointed backfill is required; ordinary one-shot extraction is closed.
    ResumableBackfill,
}

/// Exact provider and analytical identities for one configured Treasury query.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TreasuryDatasetDescriptor {
    surface: TreasurySurface,
    family: TreasuryDatasetFamily,
    provider_dataset: SourceIdentifier,
    analytical_dataset: SourceIdentifier,
    period: TreasuryDatasetPeriod,
    query_digest: EvidenceDigest,
    publication_mode: TreasuryPublicationMode,
}

impl TreasuryDatasetDescriptor {
    /// Returns the owning activation surface.
    pub const fn surface(&self) -> TreasurySurface {
        self.surface
    }

    /// Returns the exact provider family.
    pub const fn family(&self) -> TreasuryDatasetFamily {
        self.family
    }

    /// Returns the exact provider selector accepted by discovery.
    pub const fn provider_dataset(&self) -> &SourceIdentifier {
        &self.provider_dataset
    }

    /// Returns the storage-safe analytical dataset identity.
    pub const fn analytical_dataset(&self) -> &SourceIdentifier {
        &self.analytical_dataset
    }

    /// Returns the exact configured temporal scope.
    pub const fn period(&self) -> TreasuryDatasetPeriod {
        self.period
    }

    /// Returns the digest of all provider query semantics except page number.
    pub const fn query_digest(&self) -> EvidenceDigest {
        self.query_digest
    }

    /// Returns the provider acquisition protocol required by this query shape.
    pub const fn publication_mode(&self) -> TreasuryPublicationMode {
        self.publication_mode
    }
}

/// Stable, duplicate-free inventory for one configured Treasury source generation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TreasuryDatasetCatalog {
    surface: TreasurySurface,
    datasets: Box<[TreasuryDatasetDescriptor]>,
    complete_selected_family_coverage: bool,
}

/// Exact configured Treasury inventory bound to one adapter generation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TreasuryActivationIntent {
    catalog: TreasuryDatasetCatalog,
    intent_digest: EvidenceDigest,
}

impl TreasuryActivationIntent {
    /// Constructs one provider activation intent from the exact configured dataset inventory.
    pub fn try_new(config: &TreasurySourceConfig) -> Result<Self, TreasuryVerticalError> {
        let catalog = TreasuryDatasetCatalog::try_from_config(config)?;
        let wire = serde_json::to_vec(&catalog)
            .map_err(|_| TreasuryVerticalError::InvalidConfiguration)?;
        let intent_digest =
            domain_separated_digest(b"market-squawk/treasury-activation-intent/v1\0", &wire);
        Ok(Self {
            catalog,
            intent_digest,
        })
    }

    /// Returns the exact configured dataset inventory.
    pub const fn catalog(&self) -> &TreasuryDatasetCatalog {
        &self.catalog
    }

    /// Returns the stable identity of the exact configured inventory.
    pub const fn intent_digest(&self) -> EvidenceDigest {
        self.intent_digest
    }

    /// Builds one bounded representative doctor request per configured Treasury family.
    pub fn doctor_plan(
        &self,
        config: &TreasurySourceConfig,
    ) -> Result<TreasuryDoctorPlan, TreasuryVerticalError> {
        if TreasuryDatasetCatalog::try_from_config(config)? != self.catalog {
            return Err(TreasuryVerticalError::InvalidConfiguration);
        }
        self.catalog.doctor_plan(config, self.intent_digest)
    }
}

impl TreasuryDatasetCatalog {
    /// Builds the exact provider/analytical dataset inventory carried by a source configuration.
    pub(crate) fn try_from_config(
        config: &TreasurySourceConfig,
    ) -> Result<Self, TreasuryVerticalError> {
        let (surface, datasets) = match config {
            TreasurySourceConfig::AverageInterestRates(query) => {
                let descriptor = TreasuryDatasetDescriptor {
                    surface: TreasurySurface::FiscalData,
                    family: TreasuryDatasetFamily::AverageInterestRatesV2,
                    provider_dataset: query
                        .dataset()
                        .map_err(|_| TreasuryVerticalError::InvalidConfiguration)?,
                    analytical_dataset: query
                        .analytical_dataset()
                        .map_err(|_| TreasuryVerticalError::InvalidConfiguration)?,
                    period: TreasuryDatasetPeriod::FiscalDateRange {
                        first: query.first_record_date(),
                        last: query.last_record_date(),
                        page_size: query.page_size().get(),
                    },
                    query_digest: sha256(query.query_digest()),
                    publication_mode: TreasuryPublicationMode::CompletePageChain,
                };
                (TreasurySurface::FiscalData, vec![descriptor])
            }
            TreasurySourceConfig::DailyRates(config) => {
                let descriptors = config
                    .queries()
                    .iter()
                    .map(daily_rate_descriptor)
                    .collect::<Result<Vec<_>, _>>()?;
                (TreasurySurface::DailyRatesXml, descriptors)
            }
        };
        if datasets.is_empty() {
            return Err(TreasuryVerticalError::InvalidConfiguration);
        }
        let mut provider_datasets = BTreeSet::new();
        let mut analytical_datasets = BTreeSet::new();
        if datasets.iter().any(|dataset| {
            dataset.surface != surface
                || !provider_datasets.insert(dataset.provider_dataset.clone())
                || !analytical_datasets.insert(dataset.analytical_dataset.clone())
        }) {
            return Err(TreasuryVerticalError::InvalidConfiguration);
        }
        let complete_selected_family_coverage = match surface {
            TreasurySurface::FiscalData => datasets.len() == 1,
            TreasurySurface::DailyRatesXml => {
                TreasuryDailyRateFamily::ALL.into_iter().all(|family| {
                    datasets
                        .iter()
                        .any(|dataset| dataset.family == TreasuryDatasetFamily::DailyRate(family))
                })
            }
        };
        Ok(Self {
            surface,
            datasets: datasets.into_boxed_slice(),
            complete_selected_family_coverage,
        })
    }

    /// Returns the configured activation surface.
    pub const fn surface(&self) -> TreasurySurface {
        self.surface
    }

    /// Returns all exact configured datasets in stable query order.
    pub fn datasets(&self) -> &[TreasuryDatasetDescriptor] {
        &self.datasets
    }

    /// Finds one descriptor by exact provider selector.
    pub fn dataset(
        &self,
        provider_dataset: &SourceIdentifier,
    ) -> Option<&TreasuryDatasetDescriptor> {
        self.datasets
            .iter()
            .find(|dataset| dataset.provider_dataset == *provider_dataset)
    }

    /// Returns whether this configuration covers its complete selected product family set.
    ///
    /// Fiscal coverage means the one closed Average Interest Rates v2 mapping. Daily-rate
    /// coverage means all five official XML families, regardless of the number of year slices.
    pub const fn complete_selected_family_coverage(&self) -> bool {
        self.complete_selected_family_coverage
    }

    /// Builds a bounded doctor plan with one representative exact query per configured family.
    ///
    /// A multi-year activation is not probed once per year. The catalog above remains the exact
    /// activation intent; the doctor chooses the latest bounded year/month query for each family
    /// and never presents an all-history first page as completeness proof.
    fn doctor_plan(
        &self,
        config: &TreasurySourceConfig,
        activation_intent_digest: EvidenceDigest,
    ) -> Result<TreasuryDoctorPlan, TreasuryVerticalError> {
        if Self::try_from_config(config)? != *self {
            return Err(TreasuryVerticalError::InvalidConfiguration);
        }
        let probes = match config {
            TreasurySourceConfig::AverageInterestRates(query) => {
                let descriptor = self
                    .datasets
                    .first()
                    .cloned()
                    .ok_or(TreasuryVerticalError::InvalidConfiguration)?;
                let request = query
                    .page(1)
                    .map_err(|_| TreasuryVerticalError::InvalidConfiguration)?;
                vec![TreasuryDoctorProbe::fiscal(descriptor, request)]
            }
            TreasurySourceConfig::DailyRates(config) => {
                let mut selected =
                    BTreeMap::<TreasuryDailyRateFamily, &TreasuryDailyRateQuery>::new();
                for query in config.queries() {
                    match selected.get(&query.family()) {
                        Some(current) if doctor_query_rank(current) >= doctor_query_rank(query) => {
                        }
                        _ => {
                            selected.insert(query.family(), query);
                        }
                    }
                }
                let mut probes = Vec::new();
                probes
                    .try_reserve_exact(selected.len())
                    .map_err(|_| TreasuryVerticalError::AccountingOverflow)?;
                for family in TreasuryDailyRateFamily::ALL {
                    let Some(query) = selected.get(&family).copied() else {
                        continue;
                    };
                    let descriptor = self
                        .dataset(query.dataset())
                        .cloned()
                        .ok_or(TreasuryVerticalError::InvalidConfiguration)?;
                    let request = query
                        .page(0)
                        .map_err(|_| TreasuryVerticalError::InvalidConfiguration)?;
                    probes.push(TreasuryDoctorProbe::daily(descriptor, request));
                }
                probes
            }
        };
        if probes.is_empty() {
            return Err(TreasuryVerticalError::InvalidConfiguration);
        }
        Ok(TreasuryDoctorPlan {
            surface: self.surface,
            complete_selected_family_coverage: self.complete_selected_family_coverage,
            activation_intent_digest,
            probes: probes.into_boxed_slice(),
        })
    }
}

fn daily_rate_descriptor(
    query: &TreasuryDailyRateQuery,
) -> Result<TreasuryDatasetDescriptor, TreasuryVerticalError> {
    let period = match query.period().kind() {
        TreasuryDailyRatePeriodKind::Year => TreasuryDatasetPeriod::CalendarYear {
            year: query
                .period()
                .year_value()
                .ok_or(TreasuryVerticalError::InvalidConfiguration)?,
        },
        TreasuryDailyRatePeriodKind::Month => TreasuryDatasetPeriod::CalendarMonth {
            year: query
                .period()
                .year_value()
                .ok_or(TreasuryVerticalError::InvalidConfiguration)?,
            month: query
                .period()
                .month_value()
                .ok_or(TreasuryVerticalError::InvalidConfiguration)?,
        },
        TreasuryDailyRatePeriodKind::AllHistory => TreasuryDatasetPeriod::AllHistory,
    };
    Ok(TreasuryDatasetDescriptor {
        surface: TreasurySurface::DailyRatesXml,
        family: TreasuryDatasetFamily::DailyRate(query.family()),
        provider_dataset: query.dataset().clone(),
        analytical_dataset: query.analytical_dataset().clone(),
        period,
        query_digest: sha256(query.query_digest()),
        publication_mode: if query.is_all_history() {
            TreasuryPublicationMode::ResumableBackfill
        } else {
            TreasuryPublicationMode::AtomicResponse
        },
    })
}

fn doctor_query_rank(query: &TreasuryDailyRateQuery) -> (u16, u8, u8) {
    match query.period().kind() {
        TreasuryDailyRatePeriodKind::Year => {
            (query.period().year_value().unwrap_or_default(), 12, 1)
        }
        TreasuryDailyRatePeriodKind::Month => (
            query.period().year_value().unwrap_or_default(),
            query.period().month_value().unwrap_or_default(),
            2,
        ),
        TreasuryDailyRatePeriodKind::AllHistory => (0, 0, 0),
    }
}

fn dashboard_descriptor_rank(descriptor: &TreasuryDatasetDescriptor) -> (u16, u8, u8) {
    match descriptor.period {
        TreasuryDatasetPeriod::CalendarYear { year } => (year, 12, 1),
        TreasuryDatasetPeriod::CalendarMonth { year, month } => (year, month, 2),
        TreasuryDatasetPeriod::FiscalDateRange { last, .. } => (last.year(), last.month(), 3),
        TreasuryDatasetPeriod::AllHistory => (0, 0, 0),
    }
}

fn metric_available_in_period(
    metric: crate::TreasuryDailyRateMetric,
    period: TreasuryDatasetPeriod,
) -> bool {
    match period {
        TreasuryDatasetPeriod::CalendarYear { year }
        | TreasuryDatasetPeriod::CalendarMonth { year, .. } => year >= metric.first_schema_year(),
        TreasuryDatasetPeriod::AllHistory => true,
        TreasuryDatasetPeriod::FiscalDateRange { .. } => false,
    }
}

fn dashboard_descriptors(
    catalog: &TreasuryDatasetCatalog,
) -> Result<Vec<&TreasuryDatasetDescriptor>, TreasuryVerticalError> {
    match catalog.surface {
        TreasurySurface::FiscalData => {
            let selected = catalog.datasets.iter().collect::<Vec<_>>();
            if selected.len() != 1 {
                return Err(TreasuryVerticalError::InvalidConfiguration);
            }
            Ok(selected)
        }
        TreasurySurface::DailyRatesXml => {
            let mut selected =
                BTreeMap::<TreasuryDailyRateFamily, &TreasuryDatasetDescriptor>::new();
            for descriptor in &catalog.datasets {
                let TreasuryDatasetFamily::DailyRate(family) = descriptor.family else {
                    return Err(TreasuryVerticalError::InvalidConfiguration);
                };
                match selected.get(&family) {
                    Some(current)
                        if dashboard_descriptor_rank(current)
                            >= dashboard_descriptor_rank(descriptor) => {}
                    _ => {
                        selected.insert(family, descriptor);
                    }
                }
            }
            Ok(TreasuryDailyRateFamily::ALL
                .into_iter()
                .filter_map(|family| selected.get(&family).copied())
                .collect())
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TreasuryDoctorProbeRequest {
    Fiscal(TreasuryPageRequest),
    Daily(TreasuryDailyRatePageRequest),
}

/// One bounded, exact provider request selected by the Treasury doctor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreasuryDoctorProbe {
    descriptor: TreasuryDatasetDescriptor,
    request_url: String,
    request_digest: EvidenceDigest,
    request: TreasuryDoctorProbeRequest,
}

impl TreasuryDoctorProbe {
    fn fiscal(descriptor: TreasuryDatasetDescriptor, request: TreasuryPageRequest) -> Self {
        Self {
            request_url: request.url().to_owned(),
            request_digest: sha256(request.request_digest()),
            descriptor,
            request: TreasuryDoctorProbeRequest::Fiscal(request),
        }
    }

    fn daily(descriptor: TreasuryDatasetDescriptor, request: TreasuryDailyRatePageRequest) -> Self {
        Self {
            request_url: request.url().to_owned(),
            request_digest: sha256(request.request_digest()),
            descriptor,
            request: TreasuryDoctorProbeRequest::Daily(request),
        }
    }

    /// Returns the exact configured dataset represented by this probe.
    pub const fn descriptor(&self) -> &TreasuryDatasetDescriptor {
        &self.descriptor
    }

    /// Returns the allowlisted official HTTPS URL.
    pub fn request_url(&self) -> &str {
        &self.request_url
    }

    /// Returns the digest of exact request semantics including page number.
    pub const fn request_digest(&self) -> EvidenceDigest {
        self.request_digest
    }

    pub(crate) const fn fiscal_request(&self) -> Option<&TreasuryPageRequest> {
        match &self.request {
            TreasuryDoctorProbeRequest::Fiscal(request) => Some(request),
            TreasuryDoctorProbeRequest::Daily(_) => None,
        }
    }

    pub(crate) const fn daily_request(&self) -> Option<&TreasuryDailyRatePageRequest> {
        match &self.request {
            TreasuryDoctorProbeRequest::Fiscal(_) => None,
            TreasuryDoctorProbeRequest::Daily(request) => Some(request),
        }
    }

    /// Parses and accounts for one completed HTTP response without inventing provider capacity.
    ///
    /// # Errors
    ///
    /// Rejects a non-200 status, invalid latency, an oversized body, or any provider-schema,
    /// query-binding, date, decimal, namespace, or family drift.
    pub(crate) fn inspect_response(
        &self,
        metadata: &SourceMetadata,
        http_status: u16,
        body: &[u8],
        received_at: Timestamp,
        normalized_at: Timestamp,
        latency: Duration,
    ) -> Result<TreasuryDoctorObservation, TreasuryVerticalError> {
        if http_status != 200 || normalized_at < received_at {
            return Err(TreasuryVerticalError::DoctorRejected);
        }
        let body_bytes =
            u64::try_from(body.len()).map_err(|_| TreasuryVerticalError::AccountingOverflow)?;
        let latency_millis = u64::try_from(latency.as_millis())
            .map_err(|_| TreasuryVerticalError::AccountingOverflow)?;
        let limits = FiscalDataParseLimits::production_defaults();
        let (
            response_page,
            returned_source_rows,
            canonical_points,
            observed_numeric_points,
            explicit_missing_points,
            reported_total_rows,
            reported_total_pages,
            response_complete_for_query,
            provider_published_at,
            schema_digest,
        ) = match &self.request {
            TreasuryDoctorProbeRequest::Fiscal(request) => {
                let page = crate::FiscalDataPage::parse(body, request, limits)?;
                let mut canonical_admission =
                    crate::source::normalize::CanonicalRecordAdmission::new();
                for record in crate::source::normalize::canonical_fiscal_records(
                    metadata,
                    &page,
                    received_at,
                    normalized_at,
                ) {
                    canonical_admission
                        .admit(record.map_err(|_| TreasuryVerticalError::DoctorRejected)?)
                        .map_err(|_| TreasuryVerticalError::DoctorRejected)?;
                }
                (
                    u64::try_from(page.page_number())
                        .map_err(|_| TreasuryVerticalError::AccountingOverflow)?,
                    u64::try_from(page.records().len())
                        .map_err(|_| TreasuryVerticalError::AccountingOverflow)?,
                    u64::try_from(canonical_admission.record_count())
                        .map_err(|_| TreasuryVerticalError::AccountingOverflow)?,
                    u64::try_from(canonical_admission.observed_numeric_points())
                        .map_err(|_| TreasuryVerticalError::AccountingOverflow)?,
                    u64::try_from(canonical_admission.explicit_missing_points())
                        .map_err(|_| TreasuryVerticalError::AccountingOverflow)?,
                    Some(
                        u64::try_from(page.total_count())
                            .map_err(|_| TreasuryVerticalError::AccountingOverflow)?,
                    ),
                    Some(
                        u64::try_from(page.total_pages())
                            .map_err(|_| TreasuryVerticalError::AccountingOverflow)?,
                    ),
                    page.total_pages() == 1,
                    None,
                    Some(sha256(page.schema_digest())),
                )
            }
            TreasuryDoctorProbeRequest::Daily(request) => {
                let page = TreasuryDailyRatePage::parse(body, request, limits)?;
                let mut canonical_admission =
                    crate::source::normalize::CanonicalRecordAdmission::new();
                for record in crate::source::normalize::canonical_daily_rate_records(
                    metadata,
                    &page,
                    received_at,
                    normalized_at,
                ) {
                    canonical_admission
                        .admit(record.map_err(|_| TreasuryVerticalError::DoctorRejected)?)
                        .map_err(|_| TreasuryVerticalError::DoctorRejected)?;
                }
                let canonical_points = u64::try_from(canonical_admission.record_count())
                    .map_err(|_| TreasuryVerticalError::AccountingOverflow)?;
                let observed_numeric_points =
                    u64::try_from(canonical_admission.observed_numeric_points())
                        .map_err(|_| TreasuryVerticalError::AccountingOverflow)?;
                let explicit_missing_points =
                    u64::try_from(canonical_admission.explicit_missing_points())
                        .map_err(|_| TreasuryVerticalError::AccountingOverflow)?;
                (
                    u64::try_from(page.page_number())
                        .map_err(|_| TreasuryVerticalError::AccountingOverflow)?,
                    u64::try_from(page.observations().len())
                        .map_err(|_| TreasuryVerticalError::AccountingOverflow)?,
                    canonical_points,
                    observed_numeric_points,
                    explicit_missing_points,
                    None,
                    None,
                    page.period().kind() != TreasuryDailyRatePeriodKind::AllHistory,
                    Some(page.feed_published_at()),
                    None,
                )
            }
        };
        if observed_numeric_points.checked_add(explicit_missing_points) != Some(canonical_points) {
            return Err(TreasuryVerticalError::DoctorRejected);
        }
        Ok(TreasuryDoctorObservation {
            surface: self.descriptor.surface,
            family: self.descriptor.family,
            provider_dataset: self.descriptor.provider_dataset.clone(),
            analytical_dataset: self.descriptor.analytical_dataset.clone(),
            query_digest: self.descriptor.query_digest,
            request_digest: self.request_digest,
            http_status,
            response_page,
            body_bytes,
            body_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(body).into()),
            returned_source_rows,
            canonical_points,
            observed_numeric_points,
            explicit_missing_points,
            reported_total_rows,
            reported_total_pages,
            response_complete_for_query,
            producing: observed_numeric_points > 0,
            received_at,
            provider_published_at,
            schema_digest,
            latency_millis,
        })
    }
}

/// Bounded activation doctor intent for one configured Treasury generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreasuryDoctorPlan {
    surface: TreasurySurface,
    complete_selected_family_coverage: bool,
    activation_intent_digest: EvidenceDigest,
    probes: Box<[TreasuryDoctorProbe]>,
}

impl TreasuryDoctorPlan {
    /// Returns the exact activation surface under examination.
    pub const fn surface(&self) -> TreasurySurface {
        self.surface
    }

    /// Returns one representative exact probe per configured family.
    pub fn probes(&self) -> &[TreasuryDoctorProbe] {
        &self.probes
    }

    /// Returns whether the activation intent covers the full selected product family set.
    pub const fn complete_selected_family_coverage(&self) -> bool {
        self.complete_selected_family_coverage
    }

    /// Closes the doctor only when every planned probe has one exact, unique observation.
    pub(crate) fn close(
        &self,
        observations: impl IntoIterator<Item = TreasuryDoctorObservation>,
        metadata: &SourceMetadata,
    ) -> Result<TreasuryDoctorReceipt, TreasuryVerticalError> {
        let rate_policy_digest = treasury_rate_policy_digest(
            metadata
                .budget_policy()
                .ok_or(TreasuryVerticalError::DoctorRejected)?,
        )?;
        let mut accepted_observations = Vec::new();
        for observation in observations {
            if accepted_observations.len() == self.probes.len() {
                return Err(TreasuryVerticalError::DoctorRejected);
            }
            accepted_observations
                .try_reserve(1)
                .map_err(|_| TreasuryVerticalError::AccountingOverflow)?;
            accepted_observations.push(observation);
        }
        let observations = accepted_observations;
        if observations.len() != self.probes.len() {
            return Err(TreasuryVerticalError::DoctorRejected);
        }
        let expected = self
            .probes
            .iter()
            .map(|probe| probe.request_digest)
            .collect::<Vec<_>>();
        let actual = observations
            .iter()
            .map(|observation| observation.request_digest)
            .collect::<Vec<_>>();
        if has_duplicate_digests(&expected)
            || has_duplicate_digests(&actual)
            || expected.iter().any(|digest| !actual.contains(digest))
            || actual.iter().any(|digest| !expected.contains(digest))
            || observations.iter().any(|observation| {
                observation
                    .observed_numeric_points
                    .checked_add(observation.explicit_missing_points)
                    != Some(observation.canonical_points)
                    || observation.producing != (observation.observed_numeric_points > 0)
                    || self
                        .probes
                        .iter()
                        .find(|probe| {
                            probe.request_digest == observation.request_digest
                                && probe.descriptor.surface == observation.surface
                                && probe.descriptor.family == observation.family
                                && probe.descriptor.provider_dataset == observation.provider_dataset
                                && probe.descriptor.analytical_dataset
                                    == observation.analytical_dataset
                                && probe.descriptor.query_digest == observation.query_digest
                        })
                        .is_none()
            })
        {
            return Err(TreasuryVerticalError::DoctorRejected);
        }
        let (
            total_body_bytes,
            returned_source_rows,
            canonical_points,
            observed_numeric_points,
            explicit_missing_points,
        ) = observations.iter().try_fold(
            (0_u64, 0_u64, 0_u64, 0_u64, 0_u64),
            |(bytes, rows, points, observed, missing), observation| {
                Ok::<_, TreasuryVerticalError>((
                    bytes
                        .checked_add(observation.body_bytes)
                        .ok_or(TreasuryVerticalError::AccountingOverflow)?,
                    rows.checked_add(observation.returned_source_rows)
                        .ok_or(TreasuryVerticalError::AccountingOverflow)?,
                    points
                        .checked_add(observation.canonical_points)
                        .ok_or(TreasuryVerticalError::AccountingOverflow)?,
                    observed
                        .checked_add(observation.observed_numeric_points)
                        .ok_or(TreasuryVerticalError::AccountingOverflow)?,
                    missing
                        .checked_add(observation.explicit_missing_points)
                        .ok_or(TreasuryVerticalError::AccountingOverflow)?,
                ))
            },
        )?;
        if observed_numeric_points.checked_add(explicit_missing_points) != Some(canonical_points) {
            return Err(TreasuryVerticalError::DoctorRejected);
        }
        let all_probes_producing = observations.iter().all(|observation| observation.producing);
        let all_probes_complete_for_query = observations
            .iter()
            .all(|observation| observation.response_complete_for_query);
        Ok(TreasuryDoctorReceipt {
            surface: self.surface,
            activation_intent_digest: self.activation_intent_digest,
            planned_probe_count: u16::try_from(self.probes.len())
                .map_err(|_| TreasuryVerticalError::AccountingOverflow)?,
            observed_probe_count: u16::try_from(observations.len())
                .map_err(|_| TreasuryVerticalError::AccountingOverflow)?,
            complete_selected_family_coverage: self.complete_selected_family_coverage,
            all_probes_producing,
            all_probes_complete_for_query,
            activation_ready: self.complete_selected_family_coverage && all_probes_producing,
            rate_policy_digest,
            total_body_bytes,
            returned_source_rows,
            canonical_points,
            observed_numeric_points,
            explicit_missing_points,
            observations: observations.into_boxed_slice(),
        })
    }
}

/// Parsed and fully accounted evidence from one Treasury doctor response.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TreasuryDoctorObservation {
    surface: TreasurySurface,
    family: TreasuryDatasetFamily,
    provider_dataset: SourceIdentifier,
    analytical_dataset: SourceIdentifier,
    query_digest: EvidenceDigest,
    request_digest: EvidenceDigest,
    http_status: u16,
    response_page: u64,
    body_bytes: u64,
    body_digest: EvidenceDigest,
    returned_source_rows: u64,
    canonical_points: u64,
    observed_numeric_points: u64,
    explicit_missing_points: u64,
    reported_total_rows: Option<u64>,
    reported_total_pages: Option<u64>,
    response_complete_for_query: bool,
    producing: bool,
    received_at: Timestamp,
    provider_published_at: Option<Timestamp>,
    schema_digest: Option<EvidenceDigest>,
    latency_millis: u64,
}

impl TreasuryDoctorObservation {
    /// Returns the exact request identity this observation satisfies.
    pub const fn request_digest(&self) -> EvidenceDigest {
        self.request_digest
    }

    /// Returns whether at least one canonical point carried an observed numeric value.
    pub const fn producing(&self) -> bool {
        self.producing
    }

    /// Returns whether this single response completes the configured query.
    pub const fn response_complete_for_query(&self) -> bool {
        self.response_complete_for_query
    }

    /// Returns the exact source-row count before metric expansion.
    pub const fn returned_source_rows(&self) -> u64 {
        self.returned_source_rows
    }

    /// Returns the number of canonical scalar observations produced by this response.
    pub const fn canonical_points(&self) -> u64 {
        self.canonical_points
    }

    /// Returns canonical points carrying an exact observed numeric value.
    pub const fn observed_numeric_points(&self) -> u64 {
        self.observed_numeric_points
    }

    /// Returns retained canonical points carrying explicit provider missingness.
    pub const fn explicit_missing_points(&self) -> u64 {
        self.explicit_missing_points
    }
}

/// Closed doctor result retaining exact per-family response and accounting evidence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TreasuryDoctorReceipt {
    surface: TreasurySurface,
    activation_intent_digest: EvidenceDigest,
    planned_probe_count: u16,
    observed_probe_count: u16,
    complete_selected_family_coverage: bool,
    all_probes_producing: bool,
    all_probes_complete_for_query: bool,
    activation_ready: bool,
    rate_policy_digest: EvidenceDigest,
    total_body_bytes: u64,
    returned_source_rows: u64,
    canonical_points: u64,
    observed_numeric_points: u64,
    explicit_missing_points: u64,
    observations: Box<[TreasuryDoctorObservation]>,
}

impl TreasuryDoctorReceipt {
    /// Returns whether every selected family completed one bounded real schema probe.
    ///
    /// This proves activation connectivity and parser compatibility only. Immutable publication,
    /// restart reads, dashboard coverage, and analytical product production remain separate gates.
    /// Fiscal success covers Average Interest Rates V2 only, never the broader Fiscal Data catalog.
    pub const fn activation_ready(&self) -> bool {
        self.activation_ready
    }

    /// Returns the exact shared rate/cooldown policy used by every doctor probe.
    pub const fn rate_policy_digest(&self) -> EvidenceDigest {
        self.rate_policy_digest
    }

    /// Returns whether every probe happened to cover its entire configured query.
    ///
    /// This is intentionally separate from activation readiness: bounded doctor probes do not
    /// replace complete Fiscal pagination or the resumable all-history state machine.
    pub const fn all_probes_complete_for_query(&self) -> bool {
        self.all_probes_complete_for_query
    }

    /// Returns whether the intent selected the complete product family set.
    pub const fn complete_selected_family_coverage(&self) -> bool {
        self.complete_selected_family_coverage
    }

    /// Returns the exact response observations retained by the receipt.
    pub fn observations(&self) -> &[TreasuryDoctorObservation] {
        &self.observations
    }

    /// Returns every canonical point validated across the doctor probes.
    pub const fn canonical_points(&self) -> u64 {
        self.canonical_points
    }

    /// Returns observed numeric points validated across the doctor probes.
    pub const fn observed_numeric_points(&self) -> u64 {
        self.observed_numeric_points
    }

    /// Returns explicit provider-missing points retained across the doctor probes.
    pub const fn explicit_missing_points(&self) -> u64 {
        self.explicit_missing_points
    }
}

/// Activation evidence whose exact network responses are durably sealed and identity-bound.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TreasurySealedDoctorReceipt {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    receipt: TreasuryDoctorReceipt,
    sealed_captures: Box<[SealedProviderCaptureSetReceipt]>,
    sealed_receipt_digest: EvidenceDigest,
}

impl TreasurySealedDoctorReceipt {
    pub(crate) fn try_new(
        source_id: SourceId,
        metadata_revision: MetadataRevision,
        receipt: TreasuryDoctorReceipt,
        sealed_captures: Vec<SealedProviderCaptureSetReceipt>,
    ) -> Result<Self, TreasuryVerticalError> {
        if sealed_captures.len() != receipt.observations.len()
            || sealed_captures.is_empty()
            || sealed_captures.iter().zip(receipt.observations.iter()).any(
                |(sealed, observation)| {
                    let capture = sealed.capture();
                    capture.source_id() != &source_id
                        || capture.metadata_revision() != &metadata_revision
                        || capture.dataset() != &observation.provider_dataset
                        || capture.pages().len() != 1
                        || capture.pages()[0].body_digest() != observation.body_digest
                        || capture.pages()[0].body_bytes() != observation.body_bytes
                        || sealed.receipt_digest().algorithm() != DigestAlgorithm::Sha256
                        || sealed.receipt_digest().bytes() == [0; 32]
                },
            )
        {
            return Err(TreasuryVerticalError::DoctorRejected);
        }
        let capture_receipts = sealed_captures
            .iter()
            .map(SealedProviderCaptureSetReceipt::receipt_digest)
            .collect::<Vec<_>>();
        if has_duplicate_digests(&capture_receipts) {
            return Err(TreasuryVerticalError::DoctorRejected);
        }
        let wire =
            serde_json::to_vec(&(&source_id, &metadata_revision, &receipt, &capture_receipts))
                .map_err(|_| TreasuryVerticalError::DoctorRejected)?;
        Ok(Self {
            source_id,
            metadata_revision,
            receipt,
            sealed_captures: sealed_captures.into_boxed_slice(),
            sealed_receipt_digest: domain_separated_digest(
                b"market-squawk/treasury-sealed-doctor-receipt/v1\0",
                &wire,
            ),
        })
    }

    /// Returns activation connectivity/schema status only after raw capture sealing.
    pub const fn activation_ready(&self) -> bool {
        self.receipt.activation_ready
    }

    /// Returns the exact source generation whose network authority executed the probes.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact source-metadata revision whose policy executed the probes.
    pub const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    /// Returns the exact parsed doctor evidence.
    pub const fn receipt(&self) -> &TreasuryDoctorReceipt {
        &self.receipt
    }

    /// Returns the exact verified raw captures in doctor probe order.
    pub fn sealed_captures(&self) -> &[SealedProviderCaptureSetReceipt] {
        &self.sealed_captures
    }

    /// Returns the stable identity of parsed evidence and every physical capture receipt.
    pub const fn sealed_receipt_digest(&self) -> EvidenceDigest {
        self.sealed_receipt_digest
    }
}

/// Terminal state of one fully traversed configured query.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TreasuryDiscoveryCompleteness {
    /// The provider query completed and produced at least one observed numeric value.
    CompleteProducing,
    /// The provider query completed without an observed numeric value.
    ///
    /// Explicit provider-missing canonical rows may still be retained and counted.
    CompleteEmpty,
}

/// Provider-local accounting for a complete discovery traversal.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TreasuryDiscoveryAccounting {
    descriptor: TreasuryDatasetDescriptor,
    request_count: u64,
    response_count: u64,
    source_object_count: u64,
    returned_source_rows: u64,
    canonical_points: u64,
    observed_numeric_points: u64,
    explicit_missing_points: u64,
    raw_body_bytes: u64,
    terminal_response_observed: bool,
    terminal_response_represented_by_source_object: bool,
    extraction_ready: bool,
    completeness: TreasuryDiscoveryCompleteness,
    reported_total_rows: Option<u64>,
    reported_total_pages: Option<u64>,
    first_received_at: Timestamp,
    last_received_at: Timestamp,
    source_payload_digests: Box<[EvidenceDigest]>,
}

/// Checked facts used to construct [`TreasuryDiscoveryAccounting`].
pub(crate) struct TreasuryDiscoveryAccountingInput {
    pub(crate) descriptor: TreasuryDatasetDescriptor,
    pub(crate) request_count: usize,
    pub(crate) response_count: usize,
    pub(crate) source_object_count: usize,
    pub(crate) returned_source_rows: usize,
    pub(crate) canonical_points: usize,
    pub(crate) observed_numeric_points: usize,
    pub(crate) explicit_missing_points: usize,
    pub(crate) raw_body_bytes: u64,
    pub(crate) terminal_response_observed: bool,
    pub(crate) terminal_response_represented_by_source_object: bool,
    pub(crate) reported_total_rows: Option<usize>,
    pub(crate) reported_total_pages: Option<usize>,
    pub(crate) first_received_at: Timestamp,
    pub(crate) last_received_at: Timestamp,
    pub(crate) source_payload_digests: Vec<EvidenceDigest>,
}

impl TreasuryDiscoveryAccounting {
    pub(crate) fn try_new(
        input: TreasuryDiscoveryAccountingInput,
    ) -> Result<Self, TreasuryVerticalError> {
        let protocol_shape_valid = match input.descriptor.publication_mode {
            TreasuryPublicationMode::AtomicResponse => {
                input.response_count == 1
                    && input.source_object_count == 1
                    && input.terminal_response_represented_by_source_object
                    && input.reported_total_rows.is_none()
                    && input.reported_total_pages.is_none()
            }
            TreasuryPublicationMode::CompletePageChain => {
                input.source_object_count == 1
                    && input.terminal_response_represented_by_source_object
                    && input.reported_total_rows == Some(input.returned_source_rows)
                    && input.reported_total_pages == Some(input.response_count)
            }
            TreasuryPublicationMode::ResumableBackfill => {
                input.source_object_count == input.response_count
                    && input.terminal_response_represented_by_source_object
                    && input.reported_total_rows.is_none()
                    && input.reported_total_pages.is_none()
            }
        };
        if input.request_count == 0
            || input.response_count == 0
            || input.request_count != input.response_count
            || input
                .observed_numeric_points
                .checked_add(input.explicit_missing_points)
                != Some(input.canonical_points)
            || !input.terminal_response_observed
            || !protocol_shape_valid
            || input.last_received_at < input.first_received_at
            || input.source_payload_digests.len() != input.source_object_count
            || has_duplicate_digests(&input.source_payload_digests)
        {
            return Err(TreasuryVerticalError::InvalidDiscoveryAccounting);
        }
        let source_object_count = u64::try_from(input.source_object_count)
            .map_err(|_| TreasuryVerticalError::AccountingOverflow)?;
        let canonical_points = u64::try_from(input.canonical_points)
            .map_err(|_| TreasuryVerticalError::AccountingOverflow)?;
        let observed_numeric_points = u64::try_from(input.observed_numeric_points)
            .map_err(|_| TreasuryVerticalError::AccountingOverflow)?;
        let explicit_missing_points = u64::try_from(input.explicit_missing_points)
            .map_err(|_| TreasuryVerticalError::AccountingOverflow)?;
        let completeness = if observed_numeric_points == 0 {
            TreasuryDiscoveryCompleteness::CompleteEmpty
        } else {
            TreasuryDiscoveryCompleteness::CompleteProducing
        };
        let extraction_ready = matches!(
            completeness,
            TreasuryDiscoveryCompleteness::CompleteProducing
        ) && input.terminal_response_represented_by_source_object
            && source_object_count > 0
            && input.descriptor.publication_mode != TreasuryPublicationMode::ResumableBackfill;
        Ok(Self {
            descriptor: input.descriptor,
            request_count: u64::try_from(input.request_count)
                .map_err(|_| TreasuryVerticalError::AccountingOverflow)?,
            response_count: u64::try_from(input.response_count)
                .map_err(|_| TreasuryVerticalError::AccountingOverflow)?,
            source_object_count,
            returned_source_rows: u64::try_from(input.returned_source_rows)
                .map_err(|_| TreasuryVerticalError::AccountingOverflow)?,
            canonical_points,
            observed_numeric_points,
            explicit_missing_points,
            raw_body_bytes: input.raw_body_bytes,
            terminal_response_observed: input.terminal_response_observed,
            terminal_response_represented_by_source_object: input
                .terminal_response_represented_by_source_object,
            extraction_ready,
            completeness,
            reported_total_rows: input
                .reported_total_rows
                .map(u64::try_from)
                .transpose()
                .map_err(|_| TreasuryVerticalError::AccountingOverflow)?,
            reported_total_pages: input
                .reported_total_pages
                .map(u64::try_from)
                .transpose()
                .map_err(|_| TreasuryVerticalError::AccountingOverflow)?,
            first_received_at: input.first_received_at,
            last_received_at: input.last_received_at,
            source_payload_digests: input.source_payload_digests.into_boxed_slice(),
        })
    }

    /// Returns the exact configured dataset descriptor.
    pub const fn descriptor(&self) -> &TreasuryDatasetDescriptor {
        &self.descriptor
    }

    /// Returns whether this complete traversal can enter ordinary page extraction.
    ///
    /// This does not prove staging, an immutable generation, or a successful read. An empty
    /// response and an all-history chain outside its page-sealed acquisition authority both remain
    /// honest non-ready states.
    pub const fn extraction_ready(&self) -> bool {
        self.extraction_ready
    }

    /// Returns the complete/empty terminal classification.
    pub const fn completeness(&self) -> TreasuryDiscoveryCompleteness {
        self.completeness
    }

    /// Returns the exact number of discovered publishable source objects.
    pub const fn source_object_count(&self) -> u64 {
        self.source_object_count
    }

    /// Returns canonical scalar count before immutable publication.
    pub const fn canonical_points(&self) -> u64 {
        self.canonical_points
    }

    /// Returns canonical points carrying an exact observed numeric value.
    pub const fn observed_numeric_points(&self) -> u64 {
        self.observed_numeric_points
    }

    /// Returns retained canonical points carrying explicit provider missingness.
    pub const fn explicit_missing_points(&self) -> u64 {
        self.explicit_missing_points
    }

    /// Returns exact discovered provider payload identities in request order.
    pub fn source_payload_digests(&self) -> &[EvidenceDigest] {
        &self.source_payload_digests
    }
}

/// Complete discovery result plus the accounting generic source discovery otherwise discards.
#[derive(Debug)]
pub struct TreasuryDiscoveryOutput {
    batch: DiscoveryBatch,
    accounting: TreasuryDiscoveryAccounting,
}

impl TreasuryDiscoveryOutput {
    pub(crate) fn try_new(
        batch: DiscoveryBatch,
        accounting: TreasuryDiscoveryAccounting,
    ) -> Result<Self, TreasuryVerticalError> {
        if batch.request().dataset() != accounting.descriptor.provider_dataset()
            || u64::try_from(batch.objects().len())
                .map_err(|_| TreasuryVerticalError::AccountingOverflow)?
                != accounting.source_object_count
        {
            return Err(TreasuryVerticalError::InvalidDiscoveryAccounting);
        }
        Ok(Self { batch, accounting })
    }

    /// Returns the canonical bounded discovery batch.
    pub const fn batch(&self) -> &DiscoveryBatch {
        &self.batch
    }

    /// Returns terminal, row, point, byte, and raw-capture accounting.
    pub const fn accounting(&self) -> &TreasuryDiscoveryAccounting {
        &self.accounting
    }

    /// Consumes the provider-local result for source-neutral discovery.
    pub fn into_batch(self) -> DiscoveryBatch {
        self.batch
    }
}

/// Aggregate accounting for one daily response or one complete Fiscal page chain.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TreasuryExtractionAccounting {
    descriptor: TreasuryDatasetDescriptor,
    terminal_page_count: u64,
    aggregate_source_rows: u64,
    aggregate_canonical_points: u64,
    aggregate_observed_numeric_points: u64,
    aggregate_explicit_missing_points: u64,
    aggregate_raw_body_bytes: u64,
    source_object_payload_bytes: u64,
    query_digest: EvidenceDigest,
    request_set_digest: EvidenceDigest,
    source_object_payload_digest: EvidenceDigest,
    terminal_received_at: Timestamp,
    provider_published_at: Option<Timestamp>,
    terminal_for_query: bool,
}

/// Checked adapter facts used to construct [`TreasuryExtractionAccounting`].
pub(crate) struct TreasuryExtractionAccountingInput {
    pub(crate) descriptor: TreasuryDatasetDescriptor,
    pub(crate) terminal_page_count: usize,
    pub(crate) aggregate_source_rows: usize,
    pub(crate) aggregate_canonical_points: usize,
    pub(crate) aggregate_observed_numeric_points: usize,
    pub(crate) aggregate_explicit_missing_points: usize,
    pub(crate) aggregate_raw_body_bytes: usize,
    pub(crate) source_object_payload_bytes: usize,
    pub(crate) query_digest: [u8; 32],
    pub(crate) request_set_digest: [u8; 32],
    pub(crate) source_object_payload_digest: [u8; 32],
    pub(crate) terminal_received_at: Timestamp,
    pub(crate) provider_published_at: Option<Timestamp>,
    pub(crate) terminal_for_query: bool,
}

impl TreasuryExtractionAccounting {
    pub(crate) fn try_new(
        input: TreasuryExtractionAccountingInput,
    ) -> Result<Self, TreasuryVerticalError> {
        if input.terminal_page_count == 0
            || input.aggregate_raw_body_bytes == 0
            || input.source_object_payload_bytes == 0
            || input.aggregate_canonical_points == 0
            || input
                .aggregate_observed_numeric_points
                .checked_add(input.aggregate_explicit_missing_points)
                != Some(input.aggregate_canonical_points)
            || input.aggregate_source_rows == 0
            || input.aggregate_canonical_points < input.aggregate_source_rows
            || input.descriptor.query_digest != sha256(input.query_digest)
        {
            return Err(TreasuryVerticalError::InvalidDiscoveryAccounting);
        }
        Ok(Self {
            descriptor: input.descriptor,
            terminal_page_count: u64::try_from(input.terminal_page_count)
                .map_err(|_| TreasuryVerticalError::AccountingOverflow)?,
            aggregate_source_rows: u64::try_from(input.aggregate_source_rows)
                .map_err(|_| TreasuryVerticalError::AccountingOverflow)?,
            aggregate_canonical_points: u64::try_from(input.aggregate_canonical_points)
                .map_err(|_| TreasuryVerticalError::AccountingOverflow)?,
            aggregate_observed_numeric_points: u64::try_from(
                input.aggregate_observed_numeric_points,
            )
            .map_err(|_| TreasuryVerticalError::AccountingOverflow)?,
            aggregate_explicit_missing_points: u64::try_from(
                input.aggregate_explicit_missing_points,
            )
            .map_err(|_| TreasuryVerticalError::AccountingOverflow)?,
            aggregate_raw_body_bytes: u64::try_from(input.aggregate_raw_body_bytes)
                .map_err(|_| TreasuryVerticalError::AccountingOverflow)?,
            source_object_payload_bytes: u64::try_from(input.source_object_payload_bytes)
                .map_err(|_| TreasuryVerticalError::AccountingOverflow)?,
            query_digest: sha256(input.query_digest),
            request_set_digest: sha256(input.request_set_digest),
            source_object_payload_digest: sha256(input.source_object_payload_digest),
            terminal_received_at: input.terminal_received_at,
            provider_published_at: input.provider_published_at,
            terminal_for_query: input.terminal_for_query,
        })
    }

    pub(crate) fn validate_common_publication(
        &self,
        batch: &ExtractionBatch,
        capture: &ProviderCaptureMaterial,
    ) -> Result<(), TreasuryVerticalError> {
        let record_count = u64::try_from(batch.records().len())
            .map_err(|_| TreasuryVerticalError::AccountingOverflow)?;
        let receipt = capture.receipt();
        let object = batch.request().object();
        let is_chain =
            self.descriptor.publication_mode == TreasuryPublicationMode::CompletePageChain;
        let expected_terminal = if is_chain {
            market_squawk_sources::ProviderCaptureTerminalDisposition::ExhaustedWithoutNextPage
        } else {
            market_squawk_sources::ProviderCaptureTerminalDisposition::StandaloneResponse
        };
        let expected_pages = self.terminal_page_count;
        let capture_payload_evidence = if is_chain {
            crate::source::lineage::fiscal_chain_framed_evidence(
                capture.records().iter().map(|record| record.payload()),
            )
            .ok()
        } else {
            receipt
                .pages()
                .first()
                .map(|page| (page.body_digest(), page.body_bytes()))
        };
        let capture_request_digest = if is_chain {
            fiscal_capture_request_digest(receipt.pages())
        } else {
            receipt.pages().first().map(|page| page.request_identity())
        };
        if record_count != self.aggregate_canonical_points
            || self
                .aggregate_observed_numeric_points
                .checked_add(self.aggregate_explicit_missing_points)
                != Some(self.aggregate_canonical_points)
            || object.source_id() != receipt.source_id()
            || object.metadata_revision() != receipt.metadata_revision()
            || object.dataset() != self.descriptor.provider_dataset()
            || object.dataset() != receipt.dataset()
            || object.evidence().content_digest() != self.source_object_payload_digest
            || object.expected_bytes() != Some(self.source_object_payload_bytes)
            || market_squawk_sources::SourceObjectCaptureIdentity::try_from_capture(receipt).ok()
                != Some(object.capture_identity())
            || u64::try_from(receipt.pages().len()).ok() != Some(expected_pages)
            || receipt.terminal() != expected_terminal
            || receipt.request_set_identity() != self.request_set_digest
            || capture_request_digest != Some(self.request_set_digest)
            || capture_payload_evidence
                != Some((
                    self.source_object_payload_digest,
                    self.source_object_payload_bytes,
                ))
            || receipt.pages().last().map(|page| page.received_at())
                != Some(self.terminal_received_at)
            || receipt.total_body_bytes() != self.aggregate_raw_body_bytes
            || batch.records().iter().any(|record| {
                let published = record
                    .published_time()
                    .and_then(market_squawk_domain::ResearchTemporalCoordinate::exact_timestamp);
                record.source_id() != receipt.source_id()
                    || record.metadata_revision() != receipt.metadata_revision()
                    || record.dataset() != receipt.dataset()
                    || record.object_id() != object.object_id()
                    || record.object_evidence() != object.evidence()
                    || !receipt
                        .pages()
                        .iter()
                        .any(|page| record.available_at() == Some(page.received_at()))
                    || match self.provider_published_at {
                        Some(feed_published_at) => {
                            published.is_none_or(|value| value > feed_published_at)
                        }
                        None => published.is_some(),
                    }
            })
        {
            return Err(TreasuryVerticalError::InvalidExtractionHandoff);
        }
        Ok(())
    }

    /// Returns the exact provider and analytical dataset identity.
    pub const fn descriptor(&self) -> &TreasuryDatasetDescriptor {
        &self.descriptor
    }

    /// Returns one for a daily response or the complete terminal Fiscal chain page count.
    pub const fn terminal_page_count(&self) -> u64 {
        self.terminal_page_count
    }

    /// Returns aggregate provider rows across the daily response or complete Fiscal chain.
    pub const fn aggregate_source_rows(&self) -> u64 {
        self.aggregate_source_rows
    }

    /// Returns aggregate canonical observations across the complete extraction unit.
    pub const fn aggregate_canonical_points(&self) -> u64 {
        self.aggregate_canonical_points
    }

    /// Returns aggregate canonical points carrying an exact observed numeric value.
    pub const fn aggregate_observed_numeric_points(&self) -> u64 {
        self.aggregate_observed_numeric_points
    }

    /// Returns aggregate canonical points carrying explicit provider missingness.
    pub const fn aggregate_explicit_missing_points(&self) -> u64 {
        self.aggregate_explicit_missing_points
    }

    /// Returns the exact daily body digest or canonical framed Fiscal-chain digest.
    pub const fn source_object_payload_digest(&self) -> EvidenceDigest {
        self.source_object_payload_digest
    }

    /// Returns the one-request daily identity or ordered complete Fiscal request-set digest.
    pub const fn request_set_digest(&self) -> EvidenceDigest {
        self.request_set_digest
    }

    /// Returns aggregate exact provider-body bytes across the complete extraction unit.
    pub const fn aggregate_raw_body_bytes(&self) -> u64 {
        self.aggregate_raw_body_bytes
    }

    /// Returns exact daily body bytes or canonical framed Fiscal-chain payload bytes.
    pub const fn source_object_payload_bytes(&self) -> u64 {
        self.source_object_payload_bytes
    }

    /// Returns when the daily response or terminal Fiscal-chain page was received.
    pub const fn terminal_received_at(&self) -> Timestamp {
        self.terminal_received_at
    }

    /// Returns the provider publication clock when the family exposes one.
    pub const fn provider_published_at(&self) -> Option<Timestamp> {
        self.provider_published_at
    }

    /// Returns whether this response is the provider-defined terminal response for its query.
    pub const fn terminal_for_query(&self) -> bool {
        self.terminal_for_query
    }
}

fn fiscal_capture_request_digest(
    pages: &[market_squawk_sources::ProviderCapturePageReceipt],
) -> Option<EvidenceDigest> {
    if pages.is_empty() {
        return None;
    }
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/treasury-fiscal-request-set/v1\0");
    digest.update(u64::try_from(pages.len()).ok()?.to_be_bytes());
    for page in pages {
        digest.update(page.request_identity().bytes());
    }
    Some(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

pub(crate) fn treasury_rate_policy_digest(
    policy: &ProviderBudgetPolicy,
) -> Result<EvidenceDigest, TreasuryVerticalError> {
    if policy.scope().as_source_identifier().as_str() != "us-treasury"
        || policy.scope().authorization_account().is_some()
        || policy.window_count() != 1
        || policy.requests_per_window() != 1
        || policy.window_nanos() != 1_000_000_000
        || policy
            .window(0)
            .is_none_or(|window| window.semantics() != BudgetWindowSemantics::Sliding)
        || policy.max_concurrent() != 1
    {
        return Err(TreasuryVerticalError::DoctorRejected);
    }
    let wire = serde_json::to_vec(policy).map_err(|_| TreasuryVerticalError::DoctorRejected)?;
    Ok(domain_separated_digest(
        b"market-squawk/treasury-shared-rate-policy/v1\0",
        &wire,
    ))
}

/// Immutable analytical-read shape required for one Treasury dashboard dataset.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TreasuryDashboardSeriesMode {
    /// Read one latest-known point for each code-owned canonical series.
    LatestKnownAllowlist,
    /// Read the bounded published series inventory because provider-authored dimensions are open.
    PublishedSeriesInventory,
}

/// Provider-local series-selection requirement for one configured Treasury dataset.
///
/// This is not an executable query: the root reader must additionally bind a reopened manifest,
/// source ID, exact knowledge/effective cutoffs, freshness policy, limits, and result identity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TreasuryDashboardDatasetRead {
    descriptor: TreasuryDatasetDescriptor,
    series_mode: TreasuryDashboardSeriesMode,
    expected_series: Box<[SourceIdentifier]>,
}

impl TreasuryDashboardDatasetRead {
    /// Returns the exact provider and analytical dataset descriptor.
    pub const fn descriptor(&self) -> &TreasuryDatasetDescriptor {
        &self.descriptor
    }

    /// Returns how the application must select macro series from the immutable generation.
    pub const fn series_mode(&self) -> TreasuryDashboardSeriesMode {
        self.series_mode
    }

    /// Returns the code-owned canonical allowlist, empty only for provider-authored Fiscal series.
    pub fn expected_series(&self) -> &[SourceIdentifier] {
        &self.expected_series
    }
}

/// Provider-local portion of a future immutable-generation Treasury dashboard read.
///
/// The plan carries no readiness claim. Only a successful root typed read over a reopened exact
/// generation, with complete series/result accounting and currentness evidence, may produce a
/// dashboard-ready receipt for Desktop or MCP.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TreasuryDashboardReadPlan {
    surface: TreasurySurface,
    activation_intent_digest: EvidenceDigest,
    complete_selected_family_coverage: bool,
    datasets: Box<[TreasuryDashboardDatasetRead]>,
    plan_digest: EvidenceDigest,
}

impl TreasuryDashboardReadPlan {
    /// Derives provider-owned query semantics from one exact activation intent.
    pub fn try_new(activation: &TreasuryActivationIntent) -> Result<Self, TreasuryVerticalError> {
        let mut datasets = Vec::new();
        for descriptor in dashboard_descriptors(&activation.catalog)? {
            let (series_mode, expected_series) = match descriptor.family {
                TreasuryDatasetFamily::AverageInterestRatesV2 => (
                    TreasuryDashboardSeriesMode::PublishedSeriesInventory,
                    Vec::new(),
                ),
                TreasuryDatasetFamily::DailyRate(family) => {
                    let series = family
                        .dashboard_metrics()
                        .into_iter()
                        .filter(|metric| metric_available_in_period(*metric, descriptor.period))
                        .map(|metric| {
                            SourceIdentifier::try_from(metric.canonical_series())
                                .map_err(|_| TreasuryVerticalError::InvalidConfiguration)
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    if series.is_empty()
                        || series.len() > 32
                        || series
                            .iter()
                            .enumerate()
                            .any(|(index, value)| series[..index].contains(value))
                    {
                        return Err(TreasuryVerticalError::InvalidConfiguration);
                    }
                    (TreasuryDashboardSeriesMode::LatestKnownAllowlist, series)
                }
            };
            datasets.push(TreasuryDashboardDatasetRead {
                descriptor: descriptor.clone(),
                series_mode,
                expected_series: expected_series.into_boxed_slice(),
            });
        }
        if datasets.is_empty() {
            return Err(TreasuryVerticalError::InvalidConfiguration);
        }
        let complete_selected_family_coverage = match activation.catalog.surface {
            TreasurySurface::FiscalData => datasets.len() == 1,
            TreasurySurface::DailyRatesXml => {
                TreasuryDailyRateFamily::ALL.into_iter().all(|family| {
                    datasets.iter().any(|dataset| {
                        dataset.descriptor.family == TreasuryDatasetFamily::DailyRate(family)
                    })
                })
            }
        };
        let wire = serde_json::to_vec(&(
            activation.intent_digest,
            complete_selected_family_coverage,
            &datasets,
        ))
        .map_err(|_| TreasuryVerticalError::InvalidConfiguration)?;
        let plan_digest =
            domain_separated_digest(b"market-squawk/treasury-dashboard-read-plan/v1\0", &wire);
        Ok(Self {
            surface: activation.catalog.surface,
            activation_intent_digest: activation.intent_digest,
            complete_selected_family_coverage,
            datasets: datasets.into_boxed_slice(),
            plan_digest,
        })
    }

    /// Returns every exact dataset query in stable configured order.
    pub fn datasets(&self) -> &[TreasuryDashboardDatasetRead] {
        &self.datasets
    }

    /// Returns whether the read plan covers the selected complete Treasury product family set.
    pub const fn complete_selected_family_coverage(&self) -> bool {
        self.complete_selected_family_coverage
    }

    /// Returns the stable provider-side identity the root must bind into its typed query receipt.
    pub const fn plan_digest(&self) -> EvidenceDigest {
        self.plan_digest
    }
}

fn domain_separated_digest(domain: &[u8], wire: &[u8]) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update(wire);
    EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
}

const fn sha256(bytes: [u8; 32]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, bytes)
}

fn has_duplicate_digests(digests: &[EvidenceDigest]) -> bool {
    digests
        .iter()
        .enumerate()
        .any(|(index, digest)| digests[..index].contains(digest))
}

/// Treasury activation, doctor, acquisition-accounting, or extraction-handoff failure.
#[derive(Debug, Error)]
pub enum TreasuryVerticalError {
    /// The source configuration cannot produce a coherent exact dataset catalog.
    #[error("Treasury dataset configuration is invalid")]
    InvalidConfiguration,
    /// A doctor result is missing, duplicated, mismatched, or not a valid response.
    #[error("Treasury doctor evidence is invalid")]
    DoctorRejected,
    /// A complete traversal's page, row, byte, terminal, or object accounting is inconsistent.
    #[error("Treasury discovery accounting is invalid")]
    InvalidDiscoveryAccounting,
    /// Provider-local extraction and capture facts do not correspond exactly.
    #[error("Treasury extraction handoff evidence is invalid")]
    InvalidExtractionHandoff,
    /// Checked accounting exceeded the provider-local receipt representation.
    #[error("Treasury accounting overflowed its bounded representation")]
    AccountingOverflow,
    /// A strict provider response violated its typed protocol.
    #[error(transparent)]
    Protocol(#[from] TreasuryProtocolError),
}
