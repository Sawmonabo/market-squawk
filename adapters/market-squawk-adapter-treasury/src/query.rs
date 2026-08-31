use std::num::NonZeroU16;

use market_squawk_domain::{CalendarDate, SourceIdentifier};
use sha2::{Digest, Sha256};
use url::Url;

use crate::{TreasuryProtocolError, TreasurySourceError};

const FISCAL_DATA_BASE: &str = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service";
const AVERAGE_RATES_ENDPOINT: &str = "/v2/accounting/od/avg_interest_rates";
const AVERAGE_RATES_SOURCE: &str =
    "https://fiscaldata.treasury.gov/datasets/average-interest-rates-treasury-securities/";
const AVERAGE_RATES_FIELDS: &[&str] = &[
    "record_date",
    "security_type_desc",
    "security_desc",
    "avg_interest_rate_amt",
    "src_line_nbr",
    "record_fiscal_year",
    "record_fiscal_quarter",
    "record_calendar_year",
    "record_calendar_quarter",
    "record_calendar_month",
    "record_calendar_day",
];
const AVERAGE_RATES_PRIMARY_KEY: &[&str] = &[
    "record_date",
    "security_type_desc",
    "security_desc",
    "src_line_nbr",
];
const AVERAGE_RATES_SORT: &str = "record_date,src_line_nbr";

/// A closed set of official Fiscal Data datasets normalized by this adapter.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TreasuryDatasetProfile {
    /// Average interest rates for Treasury securities from Fiscal Data v2.
    AverageInterestRatesV2,
}

impl TreasuryDatasetProfile {
    const fn endpoint(self) -> &'static str {
        match self {
            Self::AverageInterestRatesV2 => AVERAGE_RATES_ENDPOINT,
        }
    }

    const fn source_identity(self) -> &'static str {
        match self {
            Self::AverageInterestRatesV2 => AVERAGE_RATES_SOURCE,
        }
    }

    const fn fields(self) -> &'static [&'static str] {
        match self {
            Self::AverageInterestRatesV2 => AVERAGE_RATES_FIELDS,
        }
    }

    const fn primary_key(self) -> &'static [&'static str] {
        match self {
            Self::AverageInterestRatesV2 => AVERAGE_RATES_PRIMARY_KEY,
        }
    }
}

/// Immutable, canonically identified Fiscal Data query semantics excluding only page number.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreasuryFiscalQuery {
    profile: TreasuryDatasetProfile,
    fields: &'static [&'static str],
    filter: String,
    sort: &'static str,
    page_size: NonZeroU16,
    first_record_date: CalendarDate,
    last_record_date: CalendarDate,
    query_digest: [u8; 32],
}

impl TreasuryFiscalQuery {
    /// Creates the supported average-interest-rates query over an inclusive civil-date range.
    ///
    /// # Errors
    ///
    /// Rejects a reversed range or a page size above the adapter's bounded parser capacity.
    pub fn average_interest_rates_v2(
        first_record_date: CalendarDate,
        last_record_date: CalendarDate,
        page_size: NonZeroU16,
    ) -> Result<Self, TreasuryProtocolError> {
        if first_record_date > last_record_date || page_size.get() > 10_000 {
            return Err(TreasuryProtocolError::InvalidQuery);
        }
        let profile = TreasuryDatasetProfile::AverageInterestRatesV2;
        let filter =
            format!("record_date:gte:{first_record_date},record_date:lte:{last_record_date}");
        let fields = profile.fields();
        let sort = AVERAGE_RATES_SORT;
        let query_digest = query_digest(profile, fields, &filter, sort, page_size);
        Ok(Self {
            profile,
            fields,
            filter,
            sort,
            page_size,
            first_record_date,
            last_record_date,
            query_digest,
        })
    }

    /// Binds a one-based page number into the exact outbound request identity.
    ///
    /// # Errors
    ///
    /// Rejects page zero or an invalid official endpoint URL.
    pub fn page(&self, page_number: usize) -> Result<TreasuryPageRequest, TreasuryProtocolError> {
        if page_number == 0 {
            return Err(TreasuryProtocolError::UnexpectedPage {
                expected: 1,
                actual: 0,
            });
        }
        let endpoint = format!("{FISCAL_DATA_BASE}{}", self.profile.endpoint());
        let mut url = Url::parse(&endpoint).map_err(|_| TreasuryProtocolError::InvalidQuery)?;
        url.query_pairs_mut()
            .append_pair("fields", &self.fields.join(","))
            .append_pair("filter", &self.filter)
            .append_pair("sort", self.sort)
            .append_pair("page[number]", &page_number.to_string())
            .append_pair("page[size]", &self.page_size.get().to_string());
        let request_digest = page_request_digest(self.query_digest, page_number);
        Ok(TreasuryPageRequest {
            profile: self.profile,
            url: url.into(),
            page_number,
            page_size: self.page_size,
            first_record_date: self.first_record_date,
            last_record_date: self.last_record_date,
            query_digest: self.query_digest,
            request_digest,
        })
    }

    /// Returns the dataset profile.
    pub const fn profile(&self) -> TreasuryDatasetProfile {
        self.profile
    }

    /// Returns the response-source identity bound into this query.
    pub const fn source_identity(&self) -> &'static str {
        self.profile.source_identity()
    }

    /// Returns the canonical query-family digest.
    pub const fn query_digest(&self) -> [u8; 32] {
        self.query_digest
    }

    /// Returns the exact provider selector accepted by the configured source.
    ///
    /// # Errors
    ///
    /// Returns [`TreasurySourceError::InvalidProtocol`] if the canonical selector cannot be
    /// represented by the shared source-identity contract.
    pub fn dataset(&self) -> Result<SourceIdentifier, TreasurySourceError> {
        crate::source::fiscal_provider_dataset(self)
    }

    /// Returns the storage-safe analytical identity derived from this exact provider selector.
    ///
    /// # Errors
    ///
    /// Returns [`TreasurySourceError::InvalidProtocol`] if the canonical analytical identity
    /// cannot be represented by the shared dataset contract.
    pub fn analytical_dataset(&self) -> Result<SourceIdentifier, TreasurySourceError> {
        crate::source::fiscal_analytical_dataset(self)
    }

    /// Returns the inclusive first record date.
    pub const fn first_record_date(&self) -> CalendarDate {
        self.first_record_date
    }

    /// Returns the inclusive final record date.
    pub const fn last_record_date(&self) -> CalendarDate {
        self.last_record_date
    }

    /// Returns the exact bounded page size.
    pub const fn page_size(&self) -> NonZeroU16 {
        self.page_size
    }
}

/// One exact page request, including its canonical query family and page identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreasuryPageRequest {
    profile: TreasuryDatasetProfile,
    url: String,
    page_number: usize,
    page_size: NonZeroU16,
    first_record_date: CalendarDate,
    last_record_date: CalendarDate,
    query_digest: [u8; 32],
    request_digest: [u8; 32],
}

impl TreasuryPageRequest {
    /// Returns the allowlist-ready official HTTPS URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Returns the bound dataset profile.
    pub const fn profile(&self) -> TreasuryDatasetProfile {
        self.profile
    }

    /// Returns the one-based page number.
    pub const fn page_number(&self) -> usize {
        self.page_number
    }

    /// Returns the exact requested page size.
    pub const fn page_size(&self) -> NonZeroU16 {
        self.page_size
    }

    /// Returns the canonical query-family digest.
    pub const fn query_digest(&self) -> [u8; 32] {
        self.query_digest
    }

    /// Returns the canonical full-request digest, including page number.
    pub const fn request_digest(&self) -> [u8; 32] {
        self.request_digest
    }

    /// Returns the exact canonical provider page-token fragment sent by this request.
    pub fn page_token(&self) -> String {
        canonical_page_token(self.page_number, self.page_size)
    }

    /// Returns the response-source identity bound into the profile.
    pub const fn source_identity(&self) -> &'static str {
        self.profile.source_identity()
    }

    pub(crate) const fn fields(&self) -> &'static [&'static str] {
        self.profile.fields()
    }

    pub(crate) const fn primary_key(&self) -> &'static [&'static str] {
        self.profile.primary_key()
    }

    pub(crate) fn contains_record_date(&self, date: CalendarDate) -> bool {
        date >= self.first_record_date && date <= self.last_record_date
    }
}

pub(crate) fn canonical_page_token(page_number: usize, page_size: NonZeroU16) -> String {
    format!(
        "&page%5Bnumber%5D={page_number}&page%5Bsize%5D={}",
        page_size.get()
    )
}

fn query_digest(
    profile: TreasuryDatasetProfile,
    fields: &[&str],
    filter: &str,
    sort: &str,
    page_size: NonZeroU16,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    update_component(&mut digest, "treasury-fiscal-query-v1");
    update_component(&mut digest, profile.endpoint());
    update_component(&mut digest, profile.source_identity());
    for field in fields {
        update_component(&mut digest, field);
    }
    update_component(&mut digest, filter);
    update_component(&mut digest, sort);
    digest.update(page_size.get().to_be_bytes());
    digest.finalize().into()
}

fn page_request_digest(query_digest: [u8; 32], page_number: usize) -> [u8; 32] {
    let mut digest = Sha256::new();
    update_component(&mut digest, "treasury-fiscal-page-v1");
    digest.update(query_digest);
    digest.update((page_number as u128).to_be_bytes());
    digest.finalize().into()
}

pub(crate) fn update_component(digest: &mut Sha256, value: &str) {
    digest.update((value.len() as u128).to_be_bytes());
    digest.update(value.as_bytes());
}
