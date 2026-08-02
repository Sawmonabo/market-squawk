use std::collections::BTreeSet;

use market_squawk_domain::CalendarDate;

use crate::TreasuryProtocolError;

use super::{TreasuryDailyRateFamily, TreasuryDailyRatePage, TreasuryDailyRateQuery};

const PROVIDER_PAGE_ROWS: usize = 300;

/// Bounded cross-page integrity authority for one Treasury all-history query.
#[derive(Clone, Debug)]
pub struct TreasuryDailyRatePaginationTracker {
    family: TreasuryDailyRateFamily,
    query_digest: [u8; 32],
    max_pages: usize,
    max_records: usize,
    expected_page: usize,
    accepted_records: usize,
    accepted_payloads: BTreeSet<[u8; 32]>,
    accepted_rows: BTreeSet<[u8; 32]>,
    last_record_date: Option<CalendarDate>,
    previous_page_rows: Option<usize>,
    terminal: bool,
}

impl TreasuryDailyRatePaginationTracker {
    /// Creates a zero-based tracker with explicit whole-query bounds.
    ///
    /// # Errors
    ///
    /// Rejects non-history queries and zero resource limits.
    pub fn try_new(
        query: &TreasuryDailyRateQuery,
        max_pages: usize,
        max_records: usize,
    ) -> Result<Self, TreasuryProtocolError> {
        if !query.is_all_history() || max_pages == 0 || max_records == 0 {
            return Err(TreasuryProtocolError::InvalidLimits);
        }
        Ok(Self {
            family: query.family(),
            query_digest: query.query_digest(),
            max_pages,
            max_records,
            expected_page: 0,
            accepted_records: 0,
            accepted_payloads: BTreeSet::new(),
            accepted_rows: BTreeSet::new(),
            last_record_date: None,
            previous_page_rows: None,
            terminal: false,
        })
    }

    /// Accepts exactly the next page and returns `true` only for the empty terminal page.
    ///
    /// Treasury documents 300-row, zero-based pages followed by a feed with no entries. This
    /// tracker rejects repeated payloads or rows, out-of-order dates, gaps in page progression,
    /// premature partial pages, and resource overruns across the whole query.
    ///
    /// # Errors
    ///
    /// Returns a typed protocol error when the page is not the next coherent page.
    pub fn accept(&mut self, page: &TreasuryDailyRatePage) -> Result<bool, TreasuryProtocolError> {
        if self.terminal {
            return Err(TreasuryProtocolError::PageDrift);
        }
        if page.query_digest() != self.query_digest {
            return Err(TreasuryProtocolError::QueryBindingMismatch);
        }
        if page.page_number() != self.expected_page {
            return Err(TreasuryProtocolError::UnexpectedPage {
                expected: self.expected_page,
                actual: page.page_number(),
            });
        }
        if self.expected_page >= self.max_pages {
            return Err(TreasuryProtocolError::PaginationLimitExceeded);
        }
        if !self
            .accepted_payloads
            .insert(page.response_payload_digest())
        {
            return Err(TreasuryProtocolError::PageDrift);
        }
        if page.is_terminal() {
            if self.accepted_records == 0 {
                return Err(TreasuryProtocolError::PageDrift);
            }
            self.terminal = true;
            return Ok(true);
        }

        let page_rows = page.observations().len();
        if page_rows > PROVIDER_PAGE_ROWS
            || self
                .previous_page_rows
                .is_some_and(|previous| previous != PROVIDER_PAGE_ROWS)
        {
            return Err(TreasuryProtocolError::PageDrift);
        }
        self.accepted_records = self
            .accepted_records
            .checked_add(page_rows)
            .filter(|records| *records <= self.max_records)
            .ok_or(TreasuryProtocolError::PaginationLimitExceeded)?;
        for observation in page.observations() {
            let out_of_order = self.last_record_date.is_some_and(|last| {
                last > observation.record_date()
                    || (last == observation.record_date()
                        && self.family != TreasuryDailyRateFamily::LongTermRates)
            });
            if out_of_order {
                return Err(TreasuryProtocolError::PageDrift);
            }
            if !self.accepted_rows.insert(observation.row_identity()) {
                return Err(TreasuryProtocolError::DuplicateRecordIdentity);
            }
            self.last_record_date = Some(observation.record_date());
        }
        self.previous_page_rows = Some(page_rows);
        self.expected_page = self
            .expected_page
            .checked_add(1)
            .ok_or(TreasuryProtocolError::PaginationLimitExceeded)?;
        Ok(false)
    }
}
