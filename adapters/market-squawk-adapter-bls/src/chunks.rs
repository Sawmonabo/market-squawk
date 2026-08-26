use std::collections::BTreeSet;

use thiserror::Error;

const MAX_PLAN_SERIES: usize = 1_000;
const MIN_YEAR: u16 = 1900;
const MAX_YEAR: u16 = 9999;

/// An official BLS Public Data API access tier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BlsAccessTier {
    /// Unregistered public v1 access.
    PublicV1,
    /// Registered v2 access using a user-supplied key.
    RegisteredV2,
}

/// Exact documented and conservatively enforced request limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlsRequestLimits {
    series_per_query: usize,
    documented_years_per_query: u16,
    enforced_years_per_query: u16,
    documented_daily_queries: u16,
    enforced_daily_queries: u16,
    enforced_requests_per_second: u16,
}

impl BlsRequestLimits {
    /// Returns the maximum series identifiers in one request.
    pub const fn series_per_query(self) -> usize {
        self.series_per_query
    }

    /// Returns the years per request stated by the provider tier table.
    pub const fn documented_years_per_query(self) -> u16 {
        self.documented_years_per_query
    }

    /// Returns the conservative years per request actually enforced.
    pub const fn enforced_years_per_query(self) -> u16 {
        self.enforced_years_per_query
    }

    /// Returns the provider-published daily request limit.
    pub const fn documented_daily_queries(self) -> u16 {
        self.documented_daily_queries
    }

    /// Returns the conservative daily attempt ceiling actually enforced by Market Squawk.
    ///
    /// Every attempted provider request consumes this budget, regardless of HTTP or semantic
    /// success. The owning provider-rate authority persists the window across restart.
    pub const fn daily_queries(self) -> u16 {
        self.enforced_daily_queries
    }

    /// Returns the conservative per-second attempt ceiling enforced for both BLS tiers.
    pub const fn enforced_requests_per_second(self) -> u16 {
        self.enforced_requests_per_second
    }

    /// Returns the exact daily discovery allowance after reserving one request for doctor.
    pub const fn discovery_queries_after_doctor(self) -> u16 {
        self.enforced_daily_queries - 1
    }
}

/// One deterministic BLS request chunk.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlsRequestChunk {
    series: Vec<String>,
    start_year: u16,
    end_year: u16,
}

impl BlsRequestChunk {
    /// Returns the ordered exact series identifiers.
    pub fn series(&self) -> &[String] {
        &self.series
    }

    /// Returns the inclusive start year.
    pub const fn start_year(&self) -> u16 {
        self.start_year
    }

    /// Returns the inclusive end year.
    pub const fn end_year(&self) -> u16 {
        self.end_year
    }

    /// Returns the inclusive year count.
    pub const fn year_count(&self) -> u16 {
        self.end_year - self.start_year + 1
    }
}

/// A bounded deterministic BLS request plan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlsRequestPlan {
    tier: BlsAccessTier,
    limits: BlsRequestLimits,
    chunks: Vec<BlsRequestChunk>,
}

impl BlsRequestPlan {
    /// Splits an exact series/year request into stable provider-compliant chunks.
    pub fn try_new(
        tier: BlsAccessTier,
        series: Vec<String>,
        start_year: u16,
        end_year: u16,
    ) -> Result<Self, BlsChunkError> {
        let limits = limits_for(tier);
        validate_series(&series)?;
        if !(MIN_YEAR..=MAX_YEAR).contains(&start_year)
            || !(MIN_YEAR..=MAX_YEAR).contains(&end_year)
            || start_year > end_year
        {
            return Err(BlsChunkError::InvalidYearRange);
        }

        let mut year_windows = Vec::new();
        let mut window_start = start_year;
        loop {
            let window_end = window_start
                .saturating_add(limits.enforced_years_per_query - 1)
                .min(end_year);
            year_windows.push((window_start, window_end));
            if window_end == end_year {
                break;
            }
            window_start = window_end
                .checked_add(1)
                .ok_or(BlsChunkError::InvalidYearRange)?;
        }

        let series_chunk_count = series.len().div_ceil(limits.series_per_query);
        let total_chunks = series_chunk_count
            .checked_mul(year_windows.len())
            .ok_or(BlsChunkError::PlanTooLarge)?;
        if total_chunks > usize::from(limits.discovery_queries_after_doctor()) {
            return Err(BlsChunkError::PlanTooLarge);
        }
        let mut chunks = Vec::with_capacity(total_chunks);
        for (year_start, year_end) in year_windows {
            for series_chunk in series.chunks(limits.series_per_query) {
                chunks.push(BlsRequestChunk {
                    series: series_chunk.to_vec(),
                    start_year: year_start,
                    end_year: year_end,
                });
            }
        }
        Ok(Self {
            tier,
            limits,
            chunks,
        })
    }

    /// Returns the selected API tier.
    pub const fn tier(&self) -> BlsAccessTier {
        self.tier
    }

    /// Returns the exact limits used to construct the plan.
    pub const fn limits(&self) -> BlsRequestLimits {
        self.limits
    }

    /// Returns deterministic request chunks ordered by year window then series group.
    pub fn chunks(&self) -> &[BlsRequestChunk] {
        &self.chunks
    }
}

/// A deterministic request-plan validation failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum BlsChunkError {
    /// The requested series set is empty or exceeds the plan bound.
    #[error("BLS series set is empty or too large")]
    InvalidSeriesCount,
    /// A series identifier is malformed or duplicated.
    #[error("BLS series identifier is malformed or duplicated")]
    InvalidSeries,
    /// The inclusive year range is invalid.
    #[error("BLS year range is invalid")]
    InvalidYearRange,
    /// The expanded plan plus its mandatory doctor exceeds the tier's daily attempt budget.
    #[error("BLS request plan exceeds its chunk budget")]
    PlanTooLarge,
}

pub(crate) const fn limits_for(tier: BlsAccessTier) -> BlsRequestLimits {
    match tier {
        BlsAccessTier::PublicV1 => BlsRequestLimits {
            series_per_query: 25,
            documented_years_per_query: 10,
            enforced_years_per_query: 10,
            documented_daily_queries: 25,
            enforced_daily_queries: 25,
            enforced_requests_per_second: 1,
        },
        BlsAccessTier::RegisteredV2 => BlsRequestLimits {
            series_per_query: 50,
            documented_years_per_query: 20,
            // The BLS FAQ currently conflicts between 20 and 10 years. The maintained provider
            // contract therefore preserves the published table value while failing closed to ten
            // inclusive years until the provider resolves that conflict.
            enforced_years_per_query: 10,
            documented_daily_queries: 500,
            enforced_daily_queries: 400,
            enforced_requests_per_second: 1,
        },
    }
}

fn validate_series(series: &[String]) -> Result<(), BlsChunkError> {
    if series.is_empty() || series.len() > MAX_PLAN_SERIES {
        return Err(BlsChunkError::InvalidSeriesCount);
    }
    let mut unique = BTreeSet::new();
    for identifier in series {
        if identifier.is_empty()
            || identifier.len() > 50
            || !identifier.bytes().all(is_valid_identifier_byte)
            || !unique.insert(identifier.as_str())
        {
            return Err(BlsChunkError::InvalidSeries);
        }
    }
    Ok(())
}

pub(crate) const fn is_valid_identifier_byte(byte: u8) -> bool {
    byte.is_ascii_uppercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-' | b'#')
}
