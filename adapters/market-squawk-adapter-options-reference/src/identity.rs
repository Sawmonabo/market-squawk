use std::fmt;
use std::num::NonZeroU32;

use market_squawk_domain::{CalendarDate, OccOptionIdentity, OptionKind, SourceIdentifier};
use serde::Serialize;
use thiserror::Error;

/// Exact option strike encoded as integer thousandths by OSI.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct OptionStrike(u64);

impl OptionStrike {
    /// Constructs an exact OSI strike from the eight-digit integer-thousandths field.
    pub const fn from_thousandths(value: u64) -> Self {
        Self(value)
    }

    /// Returns the exact integer-thousandths representation.
    pub const fn thousandths(self) -> u64 {
        self.0
    }

    /// Returns the canonical decimal scale for this value.
    pub const fn scale(self) -> u8 {
        3
    }
}

impl fmt::Display for OptionStrike {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}.{:03}", self.0 / 1_000, self.0 % 1_000)
    }
}

/// Evidence establishing how the full expiration year was obtained.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum ExpirationResolution {
    /// OSI supplied only its two-digit year; no century has been inferred.
    OsiTwoDigitOnly,
    /// A separate provider field supplied the full calendar date and matched the OSI fields.
    ProviderReported {
        /// Exact provider field or retained-document coordinate establishing the date.
        evidence: SourceIdentifier,
    },
}

/// Expiration components retained without silently assigning a century to the OSI year.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OptionExpiration {
    year_two_digits: u8,
    month: u8,
    day: u8,
    calendar_date: Option<CalendarDate>,
    resolution: ExpirationResolution,
}

impl OptionExpiration {
    fn from_osi(identity: &OccOptionIdentity) -> Self {
        Self {
            year_two_digits: identity.expiration_yy(),
            month: identity.expiration_month(),
            day: identity.expiration_day(),
            calendar_date: None,
            resolution: ExpirationResolution::OsiTwoDigitOnly,
        }
    }

    /// Returns the exact two-digit year encoded by OSI.
    pub const fn year_two_digits(&self) -> u8 {
        self.year_two_digits
    }

    /// Returns the expiration month.
    pub const fn month(&self) -> u8 {
        self.month
    }

    /// Returns the expiration day.
    pub const fn day(&self) -> u8 {
        self.day
    }

    /// Returns a full calendar date only when separately evidenced by the provider.
    pub const fn calendar_date(&self) -> Option<CalendarDate> {
        self.calendar_date
    }

    /// Returns the evidence state for the full expiration date.
    pub const fn resolution(&self) -> &ExpirationResolution {
        &self.resolution
    }
}

/// Source evidence for an option contract multiplier.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum MultiplierEvidence {
    /// The admitted reference surface did not report a multiplier.
    NotReported,
    /// A provider field or complete operative document reported the multiplier.
    ProviderReported {
        /// Number of underlying units represented by one contract.
        multiplier: NonZeroU32,
        /// Exact provider field or document coordinate establishing the value.
        evidence: SourceIdentifier,
    },
}

impl MultiplierEvidence {
    /// Returns the multiplier only when provider evidence exists.
    pub const fn multiplier(&self) -> Option<NonZeroU32> {
        match self {
            Self::NotReported => None,
            Self::ProviderReported { multiplier, .. } => Some(*multiplier),
        }
    }
}

/// Parsed OCC/OSI contract identity and the exact terms established by source evidence.
///
/// This remains provider reference evidence. It does not establish listing state, deliverables,
/// exercise style, settlement style, economic underlying, or a canonical instrument identity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OptionContractIdentity {
    osi: OccOptionIdentity,
    expiration: OptionExpiration,
    strike: OptionStrike,
    kind: OptionKind,
    multiplier: MultiplierEvidence,
}

impl OptionContractIdentity {
    /// Parses one fixed-width 21-character OCC/OSI symbol without inferring a century or standard
    /// multiplier.
    ///
    /// # Errors
    ///
    /// Rejects invalid OSI length, root, date fields, call/put code, or strike digits.
    pub fn try_from_osi(value: &str) -> Result<Self, OptionIdentityError> {
        let osi = OccOptionIdentity::try_from(value)
            .map_err(|_| OptionIdentityError::InvalidOsiIdentity)?;
        Ok(Self {
            expiration: OptionExpiration::from_osi(&osi),
            strike: OptionStrike::from_thousandths(osi.strike_thousandths()),
            kind: osi.kind(),
            multiplier: MultiplierEvidence::NotReported,
            osi,
        })
    }

    /// Returns the exact source-preserved OSI identifier.
    pub const fn osi(&self) -> &OccOptionIdentity {
        &self.osi
    }

    /// Returns the OSI root without fixed-width trailing padding.
    pub fn root(&self) -> &str {
        self.osi.root()
    }

    /// Returns expiration components and any separately evidenced full date.
    pub const fn expiration(&self) -> &OptionExpiration {
        &self.expiration
    }

    /// Returns the exact strike.
    pub const fn strike(&self) -> OptionStrike {
        self.strike
    }

    /// Returns whether this is a call or put.
    pub const fn kind(&self) -> OptionKind {
        self.kind
    }

    /// Returns provider multiplier evidence; OSI alone always yields `NotReported`.
    pub const fn multiplier(&self) -> &MultiplierEvidence {
        &self.multiplier
    }
}

/// A malformed or contradictory option contract identity.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum OptionIdentityError {
    /// The 21-character OCC/OSI grammar was invalid.
    #[error("invalid OCC/OSI option identity")]
    InvalidOsiIdentity,
}
