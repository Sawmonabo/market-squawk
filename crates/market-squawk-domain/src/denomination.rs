//! Settlement denomination independent of provider ticker aliases.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{Currency, InstrumentId};

/// A currency code or stable non-currency instrument identity used as a denomination.
///
/// [`Currency`] preserves a normalized three-letter accounting code; authoritative ISO registry
/// assignment remains source/reference-data evidence. Stablecoins, tokens, and other non-currency
/// assets use [`Self::Asset`] so a provider ticker can never masquerade as an asset identity.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum Denomination {
    /// Currency-denominated value.
    Currency(Currency),
    /// Non-currency settlement asset identified by stable internal identity.
    Asset(InstrumentId),
}

impl fmt::Display for Denomination {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Currency(currency) => currency.fmt(formatter),
            Self::Asset(instrument_id) => instrument_id.fmt(formatter),
        }
    }
}
