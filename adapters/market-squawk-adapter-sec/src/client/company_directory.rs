//! Official current SEC directory candidates; these never establish security attribution.

use market_squawk_domain::SourceIdentifier;
use market_squawk_sources::ExtractionAuthority;
use tokio_util::sync::CancellationToken;

use super::{SecClientError, SecEdgarSource, SecObjectLocator, normalized_cik};
use crate::SecParserLimits;
use crate::json::{RetainedJsonBudget, parse_bounded_json_with_allocation_authority};

/// One exact ticker row from the official SEC company directory, awaiting corroboration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecCompanyDirectoryCandidate {
    cik: SourceIdentifier,
    exchange: String,
}

impl SecCompanyDirectoryCandidate {
    /// Returns the source-native company identifier, not a canonical security identifier.
    pub const fn cik(&self) -> &SourceIdentifier {
        &self.cik
    }

    /// Returns the directory's exchange label without inferring a listing venue.
    pub fn exchange(&self) -> &str {
        &self.exchange
    }
}

impl SecEdgarSource {
    /// Retrieves exact-symbol candidates through this source's registered transport and raw store.
    /// SEC does not guarantee directory accuracy or scope. Only subsequent submissions and
    /// official listing corroboration can establish an issuer/security relationship.
    pub async fn fetch_company_directory_candidates(
        &self,
        authority: &ExtractionAuthority,
        listed_symbol: &str,
        cancellation: CancellationToken,
    ) -> Result<Vec<SecCompanyDirectoryCandidate>, SecClientError> {
        if listed_symbol.is_empty()
            || listed_symbol.len() > self.parser_limits.string_bytes()
            || listed_symbol.chars().any(char::is_control)
        {
            return Err(SecClientError::InvalidLocator);
        }
        let retained = RetainedJsonBudget::new(self.parser_limits);
        let raw = self
            .retrieve_with_allocation_authority(
                authority,
                &SecObjectLocator::company_directory()?,
                &cancellation,
                Some(retained.clone()),
            )
            .await?;
        let bytes = raw.bytes().clone();
        let limits = self.parser_limits;
        let symbol = listed_symbol.to_owned();
        let candidates = self
            .run_validation_blocking(&cancellation, move |worker_cancellation| {
                parse_candidates(&bytes, &symbol, limits, worker_cancellation, retained)
            })
            .await?;
        self.validate_authority(authority)?;
        Ok(candidates)
    }
}

fn parse_candidates(
    bytes: &[u8],
    symbol: &str,
    limits: SecParserLimits,
    cancellation: &CancellationToken,
    retained: RetainedJsonBudget,
) -> Result<Vec<SecCompanyDirectoryCandidate>, SecClientError> {
    let directory = parse_bounded_json_with_allocation_authority(
        bytes,
        limits,
        cancellation,
        retained.clone(),
    )?;
    let object = directory
        .as_object()
        .ok_or(SecClientError::InvalidCaptureMaterial)?;
    let fields = object
        .get("fields")
        .and_then(serde_json::Value::as_array)
        .ok_or(SecClientError::InvalidCaptureMaterial)?;
    if fields.len() != 4
        || fields
            .iter()
            .zip(["cik", "name", "ticker", "exchange"])
            .any(|(field, expected)| field.as_str() != Some(expected))
    {
        return Err(SecClientError::InvalidCaptureMaterial);
    }
    let rows = object
        .get("data")
        .and_then(serde_json::Value::as_array)
        .ok_or(SecClientError::InvalidCaptureMaterial)?;
    if rows.len() > limits.records() {
        return Err(SecClientError::ResponseTooLarge);
    }
    let mut candidates = Vec::new();
    for row in rows {
        if cancellation.is_cancelled() {
            return Err(SecClientError::Cancelled);
        }
        let row = row
            .as_array()
            .filter(|row| row.len() == 4)
            .ok_or(SecClientError::InvalidCaptureMaterial)?;
        let cik = row[0]
            .as_u64()
            .filter(|cik| *cik > 0 && *cik <= 9_999_999_999)
            .ok_or(SecClientError::InvalidCaptureMaterial)?;
        let name = row[1]
            .as_str()
            .ok_or(SecClientError::InvalidCaptureMaterial)?;
        let ticker = row[2]
            .as_str()
            .ok_or(SecClientError::InvalidCaptureMaterial)?;
        let exchange = match &row[3] {
            serde_json::Value::Null => None,
            serde_json::Value::String(exchange) => Some(exchange.as_str()),
            _ => return Err(SecClientError::InvalidCaptureMaterial),
        };
        if name.is_empty()
            || ticker.is_empty()
            || ticker.chars().any(char::is_control)
            || exchange.is_some_and(|exchange| {
                exchange.is_empty() || exchange.chars().any(char::is_control)
            })
        {
            return Err(SecClientError::InvalidCaptureMaterial);
        }
        if ticker != symbol {
            continue;
        }
        let Some(exchange) = exchange else {
            continue;
        };
        let cik = normalized_cik(&cik.to_string())?;
        if candidates
            .iter()
            .any(|candidate: &SecCompanyDirectoryCandidate| {
                candidate.cik().as_str() == cik && candidate.exchange() == exchange
            })
        {
            continue;
        }
        crate::json::try_reserve_exact_bounded(&mut candidates, 1, &retained)?;
        candidates.push(SecCompanyDirectoryCandidate {
            cik: crate::json::source_identifier_bounded(&cik, &retained)?,
            exchange: crate::json::owned_string_bounded(exchange, &retained)?,
        });
    }
    Ok(candidates)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_directory_discovery_preserves_conflicts_and_rejects_invalid_identity()
    -> Result<(), SecClientError> {
        let limits = SecParserLimits::production_defaults();
        let cancellation = CancellationToken::new();
        assert_eq!(
            SecObjectLocator::company_directory()?.url(),
            "https://www.sec.gov/files/company_tickers_exchange.json",
        );
        let candidates = parse_candidates(
            br#"{"fields":["cik","name","ticker","exchange"],"data":[[320193,"Apple Inc.","AAPL","Nasdaq"],[320193,"Apple duplicate","AAPL","Nasdaq"],[789019,"MICROSOFT CORP","MSFT","Nasdaq"],[123,"Other issuer","AAPL","Nasdaq"],[456,"Unlisted","AAPL",null]]}"#,
            "AAPL", limits, &cancellation, RetainedJsonBudget::new(limits),
        )?;
        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0].cik().as_str(), "0000320193");
        assert_eq!(candidates[1].cik().as_str(), "0000000123");
        assert_eq!(candidates[0].exchange(), "Nasdaq");
        assert!(parse_candidates(
            br#"{"fields":["cik","name","ticker","exchange"],"data":[[320193.5,"Apple Inc.","AAPL","Nasdaq"]]}"#,
            "AAPL", limits, &cancellation, RetainedJsonBudget::new(limits),
        ).is_err());
        assert!(parse_candidates(
            br#"{"fields":["ticker","name","cik","exchange"],"data":[[320193,"Apple Inc.","AAPL","Nasdaq"]]}"#,
            "AAPL", limits, &cancellation, RetainedJsonBudget::new(limits),
        ).is_err());
        Ok(())
    }
}
