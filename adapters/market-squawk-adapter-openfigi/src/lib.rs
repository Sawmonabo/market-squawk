//! Bounded OpenFIGI V3 mapping for current Nasdaq listing identities.
//!
//! The adapter submits source-qualified ticker/MIC pairs and returns only checksum-valid
//! exchange, composite, and share-class FIGI candidates. Provider descriptive metadata is kept
//! solely in the exact bounded raw response; it is not promoted into canonical instrument data.
//! This crate supplies no price, quantity, currency, trading-status, market-depth, or execution
//! evidence.

mod client;
mod credentials;
mod error;
mod model;
mod parser;

pub use client::OpenFigiClient;
pub use credentials::OpenFigiApiKey;
pub use error::{
    OpenFigiClientError, OpenFigiCredentialError, OpenFigiModelError, OpenFigiParseError,
    OpenFigiRateLimitError, OpenFigiRequestError,
};
pub use model::{
    OPENFIGI_API_KEY_MAX_JOBS, OPENFIGI_API_KEY_REQUEST_WINDOW_NANOS,
    OPENFIGI_API_KEY_REQUESTS_PER_WINDOW, OPENFIGI_PUBLIC_MAX_JOBS,
    OPENFIGI_PUBLIC_REQUEST_WINDOW_NANOS, OPENFIGI_PUBLIC_REQUESTS_PER_WINDOW,
    OPENFIGI_V3_MAPPING_URL, OPENFIGI_V3_PROVIDER, OpenFigiAccess, OpenFigiConflictReason,
    OpenFigiIdentityCandidate, OpenFigiListingMappingJob, OpenFigiMappingOutcome,
    OpenFigiMappingReceipt, OpenFigiMappingResult, OpenFigiRateLimitEvidence, OpenFigiRawPayload,
};
pub use parser::{
    MAX_OPENFIGI_CANDIDATES_PER_JOB, MAX_OPENFIGI_REQUEST_BYTES, MAX_OPENFIGI_RESPONSE_BYTES,
    encode_mapping_request, parse_mapping_response,
};

#[cfg(test)]
mod tests {
    use market_squawk_domain::{
        DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence, MetadataRevision,
        ProviderInstrumentId, SourceId, SourceIdentifier, Timestamp, VenueId,
    };

    use super::{
        OpenFigiAccess, OpenFigiConflictReason, OpenFigiListingMappingJob, OpenFigiMappingOutcome,
        OpenFigiRateLimitEvidence, encode_mapping_request, parse_mapping_response,
    };

    fn job(
        symbol: &str,
        mic: &str,
    ) -> Result<OpenFigiListingMappingJob, Box<dyn std::error::Error>> {
        OpenFigiListingMappingJob::try_new(
            SourceId::try_from("nasdaq-symbol-directory")?,
            MetadataRevision::new(SourceIdentifier::try_from("directory-v1")?),
            ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                [7; 32],
            )),
            Timestamp::from_unix_nanos(10),
            Timestamp::from_unix_nanos(11),
            ProviderInstrumentId::try_from(symbol)?,
            VenueId::try_from(mic)?,
        )
        .map_err(Into::into)
    }

    #[test]
    fn validates_exact_ambiguous_warning_conflict_and_rate_evidence()
    -> Result<(), Box<dyn std::error::Error>> {
        let jobs = vec![
            job("AAPL", "XNAS")?,
            job("IBM", "XNYS")?,
            job("NONE", "XNAS")?,
            job("DUPE", "XNAS")?,
        ];
        let request = encode_mapping_request(&jobs, OpenFigiAccess::Public)?;
        assert_eq!(
            std::str::from_utf8(&request)?,
            concat!(
                r#"[{"idType":"TICKER","idValue":"AAPL","micCode":"XNAS","includeUnlistedEquities":false},"#,
                r#"{"idType":"TICKER","idValue":"IBM","micCode":"XNYS","includeUnlistedEquities":false},"#,
                r#"{"idType":"TICKER","idValue":"NONE","micCode":"XNAS","includeUnlistedEquities":false},"#,
                r#"{"idType":"TICKER","idValue":"DUPE","micCode":"XNAS","includeUnlistedEquities":false}]"#,
            )
        );

        let response = br#"[
            {"data":[{"figi":"BBG000B9XVV8","ticker":"AAPL","name":"APPLE INC",
                "exchCode":"US","compositeFIGI":"BBG000B9XRY4",
                "shareClassFIGI":"BBG001S5N8V8","securityDescription":"AAPL"}]},
            {"data":[{"figi":"BBG000BLNNH6","compositeFIGI":"BBG000BLNNH6",
                "shareClassFIGI":"BBG001S5S399"},
                {"figi":"BBG000B9XVV8","compositeFIGI":"BBG000B9XRY4",
                "shareClassFIGI":"BBG001S5N8V8"}]},
            {"warning":"No identifier found."},
            {"data":[{"figi":"BBG000B9XVV8","compositeFIGI":"BBG000B9XRY4"},
                {"figi":"BBG000B9XVV8","compositeFIGI":"BBG000BLNNH6"}]}
        ]"#;
        let results = parse_mapping_response(&jobs, response)?;
        assert!(matches!(
            results[0].outcome(),
            OpenFigiMappingOutcome::Exact(_)
        ));
        assert!(matches!(
            results[1].outcome(),
            OpenFigiMappingOutcome::Ambiguous { candidates } if candidates.len() == 2
        ));
        assert_eq!(results[2].outcome(), &OpenFigiMappingOutcome::NoMatch);
        assert_eq!(
            results[3].outcome(),
            &OpenFigiMappingOutcome::Conflict {
                reason: OpenFigiConflictReason::RelationshipConflict,
            }
        );

        let rate = OpenFigiRateLimitEvidence::try_from_raw(b"025", b"024", b"60")?;
        assert_eq!(rate.limit(), 25);
        assert_eq!(rate.remaining(), 24);
        assert_eq!(rate.reset_after_seconds(), 60);
        assert_eq!(rate.raw_limit(), b"025");
        Ok(())
    }
}
