use std::collections::BTreeMap;
use std::fmt;
use std::marker::PhantomData;

use market_squawk_domain::Figi;
use serde::de::{IgnoredAny, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};

use crate::model::digest_message;
use crate::{
    OpenFigiAccess, OpenFigiConflictReason, OpenFigiIdentityCandidate, OpenFigiListingMappingJob,
    OpenFigiMappingOutcome, OpenFigiMappingResult, OpenFigiParseError, OpenFigiRequestError,
};

/// Maximum exact serialized request retained by the adapter.
pub const MAX_OPENFIGI_REQUEST_BYTES: usize = 64 * 1024;
/// Maximum exact response retained and parsed by the adapter.
pub const MAX_OPENFIGI_RESPONSE_BYTES: usize = 2 * 1024 * 1024;
/// Maximum FIGI candidates retained for one exact ticker/MIC job.
pub const MAX_OPENFIGI_CANDIDATES_PER_JOB: usize = 256;
const MAX_PROVIDER_MESSAGE_BYTES: usize = 4 * 1024;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MappingJobWire<'a> {
    id_type: &'static str,
    id_value: &'a str,
    mic_code: &'a str,
    include_unlisted_equities: bool,
}

/// Encodes the exact deterministic V3 request for source-qualified listing jobs.
///
/// # Errors
///
/// Rejects an empty request, access-tier job overflow, serialization failure, or byte overflow.
pub fn encode_mapping_request(
    jobs: &[OpenFigiListingMappingJob],
    access: OpenFigiAccess,
) -> Result<Vec<u8>, OpenFigiRequestError> {
    if jobs.is_empty() {
        return Err(OpenFigiRequestError::Empty);
    }
    let max = access.max_jobs_per_request();
    if jobs.len() > max {
        return Err(OpenFigiRequestError::TooManyJobs { max });
    }
    let wire = jobs
        .iter()
        .map(|job| MappingJobWire {
            id_type: "TICKER",
            id_value: job.symbol().as_str(),
            mic_code: job.mic().as_str(),
            include_unlisted_equities: false,
        })
        .collect::<Vec<_>>();
    let bytes = serde_json::to_vec(&wire).map_err(|_| OpenFigiRequestError::Serialization)?;
    if bytes.len() > MAX_OPENFIGI_REQUEST_BYTES {
        return Err(OpenFigiRequestError::TooLarge {
            max: MAX_OPENFIGI_REQUEST_BYTES,
        });
    }
    Ok(bytes)
}

#[derive(Deserialize)]
struct MappingResultWire {
    data: Option<CandidateListWire>,
    warning: Option<String>,
    error: Option<String>,
}

#[derive(Deserialize)]
struct CandidateWire {
    figi: Option<String>,
    #[serde(rename = "compositeFIGI")]
    composite_figi: Option<String>,
    #[serde(rename = "shareClassFIGI")]
    share_class_figi: Option<String>,
}

struct CandidateListWire {
    values: Vec<CandidateWire>,
    overflowed: bool,
}

impl<'de> Deserialize<'de> for CandidateListWire {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct CandidateListVisitor;

        impl<'de> Visitor<'de> for CandidateListVisitor {
            type Value = CandidateListWire;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an OpenFIGI candidate array")
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let capacity = sequence
                    .size_hint()
                    .unwrap_or(0)
                    .min(MAX_OPENFIGI_CANDIDATES_PER_JOB);
                let mut values = Vec::new();
                values
                    .try_reserve_exact(capacity)
                    .map_err(serde::de::Error::custom)?;
                while values.len() < MAX_OPENFIGI_CANDIDATES_PER_JOB {
                    let Some(value) = sequence.next_element()? else {
                        return Ok(CandidateListWire {
                            values,
                            overflowed: false,
                        });
                    };
                    values.push(value);
                }
                let mut overflowed = false;
                while sequence.next_element::<IgnoredAny>()?.is_some() {
                    overflowed = true;
                }
                Ok(CandidateListWire { values, overflowed })
            }
        }

        deserializer.deserialize_seq(CandidateListVisitor)
    }
}

struct MappingResultsWire(Vec<MappingResultWire>);

impl<'de> Deserialize<'de> for MappingResultsWire {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct MappingResultsVisitor(PhantomData<MappingResultWire>);

        impl<'de> Visitor<'de> for MappingResultsVisitor {
            type Value = MappingResultsWire;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an OpenFIGI mapping-result array")
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let capacity = sequence
                    .size_hint()
                    .unwrap_or(0)
                    .min(crate::OPENFIGI_API_KEY_MAX_JOBS + 1);
                let mut values = Vec::new();
                values
                    .try_reserve_exact(capacity)
                    .map_err(serde::de::Error::custom)?;
                while values.len() <= crate::OPENFIGI_API_KEY_MAX_JOBS {
                    let Some(value) = sequence.next_element()? else {
                        return Ok(MappingResultsWire(values));
                    };
                    values.push(value);
                }
                while sequence.next_element::<IgnoredAny>()?.is_some() {}
                Ok(MappingResultsWire(values))
            }
        }

        deserializer.deserialize_seq(MappingResultsVisitor(PhantomData))
    }
}

/// Parses and validates a bounded V3 response while preserving request-array position.
///
/// Descriptive provider fields are intentionally ignored by the typed parser. Callers that need
/// exact source inspection use [`crate::OpenFigiMappingReceipt::response`].
///
/// # Errors
///
/// Rejects empty/oversized/invalid JSON and any result-array cardinality mismatch. Per-job
/// contradictions are returned as typed conflict outcomes so no unaffected job is silently lost.
pub fn parse_mapping_response(
    jobs: &[OpenFigiListingMappingJob],
    payload: &[u8],
) -> Result<Vec<OpenFigiMappingResult>, OpenFigiParseError> {
    if payload.is_empty() {
        return Err(OpenFigiParseError::Empty);
    }
    if payload.len() > MAX_OPENFIGI_RESPONSE_BYTES {
        return Err(OpenFigiParseError::TooLarge {
            max: MAX_OPENFIGI_RESPONSE_BYTES,
        });
    }
    if jobs.is_empty() || jobs.len() > crate::OPENFIGI_API_KEY_MAX_JOBS {
        return Err(OpenFigiParseError::Cardinality);
    }
    let MappingResultsWire(wire) =
        serde_json::from_slice(payload).map_err(|_| OpenFigiParseError::InvalidJson)?;
    if wire.len() != jobs.len() {
        return Err(OpenFigiParseError::Cardinality);
    }
    let mut results = Vec::new();
    results
        .try_reserve_exact(jobs.len())
        .map_err(|_| OpenFigiParseError::Allocation)?;
    for (job, result) in jobs.iter().cloned().zip(wire) {
        results.push(OpenFigiMappingResult::new(job, classify(result)));
    }
    Ok(results)
}

fn classify(result: MappingResultWire) -> OpenFigiMappingOutcome {
    let present = usize::from(result.data.is_some())
        + usize::from(result.warning.is_some())
        + usize::from(result.error.is_some());
    if present == 0 {
        return conflict(OpenFigiConflictReason::MissingOutcome);
    }
    if present != 1 {
        return conflict(OpenFigiConflictReason::MultipleOutcomeKinds);
    }
    if let Some(warning) = result.warning {
        return if valid_provider_message(&warning) {
            OpenFigiMappingOutcome::NoMatch
        } else {
            conflict(OpenFigiConflictReason::InvalidProviderMessage)
        };
    }
    if let Some(error) = result.error {
        return if valid_provider_message(&error) {
            OpenFigiMappingOutcome::ProviderError {
                message_digest: digest_message(&error),
            }
        } else {
            conflict(OpenFigiConflictReason::InvalidProviderMessage)
        };
    }
    let Some(candidates) = result.data else {
        return conflict(OpenFigiConflictReason::MissingOutcome);
    };
    classify_candidates(candidates)
}

fn classify_candidates(candidates: CandidateListWire) -> OpenFigiMappingOutcome {
    if candidates.overflowed {
        return conflict(OpenFigiConflictReason::CandidateLimitExceeded);
    }
    if candidates.values.is_empty() {
        return conflict(OpenFigiConflictReason::EmptyData);
    }
    let mut normalized = Vec::new();
    if normalized
        .try_reserve_exact(candidates.values.len())
        .is_err()
    {
        return conflict(OpenFigiConflictReason::CandidateLimitExceeded);
    }
    for candidate in candidates.values {
        let Some(figi) = candidate.figi else {
            return conflict(OpenFigiConflictReason::InvalidFigi);
        };
        let Ok(exchange_figi) = Figi::try_from(figi) else {
            return conflict(OpenFigiConflictReason::InvalidFigi);
        };
        let composite_figi = match parse_optional_figi(candidate.composite_figi) {
            Ok(figi) => figi,
            Err(()) => return conflict(OpenFigiConflictReason::InvalidFigi),
        };
        let share_class_figi = match parse_optional_figi(candidate.share_class_figi) {
            Ok(figi) => figi,
            Err(()) => return conflict(OpenFigiConflictReason::InvalidFigi),
        };
        normalized.push(OpenFigiIdentityCandidate::new(
            exchange_figi,
            composite_figi,
            share_class_figi,
        ));
    }
    normalized.sort_unstable();
    if normalized.windows(2).any(|window| window[0] == window[1]) {
        return conflict(OpenFigiConflictReason::DuplicateCandidate);
    }
    let mut relationships = BTreeMap::new();
    for candidate in &normalized {
        let relationships_for_figi = (
            candidate.composite_figi().cloned(),
            candidate.share_class_figi().cloned(),
        );
        if relationships
            .insert(
                candidate.exchange_figi().clone(),
                relationships_for_figi.clone(),
            )
            .is_some_and(|previous| previous != relationships_for_figi)
        {
            return conflict(OpenFigiConflictReason::RelationshipConflict);
        }
    }
    if normalized.len() == 1 {
        return OpenFigiMappingOutcome::Exact(normalized.remove(0));
    }
    OpenFigiMappingOutcome::Ambiguous {
        candidates: normalized,
    }
}

fn parse_optional_figi(value: Option<String>) -> Result<Option<Figi>, ()> {
    value.map(Figi::try_from).transpose().map_err(|_| ())
}

fn valid_provider_message(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_PROVIDER_MESSAGE_BYTES
        && !value.chars().any(char::is_control)
}

const fn conflict(reason: OpenFigiConflictReason) -> OpenFigiMappingOutcome {
    OpenFigiMappingOutcome::Conflict { reason }
}
