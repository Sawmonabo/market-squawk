//! Exact native economic-action decoding, independent of financial coverage admission.
use super::*;

/// Original native economic terms. Missing currency remains absent; no listing-unit inference.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TiingoCorporateActionValue {
    /// Documented distribution fields, including cancellation frequency.
    Distribution {
        /// Native amount with exact decimal representation.
        amount: Decimal,
        /// Original documented distribution frequency/status code.
        frequency: Box<str>,
        /// Native payment date; null remains absent.
        payment_date: Option<CalendarDate>,
        /// Native record date; null remains absent.
        record_date: Option<CalendarDate>,
        /// Native declaration date; null remains absent.
        declaration_date: Option<CalendarDate>,
    },
    /// Native split terms; financial normalization remains with original source admission.
    Split {
        /// Original share quantity before the split.
        from: Decimal,
        /// Original share quantity after the split.
        to: Decimal,
        /// Exact provider factor.
        factor: Decimal,
        /// Original code, exactly a or c.
        status: Box<str>,
    },
}
/// One native row. All batch symbols are retained; a decoder cannot create canonical identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoCorporateActionRow {
    ticker: TiingoTicker,
    perma_ticker: Box<str>,
    ex_date: CalendarDate,
    value: TiingoCorporateActionValue,
    native_payload: Box<[u8]>,
    digest: EvidenceDigest,
}
impl TiingoCorporateActionRow {
    /// Original provider ticker, never a canonical identifier.
    pub fn ticker(&self) -> &TiingoTicker {
        &self.ticker
    }
    /// Original provider permanent ticker text, without inferred mapping.
    pub fn perma_ticker(&self) -> &str {
        &self.perma_ticker
    }
    /// Original native economic date, not a synthesized instant.
    pub const fn ex_date(&self) -> CalendarDate {
        self.ex_date
    }
    /// Exact decoded native terms.
    pub fn value(&self) -> &TiingoCorporateActionValue {
        &self.value
    }
    /// Canonical JSON representation retaining every reviewed original row field.
    pub fn native_payload(&self) -> &[u8] {
        &self.native_payload
    }
    /// Digest of the retained native row representation; whole raw bytes remain separately sealed.
    pub const fn digest(&self) -> EvidenceDigest {
        self.digest
    }
}
/// Bounded original terminal source response. This is not financial completeness authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoCorporateActionReceipt {
    rows: Box<[TiingoCorporateActionRow]>,
    evidence: TiingoResponseEvidence,
}
impl TiingoCorporateActionReceipt {
    /// Every original returned row, including other symbols in a batch query.
    pub fn rows(&self) -> &[TiingoCorporateActionRow] {
        &self.rows
    }
    /// Actual request/status/body/clock evidence.
    pub const fn evidence(&self) -> &TiingoResponseEvidence {
        &self.evidence
    }
}
impl TiingoDecoder {
    /// Decodes one exact economic-date response. Invalid, oversized or partial arrays fail closed.
    pub fn decode_corporate_actions(
        &self,
        request: TiingoRequestSpec,
        status: u16,
        body: &[u8],
        received_at: Timestamp,
        decoded_at: Timestamp,
    ) -> Result<TiingoCorporateActionReceipt, TiingoAdapterError> {
        if !matches!(
            request.endpoint(),
            TiingoEndpointFamily::CorporateActionDistributions
                | TiingoEndpointFamily::CorporateActionSplits
        ) {
            return Err(TiingoAdapterError::RequestBuild);
        }
        let body_digest = validate_response(&request, status, body, received_at, decoded_at)?;
        let rows = parse_actions(body, &request)
            .map_err(|reason| self.schema_error(&request, reason, body_digest, decoded_at))?;
        Ok(TiingoCorporateActionReceipt {
            rows: rows.into_boxed_slice(),
            evidence: TiingoResponseEvidence::new(
                request,
                self.contract_revision.clone(),
                self.entitlement_generation.clone(),
                status,
                body_digest,
                u64::try_from(body.len()).map_err(|_| TiingoAdapterError::BodyTooLarge)?,
                received_at,
                decoded_at,
            ),
        })
    }
}
fn parse_actions(
    body: &[u8],
    request: &TiingoRequestSpec,
) -> SchemaResult<Vec<TiingoCorporateActionRow>> {
    let value: Value =
        serde_json::from_slice(body).map_err(|_| TiingoSchemaChangeReason::InvalidTopLevel)?;
    let values = value
        .as_array()
        .ok_or(TiingoSchemaChangeReason::InvalidTopLevel)?;
    if values.len() > request.max_rows() {
        return Err(TiingoSchemaChangeReason::RowLimitExceeded);
    }
    let (date, distributions) = match request.scope() {
        TiingoRequestScope::Distributions { date } => (*date, true),
        TiingoRequestScope::Splits { date } => (*date, false),
        _ => return Err(TiingoSchemaChangeReason::InvalidFieldValue),
    };
    let mut rows = Vec::with_capacity(values.len());
    let mut identities = BTreeSet::new();
    for value in values {
        let object = value
            .as_object()
            .ok_or(TiingoSchemaChangeReason::InvalidFieldType)?;
        ensure_exact_fields(
            object,
            if distributions {
                &[
                    "permaTicker",
                    "ticker",
                    "exDate",
                    "paymentDate",
                    "recordDate",
                    "declarationDate",
                    "distribution",
                    "distributionFrequency",
                ]
            } else {
                &[
                    "permaTicker",
                    "ticker",
                    "exDate",
                    "splitFrom",
                    "splitTo",
                    "splitFactor",
                    "splitStatus",
                ]
            },
        )?;
        let ticker = TiingoTicker::try_new(required_nonempty_string(
            object,
            "ticker",
            MAX_TICKER_BYTES,
        )?)
        .map_err(|_| TiingoSchemaChangeReason::InvalidFieldValue)?;
        if distributions && &ticker != request.ticker() {
            return Err(TiingoSchemaChangeReason::SymbolMismatch);
        }
        let perma_ticker = required_nonempty_string(object, "permaTicker", 128)?.into_boxed_str();
        let ex_date =
            action_date(object, "exDate")?.ok_or(TiingoSchemaChangeReason::InvalidFieldValue)?;
        if ex_date != date {
            return Err(TiingoSchemaChangeReason::InvalidRowSequence);
        }
        let terms = if distributions {
            let amount = required_decimal(object, "distribution")?;
            let frequency = required_nonempty_string(object, "distributionFrequency", 8)?;
            if amount < Decimal::ZERO
                || !matches!(
                    frequency.as_str(),
                    "w" | "bm" | "m" | "tm" | "q" | "sa" | "a" | "ir" | "f" | "u" | "c"
                )
            {
                return Err(TiingoSchemaChangeReason::InvalidFieldValue);
            }
            TiingoCorporateActionValue::Distribution {
                amount,
                frequency: frequency.into_boxed_str(),
                payment_date: action_date(object, "paymentDate")?,
                record_date: action_date(object, "recordDate")?,
                declaration_date: action_date(object, "declarationDate")?,
            }
        } else {
            let from = required_decimal(object, "splitFrom")?;
            let to = required_decimal(object, "splitTo")?;
            let factor = required_decimal(object, "splitFactor")?;
            let status = required_nonempty_string(object, "splitStatus", 1)?;
            if from <= Decimal::ZERO
                || to <= Decimal::ZERO
                || factor <= Decimal::ZERO
                || !matches!(status.as_str(), "a" | "c")
                || factor.checked_mul(from) != Some(to)
            {
                return Err(TiingoSchemaChangeReason::InvalidFieldValue);
            }
            TiingoCorporateActionValue::Split {
                from,
                to,
                factor,
                status: status.into_boxed_str(),
            }
        };
        let native_payload = serde_json::to_vec(value)
            .map_err(|_| TiingoSchemaChangeReason::InvalidFieldValue)?
            .into_boxed_slice();
        let row_digest = digest(&native_payload);
        if !identities.insert(row_digest.bytes()) {
            return Err(TiingoSchemaChangeReason::InvalidRowSequence);
        }
        rows.push(TiingoCorporateActionRow {
            ticker,
            perma_ticker,
            ex_date,
            value: terms,
            native_payload,
            digest: row_digest,
        });
    }
    Ok(rows)
}
fn required_decimal(object: &Map<String, Value>, field: &str) -> SchemaResult<Decimal> {
    optional_decimal(object, field)?.ok_or(TiingoSchemaChangeReason::InvalidFieldValue)
}
fn action_date(object: &Map<String, Value>, field: &str) -> SchemaResult<Option<CalendarDate>> {
    let value = object
        .get(field)
        .ok_or(TiingoSchemaChangeReason::MissingField)?;
    if value.is_null() {
        return Ok(None);
    }
    let value = value
        .as_str()
        .ok_or(TiingoSchemaChangeReason::InvalidFieldType)?;
    if value.len() == 10 {
        parse_calendar_date(value).map(Some)
    } else {
        parse_provider_daily_date(value).map(Some)
    }
}
