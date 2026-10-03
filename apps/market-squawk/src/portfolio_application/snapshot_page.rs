//! Admission, immutable snapshot pinning and byte fitting for selected-account pages.

use market_squawk_services::{
    RequestContext, ToolResultMetadata, TypedToolRequest, TypedToolResult,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};

use super::import::hex;
use super::model::{PortfolioReadImage, PublishedRevision};
use super::read::{ReadScope, check_context, narrowed_limits, select_revision, snapshot_token};
use super::{PortfolioApplicationLimits, PortfolioApplicationServiceError};

pub(super) struct SnapshotPage<'image> {
    pub(super) revision: &'image PublishedRevision,
    pub(super) scope: ReadScope,
    pub(super) limit: usize,
    pub(super) scope_digest: String,
}

pub(super) struct PageOutput {
    pub(super) row_field: &'static str,
    pub(super) rows: Vec<Value>,
    /// Includes one lookahead row when continuation is available.
    pub(super) available: usize,
    pub(super) page_cursor: String,
    pub(super) extra: Option<(&'static str, Value)>,
}

pub(super) fn read_cursor<T: DeserializeOwned>(
    request: &TypedToolRequest,
    context: &RequestContext,
) -> Result<Option<T>, PortfolioApplicationServiceError> {
    check_context(context)?;
    request
        .arguments()
        .get("cursor")
        .filter(|value| !value.is_null())
        .map(|value| {
            let encoded = value
                .as_str()
                .filter(|value| !value.is_empty() && value.len() <= 512)
                .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
            serde_json::from_str(encoded)
                .map_err(|_| PortfolioApplicationServiceError::InvalidRequest)
        })
        .transpose()
}

pub(super) fn encode_cursor<T: Serialize>(
    cursor: &T,
) -> Result<String, PortfolioApplicationServiceError> {
    let encoded =
        serde_json::to_string(cursor).map_err(|_| PortfolioApplicationServiceError::Publication)?;
    if encoded.len() > 512 {
        return Err(PortfolioApplicationServiceError::ResourceExhausted);
    }
    Ok(encoded)
}

impl<'image> SnapshotPage<'image> {
    pub(super) fn admit(
        image: &'image PortfolioReadImage,
        request: &TypedToolRequest,
        context: &RequestContext,
        application_limits: PortfolioApplicationLimits,
        domain: &[u8],
        cursor_pin: Option<(u8, &str, &str)>,
    ) -> Result<Self, PortfolioApplicationServiceError> {
        check_context(context)?;
        if request.arguments().contains_key("accountId") {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
        let scope = ReadScope::from_product_request(image, request, application_limits)?;
        let limit = request
            .arguments()
            .get("limit")
            .filter(|value| !value.is_null())
            .map(|value| {
                value
                    .as_u64()
                    .and_then(|value| usize::try_from(value).ok())
                    .filter(|value| (1..=100).contains(value))
                    .ok_or(PortfolioApplicationServiceError::InvalidRequest)
            })
            .transpose()?
            .unwrap_or(25)
            .min(scope.maximum_items)
            .min(context.limits().maximum_result_items());
        if limit == 0 {
            return Err(PortfolioApplicationServiceError::ResourceExhausted);
        }
        let scope_digest = scope_digest(&scope, domain, context)?;
        let revision = match cursor_pin {
            Some((version, revision, cursor_scope)) => {
                if version != 1 || cursor_scope != scope_digest {
                    return Err(PortfolioApplicationServiceError::InvalidRequest);
                }
                pinned_revision(
                    image,
                    &scope,
                    |candidate| hex(&candidate.token().bytes()) == revision,
                    context,
                )?
            }
            None => select_revision(image, &scope)?,
        };
        if revision.account.account_id() != scope.account_id {
            return Err(PortfolioApplicationServiceError::CorruptPublication);
        }
        Ok(Self {
            revision,
            scope,
            limit,
            scope_digest,
        })
    }

    pub(super) fn finish(
        &self,
        context: &RequestContext,
        mut page: PageOutput,
        next_cursor: impl Fn(usize) -> Result<String, PortfolioApplicationServiceError>,
    ) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
        let revision = self.revision;
        let snapshot = snapshot_token(revision);
        let limits = narrowed_limits(context, self.scope.maximum_items, self.scope.maximum_bytes)?;
        let metadata = ToolResultMetadata::try_complete(
            json!({
                "scope":"portfolio",
                "snapshotToken":snapshot,
                "effectiveAtUnixNanos":revision.effective_at.unix_nanos().to_string(),
                "availableAtUnixNanos":revision.available_at.map(|time| time.unix_nanos().to_string()),
            }),
            json!({
                "state":if revision.discrepancies.is_empty() { "available" } else { "needs_review" },
                "confidence":"limited",
                "dataIssueCount":revision.discrepancies.len(),
            }),
        ).map_err(|_| PortfolioApplicationServiceError::Publication)?;
        loop {
            check_context(context)?;
            let count = page.rows.len();
            let next = if count < page.available {
                Some(next_cursor(count.checked_sub(1).ok_or(
                    PortfolioApplicationServiceError::ResourceExhausted,
                )?)?)
            } else {
                None
            };
            let mut output = json!({
                "pageCursor":page.page_cursor,
                "nextCursor":next,
                "snapshotToken":snapshot,
                "effectiveAtUnixNanos":revision.effective_at.unix_nanos().to_string(),
                "availableAtUnixNanos":revision.available_at.map(|time| time.unix_nanos().to_string()),
            });
            output[page.row_field] = Value::Array(page.rows.clone());
            if let Some((field, value)) = &page.extra {
                output[*field] = value.clone();
            }
            match TypedToolResult::try_new(output, count, metadata.clone(), limits) {
                Ok(result) => {
                    check_context(context)?;
                    return Ok(result);
                }
                Err(_) if count > 1 => {
                    page.rows.pop();
                }
                Err(_) => return Err(PortfolioApplicationServiceError::ResourceExhausted),
            }
        }
    }
}

/// Selects a saved product snapshot without silently falling forward to the current head.
pub(super) fn selected_revision<'image>(
    image: &'image PortfolioReadImage,
    scope: &ReadScope,
    token: &str,
    context: &RequestContext,
) -> Result<&'image PublishedRevision, PortfolioApplicationServiceError> {
    pinned_revision(
        image,
        scope,
        |revision| snapshot_token(revision) == token,
        context,
    )
}

fn pinned_revision<'image>(
    image: &'image PortfolioReadImage,
    scope: &ReadScope,
    matches: impl Fn(&PublishedRevision) -> bool,
    context: &RequestContext,
) -> Result<&'image PublishedRevision, PortfolioApplicationServiceError> {
    let history = image
        .accounts
        .get(&scope.account_id)
        .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
    for revision in &history.revisions {
        check_context(context)?;
        if !matches(revision) {
            continue;
        }
        if scope.end.is_some_and(|end| {
            revision.effective_at > end
                || revision
                    .available_at
                    .is_none_or(|available| available > end)
        }) {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
        if history
            .revisions
            .last()
            .is_some_and(|head| head.token() == revision.token())
            && image
                .revisions
                .head(scope.account_id)
                .map_err(|_| PortfolioApplicationServiceError::CorruptPublication)?
                != revision.token()
        {
            return Err(PortfolioApplicationServiceError::CorruptPublication);
        }
        return Ok(revision);
    }
    Err(PortfolioApplicationServiceError::InvalidRequest)
}

fn scope_digest(
    scope: &ReadScope,
    domain: &[u8],
    context: &RequestContext,
) -> Result<String, PortfolioApplicationServiceError> {
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update(scope.account_id.as_uuid().as_bytes());
    digest.update(
        u64::try_from(scope.instruments.len())
            .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?
            .to_be_bytes(),
    );
    for instrument in &scope.instruments {
        check_context(context)?;
        digest.update(instrument.as_uuid().as_bytes());
    }
    for time in [scope.start, scope.end] {
        match time {
            Some(time) => {
                digest.update([1]);
                digest.update(time.unix_nanos().to_be_bytes());
            }
            None => digest.update([0]),
        }
    }
    Ok(hex(&digest.finalize().into()))
}
