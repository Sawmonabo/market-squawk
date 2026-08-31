//! Sealed, surface-neutral Tiingo history completion evidence.
//!
//! Daily history is fetched and sealed one application-created date window at a time. These types
//! close the ordered plan only after every exact page is present, allowing both mutual-fund NAV
//! and equity/ETF EOD mapping to bind the same HTTP request-graph completion fact without
//! buffering raw bodies. Separate calendar authority must still prove financial-date coverage.

use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, MetadataRevision, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_sources::{ProviderCaptureTerminalDisposition, SealedProviderCaptureSetReceipt};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::{
    TiingoEndpointFamily, TiingoEodReceipt, TiingoHistoryCheckpointReceipt, TiingoHistoryPlan,
    TiingoPaginationEvidence, TiingoProviderAuthorityInstallation, TiingoRequestScope,
    TiingoRequestSpec,
};

const TIINGO_SOURCE_ID: &str = "tiingo-starter";
const TIINGO_HISTORY_DATASET: &str = "tiingo-daily-history-window";

/// One exact decoded history response after its raw body is sealed by shared authority.
#[derive(Debug, Eq, PartialEq)]
pub struct TiingoSealedHistoryPage {
    source_id: SourceId,
    source_contract_revision: MetadataRevision,
    native_contract_revision: SourceIdentifier,
    entitlement_generation: SourceIdentifier,
    request: TiingoRequestSpec,
    response_body_digest: EvidenceDigest,
    response_status: u16,
    response_bytes: u64,
    received_at: Timestamp,
    decoded_at: Timestamp,
    row_digests: Box<[EvidenceDigest]>,
    sealed_capture_receipt: EvidenceDigest,
    page_identity: EvidenceDigest,
}

impl TiingoSealedHistoryPage {
    /// Binds one strict decoded page to the exact expected plan request and shared raw seal.
    pub fn try_new(
        expected_request: &TiingoRequestSpec,
        response: &TiingoEodReceipt,
        sealed_capture: &SealedProviderCaptureSetReceipt,
    ) -> Result<Self, TiingoHistoryEvidenceError> {
        let evidence = response.evidence();
        let request = evidence.request();
        let capture = sealed_capture.capture();
        let returned_rows = u32::try_from(response.rows().len())
            .map_err(|_| TiingoHistoryEvidenceError::Allocation)?;
        let Some(capture_page) = capture.pages().first() else {
            return Err(TiingoHistoryEvidenceError::PageMismatch);
        };
        let expected_page = match expected_request.scope() {
            TiingoRequestScope::History { page, .. } => *page,
            _ => return Err(TiingoHistoryEvidenceError::PageMismatch),
        };
        if request != expected_request
            || request.endpoint() != TiingoEndpointFamily::HistoricalDailyPrices
            || response.pagination()
                != TiingoPaginationEvidence::ApplicationDateWindow(expected_page)
            || capture.source_id().as_str() != TIINGO_SOURCE_ID
            || capture.dataset().as_str() != TIINGO_HISTORY_DATASET
            || capture.terminal() != ProviderCaptureTerminalDisposition::StandaloneResponse
            || capture.pages().len() != 1
            || !(200..300).contains(&evidence.status())
            || capture.request_set_identity() != request.request_identity()
            || capture.total_body_bytes() != evidence.response_bytes()
            || capture_page.request_identity() != request.request_identity()
            || capture_page.http_status() != evidence.status()
            || capture_page.body_bytes() != evidence.response_bytes()
            || capture_page.body_digest() != evidence.body_digest()
            || capture_page.received_at() != evidence.received_at()
            || evidence.received_at() > evidence.decoded_at()
            || response.disposition().returned_rows() != returned_rows
            || response.disposition().response_bytes() != evidence.response_bytes()
        {
            return Err(TiingoHistoryEvidenceError::PageMismatch);
        }
        let mut row_digests = Vec::new();
        row_digests
            .try_reserve_exact(response.rows().len())
            .map_err(|_| TiingoHistoryEvidenceError::Allocation)?;
        row_digests.extend(response.rows().iter().map(|row| row.row_digest()));
        let sealed_capture_receipt = sealed_capture.receipt_digest();
        let page_identity = history_page_identity(response, sealed_capture_receipt, &row_digests);
        Ok(Self {
            source_id: capture.source_id().clone(),
            source_contract_revision: capture.metadata_revision().clone(),
            native_contract_revision: evidence.native_contract_revision().clone(),
            entitlement_generation: evidence.entitlement_generation().clone(),
            request: request.clone(),
            response_body_digest: evidence.body_digest(),
            response_status: evidence.status(),
            response_bytes: evidence.response_bytes(),
            received_at: evidence.received_at(),
            decoded_at: evidence.decoded_at(),
            row_digests: row_digests.into_boxed_slice(),
            sealed_capture_receipt,
            page_identity,
        })
    }

    /// Returns the exact Tiingo source identity from the shared capture receipt.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the source-contract revision that owns the raw page.
    pub const fn source_contract_revision(&self) -> &MetadataRevision {
        &self.source_contract_revision
    }

    /// Returns the exact strict decoder contract used for this page.
    pub const fn native_contract_revision(&self) -> &SourceIdentifier {
        &self.native_contract_revision
    }

    /// Returns the exact credential/entitlement generation used for this page.
    pub const fn entitlement_generation(&self) -> &SourceIdentifier {
        &self.entitlement_generation
    }

    /// Returns the exact application-created history-window request.
    pub const fn request(&self) -> &TiingoRequestSpec {
        &self.request
    }

    /// Returns the exact provider response-body digest.
    pub const fn response_body_digest(&self) -> EvidenceDigest {
        self.response_body_digest
    }

    /// Returns the successful HTTP status.
    pub const fn response_status(&self) -> u16 {
        self.response_status
    }

    /// Returns exact retained response bytes.
    pub const fn response_bytes(&self) -> u64 {
        self.response_bytes
    }

    /// Returns when the complete raw body arrived locally.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns when strict native decoding completed.
    pub const fn decoded_at(&self) -> Timestamp {
        self.decoded_at
    }

    /// Returns every exact provider-native row digest in source order.
    pub fn row_digests(&self) -> &[EvidenceDigest] {
        &self.row_digests
    }

    /// Returns shared immutable raw-seal evidence.
    pub const fn sealed_capture_receipt(&self) -> EvidenceDigest {
        self.sealed_capture_receipt
    }

    /// Returns the exact decoded-page/seal identity.
    pub const fn page_identity(&self) -> EvidenceDigest {
        self.page_identity
    }
}

/// Terminal fact for a complete application-created Tiingo HTTP request graph.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TiingoHistoryTerminalDisposition {
    /// Every exact date window completed; the reviewed Tiingo endpoint supplied no cursor.
    ApplicationDateWindowsExhaustedWithoutProviderCursor,
}

/// Surface-neutral request-graph-complete raw/native handoff for later coverage reconciliation.
#[derive(Debug, Eq, PartialEq)]
pub struct TiingoCompletedHistoryCapture {
    source_id: SourceId,
    source_contract_revision: MetadataRevision,
    native_contract_revision: SourceIdentifier,
    entitlement_generation: SourceIdentifier,
    plan: TiingoHistoryPlan,
    pages: Box<[TiingoSealedHistoryPage]>,
    terminal: TiingoHistoryTerminalDisposition,
    total_response_bytes: u64,
    total_rows: u64,
    checkpoint_receipt_identity: EvidenceDigest,
    completion_identity: EvidenceDigest,
}

impl TiingoCompletedHistoryCapture {
    /// Closes a plan only when every sealed page matches exactly and in order.
    pub(crate) fn try_new(
        plan: TiingoHistoryPlan,
        pages: Vec<TiingoSealedHistoryPage>,
        checkpoint: &TiingoHistoryCheckpointReceipt,
        installation: &TiingoProviderAuthorityInstallation,
    ) -> Result<Self, TiingoHistoryEvidenceError> {
        if pages.len() != plan.pages().len()
            || pages
                .iter()
                .zip(plan.pages())
                .any(|(page, expected)| page.request() != expected)
        {
            return Err(TiingoHistoryEvidenceError::IncompletePlan);
        }
        let Some(first) = pages.first() else {
            return Err(TiingoHistoryEvidenceError::IncompletePlan);
        };
        if pages.iter().any(|page| {
            page.source_id() != first.source_id()
                || page.source_contract_revision() != first.source_contract_revision()
                || page.native_contract_revision() != first.native_contract_revision()
                || page.entitlement_generation() != first.entitlement_generation()
        }) {
            return Err(TiingoHistoryEvidenceError::IncompletePlan);
        }
        let mut total_response_bytes = 0_u64;
        let mut total_rows = 0_u64;
        for page in &pages {
            total_response_bytes = total_response_bytes
                .checked_add(page.response_bytes())
                .ok_or(TiingoHistoryEvidenceError::Allocation)?;
            total_rows = total_rows
                .checked_add(
                    u64::try_from(page.row_digests().len())
                        .map_err(|_| TiingoHistoryEvidenceError::Allocation)?,
                )
                .ok_or(TiingoHistoryEvidenceError::Allocation)?;
        }
        if total_response_bytes > plan.maximum_response_bytes() {
            return Err(TiingoHistoryEvidenceError::IncompletePlan);
        }
        let page_count =
            u32::try_from(pages.len()).map_err(|_| TiingoHistoryEvidenceError::Allocation)?;
        checkpoint
            .validate_for(
                &plan,
                installation,
                page_count,
                pages.last().map(TiingoSealedHistoryPage::page_identity),
            )
            .map_err(|_| TiingoHistoryEvidenceError::IncompletePlan)?;
        let checkpoint_receipt_identity = checkpoint.receipt_identity();
        let terminal =
            TiingoHistoryTerminalDisposition::ApplicationDateWindowsExhaustedWithoutProviderCursor;
        let completion_identity = complete_history_identity(
            &plan,
            &pages,
            terminal,
            total_response_bytes,
            total_rows,
            checkpoint_receipt_identity,
        );
        let source_id = first.source_id().clone();
        let source_contract_revision = first.source_contract_revision().clone();
        let native_contract_revision = first.native_contract_revision().clone();
        let entitlement_generation = first.entitlement_generation().clone();
        Ok(Self {
            source_id,
            source_contract_revision,
            native_contract_revision,
            entitlement_generation,
            plan,
            pages: pages.into_boxed_slice(),
            terminal,
            total_response_bytes,
            total_rows,
            checkpoint_receipt_identity,
            completion_identity,
        })
    }

    /// Returns the exact Tiingo source identity shared by every page.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the activated source-contract revision shared by every page.
    pub const fn source_contract_revision(&self) -> &MetadataRevision {
        &self.source_contract_revision
    }

    /// Returns the strict native decoder contract shared by every page.
    pub const fn native_contract_revision(&self) -> &SourceIdentifier {
        &self.native_contract_revision
    }

    /// Returns the protected entitlement generation shared by every page.
    pub const fn entitlement_generation(&self) -> &SourceIdentifier {
        &self.entitlement_generation
    }

    /// Returns the complete exact request plan.
    pub const fn plan(&self) -> &TiingoHistoryPlan {
        &self.plan
    }

    /// Returns every sealed decoded page in exact plan order.
    pub fn pages(&self) -> &[TiingoSealedHistoryPage] {
        &self.pages
    }

    /// Returns explicit application-window exhaustion without a provider-cursor claim.
    pub const fn terminal(&self) -> TiingoHistoryTerminalDisposition {
        self.terminal
    }

    /// Returns exact retained raw response bytes across the plan.
    pub const fn total_response_bytes(&self) -> u64 {
        self.total_response_bytes
    }

    /// Returns strict provider-native row cardinality across the plan.
    pub const fn total_rows(&self) -> u64 {
        self.total_rows
    }

    /// Returns the exact terminal shared-authority checkpoint bound into completion.
    pub const fn checkpoint_receipt_identity(&self) -> EvidenceDigest {
        self.checkpoint_receipt_identity
    }

    /// Returns the exact HTTP plan/page/terminal identity consumed before financial coverage proof.
    pub const fn completion_identity(&self) -> EvidenceDigest {
        self.completion_identity
    }
}

fn history_page_identity(
    response: &TiingoEodReceipt,
    sealed_capture_receipt: EvidenceDigest,
    row_digests: &[EvidenceDigest],
) -> EvidenceDigest {
    let evidence = response.evidence();
    let disposition = response.disposition();
    let mut hasher = Sha256::new();
    append_field(&mut hasher, b"market-squawk/tiingo/sealed-history-page/v1");
    for digest in [
        evidence.request().request_identity(),
        evidence.body_digest(),
        sealed_capture_receipt,
    ] {
        append_field(&mut hasher, &digest.bytes());
    }
    append_field(
        &mut hasher,
        evidence.native_contract_revision().as_str().as_bytes(),
    );
    append_field(
        &mut hasher,
        evidence.entitlement_generation().as_str().as_bytes(),
    );
    for value in [
        u64::from(evidence.status()),
        evidence.response_bytes(),
        u64::from(disposition.requested_symbols()),
        u64::from(disposition.returned_symbols()),
        u64::from(disposition.missing_symbols()),
        u64::from(disposition.returned_rows()),
        u64::try_from(row_digests.len()).unwrap_or(u64::MAX),
    ] {
        append_field(&mut hasher, &value.to_be_bytes());
    }
    append_field(
        &mut hasher,
        &evidence.received_at().unix_nanos().to_be_bytes(),
    );
    append_field(
        &mut hasher,
        &evidence.decoded_at().unix_nanos().to_be_bytes(),
    );
    for digest in row_digests {
        append_field(&mut hasher, &digest.bytes());
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into())
}

fn complete_history_identity(
    plan: &TiingoHistoryPlan,
    pages: &[TiingoSealedHistoryPage],
    terminal: TiingoHistoryTerminalDisposition,
    total_response_bytes: u64,
    total_rows: u64,
    checkpoint_receipt_identity: EvidenceDigest,
) -> EvidenceDigest {
    let mut hasher = Sha256::new();
    append_field(
        &mut hasher,
        b"market-squawk/tiingo/sealed-history-completion/v1",
    );
    append_field(&mut hasher, &plan.request_set_identity().bytes());
    append_field(&mut hasher, &plan.maximum_response_bytes().to_be_bytes());
    append_field(
        &mut hasher,
        &u64::try_from(pages.len()).unwrap_or(u64::MAX).to_be_bytes(),
    );
    for page in pages {
        append_field(&mut hasher, &page.page_identity().bytes());
    }
    append_field(&mut hasher, &total_response_bytes.to_be_bytes());
    append_field(&mut hasher, &total_rows.to_be_bytes());
    append_field(&mut hasher, &checkpoint_receipt_identity.bytes());
    append_field(
        &mut hasher,
        match terminal {
            TiingoHistoryTerminalDisposition::ApplicationDateWindowsExhaustedWithoutProviderCursor => {
                b"application-date-windows-exhausted-without-provider-cursor"
            }
        },
    );
    EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into())
}

fn append_field(hasher: &mut Sha256, value: &[u8]) {
    hasher.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(value);
}

/// Closed failure to bind exact sealed pages into a complete Tiingo history plan.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum TiingoHistoryEvidenceError {
    /// The decoded response, expected page, and shared raw seal disagreed.
    #[error("sealed Tiingo history page evidence does not match")]
    PageMismatch,
    /// One or more plan pages were absent, reordered, cross-source, or over capacity.
    #[error("sealed Tiingo history plan is incomplete")]
    IncompletePlan,
    /// Bounded evidence allocation or checked aggregation failed.
    #[error("sealed Tiingo history evidence allocation failed")]
    Allocation,
}
