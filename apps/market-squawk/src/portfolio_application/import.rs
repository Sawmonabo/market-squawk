//! Controlled extraction-artifact import and crash-safe revision publication.

use std::collections::{BTreeMap, BTreeSet, btree_map::Entry};
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};

use market_squawk_adapter_portfolio::{
    AccountObservation, HoldingObservation, PortfolioExtractionSource, PortfolioImport,
    PortfolioImportLimits, PortfolioTransaction,
};
use market_squawk_data::{DatasetId, DatasetManifestRef, Sha256Digest, extraction_batch_digest};
use market_squawk_domain::{
    AccountId, DataQuality, DigestAlgorithm, MetadataRevision, Money, RevisionNumber, SourceId,
    SourceIdentifier, Timestamp,
};
use market_squawk_platform::{ArtifactPathError, ArtifactRoot, LocalAuthorityStateStore};
use market_squawk_portfolio::{
    CashFlow, CashFlowKind, LedgerEntry, LedgerEntryKind, PortfolioLedger, PortfolioLimitInput,
    PortfolioLimits, RevisionEvidence, TransactionRevision, ValuationSet,
};
use market_squawk_services::{RequestContext, TypedToolRequest, TypedToolResult};
use market_squawk_sources::ExtractionBatch;
use rust_decimal::Decimal;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};

use super::model::{
    AccountHistory, PortfolioReadImage, PublicationEntry, PublicationManifest, PublishedRevision,
    SourceKey,
};
use super::read;
use super::{PortfolioApplicationLimits, PortfolioApplicationServiceError, Runtime, ensure_live};

const RAW_ARCHIVE_NAMESPACE: &str = "portfolio/sources";
const PUBLICATION_AUTHORITY_DIRECTORY: &str = "portfolio/publication";
const IMMUTABLE_IMPORT_NAMESPACE: &str = "portfolio/imports";

#[derive(Clone, Debug)]
pub(super) struct ImportRequest {
    account_id: AccountId,
    artifact_id: String,
    requested_maximum_bytes: usize,
}

impl ImportRequest {
    pub(super) fn from_request(
        request: &TypedToolRequest,
    ) -> Result<Self, PortfolioApplicationServiceError> {
        let account_id = request
            .arguments()
            .get("accountId")
            .and_then(Value::as_str)
            .ok_or(PortfolioApplicationServiceError::InvalidRequest)?
            .parse()
            .map_err(|_| PortfolioApplicationServiceError::InvalidRequest)?;
        let artifact_id = request
            .arguments()
            .get("artifactId")
            .and_then(Value::as_str)
            .ok_or(PortfolioApplicationServiceError::InvalidRequest)?
            .to_owned();
        let requested_maximum_bytes = request
            .arguments()
            .get("resultLimits")
            .and_then(Value::as_object)
            .and_then(|limits| limits.get("maximumBytes"))
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
            .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
        Ok(Self {
            account_id,
            artifact_id,
            requested_maximum_bytes,
        })
    }
}

pub(super) struct ImportPublication {
    pub(super) image: PortfolioReadImage,
    pub(super) result: TypedToolResult,
}

pub(super) struct ImportAuthority {
    control_root: PathBuf,
    publication: LocalAuthorityStateStore,
    sources: BTreeMap<SourceKey, PortfolioExtractionSource>,
    accounts: BTreeMap<AccountId, AccountHistory>,
    manifest: PublicationManifest,
    limits: PortfolioApplicationLimits,
    portfolio_limits: PortfolioLimits,
}

impl ImportAuthority {
    pub(super) fn restore(
        artifacts: ArtifactRoot,
        control_root: &Path,
        limits: PortfolioApplicationLimits,
    ) -> Result<(Self, PortfolioReadImage), PortfolioApplicationServiceError> {
        let publication =
            LocalAuthorityStateStore::try_open(control_root.join(PUBLICATION_AUTHORITY_DIRECTORY))
                .map_err(|_| PortfolioApplicationServiceError::Authority)?;
        let manifest = publication
            .load()
            .map_err(|_| PortfolioApplicationServiceError::Authority)?
            .map_or_else(
                || Ok(PublicationManifest::empty()),
                |bytes| PublicationManifest::decode(&bytes),
            )?;
        if manifest.entries.len()
            > limits
                .max_accounts
                .saturating_mul(limits.max_history_per_account)
        {
            return Err(PortfolioApplicationServiceError::CorruptPublication);
        }
        let portfolio_limits = portfolio_limits(limits)?;
        let mut authority = Self {
            control_root: control_root.to_path_buf(),
            publication,
            sources: BTreeMap::new(),
            accounts: BTreeMap::new(),
            manifest: PublicationManifest::empty(),
            limits,
            portfolio_limits,
        };
        let mut seen_publications = BTreeSet::new();
        for entry in &manifest.entries {
            let canonical_reference = format!(
                "{IMMUTABLE_IMPORT_NAMESPACE}/{}.json",
                hex(&entry.artifact_sha256)
            );
            if entry.artifact_reference != canonical_reference
                || !seen_publications.insert((entry.account_id, entry.artifact_sha256))
            {
                return Err(PortfolioApplicationServiceError::CorruptPublication);
            }
            let bytes = read_artifact(
                &artifacts,
                &entry.artifact_reference,
                limits.max_artifact_bytes,
            )?;
            if Sha256::digest(&bytes).as_slice() != entry.artifact_sha256 {
                return Err(PortfolioApplicationServiceError::CorruptPublication);
            }
            let batch: ExtractionBatch = serde_json::from_slice(&bytes)
                .map_err(|_| PortfolioApplicationServiceError::CorruptPublication)?;
            authority.apply_batch(entry.account_id, &batch, entry.artifact_sha256)?;
            authority.manifest.entries.push(entry.clone());
        }
        let image =
            PortfolioReadImage::try_from_accounts(authority.accounts.clone(), authority.limits)?;
        Ok((authority, image))
    }

    pub(super) fn import(
        &mut self,
        artifacts: &ArtifactRoot,
        request: ImportRequest,
        context: &RequestContext,
        runtime: &Runtime,
    ) -> Result<ImportPublication, PortfolioApplicationServiceError> {
        ensure_live(runtime, context)?;
        let bytes = read_artifact(
            artifacts,
            &request.artifact_id,
            self.limits.max_artifact_bytes,
        )?;
        ensure_live(runtime, context)?;
        let artifact_sha256: [u8; 32] = Sha256::digest(&bytes).into();
        let batch: ExtractionBatch = serde_json::from_slice(&bytes)
            .map_err(|_| PortfolioApplicationServiceError::InvalidRequest)?;
        let canonical_reference = format!(
            "{IMMUTABLE_IMPORT_NAMESPACE}/{}.json",
            hex(&artifact_sha256)
        );

        if let Some(entry_index) = self.manifest.entries.iter().position(|entry| {
            entry.account_id == request.account_id && entry.artifact_sha256 == artifact_sha256
        }) {
            persist_immutable(artifacts, &canonical_reference, &bytes)?;
            let image = PortfolioReadImage::try_from_accounts(self.accounts.clone(), self.limits)?;
            let revision_index = self.manifest.entries[..=entry_index]
                .iter()
                .filter(|entry| entry.account_id == request.account_id)
                .count()
                .checked_sub(1)
                .ok_or(PortfolioApplicationServiceError::CorruptPublication)?;
            let revision = image
                .accounts
                .get(&request.account_id)
                .and_then(|history| history.revisions.get(revision_index))
                .ok_or(PortfolioApplicationServiceError::CorruptPublication)?;
            let result =
                import_result(revision, "replay", context, request.requested_maximum_bytes)?;
            return Ok(ImportPublication { image, result });
        }

        match self.accounts.get(&request.account_id) {
            Some(history) if history.revisions.len() >= self.limits.max_history_per_account => {
                return Err(PortfolioApplicationServiceError::ResourceExhausted);
            }
            None if self.accounts.len() >= self.limits.max_accounts => {
                return Err(PortfolioApplicationServiceError::ResourceExhausted);
            }
            _ => {}
        }
        let mut candidate_accounts = self.accounts.clone();
        let published = self.build_revision(
            request.account_id,
            &batch,
            artifact_sha256,
            &candidate_accounts,
        )?;
        let history = candidate_accounts.entry(request.account_id).or_default();
        if history.revisions.len() >= self.limits.max_history_per_account {
            return Err(PortfolioApplicationServiceError::ResourceExhausted);
        }
        history.revisions.push(published);
        let image = PortfolioReadImage::try_from_accounts(candidate_accounts.clone(), self.limits)?;
        let mut candidate_manifest = self.manifest.clone();
        candidate_manifest.entries.push(PublicationEntry {
            account_id: request.account_id,
            artifact_reference: canonical_reference.clone(),
            artifact_sha256,
        });
        let encoded = candidate_manifest.encode()?;
        let revision = image
            .accounts
            .get(&request.account_id)
            .and_then(|history| history.revisions.last())
            .ok_or(PortfolioApplicationServiceError::Publication)?;
        let result = import_result(
            revision,
            "applied",
            context,
            request.requested_maximum_bytes,
        )?;
        ensure_live(runtime, context)?;
        persist_immutable(artifacts, &canonical_reference, &bytes)?;
        ensure_live(runtime, context)?;
        self.publication
            .store(&encoded)
            .map_err(|_| PortfolioApplicationServiceError::Publication)?;
        self.accounts = candidate_accounts;
        self.manifest = candidate_manifest;
        Ok(ImportPublication { image, result })
    }

    fn apply_batch(
        &mut self,
        account_id: AccountId,
        batch: &ExtractionBatch,
        artifact_sha256: [u8; 32],
    ) -> Result<(), PortfolioApplicationServiceError> {
        let current_accounts = self.accounts.clone();
        let published =
            self.build_revision(account_id, batch, artifact_sha256, &current_accounts)?;
        let history = self.accounts.entry(account_id).or_default();
        if history.revisions.len() >= self.limits.max_history_per_account {
            return Err(PortfolioApplicationServiceError::CorruptPublication);
        }
        history.revisions.push(published);
        Ok(())
    }

    fn build_revision(
        &mut self,
        account_id: AccountId,
        batch: &ExtractionBatch,
        artifact_sha256: [u8; 32],
        accounts: &BTreeMap<AccountId, AccountHistory>,
    ) -> Result<PublishedRevision, PortfolioApplicationServiceError> {
        let object = batch.request().object();
        let key = SourceKey {
            source_id: object.source_id().clone(),
            metadata_revision: object
                .metadata_revision()
                .as_source_identifier()
                .as_str()
                .to_owned(),
        };
        let source = match self.sources.entry(key.clone()) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let namespace = source_namespace(&key);
                let store = LocalAuthorityStateStore::try_open(
                    self.control_root
                        .join(RAW_ARCHIVE_NAMESPACE)
                        .join(namespace),
                )
                .map_err(|_| PortfolioApplicationServiceError::Authority)?;
                entry.insert(
                    PortfolioExtractionSource::try_new(
                        key.source_id.clone(),
                        MetadataRevision::new(
                            SourceIdentifier::try_from(key.metadata_revision.clone()).map_err(
                                |_| PortfolioApplicationServiceError::CorruptPublication,
                            )?,
                        ),
                        DataQuality::DirectUnverified,
                        store,
                        None,
                        PortfolioImportLimits::standard(),
                    )
                    .map_err(|_| PortfolioApplicationServiceError::Import)?,
                )
            }
        };
        let imported = source
            .import_batch(batch)
            .map_err(|_| PortfolioApplicationServiceError::Import)?;
        let prior = accounts
            .get(&account_id)
            .and_then(|history| history.revisions.last());
        validate_account_binding(account_id, &imported)?;
        let account = imported
            .accounts()
            .first()
            .cloned()
            .or_else(|| prior.map(|revision| revision.account.clone()))
            .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
        if account.account_id() != account_id {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
        let holdings = merge_holdings(prior, imported.holdings());
        let transactions = merge_transactions(prior, imported.transactions());
        let effective_at = maximum_effective(batch)?;
        let available_at = maximum_conservative_availability(batch);
        let knowledge_at = available_at
            .filter(|available| *available > effective_at)
            .unwrap_or(effective_at);
        let mut source_coverage =
            prior.map_or_else(Vec::new, |revision| revision.source_coverage.clone());
        if !source_coverage.contains(&key.source_id) {
            source_coverage.push(key.source_id.clone());
            source_coverage.sort_unstable();
        }
        let core = build_core_revision(
            prior,
            &account,
            knowledge_at,
            object.source_id(),
            batch,
            artifact_sha256,
            self.portfolio_limits,
            accounts
                .get(&account_id)
                .map_or(1, |history| history.revisions.len().saturating_add(1)),
        )?;
        Ok(PublishedRevision {
            core,
            account,
            holdings,
            transactions,
            discrepancies: imported.discrepancies().to_vec(),
            source_id: object.source_id().clone(),
            source_coverage,
            effective_at,
            available_at,
            artifact_sha256,
        })
    }
}

fn validate_account_binding(
    account_id: AccountId,
    imported: &PortfolioImport,
) -> Result<(), PortfolioApplicationServiceError> {
    if imported.accounts().len() > 1
        || imported
            .accounts()
            .iter()
            .any(|value| value.account_id() != account_id)
        || imported
            .holdings()
            .iter()
            .any(|value| value.account_id() != account_id)
        || imported
            .transactions()
            .iter()
            .any(|value| value.account_id() != account_id)
        || imported
            .supplied_totals()
            .iter()
            .any(|value| value.account_id() != account_id)
    {
        return Err(PortfolioApplicationServiceError::InvalidRequest);
    }
    Ok(())
}

fn merge_holdings(
    prior: Option<&PublishedRevision>,
    imported: &[HoldingObservation],
) -> Vec<HoldingObservation> {
    let mut holdings = prior.map_or_else(Vec::new, |revision| revision.holdings.clone());
    for holding in imported {
        if let Some(index) = holdings.iter().position(|existing| {
            existing.account_id() == holding.account_id()
                && existing.instrument_id() == holding.instrument_id()
        }) {
            holdings[index] = holding.clone();
        } else {
            holdings.push(holding.clone());
        }
    }
    holdings.sort_unstable_by_key(HoldingObservation::instrument_id);
    holdings
}

fn merge_transactions(
    prior: Option<&PublishedRevision>,
    imported: &[PortfolioTransaction],
) -> Vec<PortfolioTransaction> {
    let mut transactions = prior.map_or_else(Vec::new, |revision| revision.transactions.clone());
    for transaction in imported {
        if let Some(index) = transactions.iter().position(|existing| {
            existing.account_id() == transaction.account_id()
                && existing.broker_transaction_id() == transaction.broker_transaction_id()
        }) {
            transactions[index] = transaction.clone();
        } else {
            transactions.push(transaction.clone());
        }
    }
    transactions.sort_unstable_by(|left, right| {
        left.occurred_at().cmp(&right.occurred_at()).then_with(|| {
            left.broker_transaction_id()
                .cmp(right.broker_transaction_id())
        })
    });
    transactions
}

#[allow(
    clippy::too_many_arguments,
    reason = "revision publication binds all source and point-in-time evidence explicitly"
)]
fn build_core_revision(
    prior: Option<&PublishedRevision>,
    account: &AccountObservation,
    as_of: Timestamp,
    source_id: &SourceId,
    batch: &ExtractionBatch,
    artifact_sha256: [u8; 32],
    limits: PortfolioLimits,
    manifest_version: usize,
) -> Result<market_squawk_portfolio::PortfolioRevision, PortfolioApplicationServiceError> {
    let mut ledger = prior
        .map_or_else(
            || PortfolioLedger::try_new(account.account_id(), account.currency(), limits),
            |revision| revision.core.clone().into_ledger(),
        )
        .map_err(|_| PortfolioApplicationServiceError::Publication)?;
    let prior_cash = prior.map_or(Decimal::ZERO, |revision| {
        revision.account.cash_balance().amount()
    });
    let delta = account
        .cash_balance()
        .amount()
        .checked_sub(prior_cash)
        .ok_or(PortfolioApplicationServiceError::Publication)?;
    let mut entries = Vec::new();
    if !delta.is_zero() {
        let kind = if delta.is_sign_positive() {
            CashFlowKind::Deposit
        } else {
            CashFlowKind::Withdrawal
        };
        let transaction_id =
            SourceIdentifier::try_from(format!("account-snapshot-{}", hex(&artifact_sha256)))
                .map_err(|_| PortfolioApplicationServiceError::Publication)?;
        entries.push(
            LedgerEntry::try_new(
                account.account_id(),
                TransactionRevision::try_new(
                    transaction_id,
                    RevisionNumber::new(1)
                        .map_err(|_| PortfolioApplicationServiceError::Publication)?,
                    None,
                )
                .map_err(|_| PortfolioApplicationServiceError::Publication)?,
                account.as_of(),
                account.source_reference().clone(),
                LedgerEntryKind::CashFlow(
                    CashFlow::try_new(kind, Money::new(delta.abs(), account.currency()), None)
                        .map_err(|_| PortfolioApplicationServiceError::Publication)?,
                ),
            )
            .map_err(|_| PortfolioApplicationServiceError::Publication)?,
        );
    }
    let digest = extraction_batch_digest(batch)
        .map_err(|_| PortfolioApplicationServiceError::Publication)?;
    if digest.algorithm() != DigestAlgorithm::Sha256 {
        return Err(PortfolioApplicationServiceError::Publication);
    }
    let dataset = DatasetManifestRef::try_new_with_schema(
        DatasetId::try_from("portfolio-import")
            .map_err(|_| PortfolioApplicationServiceError::Publication)?,
        u64::try_from(manifest_version)?,
        market_squawk_data::DatasetSchemaRegistry::local()
            .canonical_research_observations()
            .map_err(|_| PortfolioApplicationServiceError::Publication)?,
        Sha256Digest::new(digest.bytes()),
    )
    .map_err(|_| PortfolioApplicationServiceError::Publication)?;
    let source = SourceIdentifier::try_from(source_id.as_str())
        .map_err(|_| PortfolioApplicationServiceError::Publication)?;
    let evidence = RevisionEvidence::try_new(
        as_of,
        dataset.clone(),
        Sha256Digest::new(digest.bytes()),
        Sha256Digest::new(artifact_sha256),
        vec![source],
        Vec::new(),
        None,
    )
    .map_err(|_| PortfolioApplicationServiceError::Publication)?;
    let valuation = ValuationSet::try_new(
        account.currency(),
        as_of,
        dataset,
        Sha256Digest::new(digest.bytes()),
        Vec::new(),
        Vec::new(),
        limits,
    )
    .map_err(|_| PortfolioApplicationServiceError::Publication)?;
    ledger
        .try_apply(entries, None, valuation, evidence)
        .map_err(|_| PortfolioApplicationServiceError::Publication)
}

fn portfolio_limits(
    limits: PortfolioApplicationLimits,
) -> Result<PortfolioLimits, PortfolioApplicationServiceError> {
    PortfolioLimits::try_new(PortfolioLimitInput {
        max_accounts: limits.max_accounts,
        max_instruments: limits.max_result_items,
        max_lots: limits.max_result_items,
        max_transactions: limits.max_result_items,
        max_factors: limits.max_result_items.min(16_384),
        max_scenarios: limits.max_result_items.min(16_384),
        max_history: limits.max_history_per_account,
        max_results: limits.max_result_items,
        max_retained_bytes: limits.max_retained_bytes,
    })
    .map_err(|_| PortfolioApplicationServiceError::InvalidLimits)
}

fn maximum_effective(
    batch: &ExtractionBatch,
) -> Result<Timestamp, PortfolioApplicationServiceError> {
    batch
        .records()
        .iter()
        .filter_map(|record| record.effective_time().exact_timestamp())
        .max()
        .ok_or(PortfolioApplicationServiceError::InvalidRequest)
}

fn maximum_conservative_availability(batch: &ExtractionBatch) -> Option<Timestamp> {
    if batch
        .records()
        .iter()
        .any(|record| record.available_at().is_none())
    {
        None
    } else {
        batch
            .records()
            .iter()
            .filter_map(|record| record.available_at())
            .max()
    }
}

fn import_result(
    revision: &PublishedRevision,
    disposition: &str,
    context: &RequestContext,
    requested_maximum_bytes: usize,
) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
    read::mutation_result(
        json!({
            "accountId": revision.account.account_id().to_string(),
            "revisionId": hex(&revision.token().bytes()),
            "disposition": disposition,
            "sourceId": revision.source_id.as_str(),
            "effectiveAtUnixNanos": revision.effective_at.unix_nanos().to_string(),
            "availableAtUnixNanos": revision.available_at.map(|value| value.unix_nanos().to_string()),
            "artifactSha256": hex(&revision.artifact_sha256),
            "rawEvidenceRetained": true,
            "reconciliationDiscrepancies": revision.discrepancies.len()
        }),
        context,
        requested_maximum_bytes,
    )
}

fn source_namespace(key: &SourceKey) -> String {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/portfolio-source-namespace/v1");
    digest.update(key.source_id.as_str().as_bytes());
    digest.update([0]);
    digest.update(key.metadata_revision.as_bytes());
    hex(&digest.finalize().into())
}

fn read_artifact(
    artifacts: &ArtifactRoot,
    reference: &str,
    maximum_bytes: usize,
) -> Result<Vec<u8>, PortfolioApplicationServiceError> {
    if maximum_bytes == 0 {
        return Err(PortfolioApplicationServiceError::InvalidLimits);
    }
    let file = artifacts
        .resolve(reference)
        .map_err(|_| PortfolioApplicationServiceError::InvalidRequest)?
        .open_read()
        .map_err(|_| PortfolioApplicationServiceError::InvalidRequest)?;
    let metadata = file
        .metadata()
        .map_err(|_| PortfolioApplicationServiceError::InvalidRequest)?;
    if usize::try_from(metadata.len()).map_or(true, |bytes| bytes > maximum_bytes) {
        return Err(PortfolioApplicationServiceError::ResourceExhausted);
    }
    let maximum = u64::try_from(maximum_bytes)?
        .checked_add(1)
        .ok_or(PortfolioApplicationServiceError::ResourceExhausted)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(usize::try_from(metadata.len())?)
        .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
    file.take(maximum)
        .read_to_end(&mut bytes)
        .map_err(|_| PortfolioApplicationServiceError::InvalidRequest)?;
    if bytes.len() > maximum_bytes {
        return Err(PortfolioApplicationServiceError::ResourceExhausted);
    }
    Ok(bytes)
}

fn persist_immutable(
    artifacts: &ArtifactRoot,
    reference: &str,
    bytes: &[u8],
) -> Result<(), PortfolioApplicationServiceError> {
    let resolved = artifacts
        .resolve(reference)
        .map_err(|_| PortfolioApplicationServiceError::Publication)?;
    match resolved.create_new() {
        Ok(mut file) => {
            file.write_all(bytes)
                .map_err(|_| PortfolioApplicationServiceError::Publication)?;
            file.sync_all()
                .map_err(|_| PortfolioApplicationServiceError::Publication)?;
            Ok(())
        }
        Err(ArtifactPathError::Io { source })
            if source.kind() == std::io::ErrorKind::AlreadyExists =>
        {
            let existing = read_artifact(artifacts, reference, bytes.len())?;
            if existing == bytes {
                Ok(())
            } else {
                Err(PortfolioApplicationServiceError::CorruptPublication)
            }
        }
        Err(_) => Err(PortfolioApplicationServiceError::Publication),
    }
}

pub(super) fn hex(bytes: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(64);
    for byte in bytes {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    value
}
