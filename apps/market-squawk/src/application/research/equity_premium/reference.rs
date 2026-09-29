//! Bounded canonical replay bytes in the existing valuation macro-assumption receipt.

use super::super::RecommendationBenchmarkSelectionReadCapability;
use super::*;
use market_squawk_data::{DatasetId, DatasetSchemaRef, DatasetSchemaRegistry, Sha256Digest};
use market_squawk_domain::SchemaVersion;
use serde::{Deserialize, Serialize};

pub(crate) const MAXIMUM_EQUITY_PREMIUM_REFERENCE_BYTES: usize = 64 * 1024;
const ENDPOINTS: usize = market_squawk_valuation::EQUITY_PREMIUM_SAMPLE_YEARS + 1;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ReferenceWire {
    version: u16,
    estimator: String,
    equity_source_reference: Vec<u8>,
    benchmark: super::super::RecommendationBenchmarkSelectionReference,
    equity_evidence: EvidenceDigest,
    government_manifest: ManifestWire,
    government_binding_digest: EvidenceDigest,
    government_original_digest: EvidenceDigest,
    knowledge_cutoff: Timestamp,
    economic_origin: Timestamp,
    equity_closing_dates: [CalendarDate; ENDPOINTS],
    government_selection_digests: [EvidenceDigest; ENDPOINTS],
    government_evidence: EvidenceDigest,
    produced_at: Timestamp,
    expires_at: Timestamp,
    evidence_digest: EvidenceDigest,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ManifestWire {
    dataset_id: String,
    manifest_version: u64,
    schema_name: String,
    schema_version: u16,
    schema_fingerprint: [u8; 32],
    content_hash: [u8; 32],
}
impl ManifestWire {
    fn from_manifest(value: &DatasetManifestRef) -> Self {
        Self {
            dataset_id: value.dataset_id().as_str().to_owned(),
            manifest_version: value.manifest_version(),
            schema_name: value.schema().name().to_owned(),
            schema_version: value.schema_version().get(),
            schema_fingerprint: value.schema().fingerprint(),
            content_hash: value.content_hash().bytes(),
        }
    }
    fn into_manifest(self) -> Result<DatasetManifestRef, EquityPremiumUnavailable> {
        let mismatch = EquityPremiumUnavailable::SourceIdentityMismatch;
        if self.content_hash == [0; 32] {
            return Err(mismatch);
        }
        let schema = DatasetSchemaRef::try_new(
            self.schema_name,
            SchemaVersion::new(self.schema_version).map_err(|_| mismatch)?,
            self.schema_fingerprint,
        )
        .map_err(|_| mismatch)?;
        DatasetSchemaRegistry::local()
            .resolve(&schema)
            .map_err(|_| mismatch)?;
        DatasetManifestRef::try_new_with_schema(
            DatasetId::try_from(self.dataset_id.as_str()).map_err(|_| mismatch)?,
            self.manifest_version,
            schema,
            Sha256Digest::new(self.content_hash),
        )
        .map_err(|_| mismatch)
    }
}

impl HistoricalEquityPremiumRead {
    /// This is a reconstruction recipe, not a caller-authored evidence assertion. The canonical
    /// valuation receipt retains these bytes plus their hash beside the actually consumed rate.
    pub(crate) fn canonical_reference_bytes(&self) -> Result<Box<[u8]>, EquityPremiumUnavailable> {
        let government = self.government.reference();
        let wire = ReferenceWire {
            version: 2,
            estimator: EQUITY_PREMIUM_ESTIMATOR.to_owned(),
            equity_source_reference: self
                .equity
                .reference()
                .history()
                .canonical_bytes()
                .map_err(|_| EquityPremiumUnavailable::SourceIdentityMismatch)?
                .into_vec(),
            benchmark: self.equity.reference().benchmark().clone(),
            equity_evidence: self.equity.reference().evidence_digest(),
            government_manifest: ManifestWire::from_manifest(government.manifest()),
            government_binding_digest: government.binding_digest(),
            government_original_digest: government.original_digest(),
            knowledge_cutoff: government.knowledge_cutoff(),
            economic_origin: government.economic_origin(),
            equity_closing_dates: *government.equity_closing_dates(),
            government_selection_digests: *government.selection_digests(),
            government_evidence: government.evidence_digest(),
            produced_at: self.reference.produced_at,
            expires_at: self.reference.expires_at,
            evidence_digest: self.reference.evidence_digest,
        };
        let bytes = serde_json::to_vec(&wire)
            .map_err(|_| EquityPremiumUnavailable::SourceIdentityMismatch)?;
        if bytes.len() > MAXIMUM_EQUITY_PREMIUM_REFERENCE_BYTES {
            return Err(EquityPremiumUnavailable::SourceIdentityMismatch);
        }
        Ok(bytes.into_boxed_slice())
    }
}

impl MacroContextReadCapability {
    /// Fresh source replay through existing history, benchmark catalog and canonical Macro
    /// readers. Serialized bytes never construct a serving premium. Current LocalAnalysis
    /// authorization of every returned parent remains the final valuation owner's obligation.
    #[allow(
        clippy::too_many_arguments,
        reason = "distinct existing source capabilities retain their own authorities"
    )]
    pub(crate) async fn read_equity_premium_reference_bytes(
        &self,
        bytes: &[u8],
        research: &crate::ResearchService,
        calendars: &crate::application::market_calendar::CompletedMarketSessionReadCapability,
        benchmarks: &RecommendationBenchmarkSelectionReadCapability,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<HistoricalEquityPremiumRead, EquityPremiumReadError> {
        let mismatch = EquityPremiumUnavailable::SourceIdentityMismatch;
        if bytes.is_empty() || bytes.len() > MAXIMUM_EQUITY_PREMIUM_REFERENCE_BYTES {
            return Err(mismatch.into());
        }
        let wire: ReferenceWire = serde_json::from_slice(bytes).map_err(|_| mismatch)?;
        if wire.version != 2
            || wire.estimator != EQUITY_PREMIUM_ESTIMATOR
            || serde_json::to_vec(&wire).map_err(|_| mismatch)?.as_slice() != bytes
        {
            return Err(mismatch.into());
        }
        if current_timestamp()? >= wire.expires_at {
            return Err(EquityPremiumUnavailable::Expired.into());
        }
        let history = research
            .read_tiingo_eod_history_reference_bytes(
                &wire.equity_source_reference,
                deadline,
                &cancellation,
            )
            .await?;
        let source = selection::rejoin_source_with_original_calendar(
            research,
            calendars,
            history,
            deadline,
            &cancellation,
        )
        .await?;
        let calendar_parent = self
            .read_equity_history_calendar_parent(research, &source, deadline, &cancellation)
            .await?;
        let benchmarks = benchmarks.clone();
        let benchmark_reference = wire.benchmark.clone();
        let selected = research
            .run_owned_research_io(deadline, &cancellation, move |worker_cancellation| {
                benchmarks.read_reference(&benchmark_reference, deadline, &worker_cancellation)
            })
            .await?;
        selection::check_selection_control(deadline, &cancellation)?;
        let benchmark = selected?.ok_or(mismatch)?;
        let equity = AnnualEquityCashReturnRead::from_source(source, benchmark)?;
        if equity.reference().evidence_digest() != wire.equity_evidence
            || equity.source().knowledge_cutoff() != wire.knowledge_cutoff
            || equity.economic_origin() != wire.economic_origin
            || equity.closing_dates() != &wire.equity_closing_dates
        {
            return Err(mismatch.into());
        }
        let government_reference = AnnualGovernmentYieldReference::from_retained(
            wire.government_manifest.into_manifest()?,
            wire.government_binding_digest,
            wire.government_original_digest,
            &equity,
            wire.economic_origin,
            wire.government_selection_digests,
            wire.government_evidence,
        )?;
        let government = self
            .read_annual_government_yield_reference(
                research,
                &government_reference,
                &equity,
                deadline,
                cancellation,
            )
            .await
            .map_err(map_government_error)?;
        let original = HistoricalEquityPremiumReference {
            equity: equity.reference().clone(),
            government: government_reference,
            produced_at: wire.produced_at,
            expires_at: wire.expires_at,
            evidence_digest: wire.evidence_digest,
        };
        let read = HistoricalEquityPremiumRead::from_sources(
            equity,
            government,
            calendar_parent,
            Some(&original),
        )?;
        if read.canonical_reference_bytes()?.as_ref() != bytes {
            return Err(mismatch.into());
        }
        Ok(read)
    }
}
