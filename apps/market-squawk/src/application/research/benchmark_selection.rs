//! Predeclared benchmark identities selected before any study outcomes are read.
//!
//! CUSIPs below identify the owner's fixed SPY primary / VTI accompanying policy. They are search
//! coordinates, never canonical IDs or assignment authority. Only the existing normalized catalog
//! can establish their source-backed assignment, labels and immutable definition revisions.

use std::time::{Instant, SystemTime, UNIX_EPOCH};

use market_squawk_data::{
    MarketDataInstrumentMatchKind, MarketDataInstrumentPopulationDisposition,
    MarketDataInstrumentPopulationQuery, MarketDataInstrumentPopulationSelection,
    MarketDataInstrumentReadCapability, MarketDataInstrumentRecord, Sha256Digest,
};
use market_squawk_domain::{
    AssetClass, AssignmentVerification, EffectiveInterval, EvidenceDigest, ExternalIdentifier,
    ExternalIdentifierRecord, IdentifierEntitlement, InstrumentId, Timestamp,
};
use market_squawk_services::ServiceError;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use super::map_market_definition_read_error;

const OWNER_POLICY: &[u8] = b"market-squawk/recommendation-benchmark-owner-policy/v1\0";
const SELECTION_DOMAIN: &[u8] = b"market-squawk/recommendation-benchmark-selection/v1\0";
const REFERENCE_VERSION: u16 = 1;

// Official identity anchors, not normalized source evidence:
// https://www.ssga.com/us/en/individual/etfs/state-street-spdr-sp-500-etf-trust-spy
// https://workplace.vanguard.com/assets/corp/fund_communications/pdf_publish/us-products/investment-profiles/0970.pdf
const PRIMARY: BenchmarkPolicy = BenchmarkPolicy {
    role: 1,
    cusip: "78462F103",
    symbol: "SPY",
};
const ACCOMPANYING: BenchmarkPolicy = BenchmarkPolicy {
    role: 2,
    cusip: "922908769",
    symbol: "VTI",
};

#[derive(Clone, Copy)]
struct BenchmarkPolicy {
    role: u8,
    cusip: &'static str,
    symbol: &'static str,
}

/// Bounded restart recipe. Deserialization establishes no selection or source-use authority.
/// Labels are committed by the exact definition digests and projected only after catalog reopening.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecommendationBenchmarkSelectionReference {
    version: u16,
    knowledge_at: Timestamp,
    effective_at: Timestamp,
    selected_at: Timestamp,
    primary: BenchmarkReference,
    accompanying: BenchmarkReference,
    population_receipt_digest: EvidenceDigest,
    selection_digest: [u8; 32],
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct BenchmarkReference {
    instrument_id: InstrumentId,
    revision_digest: EvidenceDigest,
    revision_sequence: u32,
    published_at: Timestamp,
}

impl BenchmarkReference {
    fn from_record(record: &MarketDataInstrumentRecord) -> Self {
        Self {
            instrument_id: record.definition().instrument_id(),
            revision_digest: record.revision_digest(),
            revision_sequence: record.revision_sequence(),
            published_at: record.published_at(),
        }
    }
}

/// A source-attested member of the opaque pair. There is no caller-authored label constructor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SelectedRecommendationBenchmark {
    reference: BenchmarkReference,
    display_symbol: Box<str>,
    display_name: Box<str>,
    approval_digest: Sha256Digest,
}

impl SelectedRecommendationBenchmark {
    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.reference.instrument_id
    }
    pub(crate) fn display_symbol(&self) -> &str {
        &self.display_symbol
    }
    pub(crate) fn display_name(&self) -> &str {
        &self.display_name
    }
    pub(crate) const fn reference_revision_digest(&self) -> EvidenceDigest {
        self.reference.revision_digest
    }
    /// Approval of this exact benchmark identity under the predeclared owner policy, not a data
    /// license or authorization to execute orders.
    pub(crate) const fn approval_digest(&self) -> Sha256Digest {
        self.approval_digest
    }
}

/// Exact single comparison selected independently of probability-forecast availability.
/// An inert recipe; current source-use authority must be re-established on read.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SavedBenchmarkSelectionReference {
    version: u16,
    #[serde(deserialize_with = "Option::deserialize")]
    explicit: Option<InstrumentId>,
    knowledge_at: Timestamp,
    effective_at: Timestamp,
    reference: BenchmarkReference,
    label: Box<str>,
    approval_digest: [u8; 32],
}

impl SavedBenchmarkSelectionReference {
    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.reference.instrument_id
    }
    pub(crate) fn label(&self) -> &str {
        &self.label
    }
    pub(crate) const fn explicit(&self) -> Option<InstrumentId> {
        self.explicit
    }
    pub(crate) fn validate_at(&self, cutoff: Timestamp) -> Result<(), ServiceError> {
        if self.version != REFERENCE_VERSION
            || self.knowledge_at != cutoff
            || self.effective_at != cutoff
            || self.reference.published_at > cutoff
            || self
                .explicit
                .is_some_and(|value| value != self.instrument_id())
            || self.label.is_empty()
            || self.label.len() > 128
            || self.label.chars().any(char::is_control)
            || self.approval_digest == [0; 32]
        {
            return Err(ServiceError::InvalidResult);
        }
        Ok(())
    }
}

/// Both roles and their complete catalog proof retained before training or outcome selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RecommendationBenchmarkSelection {
    primary: SelectedRecommendationBenchmark,
    accompanying: SelectedRecommendationBenchmark,
    reference: RecommendationBenchmarkSelectionReference,
    population: MarketDataInstrumentPopulationSelection,
    validated_at: Timestamp,
}

impl RecommendationBenchmarkSelection {
    /// Borrows the same immutable source definition already admitted by this opaque pair.
    /// This does not look up a new symbol, replace the original revision or confer execution rights.
    pub(crate) fn source_definition(
        &self,
        selected: &SelectedRecommendationBenchmark,
    ) -> Option<&MarketDataInstrumentRecord> {
        self.population.records().iter().find(|record| {
            record.definition().instrument_id() == selected.instrument_id()
                && record.revision_digest() == selected.reference_revision_digest()
                && record.revision_sequence() == selected.reference.revision_sequence
                && record.published_at() == selected.reference.published_at
        })
    }

    pub(crate) const fn primary(&self) -> &SelectedRecommendationBenchmark {
        &self.primary
    }
    pub(crate) const fn accompanying(&self) -> &SelectedRecommendationBenchmark {
        &self.accompanying
    }
    pub(crate) const fn reference(&self) -> &RecommendationBenchmarkSelectionReference {
        &self.reference
    }
    pub(crate) const fn selection_digest(&self) -> Sha256Digest {
        Sha256Digest::new(self.reference.selection_digest)
    }
    pub(crate) const fn knowledge_at(&self) -> Timestamp {
        self.reference.knowledge_at
    }
    pub(crate) const fn effective_at(&self) -> Timestamp {
        self.reference.effective_at
    }
    pub(crate) const fn selected_at(&self) -> Timestamp {
        self.reference.selected_at
    }
    /// Actual time the catalog proof was most recently read, separate from original selection.
    pub(crate) const fn validated_at(&self) -> Timestamp {
        self.validated_at
    }
}

/// Choices display only canonical catalog identity; no symbol is a canonical instrument ID.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BenchmarkComparisonChoice {
    pub(crate) instrument_id: InstrumentId,
    pub(crate) display_name: String,
    pub(crate) symbol: String,
    pub(crate) comparison_description: String,
    pub(crate) is_default: bool,
}

/// Reads two fixed identities through the existing catalog, without provider sessions or a second
/// selection registry. Missing, ambiguous or inadmissible normalized identity evidence is absent.
#[derive(Clone, Debug)]
pub(crate) struct RecommendationBenchmarkSelectionReadCapability {
    instruments: MarketDataInstrumentReadCapability,
}

impl RecommendationBenchmarkSelectionReadCapability {
    pub(crate) const fn new(instruments: MarketDataInstrumentReadCapability) -> Self {
        Self { instruments }
    }

    pub(crate) fn select(
        &self,
        knowledge_at: Timestamp,
        effective_at: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<RecommendationBenchmarkSelection>, ServiceError> {
        self.select_at(knowledge_at, effective_at, None, deadline, cancellation)
    }

    /// Reproduces the original bounded query and complete immutable selection. Newer catalog
    /// definitions cannot replace either member. The recipe itself is never an admitted receipt.
    pub(crate) fn read_reference(
        &self,
        reference: &RecommendationBenchmarkSelectionReference,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<RecommendationBenchmarkSelection>, ServiceError> {
        if reference.version != REFERENCE_VERSION {
            return Err(ServiceError::InvalidRequest);
        }
        let selected = self.select_at(
            reference.knowledge_at,
            reference.effective_at,
            Some(reference.selected_at),
            deadline,
            cancellation,
        )?;
        match selected {
            Some(selected) if selected.reference == *reference => Ok(Some(selected)),
            Some(_) => Err(ServiceError::InvalidRequest),
            None => Ok(None),
        }
    }

    fn select_at(
        &self,
        knowledge_at: Timestamp,
        effective_at: Timestamp,
        original_selected_at: Option<Timestamp>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<RecommendationBenchmarkSelection>, ServiceError> {
        check_operation(deadline, cancellation)?;
        let started_at = current_time()?;
        if effective_at > knowledge_at
            || knowledge_at > started_at
            || original_selected_at.is_some_and(|at| at < knowledge_at || at > started_at)
        {
            return Err(ServiceError::InvalidRequest);
        }
        let Some(primary_record) =
            self.discover(PRIMARY, knowledge_at, effective_at, deadline, cancellation)?
        else {
            return Ok(None);
        };
        let Some(accompanying_record) = self.discover(
            ACCOMPANYING,
            knowledge_at,
            effective_at,
            deadline,
            cancellation,
        )?
        else {
            return Ok(None);
        };
        if primary_record.instrument_id == accompanying_record.instrument_id {
            return Ok(None);
        }
        // Discovery only identifies candidates. The final pair is one atomic catalog population.
        let query = MarketDataInstrumentPopulationQuery::try_new(
            vec![
                primary_record.instrument_id,
                accompanying_record.instrument_id,
            ],
            knowledge_at,
            effective_at,
        )
        .map_err(map_market_definition_read_error)?;
        let population = self
            .instruments
            .pin_population_as_of(query, deadline, cancellation)
            .map_err(map_market_definition_read_error)?;
        if population.disposition() != MarketDataInstrumentPopulationDisposition::Complete
            || !population.exclusions().is_empty()
            || population.records().len() != 2
        {
            return Ok(None);
        }
        let primary_record = population
            .records()
            .iter()
            .find(|record| BenchmarkReference::from_record(record) == primary_record);
        let accompanying_record = population
            .records()
            .iter()
            .find(|record| BenchmarkReference::from_record(record) == accompanying_record);
        let (Some(primary_record), Some(accompanying_record)) =
            (primary_record, accompanying_record)
        else {
            return Ok(None);
        };
        let primary = project_member(PRIMARY, primary_record, knowledge_at, effective_at)?;
        let accompanying = project_member(
            ACCOMPANYING,
            accompanying_record,
            knowledge_at,
            effective_at,
        )?;
        let (Some(primary), Some(accompanying)) = (primary, accompanying) else {
            return Ok(None);
        };
        check_operation(deadline, cancellation)?;
        let validated_at = current_time()?;
        if validated_at < started_at {
            return Err(ServiceError::Unavailable);
        }
        let selected_at = original_selected_at.unwrap_or(validated_at);
        let mut reference = RecommendationBenchmarkSelectionReference {
            version: REFERENCE_VERSION,
            knowledge_at,
            effective_at,
            selected_at,
            primary: primary.reference.clone(),
            accompanying: accompanying.reference.clone(),
            population_receipt_digest: population.receipt_digest(),
            selection_digest: [0; 32],
        };
        reference.selection_digest = selection_digest(&reference, &primary, &accompanying);
        Ok(Some(RecommendationBenchmarkSelection {
            primary,
            accompanying,
            reference,
            population,
            validated_at,
        }))
    }

    /// Bounded discovery coordinates only. Every displayed choice is independently re-pinned
    /// through original canonical assignments. QQQ is displayed under its actual source name,
    /// never as a separately invented Nasdaq index.
    pub(crate) fn comparison_choices(&self, knowledge_at: Timestamp, effective_at: Timestamp,
        deadline: Instant, cancellation: &CancellationToken,
    ) -> Result<Vec<BenchmarkComparisonChoice>, ServiceError> {
        let mut choices=Vec::with_capacity(3);
        for (coordinate,is_default) in [(PRIMARY.cusip,true),(ACCOMPANYING.cusip,false),("QQQ",false)] {
            check_operation(deadline,cancellation)?;
            let candidates=self.instruments.resolve_exact_as_of(coordinate,knowledge_at,effective_at,deadline,cancellation)
                .map_err(map_market_definition_read_error)?;
            let [candidate]=candidates.matches() else {continue};
            if candidates.has_more() || candidate.matched_value()!=coordinate {continue}
            let Some(selected)=self.select_comparison(Some(candidate.record().definition().instrument_id()),knowledge_at,effective_at,deadline,cancellation)? else {continue};
            if is_default && !admitted_identity(PRIMARY,candidate.record(),knowledge_at,effective_at) {continue}
            if coordinate==ACCOMPANYING.cusip && !admitted_identity(ACCOMPANYING,candidate.record(),knowledge_at,effective_at) {continue}
            if coordinate=="QQQ" && (candidate.match_kind()!=MarketDataInstrumentMatchKind::ExternalIdentifier
                || !candidate.record().definition().identifiers().iter().any(|identifier|
                    matches!(identifier.identifier(),ExternalIdentifier::Ticker(ticker) if ticker.as_str()=="QQQ")
                    && admitted_identifier(identifier,knowledge_at,effective_at))) {continue}
            let kind=match candidate.record().definition().asset_class(){AssetClass::Index=>"Index",AssetClass::Fund=>"Fund",AssetClass::Equity=>"Equity",_=>continue};
            if choices.iter().any(|value:&BenchmarkComparisonChoice|value.instrument_id==selected.instrument_id()){continue}
            choices.push(BenchmarkComparisonChoice{instrument_id:selected.instrument_id(),display_name:selected.display_name().to_owned(),
                symbol:selected.display_symbol().to_owned(),comparison_description:format!("{} · {}",selected.display_name(),kind),is_default});
        }
        Ok(choices)
    }

    /// Selects one explicit canonical instrument, or the admitted SPY default. An explicit
    /// missing identity never falls back. Population pinning supplies current source-use authority.
    pub(crate) fn select_comparison(
        &self, explicit: Option<InstrumentId>, knowledge_at: Timestamp, effective_at: Timestamp,
        deadline: Instant, cancellation: &CancellationToken,
    ) -> Result<Option<SelectedRecommendationBenchmark>, ServiceError> {
        check_operation(deadline, cancellation)?;
        if effective_at > knowledge_at || knowledge_at > current_time()? {
            return Err(ServiceError::InvalidRequest);
        }
        let instrument = match explicit {
            Some(value) => value,
            None => match self.discover(PRIMARY, knowledge_at, effective_at, deadline, cancellation)? {
                Some(value) => value.instrument_id,
                None => return Ok(None),
            },
        };
        let query = MarketDataInstrumentPopulationQuery::try_new(vec![instrument], knowledge_at, effective_at)
            .map_err(map_market_definition_read_error)?;
        let population = self.instruments.pin_population_as_of(query, deadline, cancellation)
            .map_err(map_market_definition_read_error)?;
        if population.disposition() != MarketDataInstrumentPopulationDisposition::Complete
            || !population.exclusions().is_empty() { return Ok(None); }
        let [record] = population.records() else { return Ok(None); };
        if record.definition().instrument_id() != instrument { return Err(ServiceError::InvalidResult); }
        if explicit.is_none() { return project_member(PRIMARY, record, knowledge_at, effective_at); }
        let definition = record.definition();
        if record.published_at() > knowledge_at || !contains(definition.effective_interval(), effective_at)
            || !matches!(definition.asset_class(), AssetClass::Equity | AssetClass::Fund | AssetClass::Index) {
            return Ok(None);
        }
        let Some(name) = definition.display_name().filter(|value|
            value.rights_policy().entitlement() != IdentifierEntitlement::UnknownOrRestricted) else { return Ok(None); };
        let Some(symbol) = definition.identifiers().iter().find_map(|identifier| match identifier.identifier() {
            ExternalIdentifier::Ticker(value) if admitted_identifier(identifier, knowledge_at, effective_at) => Some(value.as_str()),
            _ => None,
        }) else { return Ok(None); };
        let reference = BenchmarkReference::from_record(record);
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/explicit-canonical-benchmark/v1\0");
        hash_reference(&mut hash, &reference);
        hash.update(population.receipt_digest().bytes());
        hash.update(knowledge_at.unix_nanos().to_be_bytes());
        hash.update(effective_at.unix_nanos().to_be_bytes());
        check_operation(deadline, cancellation)?;
        Ok(Some(SelectedRecommendationBenchmark { reference, display_symbol: copy_text(symbol)?,
            display_name: copy_text(name.as_str())?, approval_digest: Sha256Digest::new(hash.finalize().into()) }))
    }

    /// Captures the actual original choice, including the default-selection mode.
    pub(crate) fn select_saved_comparison(
        &self,
        explicit: Option<InstrumentId>,
        cutoff: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<SavedBenchmarkSelectionReference>, ServiceError> {
        let Some(selected) =
            self.select_comparison(explicit, cutoff, cutoff, deadline, cancellation)?
        else {
            return Ok(None);
        };
        let reference = SavedBenchmarkSelectionReference {
            version: REFERENCE_VERSION,
            explicit,
            knowledge_at: cutoff,
            effective_at: cutoff,
            reference: selected.reference.clone(),
            label: copy_text(selected.display_symbol())?,
            approval_digest: selected.approval_digest().bytes(),
        };
        reference.validate_at(cutoff)?;
        Ok(Some(reference))
    }

    /// The VTI identity is admitted independently; a missing default SPY cannot hide it.
    pub(crate) fn select_saved_accompanying(
        &self,
        cutoff: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<SavedBenchmarkSelectionReference>, ServiceError> {
        let Some(member) = self.discover(ACCOMPANYING, cutoff, cutoff, deadline, cancellation)?
        else {
            return Ok(None);
        };
        self.select_saved_comparison(Some(member.instrument_id), cutoff, deadline, cancellation)
    }

    /// Revalidates original catalog proof; changed or later definitions cannot replace it.
    pub(crate) fn read_saved_comparison(
        &self,
        saved: &SavedBenchmarkSelectionReference,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<bool, ServiceError> {
        saved.validate_at(saved.knowledge_at)?;
        match self.select_saved_comparison(
            saved.explicit,
            saved.knowledge_at,
            deadline,
            cancellation,
        )? {
            Some(actual) if actual == *saved => Ok(true),
            Some(_) => Err(ServiceError::InvalidResult),
            None => Ok(false),
        }
    }

    fn discover(
        &self,
        policy: BenchmarkPolicy,
        knowledge_at: Timestamp,
        effective_at: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<BenchmarkReference>, ServiceError> {
        let candidates = self
            .instruments
            .resolve_exact_as_of(
                policy.cusip,
                knowledge_at,
                effective_at,
                deadline,
                cancellation,
            )
            .map_err(map_market_definition_read_error)?;
        if candidates.knowledge_at() != Some(knowledge_at)
            || candidates.effective_at() != Some(effective_at)
        {
            return Err(ServiceError::InvalidResult);
        }
        let [candidate] = candidates.matches() else {
            return Ok(None);
        };
        if candidates.has_more()
            || candidate.match_kind() != MarketDataInstrumentMatchKind::ExternalIdentifier
            || candidate.matched_value() != policy.cusip
            || !admitted_identity(policy, candidate.record(), knowledge_at, effective_at)
        {
            return Ok(None);
        }
        Ok(Some(BenchmarkReference::from_record(candidate.record())))
    }
}

fn admitted_identity(
    policy: BenchmarkPolicy,
    record: &MarketDataInstrumentRecord,
    knowledge_at: Timestamp,
    effective_at: Timestamp,
) -> bool {
    let definition = record.definition();
    record.published_at() <= knowledge_at
        && contains(definition.effective_interval(), effective_at)
        && definition.asset_class() == AssetClass::Fund
        && definition.quote_currency().as_str() == "USD"
        && definition.identifiers().iter().any(|identifier| {
            matches!(identifier.identifier(), ExternalIdentifier::Cusip(cusip) if cusip.as_str() == policy.cusip)
                && admitted_identifier(identifier, knowledge_at, effective_at)
        })
}

fn admitted_identifier(
    identifier: &ExternalIdentifierRecord,
    knowledge_at: Timestamp,
    effective_at: Timestamp,
) -> bool {
    identifier.assignment_verification() == AssignmentVerification::VerifiedAssigned
        && identifier.rights_policy().entitlement() != IdentifierEntitlement::UnknownOrRestricted
        && identifier.observed_at() <= knowledge_at
        && identifier
            .source_timestamp()
            .is_none_or(|at| at <= knowledge_at)
        && contains(identifier.validity(), effective_at)
}

fn project_member(
    policy: BenchmarkPolicy,
    record: &MarketDataInstrumentRecord,
    knowledge_at: Timestamp,
    effective_at: Timestamp,
) -> Result<Option<SelectedRecommendationBenchmark>, ServiceError> {
    if !admitted_identity(policy, record, knowledge_at, effective_at) {
        return Ok(None);
    }
    let definition = record.definition();
    let Some(name) = definition.display_name() else {
        return Ok(None);
    };
    if name.rights_policy().entitlement() == IdentifierEntitlement::UnknownOrRestricted {
        return Ok(None);
    }
    // The returned text is copied from a source-backed field, never from the policy constant.
    let symbol = definition
        .identifiers()
        .iter()
        .find_map(|identifier| match identifier.identifier() {
            ExternalIdentifier::Ticker(ticker)
                if ticker.as_str() == policy.symbol
                    && admitted_identifier(identifier, knowledge_at, effective_at) =>
            {
                Some(ticker.as_str())
            }
            _ => None,
        })
        .or_else(|| {
            definition.venue_mappings().iter().find_map(|mapping| {
                (mapping.venue_symbol().as_str() == policy.symbol)
                    .then(|| mapping.venue_symbol().as_str())
            })
        });
    let Some(symbol) = symbol else {
        return Ok(None);
    };
    let reference = BenchmarkReference::from_record(record);
    let mut hash = Sha256::new();
    hash.update(OWNER_POLICY);
    hash.update([policy.role]);
    hash.update(policy.cusip.as_bytes());
    hash_text(&mut hash, policy.symbol);
    hash_reference(&mut hash, &reference);
    Ok(Some(SelectedRecommendationBenchmark {
        reference,
        display_symbol: copy_text(symbol)?,
        display_name: copy_text(name.as_str())?,
        approval_digest: Sha256Digest::new(hash.finalize().into()),
    }))
}

fn selection_digest(
    reference: &RecommendationBenchmarkSelectionReference,
    primary: &SelectedRecommendationBenchmark,
    accompanying: &SelectedRecommendationBenchmark,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(SELECTION_DOMAIN);
    hash.update(reference.version.to_be_bytes());
    hash.update(reference.knowledge_at.unix_nanos().to_be_bytes());
    hash.update(reference.effective_at.unix_nanos().to_be_bytes());
    hash.update(reference.selected_at.unix_nanos().to_be_bytes());
    hash.update(reference.population_receipt_digest.bytes());
    for member in [primary, accompanying] {
        hash_reference(&mut hash, &member.reference);
        hash.update(member.approval_digest.bytes());
        hash_text(&mut hash, member.display_symbol());
        hash_text(&mut hash, member.display_name());
    }
    hash.finalize().into()
}

fn hash_reference(hash: &mut Sha256, reference: &BenchmarkReference) {
    hash.update(reference.instrument_id.as_uuid().as_bytes());
    hash.update(reference.revision_digest.bytes());
    hash.update(reference.revision_sequence.to_be_bytes());
    hash.update(reference.published_at.unix_nanos().to_be_bytes());
}

fn hash_text(hash: &mut Sha256, text: &str) {
    hash.update((text.len() as u64).to_be_bytes());
    hash.update(text.as_bytes());
}

fn copy_text(text: &str) -> Result<Box<str>, ServiceError> {
    let mut copy = String::new();
    copy.try_reserve_exact(text.len())
        .map_err(|_| ServiceError::ResourceExhausted)?;
    copy.push_str(text);
    Ok(copy.into_boxed_str())
}

fn contains(interval: EffectiveInterval, at: Timestamp) -> bool {
    interval.starts_at() <= at && interval.ends_at().is_none_or(|end| at < end)
}

fn current_time() -> Result<Timestamp, ServiceError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ServiceError::Unavailable)?;
    Ok(Timestamp::from_unix_nanos(
        i64::try_from(elapsed.as_nanos()).map_err(|_| ServiceError::Unavailable)?,
    ))
}

fn check_operation(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ServiceError> {
    if cancellation.is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}
