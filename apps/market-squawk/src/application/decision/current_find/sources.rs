//! Bounded original-source reference pages in the existing immutable decision journal.
use super::*;
use std::sync::Arc;
use market_squawk_domain::InstrumentId;
use crate::application::research::corporate_actions::SourceAppliedCorporateActionPlanReference;

const MAXIMUM_SOURCE_PAGE_BYTES: usize = 256 * 1024;
pub(super) const MAXIMUM_SOURCE_PAGES: usize = market_squawk_data::MAX_CURRENT_LISTED_POPULATION_MEMBERS.div_ceil(32);

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CurrentFindSourcePageReference {
    sha256: [u8; 32],
    instrument_ids_sha256: [u8; 32],
    knowledge_cutoff: Timestamp,
    first_instrument: InstrumentId,
    last_instrument: InstrumentId,
    instrument_count: usize,
}
impl CurrentFindSourcePageReference {
    fn validate(&self) -> Result<(), DecisionApplicationError> {
        if self.sha256 == [0; 32] || self.instrument_ids_sha256 == [0; 32] || self.knowledge_cutoff.unix_nanos() <= 0
            || !(1..=32).contains(&self.instrument_count) || self.first_instrument > self.last_instrument
            || (self.instrument_count == 1) != (self.first_instrument == self.last_instrument)
        { return Err(invalid()); }
        Ok(())
    }
    pub(crate) fn intersects(&self, ids: &[InstrumentId]) -> bool {
        ids.first().zip(ids.last()).is_some_and(|(first,last)| *first <= self.last_instrument && *last >= self.first_instrument)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CurrentFindSourcePageRecord {
    original: SourceAppliedCorporateActionPlanReference,
    training: Box<[CurrentFindTrainingSource]>,
}

/// One original requested comparison and genuine source selection per current population member.
/// An absent plan records unavailable history; replay never acquires a replacement generation.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CurrentFindTrainingSource {
    subject: InstrumentId,
    requested_benchmark: Option<InstrumentId>,
    selected_benchmark: Option<InstrumentId>,
    original: Option<SourceAppliedCorporateActionPlanReference>,
}
impl CurrentFindSourcePageRecord {
    pub(super) fn validate(&self) -> Result<(), DecisionApplicationError> {
        if serde_json::to_vec(self).map_err(|_| invalid())?.len() > MAXIMUM_SOURCE_PAGE_BYTES {
            return Err(DecisionApplicationError::Capacity);
        }
        let ids = self.original.requested_instruments();
        if !ids.iter().copied().eq(self.training.iter().map(|entry| entry.subject)) {
            return Err(invalid());
        }
        let benchmark = self.training.first().map(|entry| (entry.requested_benchmark, entry.selected_benchmark));
        for entry in &self.training {
            if Some((entry.requested_benchmark, entry.selected_benchmark)) != benchmark
                || entry.requested_benchmark.is_some_and(|requested| entry.selected_benchmark.is_some_and(|selected| selected != requested)) {
                return Err(invalid());
            }
            if let Some(original) = &entry.original {
                let instruments = original.requested_instruments();
                if original.knowledge_cutoff() != self.original.knowledge_cutoff()
                    || instruments.is_empty() || instruments.len() > 2
                    || instruments.windows(2).any(|pair| pair[0] >= pair[1])
                    || !instruments.contains(&entry.subject)
                    || instruments.iter().any(|id| *id != entry.subject && Some(*id) != entry.selected_benchmark) {
                    return Err(invalid());
                }
            }
        }
        self.reference()?.validate()
    }
    pub(super) fn reference(&self) -> Result<CurrentFindSourcePageReference, DecisionApplicationError> {
        let ids = self.original.requested_instruments();
        Ok(CurrentFindSourcePageReference {
            sha256: digest(self)?, instrument_ids_sha256: digest(&ids)?, knowledge_cutoff: self.original.knowledge_cutoff(),
            first_instrument: *ids.first().ok_or_else(invalid)?, last_instrument: *ids.last().ok_or_else(invalid)?,
            instrument_count: ids.len(),
        })
    }
    pub(super) fn key(&self) -> String {
        // Serializing the bounded original record is infallible for admitted records. Invalid
        // inputs receive a non-source key and are rejected by validate before persistence.
        self.reference().map(|reference| key(&reference)).unwrap_or_else(|_| "current-find:source:invalid".to_owned())
    }
}

/// Compact reader handle. It contains no histories or original source-reference bodies.
#[derive(Clone)]
pub(crate) struct RetainedCurrentFindSources {
    decisions: Arc<DecisionApplication>,
    pages: Arc<[CurrentFindSourcePageReference]>,
    cutoff: Timestamp,
}
impl std::fmt::Debug for RetainedCurrentFindSources {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("RetainedCurrentFindSources").field("page_count", &self.pages.len()).finish_non_exhaustive()
    }
}
impl RetainedCurrentFindSources {
    pub(crate) fn pages(&self) -> &[CurrentFindSourcePageReference] { &self.pages }
    pub(crate) fn len(&self) -> usize { self.pages.len() }
    pub(crate) fn read(&self, index: usize) -> Result<SourceAppliedCorporateActionPlanReference, DecisionApplicationError> {
        let reference = self.pages.get(index).ok_or_else(invalid)?;
        if reference.knowledge_cutoff != self.cutoff { return Err(invalid()); }
        self.decisions.current_find_source_page(reference)
    }
    pub(crate) fn read_training(
        &self, index: usize, subject: InstrumentId, requested_benchmark: Option<InstrumentId>,
    ) -> Result<Option<(SourceAppliedCorporateActionPlanReference, Option<InstrumentId>)>, DecisionApplicationError> {
        let reference = self.pages.get(index).ok_or_else(invalid)?;
        if reference.knowledge_cutoff != self.cutoff { return Err(invalid()); }
        let record = self.decisions.current_find_source_page_record(reference)?;
        let entry = record.training.iter().find(|entry| entry.subject == subject).ok_or_else(invalid)?;
        if entry.requested_benchmark != requested_benchmark { return Err(invalid()); }
        // Reopening keeps the actual original comparison, including its absence. A default
        // request is not permission to select another benchmark after source acquisition.
        Ok(entry.original.clone().map(|original| (original, entry.selected_benchmark)))
    }
    /// Verify the complete reopened canonical selection against each original source page's
    /// committed exact IDs without loading every history reference on each partition request.
    pub(crate) fn matches_instruments(&self, ids: &[InstrumentId]) -> Result<bool, DecisionApplicationError> {
        if validate_references(&self.pages, self.cutoff)? != ids.len()
            || ids.windows(2).any(|pair| pair[0] >= pair[1]) { return Ok(false); }
        let mut start = 0usize;
        for page in self.pages.iter() {
            let end = start.checked_add(page.instrument_count).ok_or_else(invalid)?;
            let actual = ids.get(start..end).ok_or_else(invalid)?;
            if actual.first() != Some(&page.first_instrument) || actual.last() != Some(&page.last_instrument)
                || digest(&actual)? != page.instrument_ids_sha256 { return Ok(false); }
            start = end;
        }
        Ok(start == ids.len())
    }
}

impl DecisionApplication {
    pub(crate) fn current_find_sources(
        self: &Arc<Self>, pages: &[CurrentFindSourcePageReference], cutoff: Timestamp,
    ) -> Result<RetainedCurrentFindSources, DecisionApplicationError> {
        validate_references(pages, cutoff)?;
        Ok(RetainedCurrentFindSources { decisions: Arc::clone(self), pages: Arc::from(pages), cutoff })
    }
    #[allow(clippy::type_complexity, reason = "original current and training source custody")]
    pub(crate) fn retain_current_find_source_page(
        &self, original: SourceAppliedCorporateActionPlanReference,
        training: Vec<(InstrumentId, Option<InstrumentId>, Option<InstrumentId>, Option<SourceAppliedCorporateActionPlanReference>)>,
        context: &RequestContext,
    ) -> Result<CurrentFindSourcePageReference, DecisionApplicationError> {
        let record = CurrentFindSourcePageRecord { original, training: training.into_iter().map(
            |(subject, requested_benchmark, selected_benchmark, original)| CurrentFindTrainingSource {
                subject, requested_benchmark, selected_benchmark, original,
            }).collect() };
        record.validate()?;
        let reference = record.reference()?;
        let custody = CurrentFindCustodyRecord::SourcePage(Box::new(record));
        let encoded = super::super::codec::current_find_custody(&custody)?;
        let state = self.writer()?;
        check_control(context)?;
        // Existing journal append returns AlreadyPresent only for identical canonical bytes.
        state.journal.append(&encoded)?;
        Ok(reference)
    }
    fn current_find_source_page(
        &self, reference: &CurrentFindSourcePageReference,
    ) -> Result<SourceAppliedCorporateActionPlanReference, DecisionApplicationError> {
        self.current_find_source_page_record(reference).map(|page| page.original)
    }
    fn current_find_source_page_record(
        &self, reference: &CurrentFindSourcePageReference,
    ) -> Result<CurrentFindSourcePageRecord, DecisionApplicationError> {
        reference.validate()?;
        let state = self.reader()?;
        match state.journal.current_find_record(&key(reference))? {
            Some(CurrentFindCustodyRecord::SourcePage(page)) => {
                page.validate()?;
                if page.reference()? != *reference { return Err(invalid()); }
                Ok(*page)
            }
            _ => Err(invalid()),
        }
    }
}

pub(super) fn validate_references(pages: &[CurrentFindSourcePageReference], cutoff: Timestamp) -> Result<usize, DecisionApplicationError> {
    if pages.len() > MAXIMUM_SOURCE_PAGES { return Err(invalid()); }
    let mut count = 0usize;
    let mut previous = None;
    for page in pages {
        page.validate()?;
        if page.knowledge_cutoff != cutoff || previous.is_some_and(|last| last >= page.first_instrument) { return Err(invalid()); }
        count = count.checked_add(page.instrument_count).ok_or_else(invalid)?;
        previous = Some(page.last_instrument);
    }
    if count > market_squawk_data::MAX_CURRENT_LISTED_POPULATION_MEMBERS { return Err(invalid()); }
    Ok(count)
}
pub(super) fn key(reference: &CurrentFindSourcePageReference) -> String {
    let hex: String = reference.sha256.iter().map(|byte| format!("{byte:02x}")).collect();
    format!("current-find:source:{hex}")
}
