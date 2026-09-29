//! Deterministic source-parent packing inside the original current preparation authority.
use super::*;

impl DatasetPreparationAuthority {
    pub(super) async fn pack_current_partitions(
        &self, population: &CurrentListedPopulation,
        references: &RetainedCurrentFindSources, cutoff: Timestamp,
        deadline: Instant, cancellation: &CancellationToken,
    ) -> Result<Box<[CurrentListedPopulationPartition]>, DatasetPreparationError> {
        let actions = SourceAppliedCorporateActionReadCapability::new(self.research.clone(), self.calendar.clone());
        let mut packer = ParentPacker::default();
        let mut position = 0;
        let ids = population.instrument_ids();
        let mut previous_source = None;
        let mut macro_date = None;
        let mut macro_parents = Vec::new();
        for index in 0..references.len() {
            check_control(deadline, cancellation)?;
            let reference = references.read(index).map_err(|_| DatasetPreparationError::InvalidEvidence)?;
            let source = actions.read_reference(&reference, deadline, cancellation.clone()).await
                .map_err(super::super::map_source_action_error)?.ok_or(DatasetPreparationError::Unavailable)?;
            let plan = source.covered_price_plan().map_err(super::super::map_source_action_error)?;
            if plan.retained_bytes() > MAXIMUM_CURRENT_SOURCE_BYTES { return Err(DatasetPreparationError::Capacity); }
            let coverage = plan.source_split_admission().ok_or(DatasetPreparationError::Unavailable)?;
            let mut roots = coverage.source_manifests().to_vec();
            let ordinary = source.ordinary_coverage().ok_or(DatasetPreparationError::InvalidEvidence)?;
            let mut date = None;
            for (read, _) in ordinary.reads() {
                let history = read.history();
                let last = history.bars().last().and_then(|bar| bar.time_semantics().nominal_daily_date())
                    .ok_or(DatasetPreparationError::InvalidEvidence)?.date();
                if history.bars().len() != 2 || date.is_some_and(|expected| expected != last) {
                    return Err(DatasetPreparationError::InvalidEvidence);
                }
                date = Some(last);
            }
            let date = date.ok_or(DatasetPreparationError::InvalidEvidence)?;
            if macro_date.is_some_and(|expected| expected != date) {
                return Err(DatasetPreparationError::InvalidEvidence);
            }
            if macro_date.is_none() {
                if let CurrentMacroSelection::Available { vector, .. } = self.read_current_macro(date, cutoff, deadline, cancellation).await? {
                    macro_parents.extend_from_slice(vector.parent_manifests());
                }
                macro_date = Some(date);
            }
            for root in &macro_parents { if !roots.contains(root) { roots.push(root.clone()); } }
            if roots.len() > market_squawk_data::MAX_DERIVED_GENERATION_PARENTS { return Err(DatasetPreparationError::Capacity); }
            // Only roots and exact end offsets survive this batch; all histories/source audit drop.
            drop(source);
            for instrument in reference.requested_instruments() {
                check_control(deadline, cancellation)?;
                if previous_source.is_some_and(|previous| previous >= *instrument) { return Err(DatasetPreparationError::InvalidEvidence); }
                while ids.get(position).is_some_and(|id| id < instrument) {
                    packer.push_member(position, &[])?;
                    position += 1;
                }
                if ids.get(position) != Some(instrument) { return Err(DatasetPreparationError::InvalidEvidence); }
                packer.push_member(position, &roots)?;
                position += 1;
                previous_source = Some(*instrument);
            }
        }
        while position < ids.len() {
            check_control(deadline, cancellation)?;
            packer.push_member(position, &[])?;
            position += 1;
        }
        let ends = packer.finish(position);
        if ends.len() > PreparedCurrentFindFeatures::maximum_partition_count(position, references.len()) {
            return Err(DatasetPreparationError::InvalidEvidence);
        }
        population.partitions_with_ends(&ends).map_err(|_| DatasetPreparationError::InvalidEvidence)
    }
}

#[derive(Default)]
struct ParentPacker {
    ends: Vec<usize>,
    start: usize,
    roots: Vec<DatasetManifestRef>,
}
impl ParentPacker {
    fn push_member(&mut self, position: usize, roots: &[DatasetManifestRef]) -> Result<(), DatasetPreparationError> {
        let extra = roots.iter().filter(|root| !self.roots.contains(root)).count();
        if position - self.start == 128 || self.roots.len() + extra > market_squawk_data::MAX_DERIVED_GENERATION_PARENTS {
            if position == self.start { return Err(DatasetPreparationError::Capacity); }
            self.ends.push(position);
            self.start = position;
            self.roots.clear();
        }
        for root in roots { if !self.roots.contains(root) { self.roots.push(root.clone()); } }
        if self.roots.len() > market_squawk_data::MAX_DERIVED_GENERATION_PARENTS { return Err(DatasetPreparationError::Capacity); }
        Ok(())
    }
    fn finish(mut self, count: usize) -> Vec<usize> {
        if count > self.start { self.ends.push(count); }
        self.ends
    }
}
