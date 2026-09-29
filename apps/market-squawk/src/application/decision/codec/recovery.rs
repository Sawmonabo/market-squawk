use std::collections::BTreeMap;

use market_squawk_decisions::{
    AppendOutcome, CandidateRecord, DecisionAuthority, InvestmentAnalysisId,
    InvestmentProposalDecision, InvestmentProposalId,
};
use market_squawk_modeling::ProductionFeatureRegistry;

use super::super::DecisionApplicationError;
use super::super::screen_workflow::ScreenJobPlan;
use super::candidate::ExecutionWire;
use super::wire::{WIRE_VERSION, WireEnvelope, WireRecord};

#[derive(Debug)]
pub(in crate::application::decision) struct RecoveryContext {
    registry: ProductionFeatureRegistry,
    candidates: BTreeMap<String, CandidateRecord>,
    screen_job_inputs: BTreeMap<String, ScreenJobPlan>,
    maximum_screen_job_inputs: usize,
    current_find: BTreeMap<uuid::Uuid, super::super::current_find::CurrentFindRecoverySummary>,
    current_find_source_pages: BTreeMap<String, super::super::current_find::CurrentFindSourcePageReference>,
}

impl RecoveryContext {
    pub(in crate::application::decision) fn try_new(
        maximum_screen_job_inputs: usize,
    ) -> Result<Self, DecisionApplicationError> {
        if maximum_screen_job_inputs == 0 {
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        Ok(Self {
            registry: ProductionFeatureRegistry::try_new()
                .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?,
            candidates: BTreeMap::new(),
            current_find: BTreeMap::new(),
            current_find_source_pages: BTreeMap::new(),
            screen_job_inputs: BTreeMap::new(),
            maximum_screen_job_inputs,
        })
    }

    pub(in crate::application::decision) fn into_screen_job_inputs(
        self,
    ) -> BTreeMap<String, ScreenJobPlan> {
        self.screen_job_inputs
    }

    pub(in crate::application::decision) fn apply(
        &mut self,
        authority: &mut DecisionAuthority,
        kind: i64,
        key: &str,
        payload: &[u8],
    ) -> Result<(), DecisionApplicationError> {
        self.apply_inner(authority, kind, key, payload)
            .map_err(|_error| DecisionApplicationError::InvalidPersistentState)
    }

    /// Source-dependent bundles await the existing retained readers before ordered authority replay.
    pub(in crate::application::decision) async fn apply_with_replay(
        &mut self,
        authority: &mut DecisionAuthority,
        kind: i64,
        key: &str,
        payload: &[u8],
        replay: &super::super::current_share::CurrentShareReplayCapability,
        context: &market_squawk_services::RequestContext,
    ) -> Result<(), DecisionApplicationError> {
        let envelope: WireEnvelope = serde_json::from_slice(payload)
            .map_err(|_| DecisionApplicationError::InvalidPersistentState)?;
        if envelope.version != WIRE_VERSION || envelope.record.kind() != kind {
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        if let WireRecord::PreparedPublishedInvestmentAnalysis(wire) = envelope.record {
            if wire.has_current_share_projection() {
                if wire.key()? != key { return Err(DecisionApplicationError::InvalidPersistentState); }
                let candidate = if let Some((candidate_id, run_id)) = wire.candidate_reference()? {
                    let (run, candidate) = authority.get_candidate(&candidate_id)?;
                    if run.id() != &run_id { return Err(DecisionApplicationError::InvalidPersistentState); }
                    let screen = authority.get_screen(run.screen().id(), run.screen().revision())?;
                    Some((screen.clone(), run.clone(), candidate.clone()))
                } else { None };
                let recovered = wire.decode_with_replay(
                    candidate.as_ref().map(|(screen, run, candidate)| (screen, run, candidate)), replay, context,
                ).await?;
                return ensure_appended(authority.replay_prepared_published_investment_analysis(recovered)?);
            }
        }
        self.apply(authority, kind, key, payload)
    }

    /// A backup's source authority is the unchanged live owner; exact encoding binds each row.
    pub(in crate::application::decision) fn apply_with_retained(
        &mut self,
        authority: &mut DecisionAuthority,
        retained: &DecisionAuthority,
        kind: i64,
        key: &str,
        payload: &[u8],
    ) -> Result<(), DecisionApplicationError> {
        let envelope: WireEnvelope = serde_json::from_slice(payload)
            .map_err(|_| DecisionApplicationError::InvalidPersistentState)?;
        if envelope.version != WIRE_VERSION || envelope.record.kind() != kind {
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        match envelope.record {
            WireRecord::PreparedPublishedInvestmentAnalysis(wire) if wire.has_current_share_projection() => {
                let value = retained.get_prepared_published_investment_analysis(wire.analysis_id()?)?;
                if wire.key()? != key || super::prepared_published_investment_analysis(value)?.payload != payload {
                    return Err(DecisionApplicationError::InvalidPersistentState);
                }
                ensure_appended(authority.replay_prepared_published_investment_analysis(value.clone())?)
            }
            WireRecord::InvestmentProposal(wire) if wire.has_current_share_projection() => {
                let value = retained.get_investment_proposal(wire.analysis_id()?)?;
                if wire.key()? != key || super::investment_proposal(value)?.payload != payload {
                    return Err(DecisionApplicationError::InvalidPersistentState);
                }
                ensure_appended(authority.replay_investment_proposal(value.clone())?)
            }
            _ => self.apply(authority, kind, key, payload),
        }
    }

    fn apply_inner(
        &mut self,
        authority: &mut DecisionAuthority,
        kind: i64,
        key: &str,
        payload: &[u8],
    ) -> Result<(), DecisionApplicationError> {
        let envelope: WireEnvelope = serde_json::from_slice(payload)
            .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
        if envelope.version != WIRE_VERSION || envelope.record.kind() != kind {
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        match envelope.record {
            WireRecord::CurrentFindCustody(record) => {
                if record.key() != key || super::current_find_custody(&record)?.payload != payload {
                    return Err(DecisionApplicationError::InvalidPersistentState);
                }
                super::super::current_find::recover(&record, &mut self.current_find, &mut self.current_find_source_pages)?;
                match record.as_ref() {
                    super::super::current_find::CurrentFindCustodyRecord::MemberUnavailable(
                        value,
                    ) => value.validate_authority(authority)?,
                    super::super::current_find::CurrentFindCustodyRecord::Results(value) => {
                        value.validate_authority(authority)?;
                    }
                    _ => {}
                }
                if let super::super::current_find::CurrentFindCustodyRecord::Screen(record) =
                    record.as_ref()
                {
                    let parent = self
                        .current_find
                        .get(&record.preparation_id)
                        .ok_or(DecisionApplicationError::InvalidPersistentState)?;
                    let plan = self
                        .screen_job_inputs
                        .get(&record.run_id)
                        .ok_or(DecisionApplicationError::InvalidPersistentState)?;
                    let admitted = plan
                        .admitted()
                        .map_err(|_| DecisionApplicationError::InvalidPersistentState)?;
                    if admitted.input_identity() != &record.input_identity
                        || admitted.input_digest() != record.input_digest
                        || admitted.population_member_count() != parent.population_count
                        || plan.run().universe_identity().evidence_digest().bytes()
                            != parent.population_content_digest
                    {
                        return Err(DecisionApplicationError::InvalidPersistentState);
                    }
                }
                Ok(())
            }
            WireRecord::Screen(wire) => {
                if wire.key() != key {
                    return Err(DecisionApplicationError::InvalidPersistentState);
                }
                let screen = wire.decode(self.registry.feature_registry())?;
                let expected = authority
                    .repository()
                    .screens()
                    .filter(|candidate| candidate.revision().id() == screen.revision().id())
                    .map(|candidate| candidate.revision().revision())
                    .max_by_key(|revision| revision.get());
                ensure_appended(authority.save_screen(expected, screen)?)
            }
            WireRecord::Execution(wire) => {
                if wire.key() != key {
                    return Err(DecisionApplicationError::InvalidPersistentState);
                }
                let expected = wire.clone();
                let (run, candidates, selected_at) =
                    wire.decode(self.registry.feature_registry())?;
                let execution = authority.run_screen(run, candidates, selected_at)?;
                if ExecutionWire::from_execution(&execution, selected_at)? != expected {
                    return Err(DecisionApplicationError::InvalidPersistentState);
                }
                for candidate in execution.candidates() {
                    self.candidates
                        .entry(candidate.record().id().as_str().to_owned())
                        .or_insert_with(|| candidate.record().clone());
                }
                Ok(())
            }
            WireRecord::Dossier(wire) => {
                if wire.key() != key {
                    return Err(DecisionApplicationError::InvalidPersistentState);
                }
                let candidate = self
                    .candidates
                    .get(wire.candidate_key())
                    .ok_or(DecisionApplicationError::InvalidPersistentState)?;
                ensure_appended(authority.append_dossier(wire.decode(candidate)?)?)
            }
            WireRecord::Target(wire) => {
                let wire = *wire;
                if wire.key()? != key {
                    return Err(DecisionApplicationError::InvalidPersistentState);
                }
                let target = wire.decode()?;
                let expected = authority
                    .repository()
                    .target_revisions(target.target().id())
                    .map(|candidate| candidate.target().revision())
                    .max_by_key(|revision| revision.get());
                ensure_appended(match expected {
                    None => authority.create_target(target)?,
                    Some(expected) => authority.reevaluate_target(expected, target)?,
                })
            }
            WireRecord::Review(wire) => {
                if wire.key() != key {
                    return Err(DecisionApplicationError::InvalidPersistentState);
                }
                let (target_id, revision) = wire.target_coordinate()?;
                let target = authority
                    .get_target(&target_id, revision)?
                    .target()
                    .target()
                    .clone();
                ensure_appended(authority.review_target(wire.decode(&target)?)?)
            }
            WireRecord::Invalidation(wire) => {
                if wire.key() != key {
                    return Err(DecisionApplicationError::InvalidPersistentState);
                }
                let (target_id, revision) = wire.target_coordinate()?;
                let target = authority
                    .get_target(&target_id, revision)?
                    .target()
                    .target()
                    .clone();
                ensure_appended(authority.invalidate_target(wire.decode(&target)?)?)
            }
            WireRecord::ScreenJobInput(wire) => {
                if wire.key() != key
                    || self.screen_job_inputs.contains_key(key)
                    || self.screen_job_inputs.len() >= self.maximum_screen_job_inputs
                {
                    return Err(DecisionApplicationError::InvalidPersistentState);
                }
                let plan = wire.decode(self.registry.feature_registry())?;
                let screen = authority
                    .get_screen(plan.run().screen().id(), plan.run().screen().revision())?;
                super::super::screen_workflow::validate_fence(&plan, screen)
                    .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
                self.screen_job_inputs.insert(key.to_owned(), plan);
                Ok(())
            }
            WireRecord::InvestmentProposal(wire) => {
                if wire.key()? != key {
                    return Err(DecisionApplicationError::InvalidPersistentState);
                }
                ensure_appended(authority.replay_investment_proposal(wire.decode()?)?)
            }
            WireRecord::PreparedPublishedInvestmentAnalysis(wire) => {
                let wire = *wire;
                if wire.key()? != key {
                    return Err(DecisionApplicationError::InvalidPersistentState);
                }
                let candidate =
                    if let Some((candidate_id, screen_run_id)) = wire.candidate_reference()? {
                        // Append order remains authority when a real saved-screen candidate is bound.
                        let (run, candidate) = authority.get_candidate(&candidate_id)?;
                        if run.id() != &screen_run_id {
                            return Err(DecisionApplicationError::InvalidPersistentState);
                        }
                        let screen =
                            authority.get_screen(run.screen().id(), run.screen().revision())?;
                        Some((screen.clone(), run.clone(), candidate.clone()))
                    } else {
                        None
                    };
                let recovered = wire.decode(
                    candidate
                        .as_ref()
                        .map(|(screen, run, candidate)| (screen, run, candidate)),
                )?;
                ensure_appended(authority.replay_prepared_published_investment_analysis(recovered)?)
            }
            WireRecord::InvestmentAnalysisPublication(wire) => {
                if wire.key() != key {
                    return Err(DecisionApplicationError::InvalidPersistentState);
                }
                let analysis_id = InvestmentAnalysisId::try_from_bytes(wire.analysis_id())
                    .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
                let decision = authority.get_investment_proposal(analysis_id)?.clone();
                ensure_appended(
                    authority.append_investment_analysis_publication(wire.decode(&decision)?)?,
                )
            }
            WireRecord::InvestmentOutcomeProjection(wire) => {
                if wire.key() != key {
                    return Err(DecisionApplicationError::InvalidPersistentState);
                }
                let proposal = generated_proposal(authority, wire.proposal_id())?;
                ensure_appended(
                    authority.append_investment_outcome_projection(wire.decode(&proposal)?)?,
                )
            }
            WireRecord::InvestmentSizingProjection(wire) => {
                let wire = *wire;
                if wire.key() != key {
                    return Err(DecisionApplicationError::InvalidPersistentState);
                }
                let proposal = generated_proposal(authority, wire.proposal_id())?;
                ensure_appended(
                    authority.append_investment_sizing_projection(wire.decode(&proposal)?)?,
                )
            }
            WireRecord::RecommendationOutcomeStatus(wire) => {
                if wire.key() != key {
                    return Err(DecisionApplicationError::InvalidPersistentState);
                }
                let analysis_id = InvestmentAnalysisId::try_from_bytes(wire.analysis_id())
                    .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
                let decision = authority.get_investment_proposal(analysis_id)?.clone();
                let publication = authority
                    .get_investment_analysis_publication(analysis_id)?
                    .clone();
                ensure_appended(
                    authority.append_recommendation_outcome_status(
                        wire.decode(&decision, &publication)?,
                    )?,
                )
            }
        }
    }
}

fn generated_proposal(
    authority: &DecisionAuthority,
    bytes: [u8; 32],
) -> Result<market_squawk_decisions::GeneratedInvestmentProposal, DecisionApplicationError> {
    let proposal_id = InvestmentProposalId::try_from_bytes(bytes)
        .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
    authority
        .repository()
        .investment_proposals()
        .find_map(|decision| match decision {
            InvestmentProposalDecision::Generated(value) if value.proposal_id() == proposal_id => {
                Some(value.clone())
            }
            InvestmentProposalDecision::Generated(_)
            | InvestmentProposalDecision::NoAction(_)
            | InvestmentProposalDecision::Unavailable(_) => None,
        })
        .ok_or(DecisionApplicationError::InvalidPersistentState)
}

fn ensure_appended(outcome: AppendOutcome) -> Result<(), DecisionApplicationError> {
    if outcome == AppendOutcome::Appended {
        Ok(())
    } else {
        Err(DecisionApplicationError::InvalidPersistentState)
    }
}
