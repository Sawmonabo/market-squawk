//! Source-issued member assessments in the existing immutable Find journal.
//! No request accepts an assessment body or a caller-supplied failure reason.
use super::{CurrentFindCustodyRecord, CurrentFindPreparationRecord};
use crate::application::{
    analytical_profile::AnalyticalProfileResolution,
    decision::{DecisionApplication, DecisionApplicationError},
};
use market_squawk_decisions::{DecisionAuthority, ScreenRunId};
use market_squawk_domain::{InstrumentId, Timestamp};
use market_squawk_services::{RequestContext, ServiceError};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct FindMemberContext {
    pub(crate) preparation_id: Uuid,
    pub(crate) preparation_sha256: String,
    pub(crate) candidate_id: String,
    pub(crate) screen_run_id: String,
}
/// Constructible only after original parent, candidate and profile admission.
pub(crate) struct AdmittedFindMember {
    parent: CurrentFindPreparationRecord,
    member: FindMemberContext,
    instrument: InstrumentId,
    selection_sha256: [u8; 32],
}
impl AdmittedFindMember {
    pub(crate) fn instrument_id(&self) -> InstrumentId {
        self.instrument
    }
    pub(crate) fn source_cutoff(&self) -> Timestamp {
        self.parent.analytical_cutoff
    }
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FindSourceUnavailableReason {
    IdentityUnavailable,
    SourceEvidenceUnavailable,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct FindSourceFailure {
    pub(crate) source: String,
    pub(crate) failure: String,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct FindMemberUnavailableValue {
    pub(crate) member: FindMemberContext,
    pub(crate) assessment_sha256: String,
    pub(crate) reason: FindSourceUnavailableReason,
    pub(crate) source_failures: Vec<FindSourceFailure>,
}
impl FindMemberUnavailableValue {
    pub(crate) fn validate(&self)->Result<(),DecisionApplicationError>{
        if self.member.preparation_id.is_nil() || !digest_text(&self.member.preparation_sha256)
            || !digest_text(&self.assessment_sha256) || self.member.candidate_id.is_empty() || self.member.candidate_id.len()>256
            || self.member.screen_run_id.is_empty() || self.member.screen_run_id.len()>256
            || self.source_failures.len()>6 {return Err(invalid());}
        let mut names=std::collections::BTreeSet::new();
        for f in &self.source_failures {
            if !matches!(f.source.as_str(),"current_session"|"government_history"|"benchmark_history"|"selected_history"|"source_actions"|"equity_premium")
                || !names.insert(&f.source) || f.failure.is_empty() || f.failure.len()>1024 {return Err(invalid());}
        }
        match self.reason {
            FindSourceUnavailableReason::IdentityUnavailable if self.source_failures.is_empty()=>Ok(()),
            FindSourceUnavailableReason::SourceEvidenceUnavailable if !self.source_failures.is_empty()=>Ok(()),
            _=>Err(invalid()),
        }
    }
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct FindMemberUnavailableRecord {
    pub(crate) member: FindMemberContext,
    profile_sha256: String,
    instrument: InstrumentId,
    selection_sha256: [u8; 32],
    started_at: Timestamp,
    completed_at: Timestamp,
    /// Actual successful producer response, before adding its custody reference.
    source_result: Value,
}
impl FindMemberUnavailableRecord {
    pub(crate) fn key(&self) -> String {
        key(&self.member)
    }
    pub(crate) fn digest(&self) -> Result<[u8; 32], DecisionApplicationError> {
        self.validate()?;
        Ok(Sha256::digest(serde_json::to_vec(self).map_err(|_| invalid())?).into())
    }
    pub(crate) fn value(&self) -> Result<FindMemberUnavailableValue, DecisionApplicationError> {
        self.validate()?;
        let reason =
            serde_json::from_value(self.source_result["reason"].clone()).map_err(|_| invalid())?;
        let mut failures = Vec::new();
        for source in self.source_result["sources"]
            .as_array()
            .ok_or_else(invalid)?
        {
            if source["status"] == "unavailable" {
                failures.push(FindSourceFailure {
                    source: source["source"].as_str().ok_or_else(invalid)?.into(),
                    failure: source["failure"].as_str().ok_or_else(invalid)?.into(),
                });
            }
        }
        Ok(FindMemberUnavailableValue {
            member: self.member.clone(),
            assessment_sha256: hex(self.digest()?),
            reason,
            source_failures: failures,
        })
    }
    pub(crate) fn response(&self) -> Result<Value, DecisionApplicationError> {
        let mut result = self.source_result.clone();
        result["findMemberUnavailable"] =
            serde_json::to_value(self.value()?).map_err(|_| invalid())?;
        Ok(result)
    }
    pub(crate) fn validate(&self) -> Result<(), DecisionApplicationError> {
        let b = &self.source_result;
        if self.member.preparation_id.is_nil()
            || !digest_text(&self.member.preparation_sha256)
            || self.member.candidate_id.is_empty()
            || self.member.candidate_id.len() > 256
            || self.member.screen_run_id.is_empty()
            || self.member.screen_run_id.len() > 256
            || !digest_text(&self.profile_sha256)
            || self.selection_sha256 == [0; 32]
            || self.started_at.unix_nanos() <= 0
            || self.completed_at < self.started_at
            || b.as_object().is_none_or(|o| o.len() != 9 || !o.keys().all(|key|matches!(key.as_str(),"status"|"scope"|"financialConfigurationDigest"|"instrumentId"|"preparedAtUnixNanos"|"reference"|"sourceActionReference"|"sources"|"reason")))
            || b["status"] != "unavailable"
            || b["scope"] != "investment_analysis"
            || b["financialConfigurationDigest"].as_str() != Some(self.profile_sha256.as_str())
            || serde_json::from_value::<InstrumentId>(b["instrumentId"].clone()).ok()
                != Some(self.instrument)
            || !matches!(
                b["reason"].as_str(),
                Some("identity_unavailable" | "source_evidence_unavailable")
            )
        {
            return Err(invalid());
        }
        let sources = b["sources"].as_array().ok_or_else(invalid)?;
        if sources.len() > 6 {
            return Err(invalid());
        }
        let mut names = std::collections::BTreeSet::new();
        for source in sources {
            let name = source["source"].as_str().ok_or_else(invalid)?;
            if source.as_object().is_none_or(|o| o.len() != 6 || !o.keys().all(|key|matches!(key.as_str(),"source"|"status"|"evidenceDigest"|"failure"|"startedAtUnixNanos"|"completedAtUnixNanos")))
                || !matches!(
                    name,
                    "current_session"
                        | "government_history"
                        | "benchmark_history"
                        | "selected_history"
                        | "source_actions"
                        | "equity_premium"
                )
                || !names.insert(name)
            {
                return Err(invalid());
            }
            let started = time(&source["startedAtUnixNanos"])?;
            let completed = time(&source["completedAtUnixNanos"])?;
            if started < self.started_at.unix_nanos()
                || completed < started
                || completed > self.completed_at.unix_nanos()
            {
                return Err(invalid());
            }
            match source["status"].as_str() {
                Some("available")
                    if source["evidenceDigest"].as_str().is_some_and(digest_text)
                        && source["failure"].is_null() => {}
                Some("unavailable")
                    if source["evidenceDigest"].is_null()
                        && source["failure"]
                            .as_str()
                            .is_some_and(|s| !s.is_empty() && s.len() <= 1024) => {}
                _ => return Err(invalid()),
            }
        }
        if b["reason"] == "identity_unavailable" {
            if !sources.is_empty()
                || !b["preparedAtUnixNanos"].is_null()
                || !b["reference"].is_null()
                || !b["sourceActionReference"].is_null()
            {
                return Err(invalid());
            }
        } else {
            let cutoff = time(&b["preparedAtUnixNanos"])?;
            if !b["sourceActionReference"].is_null()
                || !sources.iter().any(|s|s["source"]=="source_actions"&&s["status"]=="unavailable")
                || cutoff < self.started_at.unix_nanos()
                || cutoff > self.completed_at.unix_nanos()
                || !sources.iter().any(|s| s["status"] == "unavailable")
            {
                return Err(invalid());
            }
        }
        // Existing journal cap remains unchanged; evidence is never truncated to fit.
        if serde_json::to_vec(self).map_err(|_| invalid())?.len() > 256 * 1024 {
            return Err(DecisionApplicationError::Capacity);
        }
        Ok(())
    }
    pub(crate) fn validate_authority(
        &self,
        authority: &DecisionAuthority,
    ) -> Result<(), DecisionApplicationError> {
        self.validate()?;
        let run = ScreenRunId::try_new(self.member.screen_run_id.clone()).map_err(|_| invalid())?;
        let execution = authority
            .repository()
            .screen_execution(&run)
            .ok_or_else(invalid)?;
        let mut rows = execution
            .candidates()
            .iter()
            .filter(|c| c.record().id().as_str() == self.member.candidate_id);
        let candidate = rows.next().ok_or_else(invalid)?.record();
        if rows.next().is_some() || candidate.instrument_id() != self.instrument {
            return Err(invalid());
        }
        Ok(())
    }
    pub(super) fn validate_parent(
        &self,
        parent: &super::CurrentFindRecoverySummary,
    ) -> Result<(), DecisionApplicationError> {
        if self.member.preparation_sha256 != hex(parent.digest)
            || self.member.screen_run_id != parent.screen_run_id.as_deref().ok_or_else(invalid)?
            || self.profile_sha256 != hex(parent.profile_sha256)
            || self.started_at < parent.cutoff
            || parent.results_published
        {
            return Err(invalid());
        }
        Ok(())
    }
}
impl DecisionApplication {
    /// Reads the separate original training plan retained by this admitted Find parent. The
    /// current two-session feature page cannot substitute for missing training history. This
    /// never acquires later evidence or changes the original cutoff or comparison selection.
    pub(crate) fn find_member_source_reference(
        self: &std::sync::Arc<Self>,
        admitted: &AdmittedFindMember,
        requested_benchmark: Option<InstrumentId>,
        context: &RequestContext,
    ) -> Result<Option<(crate::application::research::corporate_actions::SourceAppliedCorporateActionPlanReference, Option<InstrumentId>)>, ServiceError> {
        admitted.parent.authorize(context)?;
        check_live(context)?;
        let sources = self.current_find_sources(&admitted.parent.source_pages, admitted.source_cutoff()).map_err(map)?;
        let mut found = None;
        let mut matched = false;
        for (index, page) in sources.pages().iter().enumerate() {
            check_live(context)?;
            if !page.intersects(&[admitted.instrument_id()]) { continue; }
            let original = sources.read(index).map_err(map)?;
            if original.knowledge_cutoff() != admitted.source_cutoff() { return Err(ServiceError::InvalidResult); }
            if original.requested_instruments().binary_search(&admitted.instrument_id()).is_ok() {
                if matched { return Err(ServiceError::InvalidResult); }
                matched = true;
                found = sources.read_training(index, admitted.instrument_id(), requested_benchmark).map_err(map)?;
                if found.as_ref().is_some_and(|(training, _)| training.knowledge_cutoff() != admitted.source_cutoff()) {
                    return Err(ServiceError::InvalidResult);
                }
            }
        }
        check_live(context)?;
        if !matched { return Err(ServiceError::InvalidResult); }
        Ok(found)
    }
    pub(crate) fn admit_find_member(
        &self,
        member: &FindMemberContext,
        profile: &AnalyticalProfileResolution,
        selection_token: &str,
        context: &RequestContext,
    ) -> Result<AdmittedFindMember, ServiceError> {
        check_live(context)?;
        let parent = self
            .current_find_preparation(member.preparation_id)
            .map_err(map)?
            .ok_or(ServiceError::NotFound)?;
        parent.authorize(context)?;
        if hex(parent.digest().map_err(map)?) != member.preparation_sha256
            || &parent.profile != profile
        {
            return Err(ServiceError::InvalidRequest);
        }
        let screen = self
            .current_find_screen(&parent)
            .map_err(map)?
            .ok_or(ServiceError::InvalidRequest)?;
        if screen.run_id != member.screen_run_id {
            return Err(ServiceError::InvalidRequest);
        }
        let state = self.reader().map_err(map)?;
        let run = ScreenRunId::try_new(member.screen_run_id.clone())
            .map_err(|_| ServiceError::InvalidRequest)?;
        let execution = state
            .authority
            .repository()
            .screen_execution(&run)
            .ok_or(ServiceError::InvalidResult)?;
        let mut rows = execution
            .candidates()
            .iter()
            .filter(|c| c.record().id().as_str() == member.candidate_id);
        let instrument = rows
            .next()
            .ok_or(ServiceError::InvalidRequest)?
            .record()
            .instrument_id();
        if rows.next().is_some() || selection_token.is_empty() || selection_token.len() > 1024 {
            return Err(ServiceError::InvalidRequest);
        }
        Ok(AdmittedFindMember {
            parent,
            member: member.clone(),
            instrument,
            selection_sha256: Sha256::digest(selection_token.as_bytes()).into(),
        })
    }
    pub(crate) fn find_member_source_assessment(
        &self,
        admitted: &AdmittedFindMember,
        context: &RequestContext,
    ) -> Result<Option<FindMemberUnavailableRecord>, ServiceError> {
        admitted.parent.authorize(context)?;
        check_live(context)?;
        let state = self.reader().map_err(map)?;
        match state
            .journal
            .current_find_record(&key(&admitted.member))
            .map_err(map)?
        {
            None => Ok(None),
            Some(CurrentFindCustodyRecord::MemberUnavailable(record)) => {
                if record.member != admitted.member
                    || record.selection_sha256 != admitted.selection_sha256
                    || record.instrument != admitted.instrument
                    || record.profile_sha256 != admitted.parent.profile.configuration_digest
                    || record.started_at < admitted.parent.analytical_cutoff
                {
                    return Err(ServiceError::InvalidRequest);
                }
                record.validate_authority(&state.authority).map_err(map)?;
                Ok(Some(*record))
            }
            _ => Err(ServiceError::InvalidResult),
        }
    }
    /// Only the installed source owner calls this with its actual successful preparation response.
    pub(crate) fn retain_find_member_source_assessment(
        &self,
        admitted: AdmittedFindMember,
        source_result: Value,
        started_at: Timestamp,
        completed_at: Timestamp,
        context: &RequestContext,
    ) -> Result<FindMemberUnavailableRecord, ServiceError> {
        admitted.parent.authorize(context)?;
        check_live(context)?;
        let record = FindMemberUnavailableRecord {
            member: admitted.member,
            profile_sha256: admitted.parent.profile.configuration_digest.clone(),
            instrument: admitted.instrument,
            selection_sha256: admitted.selection_sha256,
            started_at,
            completed_at,
            source_result,
        };
        if record.started_at < admitted.parent.analytical_cutoff {
            return Err(ServiceError::InvalidResult);
        }
        let mut state = self.writer().map_err(map)?;
        record.validate_authority(&state.authority).map_err(map)?;
        if let Some(existing) = state
            .journal
            .current_find_record(&record.key())
            .map_err(map)?
        {
            return match existing {
                CurrentFindCustodyRecord::MemberUnavailable(existing)
                    if existing.member == record.member
                        && existing.selection_sha256 == record.selection_sha256
                        && existing.instrument == record.instrument
                        && existing.profile_sha256 == record.profile_sha256 =>
                {
                    Ok(*existing)
                }
                _ => Err(ServiceError::InvalidResult),
            };
        }
        if state
            .journal
            .current_find_record(&format!(
                "current-find:results:{}",
                record.member.preparation_id
            ))
            .map_err(map)?
            .is_some()
        {
            return Err(ServiceError::InvalidRequest);
        }
        let custody = CurrentFindCustodyRecord::MemberUnavailable(Box::new(record.clone()));
        let encoded = super::super::codec::current_find_custody(&custody).map_err(map)?;
        check_live(context)?;
        if let Err(error) = state.journal.append(&encoded) {
            state.poisoned = true;
            return Err(map(error));
        }
        Ok(record)
    }
}
pub(crate) fn key(member: &FindMemberContext) -> String {
    format!(
        "current-find:member:{}:{}",
        member.preparation_id,
        hex(Sha256::digest(member.candidate_id.as_bytes()).into())
    )
}
fn time(v: &Value) -> Result<i64, DecisionApplicationError> {
    let s = v.as_str().ok_or_else(invalid)?;
    let n = s.parse::<i64>().map_err(|_| invalid())?;
    if n <= 0 || n.to_string() != s {
        Err(invalid())
    } else {
        Ok(n)
    }
}
fn digest_text(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        && s.bytes().any(|b| b != b'0')
}
fn hex(b: [u8; 32]) -> String {
    crate::application::model::forecast_preparation::hex(market_squawk_data::Sha256Digest::new(b))
}
fn invalid() -> DecisionApplicationError {
    DecisionApplicationError::InvalidPersistentState
}
fn map(e: DecisionApplicationError) -> ServiceError {
    match e {
        DecisionApplicationError::Allocation | DecisionApplicationError::Capacity => {
            ServiceError::ResourceExhausted
        }
        DecisionApplicationError::InvalidPersistentState => ServiceError::InvalidResult,
        DecisionApplicationError::Unavailable | DecisionApplicationError::Persistence => {
            ServiceError::Unavailable
        }
        DecisionApplicationError::Repository(_) => ServiceError::InvalidResult,
    }
}
fn check_live(c: &RequestContext) -> Result<(), ServiceError> {
    if c.cancellation().is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if std::time::Instant::now() >= c.deadline() {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}
