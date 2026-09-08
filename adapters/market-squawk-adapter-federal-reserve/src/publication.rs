//! Immutable publication, correction, repost, and replacement evidence.

use std::collections::{BTreeMap, BTreeSet};

use market_squawk_domain::{ResearchTemporalCoordinate, RevisionNumber, Timestamp};
use serde::Serialize;
use sha2::{Digest as _, Sha256};

use crate::contract::{BoardDatasetFamily, BoardRouteLifecycle};
use crate::digest::{finish, update_bool, update_bytes, update_i64, update_tag, update_u64};
use crate::{BoardAdapterError, ParsedBoardDataset};

const BOARD_PUBLICATION_RECEIPT_VERSION: u16 = 1;

/// Explicit limitation of Board DDP/current release files.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BoardVintageCapability {
    /// Only acquisitions retained locally can be reconstructed; the source file is not ALFRED.
    LocallyRetainedAcquisitionsOnly,
}

/// Source-evidenced publication event represented by an immutable generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BoardPublicationEventKind {
    /// Normal scheduled statistical release.
    ScheduledRelease,
    /// Source-announced correction outside the scheduled release.
    OffScheduleCorrection,
    /// Byte-level repost whose normalized values did not change.
    Repost,
    /// Scheduled annual revision or rebenchmarking.
    AnnualRevision,
    /// First acquisition of a discontinued historical archive.
    HistoricalArchive,
}

impl BoardPublicationEventKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ScheduledRelease => "scheduled_release",
            Self::OffScheduleCorrection => "off_schedule_correction",
            Self::Repost => "repost",
            Self::AnnualRevision => "annual_revision",
            Self::HistoricalArchive => "historical_archive",
        }
    }
}

/// Exact source notice identity and digest associated with one publication event.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BoardPublicationEvent {
    kind: BoardPublicationEventKind,
    notice_id: Box<str>,
    notice_digest: [u8; 32],
}

impl BoardPublicationEvent {
    /// Builds one event from a stable notice identifier and exact notice/evidence digest.
    pub fn try_new(
        kind: BoardPublicationEventKind,
        notice_id: impl Into<Box<str>>,
        notice_digest: [u8; 32],
    ) -> Result<Self, BoardAdapterError> {
        let notice_id = notice_id.into();
        if notice_id.is_empty() || notice_id.len() > 1024 || notice_id.chars().any(char::is_control)
        {
            return Err(BoardAdapterError::InvalidRevisionEvidence);
        }
        Ok(Self {
            kind,
            notice_id,
            notice_digest,
        })
    }

    /// Returns the event kind.
    pub const fn kind(&self) -> BoardPublicationEventKind {
        self.kind
    }
    /// Returns the stable source notice identifier.
    pub fn notice_id(&self) -> &str {
        &self.notice_id
    }
    /// Returns the digest of exact notice/evidence bytes.
    pub const fn notice_digest(&self) -> [u8; 32] {
        self.notice_digest
    }
}

/// Independent source and local clocks for one immutable publication.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BoardPublicationTiming {
    scheduled_for: Option<ResearchTemporalCoordinate>,
    source_published: Option<ResearchTemporalCoordinate>,
    correction_published: Option<ResearchTemporalCoordinate>,
    route_available_at: Timestamp,
    received_at: Timestamp,
    parsed_at: Timestamp,
}

impl BoardPublicationTiming {
    /// Builds independent clocks without inventing missing source precision.
    pub fn try_new(
        scheduled_for: Option<ResearchTemporalCoordinate>,
        source_published: Option<ResearchTemporalCoordinate>,
        correction_published: Option<ResearchTemporalCoordinate>,
        route_available_at: Timestamp,
        received_at: Timestamp,
        parsed_at: Timestamp,
    ) -> Result<Self, BoardAdapterError> {
        let value = Self {
            scheduled_for,
            source_published,
            correction_published,
            route_available_at,
            received_at,
            parsed_at,
        };
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<(), BoardAdapterError> {
        if self.route_available_at > self.received_at || self.received_at > self.parsed_at {
            return Err(BoardAdapterError::InvalidChronology);
        }
        for coordinate in [
            self.source_published.as_ref(),
            self.correction_published.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            if coordinate
                .exact_timestamp()
                .is_some_and(|instant| instant > self.route_available_at)
            {
                return Err(BoardAdapterError::InvalidChronology);
            }
        }
        Ok(())
    }

    /// Returns the source schedule coordinate when published.
    pub const fn scheduled_for(&self) -> Option<&ResearchTemporalCoordinate> {
        self.scheduled_for.as_ref()
    }
    /// Returns the source publication coordinate when published.
    pub const fn source_published(&self) -> Option<&ResearchTemporalCoordinate> {
        self.source_published.as_ref()
    }
    /// Returns a separate correction coordinate when published.
    pub const fn correction_published(&self) -> Option<&ResearchTemporalCoordinate> {
        self.correction_published.as_ref()
    }
    /// Returns when exact bytes became available on the selected route.
    pub const fn route_available_at(&self) -> Timestamp {
        self.route_available_at
    }
    /// Returns when the application received the final byte.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }
    /// Returns when bounded parsing completed.
    pub const fn parsed_at(&self) -> Timestamp {
        self.parsed_at
    }
}

/// Source-evidenced predecessor/successor coordinate change.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BoardSeriesReplacement {
    predecessor_unique_id: Box<str>,
    successor_unique_id: Box<str>,
    effective_period: Box<str>,
    evidence_digest: [u8; 32],
}

impl BoardSeriesReplacement {
    /// Builds an explicit series replacement rather than silently stitching identities.
    pub fn try_new(
        predecessor_unique_id: impl Into<Box<str>>,
        successor_unique_id: impl Into<Box<str>>,
        effective_period: impl Into<Box<str>>,
        evidence_digest: [u8; 32],
    ) -> Result<Self, BoardAdapterError> {
        let value = Self {
            predecessor_unique_id: predecessor_unique_id.into(),
            successor_unique_id: successor_unique_id.into(),
            effective_period: effective_period.into(),
            evidence_digest,
        };
        if value.predecessor_unique_id.is_empty()
            || value.successor_unique_id.is_empty()
            || value.predecessor_unique_id == value.successor_unique_id
            || value.effective_period.is_empty()
            || value.predecessor_unique_id.len() > 512
            || value.successor_unique_id.len() > 512
            || value.effective_period.len() > 64
        {
            Err(BoardAdapterError::InvalidRevisionEvidence)
        } else {
            Ok(value)
        }
    }

    /// Returns the predecessor provider coordinate.
    pub fn predecessor_unique_id(&self) -> &str {
        &self.predecessor_unique_id
    }
    /// Returns the successor provider coordinate.
    pub fn successor_unique_id(&self) -> &str {
        &self.successor_unique_id
    }
    /// Returns the exact source effective-period token.
    pub fn effective_period(&self) -> &str {
        &self.effective_period
    }
    /// Returns the replacement evidence digest.
    pub const fn evidence_digest(&self) -> [u8; 32] {
        self.evidence_digest
    }
}

/// Row-level immutable change coordinate.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BoardObservationChange {
    series_unique_id: Box<str>,
    period: Box<str>,
    previous_row_digest: Option<[u8; 32]>,
    current_row_digest: Option<[u8; 32]>,
}

impl BoardObservationChange {
    /// Returns the provider series coordinate.
    pub fn series_unique_id(&self) -> &str {
        &self.series_unique_id
    }
    /// Returns the exact source period.
    pub fn period(&self) -> &str {
        &self.period
    }
    /// Returns the predecessor row digest when the row existed.
    pub const fn previous_row_digest(&self) -> Option<[u8; 32]> {
        self.previous_row_digest
    }
    /// Returns the current row digest when the row exists.
    pub const fn current_row_digest(&self) -> Option<[u8; 32]> {
        self.current_row_digest
    }
}

/// Complete difference evidence between two locally retained acquisitions.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BoardRevisionEvidence {
    added_series: u64,
    changed_series: u64,
    removed_series: u64,
    added_observations: u64,
    changed_observations: u64,
    removed_observations: u64,
    unchanged_observations: u64,
    observation_changes: Vec<BoardObservationChange>,
}

impl BoardRevisionEvidence {
    /// Returns added series count.
    pub const fn added_series(&self) -> u64 {
        self.added_series
    }
    /// Returns metadata/content-changed series count.
    pub const fn changed_series(&self) -> u64 {
        self.changed_series
    }
    /// Returns removed series count.
    pub const fn removed_series(&self) -> u64 {
        self.removed_series
    }
    /// Returns added rows.
    pub const fn added_observations(&self) -> u64 {
        self.added_observations
    }
    /// Returns changed rows.
    pub const fn changed_observations(&self) -> u64 {
        self.changed_observations
    }
    /// Returns removed rows.
    pub const fn removed_observations(&self) -> u64 {
        self.removed_observations
    }
    /// Returns unchanged rows.
    pub const fn unchanged_observations(&self) -> u64 {
        self.unchanged_observations
    }
    /// Returns every changed row identity and before/after digest.
    pub fn observation_changes(&self) -> &[BoardObservationChange] {
        &self.observation_changes
    }
    fn has_normalized_change(&self) -> bool {
        self.added_series != 0
            || self.changed_series != 0
            || self.removed_series != 0
            || self.added_observations != 0
            || self.changed_observations != 0
            || self.removed_observations != 0
    }
}

/// Immutable publication receipt binding exact bytes, typed content, event, clocks, and revision.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BoardPublicationReceipt {
    schema_version: u16,
    family: BoardDatasetFamily,
    revision: RevisionNumber,
    event: BoardPublicationEvent,
    timing: BoardPublicationTiming,
    contract_digest: [u8; 32],
    request_digest: [u8; 32],
    source_payload_digest: [u8; 32],
    native_schema_digest: [u8; 32],
    normalized_content_digest: [u8; 32],
    predecessor_receipt_digest: Option<[u8; 32]>,
    revision_evidence: BoardRevisionEvidence,
    replacements: Vec<BoardSeriesReplacement>,
    route_lifecycle: BoardRouteLifecycle,
    vintage_capability: BoardVintageCapability,
    receipt_digest: [u8; 32],
}

impl BoardPublicationReceipt {
    /// Returns the receipt schema version.
    pub const fn schema_version(&self) -> u16 {
        self.schema_version
    }
    /// Returns the selected dataset family.
    pub const fn family(&self) -> BoardDatasetFamily {
        self.family
    }
    /// Returns the one-based locally retained revision.
    pub const fn revision(&self) -> RevisionNumber {
        self.revision
    }
    /// Returns the exact publication event.
    pub const fn event(&self) -> &BoardPublicationEvent {
        &self.event
    }
    /// Returns independent source/local clocks.
    pub const fn timing(&self) -> &BoardPublicationTiming {
        &self.timing
    }
    /// Returns the predecessor receipt identity.
    pub const fn predecessor_receipt_digest(&self) -> Option<[u8; 32]> {
        self.predecessor_receipt_digest
    }
    /// Returns the bound dataset-contract digest.
    pub const fn contract_digest(&self) -> [u8; 32] {
        self.contract_digest
    }
    /// Returns the exact acquisition-request digest.
    pub const fn request_digest(&self) -> [u8; 32] {
        self.request_digest
    }
    /// Returns the exact acquired-payload digest.
    pub const fn source_payload_digest(&self) -> [u8; 32] {
        self.source_payload_digest
    }
    /// Returns the exact native schema/header digest.
    pub const fn native_schema_digest(&self) -> [u8; 32] {
        self.native_schema_digest
    }
    /// Returns the normalized typed-content digest.
    pub const fn normalized_content_digest(&self) -> [u8; 32] {
        self.normalized_content_digest
    }
    /// Returns complete revision evidence.
    pub const fn revision_evidence(&self) -> &BoardRevisionEvidence {
        &self.revision_evidence
    }
    /// Returns explicit series replacements.
    pub fn replacements(&self) -> &[BoardSeriesReplacement] {
        &self.replacements
    }
    /// Returns the canonical receipt digest.
    pub const fn receipt_digest(&self) -> [u8; 32] {
        self.receipt_digest
    }
    /// Returns the current-definition/vintage limitation.
    pub const fn vintage_capability(&self) -> BoardVintageCapability {
        self.vintage_capability
    }
    /// Returns the source-evidenced route lifecycle.
    pub const fn route_lifecycle(&self) -> &BoardRouteLifecycle {
        &self.route_lifecycle
    }
}

/// Publication result distinguishing a new immutable generation from an exact retry.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum BoardPublicationOutcome {
    /// A new revision was admitted.
    Published { receipt: BoardPublicationReceipt },
    /// Exact acquired bytes were already published; no duplicate generation is created.
    ExactDuplicate {
        existing_receipt: BoardPublicationReceipt,
    },
}

/// Pure publication validator; durable transaction ownership remains in the shared store.
#[derive(Clone, Copy, Debug, Default)]
pub struct BoardPublisher;

impl BoardPublisher {
    /// Validates and constructs one immutable receipt or identifies an exact retry.
    pub fn publish(
        current: &ParsedBoardDataset,
        event: BoardPublicationEvent,
        timing: BoardPublicationTiming,
        previous: Option<(&ParsedBoardDataset, &BoardPublicationReceipt)>,
        replacements: Vec<BoardSeriesReplacement>,
    ) -> Result<BoardPublicationOutcome, BoardAdapterError> {
        timing.validate()?;
        if let Some((prior, receipt)) = previous {
            validate_predecessor(current, prior, receipt)?;
            if current.source_payload_digest() == prior.source_payload_digest() {
                return Ok(BoardPublicationOutcome::ExactDuplicate {
                    existing_receipt: receipt.clone(),
                });
            }
        }
        let evidence = revision_evidence(previous.map(|value| value.0), current)?;
        validate_event(event.kind, &timing, previous, &evidence, current)?;
        validate_replacements(
            event.kind,
            previous.map(|value| value.0),
            current,
            &replacements,
        )?;
        let revision_value = match previous {
            Some((_, receipt)) => receipt
                .revision
                .get()
                .checked_add(1)
                .ok_or(BoardAdapterError::CountOverflow)?,
            None => 1,
        };
        let revision =
            RevisionNumber::new(revision_value).map_err(|_| BoardAdapterError::CountOverflow)?;
        let predecessor_receipt_digest = previous.map(|value| value.1.receipt_digest);
        let mut receipt = BoardPublicationReceipt {
            schema_version: BOARD_PUBLICATION_RECEIPT_VERSION,
            family: current.family(),
            revision,
            event,
            timing,
            contract_digest: current.contract_digest(),
            request_digest: current.request_digest(),
            source_payload_digest: current.source_payload_digest(),
            native_schema_digest: current.native_schema_digest(),
            normalized_content_digest: current.normalized_content_digest(),
            predecessor_receipt_digest,
            revision_evidence: evidence,
            replacements,
            route_lifecycle: current.route_lifecycle().clone(),
            vintage_capability: BoardVintageCapability::LocallyRetainedAcquisitionsOnly,
            receipt_digest: [0; 32],
        };
        receipt.receipt_digest = publication_digest(&receipt);
        Ok(BoardPublicationOutcome::Published { receipt })
    }
}

fn validate_predecessor(
    current: &ParsedBoardDataset,
    prior: &ParsedBoardDataset,
    receipt: &BoardPublicationReceipt,
) -> Result<(), BoardAdapterError> {
    if current.family() != prior.family()
        || current.contract_digest() != prior.contract_digest()
        || receipt.family != prior.family()
        || receipt.contract_digest != prior.contract_digest()
        || receipt.request_digest != prior.request_digest()
        || receipt.source_payload_digest != prior.source_payload_digest()
        || receipt.native_schema_digest != prior.native_schema_digest()
        || receipt.normalized_content_digest != prior.normalized_content_digest()
        || receipt.receipt_digest != publication_digest(receipt)
    {
        Err(BoardAdapterError::PredecessorMismatch)
    } else {
        Ok(())
    }
}

fn validate_event(
    kind: BoardPublicationEventKind,
    timing: &BoardPublicationTiming,
    previous: Option<(&ParsedBoardDataset, &BoardPublicationReceipt)>,
    evidence: &BoardRevisionEvidence,
    current: &ParsedBoardDataset,
) -> Result<(), BoardAdapterError> {
    match (previous, kind) {
        (None, BoardPublicationEventKind::ScheduledRelease) if timing.scheduled_for.is_some() => {
            Ok(())
        }
        (None, BoardPublicationEventKind::HistoricalArchive) => Ok(()),
        (None, _) => Err(BoardAdapterError::InvalidRevisionEvidence),
        (Some((prior, _)), BoardPublicationEventKind::Repost)
            if !evidence.has_normalized_change()
                && prior.normalized_content_digest() == current.normalized_content_digest() =>
        {
            Ok(())
        }
        (Some(_), BoardPublicationEventKind::OffScheduleCorrection)
            if evidence.has_normalized_change() && timing.correction_published.is_some() =>
        {
            Ok(())
        }
        (
            Some(_),
            BoardPublicationEventKind::ScheduledRelease | BoardPublicationEventKind::AnnualRevision,
        ) if evidence.has_normalized_change() && timing.scheduled_for.is_some() => Ok(()),
        _ => Err(BoardAdapterError::InvalidRevisionEvidence),
    }
}

fn validate_replacements(
    kind: BoardPublicationEventKind,
    previous: Option<&ParsedBoardDataset>,
    current: &ParsedBoardDataset,
    replacements: &[BoardSeriesReplacement],
) -> Result<(), BoardAdapterError> {
    if !replacements.is_empty()
        && !matches!(
            kind,
            BoardPublicationEventKind::AnnualRevision
                | BoardPublicationEventKind::OffScheduleCorrection
        )
    {
        return Err(BoardAdapterError::InvalidRevisionEvidence);
    }
    let old_ids = previous
        .into_iter()
        .flat_map(ParsedBoardDataset::series)
        .map(|item| item.unique_id())
        .collect::<BTreeSet<_>>();
    let new_ids = current
        .series()
        .iter()
        .map(|item| item.unique_id())
        .collect::<BTreeSet<_>>();
    let mut pairs = BTreeSet::new();
    for replacement in replacements {
        if !old_ids.contains(replacement.predecessor_unique_id())
            || !new_ids.contains(replacement.successor_unique_id())
            || !pairs.insert((
                replacement.predecessor_unique_id(),
                replacement.successor_unique_id(),
            ))
        {
            return Err(BoardAdapterError::InvalidRevisionEvidence);
        }
    }
    Ok(())
}

fn revision_evidence(
    previous: Option<&ParsedBoardDataset>,
    current: &ParsedBoardDataset,
) -> Result<BoardRevisionEvidence, BoardAdapterError> {
    let prior_series = series_map(previous);
    let current_series = series_map(Some(current));
    let added_series = current_series
        .keys()
        .filter(|key| !prior_series.contains_key(*key))
        .count() as u64;
    let removed_series = prior_series
        .keys()
        .filter(|key| !current_series.contains_key(*key))
        .count() as u64;
    let changed_series = current_series
        .iter()
        .filter(|(key, digest)| prior_series.get(*key).is_some_and(|prior| prior != *digest))
        .count() as u64;
    let prior_rows = row_map(previous);
    let current_rows = row_map(Some(current));
    let mut changes = Vec::new();
    let mut added = 0_u64;
    let mut changed = 0_u64;
    let mut removed = 0_u64;
    let mut unchanged = 0_u64;
    for (key, current_digest) in &current_rows {
        match prior_rows.get(key) {
            None => {
                added += 1;
                changes.push(change(key, None, Some(*current_digest)));
            }
            Some(prior_digest) if prior_digest != current_digest => {
                changed += 1;
                changes.push(change(key, Some(*prior_digest), Some(*current_digest)));
            }
            Some(_) => unchanged += 1,
        }
    }
    for (key, prior_digest) in &prior_rows {
        if !current_rows.contains_key(key) {
            removed += 1;
            changes.push(change(key, Some(*prior_digest), None));
        }
    }
    Ok(BoardRevisionEvidence {
        added_series,
        changed_series,
        removed_series,
        added_observations: added,
        changed_observations: changed,
        removed_observations: removed,
        unchanged_observations: unchanged,
        observation_changes: changes,
    })
}

fn series_map(dataset: Option<&ParsedBoardDataset>) -> BTreeMap<String, [u8; 32]> {
    dataset
        .into_iter()
        .flat_map(ParsedBoardDataset::series)
        .map(|series| (series.unique_id().to_owned(), series.series_digest()))
        .collect()
}

fn row_map(dataset: Option<&ParsedBoardDataset>) -> BTreeMap<(String, String), [u8; 32]> {
    dataset
        .into_iter()
        .flat_map(ParsedBoardDataset::series)
        .flat_map(|series| {
            series.observations().iter().map(move |row| {
                (
                    (series.unique_id().to_owned(), row.period().raw().to_owned()),
                    row.row_digest(),
                )
            })
        })
        .collect()
}

fn change(
    key: &(String, String),
    previous: Option<[u8; 32]>,
    current: Option<[u8; 32]>,
) -> BoardObservationChange {
    BoardObservationChange {
        series_unique_id: key.0.clone().into(),
        period: key.1.clone().into(),
        previous_row_digest: previous,
        current_row_digest: current,
    }
}

fn publication_digest(receipt: &BoardPublicationReceipt) -> [u8; 32] {
    let mut digest = Sha256::new();
    update_tag(
        &mut digest,
        "market-squawk-federal-reserve-publication-receipt-v1",
    );
    update_u64(&mut digest, u64::from(receipt.schema_version));
    update_tag(&mut digest, receipt.family.as_str());
    update_u64(&mut digest, u64::from(receipt.revision.get()));
    update_tag(&mut digest, receipt.event.kind.as_str());
    update_tag(&mut digest, &receipt.event.notice_id);
    update_bytes(&mut digest, &receipt.event.notice_digest);
    update_timing(&mut digest, &receipt.timing);
    for value in [
        receipt.contract_digest,
        receipt.request_digest,
        receipt.source_payload_digest,
        receipt.native_schema_digest,
        receipt.normalized_content_digest,
    ] {
        update_bytes(&mut digest, &value);
    }
    match receipt.predecessor_receipt_digest {
        Some(value) => {
            update_bool(&mut digest, true);
            update_bytes(&mut digest, &value);
        }
        None => update_bool(&mut digest, false),
    }
    update_evidence(&mut digest, &receipt.revision_evidence);
    update_u64(&mut digest, receipt.replacements.len() as u64);
    for replacement in &receipt.replacements {
        update_tag(&mut digest, &replacement.predecessor_unique_id);
        update_tag(&mut digest, &replacement.successor_unique_id);
        update_tag(&mut digest, &replacement.effective_period);
        update_bytes(&mut digest, &replacement.evidence_digest);
    }
    update_lifecycle(&mut digest, &receipt.route_lifecycle);
    finish(digest)
}

fn update_timing(digest: &mut Sha256, timing: &BoardPublicationTiming) {
    for value in [
        &timing.scheduled_for,
        &timing.source_published,
        &timing.correction_published,
    ] {
        match value {
            Some(coordinate) => {
                update_bool(digest, true);
                update_coordinate(digest, coordinate);
            }
            None => update_bool(digest, false),
        }
    }
    update_i64(digest, timing.route_available_at.unix_nanos());
    update_i64(digest, timing.received_at.unix_nanos());
    update_i64(digest, timing.parsed_at.unix_nanos());
}

fn update_coordinate(digest: &mut Sha256, coordinate: &ResearchTemporalCoordinate) {
    update_tag(digest, coordinate.precision().as_str());
    if let Some(value) = coordinate.exact_timestamp() {
        update_i64(digest, value.unix_nanos());
    }
    if let Some(value) = coordinate.calendar_date_value() {
        update_tag(digest, &value.to_string());
    }
    if let Some(value) = coordinate.source_period_value() {
        update_tag(digest, value.scheme().as_str());
        update_u64(digest, u64::from(value.year()));
        update_u64(digest, u64::from(value.ordinal().get()));
        update_tag(digest, value.code().as_str());
    }
}

fn update_evidence(digest: &mut Sha256, evidence: &BoardRevisionEvidence) {
    for value in [
        evidence.added_series,
        evidence.changed_series,
        evidence.removed_series,
        evidence.added_observations,
        evidence.changed_observations,
        evidence.removed_observations,
        evidence.unchanged_observations,
    ] {
        update_u64(digest, value);
    }
    update_u64(digest, evidence.observation_changes.len() as u64);
    for change in &evidence.observation_changes {
        update_tag(digest, &change.series_unique_id);
        update_tag(digest, &change.period);
        for value in [change.previous_row_digest, change.current_row_digest] {
            match value {
                Some(value) => {
                    update_bool(digest, true);
                    update_bytes(digest, &value);
                }
                None => update_bool(digest, false),
            }
        }
    }
}

fn update_lifecycle(digest: &mut Sha256, lifecycle: &BoardRouteLifecycle) {
    match lifecycle {
        BoardRouteLifecycle::DdpTransitionAnnounced {
            announced_on,
            build_your_package_removal_week,
            board_release_xml_remains_candidate,
            fred_is_separate_provenance,
        } => {
            update_tag(digest, "ddp_transition_announced");
            update_tag(digest, &announced_on.to_string());
            update_tag(digest, &build_your_package_removal_week.to_string());
            update_bool(digest, *board_release_xml_remains_candidate);
            update_bool(digest, *fred_is_separate_provenance);
        }
        BoardRouteLifecycle::Active => update_tag(digest, "active"),
        BoardRouteLifecycle::Discontinued {
            last_observation_period,
            historical_files_remain,
        } => {
            update_tag(digest, "discontinued");
            update_tag(digest, last_observation_period);
            update_bool(digest, *historical_files_remain);
        }
        BoardRouteLifecycle::Replaced {
            replacement_route,
            effective_on,
        } => {
            update_tag(digest, "replaced");
            update_tag(digest, replacement_route);
            update_tag(digest, &effective_on.to_string());
        }
    }
}
