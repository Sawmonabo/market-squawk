//! Bounded streaming parser for XBRL instances and Inline XBRL numeric facts.

mod model;
mod normalize;
mod support;
mod wire;

use std::collections::{BTreeMap, BTreeSet};

use crate::SecParserLimits;
use market_squawk_domain::{
    CalendarDate, DigestAlgorithm, EvidenceDigest, SourceIdentifier, XbrlAccuracy,
    XbrlDimensionEvidence, XbrlDimensionLocation, XbrlDimensionMember, XbrlDuplicateClass,
    XbrlDuplicateEvidence, XbrlFactEvidence, XbrlFactEvidenceInput, XbrlPeriod, XbrlSign, XbrlText,
};
use quick_xml::Reader;
use quick_xml::events::{BytesStart, Event};
use rust_decimal::Decimal;
use sha2::{Digest as _, Sha256};

pub use model::{
    ParsedXbrlDocument, XbrlDocumentContext, XbrlNonnumericOccurrence, XbrlNumericFact,
};
use normalize::NormalizedDraft;
pub use support::SecXbrlError;
use wire::*;

/// Bounded XBRL/Inline-XBRL parser.
#[derive(Clone, Copy, Debug, Default)]
pub struct XbrlDocumentParser;

impl XbrlDocumentParser {
    /// Parses one exact filing document using a non-DOM pull reader.
    pub fn parse(
        bytes: &[u8],
        limits: SecParserLimits,
        document: XbrlDocumentContext,
    ) -> Result<ParsedXbrlDocument, SecXbrlError> {
        if bytes.len() > limits.decoded_bytes() {
            return Err(SecXbrlError::ByteLimitExceeded);
        }
        let mut reader = Reader::from_reader(bytes);
        reader.config_mut().trim_text(false);
        reader.config_mut().expand_empty_elements = true;
        let mut state = ParserState::new(limits, document);
        loop {
            match reader.read_event()? {
                Event::Start(start) => state.start(&reader, &start)?,
                Event::End(end) => state.end(name_text(end.name().as_ref(), limits)?)?,
                Event::Text(text) => {
                    let decoded = text.xml10_content()?;
                    let unescaped = quick_xml::escape::unescape(&decoded)?;
                    state.text(&unescaped)?;
                }
                Event::CData(text) => state.text(&text.decode()?)?,
                Event::DocType(_) => return Err(SecXbrlError::DoctypeForbidden),
                Event::Eof => break,
                Event::Decl(_) | Event::PI(_) | Event::Comment(_) | Event::GeneralRef(_) => {}
                Event::Empty(_) => return Err(SecXbrlError::ParserInvariant),
            }
        }
        state.finish()
    }
}

struct ParserState {
    limits: SecParserLimits,
    document: XbrlDocumentContext,
    depth: usize,
    contexts: BTreeMap<String, ContextDraft>,
    units: BTreeMap<String, String>,
    current_context: Option<ContextDraft>,
    current_unit: Option<UnitDraft>,
    segment_depth: Option<usize>,
    capture: Option<Capture>,
    active_fact: Option<FactDraft>,
    active_continuation: Option<ContinuationDraft>,
    continuations: BTreeMap<String, ContinuationDraft>,
    exclude_depth: Option<usize>,
    facts: Vec<FactDraft>,
}

impl ParserState {
    fn new(limits: SecParserLimits, document: XbrlDocumentContext) -> Self {
        Self {
            limits,
            document,
            depth: 0,
            contexts: BTreeMap::new(),
            units: BTreeMap::new(),
            current_context: None,
            current_unit: None,
            segment_depth: None,
            capture: None,
            active_fact: None,
            active_continuation: None,
            continuations: BTreeMap::new(),
            exclude_depth: None,
            facts: Vec::new(),
        }
    }

    fn start(
        &mut self,
        reader: &Reader<&[u8]>,
        start: &BytesStart<'_>,
    ) -> Result<(), SecXbrlError> {
        self.depth = self
            .depth
            .checked_add(1)
            .ok_or(SecXbrlError::DepthLimitExceeded)?;
        if self.depth > self.limits.depth() {
            return Err(SecXbrlError::DepthLimitExceeded);
        }
        let name = name_text(start.name().as_ref(), self.limits)?.to_owned();
        let local = local_name(&name);
        let attributes = attributes(reader, start, self.limits)?;
        if local == "continuation" {
            if self.active_continuation.is_some() || self.active_fact.is_some() {
                return Err(SecXbrlError::NestedContinuation);
            }
            self.active_continuation = Some(ContinuationDraft {
                start_depth: self.depth,
                id: required_attr(&attributes, "id")?,
                continued_at: attr(&attributes, "continuedAt").map(str::to_owned),
                text: String::new(),
            });
            return Ok(());
        }
        if local == "exclude" {
            if self.exclude_depth.is_some() {
                return Err(SecXbrlError::NestedExclude);
            }
            self.exclude_depth = Some(self.depth);
            return Ok(());
        }
        if local == "context" {
            if self.current_context.is_some() {
                return Err(SecXbrlError::NestedContext);
            }
            self.current_context = Some(ContextDraft::new(required_attr(&attributes, "id")?));
            return Ok(());
        }
        if local == "unit" {
            if self.current_unit.is_some() {
                return Err(SecXbrlError::NestedUnit);
            }
            self.current_unit = Some(UnitDraft::new(required_attr(&attributes, "id")?));
            return Ok(());
        }
        if self.current_context.is_some() {
            match local {
                "identifier" => {
                    self.begin_capture(CaptureKind::Identifier {
                        scheme: required_attr(&attributes, "scheme")?,
                    })?;
                }
                "instant" => self.begin_capture(CaptureKind::Instant)?,
                "startDate" => self.begin_capture(CaptureKind::StartDate)?,
                "endDate" => self.begin_capture(CaptureKind::EndDate)?,
                "explicitMember" => {
                    self.begin_capture(CaptureKind::ExplicitMember {
                        dimension: required_attr(&attributes, "dimension")?,
                        location: if self.segment_depth.is_some() {
                            XbrlDimensionLocation::Segment
                        } else {
                            XbrlDimensionLocation::Scenario
                        },
                    })?;
                }
                "typedMember" => {
                    self.begin_capture(CaptureKind::TypedMember {
                        dimension: required_attr(&attributes, "dimension")?,
                        location: if self.segment_depth.is_some() {
                            XbrlDimensionLocation::Segment
                        } else {
                            XbrlDimensionLocation::Scenario
                        },
                    })?;
                }
                "segment" => self.segment_depth = Some(self.depth),
                _ => {}
            }
            return Ok(());
        }
        if self.current_unit.is_some() {
            if local == "measure" {
                self.begin_capture(CaptureKind::Measure)?;
            }
            return Ok(());
        }
        if let Some(context_ref) = attr(&attributes, "contextRef") {
            if self.active_fact.is_some() {
                return Err(SecXbrlError::NestedFact);
            }
            let inline = local == "nonFraction" || local == "nonNumeric";
            let concept = if inline {
                required_attr(&attributes, "name")?
            } else {
                name.clone()
            };
            self.active_fact = Some(FactDraft {
                start_depth: self.depth,
                concept,
                context_id: context_ref.to_owned(),
                unit_id: attr(&attributes, "unitRef").map(str::to_owned),
                occurrence_id: attr(&attributes, "id").map(str::to_owned),
                accuracy: parse_accuracy(&attributes)?,
                scale: attr(&attributes, "scale").map(parse_i32).transpose()?,
                sign: attr(&attributes, "sign").map(parse_sign).transpose()?,
                format: attr(&attributes, "format").map(str::to_owned),
                language: attr(&attributes, "xml:lang")
                    .or_else(|| attr(&attributes, "lang"))
                    .map(str::to_owned),
                nil: attr(&attributes, "xsi:nil")
                    .or_else(|| attr(&attributes, "nil"))
                    .is_some_and(is_true),
                explicitly_nonnumeric: local == "nonNumeric",
                continued_at: attr(&attributes, "continuedAt").map(str::to_owned),
                text: String::new(),
            });
        }
        Ok(())
    }

    fn begin_capture(&mut self, kind: CaptureKind) -> Result<(), SecXbrlError> {
        if self.capture.is_some() {
            return Err(SecXbrlError::NestedCapture);
        }
        self.capture = Some(Capture {
            depth: self.depth,
            kind,
            text: String::new(),
        });
        Ok(())
    }

    fn text(&mut self, text: &str) -> Result<(), SecXbrlError> {
        if text.len() > self.limits.string_bytes() {
            return Err(SecXbrlError::StringLimitExceeded);
        }
        if self.exclude_depth.is_none()
            && let Some(fact) = &mut self.active_fact
        {
            append_bounded(&mut fact.text, text, self.limits.string_bytes())?;
        }
        if self.exclude_depth.is_none()
            && let Some(continuation) = &mut self.active_continuation
        {
            append_bounded(&mut continuation.text, text, self.limits.string_bytes())?;
        }
        if let Some(capture) = &mut self.capture {
            append_bounded(&mut capture.text, text, self.limits.string_bytes())?;
        }
        if self.segment_depth.is_some()
            && let Some(context) = &mut self.current_context
        {
            append_bounded(&mut context.segment_text, text, self.limits.string_bytes())?;
        }
        Ok(())
    }

    fn end(&mut self, name: &str) -> Result<(), SecXbrlError> {
        if self.depth == 0 {
            return Err(SecXbrlError::ParserInvariant);
        }
        let local = local_name(name);
        if self
            .active_continuation
            .as_ref()
            .is_some_and(|continuation| continuation.start_depth == self.depth)
        {
            let continuation = self
                .active_continuation
                .take()
                .ok_or(SecXbrlError::ParserInvariant)?;
            if self
                .continuations
                .insert(continuation.id.clone(), continuation)
                .is_some()
            {
                return Err(SecXbrlError::DuplicateIdentity);
            }
        }
        if self
            .capture
            .as_ref()
            .is_some_and(|capture| capture.depth == self.depth)
        {
            let capture = self.capture.take().ok_or(SecXbrlError::ParserInvariant)?;
            self.finish_capture(capture)?;
        }
        if self
            .active_fact
            .as_ref()
            .is_some_and(|fact| fact.start_depth == self.depth)
        {
            let fact = self
                .active_fact
                .take()
                .ok_or(SecXbrlError::ParserInvariant)?;
            if self.facts.len() >= self.limits.records() {
                return Err(SecXbrlError::RecordLimitExceeded);
            }
            self.facts.push(fact);
        }
        if local == "segment" && self.segment_depth == Some(self.depth) {
            self.segment_depth = None;
        }
        if local == "exclude" && self.exclude_depth == Some(self.depth) {
            self.exclude_depth = None;
        }
        if local == "context" {
            let context = self
                .current_context
                .take()
                .ok_or(SecXbrlError::ParserInvariant)?;
            if self.contexts.insert(context.id.clone(), context).is_some() {
                return Err(SecXbrlError::DuplicateIdentity);
            }
        }
        if local == "unit" {
            let unit = self
                .current_unit
                .take()
                .ok_or(SecXbrlError::ParserInvariant)?;
            let measure = unit.measure.ok_or(SecXbrlError::IncompleteUnit)?;
            if self.units.insert(unit.id, measure).is_some() {
                return Err(SecXbrlError::DuplicateIdentity);
            }
        }
        self.depth -= 1;
        Ok(())
    }

    fn finish_capture(&mut self, capture: Capture) -> Result<(), SecXbrlError> {
        let text = capture.text.trim().to_owned();
        match capture.kind {
            CaptureKind::Measure => {
                self.current_unit
                    .as_mut()
                    .ok_or(SecXbrlError::ParserInvariant)?
                    .measure = Some(text);
            }
            CaptureKind::Identifier { scheme } => {
                let context = self
                    .current_context
                    .as_mut()
                    .ok_or(SecXbrlError::ParserInvariant)?;
                context.entity_scheme = Some(scheme);
                context.entity_value = Some(text);
            }
            CaptureKind::Instant => {
                self.current_context
                    .as_mut()
                    .ok_or(SecXbrlError::ParserInvariant)?
                    .instant = Some(parse_date(&text)?);
            }
            CaptureKind::StartDate => {
                self.current_context
                    .as_mut()
                    .ok_or(SecXbrlError::ParserInvariant)?
                    .start = Some(parse_date(&text)?);
            }
            CaptureKind::EndDate => {
                self.current_context
                    .as_mut()
                    .ok_or(SecXbrlError::ParserInvariant)?
                    .end = Some(parse_date(&text)?);
            }
            CaptureKind::ExplicitMember {
                dimension,
                location,
            } => {
                self.current_context
                    .as_mut()
                    .ok_or(SecXbrlError::ParserInvariant)?
                    .dimensions
                    .push(XbrlDimensionEvidence::new(
                        SourceIdentifier::try_from(dimension)?,
                        XbrlDimensionMember::Explicit {
                            member: SourceIdentifier::try_from(text)?,
                        },
                        location,
                    ));
            }
            CaptureKind::TypedMember {
                dimension,
                location,
            } => {
                let digest: [u8; 32] = Sha256::digest(text.as_bytes()).into();
                self.current_context
                    .as_mut()
                    .ok_or(SecXbrlError::ParserInvariant)?
                    .dimensions
                    .push(XbrlDimensionEvidence::new(
                        SourceIdentifier::try_from(dimension)?,
                        XbrlDimensionMember::Typed {
                            canonical_value: XbrlText::try_from(text)?,
                            payload_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, digest),
                        },
                        location,
                    ));
            }
        }
        Ok(())
    }

    fn finish(mut self) -> Result<ParsedXbrlDocument, SecXbrlError> {
        if self.depth != 0
            || self.current_context.is_some()
            || self.current_unit.is_some()
            || self.active_fact.is_some()
            || self.active_continuation.is_some()
            || self.capture.is_some()
            || self.exclude_depth.is_some()
        {
            return Err(SecXbrlError::UnexpectedEof);
        }
        for fact in &mut self.facts {
            let mut next = fact.continued_at.take();
            let mut seen = BTreeSet::new();
            while let Some(continuation_id) = next {
                if !seen.insert(continuation_id.clone()) {
                    return Err(SecXbrlError::ContinuationCycle);
                }
                let continuation = self
                    .continuations
                    .get(&continuation_id)
                    .ok_or(SecXbrlError::UnknownContinuation)?;
                append_bounded(
                    &mut fact.text,
                    &continuation.text,
                    self.limits.string_bytes(),
                )?;
                next = continuation.continued_at.clone();
            }
        }
        let mut normalized = Vec::with_capacity(self.facts.len());
        for (index, fact) in self.facts.into_iter().enumerate() {
            normalized.push(NormalizedDraft::try_new(
                fact,
                index,
                &self.contexts,
                &self.units,
                &self.document,
            )?);
        }
        let mut group_counts: BTreeMap<(String, String, String), (usize, Option<Decimal>)> =
            BTreeMap::new();
        for draft in &normalized {
            if let NormalizedDraft::Numeric {
                concept,
                context_id,
                unit,
                value,
                ..
            } = draft
            {
                let group = group_counts
                    .entry((
                        concept.as_str().to_owned(),
                        context_id.as_str().to_owned(),
                        unit.as_str().to_owned(),
                    ))
                    .or_insert((0, Some(*value)));
                group.0 += 1;
                if group.1 != Some(*value) {
                    group.1 = None;
                }
            }
        }
        let mut numeric_facts = Vec::new();
        let mut nonnumeric_occurrences = Vec::new();
        for draft in normalized {
            match draft {
                NormalizedDraft::Nonnumeric(value) => nonnumeric_occurrences.push(value),
                NormalizedDraft::Numeric {
                    concept,
                    context_id,
                    unit,
                    value,
                    evidence_input,
                } => {
                    let group_key = (
                        concept.as_str().to_owned(),
                        context_id.as_str().to_owned(),
                        unit.as_str().to_owned(),
                    );
                    let (count, consistent) = group_counts
                        .get(&group_key)
                        .copied()
                        .ok_or(SecXbrlError::ParserInvariant)?;
                    let classification = if count == 1 {
                        XbrlDuplicateClass::Unique
                    } else if consistent.is_some() {
                        XbrlDuplicateClass::ConsistentNumeric
                    } else {
                        XbrlDuplicateClass::Inconsistent
                    };
                    let group_id = if count == 1 {
                        None
                    } else {
                        let digest: [u8; 32] = Sha256::digest(format!(
                            "{}|{}|{}",
                            group_key.0, group_key.1, group_key.2
                        ))
                        .into();
                        Some(SourceIdentifier::try_from(format!(
                            "xbrl-duplicate-{}",
                            hex_prefix(&digest, 16)
                        ))?)
                    };
                    let evidence = XbrlFactEvidence::try_new(XbrlFactEvidenceInput {
                        duplicate: XbrlDuplicateEvidence::try_new(
                            classification,
                            group_id,
                            SourceIdentifier::try_from("sec-xbrl-duplicate-v1")?,
                        )?,
                        ..*evidence_input
                    })?;
                    evidence.validate_value(value)?;
                    numeric_facts.push(XbrlNumericFact {
                        concept,
                        unit,
                        value,
                        evidence,
                    });
                }
            }
        }
        Ok(ParsedXbrlDocument {
            numeric_facts,
            nonnumeric_occurrences,
        })
    }
}

struct ContextDraft {
    id: String,
    entity_scheme: Option<String>,
    entity_value: Option<String>,
    instant: Option<CalendarDate>,
    start: Option<CalendarDate>,
    end: Option<CalendarDate>,
    dimensions: Vec<XbrlDimensionEvidence>,
    segment_text: String,
}

impl ContextDraft {
    fn new(id: String) -> Self {
        Self {
            id,
            entity_scheme: None,
            entity_value: None,
            instant: None,
            start: None,
            end: None,
            dimensions: Vec::new(),
            segment_text: String::new(),
        }
    }
    fn period(&self) -> Result<XbrlPeriod, SecXbrlError> {
        match (self.instant, self.start, self.end) {
            (Some(instant), None, None) => Ok(XbrlPeriod::instant(instant)),
            (None, Some(start), Some(end)) => Ok(XbrlPeriod::duration(start, end)?),
            _ => Err(SecXbrlError::IncompleteContext),
        }
    }
}

struct UnitDraft {
    id: String,
    measure: Option<String>,
}
impl UnitDraft {
    fn new(id: String) -> Self {
        Self { id, measure: None }
    }
}

struct Capture {
    depth: usize,
    kind: CaptureKind,
    text: String,
}
enum CaptureKind {
    Identifier {
        scheme: String,
    },
    Instant,
    StartDate,
    EndDate,
    ExplicitMember {
        dimension: String,
        location: XbrlDimensionLocation,
    },
    TypedMember {
        dimension: String,
        location: XbrlDimensionLocation,
    },
    Measure,
}

struct FactDraft {
    start_depth: usize,
    concept: String,
    context_id: String,
    unit_id: Option<String>,
    occurrence_id: Option<String>,
    accuracy: XbrlAccuracy,
    scale: Option<i32>,
    sign: Option<XbrlSign>,
    format: Option<String>,
    language: Option<String>,
    nil: bool,
    explicitly_nonnumeric: bool,
    continued_at: Option<String>,
    text: String,
}

struct ContinuationDraft {
    start_depth: usize,
    id: String,
    continued_at: Option<String>,
    text: String,
}
