//! Public bounded XBRL extraction result types.

use market_squawk_domain::{
    ExactPayloadEvidence, SourceIdentifier, Timestamp, XbrlFactEvidence,
    XbrlOccurrenceRelationships, XbrlQualifiedName, XbrlTaxonomySet, XbrlText,
};
use rust_decimal::Decimal;

/// Immutable document-level evidence shared by every parsed occurrence.
#[derive(Clone, Debug)]
pub struct XbrlDocumentContext {
    pub(super) accession: SourceIdentifier,
    pub(super) taxonomy_set: XbrlTaxonomySet,
    pub(super) source_payload: ExactPayloadEvidence,
    pub(super) evaluated_at: Timestamp,
}

impl XbrlDocumentContext {
    /// Binds parser output to accession, taxonomy set, exact payload, and evaluation time.
    pub const fn new(
        accession: SourceIdentifier,
        taxonomy_set: XbrlTaxonomySet,
        source_payload: ExactPayloadEvidence,
        evaluated_at: Timestamp,
    ) -> Self {
        Self {
            accession,
            taxonomy_set,
            source_payload,
            evaluated_at,
        }
    }
}

/// One exact normalized numeric fact plus its full occurrence evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct XbrlNumericFact {
    pub(super) concept: SourceIdentifier,
    pub(super) unit: SourceIdentifier,
    pub(super) value: Decimal,
    pub(super) evidence: XbrlFactEvidence,
}

impl XbrlNumericFact {
    /// Returns the qualified concept identity.
    pub const fn concept(&self) -> &SourceIdentifier {
        &self.concept
    }
    /// Returns the normalized unit identity.
    pub const fn unit(&self) -> &SourceIdentifier {
        &self.unit
    }
    /// Returns the exact normalized decimal.
    pub const fn value(&self) -> Decimal {
        self.value
    }
    /// Returns occurrence-level audit evidence.
    pub const fn evidence(&self) -> &XbrlFactEvidence {
        &self.evidence
    }
}

/// Nil or nonnumeric occurrence retained without fabricating a Decimal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct XbrlNonnumericOccurrence {
    pub(super) occurrence_id: SourceIdentifier,
    pub(super) accession: SourceIdentifier,
    pub(super) concept: XbrlQualifiedName,
    pub(super) context_id: SourceIdentifier,
    pub(super) lexical_value: XbrlText,
    pub(super) nil: bool,
    pub(super) source_payload: ExactPayloadEvidence,
    pub(super) occurrence_relationships: XbrlOccurrenceRelationships,
}

impl XbrlNonnumericOccurrence {
    /// Returns the source or deterministic occurrence identity.
    pub const fn occurrence_id(&self) -> &SourceIdentifier {
        &self.occurrence_id
    }

    /// Returns the source lexical and resolved concept QName.
    pub const fn concept(&self) -> &XbrlQualifiedName {
        &self.concept
    }

    /// Returns the exact bounded text, empty only for an explicit nil occurrence.
    pub const fn lexical_value(&self) -> &XbrlText {
        &self.lexical_value
    }
    /// Returns whether the occurrence carried explicit nil semantics.
    pub const fn is_nil(&self) -> bool {
        self.nil
    }

    /// Returns nesting, continuation, and explanatory relationship evidence.
    pub const fn occurrence_relationships(&self) -> &XbrlOccurrenceRelationships {
        &self.occurrence_relationships
    }
}

/// Parsed XBRL output preserving numeric and nonnumeric occurrence families separately.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedXbrlDocument {
    pub(super) numeric_facts: Vec<XbrlNumericFact>,
    pub(super) nonnumeric_occurrences: Vec<XbrlNonnumericOccurrence>,
}

impl ParsedXbrlDocument {
    /// Returns normalized numeric facts.
    pub fn numeric_facts(&self) -> &[XbrlNumericFact] {
        &self.numeric_facts
    }
    /// Returns nil and nonnumeric occurrences.
    pub fn nonnumeric_occurrences(&self) -> &[XbrlNonnumericOccurrence] {
        &self.nonnumeric_occurrences
    }
}
