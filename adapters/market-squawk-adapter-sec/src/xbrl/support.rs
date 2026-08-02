//! Typed XBRL parser failures.

use thiserror::Error;

/// Bounded filing XBRL parse failure.
#[derive(Debug, Error)]
pub enum SecXbrlError {
    #[error("XBRL input exceeds its decoded-byte bound")]
    ByteLimitExceeded,
    #[error("XBRL nesting exceeds its depth bound")]
    DepthLimitExceeded,
    #[error("XBRL text exceeds its string bound")]
    StringLimitExceeded,
    #[error("XBRL fact count exceeds its record bound")]
    RecordLimitExceeded,
    #[error("XBRL retained output exceeds its aggregate byte bound")]
    RetainedOutputLimitExceeded,
    #[error("XBRL attribute count exceeds its bound")]
    AttributeLimitExceeded,
    #[error("XBRL DTDs are forbidden")]
    DoctypeForbidden,
    #[error("XBRL parser state invariant failed")]
    ParserInvariant,
    #[error("XBRL input ended with incomplete structures")]
    UnexpectedEof,
    #[error("XBRL contains invalid UTF-8")]
    InvalidUtf8,
    #[error("XBRL attribute is missing")]
    MissingAttribute,
    #[error("XBRL element contains a duplicate attribute")]
    DuplicateAttribute,
    #[error("XBRL semantic attribute is ambiguous across namespace authorities")]
    AmbiguousSemanticAttribute,
    #[error("XBRL QName uses an unknown namespace prefix")]
    UnknownNamespacePrefix,
    #[error("XBRL identity is duplicated")]
    DuplicateIdentity,
    #[error("XBRL context is nested")]
    NestedContext,
    #[error("XBRL unit is nested")]
    NestedUnit,
    #[error("XBRL fact is nested")]
    NestedFact,
    #[error("XBRL capture structure is nested unexpectedly")]
    NestedCapture,
    #[error("Inline XBRL continuation is nested unexpectedly")]
    NestedContinuation,
    #[error("Inline XBRL exclusion is nested unexpectedly")]
    NestedExclude,
    #[error("XBRL context is incomplete")]
    IncompleteContext,
    #[error("XBRL unit is incomplete")]
    IncompleteUnit,
    #[error("XBRL unit expression is invalid")]
    InvalidUnitExpression,
    #[error("XBRL fact references an unknown context")]
    UnknownContext,
    #[error("XBRL fact references an unknown unit")]
    UnknownUnit,
    #[error("Inline XBRL fact references an unknown continuation")]
    UnknownContinuation,
    #[error("Inline XBRL continuation chain contains a cycle")]
    ContinuationCycle,
    #[error("Inline XBRL relationship references an unknown fact occurrence")]
    UnknownRelationshipReference,
    #[error("XBRL fact has conflicting accuracy attributes")]
    ConflictingAccuracy,
    #[error("XBRL numeric fact is invalid or out of range")]
    InvalidNumericFact,
    #[error("Inline XBRL numeric transform is unsupported")]
    UnsupportedTransform,
    #[error("XBRL date is invalid")]
    InvalidDate,
    #[error(transparent)]
    Xml(#[from] quick_xml::Error),
    #[error(transparent)]
    Attribute(#[from] quick_xml::events::attributes::AttrError),
    #[error(transparent)]
    Encoding(#[from] quick_xml::encoding::EncodingError),
    #[error(transparent)]
    Escape(#[from] quick_xml::escape::EscapeError),
    #[error(transparent)]
    Identity(#[from] market_squawk_domain::IdentityError),
    #[error(transparent)]
    Evidence(#[from] market_squawk_domain::XbrlEvidenceError),
}
