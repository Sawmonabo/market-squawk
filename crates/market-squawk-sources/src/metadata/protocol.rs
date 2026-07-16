/// Historical and point-in-time behavior declared by a source.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoricalCapability {
    /// No historical extraction interface is declared.
    None,
    /// Current and historical observations are available without revision vintages.
    Historical,
    /// Publication vintages and superseded revisions are retained.
    RevisionPreserving,
}

/// Provider sequence semantics bound to an exact validation-rule revision.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SequenceValidationProfile {
    /// The selected channel authoritatively supplies no sequence.
    Unsupported { rule: IntegrityRule },
    /// Every observed sequence is validated under the named provider rule.
    Provided {
        /// Exact validation rule identity and revision.
        rule: IntegrityRule,
        /// Provider progression semantics.
        progression: SequenceValidationRule,
    },
}

impl SequenceValidationProfile {
    const fn capability(&self) -> SequenceCapability {
        match self {
            Self::Unsupported { .. } => SequenceCapability::Unsupported,
            Self::Provided { .. } => SequenceCapability::Provided,
        }
    }
}

/// Checksum algorithm implemented by a provider-specific validator.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChecksumAlgorithm {
    /// ISO-HDLC CRC-32.
    Crc32IsoHdlc,
    /// Castagnoli CRC-32C.
    Crc32c,
    /// SHA-256 over the canonicalized provider payload.
    Sha256,
}

/// Typed checksum book scope independent of algorithm/canonicalization identity.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChecksumBookScope {
    depth: MarketDepth,
    level_count: Option<NonZeroU16>,
}

impl ChecksumBookScope {
    /// Constructs an exact provider checksum book-depth scope.
    pub const fn new(depth: MarketDepth, level_count: Option<NonZeroU16>) -> Self {
        Self { depth, level_count }
    }

    /// Returns market depth participating in checksum canonicalization.
    pub const fn depth(self) -> MarketDepth {
        self.depth
    }

    /// Returns an exact level count when the provider truncates checksum depth.
    pub const fn level_count(self) -> Option<NonZeroU16> {
        self.level_count
    }
}

/// Provider checksum semantics, including canonicalization and validated scope.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChecksumValidationProfile {
    /// The selected channel authoritatively supplies no checksum.
    Unsupported { rule: IntegrityRule },
    /// Checksums are required and validated under this exact profile.
    Provided {
        /// Exact validation rule identity and revision.
        rule: IntegrityRule,
        /// Provider checksum algorithm.
        algorithm: ChecksumAlgorithm,
        /// Versioned canonicalization procedure identifier.
        canonicalization: SourceIdentifier,
        /// Versioned checksum scope identifier, including any depth/level rules.
        scope: SourceIdentifier,
        /// Typed book depth/level count, absent only for payload checksums.
        book_scope: Option<ChecksumBookScope>,
    },
}

impl ChecksumValidationProfile {
    const fn capability(&self) -> ChecksumCapability {
        match self {
            Self::Unsupported { .. } => ChecksumCapability::Unsupported,
            Self::Provided { .. } => ChecksumCapability::Provided,
        }
    }
}

/// Exact provider-number conversion boundary used before canonical financial values are created.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderNumericPolicy {
    /// Preserve the provider decimal lexeme and reject values not exactly representable at the
    /// instrument's configured tick/lot scale.
    ExactDecimalLexeme,
}

/// Independent provider-code interpretation rules for typed live event semantics.
///
/// A rule revision for one event family never authorizes interpretation of another family.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticInterpretationProfile {
    aggressor_rule: IntegrityRule,
    auction_rule: IntegrityRule,
    trading_status_rule: IntegrityRule,
    corporate_action_rule: IntegrityRule,
}

impl SemanticInterpretationProfile {
    /// Constructs independently versioned semantic interpretation rules.
    pub const fn new(
        aggressor_rule: IntegrityRule,
        auction_rule: IntegrityRule,
        trading_status_rule: IntegrityRule,
        corporate_action_rule: IntegrityRule,
    ) -> Self {
        Self {
            aggressor_rule,
            auction_rule,
            trading_status_rule,
            corporate_action_rule,
        }
    }

    /// Returns the exact trade-aggressor interpretation rule.
    pub const fn aggressor_rule(&self) -> &IntegrityRule {
        &self.aggressor_rule
    }

    /// Returns the exact auction-code interpretation rule.
    pub const fn auction_rule(&self) -> &IntegrityRule {
        &self.auction_rule
    }

    /// Returns the exact halt and instrument-status interpretation rule.
    pub const fn trading_status_rule(&self) -> &IntegrityRule {
        &self.trading_status_rule
    }

    /// Returns the exact corporate-action interpretation rule.
    pub const fn corporate_action_rule(&self) -> &IntegrityRule {
        &self.corporate_action_rule
    }
}

/// Live decoder, sequence, checksum, timestamp, and numeric validation contract.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LiveProtocolProfile {
    decoder_rule: IntegrityRule,
    semantic_interpretation: SemanticInterpretationProfile,
    timestamp_rule: IntegrityRule,
    sequence: SequenceValidationProfile,
    checksum: ChecksumValidationProfile,
    source_timestamps: bool,
    numeric_policy: ProviderNumericPolicy,
}

impl LiveProtocolProfile {
    /// Constructs an exact live protocol-validation profile.
    pub const fn new(
        decoder_rule: IntegrityRule,
        semantic_interpretation: SemanticInterpretationProfile,
        timestamp_rule: IntegrityRule,
        sequence: SequenceValidationProfile,
        checksum: ChecksumValidationProfile,
        source_timestamps: bool,
        numeric_policy: ProviderNumericPolicy,
    ) -> Self {
        Self {
            decoder_rule,
            semantic_interpretation,
            timestamp_rule,
            sequence,
            checksum,
            source_timestamps,
            numeric_policy,
        }
    }

    /// Returns the exact decoder implementation/rule revision.
    pub const fn decoder_rule(&self) -> &IntegrityRule {
        &self.decoder_rule
    }

    /// Returns independently versioned typed provider-code interpretation rules.
    pub const fn semantic_interpretation(&self) -> &SemanticInterpretationProfile {
        &self.semantic_interpretation
    }

    /// Returns exact provider timestamp interpretation/absence rule.
    pub const fn timestamp_rule(&self) -> &IntegrityRule {
        &self.timestamp_rule
    }

    /// Returns authoritative sequence semantics.
    pub const fn sequence(&self) -> &SequenceValidationProfile {
        &self.sequence
    }

    /// Returns authoritative checksum semantics.
    pub const fn checksum(&self) -> &ChecksumValidationProfile {
        &self.checksum
    }

    /// Returns whether provider/exchange timestamps are required.
    pub const fn source_timestamps(&self) -> bool {
        self.source_timestamps
    }

    /// Returns the exact provider-number conversion policy.
    pub const fn numeric_policy(&self) -> ProviderNumericPolicy {
        self.numeric_policy
    }
}

/// Protocol contract selected by source capability.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceProtocolProfile {
    /// Source has no live protocol decoder.
    NotLive,
    /// Source has the exact live validation profile carried here.
    Live(Box<LiveProtocolProfile>),
}

/// Immutable protocol and extraction capabilities.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SourceCapabilities {
    live: bool,
    extraction: bool,
    sequence: SequenceCapability,
    checksum: ChecksumCapability,
    historical: HistoricalCapability,
    source_timestamps: bool,
}

impl SourceCapabilities {
    /// Constructs a capability declaration; registry validation checks cross-field consistency.
    pub const fn new(
        live: bool,
        extraction: bool,
        sequence: SequenceCapability,
        checksum: ChecksumCapability,
        historical: HistoricalCapability,
        source_timestamps: bool,
    ) -> Self {
        Self {
            live,
            extraction,
            sequence,
            checksum,
            historical,
            source_timestamps,
        }
    }

    /// Returns whether a live stream is declared.
    pub const fn live(self) -> bool {
        self.live
    }

    /// Returns whether bounded extraction is declared.
    pub const fn extraction(self) -> bool {
        self.extraction
    }

    /// Returns the declared provider sequence capability.
    pub const fn sequence(self) -> SequenceCapability {
        self.sequence
    }

    /// Returns the declared provider checksum capability.
    pub const fn checksum(self) -> ChecksumCapability {
        self.checksum
    }

    /// Returns historical/revision behavior.
    pub const fn historical(self) -> HistoricalCapability {
        self.historical
    }

    /// Returns whether the source supplies exchange/provider timestamps.
    pub const fn source_timestamps(self) -> bool {
        self.source_timestamps
    }
}
