import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"

const timestampSchema = z.iso.datetime({ offset: false, precision: 9 })
const canonicalDecimalSchema = z
  .string()
  .regex(/^-?(?:0|[1-9][0-9]*)(?:\.[0-9]*[1-9])?$/)
  .refine((value) => value !== "-0")
const positiveIntegerTextSchema = z
  .string()
  .regex(/^[1-9][0-9]*$/)
  .refine((value) => unsignedIntegerTextWithin(value, "18446744073709551615"))
const integerTextSchema = z
  .string()
  .regex(/^(?:0|-?[1-9][0-9]*)$/)
  .refine(signedIntegerTextWithinI64)
const unsignedIntegerTextSchema = z
  .string()
  .regex(/^(?:0|[1-9][0-9]*)$/)
  .refine((value) => unsignedIntegerTextWithin(value, "18446744073709551615"))
const nonemptyTextSchema = z.string().min(1).max(512)
const sourceIdSchema = z.string().min(1).max(128)
const venueIdSchema = z.string().min(1).max(64)
const providerInstrumentSchema = z.string().min(1).max(256)
const providerLexemeSchema = z.string().min(1).max(128)
const nonnegativeIntegerSchema = z.number().int().nonnegative()
const positiveIntegerSchema = z.number().int().positive()

const evidenceDigestSchema = z
  .object({
    algorithm: z.enum(["sha256", "blake3"]),
    bytes: z.string().regex(/^[0-9a-f]{64}$/),
  })
  .strict()

const sha256EvidenceDigestSchema = z
  .object({
    algorithm: z.literal("sha256"),
    bytes: z.string().regex(/^[0-9a-f]{64}$/),
  })
  .strict()

const definitionRevisionDigestSchema = sha256EvidenceDigestSchema.refine(
  (digest) => digest.bytes !== "0".repeat(64),
  { message: "definition revision digest must be nonzero" },
)

const marketTimingSchema = z.enum([
  "real_time",
  "delayed",
  "end_of_day",
  "historical",
  "stored",
])

const marketDepthSchema = z.enum(["top_of_book", "price_level", "order_level"])

const marketQualitySchema = z.enum([
  "direct_verified",
  "direct_unverified",
  "official_delayed",
  "aggregated",
  "indicative",
  "modeled",
  "estimated",
  "stale",
  "quarantined",
])

const marketCoverageSchema = z.enum([
  "consolidated",
  "multi_venue_partial",
  "single_venue",
  "benchmark",
  "reference",
  "user_owned",
])

const marketIntegritySchema = z.enum([
  "verified",
  "unverified",
  "not_applicable",
  "failed",
  "quarantined",
])

const assetClassSchema = z.enum([
  "equity",
  "fixed_income",
  "option",
  "future",
  "foreign_exchange",
  "crypto",
  "commodity",
  "fund",
  "index",
  "cash",
])

const payloadLocatorSchema = z
  .object({
    reference: nonemptyTextSchema,
    version: nonemptyTextSchema,
  })
  .strict()

const referenceEvidenceSchema = z
  .object({
    referenceRevision: nonemptyTextSchema,
    referencePayloadDigest: evidenceDigestSchema,
    quoteCurrencyPayloadDigest: evidenceDigestSchema,
    referencePayloadLocator: payloadLocatorSchema.nullable(),
    quoteCurrencyPayloadLocator: payloadLocatorSchema.nullable(),
    effectiveFrom: timestampSchema,
    effectiveUntil: timestampSchema.nullable(),
  })
  .strict()
  .superRefine((evidence, context) => {
    if (evidence.effectiveUntil !== null && evidence.effectiveUntil <= evidence.effectiveFrom) {
      context.addIssue({ code: "custom", message: "reference evidence interval mismatch" })
    }
  })

const unifiedMarketObservationSchema = z
  .object({
    availability: z.literal("unavailable"),
    reason: z.enum([
      "no_eligible_source",
      "durable_pit_evidence_not_established",
    ]),
  })
  .strict()

const marketDowngradeDimensionSchema = z.discriminatedUnion("dimension", [
  z
    .object({
      dimension: z.literal("timing"),
      required: marketTimingSchema,
      selected: marketTimingSchema,
    })
    .strict(),
  z
    .object({
      dimension: z.literal("depth"),
      minimum: marketDepthSchema,
      selected: marketDepthSchema.nullable(),
    })
    .strict(),
  z
    .object({
      dimension: z.literal("quality"),
      minimum: marketQualitySchema,
      selected: marketQualitySchema,
    })
    .strict(),
  z
    .object({
      dimension: z.literal("coverage"),
      required: marketCoverageSchema,
      selected: marketCoverageSchema,
    })
    .strict(),
  z
    .object({
      dimension: z.literal("freshness"),
      maximumAgeNanos: unsignedIntegerTextSchema,
      selectedAgeNanos: unsignedIntegerTextSchema,
    })
    .strict(),
])

const marketDowngradeDimensionsSchema = z
  .array(marketDowngradeDimensionSchema)
  .max(5)
  .superRefine((dimensions, context) => {
    if (new Set(dimensions.map((item) => item.dimension)).size !== dimensions.length) {
      context.addIssue({ code: "custom", message: "duplicate downgrade dimension" })
    }
  })

const marketSelectionReceiptSchema = z
  .object({
    policyRevision: z.number().int().min(1).max(4_294_967_295),
    policyCandidateLimit: z.number().int().min(1).max(4_096),
    policyDigest: sha256EvidenceDigestSchema,
    selectionDigest: sha256EvidenceDigestSchema,
    definitionRevisionDigest: definitionRevisionDigestSchema.nullable(),
    selectedAt: timestampSchema,
    eligibleCount: z.number().int().min(0).max(4_096),
    rejectedCount: z.number().int().min(0).max(4_096),
    availableAlternativeCount: z.number().int().min(0).max(4_096),
    returnedAlternativeCount: z.number().int().min(0).max(8),
    alternativesComplete: z.boolean(),
    selectionClass: z
      .enum(["exact_requirements", "admitted_downgrade"])
      .nullable(),
    downgradeDimensions: marketDowngradeDimensionsSchema,
  })
  .strict()

const displayAvailabilitySchema = z.discriminatedUnion("state", [
  z
    .object({
      state: z.literal("fresh"),
      staleAfter: timestampSchema,
      expiresAfter: timestampSchema,
    })
    .strict(),
  z
    .object({
      state: z.literal("stale"),
      staleAfter: timestampSchema,
      expiresAfter: timestampSchema,
    })
    .strict(),
  z
    .object({
      state: z.literal("expired"),
      expiredAfter: timestampSchema,
    })
    .strict(),
  z
    .object({
      state: z.literal("quarantined"),
      failure: nonemptyTextSchema,
    })
    .strict(),
]).superRefine((availability, context) => {
  if (
    (availability.state === "fresh" || availability.state === "stale") &&
    availability.expiresAfter <= availability.staleAfter
  ) {
    context.addIssue({ code: "custom", message: "display availability interval mismatch" })
  }
})

const displayCoverageEvidenceSchema = z
  .object({
    providerProduct: nonemptyTextSchema,
    providerChannel: nonemptyTextSchema,
    eventClass: z.enum([
      "trade",
      "quote",
      "book_snapshot",
      "book_delta",
      "auction",
      "trading_halt",
      "instrument_status",
      "corporate_action",
    ]),
    declaredDepth: marketDepthSchema.nullable(),
    delay: z.union([
      z.object({ kind: z.literal("real_time") }).strict(),
      z
        .object({ kind: z.literal("delayed"), value: positiveIntegerTextSchema })
        .strict(),
    ]),
    consolidation: z.enum(["single_venue", "partial", "consolidated"]),
    delivery: z.enum(["direct_venue", "authorized_broker", "indirect", "unknown"]),
    status: z.enum(["sufficient", "insufficient", "unknown"]),
    staticEvidenceDigest: evidenceDigestSchema,
    runtimeEvidenceDigest: evidenceDigestSchema.nullable(),
    effectiveFrom: timestampSchema,
    effectiveUntil: timestampSchema.nullable(),
  })
  .strict()
  .superRefine((coverage, context) => {
    if (coverage.effectiveUntil !== null && coverage.effectiveUntil <= coverage.effectiveFrom) {
      context.addIssue({ code: "custom", message: "display coverage interval mismatch" })
    }
  })

const displayObservationEvidenceSchema = z
  .object({
    sourceIdentifier: nonemptyTextSchema,
    sourceTimestamp: timestampSchema.nullable(),
    effectiveAt: timestampSchema,
    effectiveTimeBasis: z.enum(["provider", "received"]),
    receivedAt: timestampSchema,
    availableAt: timestampSchema,
    metadataRevision: nonemptyTextSchema,
    recordedQuality: marketQualitySchema,
    currentDisplayQuality: marketQualitySchema,
    displayDepth: marketDepthSchema.nullable(),
    connectionGeneration: positiveIntegerTextSchema,
    sessionId: nonemptyTextSchema,
    frameId: positiveIntegerTextSchema,
    payloadDigest: evidenceDigestSchema,
    captureIntegrity: z.enum(["disabled", "healthy", "incomplete"]),
    decoderRule: nonemptyTextSchema,
    decoderRuleVersion: positiveIntegerSchema,
    timestampRule: nonemptyTextSchema,
    timestampRuleVersion: positiveIntegerSchema,
    availability: displayAvailabilitySchema,
    coverage: displayCoverageEvidenceSchema,
  })
  .strict()
  .superRefine((evidence, context) => {
    if (evidence.availableAt < evidence.receivedAt) {
      context.addIssue({ code: "custom", message: "display observation ordering mismatch" })
    }
  })

const krakenExecutionTermsSchema = z
  .object({
    instrumentId: z.string().uuid(),
    definitionRevision: positiveIntegerTextSchema,
    priceTick: canonicalDecimalSchema,
    lotSize: canonicalDecimalSchema,
    quoteCurrency: z.string().regex(/^[A-Z]{3}$/),
    settlementDenomination: z.discriminatedUnion("kind", [
      z
        .object({
          kind: z.literal("currency"),
          value: z.string().regex(/^[A-Z]{3}$/),
        })
        .strict(),
      z
        .object({ kind: z.literal("asset"), value: z.string().uuid() })
        .strict(),
    ]),
    contractMultiplier: canonicalDecimalSchema,
  })
  .strict()

const marketIntegrityRuleSchema = z
  .object({
    provider_rule: nonemptyTextSchema,
    version: z.number().int().min(1).max(4_294_967_295),
  })
  .strict()

const marketChecksumTargetSchema = z.union([
  z
    .object({
      kind: z.literal("book"),
      scope: z
        .object({
          depth: marketDepthSchema,
          level_count: z.number().int().min(1).max(4_294_967_295),
          provider_scope: nonemptyTextSchema,
        })
        .strict(),
    })
    .strict(),
  z
    .object({
      kind: z.literal("payload"),
      scope: z.object({ provider_scope: nonemptyTextSchema }).strict(),
    })
    .strict(),
])

const marketSequenceEvidenceSchema = z.union([
  z
    .object({
      capability: z.literal("provided"),
      rule: marketIntegrityRuleSchema,
      validation_rule: z.enum(["consecutive", "monotonic"]),
      connection_generation: positiveIntegerTextSchema,
      snapshot_sequence: unsignedIntegerTextSchema.nullable(),
      previous_sequence: unsignedIntegerTextSchema.nullable(),
      observed_sequence: unsignedIntegerTextSchema,
      integrity: z.enum(["valid", "invalid"]),
    })
    .strict(),
  z
    .object({
      capability: z.literal("provided"),
      rule: marketIntegrityRuleSchema,
      validation_rule: z.enum(["consecutive", "monotonic"]),
      connection_generation: positiveIntegerTextSchema,
      snapshot_sequence: unsignedIntegerTextSchema.nullable(),
      previous_sequence: z.null(),
      observed_sequence: z.null(),
      integrity: z.literal("uninitialized"),
    })
    .strict(),
  z
    .object({
      capability: z.literal("unsupported"),
      rule: z.null(),
      validation_rule: z.null(),
      connection_generation: positiveIntegerTextSchema,
      snapshot_sequence: z.null(),
      previous_sequence: z.null(),
      observed_sequence: z.null(),
      integrity: z.literal("not_supported"),
    })
    .strict(),
])

const marketChecksumEvidenceSchema = z.union([
  z
    .object({
      capability: z.literal("provided"),
      rule: marketIntegrityRuleSchema,
      connection_generation: positiveIntegerTextSchema,
      target: marketChecksumTargetSchema,
      expected: unsignedIntegerTextSchema,
      computed: unsignedIntegerTextSchema,
      integrity: z.enum(["valid", "failed"]),
    })
    .strict(),
  z
    .object({
      capability: z.literal("provided"),
      rule: marketIntegrityRuleSchema,
      connection_generation: positiveIntegerTextSchema,
      target: marketChecksumTargetSchema,
      expected: z.null(),
      computed: z.null(),
      integrity: z.literal("unchecked"),
    })
    .strict(),
  z
    .object({
      capability: z.literal("unsupported"),
      rule: z.null(),
      connection_generation: positiveIntegerTextSchema,
      target: z.null(),
      expected: z.null(),
      computed: z.null(),
      integrity: z.literal("not_supported"),
    })
    .strict(),
])

const krakenProjectionEvidenceSchema = z
  .object({
    surfaceId: nonemptyTextSchema,
    providerId: nonemptyTextSchema,
    providerSymbol: nonemptyTextSchema,
    sourceId: sourceIdSchema,
    venueId: venueIdSchema,
    instrumentId: z.string().uuid(),
    providerInstrument: providerInstrumentSchema,
    connectionGeneration: positiveIntegerTextSchema,
    batchIdentifier: nonemptyTextSchema,
    revision: unsignedIntegerTextSchema,
    phase: z.enum(["awaiting_snapshot", "healthy", "quarantined"]),
    quarantineReason: z
      .enum([
        "route_mismatch",
        "sequence",
        "checksum",
        "snapshot",
        "mutation",
        "book",
        "resource",
      ])
      .nullable(),
    quality: marketQualitySchema,
    sourceDepth: z.literal("order_level"),
    projectionDepth: z.literal("price_level"),
    executionTerms: krakenExecutionTermsSchema,
    freshness: z.enum(["uninitialized", "fresh", "stale"]),
    lastMarketAt: timestampSchema.nullable(),
    sourceTimestamp: timestampSchema,
    receivedAt: timestampSchema,
    availableAt: timestampSchema,
    providerSequence: unsignedIntegerTextSchema.nullable(),
    diagnosticOrdinal: unsignedIntegerTextSchema.nullable(),
    sequenceEvidence: marketSequenceEvidenceSchema,
    checksumEvidence: marketChecksumEvidenceSchema,
    bidLevelCount: nonnegativeIntegerSchema.max(2_000_000),
    askLevelCount: nonnegativeIntegerSchema.max(2_000_000),
  })
  .strict()

const runtimeEvidenceSchema = z
  .object({
    sessionId: nonemptyTextSchema,
    assessmentId: nonemptyTextSchema,
    bindingDigest: z.string().regex(/^[0-9a-f]{64}$/),
    connection: z.union([
      z.literal("connecting"),
      z
        .object({ live: z.object({ last_activity_at: integerTextSchema }).strict() })
        .strict(),
      z
        .object({ stale: z.object({ last_activity_at: integerTextSchema }).strict() })
        .strict(),
      z
        .object({ disconnected: z.object({ disconnected_at: integerTextSchema }).strict() })
        .strict(),
    ]),
    transportFreshness: z.union([
      z.literal("uninitialized"),
      z
        .object({ fresh: z.object({ last_transport_at: integerTextSchema }).strict() })
        .strict(),
      z
        .object({ stale: z.object({ last_transport_at: integerTextSchema }).strict() })
        .strict(),
    ]),
    marketFreshness: z.union([
      z.literal("uninitialized"),
      z
        .object({ fresh: z.object({ last_market_at: integerTextSchema }).strict() })
        .strict(),
      z
        .object({ stale: z.object({ last_market_at: integerTextSchema }).strict() })
        .strict(),
    ]),
    sourceFreshness: z.union([
      z.literal("uninitialized"),
      z
        .object({ fresh: z.object({ last_source_at: integerTextSchema }).strict() })
        .strict(),
      z
        .object({ stale: z.object({ last_source_at: integerTextSchema }).strict() })
        .strict(),
    ]),
    streamIntegrity: z.enum([
      "initializing",
      "synchronizing",
      "validating",
      "healthy",
      "stale",
      "gap_detected",
      "checksum_failed",
      "divergent",
      "quarantined",
    ]),
    captureIntegrity: z.enum(["disabled", "healthy", "incomplete"]),
    coverageStatus: z.enum(["sufficient", "insufficient", "unknown"]),
    healthObservedAt: timestampSchema,
    qualificationEvaluatedAt: timestampSchema,
    qualificationValidUntil: timestampSchema,
  })
  .strict()
  .superRefine((evidence, context) => {
    if (evidence.qualificationValidUntil < evidence.qualificationEvaluatedAt) {
      context.addIssue({ code: "custom", message: "runtime qualification interval mismatch" })
    }
  })

const providerBudgetSchema = z
  .object({
    availability: z.enum([
      "not_required",
      "open",
      "interactive_only",
      "exhausted",
      "unknown",
    ]),
    observedAt: timestampSchema,
  })
  .strict()

const rightsSchema = z
  .object({
    decisionId: nonemptyTextSchema,
    state: z.literal("admitted"),
    decidedAt: timestampSchema,
    effectiveFrom: timestampSchema.nullable(),
    effectiveUntil: timestampSchema.nullable(),
    snapshotDisplayPermitted: z.literal(true),
  })
  .strict()
  .superRefine((rights, context) => {
    if (
      rights.effectiveFrom !== null &&
      rights.effectiveUntil !== null &&
      rights.effectiveUntil <= rights.effectiveFrom
    ) {
      context.addIssue({ code: "custom", message: "rights interval mismatch" })
    }
  })

const selectedSourceCommon = {
  surfaceId: nonemptyTextSchema,
  providerId: nonemptyTextSchema,
  sourceId: sourceIdSchema,
  venueId: venueIdSchema.nullable(),
  providerProduct: nonemptyTextSchema,
  providerChannel: nonemptyTextSchema,
  timing: marketTimingSchema,
  depth: marketDepthSchema.nullable(),
  depthLabel: z.enum([
    "Best quote",
    "Price-level book",
    "Order-level book",
    "Benchmark",
    "No market book",
  ]),
  quality: marketQualitySchema,
  coverage: marketCoverageSchema,
  health: z.enum(["healthy", "degraded", "unavailable", "quarantined"]),
  healthObservedAt: timestampSchema,
  stateRevision: unsignedIntegerTextSchema,
  snapshotPublishedAt: timestampSchema,
  providerBudget: providerBudgetSchema,
  rights: rightsSchema,
}

const liveSelectedSourceSchema = z
  .object({
    ...selectedSourceCommon,
    providerSymbol: z.undefined().optional(),
    shardId: z.string().uuid(),
    shardSnapshotRevision: positiveIntegerTextSchema,
    freshness: z
      .object({
        ageNanos: unsignedIntegerTextSchema,
        sourceTimestamp: timestampSchema.nullable(),
        receivedAt: timestampSchema,
        availableAt: timestampSchema,
        ingestedAt: timestampSchema,
        sourceValidUntil: timestampSchema,
        freshAtSelection: z.boolean(),
      })
      .strict(),
    integrity: z
      .object({
        state: marketIntegritySchema,
        assessedAt: timestampSchema,
        connectionGeneration: positiveIntegerTextSchema.nullable(),
        phase: z.enum([
          "disconnected",
          "awaiting_snapshot",
          "synchronizing",
          "healthy",
          "quarantined",
        ]),
        generationCurrent: z.boolean(),
        snapshotInitialized: z.boolean(),
        lastSequence: unsignedIntegerTextSchema.nullable(),
        runtimeEvidence: runtimeEvidenceSchema.nullable(),
      })
      .strict(),
  })
  .strict()

const displayStatusPayloadSchema = z.discriminatedUnion("kind", [
  z
    .object({
      kind: z.literal("trading_halt"),
      providerStatus: nonemptyTextSchema,
      transition: z.enum(["halted", "resumed"]),
      reason: nonemptyTextSchema,
    })
    .strict(),
  z
    .object({
      kind: z.literal("instrument"),
      providerStatus: nonemptyTextSchema,
      tradingStatus: z.enum(["active", "halted", "inactive", "delisted"]),
    })
    .strict(),
])

const displaySelectedSourceSchema = z
  .object({
    ...selectedSourceCommon,
    providerSymbol: nonemptyTextSchema,
    venueId: venueIdSchema,
    coverageStatus: z.enum(["sufficient", "insufficient", "unknown"]),
    freshness: z
      .object({
        ageNanos: unsignedIntegerTextSchema,
        sourceTimestamp: timestampSchema.nullable(),
        effectiveAt: timestampSchema,
        receivedAt: timestampSchema,
        availableAt: timestampSchema,
        ingestedAt: timestampSchema,
        sourceValidUntil: timestampSchema.nullable(),
        freshAtSelection: z.boolean(),
        selectedAt: timestampSchema,
        availability: displayAvailabilitySchema,
      })
      .strict(),
    integrity: z
      .object({
        state: marketIntegritySchema,
        assessedAt: timestampSchema,
        connectionGeneration: positiveIntegerTextSchema,
        phase: z.enum(["healthy", "stale", "expired", "quarantined"]),
        generationCurrent: z.null(),
        snapshotInitialized: z.boolean(),
        lastSequence: z.null(),
        terminalFailure: nonemptyTextSchema.nullable(),
        runtimeEvidence: displayObservationEvidenceSchema,
      })
      .strict(),
    status: z
      .object({
        payload: displayStatusPayloadSchema,
        evidence: displayObservationEvidenceSchema,
      })
      .strict()
      .nullable(),
  })
  .strict()

const sourceMetadataEvidenceSchema = z
  .object({
    schemaVersion: z.literal(1),
    sourceId: sourceIdSchema,
    providerId: nonemptyTextSchema,
    sourceClass: z.literal("exchange"),
    metadataRevision: nonemptyTextSchema,
    metadataPayloadDigest: evidenceDigestSchema,
    metadataPayloadLocator: payloadLocatorSchema.nullable(),
    qualityCeiling: z.literal("direct_unverified"),
    coverage: z
      .object({
        payloadDigest: evidenceDigestSchema,
        payloadLocator: payloadLocatorSchema.nullable(),
        effectiveFrom: timestampSchema,
        effectiveUntil: timestampSchema.nullable(),
        assetClasses: z.tuple([z.literal("crypto")]),
        topology: z
          .object({
            kind: z.literal("single_venue"),
            venues: z.tuple([venueIdSchema]),
          })
          .strict(),
        instruments: z
          .object({
            kind: z.literal("enumerated"),
            instruments: z.array(z.string().uuid()).min(1).max(4_096),
          })
          .strict(),
        live: z
          .object({
            provider_product: nonemptyTextSchema,
            provider_channel: nonemptyTextSchema,
            rules: z.tuple([
              z
                .object({
                  event_class: z.enum(["book_snapshot", "book_delta"]),
                  depth: z.literal("order_level"),
                  snapshot_applicability: z
                    .object({ kind: z.literal("required") })
                    .strict(),
                })
                .strict(),
              z
                .object({
                  event_class: z.enum(["book_snapshot", "book_delta"]),
                  depth: z.literal("order_level"),
                  snapshot_applicability: z
                    .object({ kind: z.literal("required") })
                    .strict(),
                })
                .strict(),
            ]),
          })
          .strict(),
        delay: z.object({ kind: z.literal("real_time") }).strict(),
        delivery: z.literal("direct_venue"),
      })
      .strict(),
  })
  .strict()
  .superRefine((metadata, context) => {
    const { topology, instruments, assetClasses, live } = metadata.coverage
    if (
      new Set(assetClasses).size !== assetClasses.length ||
      new Set(topology.venues).size !== topology.venues.length ||
      new Set(instruments.instruments).size !== instruments.instruments.length ||
      topology.venues.length !== 1 ||
      instruments.instruments.length === 0 ||
      new Set(live.rules.map((rule) => rule.event_class)).size !== 2
    ) {
      context.addIssue({ code: "custom", message: "source metadata coverage mismatch" })
    }
    if (
      metadata.coverage.effectiveUntil !== null &&
      metadata.coverage.effectiveUntil <= metadata.coverage.effectiveFrom
    ) {
      context.addIssue({ code: "custom", message: "source metadata interval mismatch" })
    }
  })

const krakenSelectedSourceSchema = z
  .object({
    ...selectedSourceCommon,
    providerSymbol: nonemptyTextSchema,
    venueId: venueIdSchema,
    sourceDepth: z.literal("order_level"),
    projectionDepth: z.literal("price_level"),
    qualityCeiling: z.literal("direct_unverified"),
    executionEligible: z.literal(false),
    freshness: z
      .object({
        ageNanos: unsignedIntegerTextSchema,
        state: z.enum(["uninitialized", "fresh", "stale"]),
        lastMarketAt: timestampSchema.nullable(),
        sourceTimestamp: timestampSchema,
        effectiveAt: timestampSchema,
        receivedAt: timestampSchema,
        availableAt: timestampSchema,
        ingestedAt: timestampSchema,
        sourceValidUntil: z.null(),
        freshAtSelection: z.boolean(),
        selectedAt: timestampSchema,
      })
      .strict(),
    integrity: z
      .object({
        state: marketIntegritySchema,
        assessedAt: timestampSchema,
        connectionGeneration: positiveIntegerTextSchema,
        phase: z.enum(["awaiting_snapshot", "healthy", "quarantined"]),
        generationCurrent: z.literal(true),
        snapshotInitialized: z.boolean(),
        lastSequence: unsignedIntegerTextSchema.nullable(),
        runtimeEvidence: krakenProjectionEvidenceSchema,
      })
      .strict(),
    sourceMetadataEvidence: sourceMetadataEvidenceSchema,
  })
  .strict()

const selectedSourceSchema = z.union([
  liveSelectedSourceSchema,
  displaySelectedSourceSchema,
  krakenSelectedSourceSchema,
])

const quoteSchema = z
  .object({
    bidPrice: canonicalDecimalSchema.nullable(),
    bidPriceProviderLexeme: providerLexemeSchema.nullable(),
    bidSize: canonicalDecimalSchema.nullable(),
    bidSizeProviderLexeme: providerLexemeSchema.nullable(),
    askPrice: canonicalDecimalSchema.nullable(),
    askPriceProviderLexeme: providerLexemeSchema.nullable(),
    askSize: canonicalDecimalSchema.nullable(),
    askSizeProviderLexeme: providerLexemeSchema.nullable(),
    midPrice: canonicalDecimalSchema.nullable(),
    midPriceBasis: z
      .literal("calculated_from_selected_bid_and_ask")
      .nullable(),
    lastPrice: canonicalDecimalSchema.nullable(),
    lastPriceProviderLexeme: providerLexemeSchema.nullable(),
    lastSize: canonicalDecimalSchema.nullable(),
    lastSizeProviderLexeme: providerLexemeSchema.nullable(),
    lastSourceTimestamp: timestampSchema.nullable(),
    lastReceivedAt: timestampSchema.nullable(),
    lastAvailableAt: timestampSchema.nullable(),
    lastQuality: marketQualitySchema.nullable(),
    lastFreshAtSelection: z.boolean().nullable(),
    quoteEvidence: z
      .union([displayObservationEvidenceSchema, krakenProjectionEvidenceSchema])
      .nullable(),
    tradeEvidence: displayObservationEvidenceSchema.nullable(),
  })
  .strict()
  .superRefine((quote, context) => {
    const bidPresent = quote.bidPrice !== null
    const askPresent = quote.askPrice !== null
    const lastPresent = quote.lastPrice !== null
    if (bidPresent !== (quote.bidSize !== null)) {
      context.addIssue({ code: "custom", message: "bid price and size must co-occur" })
    }
    if (!bidPresent &&
      (quote.bidPriceProviderLexeme !== null || quote.bidSizeProviderLexeme !== null)) {
      context.addIssue({ code: "custom", message: "bid provider lexemes require a bid" })
    }
    if (askPresent !== (quote.askSize !== null)) {
      context.addIssue({ code: "custom", message: "ask price and size must co-occur" })
    }
    if (!askPresent &&
      (quote.askPriceProviderLexeme !== null || quote.askSizeProviderLexeme !== null)) {
      context.addIssue({ code: "custom", message: "ask provider lexemes require an ask" })
    }
    if ((bidPresent && askPresent) !== (quote.midPrice !== null)) {
      context.addIssue({ code: "custom", message: "midpoint presence must match both sides" })
    }
    if ((quote.midPrice !== null) !== (quote.midPriceBasis !== null)) {
      context.addIssue({ code: "custom", message: "midpoint basis must match midpoint" })
    }
    if (
      [
        quote.lastSize,
        quote.lastReceivedAt,
        quote.lastAvailableAt,
        quote.lastQuality,
        quote.lastFreshAtSelection,
      ].some((value) => value !== null) !== lastPresent
    ) {
      context.addIssue({ code: "custom", message: "last-trade fields must co-occur" })
    }
    if (!lastPresent && quote.lastSourceTimestamp !== null) {
      context.addIssue({ code: "custom", message: "source time requires a last trade" })
    }
    if (!lastPresent &&
      (quote.lastPriceProviderLexeme !== null || quote.lastSizeProviderLexeme !== null)) {
      context.addIssue({ code: "custom", message: "last-trade provider lexemes require a trade" })
    }
  })

const marketAlternativeSchema = z
  .object({
    surfaceId: nonemptyTextSchema,
    providerId: nonemptyTextSchema,
    sourceId: sourceIdSchema,
    venueId: venueIdSchema.nullable(),
    providerProduct: nonemptyTextSchema,
    providerChannel: nonemptyTextSchema,
    timing: marketTimingSchema,
    depth: marketDepthSchema.nullable(),
    quality: marketQualitySchema,
    coverage: marketCoverageSchema,
    freshnessAgeNanos: unsignedIntegerTextSchema,
    downgradeDimensions: marketDowngradeDimensionsSchema,
  })
  .strict()

const orderLevelOrderSchema = z
  .object({
    orderId: nonemptyTextSchema,
    side: z.enum(["bid", "ask"]),
    price: canonicalDecimalSchema,
    priceTicks: integerTextSchema,
    quantity: canonicalDecimalSchema,
    quantityLots: integerTextSchema,
    providerOrderTimestamp: timestampSchema.nullable(),
    providerPriority: z
      .object({
        value: unsignedIntegerTextSchema,
        rule: nonemptyTextSchema,
      })
      .strict()
      .nullable(),
    firstSeenIn: z.enum(["snapshot", "update"]),
    lastUpdatedIn: z.enum(["snapshot", "update"]),
    lastSourceTimestamp: timestampSchema,
    lastReceivedAt: timestampSchema,
    arrivalOrdinal: unsignedIntegerTextSchema,
  })
  .strict()

const orderBookSchema = z
  .object({
    depth: z.literal("order_level"),
    revision: unsignedIntegerTextSchema,
    phase: z.enum(["awaiting_snapshot", "healthy", "quarantined"]),
    quarantineReason: z
      .enum([
        "route_mismatch",
        "sequence",
        "checksum",
        "snapshot",
        "mutation",
        "book",
        "resource",
      ])
      .nullable(),
    quality: marketQualitySchema,
    freshness: z.enum(["uninitialized", "fresh", "stale"]),
    lastMarketAt: timestampSchema.nullable(),
    availableAt: timestampSchema,
    usableForSelection: z.boolean(),
    totalOrderCount: nonnegativeIntegerSchema.max(2_000_000),
    returnedOrderCount: nonnegativeIntegerSchema,
    sampleTruncated: z.boolean(),
    samplePolicy: z.literal("stable_provider_order_id_prefix"),
    orders: z.array(orderLevelOrderSchema).max(64),
  })
  .strict()
  .superRefine((book, context) => {
    if (book.returnedOrderCount !== book.orders.length) {
      context.addIssue({ code: "custom", message: "returned order count mismatch" })
    }
    if (book.totalOrderCount < book.returnedOrderCount) {
      context.addIssue({ code: "custom", message: "total order count is too small" })
    }
    if (book.sampleTruncated !== (book.totalOrderCount > book.returnedOrderCount)) {
      context.addIssue({ code: "custom", message: "order sample truncation mismatch" })
    }
    if ((book.phase === "quarantined") !== (book.quarantineReason !== null)) {
      context.addIssue({ code: "custom", message: "order quarantine reason mismatch" })
    }
    if ((book.freshness === "uninitialized") !== (book.lastMarketAt === null)) {
      context.addIssue({ code: "custom", message: "order freshness time mismatch" })
    }
    if (new Set(book.orders.map((order) => order.orderId)).size !== book.orders.length) {
      context.addIssue({ code: "custom", message: "duplicate order identity" })
    }
  })

export const unifiedMarketRowSchema = z
  .object({
    instrumentId: z.string().uuid(),
    symbol: nonemptyTextSchema,
    symbolKind: z.enum([
      "venue_symbol",
      "provider_subscription_symbol",
      "instrument_id",
    ]),
    symbolVenueId: venueIdSchema.nullable(),
    assetClass: assetClassSchema,
    quoteCurrency: z.string().regex(/^[A-Z]{3}$/),
    definitionKind: z.enum([
      "execution_and_market_data",
      "execution",
      "market_data",
    ]),
    definitionRevision: positiveIntegerTextSchema.nullable(),
    referenceRevision: nonemptyTextSchema.nullable(),
    displayName: nonemptyTextSchema.nullable(),
    tickSize: canonicalDecimalSchema.nullable(),
    lotSize: canonicalDecimalSchema.nullable(),
    executionTermsAvailable: z.boolean(),
    executionEligible: z.literal(false),
    definitionRevisionDigest: definitionRevisionDigestSchema.nullable(),
    referenceEvidence: referenceEvidenceSchema.nullable(),
    availability: z.enum(["Live", "Delayed", "End of day", "Stored data", "Stale", "Unavailable"]),
    confidence: z.enum([
      "Verified",
      "Direct, unverified",
      "Official delayed",
      "Aggregated",
      "Indicative",
      "Modeled",
      "Estimated",
      "Stale",
      "Unavailable",
      "No eligible source",
    ]),
    quote: quoteSchema,
    orderBook: orderBookSchema.nullable(),
    analyticalReadiness: z.literal("runtime_display_only"),
    marketObservation: unifiedMarketObservationSchema,
    selectedSource: selectedSourceSchema.nullable(),
    alternatives: z.array(marketAlternativeSchema).max(8),
    selectionReceipt: marketSelectionReceiptSchema,
  })
  .strict()
  .superRefine((row, context) => {
    crossBindDefinition(row, context)
    crossBindSelection(row, context)
  })

const failedSourceSchema = z
  .object({
    surfaceId: nonemptyTextSchema,
    reason: z.enum(["resource_exhausted", "unavailable"]),
  })
  .strict()

const marketSourceCoverageSchema = z
  .object({
    mode: z.enum(["current_live_runtime", "unified_current_market_runtime"]),
    consistency: z.enum(["per_shard_current_non_atomic", "partial_provider_set"]),
    historicalDataset: z.null(),
    requestedSourceCount: nonnegativeIntegerSchema,
    listedRequestedSources: z.array(nonemptyTextSchema).max(8),
    listedRequestedSourcesComplete: z.boolean(),
    observedSourceCount: nonnegativeIntegerSchema,
    listedSources: z.array(sourceIdSchema).max(8),
    listedSourcesComplete: z.boolean(),
    observedVenueCount: nonnegativeIntegerSchema,
    listedVenues: z.array(venueIdSchema).max(8),
    listedVenuesComplete: z.boolean(),
    failedSourceCount: nonnegativeIntegerSchema,
    failedSources: z.array(failedSourceSchema).max(8),
    listedFailedSourcesComplete: z.boolean(),
    streamIdentityScope: z.literal("complete"),
    bookDepthScope: z.literal("per_record_explicit"),
    displayObservationCount: nonnegativeIntegerSchema,
    krakenOrderLevelProjectionCount: nonnegativeIntegerSchema,
    availability: z.enum(["current", "no_current_observation"]),
  })
  .strict()
  .superRefine((coverage, context) => {
    validateListedIdentities(
      coverage.requestedSourceCount,
      coverage.listedRequestedSources,
      coverage.listedRequestedSourcesComplete,
      "requested source",
      context,
    )
    validateListedIdentities(
      coverage.observedSourceCount,
      coverage.listedSources,
      coverage.listedSourcesComplete,
      "observed source",
      context,
    )
    validateListedIdentities(
      coverage.observedVenueCount,
      coverage.listedVenues,
      coverage.listedVenuesComplete,
      "observed venue",
      context,
    )
    if (coverage.failedSources.length !== Math.min(coverage.failedSourceCount, 8)) {
      context.addIssue({ code: "custom", message: "listed failed-source count mismatch" })
    }
    if (coverage.listedFailedSourcesComplete !== (coverage.failedSourceCount <= 8)) {
      context.addIssue({ code: "custom", message: "failed-source completeness mismatch" })
    }
    if (
      new Set(coverage.failedSources.map((source) => source.surfaceId)).size !==
      coverage.failedSources.length
    ) {
      context.addIssue({ code: "custom", message: "duplicate failed source" })
    }
    if (
      coverage.consistency !==
      (coverage.failedSourceCount === 0
        ? "per_shard_current_non_atomic"
        : "partial_provider_set")
    ) {
      context.addIssue({ code: "custom", message: "provider-set consistency mismatch" })
    }
  })

const qualityCountSchema = z
  .object({
    quality: marketQualitySchema,
    count: positiveIntegerSchema,
  })
  .strict()

const marketDataQualitySchema = z
  .object({
    referenceAt: timestampSchema,
    recordedClassifications: z.array(qualityCountSchema).max(9),
    currentDisplayClassifications: z.array(qualityCountSchema).max(9),
    freshObservations: nonnegativeIntegerSchema,
    staleObservations: nonnegativeIntegerSchema,
    authority: z.literal("not_exposed"),
    summaryScope: z.literal("live_price_level_streams"),
    summarizedObservationCount: nonnegativeIntegerSchema,
    displayObservationCount: nonnegativeIntegerSchema,
    krakenOrderLevelProjectionCount: nonnegativeIntegerSchema,
  })
  .strict()
  .superRefine((quality, context) => {
    if (
      quality.freshObservations + quality.staleObservations !==
      quality.summarizedObservationCount
    ) {
      context.addIssue({ code: "custom", message: "freshness summary count mismatch" })
    }
    for (const classifications of [
      quality.recordedClassifications,
      quality.currentDisplayClassifications,
    ]) {
      if (
        classifications.reduce((sum, item) => sum + item.count, 0) !==
          quality.summarizedObservationCount ||
        new Set(classifications.map((item) => item.quality)).size !==
          classifications.length
      ) {
        context.addIssue({ code: "custom", message: "quality summary mismatch" })
      }
    }
  })

export const unifiedMarketResultSchema = z
  .object({
    data: z.array(unifiedMarketRowSchema).nullable(),
    metadata: z
      .object({
        completeness: z.enum(["complete", "truncated"]),
        returnedItems: nonnegativeIntegerSchema,
        availableItems: nonnegativeIntegerSchema,
        sourceCoverage: marketSourceCoverageSchema,
        dataQuality: marketDataQualitySchema,
      })
      .strict(),
  })
  .strict()
  .superRefine((result, context) => {
    const rows = result.data ?? []
    const { metadata } = result
    if (rows.length !== metadata.returnedItems) {
      context.addIssue({ code: "custom", message: "returned item count mismatch" })
    }
    if ((result.data === null) !== (metadata.returnedItems === 0)) {
      context.addIssue({ code: "custom", message: "null data/count mismatch" })
    }
    if (metadata.availableItems < metadata.returnedItems) {
      context.addIssue({ code: "custom", message: "available item count is too small" })
    }
    if (
      metadata.completeness !==
      (metadata.returnedItems === metadata.availableItems ? "complete" : "truncated")
    ) {
      context.addIssue({ code: "custom", message: "result completeness mismatch" })
    }
    if (
      metadata.sourceCoverage.availability !==
      (metadata.dataQuality.summarizedObservationCount +
        metadata.dataQuality.displayObservationCount +
        metadata.dataQuality.krakenOrderLevelProjectionCount ===
      0
        ? "no_current_observation"
        : "current")
    ) {
      context.addIssue({ code: "custom", message: "market availability count mismatch" })
    }
    if (
      metadata.sourceCoverage.mode !==
      (metadata.sourceCoverage.displayObservationCount +
        metadata.sourceCoverage.krakenOrderLevelProjectionCount === 0
        ? "current_live_runtime"
        : "unified_current_market_runtime")
    ) {
      context.addIssue({ code: "custom", message: "market coverage mode mismatch" })
    }
    if (
      new Set(rows.map((row) => row.instrumentId)).size !== rows.length ||
      rows.some(
        (row, index) => index > 0 &&
          row.instrumentId <= (rows[index - 1]?.instrumentId ?? ""),
      )
    ) {
      context.addIssue({ code: "custom", message: "instrument row ordering mismatch" })
    }
    for (const [index, row] of rows.entries()) {
      const selected = row.selectedSource
      if (row.selectionReceipt.selectedAt !== metadata.dataQuality.referenceAt) {
        context.addIssue({
          code: "custom",
          message: "market selection reference time mismatch",
          path: ["data", index, "selectionReceipt", "selectedAt"],
        })
      }
      if (
        selected &&
        metadata.sourceCoverage.listedSourcesComplete &&
        !metadata.sourceCoverage.listedSources.includes(selected.sourceId)
      ) {
        context.addIssue({
          code: "custom",
          message: "selected source is absent from complete source evidence",
          path: ["data", index, "selectedSource", "sourceId"],
        })
      }
      if (
        selected?.venueId &&
        metadata.sourceCoverage.listedVenuesComplete &&
        !metadata.sourceCoverage.listedVenues.includes(selected.venueId)
      ) {
        context.addIssue({
          code: "custom",
          message: "selected venue is absent from complete venue evidence",
          path: ["data", index, "selectedSource", "venueId"],
        })
      }
    }
  })

export type UnifiedMarketRow = z.infer<typeof unifiedMarketRowSchema>
export type UnifiedMarketResult = z.infer<typeof unifiedMarketResultSchema>

export function parseUnifiedMarketResult(
  result: ApplicationResult,
): UnifiedMarketResult {
  return unifiedMarketResultSchema.parse(result)
}

export function unifiedMarketRows(
  result: ApplicationResult | undefined,
): UnifiedMarketRow[] {
  if (!result) return []
  return parseUnifiedMarketResult(result).data ?? []
}

type MarketRowForBinding = z.infer<typeof unifiedMarketRowSchema>

function crossBindDefinition(
  row: MarketRowForBinding,
  context: z.core.$RefinementCtx<MarketRowForBinding>,
) {
  const hasExecution = row.definitionKind !== "market_data"
  const hasReference = row.definitionKind !== "execution"
  if (
    row.executionTermsAvailable !== hasExecution ||
    (row.definitionRevision !== null) !== hasExecution ||
    (row.tickSize !== null) !== hasExecution ||
    (row.lotSize !== null) !== hasExecution
  ) {
    context.addIssue({ code: "custom", message: "execution definition binding mismatch" })
  }
  if (
    (row.referenceRevision !== null) !== hasReference ||
    (row.referenceEvidence !== null) !== hasReference
  ) {
    context.addIssue({ code: "custom", message: "market-data definition binding mismatch" })
  }
  const rowDefinitionDigest = row.definitionRevisionDigest
  const receiptDefinitionDigest = row.selectionReceipt.definitionRevisionDigest
  if (
    (rowDefinitionDigest !== null) !== hasReference ||
    (rowDefinitionDigest === null) !== (receiptDefinitionDigest === null) ||
    rowDefinitionDigest?.algorithm !== receiptDefinitionDigest?.algorithm ||
    rowDefinitionDigest?.bytes !== receiptDefinitionDigest?.bytes
  ) {
    context.addIssue({
      code: "custom",
      message: "definition revision digest binding mismatch",
    })
  }
  if (
    row.referenceEvidence &&
    row.referenceEvidence.referenceRevision !== row.referenceRevision
  ) {
    context.addIssue({ code: "custom", message: "reference evidence identity mismatch" })
  }
  if (
    row.referenceEvidence &&
    (row.referenceEvidence.effectiveFrom > row.selectionReceipt.selectedAt ||
      (row.referenceEvidence.effectiveUntil !== null &&
        row.selectionReceipt.selectedAt >= row.referenceEvidence.effectiveUntil))
  ) {
    context.addIssue({
      code: "custom",
      message: "market-data definition is not effective at selection time",
    })
  }
  if (row.symbolKind === "instrument_id" && row.symbol !== row.instrumentId) {
    context.addIssue({ code: "custom", message: "instrument ID symbol mismatch" })
  }
  if ((row.symbolKind === "instrument_id") !== (row.symbolVenueId === null)) {
    context.addIssue({ code: "custom", message: "market symbol venue mismatch" })
  }
}

function crossBindSelection(
  row: MarketRowForBinding,
  context: z.core.$RefinementCtx<MarketRowForBinding>,
) {
  const receipt = row.selectionReceipt
  const selected = row.selectedSource
  const observation = row.marketObservation
  const expectedObservationReason = selected
    ? "durable_pit_evidence_not_established"
    : "no_eligible_source"
  if (observation.reason !== expectedObservationReason) {
    context.addIssue({
      code: "custom",
      message: "runtime display analytical authority mismatch",
    })
  }
  if (receipt.returnedAlternativeCount !== row.alternatives.length) {
    context.addIssue({ code: "custom", message: "returned alternative count mismatch" })
  }
  if (receipt.eligibleCount + receipt.rejectedCount > receipt.policyCandidateLimit) {
    context.addIssue({ code: "custom", message: "selection candidate limit mismatch" })
  }
  if (receipt.availableAlternativeCount !== Math.max(0, receipt.eligibleCount - 1)) {
    context.addIssue({ code: "custom", message: "available alternative count mismatch" })
  }
  if (
    receipt.alternativesComplete !==
    (receipt.availableAlternativeCount === receipt.returnedAlternativeCount)
  ) {
    context.addIssue({ code: "custom", message: "alternative completeness mismatch" })
  }
  if (
    new Set(row.alternatives.map(candidateIdentity)).size !== row.alternatives.length ||
    selected &&
      row.alternatives.some(
        (alternative) => candidateIdentity(alternative) === candidateIdentity(selected),
      )
  ) {
    context.addIssue({ code: "custom", message: "source alternative identity mismatch" })
  }
  if (!selected) {
    if (
      receipt.eligibleCount !== 0 ||
      receipt.availableAlternativeCount !== 0 ||
      receipt.selectionClass !== null ||
      receipt.downgradeDimensions.length !== 0 ||
      row.alternatives.length !== 0 ||
      row.orderBook !== null ||
      row.availability !== "Unavailable" ||
      row.confidence !== "No eligible source" ||
      row.marketObservation.availability !== "unavailable" ||
      row.marketObservation.reason !== "no_eligible_source" ||
      Object.values(row.quote).some((value) => value !== null)
    ) {
      context.addIssue({ code: "custom", message: "unselected row contains selected evidence" })
    }
    return
  }

  if (
    receipt.eligibleCount === 0 ||
    receipt.selectionClass === null ||
    selected.integrity.connectionGeneration === null
  ) {
    context.addIssue({ code: "custom", message: "selected receipt binding mismatch" })
  }
  if (
    (receipt.selectionClass === "exact_requirements") !==
    (receipt.downgradeDimensions.length === 0)
  ) {
    context.addIssue({ code: "custom", message: "selection downgrade class mismatch" })
  }
  if (
    row.symbolVenueId !== selected.venueId ||
    ("providerSymbol" in selected && row.symbol !== selected.providerSymbol) ||
    row.symbolKind !==
      ("shardId" in selected ? "venue_symbol" : "provider_subscription_symbol") ||
    row.availability !== availabilityLabel(selected.timing, selected.quality) ||
    row.confidence !== confidenceLabel(selected.quality) ||
    selected.depthLabel !== depthLabel(selected.depth, row.assetClass) ||
    selected.rights.state !== "admitted" ||
    !selected.rights.snapshotDisplayPermitted
  ) {
    context.addIssue({ code: "custom", message: "selected source presentation mismatch" })
  }
  if ("selectedAt" in selected.freshness && selected.freshness.selectedAt !== receipt.selectedAt) {
    context.addIssue({ code: "custom", message: "selected timestamp mismatch" })
  }
  if ("shardId" in selected) {
    if (
      row.quote.quoteEvidence !== null ||
      row.quote.tradeEvidence !== null ||
      providerLexemesPresent(row.quote)
    ) {
      context.addIssue({ code: "custom", message: "live stream quote evidence must remain implicit" })
    }
  } else if ("sourceDepth" in selected) {
    crossBindKraken(row, selected, context)
  } else {
    crossBindDisplay(row, selected, context)
  }
}

function crossBindDisplay(
  row: MarketRowForBinding,
  selected: z.infer<typeof displaySelectedSourceSchema>,
  context: z.core.$RefinementCtx<MarketRowForBinding>,
) {
  if (row.orderBook !== null) {
    context.addIssue({ code: "custom", message: "display source cannot publish an order-level book" })
  }
  const runtime = selected.integrity.runtimeEvidence
  if (
    runtime.connectionGeneration !== selected.integrity.connectionGeneration ||
    runtime.coverage.providerProduct !== selected.providerProduct ||
    runtime.coverage.providerChannel !== selected.providerChannel ||
    runtime.availableAt !== selected.snapshotPublishedAt ||
    runtime.receivedAt !== selected.freshness.receivedAt ||
    runtime.availableAt !== selected.freshness.availableAt ||
    runtime.effectiveAt !== selected.freshness.effectiveAt ||
    runtime.coverage.status !== selected.coverageStatus ||
    selected.freshness.ingestedAt !== selected.freshness.availableAt
  ) {
    context.addIssue({ code: "custom", message: "display source evidence mismatch" })
  }
  if (
    (row.quote.bidPrice !== null) !== (row.quote.bidPriceProviderLexeme !== null) ||
    (row.quote.bidSize !== null) !== (row.quote.bidSizeProviderLexeme !== null) ||
    (row.quote.askPrice !== null) !== (row.quote.askPriceProviderLexeme !== null) ||
    (row.quote.askSize !== null) !== (row.quote.askSizeProviderLexeme !== null) ||
    (row.quote.lastPrice !== null) !== (row.quote.lastPriceProviderLexeme !== null) ||
    (row.quote.lastSize !== null) !== (row.quote.lastSizeProviderLexeme !== null)
  ) {
    context.addIssue({ code: "custom", message: "display provider lexeme mismatch" })
  }
  for (const evidence of [row.quote.quoteEvidence, row.quote.tradeEvidence]) {
    if (
      evidence &&
      "sourceIdentifier" in evidence &&
      (evidence.coverage.providerProduct !== selected.providerProduct ||
        evidence.coverage.providerChannel !== selected.providerChannel ||
        evidence.connectionGeneration !== selected.integrity.connectionGeneration)
    ) {
      context.addIssue({ code: "custom", message: "display quote evidence mismatch" })
    }
  }
  const statusEvidence = selected.status?.evidence
  if (
    statusEvidence &&
    (statusEvidence.coverage.providerProduct !== selected.providerProduct ||
      statusEvidence.coverage.providerChannel !== selected.providerChannel ||
      statusEvidence.connectionGeneration !== selected.integrity.connectionGeneration)
  ) {
    context.addIssue({ code: "custom", message: "display status evidence mismatch" })
  }
  if (row.quote.quoteEvidence && !("sourceIdentifier" in row.quote.quoteEvidence)) {
    context.addIssue({ code: "custom", message: "display quote has incompatible evidence" })
  }
}

function crossBindKraken(
  row: MarketRowForBinding,
  selected: z.infer<typeof krakenSelectedSourceSchema>,
  context: z.core.$RefinementCtx<MarketRowForBinding>,
) {
  const runtime = selected.integrity.runtimeEvidence
  const book = row.orderBook
  if (
    !book ||
    row.quote.tradeEvidence !== null ||
    !row.quote.quoteEvidence ||
    !("providerInstrument" in row.quote.quoteEvidence) ||
    runtime.surfaceId !== selected.surfaceId ||
    runtime.providerId !== selected.providerId ||
    runtime.providerSymbol !== selected.providerSymbol ||
    runtime.sourceId !== selected.sourceId ||
    runtime.venueId !== selected.venueId ||
    runtime.instrumentId !== row.instrumentId ||
    runtime.connectionGeneration !== selected.integrity.connectionGeneration ||
    runtime.revision !== selected.stateRevision ||
    runtime.phase !== selected.integrity.phase ||
    runtime.quality !== selected.quality ||
    runtime.sourceDepth !== selected.sourceDepth ||
    runtime.projectionDepth !== selected.projectionDepth ||
    selected.depth !== selected.projectionDepth ||
    runtime.sourceTimestamp !== selected.freshness.sourceTimestamp ||
    runtime.receivedAt !== selected.freshness.receivedAt ||
    runtime.availableAt !== selected.freshness.availableAt ||
    runtime.availableAt !== selected.snapshotPublishedAt ||
    selected.freshness.ingestedAt !== selected.freshness.availableAt ||
    runtime.executionTerms.instrumentId !== row.instrumentId ||
    runtime.executionTerms.definitionRevision !== row.definitionRevision ||
    runtime.executionTerms.priceTick !== row.tickSize ||
    runtime.executionTerms.lotSize !== row.lotSize ||
    runtime.executionTerms.quoteCurrency !== row.quoteCurrency ||
    runtime.sequenceEvidence.connection_generation !== runtime.connectionGeneration ||
    runtime.checksumEvidence.connection_generation !== runtime.connectionGeneration ||
    selected.integrity.lastSequence !== runtime.providerSequence ||
    selected.sourceMetadataEvidence.sourceId !== selected.sourceId ||
    selected.sourceMetadataEvidence.providerId !== selected.providerId ||
    selected.sourceMetadataEvidence.qualityCeiling !== selected.qualityCeiling ||
    selected.sourceMetadataEvidence.coverage.live.provider_product !==
      selected.providerProduct ||
    selected.sourceMetadataEvidence.coverage.live.provider_channel !==
      selected.providerChannel ||
    row.assetClass !== "crypto" ||
    !selected.sourceMetadataEvidence.coverage.topology.venues.includes(
      selected.venueId,
    ) ||
    (selected.sourceMetadataEvidence.coverage.instruments.kind === "enumerated" &&
      !selected.sourceMetadataEvidence.coverage.instruments.instruments.includes(
        row.instrumentId,
      ))
  ) {
    context.addIssue({ code: "custom", message: "Kraken projection identity mismatch" })
  }
  if (providerLexemesPresent(row.quote)) {
    context.addIssue({ code: "custom", message: "Kraken projection cannot invent provider lexemes" })
  }
  if (
    runtime.sequenceEvidence.capability === "provided" &&
    runtime.sequenceEvidence.integrity !== "uninitialized" &&
    runtime.sequenceEvidence.observed_sequence !== runtime.providerSequence
  ) {
    context.addIssue({ code: "custom", message: "Kraken provider sequence mismatch" })
  }
  if (
    row.quote.quoteEvidence &&
    "providerInstrument" in row.quote.quoteEvidence &&
    (sourceIdentity(row.quote.quoteEvidence) !== sourceIdentity(runtime) ||
      row.quote.quoteEvidence.connectionGeneration !== runtime.connectionGeneration ||
      row.quote.quoteEvidence.revision !== runtime.revision ||
      row.quote.quoteEvidence.batchIdentifier !== runtime.batchIdentifier ||
      row.quote.quoteEvidence.availableAt !== runtime.availableAt ||
      row.quote.quoteEvidence.providerSequence !== runtime.providerSequence)
  ) {
    context.addIssue({ code: "custom", message: "Kraken quote evidence mismatch" })
  }
}

function sourceIdentity(source: {
  surfaceId: string
  providerId: string
  sourceId: string
  venueId: string | null
}) {
  return [source.surfaceId, source.providerId, source.sourceId, source.venueId ?? ""].join("\0")
}

function candidateIdentity(source: {
  surfaceId: string
  providerId: string
  sourceId: string
  venueId: string | null
  providerProduct: string
  providerChannel: string
}) {
  return [
    source.surfaceId,
    source.providerId,
    source.sourceId,
    source.venueId ?? "",
    source.providerProduct,
    source.providerChannel,
  ].join("\0")
}

function providerLexemesPresent(quote: z.infer<typeof quoteSchema>) {
  return [
    quote.bidPriceProviderLexeme,
    quote.bidSizeProviderLexeme,
    quote.askPriceProviderLexeme,
    quote.askSizeProviderLexeme,
    quote.lastPriceProviderLexeme,
    quote.lastSizeProviderLexeme,
  ].some((value) => value !== null)
}

function availabilityLabel(
  timing: z.infer<typeof marketTimingSchema>,
  quality: z.infer<typeof marketQualitySchema>,
) {
  if (quality === "stale") return "Stale"
  switch (timing) {
    case "real_time":
      return "Live"
    case "delayed":
      return "Delayed"
    case "end_of_day":
      return "End of day"
    case "historical":
    case "stored":
      return "Stored data"
  }
}

function confidenceLabel(quality: z.infer<typeof marketQualitySchema>) {
  const labels = {
    direct_verified: "Verified",
    direct_unverified: "Direct, unverified",
    official_delayed: "Official delayed",
    aggregated: "Aggregated",
    indicative: "Indicative",
    modeled: "Modeled",
    estimated: "Estimated",
    stale: "Stale",
    quarantined: "Unavailable",
  } as const
  return labels[quality]
}

function depthLabel(
  depth: z.infer<typeof marketDepthSchema> | null,
  assetClass: z.infer<typeof assetClassSchema>,
) {
  if (depth === "top_of_book") return "Best quote"
  if (depth === "price_level") return "Price-level book"
  if (depth === "order_level") return "Order-level book"
  return assetClass === "index" ? "Benchmark" : "No market book"
}

function validateListedIdentities(
  count: number,
  listed: string[],
  complete: boolean,
  label: string,
  context: z.core.$RefinementCtx,
) {
  if (
    listed.length !== Math.min(count, 8) ||
    complete !== (count <= 8) ||
    new Set(listed).size !== listed.length ||
    listed.some((item, index) => index > 0 && item <= (listed[index - 1] ?? ""))
  ) {
    context.addIssue({ code: "custom", message: `${label} evidence mismatch` })
  }
}

function unsignedIntegerTextWithin(value: string, maximum: string) {
  return value.length < maximum.length ||
    value.length === maximum.length && value <= maximum
}

function signedIntegerTextWithinI64(value: string) {
  const negative = value.startsWith("-")
  const magnitude = negative ? value.slice(1) : value
  return unsignedIntegerTextWithin(
    magnitude,
    negative ? "9223372036854775808" : "9223372036854775807",
  )
}
