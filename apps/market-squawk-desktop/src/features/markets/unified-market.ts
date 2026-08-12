import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"

const timestampSchema = z.iso.datetime({ offset: false, precision: 9 })
const canonicalDecimalSchema = z
  .string()
  .regex(/^-?(?:0|[1-9][0-9]*)(?:\.[0-9]*[1-9])?$/)
const positiveIntegerTextSchema = z.string().regex(/^[1-9][0-9]*$/)

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

const marketFeatureAvailabilitySchema = z.discriminatedUnion("availability", [
  z
    .object({
      availability: z.literal("available"),
      sourceId: z.string().min(1),
      venueId: z.string().min(1),
      instrumentId: z.string().uuid(),
      generation: positiveIntegerTextSchema,
      availableAt: timestampSchema,
      contentDigest: evidenceDigestSchema,
      valueCount: z.number().int().nonnegative(),
    })
    .strict(),
  z
    .object({
      availability: z.literal("unavailable"),
      reason: z.enum([
        "source_does_not_publish_live_features",
        "incomplete_snapshot",
        "no_exact_source_generation",
        "available_after_selection",
        "incomplete_value_set",
      ]),
    })
    .strict(),
])

const marketInvestmentObservationSchema = z.discriminatedUnion("availability", [
  z
    .object({
      availability: z.literal("available"),
      instrumentId: z.string().uuid(),
      mark: z
        .object({
          value: canonicalDecimalSchema,
          currency: z.string().regex(/^[A-Z]{3}$/),
          basis: z.enum(["fresh_last_trade", "fresh_bid_ask_midpoint"]),
          evidenceIdentity: sha256EvidenceDigestSchema,
          freshUntil: timestampSchema.nullable(),
        })
        .strict(),
      selectionDigest: sha256EvidenceDigestSchema,
      selectedAt: timestampSchema,
      generation: positiveIntegerTextSchema.nullable(),
      quality: marketQualitySchema,
      depth: marketDepthSchema.nullable(),
      coverage: marketCoverageSchema,
      integrity: marketIntegritySchema,
      features: marketFeatureAvailabilitySchema,
    })
    .strict(),
  z
    .object({
      availability: z.literal("unavailable"),
      reason: z.enum(["no_eligible_source", "no_fresh_last_trade_or_midpoint"]),
    })
    .strict(),
])

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
      maximumAgeNanos: z.number().int().nonnegative(),
      selectedAgeNanos: z.number().int().nonnegative(),
    })
    .strict(),
])

const marketSelectionReceiptSchema = z
  .object({
    policyRevision: z.number().int().min(1).max(4_294_967_295),
    policyCandidateLimit: z.number().int().min(1).max(4_096),
    policyDigest: sha256EvidenceDigestSchema,
    selectionDigest: sha256EvidenceDigestSchema,
    selectedAt: timestampSchema,
    eligibleCount: z.number().int().min(0).max(4_096),
    rejectedCount: z.number().int().min(0).max(4_096),
    availableAlternativeCount: z.number().int().min(0).max(4_096),
    returnedAlternativeCount: z.number().int().min(0).max(8),
    alternativesComplete: z.boolean(),
    selectionClass: z
      .enum(["exact_requirements", "admitted_downgrade"])
      .nullable(),
    downgradeDimensions: z.array(marketDowngradeDimensionSchema).max(5),
  })
  .strict()

const quoteSchema = z
  .object({
    bidPrice: z.string().nullable(),
    bidPriceProviderLexeme: z.string().nullable(),
    bidSize: z.string().nullable(),
    bidSizeProviderLexeme: z.string().nullable(),
    askPrice: z.string().nullable(),
    askPriceProviderLexeme: z.string().nullable(),
    askSize: z.string().nullable(),
    askSizeProviderLexeme: z.string().nullable(),
    midPrice: z.string().nullable(),
    midPriceBasis: z.string().nullable(),
    lastPrice: z.string().nullable(),
    lastPriceProviderLexeme: z.string().nullable(),
    lastSize: z.string().nullable(),
    lastSizeProviderLexeme: z.string().nullable(),
    lastSourceTimestamp: z.string().nullable(),
    lastReceivedAt: z.string().nullable(),
    lastAvailableAt: z.string().nullable(),
    lastQuality: z.string().nullable(),
    lastFreshAtSelection: z.boolean().nullable(),
    quoteEvidence: z.record(z.string(), z.unknown()).nullable(),
    tradeEvidence: z.record(z.string(), z.unknown()).nullable(),
  })
  .strict()

const selectedSourceSchema = z
  .object({
    surfaceId: z.string().min(1),
    providerId: z.string().min(1),
    providerSymbol: z.string().min(1).optional(),
    sourceId: z.string().min(1),
    venueId: z.string().nullable(),
    providerProduct: z.string().min(1),
    providerChannel: z.string().min(1),
    timing: z.string().min(1),
    depth: z.string().nullable(),
    depthLabel: z.string().min(1),
    quality: z.string().min(1),
    coverage: z.string().min(1),
    health: z.string().min(1),
    freshness: z
      .object({
        receivedAt: z.string(),
        availableAt: z.string(),
        sourceValidUntil: z.string().nullable(),
        freshAtSelection: z.boolean(),
      })
      .passthrough(),
    integrity: z
      .object({
        state: z.string().min(1),
        phase: z.string().min(1),
        generationCurrent: z.boolean().nullable(),
        snapshotInitialized: z.boolean(),
      })
      .passthrough(),
  })
  .passthrough()

const orderLevelOrderSchema = z
  .object({
    orderId: z.string().min(1),
    side: z.enum(["bid", "ask"]),
    price: z.string().min(1),
    priceTicks: z.string().regex(/^-?[0-9]+$/),
    quantity: z.string().min(1),
    quantityLots: z.string().regex(/^-?[0-9]+$/),
    providerOrderTimestamp: z.string().nullable(),
    providerPriority: z
      .object({
        value: z.string().regex(/^[0-9]+$/),
        rule: z.string().min(1),
      })
      .strict()
      .nullable(),
    firstSeenIn: z.enum(["snapshot", "update"]),
    lastUpdatedIn: z.enum(["snapshot", "update"]),
    lastSourceTimestamp: z.string(),
    lastReceivedAt: z.string(),
    arrivalOrdinal: z.string().regex(/^[0-9]+$/),
  })
  .strict()

const orderBookSchema = z
  .object({
    depth: z.literal("order_level"),
    revision: z.string().regex(/^[0-9]+$/),
    phase: z.enum(["awaiting_snapshot", "healthy", "quarantined"]),
    quarantineReason: z.string().nullable(),
    quality: z.string().min(1),
    freshness: z.enum(["uninitialized", "fresh", "stale"]),
    lastMarketAt: z.string().nullable(),
    availableAt: timestampSchema,
    usableForSelection: z.boolean(),
    totalOrderCount: z.number().int().nonnegative(),
    returnedOrderCount: z.number().int().nonnegative(),
    sampleTruncated: z.boolean(),
    samplePolicy: z.literal("stable_provider_order_id_prefix"),
    orders: z.array(orderLevelOrderSchema).max(64),
  })
  .strict()

export const unifiedMarketRowSchema = z
  .object({
    instrumentId: z.string().uuid(),
    symbol: z.string().min(1),
    symbolKind: z.string().min(1),
    symbolVenueId: z.string().min(1).nullable(),
    assetClass: z.string().min(1),
    quoteCurrency: z.string().min(1),
    definitionKind: z.string().min(1),
    definitionRevision: z.number().int().positive().nullable(),
    referenceRevision: z.string().min(1).nullable(),
    permanentFigi: z.string().min(1).nullable(),
    displayName: z.string().min(1).nullable(),
    tickSize: z.string().min(1).nullable(),
    lotSize: z.string().min(1).nullable(),
    executionTermsAvailable: z.boolean(),
    referenceEvidence: z.record(z.string(), z.unknown()).nullable(),
    availability: z.string().min(1),
    confidence: z.string().min(1),
    quote: quoteSchema,
    orderBook: orderBookSchema.nullable(),
    marketObservation: marketInvestmentObservationSchema,
    selectedSource: selectedSourceSchema.nullable(),
    alternatives: z.array(z.record(z.string(), z.unknown())),
    selectionReceipt: marketSelectionReceiptSchema,
  })
  .strict()

const unifiedMarketRowsSchema = z.array(unifiedMarketRowSchema).nullable()

export type UnifiedMarketRow = z.infer<typeof unifiedMarketRowSchema>

export function unifiedMarketRows(result: ApplicationResult | undefined): UnifiedMarketRow[] {
  if (!result) return []
  return unifiedMarketRowsSchema.parse(result.data) ?? []
}
