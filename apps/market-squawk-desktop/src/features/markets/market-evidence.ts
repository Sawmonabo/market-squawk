import { z } from "zod"

import { losslessIntegerSchema } from "@/lib/lossless-integer"
import { applicationResultSchema, type ApplicationResult } from "@/lib/schemas"

const timestampSchema = z.iso.datetime({ offset: false, precision: 9 })
const positiveIntegerTextSchema = z
  .string()
  .regex(/^[1-9][0-9]*$/)
  .refine((value) => unsignedIntegerTextWithin(value, "18446744073709551615"))
const unsignedIntegerTextSchema = z
  .string()
  .regex(/^(?:0|[1-9][0-9]*)$/)
  .refine((value) => unsignedIntegerTextWithin(value, "18446744073709551615"))
const integerTextSchema = z
  .string()
  .regex(/^(?:0|-?[1-9][0-9]*)$/)
  .refine(signedIntegerTextWithinI64)
const midpointIntegerTextSchema = z
  .string()
  .regex(/^(?:0|-?[1-9][0-9]*)$/)
  .refine(signedIntegerTextWithinI128)
const nonnegativeIntegerSchema = z.number().int().nonnegative()
const boundedBookCountSchema = z.number().int().min(0).max(4_294_967_295)
const boundedObservationCountSchema = z.number().int().min(0).max(10_000_000)
const qualitySchema = z.enum([
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
const phaseSchema = z.enum([
  "disconnected",
  "awaiting_snapshot",
  "synchronizing",
  "healthy",
  "quarantined",
])
const tradingStatusSchema = z.enum(["active", "halted", "inactive", "delisted"])
const completenessSchema = z.enum(["complete", "truncated", "unavailable"])
const resultCompletenessSchema = z.enum(["complete", "truncated"])
const shardIdSchema = z
  .string()
  .min(1)
  .max(32)
  .regex(/^(0|[1-9]\d{0,4})\/([1-9]\d{0,4})$/)
  .superRefine((value, context) => {
    const separator = value.indexOf("/")
    if (separator < 1) {
      context.addIssue({ code: "custom", message: "Invalid live-state shard identity." })
      return
    }
    const index = Number(value.slice(0, separator))
    const count = Number(value.slice(separator + 1))
    if (index > 65_535 || count > 65_535 || index >= count) {
      context.addIssue({ code: "custom", message: "Invalid live-state shard identity." })
    }
  })

const identityShape = {
  sourceId: z.string().min(1).max(128),
  venueId: z.string().min(1).max(64),
  instrumentId: z.string().uuid(),
  providerProduct: z.string().min(1).max(512),
  providerChannel: z.string().min(1).max(512),
  connectionGeneration: losslessIntegerSchema,
  stateRevision: losslessIntegerSchema,
  shardId: shardIdSchema,
  shardSnapshotRevision: losslessIntegerSchema,
}

const detailIdentityShape = {
  sourceId: z.string().min(1).max(128),
  venueId: z.string().min(1).max(64),
  instrumentId: z.string().uuid(),
  providerProduct: z.string().min(1).max(512),
  providerChannel: z.string().min(1).max(512),
  connectionGeneration: positiveIntegerTextSchema,
  stateRevision: unsignedIntegerTextSchema,
  shardId: shardIdSchema,
  shardSnapshotRevision: positiveIntegerTextSchema,
}

type StreamIdentity = {
  sourceId: string
  venueId: string
  instrumentId: string
  providerProduct: string
  providerChannel: string
  connectionGeneration: string
  stateRevision: string
  shardId: string
  shardSnapshotRevision: string
}

const levelSchema = z
  .object({
    priceTicks: integerTextSchema,
    quantityLots: unsignedIntegerTextSchema,
  })
  .strict()

const dimensionSchema = z
  .object({
    completeness: completenessSchema,
    available: boundedBookCountSchema,
    returned: boundedBookCountSchema,
    configuredLimit: boundedBookCountSchema,
  })
  .strict()
  .superRefine((dimension, context) => {
    if (
      dimension.returned > dimension.available ||
      (dimension.completeness === "complete" &&
        dimension.returned !== dimension.available) ||
      (dimension.completeness === "truncated" &&
        (dimension.returned === 0 || dimension.returned >= dimension.available)) ||
      (dimension.completeness === "unavailable" && dimension.returned !== 0)
    ) {
      context.addIssue({ code: "custom", message: "Inconsistent book dimension." })
    }
  })

const bookSchema = z
  .object({
    configuredDepth: boundedBookCountSchema,
    stateBidDepth: boundedBookCountSchema,
    stateAskDepth: boundedBookCountSchema,
    snapshotBidDimension: dimensionSchema,
    snapshotAskDimension: dimensionSchema,
    resultBidDimension: dimensionSchema,
    resultAskDimension: dimensionSchema,
    bids: z.array(levelSchema).max(10_000),
    asks: z.array(levelSchema).max(10_000),
  })
  .strict()
  .superRefine((book, context) => {
    if (
      book.bids.length !== book.resultBidDimension.returned ||
      book.asks.length !== book.resultAskDimension.returned
    ) {
      context.addIssue({ code: "custom", message: "Book level counts are inconsistent." })
    }
  })

const tradeSchema = z
  .object({
    ...detailIdentityShape,
    sourceIdentifier: z.string().min(1).max(512),
    stableTradeId: z.string().min(1).max(512),
    tradeConnectionGeneration: positiveIntegerTextSchema,
    priceTicks: integerTextSchema,
    quantityLots: unsignedIntegerTextSchema,
    aggressorSide: z.enum(["buy", "sell", "unknown"]),
    sourceTimestamp: timestampSchema.nullable(),
    receivedAt: timestampSchema,
    availableAt: timestampSchema,
    ingestedAt: timestampSchema,
    recordedQuality: qualitySchema,
    currentDisplayQuality: qualitySchema,
    recordedCoverage: z.enum(["sufficient", "insufficient", "unknown"]),
    assessmentId: z.string().min(1).max(512),
    qualificationEvaluatedAt: timestampSchema,
    qualificationValidUntil: timestampSchema,
    freshAtReference: z.boolean(),
    payloadDigest: z
      .object({
        algorithm: z.enum(["sha256", "blake3"]),
        bytes: z.string().regex(/^[0-9a-f]{64}$/),
      })
      .strict(),
    bindingDigest: z.string().regex(/^[0-9a-f]{64}$/),
    tradeTradingStatus: tradingStatusSchema,
    committedStateRevision: positiveIntegerTextSchema,
    authority: z.literal("not_exposed"),
  })
  .strict()
  .superRefine((trade, context) => {
    if (
      trade.tradeConnectionGeneration !== trade.connectionGeneration ||
      compareUnsignedIntegerText(trade.committedStateRevision, trade.stateRevision) > 0
    ) {
      context.addIssue({ code: "custom", message: "Inconsistent trade identity." })
    }
  })

const snapshotSchema = z
  .object({
    ...identityShape,
    phase: phaseSchema,
    lastSequence: losslessIntegerSchema.nullable(),
    snapshotOriginRevision: losslessIntegerSchema.nullable(),
    snapshotInitialized: z.boolean(),
    generationCurrent: z.boolean(),
    healthEpoch: losslessIntegerSchema,
    sourceTimestamp: timestampSchema.nullable(),
    receivedAt: timestampSchema,
    evaluatedAt: timestampSchema,
    publishedAt: timestampSchema,
    recordedQuality: qualitySchema,
    currentDisplayQuality: qualitySchema,
    sourceValidUntil: timestampSchema,
    freshAtReference: z.boolean(),
    tradingStatus: tradingStatusSchema.nullable(),
    tradingStatusRevision: losslessIntegerSchema.nullable(),
    book: bookSchema,
    lastTrade: tradeSchema.nullable(),
    authority: z.literal("not_exposed"),
  })
  .strict()
  .superRefine((snapshot, context) => {
    if (
      snapshot.lastTrade !== null &&
      identityKey(snapshot.lastTrade) !== identityKey(snapshot)
    ) {
      context.addIssue({ code: "custom", message: "Inconsistent snapshot trade identity." })
    }
  })

const qualityEvidenceSchema = z
  .object({
    ...identityShape,
    recordedQuality: qualitySchema,
    currentDisplayQuality: qualitySchema,
    phase: phaseSchema,
    generationCurrent: z.boolean(),
    snapshotInitialized: z.boolean(),
    lastSequence: losslessIntegerSchema.nullable(),
    sourceTimestamp: timestampSchema.nullable(),
    receivedAt: timestampSchema,
    evaluatedAt: timestampSchema,
    sourceValidUntil: timestampSchema,
    referenceAt: timestampSchema,
    freshAtReference: z.boolean(),
    tradingStatus: tradingStatusSchema.nullable(),
    tradingStatusRevision: losslessIntegerSchema.nullable(),
    stateBidDepth: z.number().int().nonnegative(),
    stateAskDepth: z.number().int().nonnegative(),
    bidDimension: dimensionSchema,
    askDimension: dimensionSchema,
    crossedBook: z.boolean(),
    authority: z.literal("not_exposed"),
  })
  .strict()

const quoteSchema = z
  .object({
    ...detailIdentityShape,
    bid: levelSchema.nullable(),
    ask: levelSchema.nullable(),
    sourceTimestamp: z.null(),
    asOf: timestampSchema,
    stateEvaluatedAt: timestampSchema,
    recordedQuality: qualitySchema,
    currentDisplayQuality: qualitySchema,
    crossed: z.boolean(),
    authority: z.literal("not_exposed"),
  })
  .strict()

const bookReadSchema = z
  .object({
    ...detailIdentityShape,
    asOf: timestampSchema,
    stateEvaluatedAt: timestampSchema,
    book: bookSchema,
    currentDisplayQuality: qualitySchema,
  })
  .strict()

const comparisonObservationSchema = z
  .object({
    sourceId: z.string().min(1).max(128),
    venueId: z.string().min(1).max(64),
    providerProduct: z.string().min(1).max(512),
    providerChannel: z.string().min(1).max(512),
    bid: levelSchema.nullable(),
    ask: levelSchema.nullable(),
    midpoint: z
      .object({
        numeratorTicks: midpointIntegerTextSchema,
        denominator: z.literal("2"),
      })
      .strict()
      .nullable(),
    asOf: timestampSchema,
    stateEvaluatedAt: timestampSchema,
    recordedQuality: qualitySchema,
    currentDisplayQuality: qualitySchema,
  })
  .strict()

const comparisonSchema = z
  .object({
    instrumentId: z.string().uuid(),
    observationCount: boundedObservationCountSchema,
    comparable: z.boolean(),
    observations: z.array(comparisonObservationSchema).max(10_000_000),
    authority: z.literal("not_exposed"),
  })
  .strict()
  .superRefine((comparison, context) => {
    const identities = comparison.observations.map(
      (observation) =>
        `${observation.sourceId}\u0000${observation.venueId}\u0000${observation.providerProduct}\u0000${observation.providerChannel}`,
    )
    if (
      comparison.observationCount !== comparison.observations.length ||
      comparison.comparable !== (comparison.observationCount >= 2) ||
      new Set(identities).size !== identities.length
    ) {
      context.addIssue({ code: "custom", message: "Inconsistent comparison counts." })
    }
  })

const failedSourceSchema = z
  .object({
    surfaceId: z.string().min(1).max(512),
    reason: z.enum(["resource_exhausted", "unavailable"]),
  })
  .strict()

const detailSourceCoverageSchema = z
  .object({
    mode: z.literal("current_live_runtime"),
    consistency: z.enum(["per_shard_current_non_atomic", "partial_provider_set"]),
    historicalDataset: z.null(),
    requestedSourceCount: nonnegativeIntegerSchema,
    listedRequestedSources: z.array(z.string().min(1).max(512)).max(8),
    listedRequestedSourcesComplete: z.boolean(),
    observedSourceCount: nonnegativeIntegerSchema,
    listedSources: z.array(z.string().min(1).max(128)).max(8),
    listedSourcesComplete: z.boolean(),
    observedVenueCount: nonnegativeIntegerSchema,
    listedVenues: z.array(z.string().min(1).max(64)).max(8),
    listedVenuesComplete: z.boolean(),
    failedSourceCount: nonnegativeIntegerSchema,
    failedSources: z.array(failedSourceSchema).max(8),
    listedFailedSourcesComplete: z.boolean(),
    streamIdentityScope: z.literal("complete"),
    bookDepthScope: z.literal("per_record_explicit"),
    displayObservationCount: z.literal(0),
    krakenOrderLevelProjectionCount: z.literal(0),
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
    if (
      coverage.requestedSourceCount !== 0 ||
      coverage.listedRequestedSources.length !== 0 ||
      coverage.listedRequestedSourcesComplete !== true
    ) {
      context.addIssue({ code: "custom", message: "unexpected detail source request scope" })
    }
    if (
      coverage.failedSources.length !== Math.min(coverage.failedSourceCount, 8) ||
      coverage.listedFailedSourcesComplete !== (coverage.failedSourceCount <= 8) ||
      new Set(coverage.failedSources.map((source) => source.surfaceId)).size !==
        coverage.failedSources.length
    ) {
      context.addIssue({ code: "custom", message: "failed-source evidence mismatch" })
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

const qualityOrder = qualitySchema.options
const qualityCountSchema = z
  .object({
    quality: qualitySchema,
    count: z.number().int().positive(),
  })
  .strict()

const detailDataQualitySchema = z
  .object({
    referenceAt: timestampSchema,
    recordedClassifications: z.array(qualityCountSchema).max(9),
    currentDisplayClassifications: z.array(qualityCountSchema).max(9),
    freshObservations: nonnegativeIntegerSchema,
    staleObservations: nonnegativeIntegerSchema,
    authority: z.literal("not_exposed"),
  })
  .strict()
  .superRefine((quality, context) => {
    const total = quality.freshObservations + quality.staleObservations
    for (const classifications of [
      quality.recordedClassifications,
      quality.currentDisplayClassifications,
    ]) {
      if (
        classifications.reduce((sum, item) => sum + item.count, 0) !== total ||
        new Set(classifications.map((item) => item.quality)).size !== classifications.length ||
        classifications.some(
          (item, index) =>
            index > 0 &&
            qualityOrder.indexOf(item.quality) <=
              qualityOrder.indexOf(classifications[index - 1]?.quality ?? "direct_verified"),
        )
      ) {
        context.addIssue({ code: "custom", message: "quality summary mismatch" })
      }
    }
  })

function detailResultSchema<Row extends z.ZodType>(row: Row) {
  return z
    .object({
      data: z.array(row).nullable(),
      metadata: z
        .object({
          completeness: resultCompletenessSchema,
          returnedItems: nonnegativeIntegerSchema,
          availableItems: nonnegativeIntegerSchema,
          sourceCoverage: detailSourceCoverageSchema,
          dataQuality: detailDataQualitySchema,
        })
        .strict(),
    })
    .strict()
    .superRefine((result, context) => {
      const rows = result.data ?? []
      const { metadata } = result
      if (
        rows.length !== metadata.returnedItems ||
        (result.data === null) !== (metadata.returnedItems === 0) ||
        metadata.returnedItems > metadata.availableItems ||
        metadata.completeness !==
          (metadata.returnedItems === metadata.availableItems ? "complete" : "truncated")
      ) {
        context.addIssue({ code: "custom", message: "result count mismatch" })
      }
      if (
        metadata.sourceCoverage.availability !==
        (metadata.availableItems === 0 ? "no_current_observation" : "current")
      ) {
        context.addIssue({ code: "custom", message: "result availability mismatch" })
      }
    })
}

const tradeResultSchema = detailResultSchema(tradeSchema)
const quoteResultSchema = detailResultSchema(quoteSchema)
const bookResultSchema = detailResultSchema(bookReadSchema)
const comparisonResultSchema = detailResultSchema(comparisonSchema)

export interface MarketEvidence {
  key: string
  sourceId: string
  venueId: string
  instrumentId: string
  providerProduct: string
  providerChannel: string
  phase: string
  asOf: string
  receivedAt: string
  sourceValidUntil: string
  recordedQuality: string
  currentQuality: string
  fresh: boolean
  snapshotInitialized: boolean
  generationCurrent: boolean
  crossedBook: boolean | null
  lastSequence: string | null
  bidDepth: number
  askDepth: number
  bestBid: BookLevel | null
  bestAsk: BookLevel | null
}

export interface BookLevel {
  priceTicks: string
  quantityLots: string
}

export interface InstrumentTrade {
  key: string
  sourceId: string
  venueId: string
  stableTradeId: string
  priceTicks: string
  quantityLots: string
  availableAt: string
  currentQuality: string
  fresh: boolean
}

export interface InstrumentQuote {
  key: string
  sourceId: string
  venueId: string
  bid: BookLevel | null
  ask: BookLevel | null
  asOf: string
  stateEvaluatedAt: string
  currentQuality: string
  crossed: boolean
}

export interface InstrumentBook {
  key: string
  sourceId: string
  venueId: string
  bids: BookLevel[]
  asks: BookLevel[]
  asOf: string
  stateEvaluatedAt: string
  currentQuality: string
  bidCompleteness: string
  askCompleteness: string
}

export interface InstrumentComparison {
  observationCount: number
  comparable: boolean
  observations: Array<{
    key: string
    sourceId: string
    venueId: string
    bid: BookLevel | null
    ask: BookLevel | null
    asOf: string
    currentQuality: string
  }>
}

export function marketEvidence(
  snapshot: ApplicationResult | undefined,
  quality: ApplicationResult | undefined,
): MarketEvidence[] {
  const snapshots = parseRows(snapshot, snapshotSchema, "market snapshot")
  const qualities = parseRows(quality, qualityEvidenceSchema, "market quality")
  assertUniqueIdentities(snapshots, "market snapshot")
  assertUniqueIdentities(qualities, "market quality")
  const byKey = new Map<string, MarketEvidence>()

  for (const row of snapshots) {
    const key = identityKey(row)
    byKey.set(key, {
      key,
      sourceId: row.sourceId,
      venueId: row.venueId,
      instrumentId: row.instrumentId,
      providerProduct: row.providerProduct,
      providerChannel: row.providerChannel,
      phase: row.phase,
      asOf: row.publishedAt,
      receivedAt: row.receivedAt,
      sourceValidUntil: row.sourceValidUntil,
      recordedQuality: row.recordedQuality,
      currentQuality: row.currentDisplayQuality,
      fresh: row.freshAtReference,
      snapshotInitialized: row.snapshotInitialized,
      generationCurrent: row.generationCurrent,
      crossedBook: null,
      lastSequence: row.lastSequence,
      bidDepth: row.book.stateBidDepth,
      askDepth: row.book.stateAskDepth,
      bestBid: row.book.bids[0] ?? null,
      bestAsk: row.book.asks[0] ?? null,
    })
  }

  for (const row of qualities) {
    const key = identityKey(row)
    const prior = byKey.get(key)
    byKey.set(key, {
      key,
      sourceId: row.sourceId,
      venueId: row.venueId,
      instrumentId: row.instrumentId,
      providerProduct: row.providerProduct,
      providerChannel: row.providerChannel,
      phase: row.phase,
      asOf: prior?.asOf ?? row.referenceAt,
      receivedAt: row.receivedAt,
      sourceValidUntil: row.sourceValidUntil,
      recordedQuality: row.recordedQuality,
      currentQuality: row.currentDisplayQuality,
      fresh: row.freshAtReference,
      snapshotInitialized: row.snapshotInitialized,
      generationCurrent: row.generationCurrent,
      crossedBook: row.crossedBook,
      lastSequence: row.lastSequence,
      bidDepth: row.stateBidDepth,
      askDepth: row.stateAskDepth,
      bestBid: prior?.bestBid ?? null,
      bestAsk: prior?.bestAsk ?? null,
    })
  }

  return [...byKey.values()].sort((left, right) =>
    `${left.instrumentId}:${left.venueId}:${left.sourceId}`.localeCompare(
      `${right.instrumentId}:${right.venueId}:${right.sourceId}`,
    ),
  )
}

export function resultState(result: ApplicationResult | undefined) {
  if (!result) return null
  const parsed = applicationResultSchema.parse(result)
  validateCounts(parsed, parsed.metadata.returnedItems, "market")
  return {
    completeness: parsed.metadata.completeness,
    returned: parsed.metadata.returnedItems,
    available: parsed.metadata.availableItems,
  }
}

export function instrumentTrades(
  result: ApplicationResult | undefined,
  instrumentId: string,
): InstrumentTrade[] {
  const parsed = parseDetailResult(result, tradeResultSchema, "market trades")
  if (!parsed) return []
  const rows = bindInstrumentRows(
    parsed.data ?? [],
    instrumentId,
    "market trades",
  )
  validateDetailEvidence(parsed.metadata, rows, "market trades", (row) => ({
    recordedQuality: row.recordedQuality,
    currentQuality: row.currentDisplayQuality,
    fresh: row.freshAtReference,
  }))
  return rows.map((row) => ({
      key: `${identityKey(row)}\u0000${row.stableTradeId}`,
      sourceId: row.sourceId,
      venueId: row.venueId,
      stableTradeId: row.stableTradeId,
      priceTicks: row.priceTicks,
      quantityLots: row.quantityLots,
      availableAt: row.availableAt,
      currentQuality: row.currentDisplayQuality,
      fresh: row.freshAtReference,
    }))
}

export function instrumentQuotes(
  result: ApplicationResult | undefined,
  instrumentId: string,
): InstrumentQuote[] {
  const parsed = parseDetailResult(result, quoteResultSchema, "market quotes")
  if (!parsed) return []
  const rows = bindInstrumentRows(
    parsed.data ?? [],
    instrumentId,
    "market quotes",
  )
  validateDetailEvidence(parsed.metadata, rows, "market quotes", (row) => ({
    recordedQuality: row.recordedQuality,
    currentQuality: row.currentDisplayQuality,
  }))
  return rows.map((row) => ({
      key: identityKey(row),
      sourceId: row.sourceId,
      venueId: row.venueId,
      bid: row.bid,
      ask: row.ask,
      asOf: row.asOf,
      stateEvaluatedAt: row.stateEvaluatedAt,
      currentQuality: row.currentDisplayQuality,
      crossed: row.crossed,
    }))
}

export function instrumentBooks(
  result: ApplicationResult | undefined,
  instrumentId: string,
): InstrumentBook[] {
  const parsed = parseDetailResult(result, bookResultSchema, "market books")
  if (!parsed) return []
  const rows = bindInstrumentRows(
    parsed.data ?? [],
    instrumentId,
    "market books",
  )
  validateDetailEvidence(parsed.metadata, rows, "market books", (row) => ({
    currentQuality: row.currentDisplayQuality,
  }))
  return rows.map((row) => ({
      key: identityKey(row),
      sourceId: row.sourceId,
      venueId: row.venueId,
      bids: row.book.bids,
      asks: row.book.asks,
      asOf: row.asOf,
      stateEvaluatedAt: row.stateEvaluatedAt,
      currentQuality: row.currentDisplayQuality,
      bidCompleteness: row.book.resultBidDimension.completeness,
      askCompleteness: row.book.resultAskDimension.completeness,
    }))
}

export function instrumentComparison(
  result: ApplicationResult | undefined,
  instrumentId: string,
): InstrumentComparison | null {
  const parsed = parseDetailResult(
    result,
    comparisonResultSchema,
    "market comparison",
  )
  if (!parsed) return null
  const rows = parsed.data ?? []
  if (rows.some((row) => row.instrumentId !== instrumentId) || rows.length > 1) {
    throw new Error("The market comparison returned an incompatible instrument identity.")
  }
  const observations = rows.flatMap((row) => row.observations)
  validateDetailEvidence(
    parsed.metadata,
    observations,
    "market comparison",
    (row) => ({
      recordedQuality: row.recordedQuality,
      currentQuality: row.currentDisplayQuality,
    }),
    true,
  )
  const row = rows[0]
  return row
    ? {
        observationCount: row.observationCount,
        comparable: row.comparable,
        observations: row.observations.map((observation) => ({
          key: `${observation.sourceId}\u0000${observation.venueId}\u0000${observation.providerProduct}\u0000${observation.providerChannel}`,
          sourceId: observation.sourceId,
          venueId: observation.venueId,
          bid: observation.bid,
          ask: observation.ask,
          asOf: observation.asOf,
          currentQuality: observation.currentDisplayQuality,
        })),
      }
    : null
}

function bindInstrumentRows<T extends StreamIdentity>(
  rows: T[],
  instrumentId: string,
  label: string,
): T[] {
  if (rows.some((row) => row.instrumentId !== instrumentId)) {
    throw new Error(`The ${label} result does not match the selected instrument.`)
  }
  assertUniqueIdentities(rows, label)
  return rows
}

function parseDetailResult<Schema extends z.ZodType>(
  result: ApplicationResult | undefined,
  schema: Schema,
  label: string,
): z.output<Schema> | null {
  if (!result) return null
  const parsed = schema.safeParse(result)
  if (!parsed.success) {
    throw new Error(`The installed service returned an unsupported ${label} response.`)
  }
  return parsed.data
}

type DetailEvidenceRow = {
  sourceId: string
  venueId: string
}

type DetailQualityEvidence = {
  recordedQuality?: z.infer<typeof qualitySchema>
  currentQuality: z.infer<typeof qualitySchema>
  fresh?: boolean
}

function validateDetailEvidence<T extends DetailEvidenceRow>(
  metadata: z.infer<typeof tradeResultSchema>["metadata"],
  visibleRows: T[],
  label: string,
  quality: (row: T) => DetailQualityEvidence,
  nested = false,
) {
  const totalQuality =
    metadata.dataQuality.freshObservations + metadata.dataQuality.staleObservations
  const exactObservationCount = nested
    ? metadata.completeness === "complete"
      ? visibleRows.length
      : null
    : metadata.availableItems
  if (
    (exactObservationCount !== null && totalQuality !== exactObservationCount) ||
    (metadata.completeness === "truncated" && visibleRows.length > totalQuality)
  ) {
    throw new Error(`The ${label} quality evidence counts are inconsistent.`)
  }

  const visible = visibleRows.map(quality)
  validateClassificationEvidence(
    metadata.dataQuality.recordedClassifications,
    visible.flatMap((row) => row.recordedQuality ? [row.recordedQuality] : []),
    metadata.completeness === "complete" && visible.every(
      (row) => row.recordedQuality !== undefined,
    ),
    label,
  )
  validateClassificationEvidence(
    metadata.dataQuality.currentDisplayClassifications,
    visible.map((row) => row.currentQuality),
    metadata.completeness === "complete",
    label,
  )
  const visibleFreshness = visible.flatMap((row) =>
    row.fresh === undefined ? [] : [row.fresh],
  )
  if (
    visibleFreshness.length > 0 &&
    (visibleFreshness.filter(Boolean).length > metadata.dataQuality.freshObservations ||
      visibleFreshness.filter((fresh) => !fresh).length >
        metadata.dataQuality.staleObservations ||
      (metadata.completeness === "complete" &&
        visibleFreshness.length === visible.length &&
        (visibleFreshness.filter(Boolean).length !==
          metadata.dataQuality.freshObservations ||
          visibleFreshness.filter((fresh) => !fresh).length !==
            metadata.dataQuality.staleObservations)))
  ) {
    throw new Error(`The ${label} freshness evidence is inconsistent.`)
  }

  const visibleSources = new Set(visibleRows.map((row) => row.sourceId))
  const visibleVenues = new Set(visibleRows.map((row) => row.venueId))
  if (
    visibleSources.size > metadata.sourceCoverage.observedSourceCount ||
    visibleVenues.size > metadata.sourceCoverage.observedVenueCount ||
    (metadata.sourceCoverage.listedSourcesComplete &&
      [...visibleSources].some(
        (source) => !metadata.sourceCoverage.listedSources.includes(source),
      )) ||
    (metadata.sourceCoverage.listedVenuesComplete &&
      [...visibleVenues].some(
        (venue) => !metadata.sourceCoverage.listedVenues.includes(venue),
      ))
  ) {
    throw new Error(`The ${label} source evidence is inconsistent.`)
  }
}

function validateClassificationEvidence(
  summary: Array<{ quality: z.infer<typeof qualitySchema>; count: number }>,
  visible: Array<z.infer<typeof qualitySchema>>,
  exact: boolean,
  label: string,
) {
  const counts = new Map(summary.map((item) => [item.quality, item.count]))
  const visibleCounts = new Map<z.infer<typeof qualitySchema>, number>()
  for (const classification of visible) {
    visibleCounts.set(classification, (visibleCounts.get(classification) ?? 0) + 1)
  }
  if (
    [...visibleCounts].some(
      ([classification, count]) => count > (counts.get(classification) ?? 0),
    ) ||
    (exact &&
      (counts.size !== visibleCounts.size ||
        [...counts].some(
          ([classification, count]) => visibleCounts.get(classification) !== count,
        )))
  ) {
    throw new Error(`The ${label} quality classifications are inconsistent.`)
  }
}

function parseRows<T>(
  result: ApplicationResult | undefined,
  schema: z.ZodType<T>,
  label: string,
): T[] {
  if (!result) return []
  const envelope = applicationResultSchema.safeParse(result)
  if (!envelope.success) {
    throw new Error(`The installed service returned an unsupported ${label} response.`)
  }
  if (envelope.data.data === null) {
    validateCounts(envelope.data, 0, label)
    return []
  }
  const parsed = z.array(schema).safeParse(envelope.data.data)
  if (!parsed.success) {
    throw new Error(`The installed service returned an unsupported ${label} response.`)
  }
  validateCounts(envelope.data, parsed.data.length, label)
  return parsed.data
}

function validateCounts(result: ApplicationResult, actual: number, label: string) {
  const { availableItems, returnedItems } = result.metadata
  const completeness = resultCompletenessSchema.parse(result.metadata.completeness)
  if (
    returnedItems !== actual ||
    returnedItems > availableItems ||
    (completeness === "complete" && returnedItems !== availableItems) ||
    (completeness === "truncated" && returnedItems >= availableItems)
  ) {
    throw new Error(`The ${label} result counts are inconsistent.`)
  }
}

function assertUniqueIdentities(
  rows: StreamIdentity[],
  label: string,
) {
  const identities = rows.map(identityKey)
  if (new Set(identities).size !== identities.length) {
    throw new Error(`The ${label} result contains a duplicate stream identity.`)
  }
}

function identityKey(value: StreamIdentity) {
  return [
    value.sourceId,
    value.venueId,
    value.instrumentId,
    value.providerProduct,
    value.providerChannel,
    value.connectionGeneration,
    value.stateRevision,
    value.shardId,
    value.shardSnapshotRevision,
  ].join("\u0000")
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
    listed.some(
      (item, index) => index > 0 && item <= (listed[index - 1] ?? ""),
    )
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

function signedIntegerTextWithinI128(value: string) {
  const negative = value.startsWith("-")
  const magnitude = negative ? value.slice(1) : value
  return unsignedIntegerTextWithin(
    magnitude,
    negative
      ? "170141183460469231731687303715884105728"
      : "170141183460469231731687303715884105727",
  )
}

function compareUnsignedIntegerText(left: string, right: string) {
  return left.length === right.length
    ? left === right
      ? 0
      : left < right
        ? -1
        : 1
    : left.length < right.length
      ? -1
      : 1
}
