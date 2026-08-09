import { z } from "zod"

import { losslessIntegerSchema } from "@/lib/lossless-integer"
import type { ApplicationResult } from "@/lib/schemas"

const timestampSchema = z.string().datetime({ offset: true })
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
const tradingStatusSchema = z
  .enum(["active", "halted", "inactive", "delisted"])
  .nullable()
const completenessSchema = z.enum(["complete", "truncated", "unavailable"])
const resultCompletenessSchema = z.enum(["complete", "truncated"])
const shardIdSchema = z
  .string()
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
  sourceId: z.string().min(1),
  venueId: z.string().min(1),
  instrumentId: z.string().uuid(),
  providerProduct: z.string().min(1),
  providerChannel: z.string().min(1),
  connectionGeneration: losslessIntegerSchema,
  stateRevision: losslessIntegerSchema,
  shardId: shardIdSchema,
  shardSnapshotRevision: losslessIntegerSchema,
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
    priceTicks: losslessIntegerSchema,
    quantityLots: losslessIntegerSchema,
  })
  .strict()

const dimensionSchema = z
  .object({
    completeness: completenessSchema,
    available: z.number().int().nonnegative(),
    returned: z.number().int().nonnegative(),
    configuredLimit: z.number().int().nonnegative(),
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
    configuredDepth: z.number().int().nonnegative(),
    stateBidDepth: z.number().int().nonnegative(),
    stateAskDepth: z.number().int().nonnegative(),
    snapshotBidDimension: dimensionSchema,
    snapshotAskDimension: dimensionSchema,
    resultBidDimension: dimensionSchema,
    resultAskDimension: dimensionSchema,
    bids: z.array(levelSchema),
    asks: z.array(levelSchema),
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
    ...identityShape,
    sourceIdentifier: z.string().min(1),
    stableTradeId: z.string().min(1),
    tradeConnectionGeneration: losslessIntegerSchema,
    priceTicks: losslessIntegerSchema,
    quantityLots: losslessIntegerSchema,
    aggressorSide: z.enum(["buy", "sell", "unknown"]),
    sourceTimestamp: timestampSchema.nullable(),
    receivedAt: timestampSchema,
    availableAt: timestampSchema,
    ingestedAt: timestampSchema,
    recordedQuality: qualitySchema,
    currentDisplayQuality: qualitySchema,
    recordedCoverage: z.enum(["sufficient", "insufficient", "unknown"]),
    assessmentId: z.string().min(1),
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
    committedStateRevision: losslessIntegerSchema,
    authority: z.literal("not_exposed"),
  })
  .strict()
  .superRefine((trade, context) => {
    if (trade.tradeConnectionGeneration !== trade.connectionGeneration) {
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
    tradingStatus: tradingStatusSchema,
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
    tradingStatus: tradingStatusSchema,
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
    ...identityShape,
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
    ...identityShape,
    asOf: timestampSchema,
    stateEvaluatedAt: timestampSchema,
    book: bookSchema,
    currentDisplayQuality: qualitySchema,
  })
  .strict()

const comparisonObservationSchema = z
  .object({
    sourceId: z.string().min(1),
    venueId: z.string().min(1),
    providerProduct: z.string().min(1),
    providerChannel: z.string().min(1),
    bid: levelSchema.nullable(),
    ask: levelSchema.nullable(),
    midpoint: z
      .object({
        numeratorTicks: z.string().regex(/^-?\d+$/),
        denominator: z.literal(2),
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
    observationCount: z.number().int().nonnegative(),
    comparable: z.boolean(),
    observations: z.array(comparisonObservationSchema),
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
  validateCounts(result, result.metadata.returnedItems, "market")
  return {
    completeness: result.metadata.completeness,
    returned: result.metadata.returnedItems,
    available: result.metadata.availableItems,
  }
}

export function instrumentTrades(
  result: ApplicationResult | undefined,
  instrumentId: string,
): InstrumentTrade[] {
  return parseInstrumentRows(result, tradeSchema, instrumentId, "market trades").map(
    (row) => ({
      key: `${identityKey(row)}\u0000${row.stableTradeId}`,
      sourceId: row.sourceId,
      venueId: row.venueId,
      stableTradeId: row.stableTradeId,
      priceTicks: row.priceTicks,
      quantityLots: row.quantityLots,
      availableAt: row.availableAt,
      currentQuality: row.currentDisplayQuality,
      fresh: row.freshAtReference,
    }),
  )
}

export function instrumentQuotes(
  result: ApplicationResult | undefined,
  instrumentId: string,
): InstrumentQuote[] {
  return parseInstrumentRows(result, quoteSchema, instrumentId, "market quotes").map(
    (row) => ({
      key: identityKey(row),
      sourceId: row.sourceId,
      venueId: row.venueId,
      bid: row.bid,
      ask: row.ask,
      asOf: row.asOf,
      stateEvaluatedAt: row.stateEvaluatedAt,
      currentQuality: row.currentDisplayQuality,
      crossed: row.crossed,
    }),
  )
}

export function instrumentBooks(
  result: ApplicationResult | undefined,
  instrumentId: string,
): InstrumentBook[] {
  return parseInstrumentRows(result, bookReadSchema, instrumentId, "market books").map(
    (row) => ({
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
    }),
  )
}

export function instrumentComparison(
  result: ApplicationResult | undefined,
  instrumentId: string,
): InstrumentComparison | null {
  const rows = parseRows(result, comparisonSchema, "market comparison")
  if (rows.some((row) => row.instrumentId !== instrumentId) || rows.length > 1) {
    throw new Error("The market comparison returned an incompatible instrument identity.")
  }
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

function parseInstrumentRows<T extends StreamIdentity>(
  result: ApplicationResult | undefined,
  schema: z.ZodType<T>,
  instrumentId: string,
  label: string,
): T[] {
  const rows = parseRows(result, schema, label)
  if (rows.some((row) => row.instrumentId !== instrumentId)) {
    throw new Error(`The ${label} result does not match the selected instrument.`)
  }
  assertUniqueIdentities(rows, label)
  return rows
}

function parseRows<T>(
  result: ApplicationResult | undefined,
  schema: z.ZodType<T>,
  label: string,
): T[] {
  if (!result) return []
  if (result.data === null) {
    validateCounts(result, 0, label)
    return []
  }
  const parsed = z.array(schema).safeParse(result.data)
  if (!parsed.success) {
    throw new Error(`The installed service returned an unsupported ${label} response.`)
  }
  validateCounts(result, parsed.data.length, label)
  return parsed.data
}

function validateCounts(result: ApplicationResult, actual: number, label: string) {
  const { availableItems, returnedItems } = result.metadata
  const completeness = resultCompletenessSchema.parse(result.metadata.completeness)
  if (actual === 0 && availableItems > 0) {
    throw new Error(
      `The ${label} result reports available rows, but none were returned within its bounds.`,
    )
  }
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
