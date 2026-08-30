import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"

const timestampSchema = z.iso.datetime({ offset: false, precision: 9 })
const decimalSchema = z
  .string()
  .regex(/^-?(?:0|[1-9][0-9]*)(?:\.[0-9]*[1-9])?$/)
  .refine((value) => value !== "-0")
const countSchema = z.number().int().nonnegative()
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
const depthKindSchema = z.enum([
  "top_of_book",
  "price_level",
  "order_level",
  "none",
])

const currentPriceSchema = z
  .object({
    value: decimalSchema,
    currency: z.string().regex(/^[A-Z]{3}$/),
    basis: z.enum(["last_trade", "bid_ask_midpoint"]),
    observedAt: timestampSchema.nullable(),
    currentThrough: timestampSchema.nullable(),
  })
  .strict()

const quoteSchema = z
  .object({
    bidPrice: decimalSchema.nullable(),
    bidSize: decimalSchema.nullable(),
    askPrice: decimalSchema.nullable(),
    askSize: decimalSchema.nullable(),
    midPrice: decimalSchema.nullable(),
    lastPrice: decimalSchema.nullable(),
    lastSize: decimalSchema.nullable(),
    quoteObservedAt: timestampSchema.nullable(),
    lastObservedAt: timestampSchema.nullable(),
  })
  .strict()
  .superRefine((quote, context) => {
    if ((quote.bidPrice === null) !== (quote.bidSize === null)) {
      context.addIssue({ code: "custom", message: "bid price and size must agree" })
    }
    if ((quote.askPrice === null) !== (quote.askSize === null)) {
      context.addIssue({ code: "custom", message: "ask price and size must agree" })
    }
    if (
      (quote.midPrice !== null) !==
      (quote.bidPrice !== null && quote.askPrice !== null)
    ) {
      context.addIssue({ code: "custom", message: "midpoint must match both quote sides" })
    }
    if ((quote.lastPrice === null) !== (quote.lastSize === null)) {
      context.addIssue({ code: "custom", message: "last price and size must agree" })
    }
  })

const marketStateSchema = z
  .object({
    timing: z
      .enum(["real_time", "delayed", "end_of_day", "historical", "stored"])
      .nullable(),
    quality: z.enum([
      "verified",
      "direct",
      "official_delayed",
      "aggregated",
      "indicative",
      "modeled",
      "estimated",
      "stale",
      "unavailable",
    ]),
    health: z.enum(["healthy", "degraded", "unavailable", "quarantined"]),
    integrity: z.enum([
      "verified",
      "unverified",
      "not_applicable",
      "failed",
      "quarantined",
      "unavailable",
    ]),
    coverage: z.enum([
      "broad",
      "partial",
      "single_market",
      "benchmark",
      "reference",
      "account_owned",
      "unavailable",
    ]),
    depth: depthKindSchema,
    freshness: z.enum(["fresh", "stale", "unavailable"]),
    observedAt: timestampSchema.nullable(),
    updatedAt: timestampSchema,
    currentThrough: timestampSchema.nullable(),
  })
  .strict()

const observationsSchema = z
  .object({
    admittedCount: countSchema,
    independentCount: countSchema.nullable(),
    agreement: z.literal("not_established"),
  })
  .strict()

const depthSummarySchema = z
  .object({
    kind: depthKindSchema,
    bidLevels: countSchema,
    askLevels: countSchema,
    individualOrderCount: countSchema,
    truncated: z.boolean(),
  })
  .strict()

const levelSchema = z
  .object({
    price: decimalSchema,
    quantity: decimalSchema,
  })
  .strict()

const individualOrdersSchema = z
  .object({
    bidOrders: z.array(levelSchema).max(64),
    askOrders: z.array(levelSchema).max(64),
    totalCount: countSchema,
    returnedCount: countSchema,
    truncated: z.boolean(),
  })
  .strict()
  .superRefine((orders, context) => {
    if (orders.returnedCount !== orders.bidOrders.length + orders.askOrders.length) {
      context.addIssue({ code: "custom", message: "individual order count mismatch" })
    }
    if (orders.returnedCount > orders.totalCount) {
      context.addIssue({ code: "custom", message: "individual order total is too small" })
    }
  })

const depthDetailsSchema = z
  .object({
    kind: depthKindSchema,
    bids: z.array(levelSchema).max(64),
    asks: z.array(levelSchema).max(64),
    individualOrders: individualOrdersSchema.nullable(),
  })
  .strict()

export const marketProductRowSchema = z
  .object({
    instrumentId: z.string().uuid(),
    displaySymbol: z.string().min(1).max(512).nullable(),
    name: z.string().min(1).max(512).nullable(),
    assetClass: assetClassSchema,
    currency: z.string().regex(/^[A-Z]{3}$/),
    availability: z.enum([
      "live",
      "delayed",
      "end_of_day",
      "stored",
      "stale",
      "unavailable",
    ]),
    confidence: z.enum(["high", "moderate", "limited", "unavailable"]),
    currentPrice: currentPriceSchema.nullable(),
    quote: quoteSchema,
    marketState: marketStateSchema,
    observations: observationsSchema,
    depthSummary: depthSummarySchema,
    depthDetails: depthDetailsSchema.nullable(),
    analysisUse: z.enum(["current_only", "unavailable"]),
  })
  .strict()
  .superRefine((row, context) => {
    const unavailable = row.availability === "unavailable"
    if (
      unavailable !==
      (row.confidence === "unavailable" &&
        row.marketState.freshness === "unavailable" &&
        row.analysisUse === "unavailable")
    ) {
      context.addIssue({ code: "custom", message: "market availability state mismatch" })
    }
    if (row.currentPrice && row.currentPrice.currency !== row.currency) {
      context.addIssue({ code: "custom", message: "current price currency mismatch" })
    }
    if (
      row.currentPrice?.basis === "last_trade" &&
      row.currentPrice.value !== row.quote.lastPrice
    ) {
      context.addIssue({ code: "custom", message: "last-trade mark mismatch" })
    }
    if (
      row.currentPrice?.basis === "bid_ask_midpoint" &&
      row.currentPrice.value !== row.quote.midPrice
    ) {
      context.addIssue({ code: "custom", message: "midpoint mark mismatch" })
    }
    if (row.depthDetails) {
      if (
        row.depthDetails.kind !== row.depthSummary.kind ||
        row.depthDetails.bids.length !== row.depthSummary.bidLevels ||
        row.depthDetails.asks.length !== row.depthSummary.askLevels
      ) {
        context.addIssue({ code: "custom", message: "market depth summary mismatch" })
      }
    }
  })

const marketProductResultSchema = z
  .object({
    data: z.array(marketProductRowSchema).nullable(),
    metadata: z
      .object({
        completeness: z.enum(["complete", "truncated"]),
        returnedItems: countSchema,
        availableItems: countSchema,
        sourceCoverage: z
          .object({
            availability: z.enum(["available", "unavailable"]),
            complete: z.boolean(),
            returnedInstrumentCount: countSchema,
            observationCount: countSchema,
          })
          .strict(),
        dataQuality: z
          .object({
            referenceAt: timestampSchema,
            observationCount: countSchema,
          })
          .strict(),
      })
      .strict(),
  })
  .strict()
  .superRefine((result, context) => {
    const rows = result.data ?? []
    if (rows.length !== result.metadata.returnedItems) {
      context.addIssue({ code: "custom", message: "returned market count mismatch" })
    }
    if ((result.data === null) !== (result.metadata.returnedItems === 0)) {
      context.addIssue({ code: "custom", message: "empty market result mismatch" })
    }
    if (result.metadata.availableItems < result.metadata.returnedItems) {
      context.addIssue({ code: "custom", message: "available market count is too small" })
    }
    if (
      result.metadata.completeness !==
      (result.metadata.availableItems === result.metadata.returnedItems
        ? "complete"
        : "truncated")
    ) {
      context.addIssue({ code: "custom", message: "market result completeness mismatch" })
    }
    if (
      result.metadata.sourceCoverage.returnedInstrumentCount !==
        result.metadata.returnedItems ||
      result.metadata.sourceCoverage.observationCount !==
        result.metadata.dataQuality.observationCount
    ) {
      context.addIssue({ code: "custom", message: "market result evidence count mismatch" })
    }
    if (
      new Set(rows.map((row) => row.instrumentId)).size !== rows.length ||
      rows.some(
        (row, index) =>
          index > 0 && row.instrumentId <= (rows[index - 1]?.instrumentId ?? ""),
      )
    ) {
      context.addIssue({ code: "custom", message: "market instrument ordering mismatch" })
    }
  })

export type MarketProductRow = z.infer<typeof marketProductRowSchema>
export type MarketProductResult = z.infer<typeof marketProductResultSchema>

export function parseMarketProductResult(
  result: ApplicationResult,
): MarketProductResult {
  return marketProductResultSchema.parse(result)
}

export function marketOverviewRows(
  result: ApplicationResult | undefined,
): MarketProductRow[] {
  if (!result) return []
  return parseMarketProductResult(result).data ?? []
}

export function marketInstrumentRow(
  result: ApplicationResult | undefined,
  instrumentId: string,
): MarketProductRow | null {
  if (!result) return null
  const rows = parseMarketProductResult(result).data ?? []
  if (rows.length === 0) return null
  if (rows.length !== 1 || rows[0]?.instrumentId !== instrumentId) {
    throw new Error("The market detail did not match the selected investment.")
  }
  return rows[0]
}
