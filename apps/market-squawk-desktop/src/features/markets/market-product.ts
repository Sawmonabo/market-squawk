import { z } from "zod"
import { formatCalendarDate } from "@/lib/time"
import { formatMoney } from "@/lib/formatters"

import type { ApplicationResult } from "@/lib/schemas"

export const productInstantSchema = z.iso.datetime({ offset: false, precision: 9 })
export const exactDecimalSchema = z.string().regex(/^-?(?:0|[1-9][0-9]*)(?:\.[0-9]*[1-9])?$/).refine((value) => value !== "-0")
const opaqueToken = (prefix: "market" | "history" | "page") =>
  z.string().regex(new RegExp(`^${prefix}_[A-Za-z0-9_-]{32,86}$`))

export const marketSelectionTokenSchema = opaqueToken("market")
export const marketHistoryTokenSchema = opaqueToken("history")
export const marketPageTokenSchema = opaqueToken("page")

const identitySchema = z.object({
  symbol: z.string().trim().min(1).max(64).nullable(),
  name: z.string().trim().min(1).max(256).nullable(),
  assetClass: z.enum(["equity", "fixed_income", "option", "future", "foreign_exchange", "crypto", "commodity", "fund", "index", "cash"]),
}).strict().refine((value) => value.symbol !== null || value.name !== null, {
  message: "An investment needs a display name or symbol.",
})

const moneySchema = z.object({
  value: exactDecimalSchema,
  currency: z.string().regex(/^[A-Z]{3}$/),
}).strict()

const marketQuoteSchema = z.object({
  quoteSizeBasis: z.enum(["quantity", "source_units"]),
  currency: z.string().regex(/^[A-Z]{3}$/),
  bidPrice: exactDecimalSchema.nullable(),
  bidSize: exactDecimalSchema.nullable(),
  askPrice: exactDecimalSchema.nullable(),
  askSize: exactDecimalSchema.nullable(),
  midPrice: exactDecimalSchema.nullable(),
  lastPrice: exactDecimalSchema.nullable(),
  lastSize: exactDecimalSchema.nullable(),
  quoteObservedAt: productInstantSchema.nullable(),
  lastObservedAt: productInstantSchema.nullable(),
  quoteCurrentThrough: productInstantSchema.nullable(),
  lastCurrentThrough: productInstantSchema.nullable(),
  quoteFresh: z.boolean(),
  lastFresh: z.boolean(),
  tradeStatus: z.enum(["available", "ambiguous", "unavailable"]),
}).strict().superRefine((quote, context) => {
  if (quote.tradeStatus !== "available" && (quote.lastPrice !== null || quote.lastSize !== null || quote.lastFresh)) {
    context.addIssue({ code: "custom", message: "An unresolved trade cannot supply a last price or size." })
  }
  if ((quote.quoteFresh && quote.quoteObservedAt === null)
    || (quote.lastFresh && (quote.lastObservedAt === null || quote.lastPrice === null))) {
    context.addIssue({ code: "custom", message: "Current quote and trade values require their own observation clocks." })
  }
})

export const marketProductRowSchema = z.object({
  selectionToken: marketSelectionTokenSchema,
  historyToken: marketHistoryTokenSchema.nullable(),
  identity: identitySchema,
  price: moneySchema.nullable(),
  priceBasis: z.enum(["last_trade", "bid_ask_midpoint", "previous_close"]).nullable(),
  priceCurrentThrough: productInstantSchema.nullable(),
  quote: marketQuoteSchema.nullable(),
  changePercent: exactDecimalSchema.nullable(),
  changeBasis: z.object({
    priceBasis: z.enum(["last_trade", "bid_ask_midpoint"]),
    priceAsOf: productInstantSchema,
    previousClose: moneySchema.extend({ sessionDate: z.iso.date(), asOf: productInstantSchema }).strict(),
    adjustment: z.literal("raw"),
  }).strict().nullable(),
  changeUnavailableReason: z.enum(["current_price_unavailable", "previous_close_unavailable", "incompatible_basis", "arithmetic_unavailable"]).nullable(),
  asOf: productInstantSchema.nullable(),
  availability: z.enum(["current", "delayed", "last_known", "previous_close", "unavailable"]),
}).strict().superRefine((row, context) => {
  if ((row.changePercent === null) !== (row.changeBasis === null)
    || (row.changePercent === null) !== (row.changeUnavailableReason !== null)) {
    context.addIssue({ code: "custom", message: "Price change needs its dated comparison or a reason it is unavailable." })
  }
  if ((row.price === null) !== (row.priceBasis === null)) {
    context.addIssue({ code: "custom", message: "A displayed price requires its basis." })
  }
  if ((row.price === null) !== (row.asOf === null)) {
    context.addIssue({ code: "custom", message: "Price and time must be available together." })
  }
  if (row.availability === "unavailable" && row.price !== null) {
    context.addIssue({ code: "custom", message: "Unavailable investments cannot include a price." })
  }
})

const marketProductResultSchema = z.object({
  data: z.array(marketProductRowSchema).max(100),
  page: z.object({
    hasMore: z.boolean(),
    nextPageToken: marketPageTokenSchema.nullable(),
  }).strict(),
}).strict().superRefine((result, context) => {
  const tokens = result.data.map((row) => row.selectionToken)
  if (new Set(tokens).size !== tokens.length) {
    context.addIssue({ code: "custom", message: "Investment selections must be unique." })
  }
  if (result.page.hasMore !== (result.page.nextPageToken !== null)) {
    context.addIssue({ code: "custom", message: "Market page continuation is inconsistent." })
  }
})

export type MarketProductRow = z.infer<typeof marketProductRowSchema>
export type MarketProductResult = z.infer<typeof marketProductResultSchema>

export function parseMarketProductResult(result: ApplicationResult): MarketProductResult {
  const parsed = marketProductResultSchema.parse(result.data)
  if (parsed.data.length !== result.metadata.returnedItems) {
    throw new Error("Market result count is inconsistent.")
  }
  return parsed
}

export function parseMarketInstrumentResult(result: ApplicationResult, selectionToken: string): MarketProductRow {
  const parsed = parseMarketProductResult(result)
  if (parsed.data.length !== 1 || parsed.page.hasMore || parsed.page.nextPageToken !== null) {
    throw new Error("Selected market result must contain exactly one investment.")
  }
  const row = parsed.data[0]!
  if (row.selectionToken !== selectionToken) {
    throw new Error("Selected market result does not match the requested investment.")
  }
  return row
}

export const marketSessionRequestSchema = z.object({
  product: z.enum(["equity", "option", "bond", "future", "forex"]),
  date: z.iso.date(),
}).strict()

const sessionDigestSchema = z.string().regex(/^[0-9a-f]{64}$/).refine((value) => /[1-9a-f]/.test(value))
const marketSessionReferenceSchema = z.object({
  request: marketSessionRequestSchema,
  originContentSha256: sessionDigestSchema,
  captureBindingSha256: sessionDigestSchema,
}).strict()
const sessionInstantSchema = z.string().max(20).regex(/^-?(?:0|[1-9][0-9]*)$/).refine((value) => {
  if (!/^-?(?:0|[1-9][0-9]*)$/.test(value)) return false
  const nanos = BigInt(value)
  return nanos >= -9_223_372_036_854_775_808n && nanos <= 9_223_372_036_854_775_807n
})
const marketSessionContextSchema = z.object({
  reference: marketSessionReferenceSchema,
  product: marketSessionRequestSchema.shape.product,
  date: marketSessionRequestSchema.shape.date,
  coverage: z.literal("returned_entries_only"),
  entries: z.array(z.object({
    entry: z.number().int().min(1).max(64),
    status: z.enum(["scheduled_sessions", "explicitly_open", "explicitly_closed", "unknown"]),
    sessionPresence: z.enum(["reported", "missing", "source_null", "unknown"]),
    windows: z.array(z.object({
      role: z.enum(["core", "pre", "post", "intermission", "source_defined"]),
      ordinal: z.number().int().min(0).max(31),
      startUnixNanos: sessionInstantSchema,
      endUnixNanos: sessionInstantSchema,
      startUtcOffsetSeconds: z.number().int().min(-86_340).max(86_340),
      endUtcOffsetSeconds: z.number().int().min(-86_340).max(86_340),
    }).strict()).max(32),
  }).strict()).min(1).max(64),
}).strict()

export type MarketSessionRequest = z.infer<typeof marketSessionRequestSchema>
export type MarketSessionReference = z.infer<typeof marketSessionReferenceSchema>
export type MarketSessionContext = z.infer<typeof marketSessionContextSchema>

export function parseMarketSessionContext(
  result: ApplicationResult,
  request: MarketSessionRequest,
  reference?: MarketSessionReference,
): MarketSessionContext {
  const context = marketSessionContextSchema.parse(result.data)
  if (context.product !== request.product || context.date !== request.date
    || context.reference.request.product !== request.product || context.reference.request.date !== request.date
    || result.metadata.completeness !== "complete"
    || result.metadata.returnedItems !== context.entries.length
    || result.metadata.availableItems !== context.entries.length
    || context.entries.some((entry, index) => entry.entry !== index + 1)
    || (reference !== undefined && (context.reference.originContentSha256 !== reference.originContentSha256
      || context.reference.captureBindingSha256 !== reference.captureBindingSha256))) {
    throw new Error("Trading-session information does not match the requested record.")
  }
  return context
}

export function marketAvailabilityLabel(row: MarketProductRow): string | null {
  switch (row.availability) {
    case "current": return "Current"
    case "delayed": return "Delayed"
    case "last_known": return null
    case "previous_close": return "Previous close"
    case "unavailable": return "Unavailable"
  }
}

export function marketPriceBasisLabel(row: MarketProductRow): string | null {
  switch (row.priceBasis) {
    case "last_trade": return "Last trade"
    case "bid_ask_midpoint": return "Bid/ask midpoint"
    case "previous_close": return "Previous close"
    case null: return null
  }
}

export function marketChangeDescription(row: MarketProductRow): string {
  if (row.changeBasis) {
    const { previousClose } = row.changeBasis
    return `Compared with the ${formatCalendarDate(previousClose.sessionDate)} completed close of ${formatMoney({ amount: previousClose.value, currency: previousClose.currency })}.`
  }
  switch (row.changeUnavailableReason) {
    case "current_price_unavailable": return "A current price is needed to calculate change from the previous close."
    case "previous_close_unavailable": return "The previous completed close is not available yet."
    case "incompatible_basis": return "The available price and completed close cannot be compared on the same basis."
    case "arithmetic_unavailable": return "Price change could not be calculated from the available values."
    case null: return "Price change is unavailable."
  }
}
