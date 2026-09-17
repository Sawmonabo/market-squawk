import { z } from "zod"

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

export const marketProductRowSchema = z.object({
  selectionToken: marketSelectionTokenSchema,
  historyToken: marketHistoryTokenSchema.nullable(),
  identity: identitySchema,
  price: moneySchema.nullable(),
  changePercent: exactDecimalSchema.nullable(),
  asOf: productInstantSchema.nullable(),
  availability: z.enum(["current", "delayed", "previous_close", "unavailable"]),
}).strict().superRefine((row, context) => {
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
