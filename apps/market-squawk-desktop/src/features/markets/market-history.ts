import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"

import { chartDisplaySchema, chartOriginalPointFields } from "../shared/chart-projection"

import { exactDecimalSchema, marketHistoryTokenSchema, productInstantSchema } from "./market-product"

export const marketHistoryTimeSchema = z.discriminatedUnion("precision", [
  z.object({ precision: z.literal("timestamped_period"), startsAt: productInstantSchema, endsAt: productInstantSchema })
    .strict().refine((time) => time.startsAt < time.endsAt, { message: "A price period must end after it starts." }),
  z.object({ precision: z.literal("nominal_date"), date: z.iso.date() }).strict(),
])

const barSchema = z.object({
  ...chartOriginalPointFields,
  time: marketHistoryTimeSchema,
  open: exactDecimalSchema,
  high: exactDecimalSchema,
  low: exactDecimalSchema,
  close: exactDecimalSchema,
  volume: exactDecimalSchema,
}).strict()

export const marketHistoryResultSchema = z.object({
  data: z.object({
    historyToken: marketHistoryTokenSchema,
    currency: z.string().regex(/^[A-Z]{3}$/),
    bars: z.array(barSchema).max(4_096),
    partial: z.boolean(),
    generationToken: z.string().regex(/^[a-f0-9]{64}$/),
    display: chartDisplaySchema,
    viewport: z.strictObject({
      startUnixNanos: z.string().nullable(), endUnixNanos: z.string().nullable(),
      startDate: z.iso.date().nullable(), endDate: z.iso.date().nullable(),
      pointLimit: z.number().int().min(8).max(4_096),
      fullStartUnixNanos: z.string().nullable(), fullEndUnixNanos: z.string().nullable(),
      fullStartDate: z.iso.date().nullable(), fullEndDate: z.iso.date().nullable(),
    }),
  }).strict().nullable(),
  unavailableReason: z.enum(["not_selected", "not_available", "temporarily_unavailable"]).nullable(),
}).strict().superRefine((result, context) => {
  if ((result.data === null) === (result.unavailableReason === null)) {
    context.addIssue({ code: "custom", message: "Price history must be available or unavailable." })
  }
  if (result.data && (result.data.bars.length !== result.data.display.returnedPointCount
    || result.data.bars.length > result.data.viewport.pointLimit)) {
    context.addIssue({ code: "custom", message: "The price projection exceeds its requested window or reported count." })
  }
  const precision = result.data?.bars[0]?.time.precision
  let previousPeriod: string | undefined
  let previousDate: string | undefined
  result.data?.bars.forEach((bar, index) => {
    if (bar.time.precision !== precision || bar.breakBefore.length !== 3
      || index > 0 && BigInt(bar.originalOrdinal) <= BigInt(result.data!.bars[index - 1]!.originalOrdinal)) {
      context.addIssue({ code: "custom", path: ["data", "bars", index, "time"], message: "Price history must use one time precision." })
    }
    const overlaps = bar.time.precision === "timestamped_period"
      ? previousPeriod !== undefined && previousPeriod > bar.time.startsAt
      : previousDate !== undefined && previousDate >= bar.time.date
    if (overlaps) {
      context.addIssue({ code: "custom", path: ["data", "bars", index], message: "Price periods overlap." })
    }
    if (bar.time.precision === "timestamped_period") previousPeriod = bar.time.endsAt
    else previousDate = bar.time.date
  })
})

export type MarketHistoryResult = z.infer<typeof marketHistoryResultSchema>
export type MarketHistoryBar = z.infer<typeof barSchema>

export type MarketHistoryViewportInput = {
  startUnixNanos?: string; endUnixNanos?: string; startDate?: string; endDate?: string; pointLimit: number
}

// UTC source timestamps contain exactly nine fractional digits. Keep all of
// them when requesting evidence; millisecond conversion is only for drawing.
export function sourceInstantUnixNanos(value: string): string {
  const parsed = productInstantSchema.parse(value)
  const parts = /^(.*)\.(\d{9})Z$/.exec(parsed)!
  const secondsMillis = Date.parse(`${parts[1]}Z`)
  if (!Number.isSafeInteger(secondsMillis)) throw new Error("The original price timestamp is unsupported.")
  return (BigInt(secondsMillis) * 1_000_000n + BigInt(parts[2]!)).toString()
}

export function parseMarketHistoryResult(result: ApplicationResult, historyToken: string, expected: MarketHistoryViewportInput, generationToken?: string): MarketHistoryResult {
  const parsed = marketHistoryResultSchema.parse(result.data)
  const expectedItems = parsed.data?.bars.length ?? 0
  if (result.metadata.returnedItems !== expectedItems) {
    throw new Error("Price history count is inconsistent.")
  }
  if (parsed.data !== null && (parsed.data.historyToken !== historyToken
    || generationToken !== undefined && parsed.data.generationToken !== generationToken
    || parsed.data.viewport.pointLimit !== expected.pointLimit
    || parsed.data.viewport.startUnixNanos !== (expected.startUnixNanos ?? null)
    || parsed.data.viewport.endUnixNanos !== (expected.endUnixNanos ?? null)
    || parsed.data.viewport.startDate !== (expected.startDate ?? null)
    || parsed.data.viewport.endDate !== (expected.endDate ?? null))) {
    throw new Error("Price history does not match the selected investment.")
  }
  return parsed
}
