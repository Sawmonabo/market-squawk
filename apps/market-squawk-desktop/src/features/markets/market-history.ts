import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"

import { exactDecimalSchema, marketHistoryTokenSchema, productInstantSchema } from "./market-product"

export const marketHistoryTimeSchema = z.discriminatedUnion("precision", [
  z.object({ precision: z.literal("timestamped_period"), startsAt: productInstantSchema, endsAt: productInstantSchema })
    .strict().refine((time) => time.startsAt < time.endsAt, { message: "A price period must end after it starts." }),
  z.object({ precision: z.literal("nominal_date"), date: z.iso.date() }).strict(),
])

const barSchema = z.object({
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
    bars: z.array(barSchema).min(1).max(1_000),
    partial: z.boolean(),
  }).strict().nullable(),
  unavailableReason: z.enum(["not_selected", "not_available", "temporarily_unavailable"]).nullable(),
}).strict().superRefine((result, context) => {
  if ((result.data === null) === (result.unavailableReason === null)) {
    context.addIssue({ code: "custom", message: "Price history must be available or unavailable." })
  }
  const precision = result.data?.bars[0]?.time.precision
  let previousPeriod: string | undefined
  let previousDate: string | undefined
  result.data?.bars.forEach((bar, index) => {
    if (bar.time.precision !== precision) {
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

export function parseMarketHistoryResult(result: ApplicationResult, historyToken: string): MarketHistoryResult {
  const parsed = marketHistoryResultSchema.parse(result.data)
  const expectedItems = parsed.data?.bars.length ?? 0
  if (result.metadata.returnedItems !== expectedItems) {
    throw new Error("Price history count is inconsistent.")
  }
  if (parsed.data !== null && parsed.data.historyToken !== historyToken) {
    throw new Error("Price history does not match the selected investment.")
  }
  return parsed
}
