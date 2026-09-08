import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"

import { exactDecimalSchema, marketHistoryTokenSchema, productInstantSchema } from "./market-product"

const barSchema = z.object({
  startsAt: productInstantSchema,
  endsAt: productInstantSchema,
  open: exactDecimalSchema,
  high: exactDecimalSchema,
  low: exactDecimalSchema,
  close: exactDecimalSchema,
  volume: exactDecimalSchema,
}).strict().refine((bar) => bar.startsAt < bar.endsAt, {
  message: "A price period must end after it starts.",
})

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
  result.data?.bars.forEach((bar, index, bars) => {
    if (index > 0 && bars[index - 1]!.endsAt > bar.startsAt) {
      context.addIssue({ code: "custom", path: ["data", "bars", index], message: "Price periods overlap." })
    }
  })
})

export type MarketHistoryResult = z.infer<typeof marketHistoryResultSchema>

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
