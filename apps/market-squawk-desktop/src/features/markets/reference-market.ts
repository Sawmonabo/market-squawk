import { z } from "zod"

import type { ApplicationResult } from "@/lib/schemas"

export const referenceMarketRowSchema = z
  .object({
    referenceId: z.string().min(1),
    symbol: z.string().min(1),
    name: z.string().min(1),
    assetClass: z.enum(["equity", "fund"]),
    isEtf: z.boolean(),
    effectiveAt: z.string().min(1),
    availableAt: z.string().min(1),
  })
  .strict()

const referenceMarketRowsSchema = z.array(referenceMarketRowSchema).nullable()

export type ReferenceMarketRow = z.infer<typeof referenceMarketRowSchema>

export function referenceMarketRows(
  result: ApplicationResult | undefined,
): ReferenceMarketRow[] {
  if (!result) return []
  return referenceMarketRowsSchema.parse(result.data) ?? []
}
